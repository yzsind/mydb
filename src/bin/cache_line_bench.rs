//! cache_line_bench: Measure how cache lines, memory hierarchy, and false sharing affect performance.
//!
//! Usage:
//!   cargo run --release --features tools --bin cache_line_bench
//!   cargo run --release --features tools --bin cache_line_bench -- --threads 8
//!   cargo run --release --features tools --bin cache_line_bench -- --skip stride
//!
//! Three experiments:
//!   1. Stride scan: read a fixed array at different byte strides and observe latency changes
//!      near the cache-line boundary (64B), including hardware prefetch effects.
//!   2. Working-set ladder: vary working-set size with sequential reads and Sattolo pointer chasing.
//!      Sequential access measures cache/DRAM bandwidth under prefetching; pointer chasing defeats it
//!      to expose individual L1, L2, L3, and DRAM access latency.
//!   3. False sharing: N threads increment separate counters, comparing adjacent counters
//!      against counters isolated on separate 64B lines to measure cache-line ping-pong.
//!
//! Measurement: adapt each case to run >=200ms; report ns/op, bandwidth, and speedup.
//!
//! Results depend on CPU, cache sizes, and scheduling; use medians of repeated runs on the same host.

use std::hint::black_box;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::{Duration, Instant};

/// Conventional x86 and most ARM cache lines are 64B; Apple M-series (M1+) lines are 128B.
/// `CACHE_LINE` is the reporting reference for stride measurements and observed latency changes.
/// `PADDED_LINE` aligns PaddedAtomic in the false-sharing test; the larger size ensures isolation.
const CACHE_LINE: usize = 64;
const PADDED_LINE: usize = 128;
const KIB: usize = 1024;
const MIB: usize = 1024 * 1024;

/// Actual platform cache-line size for reporting; available on macOS, otherwise defaults to 64.
fn detect_cache_line() -> Option<usize> {
    #[cfg(target_os = "macos")]
    unsafe {
        let mut size: u64 = 0;
        let mut len: libc::size_t = std::mem::size_of::<u64>();
        let name = b"hw.cachelinesize\0";
        let rc = libc::sysctlbyname(
            name.as_ptr() as *const _,
            &mut size as *mut _ as *mut _,
            &mut len,
            std::ptr::null_mut(),
            0,
        );
        if rc == 0 && size > 0 {
            return Some(size as usize);
        }
    }
    None
}

// =============================================================================
// Shared utilities.
// =============================================================================

fn fmt_size(n: usize) -> String {
    if n >= MIB && n % MIB == 0 {
        format!("{}MiB", n / MIB)
    } else if n >= KIB && n % KIB == 0 {
        format!("{}KiB", n / KIB)
    } else if n >= MIB {
        format!("{:.1}MiB", n as f64 / MIB as f64)
    } else if n >= KIB {
        format!("{:.1}KiB", n as f64 / KIB as f64)
    } else {
        format!("{}B", n)
    }
}

/// Run `work` for at least approximately 200ms and return nanoseconds per internal step.
/// `steps_per_iter` counts elementary steps performed by one invocation of the work closure,
/// such as array elements scanned or pointer-chase links followed.
fn adaptive<F: FnMut() -> u64>(steps_per_iter: usize, mut work: F) -> f64 {
    // Warm up once to fill caches and train branch prediction.
    black_box(work());

    let target = Duration::from_millis(200);
    let mut iters: usize = 1;
    loop {
        let start = Instant::now();
        let mut acc: u64 = 0;
        for _ in 0..iters {
            acc = acc.wrapping_add(work());
        }
        let elapsed = start.elapsed();
        black_box(acc);
        if elapsed >= target || iters >= 1 << 22 {
            let total_steps = (iters as f64) * (steps_per_iter as f64);
            return elapsed.as_nanos() as f64 / total_steps;
        }
        // Estimate the required iteration count.
        let factor = (target.as_nanos() as f64 / elapsed.as_nanos().max(1) as f64).ceil() as usize;
        iters = iters.saturating_mul(factor.max(2)).min(1 << 22);
    }
}

// =============================================================================
// Experiment 1: stride scan.
// =============================================================================
//
// The fixed N MiB array exceeds L3; read u64 values at byte stride S for N / S accesses.
//
// S < 64: 64/S accesses share a line; amortized cache-line traffic is S bytes per access.
// S = 64: each access uses a separate cache line.
// S > 64: each access uses a separate line, skipping S/64 - 1 intervening lines.
//
// Hardware prefetchers track fixed strides (Intel L2 stride prefetcher and similar Apple mechanisms),
// so access latency need not increase linearly below their thresholds. Crossing 4 KiB pages
// increases TLB pressure, raising latency and reducing throughput.

fn fill_with_noise(buf: &mut [u64], seed: u64) {
    let mut s = seed | 1;
    for x in buf.iter_mut() {
        s = s
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        *x = s;
    }
}

fn bench_stride(buf: &[u64], stride_bytes: usize) -> f64 {
    let stride_words = stride_bytes / std::mem::size_of::<u64>();
    assert!(stride_words >= 1, "stride must be >= 8B");
    let n_access = buf.len() / stride_words;
    if n_access < 1024 {
        return f64::NAN;
    }
    adaptive(n_access, || {
        let mut acc: u64 = 0;
        let mut idx: usize = 0;
        for _ in 0..n_access {
            // SAFETY: idx increases monotonically to n_access * stride_words <= buf.len().
            acc = acc.wrapping_add(unsafe { *buf.get_unchecked(idx) });
            idx += stride_words;
        }
        acc
    })
}

fn run_stride(array_bytes: usize) {
    println!();
    println!("=== Experiment 1: stride scan (cache-line boundary) ===");
    println!(
        "Array = {} (well above L3 to avoid fitting in cache); read u64 values at byte strides.",
        fmt_size(array_bytes)
    );
    println!();

    let n_u64 = array_bytes / std::mem::size_of::<u64>();
    let mut buf = vec![0u64; n_u64];
    fill_with_noise(&mut buf, 0xA5A5_A5A5_A5A5_A5A5);

    let strides = [8usize, 16, 32, 64, 128, 256, 512, 1024, 4096];
    let col_w = 13;

    println!(
        "{:<10} {:>cw$} {:>cw$} {:>cw$}",
        "stride",
        "ns/access",
        "data GB/s",
        "line GB/s",
        cw = col_w
    );
    println!("{}", "-".repeat(10 + (col_w + 1) * 3));

    let mut base_ns_8 = 0f64;
    for (i, &s) in strides.iter().enumerate() {
        let ns = bench_stride(&buf, s);
        if i == 0 {
            base_ns_8 = ns;
        }
        // Useful bandwidth: each access reads 8 bytes of user data.
        let data_gbs = 8.0 / (ns * 1e-9) / 1e9;
        // Actual line traffic: amortized cache-line bytes.
        // stride < 64: multiple accesses per line; amortized traffic equals stride.
        // stride >= 64: a new line per access; amortized traffic is 64 bytes.
        let amortized_line = if s >= CACHE_LINE {
            CACHE_LINE as f64
        } else {
            s as f64
        };
        let line_gbs = amortized_line / (ns * 1e-9) / 1e9;

        let note = if s == 64 {
            "  <- 1 access / cache line"
        } else if s == 4096 {
            "  <- crosses a 4KiB page (TLB pressure)"
        } else {
            ""
        };
        println!(
            "{:<10} {:>cw$.2} {:>cw$.2} {:>cw$.2}{}",
            format!("{}B", s),
            ns,
            data_gbs,
            line_gbs,
            note,
            cw = col_w
        );
        let _ = base_ns_8;
    }

    println!();
    println!("Interpretation:");
    println!("- stride <= 64B: hardware prefetching keeps pace, so ns/access rises gradually;");
    println!(
        "  data GB/s falls as stride increases because each cache line serves fewer accesses."
    );
    println!(
        "- stride >= 64B: each access uses a separate line; line GB/s approaches DRAM bandwidth."
    );
    println!("- stride = 4096B (one page): TLB misses and cross-page prefetch failures usually amplify latency.");
}

// =============================================================================
// Experiment 2: working-set ladder.
// =============================================================================
//
// Working sets cover L1 (<=128 KiB), L2 (a few MiB), L3 (tens of MiB), and DRAM (>L3).
// Two access patterns:
//   - sequential: reads favor hardware prefetching and streaming bandwidth.
//   - pointer chase: Sattolo creates one random cycle; random next pointers defeat prefetching.
//
// Pointer chasing measures cache access latency; sequential reads measure achievable bandwidth.

/// Sattolo's algorithm creates a single-cycle permutation over all n elements.
/// The chase visits every node rather than getting trapped in a smaller cycle.
fn build_random_ring(n: usize, seed: u64) -> Vec<u64> {
    let mut order: Vec<usize> = (0..n).collect();
    // Sattolo's algorithm restricts j < i to produce a single cycle.
    let mut state = seed | 1;
    for i in (1..n).rev() {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        let j = (state as usize) % i;
        order.swap(i, j);
    }
    // Interpret order as traversal order: order[k] points to order[(k+1) % n].
    let mut ring = vec![0u64; n];
    for k in 0..n {
        ring[order[k]] = order[(k + 1) % n] as u64;
    }
    ring
}

fn bench_sequential(buf: &[u64]) -> f64 {
    let n = buf.len();
    adaptive(n, || {
        let mut acc: u64 = 0;
        for i in 0..n {
            acc = acc.wrapping_add(unsafe { *buf.get_unchecked(i) });
        }
        acc
    })
}

fn bench_pointer_chase(ring: &[u64], steps: usize) -> f64 {
    adaptive(steps, || {
        let mut idx: usize = 0;
        for _ in 0..steps {
            idx = unsafe { *ring.get_unchecked(idx) } as usize;
        }
        idx as u64
    })
}

fn run_working_set(sizes: &[usize]) {
    println!();
    println!("=== Experiment 2: working-set ladder (cache hierarchy) ===");
    println!("seq = sequential reads (prefetch-friendly, near the bandwidth limit);");
    println!("ptr = Sattolo random-cycle pointer chase (defeats prefetching, exposing hierarchy latency).");
    println!();

    let col_w = 12;
    println!(
        "{:<10} {:>cw$} {:>cw$} {:>cw$}",
        "Working set",
        "seq ns/op",
        "seq GB/s",
        "ptr ns/op",
        cw = col_w
    );
    println!("{}", "-".repeat(10 + (col_w + 1) * 3));

    for &bytes in sizes {
        let n = bytes / std::mem::size_of::<u64>();
        if n < 16 {
            continue;
        }

        // sequential
        let mut buf = vec![0u64; n];
        fill_with_noise(&mut buf, 0xC0FF_EE42 ^ bytes as u64);
        let ns_seq = bench_sequential(&buf);
        let gbs = 8.0 / (ns_seq * 1e-9) / 1e9;
        drop(buf);

        // Cap pointer chasing at 1M steps to bound runtime for large working sets.
        let steps = n.min(1 << 20);
        let ring = build_random_ring(n, 0xDEAD_BEEF ^ bytes as u64);
        let ns_chase = bench_pointer_chase(&ring, steps);

        println!(
            "{:<10} {:>cw$.2} {:>cw$.2} {:>cw$.2}",
            fmt_size(bytes),
            ns_seq,
            gbs,
            ns_chase,
            cw = col_w
        );
    }

    println!();
    println!("Interpretation:");
    println!("- seq: cached working sets have high GB/s; beyond the last-level cache, DRAM bandwidth limits throughput.");
    println!("- ptr: latency steps directly reveal the CPU cache-hierarchy signature;");
    println!(
        "       Apple M-series reference: L1d ~1-2 ns, L2 ~3-8 ns, SLC ~10-15 ns, DRAM ~70-110 ns;"
    );
    println!(
        "       Intel/AMD desktops typically: L1 ~1 ns, L2 ~3-4 ns, L3 ~10-15 ns, DRAM ~80-130 ns."
    );
    println!("- Look for 3-5x jumps in ptr latency to identify cache-capacity boundaries.");
}

// =============================================================================
// Experiment 3: false sharing.
// =============================================================================
//
// N threads each increment their own counter K times:
//   A. Compact layout: [AtomicU64; N] fits eight counters per line, causing mutual invalidations.
//   B. Padded layout: #[repr(align(64))] isolates each counter on its own line.
//
// The uncontended single-thread baseline measures the single-core fetch_add limit.
//
// Expected: A approaches single-thread throughput because cache-line ping-pong serializes work;
// B approaches N times single-thread throughput under ideal linear scaling.

/// Align to 128B to cover both 64B x86/conventional ARM lines and 128B Apple M-series lines.
#[repr(align(128))]
struct PaddedAtomic {
    v: AtomicU64,
}

fn run_false_sharing(threads: usize, iters_per_thread: u64) {
    println!();
    println!("=== Experiment 3: false sharing (multicore cache-line contention) ===");
    println!(
        "{} threads, {} fetch_add operations per thread; compact layout versus {}B padding per counter.",
        threads, iters_per_thread, PADDED_LINE
    );
    println!();

    // ----- Single-thread baseline -----
    let one = AtomicU64::new(0);
    // Warm up.
    for _ in 0..1024 {
        one.fetch_add(1, Ordering::Relaxed);
    }
    let t0 = Instant::now();
    for _ in 0..iters_per_thread {
        one.fetch_add(1, Ordering::Relaxed);
    }
    let elapsed_single = t0.elapsed();
    black_box(one.load(Ordering::Relaxed));

    // ----- A: compact layout (false sharing) -----
    let elapsed_packed = run_parallel_counters(
        threads,
        iters_per_thread,
        |n| {
            // Vec<AtomicU64>: elements are 8B-aligned; eight share a line.
            Arc::new((0..n).map(|_| AtomicU64::new(0)).collect::<Vec<_>>())
        },
        |arr, tid| arr[tid].fetch_add(1, Ordering::Relaxed),
    );

    // ----- B: padded layout (no false sharing) -----
    let elapsed_padded = run_parallel_counters(
        threads,
        iters_per_thread,
        |n| {
            Arc::new(
                (0..n)
                    .map(|_| PaddedAtomic {
                        v: AtomicU64::new(0),
                    })
                    .collect::<Vec<_>>(),
            )
        },
        |arr, tid| arr[tid].v.fetch_add(1, Ordering::Relaxed),
    );

    // ----- Report -----
    let total_ops = (threads as u64) * iters_per_thread;
    let ns_single = elapsed_single.as_nanos() as f64 / iters_per_thread as f64;
    let ns_packed = elapsed_packed.as_nanos() as f64 / total_ops as f64;
    let ns_padded = elapsed_padded.as_nanos() as f64 / total_ops as f64;

    // Speedup = N * single-thread total time / concurrent total time.
    // Ideal speedup is N threads; 1.0 means fully serialized execution.
    let theoretical_serial_ns = elapsed_single.as_nanos() as f64 * threads as f64;
    let speedup_packed = theoretical_serial_ns / elapsed_packed.as_nanos() as f64;
    let speedup_padded = theoretical_serial_ns / elapsed_padded.as_nanos() as f64;

    let col_w = 14;
    println!(
        "{:<32} {:>cw$} {:>cw$} {:>cw$}",
        "case",
        "Total ms",
        "ns / op",
        "Speedup",
        cw = col_w
    );
    println!("{}", "-".repeat(32 + (col_w + 1) * 3));
    println!(
        "{:<32} {:>cw$.2} {:>cw$.2} {:>cw$}",
        "Single-thread baseline",
        elapsed_single.as_secs_f64() * 1000.0,
        ns_single,
        "1.00x",
        cw = col_w
    );
    println!(
        "{:<32} {:>cw$.2} {:>cw$.2} {:>cw$}",
        "Compact layout (false sharing)",
        elapsed_packed.as_secs_f64() * 1000.0,
        ns_packed,
        format!("{:.2}x", speedup_packed),
        cw = col_w
    );
    println!(
        "{:<32} {:>cw$.2} {:>cw$.2} {:>cw$}",
        format!("{}B padding (no false sharing)", PADDED_LINE),
        elapsed_padded.as_secs_f64() * 1000.0,
        ns_padded,
        format!("{:.2}x", speedup_padded),
        cw = col_w
    );

    println!();
    println!("Interpretation:");
    println!(
        "- In the compact layout, {} AtomicU64 counters share the same or adjacent cache lines;",
        threads
    );
    println!("  fetch_add on one core invalidates other cores' cached copies via MESI/MOESI;");
    println!("  cache-line ping-pong between cores reduces speedup below 1x.");
    println!(
        "- With {}B padding, counters occupy separate lines and speedup approaches the thread count.",
        PADDED_LINE
    );
    println!("- Similar mydb patterns (per-thread statistics, counters, and connection state) need cache-line isolation,");
    println!("  or contention can eliminate parallel gains across connections and workers.");
}

/// Parallel counter increments: main and worker threads share one Barrier for a synchronized start.
fn run_parallel_counters<T, B, W>(
    threads: usize,
    iters_per_thread: u64,
    builder: B,
    work: W,
) -> Duration
where
    T: Send + Sync + 'static,
    B: Fn(usize) -> Arc<Vec<T>>,
    W: Fn(&[T], usize) -> u64 + Send + Sync + Copy + 'static,
{
    let arr = builder(threads);
    let barrier = Arc::new(Barrier::new(threads + 1));
    let mut handles = Vec::with_capacity(threads);
    for tid in 0..threads {
        let arr = Arc::clone(&arr);
        let barrier = Arc::clone(&barrier);
        handles.push(thread::spawn(move || {
            // Warm up to reach steady state.
            for _ in 0..1024 {
                let _ = work(arr.as_slice(), tid);
            }
            barrier.wait();
            for _ in 0..iters_per_thread {
                let _ = work(arr.as_slice(), tid);
            }
        }));
    }
    barrier.wait();
    let t0 = Instant::now();
    for h in handles {
        h.join().expect("worker panicked");
    }
    t0.elapsed()
}

// =============================================================================
// Arguments and entry point.
// =============================================================================

struct Args {
    threads: usize,
    fs_iters: u64,
    stride_array_bytes: usize,
    sizes: Vec<usize>,
    skip: Vec<String>,
}

fn parse_args() -> Args {
    let n_cpu = thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(8);
    let mut a = Args {
        threads: n_cpu,
        fs_iters: 50_000_000,
        stride_array_bytes: 64 * MIB,
        sizes: vec![
            16 * KIB,
            32 * KIB,
            64 * KIB,
            128 * KIB,
            256 * KIB,
            512 * KIB,
            1 * MIB,
            2 * MIB,
            4 * MIB,
            8 * MIB,
            16 * MIB,
            32 * MIB,
            64 * MIB,
            128 * MIB,
        ],
        skip: vec![],
    };
    let mut it = std::env::args().skip(1);
    while let Some(arg) = it.next() {
        match arg.as_str() {
            "--threads" => {
                a.threads = it
                    .next()
                    .expect("--threads requires a positive integer")
                    .parse()
                    .expect("--threads must be a positive integer");
            }
            "--fs-iters" => {
                a.fs_iters = it
                    .next()
                    .expect("--fs-iters requires a positive integer")
                    .parse()
                    .expect("--fs-iters must be a positive integer");
            }
            "--stride-bytes" => {
                a.stride_array_bytes = it
                    .next()
                    .expect("--stride-bytes requires a positive integer")
                    .parse()
                    .expect("--stride-bytes must be a positive integer");
            }
            "--skip" => {
                let target = it
                    .next()
                    .expect("--skip requires a name: stride|working|fs");
                a.skip.push(target);
            }
            "-h" | "--help" => {
                eprintln!(
                    "cache_line_bench [--threads N] [--fs-iters N] [--stride-bytes N]\n                  \
                     [--skip stride|working|fs]"
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

    println!(
        "cache_line_bench: CPU cache-line, memory-hierarchy, and false-sharing microbenchmark"
    );
    let detected = detect_cache_line();
    match detected {
        Some(n) => println!(
            "Detected cache line = {} B (sysctl hw.cachelinesize); PaddedAtomic alignment = {} B; threads = {}.",
            n, PADDED_LINE, args.threads
        ),
        None => println!(
            "Cache-line detection unavailable; default = {} B; PaddedAtomic alignment = {} B; threads = {}.",
            CACHE_LINE, PADDED_LINE, args.threads
        ),
    }
    println!("(The stride sweep covers 8B-4KiB; the first major ns/access jump indicates the actual cache-line size.)");

    let skip = |name: &str| args.skip.iter().any(|s| s == name);

    if !skip("stride") {
        run_stride(args.stride_array_bytes);
    }
    if !skip("working") {
        run_working_set(&args.sizes);
    }
    if !skip("fs") {
        run_false_sharing(args.threads, args.fs_iters);
    }

    println!();
    println!("====================");
    println!("Tips:");
    println!("- Repeat and take the median; connect laptop power and close other CPU-intensive applications;");
    println!("- Browsers, IDEs, and compilers can significantly disturb L3/DRAM measurements;");
    println!("- On M-series macOS, efficiency cores can be 2-3x slower than performance cores.");
}
