//! Temporary benchmark comparing four multidimensional KNN/L2 computation paths.
//!
//!   1. `scalar`: pure Rust scalar element-wise fma, an auto-vectorization baseline.
//!   2. `neon`: handwritten AArch64 NEON SIMD using float32x4 and two accumulators.
//!   3. `amx`: Apple Accelerate `cblas_sgemm`. On Apple Silicon,
//!      Accelerate BLAS dispatches to the AMX matrix coprocessor, representing
//!      the AMX path. Convert L2 via ||q-x||^2 = ||q||^2 + ||x||^2 - 2*q*x
//!      to compute all dot products with one GEMM.
//!   4. `sme`: exact L2 Streaming-SVE batch kernel used by production brute-force KNN.
//!      Timing includes production scan-side candidate batch copying.
//!
//! Find L2 top-k neighbors for batches of queries against `DB` vectors of dimension `DIM`.
//! Each worker processes its own query batch; measure wall-clock throughput with 1/2/4/8/16 threads.
//!
//! Run:
//!   cargo run --release --features tools --bin vector_search_bench
//!   VS_DIM=1536 VS_DB=100000 VS_QPT=16 cargo run --release --features tools --bin vector_search_bench
//!
//! Key environment variables, all with defaults:
//!   VS_DIM: vector dimension (default 768).
//!   VS_DB: database vector count (default 50000).
//!   VS_QPT: query count per thread (default 16).
//!   VS_K: top-k (default 10).
//!   VS_ITERS: repetitions per configuration, reporting the median (default 5).
//!   VS_THREADS: comma-separated concurrency list (default 1,2,4,8,16).
//!   VS_SME_BATCH_ROWS: SME batch rows (default 512, matching production).
//!   VS_DIM_SWEEP=1: additionally sweep {128,384,768,1536} dimensions with one thread.

use std::sync::Arc;
use std::time::Instant;

// ---------------------------------------------------------------------------
// Apple Accelerate (AMX path).
// ---------------------------------------------------------------------------
#[cfg(target_os = "macos")]
#[allow(non_camel_case_types)]
mod accel {
    use std::os::raw::c_int;
    pub const CBLAS_ROW_MAJOR: c_int = 101;
    pub const CBLAS_NO_TRANS: c_int = 111;
    pub const CBLAS_TRANS: c_int = 112;

    #[link(name = "Accelerate", kind = "framework")]
    extern "C" {
        // C = alpha * op(A) * op(B) + beta * C, single precision.
        pub fn cblas_sgemm(
            order: c_int,
            transa: c_int,
            transb: c_int,
            m: c_int,
            n: c_int,
            k: c_int,
            alpha: f32,
            a: *const f32,
            lda: c_int,
            b: *const f32,
            ldb: c_int,
            beta: f32,
            c: *mut f32,
            ldc: c_int,
        );
    }
}

// ---------------------------------------------------------------------------
// Distance kernels.
// ---------------------------------------------------------------------------

#[inline]
fn l2_scalar(a: &[f32], b: &[f32]) -> f32 {
    debug_assert_eq!(a.len(), b.len());
    let mut s = 0.0f32;
    for i in 0..a.len() {
        let d = a[i] - b[i];
        s += d * d;
    }
    s
}

#[cfg(target_arch = "aarch64")]
#[inline]
unsafe fn l2_neon(a: &[f32], b: &[f32]) -> f32 {
    use std::arch::aarch64::*;
    let n = a.len();
    let pa = a.as_ptr();
    let pb = b.as_ptr();
    let mut acc0 = vdupq_n_f32(0.0);
    let mut acc1 = vdupq_n_f32(0.0);
    let mut acc2 = vdupq_n_f32(0.0);
    let mut acc3 = vdupq_n_f32(0.0);
    let mut i = 0usize;
    while i + 16 <= n {
        let d0 = vsubq_f32(vld1q_f32(pa.add(i)), vld1q_f32(pb.add(i)));
        acc0 = vfmaq_f32(acc0, d0, d0);
        let d1 = vsubq_f32(vld1q_f32(pa.add(i + 4)), vld1q_f32(pb.add(i + 4)));
        acc1 = vfmaq_f32(acc1, d1, d1);
        let d2 = vsubq_f32(vld1q_f32(pa.add(i + 8)), vld1q_f32(pb.add(i + 8)));
        acc2 = vfmaq_f32(acc2, d2, d2);
        let d3 = vsubq_f32(vld1q_f32(pa.add(i + 12)), vld1q_f32(pb.add(i + 12)));
        acc3 = vfmaq_f32(acc3, d3, d3);
        i += 16;
    }
    while i + 4 <= n {
        let d0 = vsubq_f32(vld1q_f32(pa.add(i)), vld1q_f32(pb.add(i)));
        acc0 = vfmaq_f32(acc0, d0, d0);
        i += 4;
    }
    let acc = vaddq_f32(vaddq_f32(acc0, acc1), vaddq_f32(acc2, acc3));
    let mut s = vaddvq_f32(acc);
    while i < n {
        let d = *pa.add(i) - *pb.add(i);
        s += d * d;
        i += 1;
    }
    s
}

// ---------------------------------------------------------------------------
// Maintain top-k; small k permits linear insertion.
// ---------------------------------------------------------------------------
struct TopK {
    k: usize,
    // Store (dist, idx) in ascending distance order, with the worst at the end.
    buf: Vec<(f32, u32)>,
}
impl TopK {
    fn new(k: usize) -> Self {
        TopK {
            k,
            buf: Vec::with_capacity(k + 1),
        }
    }
    #[inline]
    fn push(&mut self, dist: f32, idx: u32) {
        if self.buf.len() == self.k {
            // Worst element is last.
            if dist >= self.buf[self.k - 1].0 {
                return;
            }
            self.buf.pop();
        }
        // Preserve ascending order when inserting.
        let mut p = self.buf.len();
        self.buf.push((dist, idx));
        while p > 0 && self.buf[p - 1].0 > dist {
            self.buf.swap(p, p - 1);
            p -= 1;
        }
    }
    fn checksum(&self) -> f64 {
        // Prevent optimization and cross-check correctness.
        self.buf
            .iter()
            .map(|(d, i)| *d as f64 + *i as f64 * 1e-6)
            .sum()
    }
}

// ---------------------------------------------------------------------------
// Three search implementations: query batch against the full database; accumulate a checksum to prevent elimination.
// ---------------------------------------------------------------------------

fn search_scalar(queries: &[f32], db: &[f32], dim: usize, n: usize, k: usize) -> f64 {
    let nq = queries.len() / dim;
    let mut acc = 0.0f64;
    for qi in 0..nq {
        let q = &queries[qi * dim..qi * dim + dim];
        let mut tk = TopK::new(k);
        for j in 0..n {
            let x = &db[j * dim..j * dim + dim];
            tk.push(l2_scalar(q, x), j as u32);
        }
        acc += tk.checksum();
    }
    acc
}

#[cfg(target_arch = "aarch64")]
fn search_neon(queries: &[f32], db: &[f32], dim: usize, n: usize, k: usize) -> f64 {
    let nq = queries.len() / dim;
    let mut acc = 0.0f64;
    for qi in 0..nq {
        let q = &queries[qi * dim..qi * dim + dim];
        let mut tk = TopK::new(k);
        for j in 0..n {
            let x = &db[j * dim..j * dim + dim];
            let d = unsafe { l2_neon(q, x) };
            tk.push(d, j as u32);
        }
        acc += tk.checksum();
    }
    acc
}

/// Production-equivalent SME path: heap rows are not globally contiguous, so
/// each worker copies a bounded candidate block into row-major scratch before
/// entering streaming mode once for the whole block. Keep that copy in this
/// benchmark rather than reporting an unattainable contiguous-array result.
#[cfg(target_arch = "aarch64")]
fn search_sme(
    queries: &[f32],
    db: &[f32],
    dim: usize,
    n: usize,
    k: usize,
    batch_rows: usize,
) -> f64 {
    let nq = queries.len() / dim;
    let mut acc = 0.0f64;
    let mut packed = Vec::<f32>::with_capacity(batch_rows * dim);
    let mut dist = vec![0.0f32; batch_rows];
    for qi in 0..nq {
        let q = &queries[qi * dim..qi * dim + dim];
        let mut tk = TopK::new(k);
        let mut base = 0usize;
        while base < n {
            let rows = batch_rows.min(n - base);
            packed.clear();
            packed.extend_from_slice(&db[base * dim..(base + rows) * dim]);
            assert!(mydb::engine::vec_bench_api::sme_l2_batch(
                q,
                &packed,
                &mut dist[..rows]
            ));
            for (j, &d) in dist[..rows].iter().enumerate() {
                tk.push(d, (base + j) as u32);
            }
            base += rows;
        }
        acc += tk.checksum();
    }
    acc
}

/// AMX path: GEMM computes an nq-by-n dot-product matrix, then norms recover L2 distances for top-k.
#[cfg(target_os = "macos")]
fn search_amx(
    queries: &[f32],
    db: &[f32],
    dim: usize,
    n: usize,
    k: usize,
    xnorm: &[f32],        // Squared database vector norms, computed once.
    gemm_buf: &mut [f32], // Reusable nq*n buffer.
) -> f64 {
    let nq = queries.len() / dim;
    // qnorm
    let mut qnorm = vec![0.0f32; nq];
    for qi in 0..nq {
        let q = &queries[qi * dim..qi * dim + dim];
        qnorm[qi] = q.iter().map(|v| v * v).sum();
    }
    // G = -2 * Q(nq×dim) * X(n×dim)^T  ->  (nq × n) row-major
    unsafe {
        accel::cblas_sgemm(
            accel::CBLAS_ROW_MAJOR,
            accel::CBLAS_NO_TRANS,
            accel::CBLAS_TRANS,
            nq as i32,
            n as i32,
            dim as i32,
            -2.0,
            queries.as_ptr(),
            dim as i32,
            db.as_ptr(),
            dim as i32,
            0.0,
            gemm_buf.as_mut_ptr(),
            n as i32,
        );
    }
    let mut acc = 0.0f64;
    for qi in 0..nq {
        let row = &gemm_buf[qi * n..qi * n + n];
        let qn = qnorm[qi];
        let mut tk = TopK::new(k);
        for j in 0..n {
            // Minor negative roundoff does not affect top-k ordering.
            let d = qn + xnorm[j] + row[j];
            tk.push(d, j as u32);
        }
        acc += tk.checksum();
    }
    acc
}

// ---------------------------------------------------------------------------
// Deterministic data generation without external dependencies.
// ---------------------------------------------------------------------------
fn gen(count: usize, dim: usize, seed: u64) -> Vec<f32> {
    let mut s = seed.wrapping_mul(0x9E3779B97F4A7C15).wrapping_add(1);
    let mut v = Vec::with_capacity(count * dim);
    for _ in 0..count * dim {
        // xorshift64*
        s ^= s >> 12;
        s ^= s << 25;
        s ^= s >> 27;
        let r = (s.wrapping_mul(0x2545F4914F6CDD1D) >> 40) as u32; // 24-bit
                                                                   // Map to [-1, 1).
        v.push((r as f32 / (1u32 << 23) as f32) - 1.0);
    }
    v
}

#[derive(Clone, Copy)]
enum Method {
    Scalar,
    Neon,
    Sme,
    Amx,
}
impl Method {
    fn name(&self) -> &'static str {
        match self {
            Method::Scalar => "scalar",
            Method::Neon => "neon(simd)",
            Method::Sme => "sme(streaming)",
            Method::Amx => "amx(accelerate)",
        }
    }
}

struct Shared {
    db: Vec<f32>,
    xnorm: Vec<f32>,
    queries: Vec<f32>, // All queries needed at maximum concurrency.
    dim: usize,
    n: usize,
    k: usize,
    qpt: usize,
    sme_batch_rows: usize,
}

/// Run one (method, threads) configuration; return wall-clock seconds and checksum.
fn run_config(sh: &Arc<Shared>, method: Method, threads: usize) -> (f64, f64) {
    let start = Instant::now();
    let mut handles = Vec::with_capacity(threads);
    for t in 0..threads {
        let sh = Arc::clone(sh);
        handles.push(std::thread::spawn(move || {
            let q0 = t * sh.qpt * sh.dim;
            let q = &sh.queries[q0..q0 + sh.qpt * sh.dim];
            match method {
                Method::Scalar => search_scalar(q, &sh.db, sh.dim, sh.n, sh.k),
                #[cfg(target_arch = "aarch64")]
                Method::Neon => search_neon(q, &sh.db, sh.dim, sh.n, sh.k),
                #[cfg(not(target_arch = "aarch64"))]
                Method::Neon => search_scalar(q, &sh.db, sh.dim, sh.n, sh.k),
                #[cfg(target_arch = "aarch64")]
                Method::Sme => search_sme(q, &sh.db, sh.dim, sh.n, sh.k, sh.sme_batch_rows),
                #[cfg(not(target_arch = "aarch64"))]
                Method::Sme => search_scalar(q, &sh.db, sh.dim, sh.n, sh.k),
                #[cfg(target_os = "macos")]
                Method::Amx => {
                    let mut buf = vec![0.0f32; sh.qpt * sh.n];
                    search_amx(q, &sh.db, sh.dim, sh.n, sh.k, &sh.xnorm, &mut buf)
                }
                #[cfg(not(target_os = "macos"))]
                Method::Amx => search_scalar(q, &sh.db, sh.dim, sh.n, sh.k),
            }
        }));
    }
    let mut checksum = 0.0f64;
    for h in handles {
        checksum += h.join().unwrap();
    }
    (start.elapsed().as_secs_f64(), checksum)
}

fn env_usize(key: &str, default: usize) -> usize {
    std::env::var(key)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

fn env_usizes(key: &str, default: &[usize]) -> Vec<usize> {
    std::env::var(key)
        .ok()
        .map(|v| {
            v.split(',')
                .filter_map(|s| s.trim().parse::<usize>().ok())
                .filter(|&v| v > 0)
                .collect::<Vec<_>>()
        })
        .filter(|v| !v.is_empty())
        .unwrap_or_else(|| default.to_vec())
}

fn median(mut v: Vec<f64>) -> f64 {
    v.sort_by(|a, b| a.partial_cmp(b).unwrap());
    v[v.len() / 2]
}

fn main() {
    let dim = env_usize("VS_DIM", 768);
    let n = env_usize("VS_DB", 50_000);
    let qpt = env_usize("VS_QPT", 16);
    let k = env_usize("VS_K", 10);
    let iters = env_usize("VS_ITERS", 5);
    let sme_batch_rows = env_usize("VS_SME_BATCH_ROWS", 512).clamp(16, 4096);
    let concurrency = env_usizes("VS_THREADS", &[1usize, 2, 4, 8, 16]);
    let max_threads = *concurrency.iter().max().unwrap();

    println!("=== vector_search_bench ===");
    println!(
        "machine={}  arch={}  profile={}",
        machine_brand(),
        std::env::consts::ARCH,
        if cfg!(debug_assertions) {
            "debug(!)"
        } else {
            "release"
        },
    );
    println!("config: DIM={dim} DB={n} queries/thread={qpt} k={k} iters={iters} (median)");
    println!(
        "SME: available={} batch_rows={} (includes production row-pack copy)",
        mydb::engine::vec_bench_api::sme_available(),
        sme_batch_rows
    );
    println!(
        "each thread scans full DB; per-thread work = {qpt} queries × {n} vecs × {dim} dims = {:.1}M FMA",
        (qpt * n * dim) as f64 / 1e6
    );

    let db = gen(n, dim, 0xABCDEF);
    let xnorm: Vec<f32> = (0..n)
        .map(|j| db[j * dim..j * dim + dim].iter().map(|v| v * v).sum())
        .collect();
    let queries = gen(max_threads * qpt, dim, 0x123456);

    let sh = Arc::new(Shared {
        db,
        xnorm,
        queries,
        dim,
        n,
        k,
        qpt,
        sme_batch_rows,
    });

    // ---- Cross-check correctness: one thread, same query batch ----
    println!(
        "\n[Correctness] All paths should have identical top-k checksums for the same query batch:"
    );
    {
        let q = &sh.queries[0..qpt * dim];
        let s = search_scalar(q, &sh.db, dim, n, k);
        #[cfg(target_arch = "aarch64")]
        let ne = search_neon(q, &sh.db, dim, n, k);
        #[cfg(not(target_arch = "aarch64"))]
        let ne = s;
        #[cfg(target_arch = "aarch64")]
        let sme = if mydb::engine::vec_bench_api::sme_available() {
            search_sme(q, &sh.db, dim, n, k, sme_batch_rows)
        } else {
            s
        };
        #[cfg(not(target_arch = "aarch64"))]
        let sme = s;
        #[cfg(target_os = "macos")]
        let am = {
            let mut buf = vec![0.0f32; qpt * n];
            search_amx(q, &sh.db, dim, n, k, &sh.xnorm, &mut buf)
        };
        #[cfg(not(target_os = "macos"))]
        let am = s;
        println!("  scalar = {s:.6}");
        println!(
            "  neon   = {ne:.6}   (Δ vs scalar = {:.2e})",
            (ne - s).abs()
        );
        println!(
            "  sme    = {sme:.6}   (Δ vs scalar = {:.2e})",
            (sme - s).abs()
        );
        println!(
            "  amx    = {am:.6}   (Δ vs scalar = {:.2e})",
            (am - s).abs()
        );
        println!("  Note: AMX rearranges L2 as squared norms minus 2q*x; small differences from floating-point accumulation order are normal.");
    }

    // ---- Main matrix: method by concurrency ----
    let methods = [Method::Scalar, Method::Neon, Method::Sme, Method::Amx];
    // Collect QPS values for the comparison table.
    let mut qps_table: Vec<(Method, Vec<f64>)> = Vec::new();

    for &m in &methods {
        println!(
            "\n[{}] (median of {} repetitions per configuration)",
            m.name(),
            iters
        );
        println!(
            "{:>8} {:>12} {:>14} {:>14} {:>12} {:>12}",
            "threads", "wall_ms", "queries/s", "us/query", "scale×", "efficiency"
        );
        let mut base_qps = 0.0f64;
        let mut qps_row = Vec::new();
        for (ci, &t) in concurrency.iter().enumerate() {
            // Warm up once.
            let _ = run_config(&sh, m, t);
            let mut times = Vec::with_capacity(iters);
            let mut checksum = 0.0;
            for _ in 0..iters {
                let (secs, cs) = run_config(&sh, m, t);
                times.push(secs);
                checksum = cs;
            }
            std::hint::black_box(checksum);
            let secs = median(times);
            let total_q = (t * qpt) as f64;
            let qps = total_q / secs;
            let us_per_q = secs * 1e6 / total_q;
            if ci == 0 {
                base_qps = qps;
            }
            let scale = qps / base_qps;
            let eff = scale / t as f64 * 100.0;
            println!(
                "{:>8} {:>12.2} {:>14.0} {:>14.2} {:>11.2}× {:>10.1}%",
                t,
                secs * 1e3,
                qps,
                us_per_q,
                scale,
                eff
            );
            qps_row.push(qps);
        }
        qps_table.push((m, qps_row));
    }

    // ---- Method comparison: speedup over scalar at each concurrency ----
    println!("\n[Speedup] Queries/s relative to scalar:");
    print!("{:>16}", "method");
    for &t in &concurrency {
        print!(" {:>9}", format!("{t}T"));
    }
    println!();
    let scalar_row = qps_table[0].1.clone();
    for (m, row) in &qps_table {
        print!("{:>16}", m.name());
        for (i, q) in row.iter().enumerate() {
            print!(" {:>9.2}×", q / scalar_row[i]);
        }
        println!();
    }

    // ---- Optional single-thread dimension sweep ----
    if env_usize("VS_DIM_SWEEP", 0) == 1 {
        println!("\n[Dimension sweep] One thread, DB={n}, {qpt} queries per dimension:");
        println!(
            "{:>8} {:>14} {:>14} {:>14} {:>12} {:>12}",
            "dim", "scalar us/q", "neon us/q", "amx us/q", "neon×", "amx×"
        );
        for &d in &[128usize, 384, 768, 1536] {
            let dbx = gen(n, d, 0xABCDEF);
            let xn: Vec<f32> = (0..n)
                .map(|j| dbx[j * d..j * d + d].iter().map(|v| v * v).sum())
                .collect();
            let qx = gen(qpt, d, 0x123456);
            let us = |secs: f64| secs * 1e6 / qpt as f64;
            let t_scalar = time_it(iters, || {
                std::hint::black_box(search_scalar(&qx, &dbx, d, n, k));
            });
            #[cfg(target_arch = "aarch64")]
            let t_neon = time_it(iters, || {
                std::hint::black_box(search_neon(&qx, &dbx, d, n, k));
            });
            #[cfg(not(target_arch = "aarch64"))]
            let t_neon = t_scalar;
            #[cfg(target_os = "macos")]
            let t_amx = {
                let mut buf = vec![0.0f32; qpt * n];
                time_it(iters, || {
                    std::hint::black_box(search_amx(&qx, &dbx, d, n, k, &xn, &mut buf));
                })
            };
            #[cfg(not(target_os = "macos"))]
            let t_amx = {
                let _ = &xn;
                t_scalar
            };
            println!(
                "{:>8} {:>14.2} {:>14.2} {:>14.2} {:>11.2}× {:>11.2}×",
                d,
                us(t_scalar),
                us(t_neon),
                us(t_amx),
                t_scalar / t_neon,
                t_scalar / t_amx
            );
        }
    }

    println!("\nDone.");
}

fn time_it(iters: usize, mut f: impl FnMut()) -> f64 {
    f(); // warmup
    let mut times = Vec::with_capacity(iters);
    for _ in 0..iters {
        let s = Instant::now();
        f();
        times.push(s.elapsed().as_secs_f64());
    }
    median(times)
}

#[cfg(target_os = "macos")]
fn machine_brand() -> String {
    use std::process::Command;
    Command::new("sysctl")
        .args(["-n", "machdep.cpu.brand_string"])
        .output()
        .ok()
        .and_then(|o| String::from_utf8(o.stdout).ok())
        .map(|s| s.trim().to_string())
        .unwrap_or_else(|| "unknown".into())
}
#[cfg(not(target_os = "macos"))]
fn machine_brand() -> String {
    "unknown".into()
}
