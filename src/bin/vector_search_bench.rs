//! Multidimensional KNN/L2 benchmark with scalar, SIMD, and matrix paths.
//!
//!   1. `scalar`: pure Rust scalar element-wise fma, an auto-vectorization baseline.
//!   2. `neon`: handwritten AArch64 NEON SIMD with four independent accumulators.
//!   3. `amx`: Apple Accelerate `cblas_sgemm`. On Apple Silicon,
//!      Accelerate BLAS dispatches to the AMX matrix coprocessor, representing
//!      the AMX path. Convert L2 via ||q-x||^2 = ||q||^2 + ||x||^2 - 2*q*x
//!      to compute all dot products with one GEMM.
//!   4. `sme(streaming)`: production single-query L2 kernel, including row copying.
//!   5. `sme(matrix)`: batch queries with f32 outer products. Database packing is
//!      timed and shared across queries; potential top-k candidates are rescored
//!      with direct NEON L2 to avoid cancellation and approximate rankings.
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
//!   VS_METHODS: optional comma-separated scalar,neon,sme,sme-streaming,amx filter.

use std::sync::Arc;
use std::time::Instant;

#[cfg(all(target_arch = "aarch64", target_os = "macos"))]
#[path = "vector_search_bench/sme.rs"]
mod sme;

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

    fn cutoff(&self) -> f32 {
        if self.buf.len() < self.k {
            f32::INFINITY
        } else {
            self.buf[self.k - 1].0
        }
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

/// Return one bit per candidate whose conservative bound can reach the cutoff.
///
/// # Safety
/// Both pointers must address at least four initialized f32 values.
#[cfg(all(target_arch = "aarch64", target_os = "macos"))]
#[inline]
unsafe fn candidate_mask4(
    dots: *const f32,
    norms: *const f32,
    query_norm: f64,
    factor: f64,
    allowance: f64,
    cutoff: f32,
) -> u32 {
    use std::arch::aarch64::*;
    if !query_norm.is_finite() || !factor.is_finite() || cutoff.is_nan() {
        return 0b1111;
    }
    let dot = vld1q_f32(dots);
    let norm = vld1q_f32(norms);
    let qn = vdupq_n_f64(query_norm);
    let factor = vdupq_n_f64(factor);
    let allowance = vdupq_n_f64(allowance);
    let cutoff = vdupq_n_f64(f64::from(cutoff));
    let two = vdupq_n_f64(2.0);
    // Preserve the scalar expression's operation order and f64 precision.
    let low_norm = vaddq_f64(qn, vcvt_f64_f32(vget_low_f32(norm)));
    let high_norm = vaddq_f64(qn, vcvt_f64_f32(vget_high_f32(norm)));
    let low_distance = vsubq_f64(low_norm, vmulq_f64(two, vcvt_f64_f32(vget_low_f32(dot))));
    let high_distance = vsubq_f64(high_norm, vmulq_f64(two, vcvt_f64_f32(vget_high_f32(dot))));
    let low_error = vaddq_f64(vmulq_f64(factor, low_norm), allowance);
    let high_error = vaddq_f64(vmulq_f64(factor, high_norm), allowance);
    let low_mask = vcleq_f64(vsubq_f64(low_distance, low_error), cutoff);
    let high_mask = vcleq_f64(vsubq_f64(high_distance, high_error), cutoff);
    let needs_score = vcombine_u32(vmovn_u64(low_mask), vmovn_u64(high_mask));
    // Infinite or NaN norms/dots must be rescored, even if comparison with the
    // resulting NaN bound is false. Finite f32 inputs cannot overflow f64 here.
    let exponent = vdupq_n_u32(0x7f80_0000);
    let nonfinite_dot = vceqq_u32(vandq_u32(vreinterpretq_u32_f32(dot), exponent), exponent);
    let nonfinite_norm = vceqq_u32(vandq_u32(vreinterpretq_u32_f32(norm), exponent), exponent);
    let mask = vorrq_u32(needs_score, vorrq_u32(nonfinite_dot, nonfinite_norm));
    let weights = [1u32, 2, 4, 8];
    vaddvq_u32(vandq_u32(mask, vld1q_u32(weights.as_ptr())))
}

/// Matrix dots only prune candidates. Direct L2 decides every retained distance
/// and rank, including close vectors for which the norm identity cancels.
#[cfg(all(target_arch = "aarch64", target_os = "macos"))]
fn search_sme_matrix_topk(
    queries: &[f32],
    db: &[f32],
    dim: usize,
    n: usize,
    k: usize,
    batch_rows: usize,
    xnorm: &[f32],
) -> Vec<TopK> {
    let width = sme::lanes();
    let nq = queries.len() / dim;
    let query_tiles = nq.div_ceil(width);
    let tile_floats = dim * width;
    let capacity_tiles = batch_rows.div_ceil(width);
    let capacity_rows = capacity_tiles * width;
    let mut packed_queries = vec![0.0; query_tiles * tile_floats];
    for (source, panel) in queries
        .chunks(dim * width)
        .zip(packed_queries.chunks_exact_mut(tile_floats))
    {
        sme::pack_tile(source, dim, width, panel);
    }
    let qnorm: Vec<f32> = queries
        .chunks_exact(dim)
        .map(|q| q.iter().map(|v| v * v).sum())
        .collect();
    let mut dots = vec![0.0; capacity_rows * width];
    let mut topk: Vec<_> = (0..nq).map(|_| TopK::new(k)).collect();

    // gamma bounds accumulation error in the norm/dot identity and the direct
    // SIMD reference. A deliberately conservative factor also covers the
    // subtraction, FMA reduction, and rounded stored norms. Ill-conditioned,
    // nonfinite, or excessively long inputs fall back to direct scoring.
    let neps = (dim as f64 + 16.0) * f32::EPSILON as f64;
    let error_factor = if neps < 0.5 {
        16.0 * neps / (1.0 - neps)
    } else {
        f64::INFINITY
    };
    let underflow_allowance = 16.0 * dim as f64 * f32::MIN_POSITIVE as f64;
    for base in (0..n).step_by(batch_rows) {
        let rows = batch_rows.min(n - base);
        let tiles = rows.div_ceil(width);
        for (qt, panel) in packed_queries.chunks_exact(tile_floats).enumerate() {
            sme::dot_rows(
                panel,
                &db[base * dim..(base + rows) * dim],
                dim,
                width,
                &mut dots[..tiles * width * width],
            );
            let query_start = qt * width;
            for (ct, tile) in dots[..tiles * width * width]
                .chunks_exact(width * width)
                .enumerate()
            {
                let first = base + ct * width;
                let valid_rows = width.min(base + rows - first);
                for qi in query_start..(query_start + width).min(nq) {
                    let q = &queries[qi * dim..(qi + 1) * dim];
                    let tk = &mut topk[qi];
                    let qn = f64::from(qnorm[qi]);
                    let dot_start = (qi - query_start) * width;
                    let row_dots = &tile[dot_start..dot_start + valid_rows];
                    let vector_rows = valid_rows / 4 * 4;
                    for j in (0..vector_rows).step_by(4) {
                        // SAFETY: this group has four initialized dots and norms.
                        let mut mask = unsafe {
                            candidate_mask4(
                                row_dots.as_ptr().add(j),
                                xnorm.as_ptr().add(first + j),
                                qn,
                                error_factor,
                                underflow_allowance,
                                tk.cutoff(),
                            )
                        };
                        // A cutoff snapshot can admit extra candidates after an
                        // earlier lane improves top-k, but cannot exclude one.
                        // Visit set bits in row order to preserve tie ordering.
                        while mask != 0 {
                            let lane = mask.trailing_zeros() as usize;
                            mask &= mask - 1;
                            let idx = first + j + lane;
                            let row = &db[idx * dim..(idx + 1) * dim];
                            // SAFETY: NEON is mandatory on AArch64.
                            tk.push(unsafe { l2_neon(q, row) }, idx as u32);
                            if tk.cutoff().is_nan() {
                                // A NaN invalidates the cutoff used for this
                                // mask. Visit every remaining lane in order.
                                mask |= (0b1111u32 << (lane + 1)) & 0b1111;
                            }
                        }
                    }
                    for (tail, &dot) in row_dots[vector_rows..].iter().enumerate() {
                        let j = vector_rows + tail;
                        let idx = first + j;
                        let norms = qn + f64::from(xnorm[idx]);
                        let approximate = norms - 2.0 * f64::from(dot);
                        let error = error_factor * norms + underflow_allowance;
                        if !approximate.is_finite()
                            || !error.is_finite()
                            || tk.cutoff().is_nan()
                            || approximate - error <= f64::from(tk.cutoff())
                        {
                            let row = &db[idx * dim..(idx + 1) * dim];
                            // SAFETY: NEON is mandatory on AArch64; both slices have dim elements.
                            tk.push(unsafe { l2_neon(q, row) }, idx as u32);
                        }
                    }
                }
            }
        }
    }
    topk
}

#[cfg(all(target_arch = "aarch64", target_os = "macos"))]
fn search_sme_matrix(sh: &Shared, queries: &[f32]) -> f64 {
    search_sme_matrix_topk(
        queries,
        &sh.db,
        sh.dim,
        sh.n,
        sh.k,
        sh.sme_batch_rows,
        &sh.xnorm,
    )
    .iter()
    .map(TopK::checksum)
    .sum()
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

#[derive(Clone, Copy, PartialEq, Eq)]
enum Method {
    Scalar,
    Neon,
    Sme,
    SmeMatrix,
    Amx,
}
impl Method {
    fn name(&self) -> &'static str {
        match self {
            Method::Scalar => "scalar",
            Method::Neon => "neon(simd)",
            Method::Sme => "sme(streaming)",
            Method::SmeMatrix => "sme(matrix)",
            Method::Amx => "amx(accelerate)",
        }
    }

    fn key(self) -> &'static str {
        match self {
            Self::Scalar => "scalar",
            Self::Neon => "neon",
            Self::Sme => "sme-streaming",
            Self::SmeMatrix => "sme",
            Self::Amx => "amx",
        }
    }

    fn available(self) -> bool {
        match self {
            Self::Scalar => true,
            Self::Neon => cfg!(target_arch = "aarch64"),
            Self::Sme => mydb::engine::vec_bench_api::sme_available(),
            Self::SmeMatrix => {
                #[cfg(all(target_arch = "aarch64", target_os = "macos"))]
                {
                    sme::available()
                }
                #[cfg(not(all(target_arch = "aarch64", target_os = "macos")))]
                {
                    false
                }
            }
            Self::Amx => cfg!(target_os = "macos"),
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
                #[cfg(all(target_arch = "aarch64", target_os = "macos"))]
                Method::SmeMatrix => search_sme_matrix(&sh, q),
                #[cfg(not(all(target_arch = "aarch64", target_os = "macos")))]
                Method::SmeMatrix => unreachable!("SME matrix path is unavailable"),
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
    assert!(dim > 0 && n > 0 && qpt > 0 && k > 0 && k <= n && iters > 0);
    assert!(n <= u32::MAX as usize, "vector IDs must fit in u32");
    let mut methods = vec![
        Method::Scalar,
        Method::Neon,
        Method::Sme,
        Method::SmeMatrix,
        Method::Amx,
    ];
    if let Ok(filter) = std::env::var("VS_METHODS") {
        let requested: Vec<_> = filter.split(',').map(str::trim).collect();
        for name in &requested {
            assert!(
                methods.iter().any(|m| m.key() == *name),
                "unknown VS_METHODS entry: {name}"
            );
        }
        methods.retain(|m| requested.contains(&m.key()));
        for method in &methods {
            assert!(method.available(), "{} is unavailable", method.name());
        }
    } else {
        methods.retain(|m| m.available());
    }
    assert!(!methods.is_empty(), "no benchmark methods selected");

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
    #[cfg(all(target_arch = "aarch64", target_os = "macos"))]
    println!(
        "SME matrix: available={} (timed panel packing, batch query reuse, direct L2 refinement)",
        sme::available()
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

    #[cfg(all(target_arch = "aarch64", target_os = "macos"))]
    if methods.contains(&Method::SmeMatrix) {
        let actual =
            search_sme_matrix_topk(&sh.queries, &sh.db, dim, n, k, sme_batch_rows, &sh.xnorm);
        for (qi, got) in actual.iter().enumerate() {
            let q = &sh.queries[qi * dim..(qi + 1) * dim];
            let mut reference = TopK::new(k);
            for (j, row) in sh.db.chunks_exact(dim).enumerate() {
                // SAFETY: NEON is mandatory on AArch64.
                reference.push(unsafe { l2_neon(q, row) }, j as u32);
            }
            assert_eq!(
                got.buf, reference.buf,
                "SME matrix top-k mismatch, query {qi}"
            );
        }
        println!(
            "  sme(matrix): all {} queries match direct NEON top-k IDs, order, and distance bits",
            actual.len()
        );
    }

    // ---- Main matrix: method by concurrency ----
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
    if let Some((_, scalar_row)) = qps_table.iter().find(|(m, _)| *m == Method::Scalar) {
        println!("\n[Speedup] Queries/s relative to scalar:");
        print!("{:>16}", "method");
        for &t in &concurrency {
            print!(" {:>9}", format!("{t}T"));
        }
        println!();
        for (m, row) in &qps_table {
            print!("{:>16}", m.name());
            for (i, q) in row.iter().enumerate() {
                print!(" {:>9.2}×", q / scalar_row[i]);
            }
            println!();
        }
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

#[cfg(all(test, target_arch = "aarch64", target_os = "macos"))]
mod tests {
    use super::*;

    fn norms(db: &[f32], dim: usize) -> Vec<f32> {
        db.chunks_exact(dim)
            .map(|row| row.iter().map(|x| x * x).sum())
            .collect()
    }

    fn assert_topk(queries: &[f32], db: &[f32], dim: usize, k: usize, batch: usize) {
        if !sme::available() {
            return;
        }
        let n = db.len() / dim;
        let actual = search_sme_matrix_topk(queries, db, dim, n, k, batch, &norms(db, dim));
        for (qi, got) in actual.iter().enumerate() {
            let q = &queries[qi * dim..(qi + 1) * dim];
            let mut expected = TopK::new(k);
            for (j, row) in db.chunks_exact(dim).enumerate() {
                // SAFETY: this module is only compiled on AArch64.
                expected.push(unsafe { l2_neon(q, row) }, j as u32);
            }
            let actual: Vec<_> = got.buf.iter().map(|(d, id)| (d.to_bits(), *id)).collect();
            let expected: Vec<_> = expected
                .buf
                .iter()
                .map(|(d, id)| (d.to_bits(), *id))
                .collect();
            assert_eq!(
                actual, expected,
                "dim={dim} n={n} k={k} batch={batch} query={qi}"
            );
        }
    }

    #[test]
    fn panel_transpose_covers_widths_and_partial_tiles() {
        for width in [4, 8, 16, 32, 64] {
            for dim in [1, 3, 4, 5, 15, 16, 17, 127] {
                let mut panel = vec![f32::NAN; dim * width];
                for count in 0..=width {
                    let rows = gen(count, dim, count as u64);
                    sme::pack_tile(&rows, dim, width, &mut panel);
                    for k in 0..dim {
                        for row in 0..width {
                            let expected = if row < count {
                                rows[row * dim + k]
                            } else {
                                0.0
                            };
                            assert_eq!(panel[k * width + row], expected);
                        }
                    }
                }
            }
        }
    }

    #[test]
    fn matrix_dots_cover_dimension_and_candidate_tails() {
        if !sme::available() {
            return;
        }
        let width = sme::lanes();
        for dim in [
            1, 2, 3, 4, 5, 7, 8, 9, 15, 16, 17, 18, 19, 20, 31, 32, 33, 63, 64, 65, 127, 128, 129,
            768, 769, 1536,
        ] {
            for nq in [1, width - 1, width] {
                for n in [
                    1,
                    2,
                    3,
                    4,
                    5,
                    width - 1,
                    width,
                    width + 1,
                    width + 2,
                    width + 3,
                    2 * width,
                    2 * width + 1,
                ] {
                    let queries = gen(nq, dim, 1);
                    let db = gen(n, dim, 2);
                    let mut a = vec![0.0; dim * width];
                    sme::pack_tile(&queries, dim, width, &mut a);
                    let padded_rows = n.div_ceil(width) * width;
                    let mut dots = vec![f32::NAN; padded_rows * width];
                    sme::dot_rows(&a, &db, dim, width, &mut dots);
                    for qi in 0..width {
                        for j in 0..padded_rows {
                            let actual =
                                f64::from(dots[(j / width * width + qi) * width + j % width]);
                            if qi >= nq || j >= n {
                                assert_eq!(actual, 0.0);
                                continue;
                            }
                            let q = &queries[qi * dim..(qi + 1) * dim];
                            let row = &db[j * dim..(j + 1) * dim];
                            let expected: f64 = q
                                .iter()
                                .zip(row)
                                .map(|(&a, &b)| f64::from(a) * f64::from(b))
                                .sum();
                            let absolute: f64 = q
                                .iter()
                                .zip(row)
                                .map(|(&a, &b)| (f64::from(a) * f64::from(b)).abs())
                                .sum();
                            let tolerance = 2.0 * dim as f64 * f32::EPSILON as f64 * absolute;
                            assert!(
                                (actual - expected).abs() <= tolerance.max(1e-7),
                                "dim={dim} query={qi} row={j}: {actual} != {expected}"
                            );
                        }
                    }
                }
            }
        }
    }

    #[test]
    fn simd_candidate_mask_matches_scalar_bounds() {
        let values = [
            0.0f32,
            -0.0,
            f32::MIN_POSITIVE,
            1e-30,
            -1e-30,
            1.0,
            -1.0,
            1e18,
            -1e18,
            f32::MAX,
            -f32::MAX,
            f32::INFINITY,
            f32::NEG_INFINITY,
            f32::NAN,
        ];
        for shift in 0..values.len() {
            let dots = std::array::from_fn::<_, 4, _>(|i| values[(shift + i) % values.len()]);
            let norms =
                std::array::from_fn::<_, 4, _>(|i| values[(shift + i * 3) % values.len()].abs());
            for qn in [0.0, 256.0, f32::MAX as f64, f64::INFINITY, f64::NAN] {
                for factor in [0.0, 0.0015, 0.5, 15.0, f64::INFINITY] {
                    for cutoff in [
                        -1.250001f32,
                        -1.25,
                        -1.249999,
                        0.0,
                        440.0,
                        f32::INFINITY,
                        f32::NAN,
                    ] {
                        let mut expected = 0u32;
                        for lane in 0..4 {
                            let sum = qn + f64::from(norms[lane]);
                            let distance = sum - 2.0 * f64::from(dots[lane]);
                            let error = factor * sum + 1e-30;
                            if cutoff.is_nan()
                                || !distance.is_finite()
                                || !error.is_finite()
                                || distance - error <= f64::from(cutoff)
                            {
                                expected |= 1 << lane;
                            }
                        }
                        let actual = unsafe {
                            candidate_mask4(
                                dots.as_ptr(),
                                norms.as_ptr(),
                                qn,
                                factor,
                                1e-30,
                                cutoff,
                            )
                        };
                        assert_eq!(
                            actual, expected,
                            "shift={shift} qn={qn} factor={factor} cutoff={cutoff}"
                        );
                    }
                }
            }
        }
    }

    #[test]
    fn matrix_topk_preserves_reference_order_with_nonfinite_rows() {
        let dim = 17;
        let mut db: Vec<f32> = (0..37).flat_map(|j| vec![j as f32 * 1000.0; dim]).collect();
        db[4 * dim] = f32::NAN;
        for batch in [1, 3, 4, 5, 16, 17, 31] {
            assert_topk(&vec![0.0; dim], &db, dim, 1, batch);
        }
        db[4 * dim] = f32::INFINITY;
        db[9 * dim] = f32::NEG_INFINITY;
        db[12 * dim] = f32::NAN;
        let mut queries = vec![0.0; 3 * dim];
        queries[dim] = f32::INFINITY;
        queries[2 * dim] = f32::NAN;
        for k in [1, 10, 37] {
            for batch in [1, 3, 4, 5, 16, 17, 31] {
                assert_topk(&queries, &db, dim, k, batch);
            }
        }
    }

    #[test]
    fn matrix_topk_matches_direct_l2_across_shapes() {
        if !sme::available() {
            return;
        }
        let width = sme::lanes();
        for dim in [1, 3, 17, 64, 127, 768, 769] {
            for (n, nq, batch) in [
                (1, 1, 1),
                (7, 3, 3),
                (19, width - 1, 17),
                (37, width, 16),
                (65, width + 3, 31),
                (129, width * 2 + 1, 128),
            ] {
                let queries = gen(nq, dim, dim as u64);
                let db = gen(n, dim, n as u64);
                for k in [1, n.min(10), n] {
                    assert_topk(&queries, &db, dim, k, batch);
                }
            }
        }
    }

    #[test]
    fn matrix_topk_refines_cancellation_overflow_underflow_and_ties() {
        for dim in [1, 17, 128, 769] {
            for center in [0.0f32, 8192.0, 1e18, 1e-30, f32::MAX / 8.0] {
                let n = 37;
                let mut db = vec![center; n * dim];
                for (j, row) in db.chunks_exact_mut(dim).enumerate() {
                    row[j % dim] = f32::from_bits(center.to_bits() + (j % 7) as u32);
                }
                let mut queries = db[dim * 3..dim * 22].to_vec();
                queries.extend_from_slice(&db[..dim]);
                for k in [1, 10, n] {
                    assert_topk(&queries, &db, dim, k, 17);
                }
            }
        }
    }
}
