//! GPU dots are conservative pruning hints; NEON decides final FP32 ranks.
//! A compact half filter includes conversion/flush error in every pruning bound.
//! Shared Metal output stays alive through refinement without a CPU result copy.

use super::{candidate_mask4, l2_neon, TopK};
use std::ffi::{c_char, c_void, CStr};
use std::ptr::NonNull;
use std::sync::OnceLock;
use std::time::Instant;

unsafe extern "C" {
    fn mydb_vector_gpu_available() -> i32;
    fn mydb_vector_gpu_create(
        db: *const f32,
        rows: usize,
        dim: usize,
        source: *const c_char,
        use_mps: u32,
        name: *mut c_char,
        name_capacity: usize,
        bytes: *mut usize,
        filter_bytes: *mut usize,
        error: *mut c_char,
        error_capacity: usize,
    ) -> *mut c_void;
    fn mydb_vector_gpu_release(object: *mut c_void);
    fn mydb_vector_gpu_dots(
        context: *mut c_void,
        queries: *const f32,
        count: usize,
        dots: *mut *const f32,
        row_stride: *mut usize,
        stats: *mut f64,
        error: *mut c_char,
        error_capacity: usize,
    ) -> *mut c_void;
}

fn message(buffer: &[c_char]) -> String {
    // The bridge always NUL-terminates nonempty error/name buffers.
    unsafe { CStr::from_ptr(buffer.as_ptr()) }
        .to_string_lossy()
        .into_owned()
}

pub(super) fn available() -> bool {
    static AVAILABLE: OnceLock<bool> = OnceLock::new();
    *AVAILABLE.get_or_init(|| unsafe { mydb_vector_gpu_available() != 0 })
}

struct Dots {
    lease: NonNull<c_void>,
    data: NonNull<f32>,
    stride: usize,
    rows: usize,
    queries: usize,
    stats: [f64; 5],
}
impl Dots {
    fn row(&self, query: usize) -> &[f32] {
        assert!(query < self.queries);
        // The completed command initialized these rows. The ARC lease owns the
        // immutable buffer until this view and all other borrowed views expire.
        unsafe {
            std::slice::from_raw_parts(self.data.as_ptr().add(query * self.stride), self.rows)
        }
    }
}
impl Drop for Dots {
    fn drop(&mut self) {
        unsafe { mydb_vector_gpu_release(self.lease.as_ptr()) };
    }
}

fn error_bound(dim: usize, half_filter: bool) -> (f64, f64) {
    let neps = (dim as f64 + 16.0) * f32::EPSILON as f64;
    let base = if neps < 0.5 {
        16.0 * neps / (1.0 - neps)
    } else {
        f64::INFINITY
    };
    let underflow = 16.0 * dim as f64 * f32::MIN_POSITIVE as f64;
    if !half_filter {
        return (base, underflow);
    }
    // The encoder explicitly flushes half subnormals: |h(v)-v| <= u|v|+eta.
    // 2|h(q)h(x)-qx| <= A(q^2+x^2)+B per coordinate by 2ab <= a^2+b^2.
    // Encoded squared norms are <= 2(1+u)^2 original_norm + 2*dim*eta^2.
    // 2.01 exceeds 2(1+u)^2; include the additive norm term as well.
    let u = 1.0 / 2048.0;
    let eta = 1.0 / 16384.0;
    let a = 2.0 * u + u * u + eta * (1.0 + u);
    let b = 2.0 * eta * (1.0 + u + eta);
    (
        2.01 * base + a,
        2.01 * underflow + dim as f64 * (b + 4.0 * base * eta * eta),
    )
}

pub(super) struct Prepared {
    context: NonNull<c_void>,
    dim: usize,
    rows: usize,
    kernel: &'static str,
}
// The ARC context owns immutable database storage and a thread-safe Metal queue.
// Each call creates independent query/output matrices, kernel and command buffer.
unsafe impl Send for Prepared {}
unsafe impl Sync for Prepared {}

impl Drop for Prepared {
    fn drop(&mut self) {
        unsafe { mydb_vector_gpu_release(self.context.as_ptr()) };
    }
}

impl Prepared {
    pub(super) fn new(db: &[f32], dim: usize) -> Result<Self, String> {
        if dim == 0 || db.is_empty() || !db.len().is_multiple_of(dim) {
            return Err("GPU database requires complete, nonempty vectors".into());
        }
        let rows = db.len() / dim;
        if rows > u32::MAX as usize {
            return Err("GPU vector IDs must fit in u32".into());
        }
        let kernel = match std::env::var("VS_GPU_KERNEL").as_deref().unwrap_or("auto") {
            "auto" => "auto",
            "tiled" => "tiled",
            "mps" => "mps",
            "filter" => "filter",
            other => {
                return Err(format!(
                    "unknown VS_GPU_KERNEL: {other} (expected auto, tiled, mps or filter)"
                ))
            }
        };
        const SOURCE: &str = concat!(include_str!("gpu.metal"), "\0");
        let start = Instant::now();
        let mut error = [0 as c_char; 1024];
        let mut name = [0 as c_char; 256];
        let mut bytes = 0;
        let mut filter_bytes = 0;
        let context = unsafe {
            mydb_vector_gpu_create(
                db.as_ptr(),
                rows,
                dim,
                SOURCE.as_ptr().cast(),
                match kernel {
                    "mps" => 1,
                    "auto" => 2,
                    "filter" => 3,
                    _ => 0,
                },
                name.as_mut_ptr(),
                name.len(),
                &mut bytes,
                &mut filter_bytes,
                error.as_mut_ptr(),
                error.len(),
            )
        };
        let context = NonNull::new(context).ok_or_else(|| message(&error))?;
        println!(
            "[GPU setup] device={} kernel={} FP32 database={} bytes, {:.3} ms (excluded from query timing), FP16 filter={} bytes",
            message(&name),
            kernel,
            bytes,
            start.elapsed().as_secs_f64() * 1e3,
            filter_bytes
        );
        Ok(Self {
            context,
            dim,
            rows,
            kernel,
        })
    }

    fn dots(&self, queries: &[f32]) -> Result<Dots, String> {
        if queries.is_empty() || !queries.len().is_multiple_of(self.dim) {
            return Err("GPU queries require complete, nonempty vectors".into());
        }
        let count = queries.len() / self.dim;
        count
            .checked_mul(self.rows)
            .and_then(|n| n.checked_mul(4))
            .ok_or("GPU result size overflow")?;
        let mut error = [0 as c_char; 1024];
        let mut stats = [0.0; 5];
        let mut data = std::ptr::null();
        let mut stride = 0;
        let lease = unsafe {
            mydb_vector_gpu_dots(
                self.context.as_ptr(),
                queries.as_ptr(),
                count,
                &mut data,
                &mut stride,
                stats.as_mut_ptr(),
                error.as_mut_ptr(),
                error.len(),
            )
        };
        let lease = NonNull::new(lease).ok_or_else(|| message(&error))?;
        let valid = !data.is_null()
            && stride >= self.rows
            && count
                .checked_mul(stride)
                .and_then(|n| n.checked_mul(4))
                .is_some_and(|n| n <= isize::MAX as usize);
        if !valid {
            unsafe { mydb_vector_gpu_release(lease.as_ptr()) };
            return Err("invalid Metal result lease".into());
        }
        Ok(Dots {
            lease,
            data: NonNull::new(data.cast_mut()).unwrap(),
            stride,
            rows: self.rows,
            queries: count,
            stats,
        })
    }

    pub(super) fn topk(
        &self,
        queries: &[f32],
        db: &[f32],
        k: usize,
        xnorm: &[f32],
    ) -> Result<Vec<TopK>, String> {
        if db.len() != self.rows * self.dim || xnorm.len() != self.rows || k == 0 || k > self.rows {
            return Err("GPU database shape, norms or k mismatch".into());
        }
        let dots = self.dots(queries)?;
        static PROFILE: OnceLock<bool> = OnceLock::new();
        let profile =
            *PROFILE.get_or_init(|| std::env::var("VS_GPU_PROFILE").is_ok_and(|v| v == "1"));
        let refine_start = profile.then(Instant::now);
        let mut rescored = 0usize;
        let (factor, allowance) = error_bound(self.dim, dots.stats[4] != 0.0);
        let mut results = Vec::with_capacity(queries.len() / self.dim);
        for (qi, q) in queries.chunks_exact(self.dim).enumerate() {
            let row = dots.row(qi);
            let qnorm = q.iter().map(|v| v * v).sum::<f32>() as f64;
            let mut tk = TopK::new(k);
            let vector_rows = self.rows / 4 * 4;
            for base in (0..vector_rows).step_by(4) {
                let mut mask = unsafe {
                    candidate_mask4(
                        row.as_ptr().add(base),
                        xnorm.as_ptr().add(base),
                        qnorm,
                        factor,
                        allowance,
                        tk.cutoff(),
                    )
                };
                while mask != 0 {
                    let lane = mask.trailing_zeros() as usize;
                    mask &= mask - 1;
                    let id = base + lane;
                    rescored += 1;
                    tk.push(
                        unsafe { l2_neon(q, &db[id * self.dim..(id + 1) * self.dim]) },
                        id as u32,
                    );
                    if tk.cutoff().is_nan() {
                        mask |= (0b1111u32 << (lane + 1)) & 0b1111;
                    }
                }
            }
            for id in vector_rows..self.rows {
                let norms = qnorm + f64::from(xnorm[id]);
                let lower = norms - 2.0 * f64::from(row[id]) - (factor * norms + allowance);
                if !lower.is_finite() || tk.cutoff().is_nan() || lower <= f64::from(tk.cutoff()) {
                    rescored += 1;
                    tk.push(
                        unsafe { l2_neon(q, &db[id * self.dim..(id + 1) * self.dim]) },
                        id as u32,
                    );
                }
            }
            results.push(tk);
        }
        if let Some(start) = refine_start {
            eprintln!("[GPU profile] kernel={} queries={} native_alloc_ms={:.4} encode_ms={:.4} submit_wait_ms={:.4} gpu_ms={:.4} cpu_refine_ms={:.4} fp16_filter={} rescored={} result_copy_bytes=0",
                self.kernel, dots.queries, dots.stats[0]*1e3, dots.stats[1]*1e3,
                dots.stats[2]*1e3, dots.stats[3]*1e3, start.elapsed().as_secs_f64()*1e3, dots.stats[4] != 0.0, rescored);
        }
        Ok(results)
    }

    pub(super) fn search(&self, queries: &[f32], db: &[f32], k: usize, norms: &[f32]) -> f64 {
        self.topk(queries, db, k, norms)
            .expect("GPU search failed")
            .iter()
            .map(TopK::checksum)
            .sum()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::gen;
    use std::sync::Arc;

    fn norms(db: &[f32], dim: usize) -> Vec<f32> {
        db.chunks_exact(dim)
            .map(|v| v.iter().map(|x| x * x).sum())
            .collect()
    }
    fn bits(topk: &[TopK]) -> Vec<Vec<(u32, u32)>> {
        topk.iter()
            .map(|t| t.buf.iter().map(|(d, id)| (d.to_bits(), *id)).collect())
            .collect()
    }
    fn reference(queries: &[f32], db: &[f32], dim: usize, k: usize) -> Vec<TopK> {
        queries
            .chunks_exact(dim)
            .map(|q| {
                let mut t = TopK::new(k);
                for (id, row) in db.chunks_exact(dim).enumerate() {
                    t.push(unsafe { l2_neon(q, row) }, id as u32);
                }
                t
            })
            .collect()
    }
    fn check(db: &[f32], queries: &[f32], dim: usize, ks: &[usize]) {
        let p = Prepared::new(db, dim).unwrap();
        let norms = norms(db, dim);
        for &k in ks {
            let actual = p.topk(queries, db, k, &norms).unwrap();
            assert_eq!(
                bits(&actual),
                bits(&reference(queries, db, dim, k)),
                "dim={dim} k={k}"
            );
            assert_eq!(
                p.search(queries, db, k, &norms),
                actual.iter().map(TopK::checksum).sum::<f64>()
            );
        }
    }

    #[test]
    fn metal_dots_are_real_fp32_results_with_completed_commands() {
        if !available() {
            return;
        }
        for dim in [1, 3, 17, 65, 769] {
            let db = gen(19, dim, 123);
            let q = gen(5, dim, 456);
            let p = Prepared::new(&db, dim).unwrap();
            let dots = p.dots(&q).unwrap();
            assert!(dots.stats[3].is_finite() && dots.stats[3] >= 0.0);
            for (qi, query) in q.chunks_exact(dim).enumerate() {
                for (id, row) in db.chunks_exact(dim).enumerate() {
                    let exact: f64 = query
                        .iter()
                        .zip(row)
                        .map(|(a, b)| f64::from(*a) * f64::from(*b))
                        .sum();
                    let abs: f64 = query
                        .iter()
                        .zip(row)
                        .map(|(a, b)| (f64::from(*a) * f64::from(*b)).abs())
                        .sum();
                    let (factor, allowance) = error_bound(dim, dots.stats[4] != 0.0);
                    let norm: f64 = query.iter().chain(row).map(|v| f64::from(*v).powi(2)).sum();
                    let bound = if dots.stats[4] != 0.0 {
                        (factor * norm + allowance) / 2.0
                    } else {
                        8.0 * (dim + 16) as f64 * f32::EPSILON as f64 * abs
                    };
                    assert!((f64::from(dots.row(qi)[id]) - exact).abs() <= bound);
                }
            }
        }
    }

    #[test]
    fn metal_topk_matches_neon_across_tails_and_duplicate_rows() {
        if !available() {
            return;
        }
        for (dim, n, nq) in [
            (1, 1, 1),
            (3, 7, 3),
            (15, 19, 5),
            (16, 32, 16),
            (17, 33, 17),
            (65, 127, 19),
            (7, 31, 8),
            (9, 32, 9),
            (33, 33, 33),
            (128, 129, 4),
            (768, 257, 16),
            (769, 511, 19),
            (1536, 67, 3),
        ] {
            let mut db = gen(n, dim, 17);
            let queries = gen(nq, dim, 19);
            for row in db.chunks_exact_mut(dim).take(2) {
                row.copy_from_slice(&queries[..dim]);
            }
            check(&db, &queries, dim, &[1, 10.min(n), n]);
        }
    }

    #[test]
    fn metal_refinement_handles_cancellation_overflow_and_underflow() {
        if !available() {
            return;
        }
        for dim in [3, 17, 129, 769] {
            for (center, scale) in [(8192.0, 0.01), (1e15, 1e9), (0.0, 1e-20), (0.0, 1e30)] {
                let mut db = gen(23, dim, 23);
                let mut queries = gen(3, dim, 29);
                for value in db.iter_mut().chain(queries.iter_mut()) {
                    *value = center + *value * scale;
                }
                db[2 * dim..3 * dim].copy_from_slice(&queries[..dim]);
                check(&db, &queries, dim, &[1, 10, 23]);
            }
        }
    }

    #[test]
    fn metal_refinement_preserves_nonfinite_reference_order() {
        if !available() {
            return;
        }
        let dim = 17;
        let mut db = gen(19, dim, 31);
        let mut q = gen(3, dim, 37);
        db[0] = f32::NAN;
        db[2 * dim] = f32::INFINITY;
        db[4 * dim] = f32::NEG_INFINITY;
        q[dim] = f32::INFINITY;
        q[2 * dim] = f32::NAN;
        let p = Prepared::new(&db, dim).unwrap();
        for k in [1, 10, 19] {
            assert_eq!(
                bits(&p.topk(&q, &db, k, &norms(&db, dim)).unwrap()),
                bits(&reference(&q, &db, dim, k))
            );
        }
    }

    #[test]
    fn filter_bound_covers_half_rounding_flush_and_large_common_offsets() {
        if !available() {
            return;
        }
        for dim in [1, 3, 17, 65, 769] {
            for (center, scale) in [
                (0.0, 1.0),
                (0.0, 6.0e-5),
                (0.0, 3.0e-8),
                (1.0, 0.0005),
                (1024.0, 0.5),
                (65500.0, 1.0),
            ] {
                let mut db = gen(37, dim, 79);
                let mut queries = gen(6, dim, 83);
                for v in db.iter_mut().chain(queries.iter_mut()) {
                    *v = center + *v * scale;
                }
                db[dim..2 * dim].copy_from_slice(&queries[..dim]);
                let p = Prepared::new(&db, dim).unwrap();
                let dots = p.dots(&queries).unwrap();
                if matches!(p.kernel, "auto" | "filter") {
                    assert_eq!(dots.stats[4], 1.0);
                }
                let (factor, allowance) = error_bound(dim, dots.stats[4] != 0.0);
                for (qi, q) in queries.chunks_exact(dim).enumerate() {
                    for (id, x) in db.chunks_exact(dim).enumerate() {
                        let dot: f64 = q
                            .iter()
                            .zip(x)
                            .map(|(a, b)| f64::from(*a) * f64::from(*b))
                            .sum();
                        let norm: f64 = q.iter().chain(x).map(|v| f64::from(*v).powi(2)).sum();
                        assert!(
                            (f64::from(dots.row(qi)[id]) - dot).abs()
                                <= (factor * norm + allowance) / 2.0
                        );
                    }
                }
                check(&db, &queries, dim, &[1, 10, 37]);
            }
        }
    }

    #[test]
    fn filter_falls_back_to_fp32_for_unrepresentable_inputs() {
        if !available() {
            return;
        }
        let dim = 17;
        let original = gen(19, dim, 89);
        let normal_queries = gen(3, dim, 97);
        for value in [65505.0, 1e20, f32::INFINITY, f32::NEG_INFINITY, f32::NAN] {
            let mut db = original.clone();
            db[dim] = value;
            let p = Prepared::new(&db, dim).unwrap();
            assert_eq!(p.dots(&normal_queries).unwrap().stats[4], 0.0);
            for k in [1, 10] {
                assert_eq!(
                    bits(&p.topk(&normal_queries, &db, k, &norms(&db, dim)).unwrap()),
                    bits(&reference(&normal_queries, &db, dim, k))
                );
            }
            let p = Prepared::new(&original, dim).unwrap();
            for count in [1, 3] {
                let mut queries = normal_queries[..count * dim].to_vec();
                queries[0] = value;
                assert_eq!(p.dots(&queries).unwrap().stats[4], 0.0);
                assert_eq!(
                    bits(
                        &p.topk(&queries, &original, 10, &norms(&original, dim))
                            .unwrap()
                    ),
                    bits(&reference(&queries, &original, dim, 10))
                );
            }
        }
    }

    #[test]
    fn filter_topk_preserves_distinctions_below_half_resolution() {
        if !available() {
            return;
        }
        for dim in [3, 17, 129] {
            let mut db = vec![1.0f32; 37 * dim];
            for (id, row) in db.chunks_exact_mut(dim).enumerate() {
                row[0] += id as f32 * 0.00001;
                row[dim - 1] = id as f32 * 0.000001;
            }
            let queries: Vec<_> = [3usize, 17, 36]
                .into_iter()
                .flat_map(|id| db[id * dim..(id + 1) * dim].iter().copied())
                .collect();
            check(&db, &queries, dim, &[1, 10, 37]);
        }
    }

    #[test]
    fn filter_selection_is_per_request_under_mixed_concurrency() {
        if !available() {
            return;
        }
        let dim = 65;
        let db = Arc::new(gen(127, dim, 101));
        let norms = Arc::new(norms(&db, dim));
        let prepared = Arc::new(Prepared::new(&db, dim).unwrap());
        let handles: Vec<_> = (0..4)
            .map(|worker| {
                let (db, norms, p) = (db.clone(), norms.clone(), prepared.clone());
                std::thread::spawn(move || {
                    let count = [1, 3, 9, 17][worker];
                    let mut queries = gen(count, dim, 103 + worker as u64);
                    if worker == 1 {
                        queries[0] = 1e20;
                    }
                    let dots = p.dots(&queries).unwrap();
                    if worker == 1 {
                        assert_eq!(dots.stats[4], 0.0);
                    } else if matches!(p.kernel, "auto" | "filter") && count <= 16 {
                        assert_eq!(dots.stats[4], 1.0);
                    }
                    assert_eq!(
                        bits(&p.topk(&queries, &db, 10, &norms).unwrap()),
                        bits(&reference(&queries, &db, dim, 10))
                    );
                })
            })
            .collect();
        for handle in handles {
            handle.join().unwrap();
        }
    }

    #[test]
    fn metal_result_leases_survive_other_queries_and_context_destruction() {
        if !available() {
            return;
        }
        let dim = 17;
        let db = gen(33, dim, 61);
        let q = gen(9, dim, 67);
        let p = Prepared::new(&db, dim).unwrap();
        let old = p.dots(&q).unwrap();
        let saved: Vec<Vec<u32>> = (0..9)
            .map(|i| old.row(i).iter().map(|v| v.to_bits()).collect())
            .collect();
        let next = p.dots(&gen(1, dim, 71)).unwrap();
        assert_eq!(next.row(0).len(), 33);
        drop(p);
        for (i, row) in saved.iter().enumerate() {
            assert_eq!(
                &old.row(i).iter().map(|v| v.to_bits()).collect::<Vec<_>>(),
                row
            );
        }
        assert!(next.row(0).iter().all(|v| v.is_finite()));
    }

    #[test]
    fn metal_shared_database_supports_concurrent_queries_and_rejects_bad_shapes() {
        assert!(Prepared::new(&[], 1).is_err());
        assert!(Prepared::new(&[1.0], 0).is_err());
        assert!(Prepared::new(&[1.0; 3], 2).is_err());
        if !available() {
            return;
        }
        let dim = 65;
        let db = Arc::new(gen(127, dim, 41));
        let q = Arc::new(gen(5, dim, 43));
        let norms = Arc::new(norms(&db, dim));
        let p = Arc::new(Prepared::new(&db, dim).unwrap());
        assert!(p.dots(&[]).is_err());
        assert!(p.dots(&q[..q.len() - 1]).is_err());
        assert!(p.topk(&q, &db, 0, &norms).is_err());
        assert!(p.topk(&q, &db, 128, &norms).is_err());
        assert!(p.topk(&q, &db[..db.len() - 1], 10, &norms).is_err());
        assert!(p.topk(&q, &db, 10, &norms[..126]).is_err());
        let expected = bits(&reference(&q, &db, dim, 10));
        let handles: Vec<_> = (0..4)
            .map(|_| {
                let (p, db, q, norms) = (p.clone(), db.clone(), q.clone(), norms.clone());
                std::thread::spawn(move || bits(&p.topk(&q, &db, 10, &norms).unwrap()))
            })
            .collect();
        for handle in handles {
            assert_eq!(handle.join().unwrap(), expected);
        }
    }
}
