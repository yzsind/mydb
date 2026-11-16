//! Direct f32 squared-L2 kernels with a shared 64-lane accumulation tree.
//!
//! AVX2 uses eight independent accumulators and AVX-512 uses four. Both retain
//! the same lane assignment, FMA order, and final reduction, so backend changes
//! do not change finite distance bits or top-k tie ordering.

use super::TopK;
use std::arch::x86_64::*;

pub(super) type Kernel = unsafe fn(*const f32, *const f32, usize) -> f32;

pub(super) fn avx2_available() -> bool {
    is_x86_feature_detected!("avx2") && is_x86_feature_detected!("fma")
}

pub(super) fn avx512_available() -> bool {
    avx2_available() && is_x86_feature_detected!("avx512f")
}

#[inline]
pub(super) unsafe fn sum4(v: __m128) -> f32 {
    let pairs = _mm_add_ps(v, _mm_shuffle_ps::<0xb1>(v, v));
    _mm_cvtss_f32(_mm_add_ss(pairs, _mm_movehl_ps(pairs, pairs)))
}

/// # Safety
/// AVX2/FMA must be available and both pointers must cover dim initialized f32s.
#[target_feature(enable = "avx2,fma")]
#[inline]
pub(super) unsafe fn l2_avx2(a: *const f32, b: *const f32, dim: usize) -> f32 {
    let mut s0 = _mm256_setzero_ps();
    let mut s1 = s0;
    let mut s2 = s0;
    let mut s3 = s0;
    let mut s4 = s0;
    let mut s5 = s0;
    let mut s6 = s0;
    let mut s7 = s0;
    let mut i = 0;
    macro_rules! accumulate {
        ($sum:ident, $offset:expr) => {{
            let d = _mm256_sub_ps(
                _mm256_loadu_ps(a.add(i + $offset)),
                _mm256_loadu_ps(b.add(i + $offset)),
            );
            $sum = _mm256_fmadd_ps(d, d, $sum);
        }};
    }
    while dim - i >= 64 {
        accumulate!(s0, 0);
        accumulate!(s1, 8);
        accumulate!(s2, 16);
        accumulate!(s3, 24);
        accumulate!(s4, 32);
        accumulate!(s5, 40);
        accumulate!(s6, 48);
        accumulate!(s7, 56);
        i += 64;
    }
    if i < dim {
        let lanes = _mm256_setr_epi32(0, 1, 2, 3, 4, 5, 6, 7);
        macro_rules! tail {
            ($sum:ident, $offset:expr) => {
                if dim - i > $offset {
                    let count = (dim - i - $offset).min(8) as i32;
                    let mask = _mm256_cmpgt_epi32(_mm256_set1_epi32(count), lanes);
                    let d = _mm256_sub_ps(
                        _mm256_maskload_ps(a.add(i + $offset), mask),
                        _mm256_maskload_ps(b.add(i + $offset), mask),
                    );
                    $sum = _mm256_fmadd_ps(d, d, $sum);
                }
            };
        }
        tail!(s0, 0);
        tail!(s1, 8);
        tail!(s2, 16);
        tail!(s3, 24);
        tail!(s4, 32);
        tail!(s5, 40);
        tail!(s6, 48);
        tail!(s7, 56);
    }
    let lo = _mm256_add_ps(_mm256_add_ps(s0, s4), _mm256_add_ps(s2, s6));
    let hi = _mm256_add_ps(_mm256_add_ps(s1, s5), _mm256_add_ps(s3, s7));
    let v = _mm256_add_ps(lo, hi);
    sum4(_mm_add_ps(
        _mm256_castps256_ps128(v),
        _mm256_extractf128_ps::<1>(v),
    ))
}

/// # Safety
/// AVX-512F/FMA must be available; pointers must cover dim initialized f32s.
#[target_feature(enable = "avx512f,fma")]
#[inline]
pub(super) unsafe fn l2_avx512(a: *const f32, b: *const f32, dim: usize) -> f32 {
    let mut s0 = _mm512_setzero_ps();
    let mut s1 = s0;
    let mut s2 = s0;
    let mut s3 = s0;
    let mut i = 0;
    macro_rules! accumulate {
        ($sum:ident, $offset:expr) => {{
            let d = _mm512_sub_ps(
                _mm512_loadu_ps(a.add(i + $offset)),
                _mm512_loadu_ps(b.add(i + $offset)),
            );
            $sum = _mm512_fmadd_ps(d, d, $sum);
        }};
    }
    while dim - i >= 64 {
        accumulate!(s0, 0);
        accumulate!(s1, 16);
        accumulate!(s2, 32);
        accumulate!(s3, 48);
        i += 64;
    }
    if i < dim {
        macro_rules! tail {
            ($sum:ident, $offset:expr) => {
                if dim - i > $offset {
                    let count = (dim - i - $offset).min(16);
                    let mask = ((1u32 << count) - 1) as __mmask16;
                    let d = _mm512_sub_ps(
                        _mm512_maskz_loadu_ps(mask, a.add(i + $offset)),
                        _mm512_maskz_loadu_ps(mask, b.add(i + $offset)),
                    );
                    $sum = _mm512_fmadd_ps(d, d, $sum);
                }
            };
        }
        tail!(s0, 0);
        tail!(s1, 16);
        tail!(s2, 32);
        tail!(s3, 48);
    }
    let v = _mm512_add_ps(_mm512_add_ps(s0, s2), _mm512_add_ps(s1, s3));
    let lo = _mm_add_ps(_mm512_castps512_ps128(v), _mm512_extractf32x4_ps::<2>(v));
    let hi = _mm_add_ps(
        _mm512_extractf32x4_ps::<1>(v),
        _mm512_extractf32x4_ps::<3>(v),
    );
    sum4(_mm_add_ps(lo, hi))
}

macro_rules! search_body {
    ($kernel:ident, $queries:ident, $db:ident, $dim:ident, $n:ident, $k:ident) => {{
        let mut checksum = 0.0;
        for query in $queries.chunks_exact($dim) {
            let mut topk = TopK::new($k);
            for j in 0..$n {
                topk.push(
                    $kernel(query.as_ptr(), $db.as_ptr().add(j * $dim), $dim),
                    j as u32,
                );
            }
            checksum += topk.checksum();
        }
        checksum
    }};
}

#[target_feature(enable = "avx2,fma")]
unsafe fn search_avx2_inner(queries: &[f32], db: &[f32], dim: usize, n: usize, k: usize) -> f64 {
    search_body!(l2_avx2, queries, db, dim, n, k)
}

#[target_feature(enable = "avx512f,fma")]
unsafe fn search_avx512_inner(queries: &[f32], db: &[f32], dim: usize, n: usize, k: usize) -> f64 {
    search_body!(l2_avx512, queries, db, dim, n, k)
}

pub(super) fn search_avx2(queries: &[f32], db: &[f32], dim: usize, n: usize, k: usize) -> f64 {
    assert!(avx2_available(), "AVX2/FMA is unavailable");
    assert!(dim > 0 && queries.len().is_multiple_of(dim) && db.len() / dim == n);
    // SAFETY: support and buffer shapes were checked once outside the hot loop.
    unsafe { search_avx2_inner(queries, db, dim, n, k) }
}

pub(super) fn search_avx512(queries: &[f32], db: &[f32], dim: usize, n: usize, k: usize) -> f64 {
    assert!(avx512_available(), "AVX-512F/FMA is unavailable");
    assert!(dim > 0 && queries.len().is_multiple_of(dim) && db.len() / dim == n);
    // SAFETY: support and buffer shapes were checked once outside the hot loop.
    unsafe { search_avx512_inner(queries, db, dim, n, k) }
}

/// Correctness-only collection, deliberately outside timed search calls.
///
/// # Safety
/// The kernel's CPU features must be available; inputs must contain complete
/// vectors of positive dimension.
pub(super) unsafe fn topk(
    queries: &[f32],
    db: &[f32],
    dim: usize,
    k: usize,
    kernel: Kernel,
) -> Vec<TopK> {
    queries
        .chunks_exact(dim)
        .map(|query| {
            let mut topk = TopK::new(k);
            for (j, row) in db.chunks_exact(dim).enumerate() {
                topk.push(kernel(query.as_ptr(), row.as_ptr(), dim), j as u32);
            }
            topk
        })
        .collect()
}

/// Four independent queries share each database load; arithmetic order within
/// each query remains identical to the single-query 64-lane reference.
#[target_feature(enable = "avx512f,fma")]
#[inline]
unsafe fn l2_queries4(q: [*const f32; 4], row: *const f32, dim: usize) -> [f32; 4] {
    let mut sums = [[_mm512_setzero_ps(); 4]; 4];
    let mut i = 0;
    macro_rules! step {
        ($offset:expr, $lane:expr) => {{
            let x = _mm512_loadu_ps(row.add(i + $offset));
            macro_rules! query {
                ($qi:expr) => {{
                    let d = _mm512_sub_ps(_mm512_loadu_ps(q[$qi].add(i + $offset)), x);
                    sums[$qi][$lane] = _mm512_fmadd_ps(d, d, sums[$qi][$lane]);
                }};
            }
            query!(0);
            query!(1);
            query!(2);
            query!(3);
        }};
    }
    while dim - i >= 64 {
        step!(0, 0);
        step!(16, 1);
        step!(32, 2);
        step!(48, 3);
        i += 64;
    }
    if i < dim {
        macro_rules! tail {
            ($offset:expr, $lane:expr) => {
                if dim - i > $offset {
                    let count = (dim - i - $offset).min(16);
                    let mask = ((1u32 << count) - 1) as u16;
                    let x = _mm512_maskz_loadu_ps(mask, row.add(i + $offset));
                    macro_rules! query {
                        ($qi:expr) => {{
                            let d = _mm512_sub_ps(
                                _mm512_maskz_loadu_ps(mask, q[$qi].add(i + $offset)),
                                x,
                            );
                            sums[$qi][$lane] = _mm512_fmadd_ps(d, d, sums[$qi][$lane]);
                        }};
                    }
                    query!(0);
                    query!(1);
                    query!(2);
                    query!(3);
                }
            };
        }
        tail!(0, 0);
        tail!(16, 1);
        tail!(32, 2);
        tail!(48, 3);
    }
    std::array::from_fn(|qi| {
        let s = sums[qi];
        let v = _mm512_add_ps(_mm512_add_ps(s[0], s[2]), _mm512_add_ps(s[1], s[3]));
        let lo = _mm_add_ps(_mm512_castps512_ps128(v), _mm512_extractf32x4_ps::<2>(v));
        let hi = _mm_add_ps(
            _mm512_extractf32x4_ps::<1>(v),
            _mm512_extractf32x4_ps::<3>(v),
        );
        sum4(_mm_add_ps(lo, hi))
    })
}

#[target_feature(enable = "avx512f,fma")]
unsafe fn batch_topk_inner(queries: &[f32], db: &[f32], dim: usize, k: usize) -> Vec<TopK> {
    let nq = queries.len() / dim;
    let mut result: Vec<_> = (0..nq).map(|_| TopK::new(k)).collect();
    let full = nq / 4 * 4;
    for qi in (0..full).step_by(4) {
        let q = std::array::from_fn(|j| queries.as_ptr().add((qi + j) * dim));
        for (id, row) in db.chunks_exact(dim).enumerate() {
            let distances = l2_queries4(q, row.as_ptr(), dim);
            for j in 0..4 {
                result[qi + j].push(distances[j], id as u32);
            }
        }
    }
    for qi in full..nq {
        for (id, row) in db.chunks_exact(dim).enumerate() {
            result[qi].push(
                l2_avx512(queries.as_ptr().add(qi * dim), row.as_ptr(), dim),
                id as u32,
            );
        }
    }
    result
}

pub(super) fn batch_topk(queries: &[f32], db: &[f32], dim: usize, k: usize) -> Vec<TopK> {
    assert!(avx512_available(), "AVX-512F/FMA is unavailable");
    assert!(dim > 0 && queries.len().is_multiple_of(dim) && db.len().is_multiple_of(dim));
    // SAFETY: complete vector slices and feature support are checked above.
    unsafe { batch_topk_inner(queries, db, dim, k) }
}

pub(super) fn search_batch(queries: &[f32], db: &[f32], dim: usize, k: usize) -> f64 {
    batch_topk(queries, db, dim, k)
        .iter()
        .map(TopK::checksum)
        .sum()
}

/// Portable oracle for the shared accumulation/reduction order.
pub(super) fn canonical_l2(a: &[f32], b: &[f32]) -> f32 {
    let mut lanes = [0.0f32; 64];
    for (i, (&a, &b)) in a.iter().zip(b).enumerate() {
        let d = a - b;
        lanes[i & 63] = d.mul_add(d, lanes[i & 63]);
    }
    for step in [32, 16, 8, 4] {
        for i in 0..step {
            lanes[i] += lanes[i + step];
        }
    }
    (lanes[0] + lanes[1]) + (lanes[2] + lanes[3])
}

fn check_distance(a: &[f32], b: &[f32], actual: f32) {
    let expected = canonical_l2(a, b);
    if expected.is_nan() {
        assert!(actual.is_nan(), "expected NaN, got {actual}");
        return;
    }
    assert_eq!(
        actual.to_bits(),
        expected.to_bits(),
        "canonical L2 mismatch at dim={}",
        a.len()
    );
    let exact: f64 = a
        .iter()
        .zip(b)
        .map(|(&a, &b)| {
            let d = f64::from(a) - f64::from(b);
            d * d
        })
        .sum();
    if exact.is_finite() && exact < f32::MAX as f64 * 0.5 {
        let error = 4.0 * (a.len().div_ceil(64) + 7) as f64 * f32::EPSILON as f64 * exact
            + a.len() as f64 * f32::MIN_POSITIVE as f64;
        assert!(
            (f64::from(actual) - exact).abs() <= error,
            "f64 oracle mismatch: dim={} actual={actual} expected={exact} bound={error}",
            a.len()
        );
    }
}

fn topk_bits(rows: &[TopK]) -> Vec<Vec<(u32, u32)>> {
    rows.iter()
        .map(|r| r.buf.iter().map(|(d, id)| (d.to_bits(), *id)).collect())
        .collect()
}

pub(super) fn validate(
    queries: &[f32],
    db: &[f32],
    dim: usize,
    k: usize,
    check_avx512: bool,
    check_batch: bool,
) -> Vec<TopK> {
    assert!(avx2_available());
    // SAFETY: feature support was checked and slices contain complete vectors.
    let reference = unsafe { topk(queries, db, dim, k, l2_avx2) };
    for (qi, row) in reference.iter().enumerate() {
        let q = &queries[qi * dim..(qi + 1) * dim];
        for &(distance, id) in &row.buf {
            check_distance(q, &db[id as usize * dim..(id as usize + 1) * dim], distance);
        }
    }
    println!(
        "  avx2: all {} queries' selected distances match canonical FMA bits and f64 error bounds",
        reference.len()
    );
    if check_avx512 {
        assert!(avx512_available());
        let actual = unsafe { topk(queries, db, dim, k, l2_avx512) };
        assert_eq!(
            topk_bits(&actual),
            topk_bits(&reference),
            "AVX-512 top-k differs from AVX2"
        );
        println!(
            "  avx512: all {} queries match AVX2 top-k IDs, order, and distance bits",
            actual.len()
        );
    }
    if check_batch {
        let actual = batch_topk(queries, db, dim, k);
        assert_eq!(
            topk_bits(&actual),
            topk_bits(&reference),
            "batched AVX-512 top-k differs from AVX2"
        );
        println!(
            "  avx512-batch: all {} queries match canonical FP32 IDs, order, and distance bits",
            actual.len()
        );
    }
    reference
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{gen, Method};

    fn kernels() -> Vec<Kernel> {
        let mut out: Vec<Kernel> = Vec::new();
        if avx2_available() {
            out.push(l2_avx2);
        }
        if avx512_available() {
            out.push(l2_avx512);
        }
        out
    }

    unsafe fn canonical_ptr(a: *const f32, b: *const f32, dim: usize) -> f32 {
        canonical_l2(
            std::slice::from_raw_parts(a, dim),
            std::slice::from_raw_parts(b, dim),
        )
    }

    #[test]
    fn feature_gates_match_cpu_and_os_detection() {
        assert_eq!(
            Method::Avx2.available(),
            is_x86_feature_detected!("avx2") && is_x86_feature_detected!("fma")
        );
        assert_eq!(
            Method::Avx512.available(),
            Method::Avx2.available() && is_x86_feature_detected!("avx512f")
        );
        assert!(!Method::Neon.available());
        assert!(!Method::Sme.available());
        assert!(!Method::SmeMatrix.available());
    }

    #[test]
    fn distances_cover_all_residues_and_unaligned_inputs() {
        for dim in (0..=257).chain([383, 384, 385, 767, 768, 769, 1535, 1536, 1537, 4096, 4097]) {
            for offset in [0, 1, 3] {
                let a = gen(1, dim + offset, 17);
                let b = gen(1, dim + offset, 23);
                for kernel in kernels() {
                    let actual =
                        unsafe { kernel(a.as_ptr().add(offset), b.as_ptr().add(offset), dim) };
                    check_distance(&a[offset..], &b[offset..], actual);
                }
            }
        }
    }

    #[test]
    fn distances_cover_special_values_and_cancellation() {
        for dim in [0, 1, 7, 8, 15, 16, 31, 32, 63, 64, 65, 129, 769] {
            let values = [
                gen(1, dim, 3),
                gen(1, dim, 5),
                vec![0.0; dim],
                vec![-0.0; dim],
                vec![f32::from_bits(1); dim],
                vec![f32::MIN_POSITIVE; dim],
                (0..dim)
                    .map(|i| f32::from_bits(8192.0f32.to_bits() + (i % 7) as u32))
                    .collect(),
                vec![f32::MAX; dim],
                vec![f32::INFINITY; dim],
                vec![f32::NAN; dim],
            ];
            for a in &values {
                for b in &values {
                    for kernel in kernels() {
                        check_distance(a, b, unsafe { kernel(a.as_ptr(), b.as_ptr(), dim) });
                    }
                }
            }
        }
    }

    #[test]
    fn topk_ids_order_and_bits_match_canonical_reference() {
        for dim in [
            1, 3, 7, 8, 9, 15, 16, 17, 31, 32, 33, 63, 64, 65, 127, 128, 129, 384, 768, 769, 1536,
        ] {
            for (n, nq) in [(1, 1), (7, 3), (19, 7), (37, 16), (65, 19), (129, 3)] {
                let mut db = gen(n, dim, n as u64);
                if n > 3 {
                    // Duplicate rows exercise deterministic cutoff ties.
                    let duplicate = db[..dim].to_vec();
                    db[3 * dim..4 * dim].copy_from_slice(&duplicate);
                }
                let queries = gen(nq, dim, dim as u64);
                for k in [1, n.min(10), n] {
                    let reference = unsafe { topk(&queries, &db, dim, k, canonical_ptr) };
                    for kernel in kernels() {
                        let actual = unsafe { topk(&queries, &db, dim, k, kernel) };
                        assert_eq!(
                            topk_bits(&actual),
                            topk_bits(&reference),
                            "dim={dim} n={n} nq={nq} k={k}"
                        );
                    }
                    if avx512_available() {
                        assert_eq!(
                            topk_bits(&batch_topk(&queries, &db, dim, k)),
                            topk_bits(&reference)
                        );
                    }
                }
            }
        }
    }

    #[test]
    fn timed_search_checksums_match_collected_results() {
        for (dim, n, nq, k) in [
            (1, 19, 3, 1),
            (65, 37, 7, 10),
            (768, 129, 16, 10),
            (769, 65, 19, 65),
        ] {
            let db = gen(n, dim, 11);
            let queries = gen(nq, dim, 13);
            let reference = unsafe { topk(&queries, &db, dim, k, canonical_ptr) };
            let expected: f64 = reference.iter().map(TopK::checksum).sum();
            if avx2_available() {
                assert_eq!(search_avx2(&queries, &db, dim, n, k), expected);
            }
            if avx512_available() {
                assert_eq!(search_avx512(&queries, &db, dim, n, k), expected);
                assert_eq!(search_batch(&queries, &db, dim, k), expected);
            }
        }
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn masked_tails_do_not_cross_guard_pages() {
        use std::ffi::{c_int, c_void};
        unsafe extern "C" {
            fn mmap(
                addr: *mut c_void,
                len: usize,
                prot: c_int,
                flags: c_int,
                fd: c_int,
                offset: i64,
            ) -> *mut c_void;
            fn mprotect(addr: *mut c_void, len: usize, prot: c_int) -> c_int;
            fn munmap(addr: *mut c_void, len: usize) -> c_int;
            fn getpagesize() -> c_int;
        }
        struct Guard {
            ptr: *mut u8,
            page: usize,
        }
        impl Guard {
            unsafe fn new() -> Self {
                let page = getpagesize() as usize;
                let ptr = mmap(std::ptr::null_mut(), page * 2, 3, 0x22, -1, 0);
                assert_ne!(ptr as isize, -1);
                assert_eq!(mprotect(ptr.cast::<u8>().add(page).cast(), page, 0), 0);
                Self {
                    ptr: ptr.cast(),
                    page,
                }
            }
            unsafe fn start(&self, dim: usize) -> *mut f32 {
                assert!(dim * 4 <= self.page);
                self.ptr.add(self.page - dim * 4).cast()
            }
        }
        impl Drop for Guard {
            fn drop(&mut self) {
                unsafe {
                    assert_eq!(munmap(self.ptr.cast(), self.page * 2), 0);
                }
            }
        }
        let a = unsafe { Guard::new() };
        let b = unsafe { Guard::new() };
        for dim in 0..=257 {
            let qa = gen(1, dim, 7);
            let qb = gen(1, dim, 13);
            let pa = unsafe { a.start(dim) };
            let pb = unsafe { b.start(dim) };
            for i in 0..dim {
                unsafe {
                    pa.add(i).write(qa[i]);
                    pb.add(i).write(qb[i]);
                }
            }
            for kernel in kernels() {
                check_distance(&qa, &qb, unsafe { kernel(pa, pb, dim) });
            }
            if avx512_available() {
                for d in unsafe { l2_queries4([pa; 4], pb, dim) } {
                    check_distance(&qa, &qb, d);
                }
            }
        }
    }
}
