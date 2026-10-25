//! Vector distance kernels and metric definitions.
//!
//! The kernels dispatch to explicit SIMD where the target supports it: NEON on
//! aarch64 (a baseline feature — Apple silicon and every armv8 server), AVX2+FMA
//! on x86-64 (runtime-detected), and a portable scalar fallback everywhere else.
//! Each SIMD kernel keeps **multiple independent accumulators** so the
//! floating-point reduction is not a single serial dependency chain — that is
//! the difference between memory-bandwidth throughput and one-FMA-per-cycle.
//! (Plain `acc += a[i]*b[i]` cannot be auto-vectorised: IEEE addition is not
//! associative and Rust does not assume `-ffast-math`.)
//!
//! The kernels operate on **raw `*const f32` pointers** with unaligned loads, so
//! a single implementation serves two entry points:
//!   * `&[f32]` slices — the HNSW graph stores each node's vector as an owned
//!     `Vec<f32>`, so navigation never re-decodes bytes.
//!   * `&[u8]` little-endian byte strings — the on-disk / `Value::Vector` form.
//!     The `*_bytes` entry points let the brute-force scan and `DISTANCE()` SQL
//!     function score a stored row **without allocating** an intermediate
//!     `Vec<f32>` per row (the dominant cost of a 20k-row brute KNN).
//!
//! The scalar form is retained as `*_scalar` and is the correctness oracle the
//! SIMD kernels are differentially tested against.

use std::ptr::read_unaligned;

/// Runtime policy for the exact brute-force KNN SME path.
///
/// The path is opt-in because it needs a small row-packing buffer in the heap
/// scan.  `auto` enables it only when the operating system advertises SME;
/// `on` has the same safety check (unsupported CPUs always fall back), while
/// `off` is the default and preserves the established NEON/scalar path.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum BruteSmeMode {
    Off,
    On,
    Auto,
}

#[inline]
pub(crate) fn brute_sme_mode() -> BruteSmeMode {
    match std::env::var("MYDB_VECTOR_BRUTE_SME") {
        Ok(v) if v.eq_ignore_ascii_case("on") || v == "1" || v.eq_ignore_ascii_case("true") => {
            BruteSmeMode::On
        }
        Ok(v) if v.eq_ignore_ascii_case("auto") => BruteSmeMode::Auto,
        _ => BruteSmeMode::Off,
    }
}

/// Whether the current process may execute SME instructions.  SME state is an
/// OS-managed architectural context, so hardware presence alone is not enough:
/// use the kernel-published capability bit on Linux and Darwin.
#[inline]
pub(crate) fn brute_sme_available() -> bool {
    #[cfg(all(target_arch = "aarch64", target_os = "macos"))]
    {
        static AVAILABLE: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
        *AVAILABLE.get_or_init(|| unsafe {
            let mut value: libc::c_int = 0;
            let mut len = std::mem::size_of_val(&value);
            let name = b"hw.optional.arm.FEAT_SME\0";
            libc::sysctlbyname(
                name.as_ptr().cast(),
                (&mut value as *mut libc::c_int).cast(),
                &mut len,
                std::ptr::null_mut(),
                0,
            ) == 0
                && value != 0
        })
    }
    #[cfg(all(target_arch = "aarch64", target_os = "linux"))]
    {
        // Linux arm64 UAPI: HWCAP2_SME = 1 << 23. `getauxval` returning zero
        // also safely covers old kernels that do not preserve SME state.
        const AT_HWCAP2: libc::c_ulong = 26;
        const HWCAP2_SME: libc::c_ulong = 1 << 23;
        unsafe { libc::getauxval(AT_HWCAP2) & HWCAP2_SME != 0 }
    }
    #[cfg(not(all(target_arch = "aarch64", any(target_os = "macos", target_os = "linux"))))]
    {
        false
    }
}

#[inline]
pub(crate) fn brute_sme_enabled() -> bool {
    !matches!(brute_sme_mode(), BruteSmeMode::Off) && brute_sme_available()
}

// One call owns streaming mode for the complete candidate batch.  This is
// deliberately a global-assembly leaf rather than one SME call per row: the
// latter pays SMSTART/SMSTOP for every 768-d distance and loses to NEON.  The
// arithmetic uses Streaming SVE, whose vector length is the implementation's
// SVL (512 bits on M4), and returns exact-f32 L2 distances with the usual SIMD
// reduction-order tolerance.
#[cfg(all(target_arch = "aarch64", target_os = "macos"))]
std::arch::global_asm!(
    r#"
    .text
    .p2align 2
    .globl _mydb_sme_l2_batch
_mydb_sme_l2_batch:
    .arch_extension sme
    // SMSTART/SMSTOP zero Z0-Z31 / P0-P15, which alias the low 64 bits of
    // V8-V15 — callee-saved under AAPCS64. Preserve them across streaming mode.
    stp d8, d9, [sp, #-64]!
    stp d10, d11, [sp, #16]
    stp d12, d13, [sp, #32]
    stp d14, d15, [sp, #48]
    smstart sm
    ptrue p0.s
    mov x5, #0
1:
    cmp x5, x2
    b.hs 4f
    mul x6, x5, x3
    add x6, x1, x6, lsl #2
    mov z0.s, #0
    mov x7, #0
2:
    whilelo p1.s, x7, x3
    b.none 3f
    ld1w {{ z1.s }}, p1/z, [x0, x7, lsl #2]
    ld1w {{ z2.s }}, p1/z, [x6, x7, lsl #2]
    fsub z1.s, p1/m, z1.s, z2.s
    fmla z0.s, p1/m, z1.s, z1.s
    incw x7
    b 2b
3:
    st1w {{ z0.s }}, p0, [x4]
    addvl x4, x4, #1
    add x5, x5, #1
    b 1b
4:
    smstop sm
    ldp d10, d11, [sp, #16]
    ldp d12, d13, [sp, #32]
    ldp d14, d15, [sp, #48]
    ldp d8, d9, [sp], #64
    ret
    .p2align 2
    .globl _mydb_sme_cntw
_mydb_sme_cntw:
    stp d8, d9, [sp, #-64]!
    stp d10, d11, [sp, #16]
    stp d12, d13, [sp, #32]
    stp d14, d15, [sp, #48]
    smstart sm
    cntw x0
    smstop sm
    ldp d10, d11, [sp, #16]
    ldp d12, d13, [sp, #32]
    ldp d14, d15, [sp, #48]
    ldp d8, d9, [sp], #64
    ret
"#
);

#[cfg(all(target_arch = "aarch64", target_os = "linux"))]
std::arch::global_asm!(
    r#"
    .text
    .p2align 2
    .globl mydb_sme_l2_batch
    .type mydb_sme_l2_batch,%function
mydb_sme_l2_batch:
    .arch_extension sme
    // SMSTART/SMSTOP zero Z0-Z31 / P0-P15, which alias the low 64 bits of
    // V8-V15 — callee-saved under AAPCS64. Preserve them across streaming mode.
    stp d8, d9, [sp, #-64]!
    stp d10, d11, [sp, #16]
    stp d12, d13, [sp, #32]
    stp d14, d15, [sp, #48]
    smstart sm
    ptrue p0.s
    mov x5, #0
1:
    cmp x5, x2
    b.hs 4f
    mul x6, x5, x3
    add x6, x1, x6, lsl #2
    mov z0.s, #0
    mov x7, #0
2:
    whilelo p1.s, x7, x3
    b.none 3f
    ld1w {{ z1.s }}, p1/z, [x0, x7, lsl #2]
    ld1w {{ z2.s }}, p1/z, [x6, x7, lsl #2]
    fsub z1.s, p1/m, z1.s, z2.s
    fmla z0.s, p1/m, z1.s, z1.s
    incw x7
    b 2b
3:
    st1w {{ z0.s }}, p0, [x4]
    addvl x4, x4, #1
    add x5, x5, #1
    b 1b
4:
    smstop sm
    ldp d10, d11, [sp, #16]
    ldp d12, d13, [sp, #32]
    ldp d14, d15, [sp, #48]
    ldp d8, d9, [sp], #64
    ret
    .size mydb_sme_l2_batch, .-mydb_sme_l2_batch
    .p2align 2
    .globl mydb_sme_cntw
    .type mydb_sme_cntw,%function
mydb_sme_cntw:
    stp d8, d9, [sp, #-64]!
    stp d10, d11, [sp, #16]
    stp d12, d13, [sp, #32]
    stp d14, d15, [sp, #48]
    smstart sm
    cntw x0
    smstop sm
    ldp d10, d11, [sp, #16]
    ldp d12, d13, [sp, #32]
    ldp d14, d15, [sp, #48]
    ldp d8, d9, [sp], #64
    ret
    .size mydb_sme_cntw, .-mydb_sme_cntw
"#
);

#[cfg(all(target_arch = "aarch64", any(target_os = "macos", target_os = "linux")))]
extern "C" {
    fn mydb_sme_l2_batch(
        query: *const f32,
        rows: *const f32,
        row_count: usize,
        dim: usize,
        out: *mut f32,
    );
    fn mydb_sme_cntw() -> usize;
}

/// Score a packed row-major candidate block with SME. Returns `false` without
/// touching `out` when SME is unavailable or the buffers are malformed.
pub(crate) fn l2_sq_sme_batch(query: &[u8], rows: &[u8], out: &mut [f32]) -> bool {
    if !brute_sme_available() || query.is_empty() || query.len() % 4 != 0 {
        return false;
    }
    let row_bytes = query.len();
    if rows.len() != out.len().saturating_mul(row_bytes) {
        return false;
    }
    #[cfg(all(target_arch = "aarch64", any(target_os = "macos", target_os = "linux")))]
    unsafe {
        let lanes = mydb_sme_cntw();
        if lanes == 0 || lanes > 64 {
            return false;
        }
        thread_local! {
            static PARTIALS: std::cell::RefCell<Vec<f32>> = const {
                std::cell::RefCell::new(Vec::new())
            };
        }
        PARTIALS.with(|cell| {
            let mut partials = cell.borrow_mut();
            partials.resize(out.len() * lanes, 0.0);
            mydb_sme_l2_batch(
                query.as_ptr().cast(),
                rows.as_ptr().cast(),
                out.len(),
                row_bytes / 4,
                partials.as_mut_ptr(),
            );
            for (dst, lane) in out.iter_mut().zip(partials.chunks_exact(lanes)) {
                // Four chains keep this tiny post-pass independent; the heavy
                // dimension loop above already ran at the streaming VL.
                let mut sums = [0.0f32; 4];
                for (i, &v) in lane.iter().enumerate() {
                    sums[i & 3] += v;
                }
                *dst = (sums[0] + sums[1]) + (sums[2] + sums[3]);
            }
        });
        true
    }
    #[cfg(not(all(target_arch = "aarch64", any(target_os = "macos", target_os = "linux"))))]
    {
        let _ = (rows, out);
        false
    }
}

/// Similarity metric a HNSW index / `DISTANCE()` call operates under.
#[derive(Clone, Copy, PartialEq, Eq, Debug, Default)]
pub(crate) enum Metric {
    /// Euclidean (L2) distance. Ordering uses squared L2 (monotone, sqrt-free);
    /// `DISTANCE()` returns the true L2 distance.
    #[default]
    L2,
    /// Cosine distance `1 - cos(a, b)` ∈ [0, 2]. Identical for ordering and
    /// display.
    Cosine,
    /// Negative inner product `-(a · b)`. Smaller = more similar, so KNN
    /// `ORDER BY DISTANCE(...) LIMIT k` selects the largest dot products
    /// (pgvector's `<#>` semantics).
    Dot,
    Hamming,
    Jaccard,
}

impl Metric {
    pub(crate) fn from_u8(value: u8) -> Option<Self> {
        Some(match value {
            0 => Self::L2, 1 => Self::Cosine, 2 => Self::Dot,
            3 => Self::Hamming, 4 => Self::Jaccard, _ => return None,
        })
    }

    /// Parse a metric name from `DISTANCE(v1, v2, 'name')` or an index option.
    /// Accepts common spellings/aliases. Case-insensitive.
    pub(crate) fn parse(s: &str) -> Option<Metric> {
        match s.trim().to_ascii_uppercase().as_str() {
            "L2" | "EUCLIDEAN" | "EUCLIDEAN_DISTANCE" => Some(Metric::L2),
            "COSINE" | "COS" => Some(Metric::Cosine),
            "DOT" | "IP" | "INNER_PRODUCT" | "DOT_PRODUCT" | "NEGATIVE_INNER_PRODUCT" => {
                Some(Metric::Dot)
            }
            "HAMMING" => Some(Metric::Hamming),
            "JACCARD" => Some(Metric::Jaccard),
            _ => None,
        }
    }

    /// Canonical name for persistence / EXPLAIN / SHOW.
    pub(crate) fn as_str(&self) -> &'static str {
        match self {
            Metric::L2 => "L2",
            Metric::Cosine => "COSINE",
            Metric::Dot => "DOT",
            Metric::Hamming => "HAMMING",
            Metric::Jaccard => "JACCARD",
        }
    }

    /// Encode to the `u8` stored in the paged `.hvec` meta page. Must match
    /// the decode in `PagedHnsw::new` (`1 => Cosine, 2 => Dot, _ => L2`).
    pub(crate) fn as_u8(&self) -> u8 {
        match self {
            Metric::L2 => 0,
            Metric::Cosine => 1,
            Metric::Dot => 2,
            Metric::Hamming => 3,
            Metric::Jaccard => 4,
        }
    }
}

// ── Public kernels over `&[f32]` ───────────────────────────────────────────

/// Inner product `Σ aᵢ·bᵢ` over the first `min(len)` elements.
#[inline]
pub(crate) fn dot(a: &[f32], b: &[f32]) -> f32 {
    let n = a.len().min(b.len());
    // SAFETY: pointers are valid for `n` reads (n ≤ both lengths).
    unsafe { dot_ptr(a.as_ptr(), b.as_ptr(), n) }
}

/// Squared Euclidean distance `Σ (aᵢ-bᵢ)²`. Monotone in true L2, so used for
/// all ordering; the sqrt is taken only when `DISTANCE()` must return a value.
#[inline]
pub(crate) fn l2_sq(a: &[f32], b: &[f32]) -> f32 {
    let n = a.len().min(b.len());
    // SAFETY: pointers are valid for `n` reads (n ≤ both lengths).
    unsafe { l2_sq_ptr(a.as_ptr(), b.as_ptr(), n) }
}

/// Four contiguous centroids share query loads and expose independent FMA
/// chains. Each row retains the single-row kernel's accumulation order.
pub(crate) fn l2_sq_batch4(query: &[f32], rows: &[f32], dim: usize) -> [f32; 4] {
    assert!(rows.len() >= dim.saturating_mul(4));
    let n = query.len().min(dim);
    #[cfg(target_arch = "x86_64")]
    if std::is_x86_feature_detected!("avx2") && std::is_x86_feature_detected!("fma") {
        // SAFETY: four complete rows and n query elements were checked above.
        return unsafe { l2_sq_batch4_avx2(query.as_ptr(), rows.as_ptr(), dim, n) };
    }
    std::array::from_fn(|j| l2_sq(query, &rows[j * dim..j * dim + n]))
}

/// Zero detection over native little-endian FLOAT32 bytes.
#[inline]
pub(crate) fn is_zero_norm_bytes(v: &[u8]) -> bool {
    v.chunks_exact(4)
        .all(|x| f32::from_le_bytes(x.try_into().unwrap()) == 0.0)
}

/// Stable norm across the complete finite f32 input domain.
pub(crate) fn norm(v: &[f32]) -> f64 {
    let squared = dot(v, v);
    if squared.is_normal() {
        (squared as f64).sqrt()
    } else {
        v.iter()
            .map(|&x| (x as f64) * (x as f64))
            .sum::<f64>()
            .sqrt()
    }
}

pub(crate) fn norm_bytes(v: &[u8]) -> f64 {
    let squared = dot_bytes(v, v);
    if squared.is_normal() {
        (squared as f64).sqrt()
    } else {
        v.chunks_exact(4)
            .map(|b| {
                let x = f32::from_le_bytes(b.try_into().unwrap()) as f64;
                x * x
            })
            .sum::<f64>()
            .sqrt()
    }
}

pub(crate) fn normalize_in_place(v: &mut [f32]) {
    let n = norm(v);
    if n > 0.0 {
        for x in v {
            *x = (*x as f64 / n) as f32;
        }
    }
}

/// Cosine similarity `(a · b) / (|a|·|b|)`. Zero-norm operands yield NaN.
#[cfg(test)]
#[inline]
pub(crate) fn cosine_similarity(a: &[f32], b: &[f32]) -> f32 {
    (1.0 - ordering_distance(a, b, Metric::Cosine)) as f32
}

/// Ordering distance: **smaller = more similar**. Used by the HNSW graph for
/// navigation and by the executor to rank candidates. L2 uses squared distance
/// (sqrt-free) since it ranks identically to true L2.
#[inline]
pub(crate) fn ordering_distance(a: &[f32], b: &[f32], metric: Metric) -> f64 {
    unsafe { ordering_distance_ptr(a.as_ptr(), b.as_ptr(), a.len().min(b.len()), metric) }
}

/// Keep ordinary SIMD reductions unchanged; exceptional magnitudes use f64
/// before subtraction/multiplication. Finite f32 vectors have finite f64 norms.
#[inline]
unsafe fn ordering_distance_ptr(a: *const f32, b: *const f32, n: usize, metric: Metric) -> f64 {
    match metric {
        Metric::Hamming | Metric::Jaccard => return f64::NAN,
        Metric::L2 => {
            let value = l2_sq_ptr(a, b, n);
            if value.is_normal() {
                return value as f64;
            }
            if value == 0.0 && (a == b || libc::memcmp(a.cast(), b.cast(), n * 4) == 0) {
                return 0.0;
            }
        }
        Metric::Dot => {
            let value = -dot_ptr(a, b, n);
            if value.is_normal() {
                return value as f64;
            }
        }
        Metric::Cosine => {
            let (d, na, nb) = cosine_parts_ptr(a, b, n);
            if d.is_finite() && na.is_normal() && nb.is_normal() {
                return (1.0 - finish_cosine(d, na, nb)) as f64;
            }
        }
    }
    wide_ordering_distance_ptr(a, b, n, metric)
}

#[cold]
#[inline(never)]
unsafe fn wide_ordering_distance_ptr(
    a: *const f32,
    b: *const f32,
    n: usize,
    metric: Metric,
) -> f64 {
    let (mut dot, mut na, mut nb, mut l2) = (0.0f64, 0.0f64, 0.0f64, 0.0f64);
    for i in 0..n {
        let x = read_unaligned(a.add(i)) as f64;
        let y = read_unaligned(b.add(i)) as f64;
        match metric {
            Metric::Hamming | Metric::Jaccard => return f64::NAN,
            Metric::L2 => {
                let d = x - y;
                l2 += d * d;
            }
            Metric::Dot => dot += x * y,
            Metric::Cosine => {
                dot += x * y;
                na += x * x;
                nb += y * y;
            }
        }
    }
    match metric {
        Metric::Hamming | Metric::Jaccard => return f64::NAN,
        Metric::L2 => l2,
        Metric::Dot => -dot,
        Metric::Cosine => {
            if na == 0.0 || nb == 0.0 {
                f64::NAN
            } else if dot == na && na == nb {
                0.0
            } else {
                1.0 - (dot / (na.sqrt() * nb.sqrt())).clamp(-1.0, 1.0)
            }
        }
    }
}

// ── Public kernels over `&[u8]` little-endian vectors (zero-alloc) ──────────
//
// The byte form is the on-disk / `Value::Vector` representation: `len/4`
// consecutive little-endian f32s. These score a stored row directly, skipping
// the per-row `Vec<f32>` decode. The pointer kernels use unaligned loads, so a
// non-4-aligned `&[u8]` is fine; on little-endian targets (x86-64, aarch64,
// every platform mydb runs on) the in-memory bytes already match.

/// Element count shared by two byte vectors (`min` of the two, in f32s).
#[inline]
fn byte_elems(a: &[u8], b: &[u8]) -> usize {
    a.len().min(b.len()) / 4
}

/// Ordering distance directly over little-endian byte vectors — no allocation.
#[inline]
pub(crate) fn ordering_distance_bytes(a: &[u8], b: &[u8], metric: Metric) -> f64 {
    unsafe {
        ordering_distance_ptr(
            a.as_ptr().cast(),
            b.as_ptr().cast(),
            byte_elems(a, b),
            metric,
        )
    }
}

/// Ordering distance between a native f32 query and a persisted little-endian
/// f32 byte vector. The paged HNSW re-rank calls this while the candidate page
/// is pinned, avoiding a full vector copy into transient `Vec<f32>` scratch.
#[inline]
pub(crate) fn ordering_distance_f32_bytes(a: &[f32], b: &[u8], metric: Metric) -> f64 {
    let n = a.len().min(b.len() / 4);
    #[cfg(target_endian = "little")]
    {
        unsafe { ordering_distance_ptr(a.as_ptr(), b.as_ptr().cast(), n, metric) }
    }
    #[cfg(target_endian = "big")]
    {
        let decoded: Vec<f32> = b[..n * 4]
            .chunks_exact(4)
            .map(|v| f32::from_le_bytes(v.try_into().unwrap()))
            .collect();
        ordering_distance(&a[..n], &decoded, metric)
    }
}

/// Dot product directly over little-endian byte vectors — no allocation.
#[inline]
pub(crate) fn dot_bytes(a: &[u8], b: &[u8]) -> f32 {
    let n = byte_elems(a, b);
    // SAFETY: as above.
    unsafe { dot_ptr(a.as_ptr() as *const f32, b.as_ptr() as *const f32, n) }
}

// ── SQ8 int8 quantization: kernels + helper ────────────────────────────────
//
// Scalar (SQ8) quantization maps each f32 component into a signed byte via a
// single global affine transform `code = round((x - mid) * inv_scale)`, clamped
// to `[-127, 127]`. The HNSW graph navigates on the int8 codes — 4× less memory
// traffic per distance and a tight integer SIMD inner loop — and the executor
// re-ranks the small candidate set with exact f32, so recall is preserved.
//
// For L2 the *squared* distance between two reconstructions is
// `scale² · Σ(code_a − code_b)²` (the `mid` offset cancels in the difference),
// so the integer SSD below is order-equivalent to true squared-L2 up to the
// quantization error; the caller multiplies by `scale²` only to keep the value
// on the same scale as the f32 fallback for the un-quantized tail.

/// Quantize `v` into `out` (same length) using a global affine transform.
/// `inv_scale = 254 / (vmax - vmin)` and `mid = (vmax + vmin) / 2`.
#[inline]
/// SQ8 with a per-dimension centre and one shared step. Offsets cancel in
/// code differences, so the integer SSD identity used by the arena kernels
/// stays exact while each dimension's range is centred on its own data.
pub(crate) fn quantize_centered_into(v: &[f32], mids: &[f32], inv_scale: f32, out: &mut [i8]) {
    for ((o, &x), &mid) in out.iter_mut().zip(v.iter()).zip(mids.iter()) {
        let q = ((x - mid) * inv_scale).round();
        *o = q.clamp(-127.0, 127.0) as i8;
    }
}

pub(crate) fn quantize_into(v: &[f32], mid: f32, inv_scale: f32, out: &mut [i8]) {
    for (o, &x) in out.iter_mut().zip(v.iter()) {
        let q = ((x - mid) * inv_scale).round();
        *o = q.clamp(-127.0, 127.0) as i8;
    }
}

/// Software-prefetch a read-only byte address into L1 (best-effort hint; a
/// no-op where the target has no prefetch intrinsic). Used to overlap the
/// random arena fetch of the *next* neighbour's int8 code row with the distance
/// computation of the current one — the SQ8 HNSW build/search hot loop is
/// memory-latency bound on those scattered 768-byte rows, not compute bound.
/// Purely a timing hint: it never changes results.
#[inline(always)]
pub(crate) fn prefetch_read(p: *const i8) {
    #[cfg(target_arch = "aarch64")]
    // SAFETY: `prfm` is a hint; any address (even invalid) is architecturally
    // safe — it cannot fault or alter state.
    unsafe {
        std::arch::asm!("prfm pldl1keep, [{0}]", in(reg) p, options(nostack, readonly, preserves_flags));
    }
    #[cfg(target_arch = "x86_64")]
    // SAFETY: `_mm_prefetch` is a hint and never faults.
    unsafe {
        std::arch::x86_64::_mm_prefetch(p as *const i8 as *const _, std::arch::x86_64::_MM_HINT_T0);
    }
    #[cfg(not(any(target_arch = "aarch64", target_arch = "x86_64")))]
    {
        let _ = p;
    }
}

/// Integer squared-distance `Σ (a_i − b_i)²` between two int8 code vectors.
/// `a`/`b` are scored over `min(len)` elements. Returns `i32` — exact (no
/// rounding); safe from overflow for any realistic dimension (worst case
/// `254² · dim`, so `dim ≤ ~33k` stays within `i32`).
#[inline]
pub(crate) fn l2_ssd_i8(a: &[i8], b: &[i8]) -> i32 {
    let n = a.len().min(b.len());
    // SAFETY: `n` ≤ both lengths; the kernels do unaligned byte reads.
    unsafe { l2_ssd_i8_ptr(a.as_ptr(), b.as_ptr(), n) }
}

/// # Safety
/// `pa`/`pb` valid for `n` consecutive `i8` reads (unaligned ok).
#[inline]
unsafe fn l2_ssd_i8_ptr(pa: *const i8, pb: *const i8, n: usize) -> i32 {
    #[cfg(target_arch = "aarch64")]
    {
        l2_ssd_i8_neon(pa, pb, n)
    }
    #[cfg(target_arch = "x86_64")]
    {
        if std::is_x86_feature_detected!("avx2") {
            l2_ssd_i8_avx2(pa, pb, n)
        } else {
            l2_ssd_i8_scalar(pa, pb, n)
        }
    }
    #[cfg(not(any(target_arch = "aarch64", target_arch = "x86_64")))]
    {
        l2_ssd_i8_scalar(pa, pb, n)
    }
}

/// Scalar oracle + portable fallback for [`l2_ssd_i8`].
/// # Safety
/// See [`l2_ssd_i8_ptr`].
#[allow(dead_code)]
#[inline]
unsafe fn l2_ssd_i8_scalar(pa: *const i8, pb: *const i8, n: usize) -> i32 {
    let mut acc: i32 = 0;
    for i in 0..n {
        let d = (*pa.add(i) as i32) - (*pb.add(i) as i32);
        acc += d * d;
    }
    acc
}

/// NEON int8 SSD: 16 codes/iteration, widen-subtract to i16, square-accumulate
/// to four i32 lanes.
/// # Safety
/// See [`l2_ssd_i8_ptr`].
#[cfg(target_arch = "aarch64")]
#[inline]
unsafe fn l2_ssd_i8_neon(pa: *const i8, pb: *const i8, n: usize) -> i32 {
    use std::arch::aarch64::*;
    let mut acc0 = vdupq_n_s32(0);
    let mut acc1 = vdupq_n_s32(0);
    let mut acc2 = vdupq_n_s32(0);
    let mut acc3 = vdupq_n_s32(0);
    let mut i = 0usize;
    while i + 16 <= n {
        let a = vld1q_s8(pa.add(i));
        let b = vld1q_s8(pb.add(i));
        // Widening subtract → i16 differences (no overflow: i8 − i8 ∈ ±254).
        let dlo = vsubl_s8(vget_low_s8(a), vget_low_s8(b)); // int16x8
        let dhi = vsubl_s8(vget_high_s8(a), vget_high_s8(b));
        // Square (i16·i16 → i32) and accumulate.
        acc0 = vmlal_s16(acc0, vget_low_s16(dlo), vget_low_s16(dlo));
        acc1 = vmlal_s16(acc1, vget_high_s16(dlo), vget_high_s16(dlo));
        acc2 = vmlal_s16(acc2, vget_low_s16(dhi), vget_low_s16(dhi));
        acc3 = vmlal_s16(acc3, vget_high_s16(dhi), vget_high_s16(dhi));
        i += 16;
    }
    let acc = vaddq_s32(vaddq_s32(acc0, acc1), vaddq_s32(acc2, acc3));
    let mut sum = vaddvq_s32(acc);
    while i < n {
        let d = (*pa.add(i) as i32) - (*pb.add(i) as i32);
        sum += d * d;
        i += 1;
    }
    sum
}

/// AVX2 int8 SSD: 16 codes/iteration, sign-extend to i16, subtract, then
/// `madd_epi16(d, d)` squares + pairwise-sums into i32 lanes.
/// # Safety
/// See [`l2_ssd_i8_ptr`].
#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2")]
unsafe fn l2_ssd_i8_avx2(pa: *const i8, pb: *const i8, n: usize) -> i32 {
    use std::arch::x86_64::*;
    let mut acc = _mm256_setzero_si256();
    let mut i = 0usize;
    while i + 16 <= n {
        let a = _mm256_cvtepi8_epi16(_mm_loadu_si128(pa.add(i) as *const __m128i));
        let b = _mm256_cvtepi8_epi16(_mm_loadu_si128(pb.add(i) as *const __m128i));
        let d = _mm256_sub_epi16(a, b);
        acc = _mm256_add_epi32(acc, _mm256_madd_epi16(d, d));
        i += 16;
    }
    // Horizontal sum of the 8 i32 lanes.
    let lo = _mm256_castsi256_si128(acc);
    let hi = _mm256_extracti128_si256(acc, 1);
    let s = _mm_add_epi32(lo, hi);
    let s = _mm_add_epi32(s, _mm_shuffle_epi32(s, 0b01_00_11_10));
    let s = _mm_add_epi32(s, _mm_shuffle_epi32(s, 0b00_00_00_01));
    let mut sum = _mm_cvtsi128_si32(s);
    while i < n {
        let d = (*pa.add(i) as i32) - (*pb.add(i) as i32);
        sum += d * d;
        i += 1;
    }
    sum
}

/// Integer dot product `Σ a_i·b_i` between two int8 code vectors, scored over
/// `min(len)` elements. Exact `i32` (worst case `127²·dim`, safe to `dim ≤ ~133k`).
///
/// Used by the norm-decomposed SQ8 distance: with a precomputed per-node
/// `‖n‖²` and a once-per-query `‖q‖²`, squared-L2 between two int8 codes is the
/// exact integer identity `‖q‖² + ‖n‖² − 2·dot(q,n)`. The dot product is the
/// only per-probe inner loop; on aarch64 `sdot` retires 16 int8 MACs per
/// instruction (≈2.5× fewer ops than the widen-subtract-square `l2_ssd_i8`),
/// which lets the prefetcher hide more of the random arena fetch on the
/// memory-bound search hot path. The result is **bit-identical** to
/// [`l2_ssd_i8`] (integer arithmetic, no rounding).
#[inline]
pub(crate) fn dot_i8(a: &[i8], b: &[i8]) -> i32 {
    let n = a.len().min(b.len());
    // SAFETY: `n` ≤ both lengths; the kernels do unaligned byte reads.
    unsafe { dot_i8_ptr(a.as_ptr(), b.as_ptr(), n) }
}

/// Dot one query against four independently located int8 rows. HNSW
/// callers do not have contiguous candidates: graph
/// neighbours are random ordinals in the resident code arena. Sharing each
/// query load across four SDOT streams avoids re-reading the query four times
/// and exposes enough independent accumulators to cover random-row latency.
/// Results are integer-exact and match four scalar [`dot_i8`] calls.
#[inline]
pub(crate) fn dot_i8_batch4_ptrs(query: &[i8], rows: [&[i8]; 4]) -> [i32; 4] {
    let n = query.len();
    if rows.iter().any(|row| row.len() < n) {
        return [
            dot_i8(query, rows[0]),
            dot_i8(query, rows[1]),
            dot_i8(query, rows[2]),
            dot_i8(query, rows[3]),
        ];
    }
    #[cfg(target_arch = "aarch64")]
    unsafe {
        let has_dotprod = if cfg!(target_os = "macos") {
            true
        } else {
            std::arch::is_aarch64_feature_detected!("dotprod")
        };
        if has_dotprod {
            let mut out = [0i32; 4];
            dot_i8_batch4_ptrs_neon_dotprod(
                query.as_ptr(),
                rows[0].as_ptr(),
                rows[1].as_ptr(),
                rows[2].as_ptr(),
                rows[3].as_ptr(),
                n,
                out.as_mut_ptr(),
            );
            return out;
        }
    }
    [
        dot_i8(query, rows[0]),
        dot_i8(query, rows[1]),
        dot_i8(query, rows[2]),
        dot_i8(query, rows[3]),
    ]
}

/// Four-row SDOT kernel for independently located rows.
#[cfg(target_arch = "aarch64")]
#[target_feature(enable = "dotprod")]
#[inline]
unsafe fn dot_i8_batch4_ptrs_neon_dotprod(
    query: *const i8,
    r0: *const i8,
    r1: *const i8,
    r2: *const i8,
    r3: *const i8,
    n: usize,
    out: *mut i32,
) {
    use std::arch::aarch64::*;
    let mut a00 = vdupq_n_s32(0);
    let mut a01 = vdupq_n_s32(0);
    let mut a10 = vdupq_n_s32(0);
    let mut a11 = vdupq_n_s32(0);
    let mut a20 = vdupq_n_s32(0);
    let mut a21 = vdupq_n_s32(0);
    let mut a30 = vdupq_n_s32(0);
    let mut a31 = vdupq_n_s32(0);
    let mut i = 0usize;
    while i + 32 <= n {
        let q0 = vld1q_s8(query.add(i));
        let q1 = vld1q_s8(query.add(i + 16));
        let b00 = vld1q_s8(r0.add(i));
        let b01 = vld1q_s8(r0.add(i + 16));
        let b10 = vld1q_s8(r1.add(i));
        let b11 = vld1q_s8(r1.add(i + 16));
        let b20 = vld1q_s8(r2.add(i));
        let b21 = vld1q_s8(r2.add(i + 16));
        let b30 = vld1q_s8(r3.add(i));
        let b31 = vld1q_s8(r3.add(i + 16));
        std::arch::asm!(
            "sdot {a00:v}.4s, {q0:v}.16b, {b00:v}.16b",
            "sdot {a10:v}.4s, {q0:v}.16b, {b10:v}.16b",
            "sdot {a20:v}.4s, {q0:v}.16b, {b20:v}.16b",
            "sdot {a30:v}.4s, {q0:v}.16b, {b30:v}.16b",
            "sdot {a01:v}.4s, {q1:v}.16b, {b01:v}.16b",
            "sdot {a11:v}.4s, {q1:v}.16b, {b11:v}.16b",
            "sdot {a21:v}.4s, {q1:v}.16b, {b21:v}.16b",
            "sdot {a31:v}.4s, {q1:v}.16b, {b31:v}.16b",
            a00 = inout(vreg) a00, a01 = inout(vreg) a01,
            a10 = inout(vreg) a10, a11 = inout(vreg) a11,
            a20 = inout(vreg) a20, a21 = inout(vreg) a21,
            a30 = inout(vreg) a30, a31 = inout(vreg) a31,
            q0 = in(vreg) q0, q1 = in(vreg) q1,
            b00 = in(vreg) b00, b01 = in(vreg) b01,
            b10 = in(vreg) b10, b11 = in(vreg) b11,
            b20 = in(vreg) b20, b21 = in(vreg) b21,
            b30 = in(vreg) b30, b31 = in(vreg) b31,
            options(nomem, nostack, preserves_flags),
        );
        i += 32;
    }
    let mut sums = [
        vaddvq_s32(vaddq_s32(a00, a01)),
        vaddvq_s32(vaddq_s32(a10, a11)),
        vaddvq_s32(vaddq_s32(a20, a21)),
        vaddvq_s32(vaddq_s32(a30, a31)),
    ];
    while i < n {
        let q = *query.add(i) as i32;
        sums[0] += q * *r0.add(i) as i32;
        sums[1] += q * *r1.add(i) as i32;
        sums[2] += q * *r2.add(i) as i32;
        sums[3] += q * *r3.add(i) as i32;
        i += 1;
    }
    out.copy_from_nonoverlapping(sums.as_ptr(), 4);
}

/// Sum of squares `Σ a_i²` of an int8 code vector — the per-node `‖n‖²` term.
#[inline]
pub(crate) fn ssq_i8(a: &[i8]) -> i32 {
    let n = a.len();
    // SAFETY: reads exactly `n` elements.
    unsafe { dot_i8_ptr(a.as_ptr(), a.as_ptr(), n) }
}

/// # Safety
/// `pa`/`pb` valid for `n` consecutive `i8` reads (unaligned ok).
#[inline]
unsafe fn dot_i8_ptr(pa: *const i8, pb: *const i8, n: usize) -> i32 {
    // Every Apple-Silicon CPU implements FEAT_DotProd. Avoid repeating the
    // runtime feature probe in the innermost HNSW neighbour loop on Darwin;
    // generic aarch64 (including older server cores) keeps safe detection.
    #[cfg(all(target_arch = "aarch64", target_os = "macos"))]
    {
        dot_i8_neon_dotprod(pa, pb, n)
    }
    #[cfg(all(target_arch = "aarch64", not(target_os = "macos")))]
    {
        if std::arch::is_aarch64_feature_detected!("dotprod") {
            return dot_i8_neon_dotprod(pa, pb, n);
        }
        dot_i8_scalar(pa, pb, n)
    }
    #[cfg(target_arch = "x86_64")]
    {
        if std::is_x86_feature_detected!("avx2") {
            if n >= 64
                && std::is_x86_feature_detected!("avx512vnni")
                && std::is_x86_feature_detected!("avx512vl")
            {
                return dot_i8_vnni(pa, pb, n);
            }
            dot_i8_avx2(pa, pb, n)
        } else {
            dot_i8_scalar(pa, pb, n)
        }
    }
    #[cfg(not(any(target_arch = "aarch64", target_arch = "x86_64")))]
    {
        dot_i8_scalar(pa, pb, n)
    }
}

/// Scalar oracle + portable fallback for [`dot_i8`].
/// # Safety
/// See [`dot_i8_ptr`].
#[allow(dead_code)]
#[inline]
unsafe fn dot_i8_scalar(pa: *const i8, pb: *const i8, n: usize) -> i32 {
    let mut acc: i32 = 0;
    for i in 0..n {
        acc += (*pa.add(i) as i32) * (*pb.add(i) as i32);
    }
    acc
}

/// NEON `sdot` int8 dot product: 32 codes/iteration across two i32x4
/// accumulators. `vdotq_s32` is still unstable on stable Rust (the
/// `stdarch_neon_dotprod` feature gate), so the `sdot` instruction is emitted
/// via inline asm — the M-series (ARMv8.4) always has FEAT_DotProd, but the
/// caller still runtime-checks `dotprod` before dispatching here.
/// # Safety
/// See [`dot_i8_ptr`]. Runs on any aarch64 (FEAT_DotProd guaranteed by the
/// caller's feature check).
#[cfg(target_arch = "aarch64")]
#[target_feature(enable = "dotprod")]
#[inline]
unsafe fn dot_i8_neon_dotprod(pa: *const i8, pb: *const i8, n: usize) -> i32 {
    use std::arch::aarch64::*;
    let mut acc0 = vdupq_n_s32(0);
    let mut acc1 = vdupq_n_s32(0);
    let mut acc2 = vdupq_n_s32(0);
    let mut acc3 = vdupq_n_s32(0);
    let mut i = 0usize;
    // Four independent accumulators break the dependency chain so all `sdot`
    // pipes stay busy: at high dim (768/1536) the dot product is the dominant
    // query cost and SDOT has multi-cycle latency, so a 2-wide chain stalls
    // waiting on each accumulator's previous add. 64 codes/iteration.
    while i + 64 <= n {
        let a0 = vld1q_s8(pa.add(i));
        let b0 = vld1q_s8(pb.add(i));
        let a1 = vld1q_s8(pa.add(i + 16));
        let b1 = vld1q_s8(pb.add(i + 16));
        let a2 = vld1q_s8(pa.add(i + 32));
        let b2 = vld1q_s8(pb.add(i + 32));
        let a3 = vld1q_s8(pa.add(i + 48));
        let b3 = vld1q_s8(pb.add(i + 48));
        // acc += dot(a, b), grouped 4-int8 → 1-int32 per lane.
        std::arch::asm!(
            "sdot {acc0:v}.4s, {a0:v}.16b, {b0:v}.16b",
            "sdot {acc1:v}.4s, {a1:v}.16b, {b1:v}.16b",
            "sdot {acc2:v}.4s, {a2:v}.16b, {b2:v}.16b",
            "sdot {acc3:v}.4s, {a3:v}.16b, {b3:v}.16b",
            acc0 = inout(vreg) acc0,
            acc1 = inout(vreg) acc1,
            acc2 = inout(vreg) acc2,
            acc3 = inout(vreg) acc3,
            a0 = in(vreg) a0, b0 = in(vreg) b0,
            a1 = in(vreg) a1, b1 = in(vreg) b1,
            a2 = in(vreg) a2, b2 = in(vreg) b2,
            a3 = in(vreg) a3, b3 = in(vreg) b3,
            options(nomem, nostack, preserves_flags),
        );
        i += 64;
    }
    while i + 32 <= n {
        let a0 = vld1q_s8(pa.add(i));
        let b0 = vld1q_s8(pb.add(i));
        let a1 = vld1q_s8(pa.add(i + 16));
        let b1 = vld1q_s8(pb.add(i + 16));
        std::arch::asm!(
            "sdot {acc0:v}.4s, {a0:v}.16b, {b0:v}.16b",
            "sdot {acc1:v}.4s, {a1:v}.16b, {b1:v}.16b",
            acc0 = inout(vreg) acc0,
            acc1 = inout(vreg) acc1,
            a0 = in(vreg) a0, b0 = in(vreg) b0,
            a1 = in(vreg) a1, b1 = in(vreg) b1,
            options(nomem, nostack, preserves_flags),
        );
        i += 32;
    }
    acc0 = vaddq_s32(acc0, acc2);
    acc1 = vaddq_s32(acc1, acc3);
    while i + 16 <= n {
        let a0 = vld1q_s8(pa.add(i));
        let b0 = vld1q_s8(pb.add(i));
        std::arch::asm!(
            "sdot {acc0:v}.4s, {a0:v}.16b, {b0:v}.16b",
            acc0 = inout(vreg) acc0,
            a0 = in(vreg) a0, b0 = in(vreg) b0,
            options(nomem, nostack, preserves_flags),
        );
        i += 16;
    }
    let mut sum = vaddvq_s32(vaddq_s32(acc0, acc1));
    while i < n {
        sum += (*pa.add(i) as i32) * (*pb.add(i) as i32);
        i += 1;
    }
    sum
}

/// Exact signed int8 dot product with 256-bit VNNI. VPDPBUSD multiplies
/// unsigned by signed bytes, so subtract the 128*sum(b) bias. Independent
/// accumulators hide instruction latency without saturating intermediate sums.
/// # Safety
/// See [`dot_i8_ptr`]; the caller detects all required CPU features.
#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2,avx512vnni,avx512vl")]
unsafe fn dot_i8_vnni(pa: *const i8, pb: *const i8, n: usize) -> i32 {
    use std::arch::x86_64::*;
    let bias = _mm256_set1_epi8(i8::MIN);
    let (mut a0, mut a1, mut a2, mut a3) = (
        _mm256_setzero_si256(),
        _mm256_setzero_si256(),
        _mm256_setzero_si256(),
        _mm256_setzero_si256(),
    );
    let (mut c0, mut c1, mut c2, mut c3) = (
        _mm256_setzero_si256(),
        _mm256_setzero_si256(),
        _mm256_setzero_si256(),
        _mm256_setzero_si256(),
    );
    macro_rules! step {
        ($at:expr, $acc:ident, $correction:ident) => {{
            let a = _mm256_xor_si256(_mm256_loadu_si256(pa.add($at).cast()), bias);
            let b = _mm256_loadu_si256(pb.add($at).cast());
            $acc = _mm256_dpbusd_epi32($acc, a, b);
            $correction = _mm256_dpbusd_epi32($correction, bias, b);
        }};
    }
    let mut i = 0;
    while i + 128 <= n {
        step!(i, a0, c0);
        step!(i + 32, a1, c1);
        step!(i + 64, a2, c2);
        step!(i + 96, a3, c3);
        i += 128;
    }
    a0 = _mm256_add_epi32(_mm256_add_epi32(a0, a1), _mm256_add_epi32(a2, a3));
    c0 = _mm256_add_epi32(_mm256_add_epi32(c0, c1), _mm256_add_epi32(c2, c3));
    while i + 32 <= n {
        step!(i, a0, c0);
        i += 32;
    }
    let acc = _mm256_sub_epi32(a0, c0);
    let sum = _mm_add_epi32(
        _mm256_castsi256_si128(acc),
        _mm256_extracti128_si256(acc, 1),
    );
    let sum = _mm_add_epi32(sum, _mm_shuffle_epi32(sum, 0b01_00_11_10));
    let sum = _mm_add_epi32(sum, _mm_shuffle_epi32(sum, 0b00_00_00_01));
    let mut sum = _mm_cvtsi128_si32(sum);
    while i < n {
        sum += (*pa.add(i) as i32) * (*pb.add(i) as i32);
        i += 1;
    }
    sum
}

/// AVX2 int8 dot product: sign-extend to i16, `madd_epi16` to i32 lanes.
/// # Safety
/// See [`dot_i8_ptr`].
#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2")]
unsafe fn dot_i8_avx2(pa: *const i8, pb: *const i8, n: usize) -> i32 {
    use std::arch::x86_64::*;
    let mut acc = _mm256_setzero_si256();
    let mut i = 0usize;
    while i + 16 <= n {
        let a = _mm256_cvtepi8_epi16(_mm_loadu_si128(pa.add(i) as *const __m128i));
        let b = _mm256_cvtepi8_epi16(_mm_loadu_si128(pb.add(i) as *const __m128i));
        acc = _mm256_add_epi32(acc, _mm256_madd_epi16(a, b));
        i += 16;
    }
    let lo = _mm256_castsi256_si128(acc);
    let hi = _mm256_extracti128_si256(acc, 1);
    let s = _mm_add_epi32(lo, hi);
    let s = _mm_add_epi32(s, _mm_shuffle_epi32(s, 0b01_00_11_10));
    let s = _mm_add_epi32(s, _mm_shuffle_epi32(s, 0b00_00_00_01));
    let mut sum = _mm_cvtsi128_si32(s);
    while i < n {
        sum += (*pa.add(i) as i32) * (*pb.add(i) as i32);
        i += 1;
    }
    sum
}

#[inline]
fn finish_cosine(dot: f32, na: f32, nb: f32) -> f32 {
    // The shared reduction produces identical dot/norm values for a vector
    // compared with itself. Avoid two rounded square roots breaking identity.
    if dot == na && na == nb && na > 0.0 && na.is_finite() {
        return 1.0;
    }
    let denom = na.sqrt() * nb.sqrt();
    if denom == 0.0 {
        f32::NAN
    } else {
        (dot / denom).clamp(-1.0, 1.0)
    }
}

// ── Pointer kernels: dispatch to the best available implementation ─────────

/// # Safety
/// `pa` and `pb` must each be valid for `n` consecutive `f32` reads. Alignment
/// is *not* required (unaligned loads are used throughout).
#[inline]
unsafe fn l2_sq_ptr(pa: *const f32, pb: *const f32, n: usize) -> f32 {
    #[cfg(target_arch = "aarch64")]
    {
        l2_sq_neon(pa, pb, n)
    }
    #[cfg(target_arch = "x86_64")]
    {
        if std::is_x86_feature_detected!("avx2") && std::is_x86_feature_detected!("fma") {
            l2_sq_avx2(pa, pb, n)
        } else {
            l2_sq_scalar_ptr(pa, pb, n)
        }
    }
    #[cfg(not(any(target_arch = "aarch64", target_arch = "x86_64")))]
    {
        l2_sq_scalar_ptr(pa, pb, n)
    }
}

/// # Safety
/// See [`l2_sq_ptr`].
#[inline]
unsafe fn dot_ptr(pa: *const f32, pb: *const f32, n: usize) -> f32 {
    #[cfg(target_arch = "aarch64")]
    {
        dot_neon(pa, pb, n)
    }
    #[cfg(target_arch = "x86_64")]
    {
        if std::is_x86_feature_detected!("avx2") && std::is_x86_feature_detected!("fma") {
            dot_avx2(pa, pb, n)
        } else {
            dot_scalar_ptr(pa, pb, n)
        }
    }
    #[cfg(not(any(target_arch = "aarch64", target_arch = "x86_64")))]
    {
        dot_scalar_ptr(pa, pb, n)
    }
}

/// # Safety
/// See [`l2_sq_ptr`].
#[inline]
unsafe fn cosine_parts_ptr(pa: *const f32, pb: *const f32, n: usize) -> (f32, f32, f32) {
    #[cfg(target_arch = "aarch64")]
    {
        cosine_parts_neon(pa, pb, n)
    }
    #[cfg(target_arch = "x86_64")]
    {
        if std::is_x86_feature_detected!("avx2") && std::is_x86_feature_detected!("fma") {
            cosine_parts_avx2(pa, pb, n)
        } else {
            cosine_parts_scalar_ptr(pa, pb, n)
        }
    }
    #[cfg(not(any(target_arch = "aarch64", target_arch = "x86_64")))]
    {
        cosine_parts_scalar_ptr(pa, pb, n)
    }
}

// ── Scalar reference kernels (correctness oracle + portable fallback) ───────

#[allow(dead_code)]
#[inline]
pub(crate) fn dot_scalar(a: &[f32], b: &[f32]) -> f32 {
    let n = a.len().min(b.len());
    // SAFETY: n ≤ both lengths.
    unsafe { dot_scalar_ptr(a.as_ptr(), b.as_ptr(), n) }
}

#[allow(dead_code)]
#[inline]
pub(crate) fn l2_sq_scalar(a: &[f32], b: &[f32]) -> f32 {
    let n = a.len().min(b.len());
    // SAFETY: n ≤ both lengths.
    unsafe { l2_sq_scalar_ptr(a.as_ptr(), b.as_ptr(), n) }
}

/// Returns `(dot, |a|², |b|²)` — the three reductions cosine needs.
#[allow(dead_code)]
#[inline]
pub(crate) fn cosine_parts_scalar(a: &[f32], b: &[f32]) -> (f32, f32, f32) {
    let n = a.len().min(b.len());
    // SAFETY: n ≤ both lengths.
    unsafe { cosine_parts_scalar_ptr(a.as_ptr(), b.as_ptr(), n) }
}

/// # Safety
/// `pa`/`pb` valid for `n` unaligned `f32` reads.
#[allow(dead_code)]
#[inline]
unsafe fn dot_scalar_ptr(pa: *const f32, pb: *const f32, n: usize) -> f32 {
    let mut acc = 0.0f32;
    for i in 0..n {
        acc += read_unaligned(pa.add(i)) * read_unaligned(pb.add(i));
    }
    acc
}

/// # Safety
/// See [`dot_scalar_ptr`].
#[allow(dead_code)]
#[inline]
unsafe fn l2_sq_scalar_ptr(pa: *const f32, pb: *const f32, n: usize) -> f32 {
    let mut acc = 0.0f32;
    for i in 0..n {
        let d = read_unaligned(pa.add(i)) - read_unaligned(pb.add(i));
        acc += d * d;
    }
    acc
}

/// # Safety
/// See [`dot_scalar_ptr`].
#[allow(dead_code)]
#[inline]
unsafe fn cosine_parts_scalar_ptr(pa: *const f32, pb: *const f32, n: usize) -> (f32, f32, f32) {
    let mut dot = 0.0f32;
    let mut na = 0.0f32;
    let mut nb = 0.0f32;
    for i in 0..n {
        let x = read_unaligned(pa.add(i));
        let y = read_unaligned(pb.add(i));
        dot += x * y;
        na += x * x;
        nb += y * y;
    }
    (dot, na, nb)
}

// ── aarch64 NEON kernels ───────────────────────────────────────────────────
//
// NEON is a baseline feature on aarch64, so these need no runtime detection.
// Each processes 16 lanes/iteration across four independent `float32x4_t`
// accumulators (FMA latency on Apple silicon is ~3-4 cycles, so four in-flight
// accumulators keep the pipeline full), then a 4-wide tail and a scalar
// remainder. `vld1q_f32` permits unaligned addresses on aarch64.

#[cfg(target_arch = "aarch64")]
#[inline]
unsafe fn l2_sq_neon(pa: *const f32, pb: *const f32, n: usize) -> f32 {
    use std::arch::aarch64::*;
    let mut acc0 = vdupq_n_f32(0.0);
    let mut acc1 = vdupq_n_f32(0.0);
    let mut acc2 = vdupq_n_f32(0.0);
    let mut acc3 = vdupq_n_f32(0.0);
    let mut i = 0usize;
    while i + 16 <= n {
        let d0 = vsubq_f32(vld1q_f32(pa.add(i)), vld1q_f32(pb.add(i)));
        let d1 = vsubq_f32(vld1q_f32(pa.add(i + 4)), vld1q_f32(pb.add(i + 4)));
        let d2 = vsubq_f32(vld1q_f32(pa.add(i + 8)), vld1q_f32(pb.add(i + 8)));
        let d3 = vsubq_f32(vld1q_f32(pa.add(i + 12)), vld1q_f32(pb.add(i + 12)));
        acc0 = vfmaq_f32(acc0, d0, d0);
        acc1 = vfmaq_f32(acc1, d1, d1);
        acc2 = vfmaq_f32(acc2, d2, d2);
        acc3 = vfmaq_f32(acc3, d3, d3);
        i += 16;
    }
    while i + 4 <= n {
        let d0 = vsubq_f32(vld1q_f32(pa.add(i)), vld1q_f32(pb.add(i)));
        acc0 = vfmaq_f32(acc0, d0, d0);
        i += 4;
    }
    let acc = vaddq_f32(vaddq_f32(acc0, acc1), vaddq_f32(acc2, acc3));
    let mut sum = vaddvq_f32(acc);
    while i < n {
        let d = read_unaligned(pa.add(i)) - read_unaligned(pb.add(i));
        sum += d * d;
        i += 1;
    }
    sum
}

#[cfg(target_arch = "aarch64")]
#[inline]
unsafe fn dot_neon(pa: *const f32, pb: *const f32, n: usize) -> f32 {
    use std::arch::aarch64::*;
    let mut acc0 = vdupq_n_f32(0.0);
    let mut acc1 = vdupq_n_f32(0.0);
    let mut acc2 = vdupq_n_f32(0.0);
    let mut acc3 = vdupq_n_f32(0.0);
    let mut i = 0usize;
    while i + 16 <= n {
        acc0 = vfmaq_f32(acc0, vld1q_f32(pa.add(i)), vld1q_f32(pb.add(i)));
        acc1 = vfmaq_f32(acc1, vld1q_f32(pa.add(i + 4)), vld1q_f32(pb.add(i + 4)));
        acc2 = vfmaq_f32(acc2, vld1q_f32(pa.add(i + 8)), vld1q_f32(pb.add(i + 8)));
        acc3 = vfmaq_f32(acc3, vld1q_f32(pa.add(i + 12)), vld1q_f32(pb.add(i + 12)));
        i += 16;
    }
    while i + 4 <= n {
        acc0 = vfmaq_f32(acc0, vld1q_f32(pa.add(i)), vld1q_f32(pb.add(i)));
        i += 4;
    }
    let acc = vaddq_f32(vaddq_f32(acc0, acc1), vaddq_f32(acc2, acc3));
    let mut sum = vaddvq_f32(acc);
    while i < n {
        sum += read_unaligned(pa.add(i)) * read_unaligned(pb.add(i));
        i += 1;
    }
    sum
}

#[cfg(target_arch = "aarch64")]
#[inline]
unsafe fn cosine_parts_neon(pa: *const f32, pb: *const f32, n: usize) -> (f32, f32, f32) {
    use std::arch::aarch64::*;
    let mut dot0 = vdupq_n_f32(0.0);
    let mut dot1 = vdupq_n_f32(0.0);
    let mut na0 = vdupq_n_f32(0.0);
    let mut na1 = vdupq_n_f32(0.0);
    let mut nb0 = vdupq_n_f32(0.0);
    let mut nb1 = vdupq_n_f32(0.0);
    let mut i = 0usize;
    while i + 8 <= n {
        let va0 = vld1q_f32(pa.add(i));
        let vb0 = vld1q_f32(pb.add(i));
        let va1 = vld1q_f32(pa.add(i + 4));
        let vb1 = vld1q_f32(pb.add(i + 4));
        dot0 = vfmaq_f32(dot0, va0, vb0);
        dot1 = vfmaq_f32(dot1, va1, vb1);
        na0 = vfmaq_f32(na0, va0, va0);
        na1 = vfmaq_f32(na1, va1, va1);
        nb0 = vfmaq_f32(nb0, vb0, vb0);
        nb1 = vfmaq_f32(nb1, vb1, vb1);
        i += 8;
    }
    let mut dot = vaddvq_f32(vaddq_f32(dot0, dot1));
    let mut na = vaddvq_f32(vaddq_f32(na0, na1));
    let mut nb = vaddvq_f32(vaddq_f32(nb0, nb1));
    while i < n {
        let x = read_unaligned(pa.add(i));
        let y = read_unaligned(pb.add(i));
        dot += x * y;
        na += x * x;
        nb += y * y;
        i += 1;
    }
    (dot, na, nb)
}

// ── x86-64 AVX2 + FMA kernels ──────────────────────────────────────────────
//
// Runtime-detected in the dispatchers. 16 lanes/iteration across two `__m256`
// accumulators, an 8-wide tail, and a scalar remainder. `_mm256_loadu_ps` is
// the explicitly-unaligned load.

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2,fma")]
unsafe fn l2_sq_avx2(pa: *const f32, pb: *const f32, n: usize) -> f32 {
    use std::arch::x86_64::*;
    let mut acc0 = _mm256_setzero_ps();
    let mut acc1 = _mm256_setzero_ps();
    let mut i = 0usize;
    while i + 16 <= n {
        let d0 = _mm256_sub_ps(_mm256_loadu_ps(pa.add(i)), _mm256_loadu_ps(pb.add(i)));
        let d1 = _mm256_sub_ps(
            _mm256_loadu_ps(pa.add(i + 8)),
            _mm256_loadu_ps(pb.add(i + 8)),
        );
        acc0 = _mm256_fmadd_ps(d0, d0, acc0);
        acc1 = _mm256_fmadd_ps(d1, d1, acc1);
        i += 16;
    }
    while i + 8 <= n {
        let d0 = _mm256_sub_ps(_mm256_loadu_ps(pa.add(i)), _mm256_loadu_ps(pb.add(i)));
        acc0 = _mm256_fmadd_ps(d0, d0, acc0);
        i += 8;
    }
    let mut sum = hsum256(_mm256_add_ps(acc0, acc1));
    while i < n {
        let d = read_unaligned(pa.add(i)) - read_unaligned(pb.add(i));
        sum += d * d;
        i += 1;
    }
    sum
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2,fma")]
unsafe fn l2_sq_batch4_avx2(
    query: *const f32,
    rows: *const f32,
    stride: usize,
    n: usize,
) -> [f32; 4] {
    use std::arch::x86_64::*;
    let mut acc0 = [_mm256_setzero_ps(); 4];
    let mut acc1 = [_mm256_setzero_ps(); 4];
    let mut i = 0;
    while i + 16 <= n {
        let q0 = _mm256_loadu_ps(query.add(i));
        let q1 = _mm256_loadu_ps(query.add(i + 8));
        for j in 0..4 {
            let row = rows.add(j * stride + i);
            let d0 = _mm256_sub_ps(q0, _mm256_loadu_ps(row));
            let d1 = _mm256_sub_ps(q1, _mm256_loadu_ps(row.add(8)));
            acc0[j] = _mm256_fmadd_ps(d0, d0, acc0[j]);
            acc1[j] = _mm256_fmadd_ps(d1, d1, acc1[j]);
        }
        i += 16;
    }
    if i + 8 <= n {
        let q = _mm256_loadu_ps(query.add(i));
        for j in 0..4 {
            let d = _mm256_sub_ps(q, _mm256_loadu_ps(rows.add(j * stride + i)));
            acc0[j] = _mm256_fmadd_ps(d, d, acc0[j]);
        }
        i += 8;
    }
    let mut out = std::array::from_fn(|j| hsum256(_mm256_add_ps(acc0[j], acc1[j])));
    while i < n {
        let q = read_unaligned(query.add(i));
        for j in 0..4 {
            let d = q - read_unaligned(rows.add(j * stride + i));
            out[j] += d * d;
        }
        i += 1;
    }
    out
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2,fma")]
unsafe fn dot_avx2(pa: *const f32, pb: *const f32, n: usize) -> f32 {
    use std::arch::x86_64::*;
    let mut acc0 = _mm256_setzero_ps();
    let mut acc1 = _mm256_setzero_ps();
    let mut i = 0usize;
    while i + 16 <= n {
        acc0 = _mm256_fmadd_ps(_mm256_loadu_ps(pa.add(i)), _mm256_loadu_ps(pb.add(i)), acc0);
        acc1 = _mm256_fmadd_ps(
            _mm256_loadu_ps(pa.add(i + 8)),
            _mm256_loadu_ps(pb.add(i + 8)),
            acc1,
        );
        i += 16;
    }
    while i + 8 <= n {
        acc0 = _mm256_fmadd_ps(_mm256_loadu_ps(pa.add(i)), _mm256_loadu_ps(pb.add(i)), acc0);
        i += 8;
    }
    let mut sum = hsum256(_mm256_add_ps(acc0, acc1));
    while i < n {
        sum += read_unaligned(pa.add(i)) * read_unaligned(pb.add(i));
        i += 1;
    }
    sum
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2,fma")]
unsafe fn cosine_parts_avx2(pa: *const f32, pb: *const f32, n: usize) -> (f32, f32, f32) {
    use std::arch::x86_64::*;
    let mut dot = _mm256_setzero_ps();
    let mut na = _mm256_setzero_ps();
    let mut nb = _mm256_setzero_ps();
    let mut i = 0usize;
    while i + 8 <= n {
        let va = _mm256_loadu_ps(pa.add(i));
        let vb = _mm256_loadu_ps(pb.add(i));
        dot = _mm256_fmadd_ps(va, vb, dot);
        na = _mm256_fmadd_ps(va, va, na);
        nb = _mm256_fmadd_ps(vb, vb, nb);
        i += 8;
    }
    let mut sdot = hsum256(dot);
    let mut sna = hsum256(na);
    let mut snb = hsum256(nb);
    while i < n {
        let x = read_unaligned(pa.add(i));
        let y = read_unaligned(pb.add(i));
        sdot += x * y;
        sna += x * x;
        snb += y * y;
        i += 1;
    }
    (sdot, sna, snb)
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2")]
#[inline]
unsafe fn hsum256(v: std::arch::x86_64::__m256) -> f32 {
    use std::arch::x86_64::*;
    let lo = _mm256_castps256_ps128(v);
    let hi = _mm256_extractf128_ps(v, 1);
    let sum128 = _mm_add_ps(lo, hi);
    let shuf = _mm_movehdup_ps(sum128);
    let sums = _mm_add_ps(sum128, shuf);
    let shuf2 = _mm_movehl_ps(shuf, sums);
    let sums = _mm_add_ss(sums, shuf2);
    _mm_cvtss_f32(sums)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn pseudo(n: usize, seed: u64) -> Vec<f32> {
        // SplitMix64 → f32 in [-1, 1], deterministic.
        let mut s = seed;
        (0..n)
            .map(|_| {
                s = s.wrapping_add(0x9E37_79B9_7F4A_7C15);
                let mut z = s;
                z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
                z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
                z ^= z >> 31;
                ((z >> 40) as f32 / (1u64 << 24) as f32) * 2.0 - 1.0
            })
            .collect()
    }

    fn to_bytes(v: &[f32]) -> Vec<u8> {
        let mut out = Vec::with_capacity(v.len() * 4);
        for &x in v {
            out.extend_from_slice(&x.to_le_bytes());
        }
        out
    }

    /// SIMD kernels must match the scalar oracle to a tight float tolerance
    /// across every length residue class (multiples of 16/8/4 plus tails) so
    /// the unrolled main loop, the narrow tail, and the scalar remainder are
    /// all exercised — over both the `&[f32]` and `&[u8]` entry points.
    #[test]
    fn simd_matches_scalar_oracle() {
        for &dim in &[
            1usize, 3, 4, 7, 8, 15, 16, 17, 31, 33, 64, 128, 129, 384, 1536,
        ] {
            let a = pseudo(dim, 0x1234 ^ dim as u64);
            let b = pseudo(dim, 0x9ABC ^ dim as u64);
            let ab = to_bytes(&a);
            let bb = to_bytes(&b);

            let want_l2 = l2_sq_scalar(&a, &b) as f64;
            for got in [
                l2_sq(&a, &b) as f64,
                ordering_distance_bytes(&ab, &bb, Metric::L2),
                ordering_distance_f32_bytes(&a, &bb, Metric::L2),
            ] {
                assert!(
                    (got - want_l2).abs() <= 1e-3 * want_l2.abs().max(1.0),
                    "l2 dim={dim}: got={got} scalar={want_l2}"
                );
            }

            let want_dot = dot_scalar(&a, &b);
            for got in [dot(&a, &b), dot_bytes(&ab, &bb)] {
                assert!(
                    (got - want_dot).abs() <= 1e-3 * want_dot.abs().max(1.0),
                    "dot dim={dim}: got={got} scalar={want_dot}"
                );
            }

            let (d, na, nb) = cosine_parts_scalar(&a, &b);
            let want_cos = finish_cosine(d, na, nb);
            assert!(
                (cosine_similarity(&a, &b) - want_cos).abs() <= 1e-3,
                "cosine dim={dim}"
            );
            // Cosine via the byte ordering path: 1 - sim.
            let got_cos_dist = ordering_distance_bytes(&ab, &bb, Metric::Cosine);
            assert!(
                (got_cos_dist - (1.0 - want_cos) as f64).abs() <= 1e-3,
                "cosine-bytes dim={dim}"
            );
            let got_mixed_cos = ordering_distance_f32_bytes(&a, &bb, Metric::Cosine);
            assert!(
                (got_mixed_cos - (1.0 - want_cos) as f64).abs() <= 1e-3,
                "cosine-mixed dim={dim}"
            );
            let got_mixed_dot = ordering_distance_f32_bytes(&a, &bb, Metric::Dot);
            assert!(
                (got_mixed_dot + want_dot as f64).abs() <= 1e-3 * (want_dot as f64).abs().max(1.0),
                "dot-mixed dim={dim}"
            );
        }
    }

    #[test]
    fn sme_batch_matches_neon_oracle_when_available() {
        if !brute_sme_available() {
            return;
        }
        for &dim in &[1usize, 15, 16, 17, 127, 128, 768, 769] {
            let q = pseudo(dim, 0x5151 ^ dim as u64);
            let rows_f = pseudo(dim * 37, 0xA5A5 ^ dim as u64);
            let qb = to_bytes(&q);
            let rb = to_bytes(&rows_f);
            let mut got = vec![0.0f32; 37];
            assert!(l2_sq_sme_batch(&qb, &rb, &mut got));
            for (row, &actual) in rows_f.chunks_exact(dim).zip(&got) {
                let want = l2_sq(&q, row);
                assert!(
                    (actual - want).abs() <= 2e-3 * want.abs().max(1.0),
                    "SME L2 dim={dim}: got={actual}, NEON={want}"
                );
            }
        }
    }

    /// The int8 SSD SIMD kernel must match its scalar oracle exactly (integer
    /// arithmetic, so no tolerance), across every length residue class —
    /// including ramps that exercise the saturating `±127` clamp.
    #[test]
    fn ssd_i8_matches_scalar_oracle() {
        // SAFETY helper: scalar reference over slices.
        fn ssd_scalar(a: &[i8], b: &[i8]) -> i32 {
            a.iter()
                .zip(b)
                .map(|(&x, &y)| (x as i32 - y as i32).pow(2))
                .sum()
        }
        for &dim in &[
            1usize, 3, 4, 7, 8, 15, 16, 17, 31, 33, 64, 128, 129, 384, 768, 1536,
        ] {
            // Quantize two pseudo-random vectors and compare kernels.
            let a = pseudo(dim, 0x55 ^ dim as u64);
            let b = pseudo(dim, 0xAA ^ dim as u64);
            let mut qa = vec![0i8; dim];
            let mut qb = vec![0i8; dim];
            // mid=0, inv_scale=127 maps [-1,1] → full int8 range, exercising the clamp.
            quantize_into(&a, 0.0, 127.0, &mut qa);
            quantize_into(&b, 0.0, 127.0, &mut qb);
            let want = ssd_scalar(&qa, &qb);
            assert_eq!(l2_ssd_i8(&qa, &qb), want, "ssd dim={dim}");
            // Identity → 0.
            assert_eq!(l2_ssd_i8(&qa, &qa), 0, "ssd self dim={dim}");
            // Norm-decomposed SSD (sdot path) must be bit-identical: the very
            // identity probe_dist relies on, `‖a‖²+‖b‖²−2·a·b == Σ(a−b)²`.
            let decomposed = ssq_i8(&qa) + ssq_i8(&qb) - 2 * dot_i8(&qa, &qb);
            assert_eq!(decomposed, want, "norm-decomposed ssd dim={dim}");
            // Dot kernel itself matches the scalar reference.
            let dot_ref: i32 = qa.iter().zip(&qb).map(|(&x, &y)| x as i32 * y as i32).sum();
            assert_eq!(dot_i8(&qa, &qb), dot_ref, "dot_i8 dim={dim}");
            assert_eq!(ssq_i8(&qa), dot_ref_self(&qa), "ssq_i8 dim={dim}");
        }
        fn dot_ref_self(a: &[i8]) -> i32 {
            a.iter().map(|&x| x as i32 * x as i32).sum()
        }
    }

    /// Quantized L2 ordering must agree with exact f32 L2 ordering on a small
    /// set — the property the HNSW graph relies on (navigate on codes, rank by
    /// f32). Checks the argmin matches.
    #[test]
    fn ssd_i8_preserves_nearest_ordering() {
        let dim = 128;
        let query = pseudo(dim, 1);
        let mut qq = vec![0i8; dim];
        quantize_into(&query, 0.0, 127.0, &mut qq);
        let mut best_i8 = (i32::MAX, usize::MAX);
        let mut best_f32 = (f32::INFINITY, usize::MAX);
        for cand in 0..50usize {
            let v = pseudo(dim, 100 + cand as u64);
            let mut qv = vec![0i8; dim];
            quantize_into(&v, 0.0, 127.0, &mut qv);
            let di8 = l2_ssd_i8(&qq, &qv);
            if di8 < best_i8.0 {
                best_i8 = (di8, cand);
            }
            let df = l2_sq_scalar(&query, &v);
            if df < best_f32.0 {
                best_f32 = (df, cand);
            }
        }
        assert_eq!(
            best_i8.1, best_f32.1,
            "quantized argmin must match f32 argmin"
        );
    }

    #[cfg(target_arch = "x86_64")]
    #[test]
    fn vnni_signed_dot_matches_scalar_extremes_tails_and_unaligned_rows() {
        if !(std::is_x86_feature_detected!("avx2")
            && std::is_x86_feature_detected!("avx512vnni")
            && std::is_x86_feature_detected!("avx512vl"))
        {
            return;
        }
        for n in [
            0usize, 1, 31, 32, 63, 64, 65, 127, 128, 129, 511, 768, 1536, 4096, 131071,
        ] {
            for (a, b) in [
                (-128i8, -128i8),
                (-128, 127),
                (127, 127),
                (0, -128),
                (-1, 1),
            ] {
                let va = vec![a; n + 1];
                let vb = vec![b; n + 3];
                let expected = (n as i64 * a as i64 * b as i64) as i32;
                assert_eq!(
                    unsafe { super::dot_i8_vnni(va.as_ptr().add(1), vb.as_ptr().add(3), n) },
                    expected
                );
            }
            let a: Vec<i8> = (0..n + 1)
                .map(|i| (i.wrapping_mul(37).wrapping_add(11) as u8) as i8)
                .collect();
            let b: Vec<i8> = (0..n + 3)
                .map(|i| (i.wrapping_mul(61).wrapping_add(93) as u8) as i8)
                .collect();
            let expected = a[1..]
                .iter()
                .zip(&b[3..])
                .map(|(&a, &b)| a as i64 * b as i64)
                .sum::<i64>() as i32;
            assert_eq!(
                unsafe { super::dot_i8_vnni(a.as_ptr().add(1), b.as_ptr().add(3), n) },
                expected
            );
            assert_eq!(super::dot_i8(&a[1..], &b[3..]), expected);
        }
    }

    #[test]
    fn dot_i8_batch4_ptrs_matches_independent_rows() {
        for &dim in &[1usize, 31, 32, 33, 128, 768] {
            let query_f = pseudo(dim, 0xC001 + dim as u64);
            let mut query = vec![0i8; dim];
            quantize_into(&query_f, 0.0, 127.0, &mut query);
            let rows: Vec<Vec<i8>> = (0..4u64)
                .map(|row| {
                    let values = pseudo(dim, 0xD000 + row * 19 + dim as u64);
                    let mut codes = vec![0i8; dim];
                    quantize_into(&values, 0.0, 127.0, &mut codes);
                    codes
                })
                .collect();
            let expected = [
                dot_i8(&query, &rows[0]),
                dot_i8(&query, &rows[1]),
                dot_i8(&query, &rows[2]),
                dot_i8(&query, &rows[3]),
            ];
            let got = dot_i8_batch4_ptrs(&query, [&rows[0], &rows[1], &rows[2], &rows[3]]);
            assert_eq!(got, expected, "pointer batch dot dim={dim}");
        }
    }

    #[test]
    fn l2_batch4_preserves_single_row_accumulation() {
        for dim in [
            0, 1, 7, 8, 15, 16, 17, 31, 32, 33, 63, 65, 128, 767, 768, 769,
        ] {
            let query: Vec<_> = (0..dim)
                .map(|i| ((i * 17 % 103) as f32 - 50.0) / 13.0)
                .collect();
            let rows: Vec<_> = (0..dim * 4)
                .map(|i| ((i * 29 % 173) as f32 - 80.0) / 19.0)
                .collect();
            for n in [0, dim / 2, dim] {
                let got = l2_sq_batch4(&query[..n], &rows, dim);
                for j in 0..4 {
                    assert_eq!(
                        got[j].to_bits(),
                        l2_sq(&query[..n], &rows[j * dim..(j + 1) * dim]).to_bits(),
                        "dim={dim} n={n} row={j}"
                    );
                }
            }
        }
    }

    #[test]
    fn l2_sq_zero_for_identical() {
        let a = pseudo(128, 42);
        assert_eq!(l2_sq(&a, &a), 0.0);
        let ab = to_bytes(&a);
        assert_eq!(ordering_distance_bytes(&ab, &ab, Metric::L2), 0.0);
    }

    /// Unaligned byte offset must still produce the right answer (the byte
    /// entry points promise alignment-independence).
    #[test]
    fn byte_kernels_handle_unaligned() {
        let a = pseudo(128, 7);
        let b = pseudo(128, 9);
        let want = l2_sq_scalar(&a, &b);
        // Shift both vectors by 1 byte inside a larger buffer so `as_ptr()` is
        // not 4-aligned.
        let mut ab = vec![0u8; 1];
        ab.extend_from_slice(&to_bytes(&a));
        let mut bb = vec![0u8; 1];
        bb.extend_from_slice(&to_bytes(&b));
        let got = ordering_distance_bytes(&ab[1..], &bb[1..], Metric::L2);
        assert!((got - want as f64).abs() <= 1e-3 * (want as f64).abs().max(1.0));
    }
}

/// Exact scoring of native payloads, with no per-candidate allocation.
pub(crate) fn typed_ordering_distance<
    A: crate::vector::VectorInput + ?Sized,
    B: crate::vector::VectorInput + ?Sized,
>(
    a: &A,
    b: &B,
    metric: Metric,
) -> Result<f64, crate::types::QueryError> {
    use super::codec::vector_err;
    use crate::vector::VectorDataType as D;
    if a.dimension() != b.dimension() {
        return Err(vector_err("VECTOR dimension mismatch"));
    }
    let binary = a.datatype() == D::Binary || b.datatype() == D::Binary;
    if binary {
        if a.datatype() != D::Binary
            || b.datatype() != D::Binary
            || !matches!(metric, Metric::Hamming | Metric::Jaccard)
        {
            return Err(vector_err(
                "BINARY vectors require HAMMING or JACCARD and two BINARY operands",
            ));
        }
        let mut different = 0u64;
        let mut intersection = 0u64;
        let mut union = 0u64;
        for (x, y) in a.payload().chunks(8).zip(b.payload().chunks(8)) {
            let mut xb = [0; 8];
            let mut yb = [0; 8];
            xb[..x.len()].copy_from_slice(x);
            yb[..y.len()].copy_from_slice(y);
            let x = u64::from_le_bytes(xb);
            let y = u64::from_le_bytes(yb);
            if metric == Metric::Hamming {
                different += (x ^ y).count_ones() as u64;
            } else {
                intersection += (x & y).count_ones() as u64;
                union += (x | y).count_ones() as u64;
            }
        }
        return Ok(if metric == Metric::Hamming {
            different as f64
        } else if union == 0 {
            0.0
        } else {
            1.0 - intersection as f64 / union as f64
        });
    }
    if matches!(metric, Metric::Hamming | Metric::Jaccard) {
        return Err(vector_err("HAMMING/JACCARD require BINARY vectors"));
    }
    if a.datatype() == D::Float32 && b.datatype() == D::Float32 {
        return Ok(ordering_distance_bytes(a.payload(), b.payload(), metric));
    }
    if a.datatype() == D::Int8 && b.datatype() == D::Int8 {
        let (mut l2, mut dot, mut na, mut nb) = (0i64, 0i64, 0i64, 0i64);
        for (&x, &y) in a.payload().iter().zip(b.payload()) {
            let x = x as i8 as i64;
            let y = y as i8 as i64;
            match metric {
                Metric::L2 => l2 += (x - y) * (x - y),
                Metric::Dot => dot += x * y,
                Metric::Cosine => {
                    dot += x * y;
                    na += x * x;
                    nb += y * y;
                }
                _ => unreachable!(),
            }
        }
        return Ok(match metric {
            Metric::L2 => l2 as f64,
            Metric::Dot => -(dot as f64),
            Metric::Cosine => {
                if na == 0 || nb == 0 {
                    f64::NAN
                } else {
                    1.0 - ((dot as f64) / ((na as f64).sqrt() * (nb as f64).sqrt()))
                        .clamp(-1.0, 1.0)
                }
            }
            _ => unreachable!(),
        });
    }
    let (mut sum, mut na, mut nb) = (0.0f64, 0.0f64, 0.0f64);
    let mut left = [0.0f32; 64];
    let mut right = [0.0f32; 64];
    for start in (0..a.dimension() as usize).step_by(64) {
        let length = (a.dimension() as usize - start).min(64);
        decode_native_block(a, start, &mut left[..length]);
        decode_native_block(b, start, &mut right[..length]);
        let l = &left[..length];
        let r = &right[..length];
        match metric {
            Metric::L2 => sum += ordering_distance(l, r, Metric::L2),
            Metric::Dot => sum += ordering_distance(l, r, Metric::Dot),
            Metric::Cosine => {
                sum -= ordering_distance(l, r, Metric::Dot);
                na -= ordering_distance(l, l, Metric::Dot);
                nb -= ordering_distance(r, r, Metric::Dot);
            }
            _ => unreachable!(),
        }
    }
    Ok(match metric {
        Metric::L2 | Metric::Dot => sum,
        Metric::Cosine => {
            if na == 0.0 || nb == 0.0 {
                f64::NAN
            } else {
                1.0 - (sum / (na.sqrt() * nb.sqrt())).clamp(-1.0, 1.0)
            }
        }
        _ => unreachable!(),
    })
}

fn decode_native_block<V: crate::vector::VectorInput + ?Sized>(
    value: &V,
    start: usize,
    out: &mut [f32],
) {
    use crate::vector::VectorDataType as D;
    use half::slice::{HalfBitsSliceExt, HalfFloatSliceExt};
    match value.datatype() {
        D::Float16 | D::BFloat16 => {
            let mut words = [0u16; 64];
            for (word, bytes) in words
                .iter_mut()
                .zip(value.payload()[start * 2..(start + out.len()) * 2].chunks_exact(2))
            {
                *word = u16::from_le_bytes(bytes.try_into().unwrap());
            }
            let words = &words[..out.len()];
            if value.datatype() == D::Float16 {
                words
                    .reinterpret_cast::<half::f16>()
                    .convert_to_f32_slice(out);
            } else {
                words
                    .reinterpret_cast::<half::bf16>()
                    .convert_to_f32_slice(out);
            }
        }
        D::Float32 => {
            for (x, bytes) in out
                .iter_mut()
                .zip(value.payload()[start * 4..].chunks_exact(4))
            {
                *x = f32::from_le_bytes(bytes.try_into().unwrap());
            }
        }
        D::Int8 => {
            for (x, &v) in out.iter_mut().zip(&value.payload()[start..]) {
                *x = (v as i8) as f32;
            }
        }
        D::Binary => unreachable!("binary distances use packed kernels"),
    }
}

/// Domain-independent zero detection does not underflow on floating subnormals.
pub(crate) fn typed_is_zero(value: &crate::vector::VectorValue) -> bool {
    use crate::vector::VectorDataType as D;
    match value.datatype {
        D::Float32 => is_zero_norm_bytes(&value.payload),
        D::Float16 | D::BFloat16 => value
            .payload
            .chunks_exact(2)
            .all(|v| u16::from_le_bytes(v.try_into().unwrap()) & 0x7fff == 0),
        D::Int8 | D::Binary => value.payload.iter().all(|&v| v == 0),
    }
}
