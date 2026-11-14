//! Blocked coarse-quantizer scoring for IVF.
//!
//! Assigning rows to their nearest centroid is a dense `rows × centroids` dot
//! product (k-means assign, the full-table list assignment at CREATE INDEX,
//! single-row DML and per-query probe ranking). Every metric reduces to an
//! affine function of the dot product once per-centroid terms are cached:
//!
//! * L2:     `‖c‖² − 2·x·c`   (`‖x‖²` is constant per row)
//! * COSINE: `−x·c / ‖c‖`     (`‖x‖` is constant per row)
//! * DOT:    `−x·c`
//!
//! so one register-tiled dot kernel (AVX-512F / AVX2+FMA / NEON / scalar,
//! selected at run time) serves them all. The keys only *rank* centroids; any
//! caller that needs an authoritative distance keeps the exact scorers. Rows or
//! centroids in exceptional domains (non-finite, near the f32 overflow or
//! underflow limits) are reported as `None` / [`EXACT`] so callers fall back to
//! the established wide-precision path.
use super::distance::Metric;
use std::marker::PhantomData;
use std::ptr::read_unaligned;
use std::sync::atomic::{AtomicU8, Ordering};

/// Assignment marker for a row that must be scored by the exact path.
pub(crate) const EXACT: u32 = u32::MAX;

/// Rows per cache block: their payloads stay L1/L2 resident while a centroid
/// block streams past.
const ROW_BLOCK: usize = 32;
/// Centroids per cache block (64 × 768 × 4 B ≈ 192 KiB, L2 resident).
const CENTER_BLOCK: usize = 64;
/// Magnitude window for the f32 key path. Squared norms outside it either
/// overflow the f32 dot accumulator or lose ordering precision to subnormals.
const MIN_NORM_SQUARED: f32 = 1e-30;
const MAX_NORM_SQUARED: f32 = 1e30;
/// Centroids per AVX-512 panel: one zmm register of f32 lanes.
const PANEL: usize = 16;
/// Rows per panel micro-kernel step (6 rows × 2 panels = 12 accumulators).
const PANEL_ROWS: usize = 6;
/// Rows per panel cache block (48 × 768 × 4 B ≈ 144 KiB).
const PANEL_ROW_BLOCK: usize = 48;
/// Panels per cache block (8 × 16 × 768 × 4 B ≈ 384 KiB, L2 resident).
const PANEL_BLOCK: usize = 8;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Isa {
    Scalar = 1,
    #[cfg(target_arch = "x86_64")]
    Avx2 = 2,
    #[cfg(target_arch = "x86_64")]
    Avx512 = 3,
    #[cfg(target_arch = "aarch64")]
    Neon = 4,
}

impl Isa {
    /// The widest kernel this CPU supports, detected once.
    pub(crate) fn detected() -> Isa {
        static CACHED: AtomicU8 = AtomicU8::new(0);
        match CACHED.load(Ordering::Relaxed) {
            0 => {
                let isa = Self::detect();
                CACHED.store(isa as u8, Ordering::Relaxed);
                isa
            }
            value => Self::from_u8(value),
        }
    }

    fn detect() -> Isa {
        #[cfg(target_arch = "x86_64")]
        {
            if std::is_x86_feature_detected!("avx512f") {
                return Isa::Avx512;
            }
            if std::is_x86_feature_detected!("avx2") && std::is_x86_feature_detected!("fma") {
                return Isa::Avx2;
            }
        }
        #[cfg(target_arch = "aarch64")]
        {
            return Isa::Neon;
        }
        #[allow(unreachable_code)]
        Isa::Scalar
    }

    fn from_u8(value: u8) -> Isa {
        match value {
            #[cfg(target_arch = "x86_64")]
            2 => Isa::Avx2,
            #[cfg(target_arch = "x86_64")]
            3 => Isa::Avx512,
            #[cfg(target_arch = "aarch64")]
            4 => Isa::Neon,
            _ => Isa::Scalar,
        }
    }

    /// Every kernel this CPU can execute, narrowest first (test oracle sweep).
    #[cfg(test)]
    pub(crate) fn supported() -> Vec<Isa> {
        let mut out = vec![Isa::Scalar];
        #[cfg(target_arch = "x86_64")]
        {
            if std::is_x86_feature_detected!("avx2") && std::is_x86_feature_detected!("fma") {
                out.push(Isa::Avx2);
            }
            if std::is_x86_feature_detected!("avx512f") {
                out.push(Isa::Avx512);
            }
        }
        #[cfg(target_arch = "aarch64")]
        out.push(Isa::Neon);
        out
    }
}

/// A borrowed row of exactly `dim` little-endian f32 values, possibly
/// unaligned (heap and page payloads are byte slices).
#[derive(Clone, Copy)]
pub(crate) struct RowRef<'a> {
    ptr: *const f32,
    _borrow: PhantomData<&'a [u8]>,
}

// SAFETY: a RowRef is an immutable borrow of plain f32 data.
unsafe impl Send for RowRef<'_> {}
unsafe impl Sync for RowRef<'_> {}

impl<'a> RowRef<'a> {
    pub(crate) fn from_f32(row: &'a [f32]) -> Self {
        Self {
            ptr: row.as_ptr(),
            _borrow: PhantomData,
        }
    }

    #[cfg(target_endian = "little")]
    pub(crate) fn from_le_bytes(row: &'a [u8]) -> Self {
        Self {
            ptr: row.as_ptr().cast(),
            _borrow: PhantomData,
        }
    }
}

/// Dot products of four rows against four centroids (`out[row][centroid]`).
///
/// # Safety
/// Every pointer must address `n` readable f32 values (alignment not required),
/// and `isa` must be supported by the running CPU.
#[inline]
unsafe fn dot_tile(
    isa: Isa,
    rows: [*const f32; 4],
    centers: [*const f32; 4],
    n: usize,
) -> [[f32; 4]; 4] {
    match isa {
        #[cfg(target_arch = "x86_64")]
        Isa::Avx512 => dot_tile_avx512(rows, centers, n),
        #[cfg(target_arch = "x86_64")]
        Isa::Avx2 => {
            let low = dot_tile2_avx2([rows[0], rows[1]], centers, n);
            let high = dot_tile2_avx2([rows[2], rows[3]], centers, n);
            [low[0], low[1], high[0], high[1]]
        }
        #[cfg(target_arch = "aarch64")]
        Isa::Neon => dot_tile_neon(rows, centers, n),
        Isa::Scalar => dot_tile_scalar(rows, centers, n),
    }
}

/// Dot products of one row against four centroids.
///
/// # Safety
/// As [`dot_tile`].
#[inline]
unsafe fn dot_row4(isa: Isa, row: *const f32, centers: [*const f32; 4], n: usize) -> [f32; 4] {
    match isa {
        #[cfg(target_arch = "x86_64")]
        Isa::Avx512 => dot_row4_avx512(row, centers, n),
        #[cfg(target_arch = "x86_64")]
        Isa::Avx2 => dot_tile2_avx2([row, row], centers, n)[0],
        #[cfg(target_arch = "aarch64")]
        Isa::Neon => dot_tile_neon([row; 4], centers, n)[0],
        Isa::Scalar => dot_tile_scalar([row; 4], centers, n)[0],
    }
}

unsafe fn dot_tile_scalar(
    rows: [*const f32; 4],
    centers: [*const f32; 4],
    n: usize,
) -> [[f32; 4]; 4] {
    let mut out = [[0.0f32; 4]; 4];
    for i in 0..n {
        let c = centers.map(|p| read_unaligned(p.add(i)));
        for (row, sums) in rows.iter().zip(out.iter_mut()) {
            let x = read_unaligned(row.add(i));
            for (sum, c) in sums.iter_mut().zip(c) {
                *sum += x * c;
            }
        }
    }
    out
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx512f")]
unsafe fn dot_tile_avx512(
    rows: [*const f32; 4],
    centers: [*const f32; 4],
    n: usize,
) -> [[f32; 4]; 4] {
    use std::arch::x86_64::*;
    // 16 accumulators + 4 row registers + 1 centroid register: fits in the
    // 32 zmm registers, so each centroid load feeds four FMAs.
    let mut acc = [[_mm512_setzero_ps(); 4]; 4];
    let mut i = 0usize;
    while i + 16 <= n {
        let x = rows.map(|row| _mm512_loadu_ps(row.add(i)));
        for (b, center) in centers.iter().enumerate() {
            let c = _mm512_loadu_ps(center.add(i));
            for a in 0..4 {
                acc[a][b] = _mm512_fmadd_ps(x[a], c, acc[a][b]);
            }
        }
        i += 16;
    }
    if i < n {
        let mask: __mmask16 = ((1u32 << (n - i)) - 1) as u16;
        let x = rows.map(|row| _mm512_maskz_loadu_ps(mask, row.add(i)));
        for (b, center) in centers.iter().enumerate() {
            let c = _mm512_maskz_loadu_ps(mask, center.add(i));
            for a in 0..4 {
                acc[a][b] = _mm512_fmadd_ps(x[a], c, acc[a][b]);
            }
        }
    }
    acc.map(|sums| sums.map(|sum| _mm512_reduce_add_ps(sum)))
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx512f")]
unsafe fn dot_row4_avx512(row: *const f32, centers: [*const f32; 4], n: usize) -> [f32; 4] {
    use std::arch::x86_64::*;
    // Two accumulators per centroid hide the FMA latency of a single row.
    let mut acc = [[_mm512_setzero_ps(); 2]; 4];
    let mut i = 0usize;
    while i + 32 <= n {
        let x0 = _mm512_loadu_ps(row.add(i));
        let x1 = _mm512_loadu_ps(row.add(i + 16));
        for (b, center) in centers.iter().enumerate() {
            acc[b][0] = _mm512_fmadd_ps(x0, _mm512_loadu_ps(center.add(i)), acc[b][0]);
            acc[b][1] = _mm512_fmadd_ps(x1, _mm512_loadu_ps(center.add(i + 16)), acc[b][1]);
        }
        i += 32;
    }
    while i + 16 <= n {
        let x = _mm512_loadu_ps(row.add(i));
        for (b, center) in centers.iter().enumerate() {
            acc[b][0] = _mm512_fmadd_ps(x, _mm512_loadu_ps(center.add(i)), acc[b][0]);
        }
        i += 16;
    }
    if i < n {
        let mask: __mmask16 = ((1u32 << (n - i)) - 1) as u16;
        let x = _mm512_maskz_loadu_ps(mask, row.add(i));
        for (b, center) in centers.iter().enumerate() {
            acc[b][1] = _mm512_fmadd_ps(x, _mm512_maskz_loadu_ps(mask, center.add(i)), acc[b][1]);
        }
    }
    acc.map(|sums| _mm512_reduce_add_ps(_mm512_add_ps(sums[0], sums[1])))
}

/// Two rows × four centroids: 8 accumulators + 2 row + 1 centroid registers
/// stay within AVX2's 16 ymm registers.
#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2,fma")]
unsafe fn dot_tile2_avx2(
    rows: [*const f32; 2],
    centers: [*const f32; 4],
    n: usize,
) -> [[f32; 4]; 2] {
    use std::arch::x86_64::*;
    let mut acc = [[_mm256_setzero_ps(); 4]; 2];
    let mut i = 0usize;
    while i + 8 <= n {
        let x = rows.map(|row| _mm256_loadu_ps(row.add(i)));
        for (b, center) in centers.iter().enumerate() {
            let c = _mm256_loadu_ps(center.add(i));
            for a in 0..2 {
                acc[a][b] = _mm256_fmadd_ps(x[a], c, acc[a][b]);
            }
        }
        i += 8;
    }
    let mut out = acc.map(|sums| sums.map(|sum| hsum256(sum)));
    while i < n {
        let c = centers.map(|p| read_unaligned(p.add(i)));
        for (row, sums) in rows.iter().zip(out.iter_mut()) {
            let x = read_unaligned(row.add(i));
            for (sum, c) in sums.iter_mut().zip(c) {
                *sum += x * c;
            }
        }
        i += 1;
    }
    out
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2")]
#[inline]
unsafe fn hsum256(v: std::arch::x86_64::__m256) -> f32 {
    use std::arch::x86_64::*;
    let sum = _mm_add_ps(_mm256_castps256_ps128(v), _mm256_extractf128_ps(v, 1));
    let sum = _mm_add_ps(sum, _mm_movehl_ps(sum, sum));
    _mm_cvtss_f32(_mm_add_ss(sum, _mm_movehdup_ps(sum)))
}

#[cfg(target_arch = "aarch64")]
unsafe fn dot_tile_neon(
    rows: [*const f32; 4],
    centers: [*const f32; 4],
    n: usize,
) -> [[f32; 4]; 4] {
    use std::arch::aarch64::*;
    let mut acc = [[vdupq_n_f32(0.0); 4]; 4];
    let mut i = 0usize;
    while i + 4 <= n {
        let x = rows.map(|row| vld1q_f32(row.add(i)));
        for (b, center) in centers.iter().enumerate() {
            let c = vld1q_f32(center.add(i));
            for a in 0..4 {
                acc[a][b] = vfmaq_f32(acc[a][b], x[a], c);
            }
        }
        i += 4;
    }
    let mut out = acc.map(|sums| sums.map(|sum| vaddvq_f32(sum)));
    while i < n {
        let c = centers.map(|p| read_unaligned(p.add(i)));
        for (row, sums) in rows.iter().zip(out.iter_mut()) {
            let x = read_unaligned(row.add(i));
            for (sum, c) in sums.iter_mut().zip(c) {
                *sum += x * c;
            }
        }
        i += 1;
    }
    out
}

/// Sum of squares with eight independent lanes (only used to gate the f32
/// key path, so its rounding never reaches a result).
unsafe fn squared_norm(row: *const f32, n: usize) -> f32 {
    let mut lanes = [0.0f32; 8];
    let mut i = 0usize;
    while i + 8 <= n {
        for (lane, sum) in lanes.iter_mut().enumerate() {
            let x = read_unaligned(row.add(i + lane));
            *sum += x * x;
        }
        i += 8;
    }
    while i < n {
        let x = read_unaligned(row.add(i));
        lanes[0] += x * x;
        i += 1;
    }
    lanes.iter().sum()
}

fn in_key_domain(squared: f32) -> bool {
    squared.is_finite() && (MIN_NORM_SQUARED..=MAX_NORM_SQUARED).contains(&squared)
}

/// Centroids prepared for blocked nearest-centroid ranking.
#[derive(Debug)]
pub(crate) struct PreparedCentroids {
    dim: usize,
    count: usize,
    data: Vec<f32>,
    bias: Vec<f32>,
    scale: Vec<f32>,
    /// AVX-512 GEMM layout (built only when that kernel is in use).
    panels: Option<Panels>,
}

/// Centroids packed for the AVX-512 broadcast micro-kernel: each panel holds
/// 16 centroids dimension-major (`data[(panel·dim + k)·16 + lane]`), so one
/// aligned load yields coordinate `k` of 16 centroids and the inner loop needs
/// no horizontal reductions. Padding lanes (and the padding panel that makes
/// the count even) carry `bias = +∞, scale = 0`, so they never win.
#[derive(Debug)]
struct Panels {
    data: Vec<f32>,
    bias: Vec<f32>,
    scale: Vec<f32>,
    count: usize,
}

impl Panels {
    fn new(data: &[f32], bias: &[f32], scale: &[f32], dim: usize) -> Self {
        let centers = bias.len();
        let count = centers.div_ceil(PANEL).next_multiple_of(2);
        let mut packed = vec![0.0f32; count * dim * PANEL];
        let mut padded_bias = vec![f32::INFINITY; count * PANEL];
        let mut padded_scale = vec![0.0f32; count * PANEL];
        for c in 0..centers {
            let (panel, lane) = (c / PANEL, c % PANEL);
            for k in 0..dim {
                packed[(panel * dim + k) * PANEL + lane] = data[c * dim + k];
            }
            padded_bias[c] = bias[c];
            padded_scale[c] = scale[c];
        }
        Self {
            data: packed,
            bias: padded_bias,
            scale: padded_scale,
            count,
        }
    }

    fn bytes_for(count: usize, dim: usize) -> usize {
        count
            .div_ceil(PANEL)
            .next_multiple_of(2)
            .saturating_mul(PANEL)
            .saturating_mul(dim.saturating_add(2))
            .saturating_mul(std::mem::size_of::<f32>())
    }
}

impl PreparedCentroids {
    /// Prepare `data.len() / dim` row-major centroids for `metric`. Returns
    /// `None` when the metric is not numeric or any centroid lies outside the
    /// f32 key domain; callers then keep exact scoring for every row.
    pub(crate) fn new(data: Vec<f32>, dim: usize, metric: Metric) -> Option<Self> {
        if dim == 0
            || data.is_empty()
            || !data.len().is_multiple_of(dim)
            || data.len() / dim >= EXACT as usize
        {
            return None;
        }
        let count = data.len() / dim;
        let mut bias = Vec::with_capacity(count);
        let mut scale = Vec::with_capacity(count);
        for center in data.chunks_exact(dim) {
            // SAFETY: `center` holds `dim` f32 values.
            let squared = unsafe { squared_norm(center.as_ptr(), dim) };
            if !in_key_domain(squared) || center.iter().any(|x| !x.is_finite()) {
                return None;
            }
            match metric {
                Metric::L2 => {
                    bias.push(squared);
                    scale.push(-2.0);
                }
                Metric::Cosine => {
                    bias.push(0.0);
                    scale.push(-1.0 / squared.sqrt());
                }
                Metric::Dot => {
                    bias.push(0.0);
                    scale.push(-1.0);
                }
                Metric::Hamming | Metric::Jaccard => return None,
            }
        }
        #[cfg(target_arch = "x86_64")]
        let panels =
            (Isa::detected() == Isa::Avx512).then(|| Panels::new(&data, &bias, &scale, dim));
        #[cfg(not(target_arch = "x86_64"))]
        let panels = None;
        Some(Self {
            dim,
            count,
            data,
            bias,
            scale,
            panels,
        })
    }

    /// Bytes a preparation of `count × dim` centroids allocates (upper bound).
    pub(crate) fn bytes_for(count: usize, dim: usize) -> usize {
        count
            .saturating_mul(dim.saturating_add(2))
            .saturating_mul(std::mem::size_of::<f32>())
            .saturating_add(Panels::bytes_for(count, dim))
    }

    #[inline]
    fn center(&self, c: usize) -> *const f32 {
        // SAFETY: c < count, so the offset stays within `data`.
        unsafe { self.data.as_ptr().add(c * self.dim) }
    }

    /// Borrow a row of `dim` f32 values; `None` for a length mismatch.
    pub(crate) fn row_f32<'a>(&self, row: &'a [f32]) -> Option<RowRef<'a>> {
        (row.len() == self.dim).then(|| RowRef::from_f32(row))
    }

    /// Borrow a little-endian FLOAT32 payload; `None` for a length mismatch.
    pub(crate) fn row_bytes<'a>(&self, row: &'a [u8]) -> Option<RowRef<'a>> {
        #[cfg(target_endian = "little")]
        {
            (row.len() == self.dim * 4).then(|| RowRef::from_le_bytes(row))
        }
        #[cfg(target_endian = "big")]
        {
            let _ = row;
            None
        }
    }

    fn row_in_domain(&self, row: RowRef<'_>) -> bool {
        // SAFETY: RowRef construction proved `dim` readable values.
        in_key_domain(unsafe { squared_norm(row.ptr, self.dim) })
    }

    /// Nearest centroid of one row, or `None` when the row needs exact scoring.
    /// Identical to that row's [`Self::assign`] result.
    pub(crate) fn nearest(&self, row: RowRef<'_>) -> Option<usize> {
        self.nearest_with(Isa::detected(), row)
    }

    fn nearest_with(&self, isa: Isa, row: RowRef<'_>) -> Option<usize> {
        // The batch kernel reduces each row independently of its tile
        // neighbours, so a single-row call reproduces the batched assignment
        // bit for bit (DML inserts agree with the bulk build).
        let mut out = [EXACT];
        self.assign_with(isa, &[row], &mut out);
        (out[0] != EXACT).then_some(out[0] as usize)
    }

    /// Ranking keys of every centroid for one row (smaller is nearer), or
    /// `false` when the row needs exact scoring.
    pub(crate) fn keys(&self, row: RowRef<'_>, out: &mut Vec<f32>) -> bool {
        out.clear();
        if !self.row_in_domain(row) {
            return false;
        }
        let isa = Isa::detected();
        let mut c = 0usize;
        while c < self.count {
            let width = (self.count - c).min(4);
            let centers = std::array::from_fn(|b| self.center(c + b.min(width - 1)));
            // SAFETY: as in `nearest_with`.
            let dots = unsafe { dot_row4(isa, row.ptr, centers, self.dim) };
            for (b, dot) in dots.iter().enumerate().take(width) {
                let key = self.bias[c + b] + self.scale[c + b] * dot;
                if !key.is_finite() {
                    out.clear();
                    return false;
                }
                out.push(key);
            }
            c += 4;
        }
        true
    }

    /// Assign each row to its nearest centroid; rows needing exact scoring get
    /// [`EXACT`]. Ties resolve to the lowest centroid index, as the exact path.
    pub(crate) fn assign(&self, rows: &[RowRef<'_>], out: &mut [u32]) {
        self.assign_with(Isa::detected(), rows, out);
    }

    fn assign_with(&self, isa: Isa, rows: &[RowRef<'_>], out: &mut [u32]) {
        assert_eq!(rows.len(), out.len());
        #[cfg(target_arch = "x86_64")]
        if isa == Isa::Avx512 {
            if let Some(panels) = &self.panels {
                // SAFETY: Avx512 is only passed when the CPU supports it.
                return unsafe { self.assign_panels_avx512(panels, rows, out) };
            }
        }
        for (block, assigned) in rows.chunks(ROW_BLOCK).zip(out.chunks_mut(ROW_BLOCK)) {
            let mut best_key = [f32::INFINITY; ROW_BLOCK];
            let mut best = [EXACT; ROW_BLOCK];
            let valid: [bool; ROW_BLOCK] =
                std::array::from_fn(|r| r < block.len() && self.row_in_domain(block[r]));
            // Rows outside the key domain are still memory-safe to read; they
            // are scored like the others and discarded below.
            for start in (0..self.count).step_by(CENTER_BLOCK) {
                let end = (start + CENTER_BLOCK).min(self.count);
                for r in (0..block.len()).step_by(4) {
                    let height = (block.len() - r).min(4);
                    let row_ptrs = std::array::from_fn(|a| block[r + a.min(height - 1)].ptr);
                    let mut c = start;
                    while c < end {
                        let width = (end - c).min(4);
                        let centers = std::array::from_fn(|b| self.center(c + b.min(width - 1)));
                        // SAFETY: rows and centroids cover `dim` values; `isa`
                        // is supported.
                        let tile = unsafe { dot_tile(isa, row_ptrs, centers, self.dim) };
                        for (a, dots) in tile.iter().enumerate().take(height) {
                            for (b, dot) in dots.iter().enumerate().take(width) {
                                let key = self.bias[c + b] + self.scale[c + b] * dot;
                                if key < best_key[r + a] {
                                    best_key[r + a] = key;
                                    best[r + a] = (c + b) as u32;
                                }
                            }
                        }
                        c += 4;
                    }
                }
            }
            for (r, slot) in assigned.iter_mut().enumerate() {
                *slot = if valid[r] && best_key[r].is_finite() {
                    best[r]
                } else {
                    EXACT
                };
            }
        }
    }

    /// AVX-512 GEMM-style assignment over [`Panels`]: each step broadcasts
    /// one coordinate of six rows against two 16-centroid panels (twelve
    /// independent FMA chains, no horizontal reductions), and keeps a lane-wise
    /// running minimum. Rows and panels are blocked to stay L2 resident.
    #[cfg(target_arch = "x86_64")]
    #[target_feature(enable = "avx512f")]
    unsafe fn assign_panels_avx512(&self, panels: &Panels, rows: &[RowRef<'_>], out: &mut [u32]) {
        use std::arch::x86_64::*;
        let dim = self.dim;
        let lanes = _mm512_setr_epi32(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15);
        for (block, assigned) in rows
            .chunks(PANEL_ROW_BLOCK)
            .zip(out.chunks_mut(PANEL_ROW_BLOCK))
        {
            let mut best_key = [_mm512_set1_ps(f32::INFINITY); PANEL_ROW_BLOCK];
            let mut best_id = [_mm512_set1_epi32(EXACT as i32); PANEL_ROW_BLOCK];
            for first in (0..panels.count).step_by(PANEL_BLOCK) {
                let last = (first + PANEL_BLOCK).min(panels.count);
                for group in (0..block.len()).step_by(PANEL_ROWS) {
                    let height = (block.len() - group).min(PANEL_ROWS);
                    let row: [*const f32; PANEL_ROWS] =
                        std::array::from_fn(|r| block[group + r.min(height - 1)].ptr);
                    for p in (first..last).step_by(2) {
                        let p0 = panels.data.as_ptr().add(p * dim * PANEL);
                        let p1 = p0.add(dim * PANEL);
                        let mut acc = [[_mm512_setzero_ps(); 2]; PANEL_ROWS];
                        for k in 0..dim {
                            let c0 = _mm512_loadu_ps(p0.add(k * PANEL));
                            let c1 = _mm512_loadu_ps(p1.add(k * PANEL));
                            for r in 0..PANEL_ROWS {
                                let x = _mm512_set1_ps(read_unaligned(row[r].add(k)));
                                acc[r][0] = _mm512_fmadd_ps(x, c0, acc[r][0]);
                                acc[r][1] = _mm512_fmadd_ps(x, c1, acc[r][1]);
                            }
                        }
                        for (half, panel) in [p, p + 1].into_iter().enumerate() {
                            let bias = _mm512_loadu_ps(panels.bias.as_ptr().add(panel * PANEL));
                            let scale = _mm512_loadu_ps(panels.scale.as_ptr().add(panel * PANEL));
                            let ids =
                                _mm512_add_epi32(lanes, _mm512_set1_epi32((panel * PANEL) as i32));
                            for (r, sums) in acc.iter().enumerate().take(height) {
                                let key = _mm512_fmadd_ps(sums[half], scale, bias);
                                let slot = group + r;
                                let better = _mm512_cmp_ps_mask::<_CMP_LT_OQ>(key, best_key[slot]);
                                best_key[slot] = _mm512_mask_mov_ps(best_key[slot], better, key);
                                best_id[slot] = _mm512_mask_mov_epi32(best_id[slot], better, ids);
                            }
                        }
                    }
                }
            }
            for (r, slot) in assigned.iter_mut().enumerate() {
                let mut keys = [0.0f32; PANEL];
                let mut ids = [0u32; PANEL];
                _mm512_storeu_ps(keys.as_mut_ptr(), best_key[r]);
                _mm512_storeu_si512(ids.as_mut_ptr().cast(), best_id[r]);
                // Lowest key; among equal keys the lowest centroid id, as the
                // sequential scan would pick.
                let mut best = (f32::INFINITY, EXACT);
                for (&key, &id) in keys.iter().zip(&ids) {
                    if key < best.0 || (key == best.0 && id < best.1) {
                        best = (key, id);
                    }
                }
                *slot = if best.0.is_finite() && self.row_in_domain(block[r]) {
                    best.1
                } else {
                    EXACT
                };
            }
        }
    }

    /// [`Self::assign`] fanned out over `threads` disjoint row ranges.
    /// Deterministic: each row's result is independent of the split.
    pub(crate) fn assign_parallel(&self, rows: &[RowRef<'_>], out: &mut [u32], threads: usize) {
        assert_eq!(rows.len(), out.len());
        let threads = threads.clamp(1, rows.len().div_ceil(ROW_BLOCK).max(1));
        if threads <= 1 {
            return self.assign(rows, out);
        }
        let chunk = rows.len().div_ceil(threads).next_multiple_of(ROW_BLOCK);
        crate::util::thread_spawn::scope(|scope| {
            for (rows, out) in rows.chunks(chunk).zip(out.chunks_mut(chunk)) {
                scope.spawn(move || self.assign(rows, out));
            }
        });
    }
}

/// Worker count for a dense `rows × centroids × dim` assignment: serial for
/// small problems, else the core count. `MYDB_IVF_BUILD_THREADS` overrides
/// (`0`/`1` forces the serial path, the determinism oracle).
pub(crate) fn assignment_threads(rows: usize, centroids: usize, dim: usize) -> usize {
    if let Ok(v) = std::env::var("MYDB_IVF_BUILD_THREADS") {
        if let Ok(t) = v.parse::<usize>() {
            return t.max(1);
        }
    }
    if rows < 4096 || rows.saturating_mul(centroids).saturating_mul(dim) < 1 << 26 {
        return 1;
    }
    crate::util::thread_spawn::available_parallelism()
        .map(|c| c.get())
        .unwrap_or(1)
        .clamp(1, 64)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::vector::distance::ordering_distance;

    fn data(n: usize, dim: usize, seed: u64) -> Vec<f32> {
        let mut s = seed;
        (0..n * dim)
            .map(|_| {
                s = s
                    .wrapping_mul(6364136223846793005)
                    .wrapping_add(1442695040888963407);
                ((s >> 33) as f32 / (1u64 << 31) as f32) - 1.0
            })
            .collect()
    }

    fn reference_dot(a: &[f32], b: &[f32]) -> f64 {
        a.iter().zip(b).map(|(&x, &y)| x as f64 * y as f64).sum()
    }

    #[test]
    fn dot_kernels_match_f64_reference_for_every_isa_and_tail() {
        for dim in [1, 7, 8, 9, 15, 16, 17, 31, 32, 33, 65, 768, 1536] {
            // One extra element so the shifted (unaligned) views stay in bounds.
            let rows = data(4, dim + 1, 3);
            let centers = data(4, dim + 1, 11);
            for offset in [0, 1] {
                let row = |a: usize| unsafe { rows.as_ptr().add(a * (dim + 1) + offset) };
                let center = |b: usize| unsafe { centers.as_ptr().add(b * (dim + 1) + offset) };
                let slice = |v: &[f32], i: usize| v[i * (dim + 1) + offset..][..dim].to_vec();
                for isa in Isa::supported() {
                    let tile = unsafe {
                        dot_tile(
                            isa,
                            std::array::from_fn(row),
                            std::array::from_fn(center),
                            dim,
                        )
                    };
                    let single = unsafe { dot_row4(isa, row(2), std::array::from_fn(center), dim) };
                    for (a, dots) in tile.iter().enumerate() {
                        for (b, &dot) in dots.iter().enumerate() {
                            let want = reference_dot(&slice(&rows, a), &slice(&centers, b));
                            let tol = 1e-5 * (dim as f64).max(1.0);
                            assert!(
                                (dot as f64 - want).abs() <= tol,
                                "{isa:?} dim={dim} tile[{a}][{b}]"
                            );
                            if a == 2 {
                                assert!((single[b] as f64 - want).abs() <= tol, "{isa:?} row4");
                            }
                        }
                    }
                }
            }
        }
    }

    /// The blocked keys must pick a centroid whose exact distance equals the
    /// exact optimum up to f32 rounding, for every metric, ISA and shape.
    #[test]
    fn assignment_matches_exact_nearest_within_rounding() {
        for dim in [1, 3, 16, 31, 65, 768] {
            for count in [1, 3, 4, 5, 17, 70, 130] {
                let centers = data(count, dim, 7 + dim as u64);
                let rows = data(37, dim, 99 + count as u64);
                for metric in [Metric::L2, Metric::Cosine, Metric::Dot] {
                    let prepared = PreparedCentroids::new(centers.clone(), dim, metric).unwrap();
                    let refs: Vec<_> = rows
                        .chunks_exact(dim)
                        .map(|r| prepared.row_f32(r).unwrap())
                        .collect();
                    for isa in Isa::supported() {
                        let mut out = vec![0; refs.len()];
                        prepared.assign_with(isa, &refs, &mut out);
                        for (i, row) in rows.chunks_exact(dim).enumerate() {
                            let exact: Vec<f64> = centers
                                .chunks_exact(dim)
                                .map(|c| ordering_distance(row, c, metric))
                                .collect();
                            let best = exact.iter().cloned().fold(f64::INFINITY, f64::min);
                            let got = out[i] as usize;
                            assert!(got < count, "{isa:?} {metric:?} returned EXACT");
                            let tol = 1e-4 * best.abs().max(1.0);
                            assert!(exact[got] - best <= tol, "{isa:?} {metric:?} dim={dim}");
                            assert_eq!(
                                prepared
                                    .nearest_with(isa, refs[i])
                                    .map(|c| exact[c] - best <= tol),
                                Some(true)
                            );
                        }
                    }
                    let mut keys = Vec::new();
                    assert!(prepared.keys(refs[0], &mut keys));
                    assert_eq!(keys.len(), count);
                }
            }
        }
    }

    #[test]
    fn ties_resolve_to_lowest_index_and_parallel_matches_serial() {
        let dim = 24;
        let mut centers = data(9, dim, 5);
        let duplicate = centers[3 * dim..4 * dim].to_vec();
        centers[7 * dim..8 * dim].copy_from_slice(&duplicate);
        let prepared = PreparedCentroids::new(centers, dim, Metric::L2).unwrap();
        assert_eq!(
            prepared.nearest(prepared.row_f32(&duplicate).unwrap()),
            Some(3)
        );
        let rows = data(10_000, dim, 17);
        let refs: Vec<_> = rows
            .chunks_exact(dim)
            .map(|r| prepared.row_f32(r).unwrap())
            .collect();
        let mut serial = vec![0; refs.len()];
        let mut parallel = vec![0; refs.len()];
        prepared.assign(&refs, &mut serial);
        prepared.assign_parallel(&refs, &mut parallel, 7);
        assert_eq!(serial, parallel);
        for (row, &list) in refs.iter().zip(&serial).step_by(13) {
            assert_eq!(prepared.nearest(*row), Some(list as usize));
        }
    }

    #[test]
    fn exceptional_domains_defer_to_exact_scoring() {
        let dim = 32;
        let centers = data(5, dim, 1);
        for metric in [Metric::L2, Metric::Cosine, Metric::Dot] {
            let prepared = PreparedCentroids::new(centers.clone(), dim, metric).unwrap();
            for row in [
                vec![1e-30f32; dim],
                vec![1e30f32; dim],
                vec![0.0; dim],
                {
                    let mut v = vec![0.5; dim];
                    v[3] = f32::NAN;
                    v
                },
                {
                    let mut v = vec![0.5; dim];
                    v[0] = f32::INFINITY;
                    v
                },
            ] {
                let row = prepared.row_f32(&row).unwrap();
                assert_eq!(prepared.nearest(row), None);
                let mut out = [0];
                prepared.assign(&[row], &mut out);
                assert_eq!(out[0], EXACT);
                assert!(!prepared.keys(row, &mut Vec::new()));
            }
            assert!(prepared.row_f32(&[1.0; 31]).is_none());
            assert!(prepared.row_bytes(&[0; 31 * 4]).is_none());
        }
        for bad in [vec![1e20f32; dim], vec![0.0; dim], vec![1e-20f32; dim]] {
            let mut centers = data(3, dim, 2);
            centers.extend_from_slice(&bad);
            assert!(PreparedCentroids::new(centers, dim, Metric::L2).is_none());
        }
        assert!(PreparedCentroids::new(data(2, dim, 3), dim, Metric::Hamming).is_none());
        assert!(PreparedCentroids::new(Vec::new(), dim, Metric::L2).is_none());
    }
}
