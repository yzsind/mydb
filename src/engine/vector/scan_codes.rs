//! Compact scan codes with strict error bounds for exact IVF list scans.
//!
//! An unquantized IVF index scans every row of its probed lists. With wide
//! rows that scan is bound by memory bandwidth, not arithmetic. Each row
//! therefore also keeps an 8-bit per-dimension code `ŷ = low + scale · c`
//! of `y` (the row itself, or the unit row for COSINE) together with the
//! exact residual norm `‖y − ŷ‖` and `‖ŷ‖`. For a query `q`,
//!
//! ```text
//!   |q·y − q·ŷ| ≤ ‖q‖ · ‖y − ŷ‖          (Cauchy–Schwarz)
//!   ‖q − y‖ ≥ ‖q − ŷ‖ − ‖y − ŷ‖          (triangle inequality)
//! ```
//!
//! turn the cheap code estimate into a lower and an upper bound of the row's
//! exact ordering distance. Rounding of the estimate and of the exact scorers
//! is covered by explicit worst-case margins, so the bounds hold for the
//! distance the exact kernels actually compute. A scan can then skip every
//! row whose lower bound cannot beat the current k-th exact distance without
//! changing its result: the codes only decide which rows need their exact
//! payload, never which rows are returned.
use super::centroids::Isa;
use super::distance::Metric;
use crate::types::QueryError;
use crate::vector::VectorDataType;

/// `err: f32`, `norm: f32`, then one code byte per dimension.
pub(crate) const RECORD_HEADER: usize = 8;
const LEVELS: f64 = 255.0;
/// Unit roundoff doubled (covers separate multiply/add rounding).
const ROUNDING: f64 = f32::EPSILON as f64;
const CODEC_VERSION: u8 = 1;

/// Per-dimension affine 8-bit code shared by every row of one index.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct ScanCodec {
    metric: Metric,
    low: Vec<f32>,
    scale: Vec<f32>,
}

/// Whether rows of this shape gain from a code: numeric, a bounded metric,
/// and an exact payload at least 1.5× the code record.
pub(crate) fn eligible(datatype: VectorDataType, dimension: u32, metric: Metric) -> bool {
    let dim = dimension as usize;
    matches!(
        datatype,
        VectorDataType::Float32 | VectorDataType::Float16 | VectorDataType::BFloat16
    ) && matches!(metric, Metric::L2 | Metric::Cosine | Metric::Dot)
        && dim >= 16
        && 2 * datatype.payload_len(dimension) >= 3 * (dim + RECORD_HEADER)
}

/// `y` of a row in f64: the row, or the unit row for COSINE. `None` when the
/// row is outside the bounded domain (non-finite, or a degenerate norm).
fn coded_space(values: &[f32], metric: Metric) -> Option<Vec<f64>> {
    let mut y: Vec<f64> = values.iter().map(|&v| f64::from(v)).collect();
    if y.iter().any(|v| !v.is_finite()) {
        return None;
    }
    if metric == Metric::Cosine {
        let norm = y.iter().map(|v| v * v).sum::<f64>().sqrt();
        if !norm.is_normal() || !(1e-150..=1e150).contains(&norm) {
            return None;
        }
        for v in &mut y {
            *v /= norm;
        }
    }
    Some(y)
}

/// The smallest f32 that is not below `value` (finite, non-negative input).
fn f32_at_least(value: f64) -> f32 {
    let mut rounded = value as f32;
    if f64::from(rounded) < value {
        rounded = f32::from_bits(rounded.to_bits() + 1);
    }
    rounded
}

impl ScanCodec {
    /// Fit per-dimension ranges to a training sample. `None` when the sample
    /// has no row in the bounded domain or a range is not representable.
    pub(crate) fn train<T: AsRef<[f32]>>(
        dimension: usize,
        metric: Metric,
        sample: impl IntoIterator<Item = T>,
    ) -> Option<Self> {
        let mut low = vec![f64::INFINITY; dimension];
        let mut high = vec![f64::NEG_INFINITY; dimension];
        let mut rows = 0usize;
        for values in sample {
            let values = values.as_ref();
            if values.len() != dimension {
                return None;
            }
            let Some(y) = coded_space(values, metric) else {
                continue;
            };
            rows += 1;
            for ((lo, hi), v) in low.iter_mut().zip(&mut high).zip(y) {
                *lo = lo.min(v);
                *hi = hi.max(v);
            }
        }
        if rows == 0 {
            return None;
        }
        let mut codec = Self {
            metric,
            low: Vec::with_capacity(dimension),
            scale: Vec::with_capacity(dimension),
        };
        for (lo, hi) in low.into_iter().zip(high) {
            let base = lo as f32;
            let step = ((hi - f64::from(base)) / LEVELS) as f32;
            if !base.is_finite() || !step.is_finite() || step < 0.0 {
                return None;
            }
            codec.low.push(base);
            codec.scale.push(step);
        }
        Some(codec)
    }

    pub(crate) fn dimension(&self) -> usize {
        self.low.len()
    }

    /// A codec over explicit per-dimension coded-space ranges (`low[i]` ≤
    /// `high[i]`, finite). Values outside a range are clamped when encoded;
    /// their recorded residual keeps every bound exact. `None` when a range
    /// is not representable.
    pub(crate) fn from_ranges(metric: Metric, low: &[f64], high: &[f64]) -> Option<Self> {
        if low.is_empty()
            || low.len() != high.len()
            || !matches!(metric, Metric::L2 | Metric::Cosine | Metric::Dot)
        {
            return None;
        }
        let mut codec = Self {
            metric,
            low: Vec::with_capacity(low.len()),
            scale: Vec::with_capacity(low.len()),
        };
        for (&lo, &hi) in low.iter().zip(high) {
            if !lo.is_finite() || !hi.is_finite() {
                return None;
            }
            let base = lo as f32;
            let step = ((hi.max(lo) - f64::from(base)) / LEVELS).max(0.0) as f32;
            if !base.is_finite() || !step.is_finite() {
                return None;
            }
            codec.low.push(base);
            codec.scale.push(step);
        }
        Some(codec)
    }

    pub(crate) fn metric(&self) -> Metric {
        self.metric
    }

    /// Residual norm above which a row's code is too coarse to navigate on:
    /// four times the RMS rounding error of a row inside every range, so
    /// only rows clamped by a clipped range exceed it.
    pub(crate) fn exact_residual_limit(&self) -> f32 {
        let rounding: f64 = self
            .scale
            .iter()
            .map(|&step| f64::from(step) * f64::from(step) / 12.0)
            .sum();
        (4.0 * rounding.sqrt()) as f32
    }

    /// Recorded residual norm `‖y − ŷ‖` of an encoded record.
    #[inline]
    pub(crate) fn record_residual(record: &[u8]) -> f32 {
        f32::from_le_bytes([record[0], record[1], record[2], record[3]])
    }

    pub(crate) fn record_len(&self) -> usize {
        RECORD_HEADER + self.dimension()
    }

    pub(crate) fn allocated_bytes(&self) -> usize {
        self.dimension() * 8 + std::mem::size_of::<Self>()
    }

    /// Write `values`' code record into `out` (`record_len` bytes). Returns
    /// `false` (leaving `out` unspecified) for rows outside the bounded
    /// domain; such rows are always scored exactly.
    pub(crate) fn encode(&self, values: &[f32], out: &mut [u8]) -> bool {
        if values.len() != self.dimension() || out.len() != self.record_len() {
            return false;
        }
        let Some(y) = coded_space(values, self.metric) else {
            return false;
        };
        let (mut residual, mut norm) = (0.0f64, 0.0f64);
        for (i, v) in y.into_iter().enumerate() {
            let (low, scale) = (f64::from(self.low[i]), f64::from(self.scale[i]));
            let code = if scale > 0.0 {
                ((v - low) / scale).round().clamp(0.0, LEVELS)
            } else {
                0.0
            };
            let decoded = low + scale * code;
            residual += (v - decoded) * (v - decoded);
            norm += decoded * decoded;
            out[RECORD_HEADER + i] = code as u8;
        }
        // Inflate the f64 reductions' own rounding before narrowing upward.
        let slack = 1.0 + 4.0 * f64::EPSILON * self.dimension() as f64;
        let residual = f32_at_least(residual.sqrt() * slack);
        let norm = norm.sqrt() as f32;
        if !residual.is_finite() || !norm.is_finite() {
            return false;
        }
        out[..4].copy_from_slice(&residual.to_le_bytes());
        out[4..8].copy_from_slice(&norm.to_le_bytes());
        true
    }

    pub(crate) fn serialize(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(6 + self.dimension() * 8);
        out.push(CODEC_VERSION);
        out.push(self.metric.as_u8());
        out.extend_from_slice(&(self.dimension() as u32).to_le_bytes());
        for v in self.low.iter().chain(&self.scale) {
            out.extend_from_slice(&v.to_le_bytes());
        }
        out
    }

    pub(crate) fn deserialize(bytes: &[u8]) -> Result<Self, QueryError> {
        let corrupt = super::persistence::corrupt;
        if bytes.len() < 6 || bytes[0] != CODEC_VERSION {
            return Err(corrupt());
        }
        let metric = Metric::from_u8(bytes[1]).ok_or_else(corrupt)?;
        let dimension = u32::from_le_bytes(bytes[2..6].try_into().map_err(|_| corrupt())?);
        let dimension = dimension as usize;
        if !matches!(metric, Metric::L2 | Metric::Cosine | Metric::Dot)
            || dimension == 0
            || bytes.len() != 6 + dimension * 8
        {
            return Err(corrupt());
        }
        let floats: Vec<f32> = bytes[6..]
            .chunks_exact(4)
            .map(|v| f32::from_le_bytes([v[0], v[1], v[2], v[3]]))
            .collect();
        let (low, scale) = floats.split_at(dimension);
        if low.iter().chain(scale).any(|v| !v.is_finite()) || scale.iter().any(|v| *v < 0.0) {
            return Err(corrupt());
        }
        Ok(Self {
            metric,
            low: low.to_vec(),
            scale: scale.to_vec(),
        })
    }
}

/// One query prepared against a codec: `q·ŷ ≈ bias + Σ weights[i]·code[i]`.
#[derive(Debug)]
pub(crate) struct ScanQuery {
    metric: Metric,
    weights: CodeWeights,
    bias: f64,
    /// Worst-case error of the estimate of `q·ŷ`.
    estimate_error: f64,
    /// `‖q‖` in the coded space (1 for COSINE) and `‖q‖²`.
    norm: f64,
    squared: f64,
    /// Relative worst-case rounding of an exact scorer.
    exact_rounding: f64,
}

impl ScanQuery {
    /// `None` when the query is outside the bounded domain; callers then
    /// score every row exactly.
    pub(crate) fn new(codec: &ScanCodec, query: &[f32]) -> Option<Self> {
        Self::with_kernel(codec, query, CodeKernel::detected())
    }

    fn with_kernel(codec: &ScanCodec, query: &[f32], kernel: CodeKernel) -> Option<Self> {
        let dim = codec.dimension();
        if query.len() != dim {
            return None;
        }
        let q = coded_space(query, codec.metric)?;
        let squared: f64 = q.iter().map(|v| v * v).sum();
        let norm = squared.sqrt();
        if !squared.is_finite() || (codec.metric != Metric::Dot && !squared.is_normal()) {
            return None;
        }
        let (mut bias, mut bias_magnitude) = (0.0f64, 0.0f64);
        let mut weights = Vec::with_capacity(dim);
        for (i, &v) in q.iter().enumerate() {
            weights.push(v * f64::from(codec.scale[i]));
            bias += v * f64::from(codec.low[i]);
            bias_magnitude += (v * f64::from(codec.low[i])).abs();
        }
        let weights = CodeWeights::with_kernel(&weights, kernel)?;
        let estimate_error = weights.error + 1e-12 * bias_magnitude;
        // Exact scorers may reduce sequentially in f32 with separate multiply
        // and add roundings, and COSINE divides by two reduced norms.
        let exact_rounding = 2.0 * (dim + 32) as f64 * ROUNDING;
        if !estimate_error.is_finite() || !bias.is_finite() {
            return None;
        }
        Some(Self {
            metric: codec.metric,
            weights,
            bias,
            estimate_error,
            norm,
            squared,
            exact_rounding,
        })
    }

    /// Code estimate of the row's ordering distance, on the scale of the
    /// exact scorers (`1 − cos`, `−dot`, squared L2). It navigates; only
    /// [`Self::bounds`] or an exact score may decide a result.
    #[inline]
    pub(crate) fn estimate(&self, record: &[u8]) -> f64 {
        self.estimate_from_dot(record, self.weights.dot(&record[RECORD_HEADER..]))
    }

    /// [`Self::estimate`] for four records at once; each lane equals the
    /// single-record value.
    #[inline]
    pub(crate) fn estimate_batch4(&self, records: [&[u8]; 4]) -> [f64; 4] {
        let dots = self
            .weights
            .dot_batch4(records.map(|record| &record[RECORD_HEADER..]));
        std::array::from_fn(|j| self.estimate_from_dot(records[j], dots[j]))
    }

    #[inline]
    fn estimate_from_dot(&self, record: &[u8], dot: f64) -> f64 {
        let estimate = self.bias + dot;
        match self.metric {
            Metric::Cosine => 1.0 - estimate,
            Metric::Dot => -estimate,
            _ => {
                let coded_norm = f64::from(f32::from_le_bytes([
                    record[4], record[5], record[6], record[7],
                ]));
                (self.squared + coded_norm * coded_norm - 2.0 * estimate).max(0.0)
            }
        }
    }

    /// Lower and upper bounds of the exact ordering distance of the row
    /// whose code record is `record`.
    #[inline]
    pub(crate) fn bounds(&self, record: &[u8]) -> (f64, f64) {
        let codes = &record[RECORD_HEADER..];
        let residual = f64::from(f32::from_le_bytes([
            record[0], record[1], record[2], record[3],
        ]));
        let coded_norm = f64::from(f32::from_le_bytes([
            record[4], record[5], record[6], record[7],
        ]));
        let estimate = self.bias + self.weights.dot(codes);
        let error = self.estimate_error;
        match self.metric {
            Metric::Cosine => {
                let slack = self.exact_rounding;
                (
                    1.0 - (estimate + error + residual) - slack,
                    1.0 - (estimate - error - residual) + slack,
                )
            }
            Metric::Dot => {
                let slack = self.exact_rounding * self.norm * (coded_norm + residual);
                let spread = error + self.norm * residual;
                (-(estimate + spread) - slack, -(estimate - spread) + slack)
            }
            _ => {
                // ‖q − ŷ‖² from the estimate; ‖ŷ‖ was rounded to f32.
                let coded = coded_norm * coded_norm;
                let near = (self.squared + coded * (1.0 - 4.0 * ROUNDING)
                    - 2.0 * (estimate + error))
                    .max(0.0)
                    .sqrt();
                let far = (self.squared + coded * (1.0 + 4.0 * ROUNDING)
                    - 2.0 * (estimate - error))
                    .max(0.0)
                    .sqrt();
                let magnitude = self.norm + coded_norm + residual;
                let slack = self.exact_rounding * magnitude * magnitude;
                let low = (near - residual).max(0.0);
                let high = far + residual;
                (low * low - slack, high * high + slack)
            }
        }
    }
}

/// How `CodeWeights::dot` reduces a row.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CodeKernel {
    /// f32 multiply-add on the given ISA.
    Float(Isa),
    /// Exact 16-bit integer dot products (AVX-512 VNNI `vpdpwssd`) of the
    /// codes with weights rounded to 16 bits.
    #[cfg(target_arch = "x86_64")]
    Vnni,
}

impl CodeKernel {
    /// The fastest kernel this CPU supports, detected once.
    pub(crate) fn detected() -> Self {
        #[cfg(target_arch = "x86_64")]
        {
            static VNNI: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
            if *VNNI.get_or_init(|| {
                std::is_x86_feature_detected!("avx512f")
                    && std::is_x86_feature_detected!("avx512bw")
                    && std::is_x86_feature_detected!("avx512vnni")
            }) {
                return Self::Vnni;
            }
        }
        Self::Float(Isa::detected())
    }

    /// Every kernel this CPU can execute (test oracle sweep).
    #[cfg(test)]
    pub(crate) fn supported() -> Vec<Self> {
        let mut kernels: Vec<Self> = Isa::supported().into_iter().map(Self::Float).collect();
        #[cfg(target_arch = "x86_64")]
        if Self::detected() == Self::Vnni {
            kernels.push(Self::Vnni);
        }
        kernels
    }
}

/// Per-dimension weights of `Σ wᵢ·cᵢ` over 8-bit codes, prepared once per
/// query for one kernel, with that kernel's worst-case absolute error.
#[derive(Debug)]
pub(crate) struct CodeWeights {
    kernel: CodeKernel,
    float: Vec<f32>,
    integer: Vec<i16>,
    /// `1 / s` for integer weights `round(wᵢ·s)`.
    inverse_scale: f64,
    /// `127.5 · Σ (wᵢ − round(wᵢ·s)/s)`: the expected weight-rounding term.
    shift: f64,
    error: f64,
}

impl CodeWeights {
    pub(crate) fn new(weights: &[f64]) -> Option<Self> {
        Self::with_kernel(weights, CodeKernel::detected())
    }

    pub(crate) fn with_kernel(weights: &[f64], kernel: CodeKernel) -> Option<Self> {
        if weights.iter().any(|w| !w.is_finite()) {
            return None;
        }
        let magnitude: f64 = weights.iter().map(|w| w.abs()).sum();
        match kernel {
            CodeKernel::Float(_) => {
                let float: Vec<f32> = weights.iter().map(|&w| w as f32).collect();
                if float.iter().any(|w| !w.is_finite()) {
                    return None;
                }
                // Narrowed weights are exact inputs; every f32 kernel then
                // reduces at most `len / 8 + 32` sequential roundings per lane
                // (tails included), each separate multiply and add rounding.
                let narrowing: f64 = weights
                    .iter()
                    .zip(&float)
                    .map(|(&w, &f)| (w - f64::from(f)).abs())
                    .sum();
                let depth = (weights.len() / 8 + 32) as f64;
                let reduced: f64 = float.iter().map(|w| f64::from(w.abs())).sum();
                let error = LEVELS * narrowing + depth * ROUNDING * LEVELS * reduced;
                Some(Self {
                    kernel,
                    float,
                    integer: Vec::new(),
                    inverse_scale: 0.0,
                    shift: 0.0,
                    error: error * (1.0 + 1e-9),
                })
            }
            #[cfg(target_arch = "x86_64")]
            CodeKernel::Vnni => {
                let largest = weights.iter().fold(0.0f64, |m, w| m.max(w.abs()));
                let scale = if largest > 0.0 {
                    f64::from(i16::MAX) / largest
                } else {
                    1.0
                };
                if !scale.is_finite() || !scale.is_normal() {
                    return None;
                }
                let integer: Vec<i16> = weights
                    .iter()
                    .map(|&w| (w * scale).round().clamp(-32767.0, 32767.0) as i16)
                    .collect();
                // Σ δᵢcᵢ with δᵢ = wᵢ − ŵᵢ/s and cᵢ ∈ [0, 255] lies within
                // 127.5·Σ|δᵢ| of 127.5·Σδᵢ. The integer sum itself is exact.
                let (mut shift, mut spread) = (0.0f64, 0.0f64);
                for (&w, &q) in weights.iter().zip(&integer) {
                    let delta = w - f64::from(q) / scale;
                    shift += delta;
                    spread += delta.abs();
                }
                let error = 127.5 * spread + 1e-12 * LEVELS * magnitude;
                Some(Self {
                    kernel,
                    float: Vec::new(),
                    integer,
                    inverse_scale: 1.0 / scale,
                    shift: 127.5 * shift,
                    error: error * (1.0 + 1e-9),
                })
            }
        }
    }

    /// Worst-case `|dot(codes) − Σ wᵢ·codes[i]|`.
    pub(crate) fn error(&self) -> f64 {
        self.error
    }

    /// `Σ wᵢ·codes[i]` within `error()`; `codes` has one byte per weight.
    #[inline]
    pub(crate) fn dot(&self, codes: &[u8]) -> f64 {
        match self.kernel {
            CodeKernel::Float(isa) => f64::from(weighted_codes(isa, codes, &self.float)),
            #[cfg(target_arch = "x86_64")]
            CodeKernel::Vnni => {
                let len = codes.len().min(self.integer.len());
                // SAFETY: `CodeKernel::detected` confirmed AVX-512F/BW/VNNI;
                // `len` bounds every load.
                let sum = unsafe { integer_codes_vnni(codes.as_ptr(), self.integer.as_ptr(), len) };
                self.shift + sum as f64 * self.inverse_scale
            }
        }
    }
}

impl CodeWeights {
    /// [`Self::dot`] of four code rows (each at least as long as the
    /// weights); every lane is identical to the single-row result.
    #[inline]
    pub(crate) fn dot_batch4(&self, codes: [&[u8]; 4]) -> [f64; 4] {
        match self.kernel {
            #[cfg(target_arch = "x86_64")]
            CodeKernel::Vnni => {
                let len = codes
                    .iter()
                    .fold(self.integer.len(), |len, row| len.min(row.len()));
                // SAFETY: `CodeKernel::detected` confirmed AVX-512F/BW/VNNI;
                // `len` bounds every load of every row.
                let sums = unsafe {
                    integer_codes_vnni_batch4(codes.map(<[u8]>::as_ptr), self.integer.as_ptr(), len)
                };
                sums.map(|sum| self.shift + sum as f64 * self.inverse_scale)
            }
            CodeKernel::Float(_) => codes.map(|row| self.dot(row)),
        }
    }
}

/// [`integer_codes_vnni`] over four rows sharing each weight load. Every
/// accumulator sees the same per-block product count as the single-row
/// kernel (32 per lane per 1024 dimensions), so sums stay exact.
#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx512f,avx512bw,avx512vnni")]
unsafe fn integer_codes_vnni_batch4(
    codes: [*const u8; 4],
    weights: *const i16,
    len: usize,
) -> [i64; 4] {
    use std::arch::x86_64::*;
    let mut total = [0i64; 4];
    let mut i = 0;
    while i + 32 <= len {
        let block_end = (i + 1024).min(len - (len - i) % 32);
        let mut acc = [_mm512_setzero_si512(); 4];
        while i + 32 <= block_end {
            let w = _mm512_loadu_si512(weights.add(i).cast());
            for (row, acc) in acc.iter_mut().enumerate() {
                let c = _mm512_cvtepu8_epi16(_mm256_loadu_si256(codes[row].add(i).cast()));
                *acc = _mm512_dpwssd_epi32(*acc, c, w);
            }
            i += 32;
        }
        for (row, acc) in acc.iter().enumerate() {
            let low = _mm512_cvtepi32_epi64(_mm512_castsi512_si256(*acc));
            let high = _mm512_cvtepi32_epi64(_mm512_extracti64x4_epi64::<1>(*acc));
            total[row] += _mm512_reduce_add_epi64(_mm512_add_epi64(low, high));
        }
    }
    while i < len {
        for (row, total) in total.iter_mut().enumerate() {
            *total += i64::from(*codes[row].add(i)) * i64::from(*weights.add(i));
        }
        i += 1;
    }
    total
}

/// `Σ weights[i] · codes[i]` in f32 with at most `len / 8 + 32` sequential
/// roundings per lane on every kernel.
#[inline]
fn weighted_codes(isa: Isa, codes: &[u8], weights: &[f32]) -> f32 {
    let len = codes.len().min(weights.len());
    match isa {
        #[cfg(target_arch = "x86_64")]
        // SAFETY: `Isa::detected` confirmed AVX-512F; `len` bounds every load.
        Isa::Avx512 => unsafe { weighted_codes_avx512(codes.as_ptr(), weights.as_ptr(), len) },
        #[cfg(target_arch = "x86_64")]
        // SAFETY: `Isa::detected` confirmed AVX2 and FMA; `len` bounds loads.
        Isa::Avx2 => unsafe { weighted_codes_avx2(codes.as_ptr(), weights.as_ptr(), len) },
        _ => weighted_codes_scalar(&codes[..len], &weights[..len]),
    }
}

/// Exact `Σ weights[i] · codes[i]`. Blocks of 1024 dimensions keep every
/// 32-bit lane below 2³¹ (at most 11 pair products of ≤ 2·255·32767 per
/// accumulator, four accumulators summed) before widening to 64 bits.
#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx512f,avx512bw,avx512vnni")]
unsafe fn integer_codes_vnni(codes: *const u8, weights: *const i16, len: usize) -> i64 {
    use std::arch::x86_64::*;
    let product = |offset: usize, acc: __m512i| {
        let c = _mm512_cvtepu8_epi16(_mm256_loadu_si256(codes.add(offset).cast()));
        _mm512_dpwssd_epi32(acc, c, _mm512_loadu_si512(weights.add(offset).cast()))
    };
    let mut total = 0i64;
    let mut i = 0;
    while i + 32 <= len {
        let block_end = (i + 1024).min(len - (len - i) % 32);
        let mut acc = [_mm512_setzero_si512(); 4];
        while i + 128 <= block_end {
            for (k, acc) in acc.iter_mut().enumerate() {
                *acc = product(i + 32 * k, *acc);
            }
            i += 128;
        }
        while i + 32 <= block_end {
            acc[0] = product(i, acc[0]);
            i += 32;
        }
        let sum = _mm512_add_epi32(
            _mm512_add_epi32(acc[0], acc[1]),
            _mm512_add_epi32(acc[2], acc[3]),
        );
        let low = _mm512_cvtepi32_epi64(_mm512_castsi512_si256(sum));
        let high = _mm512_cvtepi32_epi64(_mm512_extracti64x4_epi64::<1>(sum));
        total += _mm512_reduce_add_epi64(_mm512_add_epi64(low, high));
    }
    while i < len {
        total += i64::from(*codes.add(i)) * i64::from(*weights.add(i));
        i += 1;
    }
    total
}

fn weighted_codes_scalar(codes: &[u8], weights: &[f32]) -> f32 {
    let mut lanes = [0.0f32; 8];
    let mut blocks = codes.chunks_exact(8).zip(weights.chunks_exact(8));
    for (c, w) in &mut blocks {
        for lane in 0..8 {
            lanes[lane] += f32::from(c[lane]) * w[lane];
        }
    }
    let tail = codes.len() / 8 * 8;
    let mut total = ((lanes[0] + lanes[4]) + (lanes[1] + lanes[5]))
        + ((lanes[2] + lanes[6]) + (lanes[3] + lanes[7]));
    for (c, w) in codes[tail..].iter().zip(&weights[tail..]) {
        total += f32::from(*c) * w;
    }
    total
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx512f")]
unsafe fn weighted_codes_avx512(codes: *const u8, weights: *const f32, len: usize) -> f32 {
    use std::arch::x86_64::*;
    let lane = |offset: usize| {
        _mm512_cvtepi32_ps(_mm512_cvtepu8_epi32(_mm_loadu_si128(
            codes.add(offset).cast(),
        )))
    };
    let mut acc = [_mm512_setzero_ps(); 4];
    let mut i = 0;
    while i + 64 <= len {
        for (k, acc) in acc.iter_mut().enumerate() {
            let offset = i + 16 * k;
            *acc = _mm512_fmadd_ps(lane(offset), _mm512_loadu_ps(weights.add(offset)), *acc);
        }
        i += 64;
    }
    while i + 16 <= len {
        acc[0] = _mm512_fmadd_ps(lane(i), _mm512_loadu_ps(weights.add(i)), acc[0]);
        i += 16;
    }
    let sum = _mm512_add_ps(_mm512_add_ps(acc[0], acc[1]), _mm512_add_ps(acc[2], acc[3]));
    let mut total = _mm512_reduce_add_ps(sum);
    while i < len {
        total += f32::from(*codes.add(i)) * *weights.add(i);
        i += 1;
    }
    total
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2,fma")]
unsafe fn weighted_codes_avx2(codes: *const u8, weights: *const f32, len: usize) -> f32 {
    use std::arch::x86_64::*;
    let lane = |offset: usize| {
        _mm256_cvtepi32_ps(_mm256_cvtepu8_epi32(_mm_loadl_epi64(
            codes.add(offset).cast(),
        )))
    };
    let mut acc = [_mm256_setzero_ps(); 4];
    let mut i = 0;
    while i + 32 <= len {
        for (k, acc) in acc.iter_mut().enumerate() {
            let offset = i + 8 * k;
            *acc = _mm256_fmadd_ps(lane(offset), _mm256_loadu_ps(weights.add(offset)), *acc);
        }
        i += 32;
    }
    while i + 8 <= len {
        acc[0] = _mm256_fmadd_ps(lane(i), _mm256_loadu_ps(weights.add(i)), acc[0]);
        i += 8;
    }
    let sum = _mm256_add_ps(_mm256_add_ps(acc[0], acc[1]), _mm256_add_ps(acc[2], acc[3]));
    let half = _mm_add_ps(_mm256_castps256_ps128(sum), _mm256_extractf128_ps(sum, 1));
    let pair = _mm_add_ps(half, _mm_movehl_ps(half, half));
    let mut total = _mm_cvtss_f32(_mm_add_ss(pair, _mm_shuffle_ps(pair, pair, 1)));
    while i < len {
        total += f32::from(*codes.add(i)) * *weights.add(i);
        i += 1;
    }
    total
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::vector::distance::{typed_ordering_distance, PreparedF32Distance};
    use crate::vector::VectorValue;

    struct Rng(u64);
    impl Rng {
        fn next(&mut self) -> f64 {
            self.0 ^= self.0 << 13;
            self.0 ^= self.0 >> 7;
            self.0 ^= self.0 << 17;
            (self.0 >> 11) as f64 / (1u64 << 53) as f64
        }
        fn vector(&mut self, dim: usize, center: f64, spread: f64) -> Vec<f32> {
            (0..dim)
                .map(|_| (center + spread * (self.next() - 0.5)) as f32)
                .collect()
        }
    }

    fn value(values: &[f32], datatype: VectorDataType) -> VectorValue {
        let dense = super::super::codec::from_f32(values);
        match datatype {
            VectorDataType::Float32 => dense,
            other => super::super::codec::convert_value(&dense, other, None).unwrap(),
        }
    }

    #[test]
    fn code_weights_match_f64_reference_within_their_error_for_every_kernel() {
        let mut rng = Rng(0x5eed);
        for len in [
            1usize, 7, 8, 15, 16, 17, 31, 32, 63, 64, 65, 127, 768, 1537, 4099,
        ] {
            let codes: Vec<u8> = (0..len + 3).map(|_| (rng.next() * 256.0) as u8).collect();
            let weights: Vec<f64> = (0..len + 3).map(|_| (rng.next() - 0.5) * 1e-2).collect();
            for start in [0usize, 1, 3] {
                let (codes, weights) = (&codes[start..start + len], &weights[start..start + len]);
                let exact: f64 = codes
                    .iter()
                    .zip(weights)
                    .map(|(&c, &w)| f64::from(c) * w)
                    .sum();
                for kernel in CodeKernel::supported() {
                    let prepared = CodeWeights::with_kernel(weights, kernel).unwrap();
                    let got = prepared.dot(codes);
                    assert!(
                        (got - exact).abs() <= prepared.error(),
                        "{kernel:?} len={len} start={start}: {got} vs {exact} ± {}",
                        prepared.error()
                    );
                    assert!(
                        prepared.error()
                            < 1e-4 * weights.iter().map(|w| w.abs()).sum::<f64>() * LEVELS
                    );
                }
            }
        }
        // Extreme codes saturate every lane of the integer kernel.
        let codes = vec![255u8; 16384];
        let weights = vec![-1.0f64; 16384];
        for kernel in CodeKernel::supported() {
            let prepared = CodeWeights::with_kernel(&weights, kernel).unwrap();
            let got = prepared.dot(&codes);
            assert!(
                (got + 255.0 * 16384.0).abs() <= prepared.error() + 1e-6,
                "{kernel:?} {got}"
            );
        }
    }

    /// Bounds enclose the distance the production exact scorers compute,
    /// including clamped outliers, rows far outside the trained range and
    /// near-duplicates of the query.
    #[test]
    fn bounds_enclose_exact_scores_for_every_metric_datatype_and_isa() {
        let dim = 96;
        for metric in [Metric::Cosine, Metric::L2, Metric::Dot] {
            for datatype in [
                VectorDataType::Float32,
                VectorDataType::Float16,
                VectorDataType::BFloat16,
            ] {
                let mut rng = Rng(0x1234 + metric.as_u8() as u64);
                let to_type = |v: Vec<f32>| super::super::codec::to_f32(&value(&v, datatype));
                let sample: Vec<Vec<f32>> = (0..200)
                    .map(|_| to_type(rng.vector(dim, 0.2, 2.0)))
                    .collect();
                let codec =
                    ScanCodec::train(dim, metric, sample.iter().map(Vec::as_slice)).unwrap();
                let mut rows: Vec<Vec<f32>> = (0..300)
                    .map(|_| to_type(rng.vector(dim, 0.2, 2.0)))
                    .collect();
                rows.push(to_type(rng.vector(dim, 30.0, 80.0))); // far outside the range
                rows.push(to_type(rng.vector(dim, -0.1, 1e-3)));
                let queries: Vec<Vec<f32>> = (0..12)
                    .map(|i| {
                        if i < 3 {
                            rows[i].clone() // the row itself
                        } else {
                            to_type(rng.vector(dim, 0.1, 3.0))
                        }
                    })
                    .collect();
                let mut record = vec![0u8; codec.record_len()];
                let mut widths = Vec::new();
                for query in &queries {
                    let query_value = value(query, datatype);
                    let exact = (datatype == VectorDataType::Float32)
                        .then(|| PreparedF32Distance::new(query, metric));
                    for kernel in CodeKernel::supported() {
                        let prepared = ScanQuery::with_kernel(&codec, query, kernel).unwrap();
                        for (index, row) in rows.iter().enumerate() {
                            assert!(codec.encode(row, &mut record));
                            let (low, high) = prepared.bounds(&record);
                            let row_value = value(row, datatype);
                            let mut scores =
                                vec![typed_ordering_distance(&query_value, &row_value, metric)
                                    .unwrap()];
                            if let Some(exact) = &exact {
                                scores.push(exact.score_bytes(&row_value.payload));
                            }
                            for score in scores {
                                assert!(
                                    low <= score && score <= high,
                                    "{metric:?} {datatype:?} {kernel:?}: {low} <= {score} <= {high}"
                                );
                            }
                            if index < 300 && metric == Metric::Cosine {
                                widths.push(high - low);
                            }
                        }
                    }
                }
                // Rows from the training distribution get tight intervals (a
                // few rows outside the sampled ranges are clamped and wider).
                if metric == Metric::Cosine {
                    widths.sort_by(f64::total_cmp);
                    let median = widths[widths.len() / 2];
                    assert!(median < 0.02, "{datatype:?} median width {median}");
                }
            }
        }
    }

    #[test]
    fn codec_round_trips_and_rejects_unbounded_rows() {
        let mut rng = Rng(9);
        let sample: Vec<Vec<f32>> = (0..64).map(|_| rng.vector(32, 0.0, 1.0)).collect();
        let codec = ScanCodec::train(32, Metric::Cosine, sample.iter().map(Vec::as_slice)).unwrap();
        assert_eq!(ScanCodec::deserialize(&codec.serialize()).unwrap(), codec);
        assert!(ScanCodec::deserialize(&codec.serialize()[1..]).is_err());
        let mut record = vec![0u8; codec.record_len()];
        assert!(!codec.encode(&[0.0; 32], &mut record), "zero COSINE row");
        let mut infinite = sample[0].clone();
        infinite[3] = f32::INFINITY;
        assert!(!codec.encode(&infinite, &mut record));
        assert!(ScanQuery::new(&codec, &[0.0; 32]).is_none());
        assert!(ScanCodec::train(32, Metric::Cosine, [[0.0f32; 32].as_slice()]).is_none());
        // Constant dimensions code exactly.
        let flat = vec![vec![0.5f32; 32]; 4];
        let codec = ScanCodec::train(32, Metric::L2, flat.iter().map(Vec::as_slice)).unwrap();
        assert!(codec.encode(&flat[0], &mut record));
        assert_eq!(f32::from_le_bytes(record[..4].try_into().unwrap()), 0.0);
    }

    #[test]
    fn batch_dots_match_single_rows_for_every_kernel() {
        let mut rng = Rng(0xba7c4);
        for len in [1usize, 31, 32, 33, 768, 1024, 1056, 2049] {
            let weights: Vec<f64> = (0..len).map(|_| (rng.next() - 0.5) * 1e-2).collect();
            let rows: Vec<Vec<u8>> = (0..4)
                .map(|_| (0..len + 1).map(|_| (rng.next() * 256.0) as u8).collect())
                .collect();
            for kernel in CodeKernel::supported() {
                let prepared = CodeWeights::with_kernel(&weights, kernel).unwrap();
                // Unaligned starts exercise unaligned loads.
                let batch = prepared.dot_batch4(std::array::from_fn(|j| &rows[j][1..]));
                for j in 0..4 {
                    assert_eq!(
                        batch[j].to_bits(),
                        prepared.dot(&rows[j][1..]).to_bits(),
                        "{kernel:?} len={len} row={j}"
                    );
                }
            }
        }
    }

    /// Navigation estimates lie inside the strict bounds, so mixing them with
    /// exact scores in one candidate heap never needs a rescaling.
    #[test]
    fn estimates_lie_within_bounds_for_clipped_ranges() {
        let dim = 64;
        for metric in [Metric::Cosine, Metric::L2, Metric::Dot] {
            let mut rng = Rng(0xe57 + metric.as_u8() as u64);
            let rows: Vec<Vec<f32>> = (0..200).map(|_| rng.vector(dim, 0.1, 2.0)).collect();
            // Ranges deliberately narrower than the data clamp some values.
            let (mut low, mut high) = (vec![-0.4f64; dim], vec![0.5f64; dim]);
            if metric == Metric::Cosine {
                low.fill(-0.15);
                high.fill(0.2);
            }
            low[3] = 0.0;
            high[3] = 0.0; // a constant dimension
            let codec = ScanCodec::from_ranges(metric, &low, &high).unwrap();
            let limit = codec.exact_residual_limit();
            assert!(limit.is_finite() && limit > 0.0);
            let mut records = vec![vec![0u8; codec.record_len()]; rows.len()];
            for (row, record) in rows.iter().zip(&mut records) {
                assert!(codec.encode(row, record));
            }
            for query in rows.iter().take(8) {
                for kernel in CodeKernel::supported() {
                    let prepared = ScanQuery::with_kernel(&codec, query, kernel).unwrap();
                    for group in records.chunks_exact(4) {
                        let batch =
                            prepared.estimate_batch4(std::array::from_fn(|j| group[j].as_slice()));
                        for (record, estimate) in group.iter().zip(batch) {
                            assert_eq!(estimate.to_bits(), prepared.estimate(record).to_bits());
                            let (lo, hi) = prepared.bounds(record);
                            assert!(lo <= estimate && estimate <= hi, "{metric:?} {kernel:?}");
                        }
                    }
                }
            }
        }
        assert!(ScanCodec::from_ranges(Metric::Cosine, &[0.0], &[f64::NAN]).is_none());
        assert!(ScanCodec::from_ranges(Metric::Hamming, &[0.0], &[1.0]).is_none());
        assert!(ScanCodec::from_ranges(Metric::L2, &[], &[]).is_none());
    }

    #[test]
    fn eligibility_requires_a_compressible_numeric_shape() {
        assert!(eligible(VectorDataType::Float32, 768, Metric::Cosine));
        assert!(eligible(VectorDataType::Float16, 64, Metric::L2));
        assert!(!eligible(VectorDataType::Float16, 8, Metric::L2));
        assert!(!eligible(VectorDataType::Int8, 768, Metric::Dot));
        assert!(!eligible(VectorDataType::Binary, 768, Metric::Hamming));
        assert!(!eligible(VectorDataType::Float32, 768, Metric::Hamming));
    }
}
