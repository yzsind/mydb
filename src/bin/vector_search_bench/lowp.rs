//! Compressed-vector experiments. Encoding/setup and search timing are reported
//! separately. FP16/BF16/INT8 change the input domain; none claims FP32 recall.

use super::{x86, TopK};

use std::arch::x86_64::*;
use std::time::Instant;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Kind {
    F16,
    Bf16,
    Vnni,
    Vnni256Evex,
    Vnni512,
}

impl Kind {
    pub(super) fn available(self) -> bool {
        match self {
            Self::F16 => x86::avx2_available() && is_x86_feature_detected!("f16c"),
            Self::Bf16 => {
                x86::avx512_available()
                    && is_x86_feature_detected!("avx512bf16")
                    && is_x86_feature_detected!("avx512bw")
            }
            Self::Vnni => x86::avx2_available() && is_x86_feature_detected!("avxvnni"),
            Self::Vnni256Evex => {
                x86::avx512_available()
                    && is_x86_feature_detected!("avx512vnni")
                    && is_x86_feature_detected!("avx512vl")
            }
            Self::Vnni512 => {
                x86::avx512_available()
                    && is_x86_feature_detected!("avx512vnni")
                    && is_x86_feature_detected!("avx512bw")
            }
        }
    }
}

fn stride(dim: usize) -> Result<usize, String> {
    if dim == 0 {
        return Err("compressed vectors require a positive dimension".into());
    }
    dim.checked_add(63)
        .map(|n| n / 64 * 64)
        .ok_or_else(|| "padded dimension overflow".into())
}

fn valid(values: &[f32], dim: usize) -> Result<usize, String> {
    if dim == 0 || !values.len().is_multiple_of(dim) {
        return Err("incomplete vector input".into());
    }
    if let Some(i) = values.iter().position(|v| !v.is_finite()) {
        return Err(format!("compressed input must be finite (element {i})"));
    }
    Ok(values.len() / dim)
}

fn decode_half(v: u16) -> f32 {
    let sign = if v & 0x8000 != 0 { -1.0 } else { 1.0 };
    let exp = (v >> 10) & 31;
    let mant = v & 1023;
    if exp == 0 {
        sign * mant as f32 * 2.0f32.powi(-24)
    } else {
        f32::from_bits(
            ((v as u32 & 0x8000) << 16) | (((exp as u32) + 112) << 23) | ((mant as u32) << 13),
        )
    }
}

pub(super) struct Packed16 {
    data: Vec<u16>,
    stride: usize,
    dim: usize,
    bf16: bool,
    norms: Vec<f64>,
}
impl Packed16 {
    fn encode(values: &[f32], dim: usize, bf16: bool) -> Result<Self, String> {
        let n = valid(values, dim)?;
        let stride = stride(dim)?;
        let mut data = vec![0u16; n.checked_mul(stride).ok_or("encoded length overflow")?];
        let supported = if bf16 {
            Kind::Bf16.available()
        } else {
            Kind::F16.available()
        };
        if !supported {
            return Err("requested 16-bit conversion ISA is unavailable".into());
        }
        for (row, input) in values.chunks_exact(dim).enumerate() {
            // SAFETY: feature support is checked, output is padded to 64 elements.
            unsafe {
                if bf16 {
                    encode_bf16(input, &mut data[row * stride..(row + 1) * stride]);
                } else {
                    encode_half(input, &mut data[row * stride..(row + 1) * stride]);
                }
            }
            let bad = if bf16 { 0x7f80 } else { 0x7c00 };
            if data[row * stride..row * stride + dim]
                .iter()
                .any(|v| v & bad == bad)
            {
                return Err(format!(
                    "{} encoding overflow in vector {row}",
                    if bf16 { "BF16" } else { "FP16" }
                ));
            }
        }
        let mut result = Self {
            data,
            stride,
            dim,
            bf16,
            norms: Vec::new(),
        };
        result.norms = (0..n)
            .map(|row| {
                (0..dim)
                    .map(|i| {
                        let v = f64::from(result.value(row, i));
                        v * v
                    })
                    .sum()
            })
            .collect();
        Ok(result)
    }
    fn value(&self, row: usize, i: usize) -> f32 {
        let v = self.data[row * self.stride + i];
        if self.bf16 {
            f32::from_bits((v as u32) << 16)
        } else {
            decode_half(v)
        }
    }
    fn decoded(&self, row: usize) -> Vec<f32> {
        (0..self.dim).map(|i| self.value(row, i)).collect()
    }
}

#[target_feature(enable = "avx2,f16c")]
unsafe fn encode_half(input: &[f32], output: &mut [u16]) {
    let mut i = 0;
    while input.len() - i >= 8 {
        let v = _mm256_cvtps_ph::<0>(_mm256_loadu_ps(input.as_ptr().add(i)));
        _mm_storeu_si128(output.as_mut_ptr().add(i).cast(), v);
        i += 8;
    }
    if i < input.len() {
        let mut tail = [0f32; 8];
        tail[..input.len() - i].copy_from_slice(&input[i..]);
        _mm_storeu_si128(
            output.as_mut_ptr().add(i).cast(),
            _mm256_cvtps_ph::<0>(_mm256_loadu_ps(tail.as_ptr())),
        );
    }
}

#[target_feature(enable = "avx512f,avx512bf16,avx512bw")]
unsafe fn encode_bf16(input: &[f32], output: &mut [u16]) {
    let mut i = 0;
    while input.len() - i >= 16 {
        let v = _mm512_cvtneps_pbh(_mm512_loadu_ps(input.as_ptr().add(i)));
        _mm256_storeu_si256(
            output.as_mut_ptr().add(i).cast(),
            std::mem::transmute::<__m256bh, __m256i>(v),
        );
        i += 16;
    }
    if i < input.len() {
        let mut tail = [0f32; 16];
        tail[..input.len() - i].copy_from_slice(&input[i..]);
        let v = _mm512_cvtneps_pbh(_mm512_loadu_ps(tail.as_ptr()));
        _mm256_storeu_si256(
            output.as_mut_ptr().add(i).cast(),
            std::mem::transmute::<__m256bh, __m256i>(v),
        );
    }
}

/// Pointers cover a padded, positive multiple of 64 u16 values.
#[target_feature(enable = "avx2,fma,f16c")]
unsafe fn half_l2(a: *const u16, b: *const u16, n: usize) -> f32 {
    let mut s = [_mm256_setzero_ps(); 8];
    let mut i = 0;
    macro_rules! step {
        ($j:expr) => {{
            let va = _mm256_cvtph_ps(_mm_loadu_si128(a.add(i + 8 * $j).cast()));
            let vb = _mm256_cvtph_ps(_mm_loadu_si128(b.add(i + 8 * $j).cast()));
            let d = _mm256_sub_ps(va, vb);
            s[$j] = _mm256_fmadd_ps(d, d, s[$j]);
        }};
    }
    while i < n {
        step!(0);
        step!(1);
        step!(2);
        step!(3);
        step!(4);
        step!(5);
        step!(6);
        step!(7);
        i += 64;
    }
    let lo = _mm256_add_ps(_mm256_add_ps(s[0], s[4]), _mm256_add_ps(s[2], s[6]));
    let hi = _mm256_add_ps(_mm256_add_ps(s[1], s[5]), _mm256_add_ps(s[3], s[7]));
    let v = _mm256_add_ps(lo, hi);
    x86::sum4(_mm_add_ps(
        _mm256_castps256_ps128(v),
        _mm256_extractf128_ps::<1>(v),
    ))
}

#[target_feature(enable = "avx512f,avx512bw,fma")]
unsafe fn bf16_l2(a: *const u16, b: *const u16, n: usize) -> f32 {
    let mut s = [_mm512_setzero_ps(); 4];
    let mut i = 0;
    macro_rules! step {
        ($j:expr) => {{
            let va = _mm512_castsi512_ps(_mm512_slli_epi32::<16>(_mm512_cvtepu16_epi32(
                _mm256_loadu_si256(a.add(i + 16 * $j).cast()),
            )));
            let vb = _mm512_castsi512_ps(_mm512_slli_epi32::<16>(_mm512_cvtepu16_epi32(
                _mm256_loadu_si256(b.add(i + 16 * $j).cast()),
            )));
            let d = _mm512_sub_ps(va, vb);
            s[$j] = _mm512_fmadd_ps(d, d, s[$j]);
        }};
    }
    while i < n {
        step!(0);
        step!(1);
        step!(2);
        step!(3);
        i += 64;
    }
    let v = _mm512_add_ps(_mm512_add_ps(s[0], s[2]), _mm512_add_ps(s[1], s[3]));
    let lo = _mm_add_ps(_mm512_castps512_ps128(v), _mm512_extractf32x4_ps::<2>(v));
    let hi = _mm_add_ps(
        _mm512_extractf32x4_ps::<1>(v),
        _mm512_extractf32x4_ps::<3>(v),
    );
    x86::sum4(_mm_add_ps(lo, hi))
}

#[target_feature(enable = "avx512f,avx512bf16,avx512bw")]
unsafe fn bf16_dot(a: *const u16, b: *const u16, n: usize) -> f32 {
    let mut s = [_mm512_setzero_ps(); 4];
    let mut i = 0;
    macro_rules! step {
        ($j:expr) => {{
            let av = std::mem::transmute::<__m512i, __m512bh>(_mm512_loadu_si512(
                a.add(i + 32 * $j).cast(),
            ));
            let bv = std::mem::transmute::<__m512i, __m512bh>(_mm512_loadu_si512(
                b.add(i + 32 * $j).cast(),
            ));
            s[$j] = _mm512_dpbf16_ps(s[$j], av, bv);
        }};
    }
    while n - i >= 128 {
        step!(0);
        step!(1);
        step!(2);
        step!(3);
        i += 128;
    }
    if i < n {
        step!(0);
        step!(1);
    }
    _mm512_reduce_add_ps(_mm512_add_ps(
        _mm512_add_ps(s[0], s[2]),
        _mm512_add_ps(s[1], s[3]),
    ))
}

pub(super) struct Packed8 {
    data: Vec<i8>,
    stride: usize,
    dim: usize,
    scales: Vec<f64>,
    norms: Vec<i64>,
    sums: Vec<i64>,
}
impl Packed8 {
    fn encode(values: &[f32], dim: usize) -> Result<Self, String> {
        let n = valid(values, dim)?;
        let stride = stride(dim)?;
        if dim as u128 > i64::MAX as u128 / 65025 {
            return Err("INT8 vector exceeds exact accumulator range".into());
        }
        let mut result = Self {
            data: vec![0; n.checked_mul(stride).ok_or("encoded length overflow")?],
            stride,
            dim,
            scales: Vec::with_capacity(n),
            norms: Vec::with_capacity(n),
            sums: Vec::with_capacity(n),
        };
        for (row, v) in values.chunks_exact(dim).enumerate() {
            let max = v.iter().map(|x| f64::from(*x).abs()).fold(0.0f64, f64::max);
            let scale = if max == 0.0 { 1.0 } else { max / 127.0 };
            let mut norm = 0i64;
            let mut sum = 0i64;
            for (i, &x) in v.iter().enumerate() {
                let q = (f64::from(x) / scale)
                    .round_ties_even()
                    .clamp(-127.0, 127.0) as i8;
                result.data[row * stride + i] = q;
                norm += i64::from(q) * i64::from(q);
                sum += i64::from(q);
            }
            result.scales.push(scale);
            result.norms.push(norm);
            result.sums.push(sum);
        }
        Ok(result)
    }
    fn value(&self, row: usize, i: usize) -> f64 {
        self.data[row * self.stride + i] as f64 * self.scales[row]
    }
}

/// Bounded blocks keep both lane accumulators and horizontal sums in i32.
/// Inputs contain a padded multiple of 64 bytes.
#[target_feature(enable = "avx2,avxvnni")]
unsafe fn dot_vnni(a: *const u8, b: *const i8, n: usize) -> i64 {
    let mut total = 0i64;
    let mut base = 0;
    while base < n {
        let end = (base + 8192).min(n);
        let mut s0 = _mm256_setzero_si256();
        let mut s1 = s0;
        while base < end {
            s0 = _mm256_dpbusd_avx_epi32(
                s0,
                _mm256_loadu_si256(a.add(base).cast()),
                _mm256_loadu_si256(b.add(base).cast()),
            );
            s1 = _mm256_dpbusd_avx_epi32(
                s1,
                _mm256_loadu_si256(a.add(base + 32).cast()),
                _mm256_loadu_si256(b.add(base + 32).cast()),
            );
            base += 64;
        }
        let s = _mm256_add_epi32(s0, s1);
        let mut lanes = [0i32; 8];
        _mm256_storeu_si256(lanes.as_mut_ptr().cast(), s);
        total += lanes.iter().map(|&v| i64::from(v)).sum::<i64>();
    }
    total
}

/// EVEX-encoded YMM VNNI is independent of the VEX AVX-VNNI extension.
/// Bounded blocks keep both lane accumulators and horizontal sums in i32.
/// Inputs contain a padded multiple of 64 bytes.
#[target_feature(enable = "avx2,avx512f,avx512vl,avx512vnni")]
unsafe fn dot_vnni256_evex(a: *const u8, b: *const i8, n: usize) -> i64 {
    let mut total = 0i64;
    let mut base = 0;
    while base < n {
        let end = (base + 8192).min(n);
        let mut s0 = _mm256_setzero_si256();
        let mut s1 = s0;
        while base < end {
            s0 = _mm256_dpbusd_epi32(
                s0,
                _mm256_loadu_si256(a.add(base).cast()),
                _mm256_loadu_si256(b.add(base).cast()),
            );
            s1 = _mm256_dpbusd_epi32(
                s1,
                _mm256_loadu_si256(a.add(base + 32).cast()),
                _mm256_loadu_si256(b.add(base + 32).cast()),
            );
            base += 64;
        }
        let s = _mm256_add_epi32(s0, s1);
        let mut lanes = [0i32; 8];
        _mm256_storeu_si256(lanes.as_mut_ptr().cast(), s);
        total += lanes.iter().map(|&v| i64::from(v)).sum::<i64>();
    }
    total
}

#[target_feature(enable = "avx512f,avx512bw,avx512vnni")]
unsafe fn dot_vnni512(a: *const u8, b: *const i8, n: usize) -> i64 {
    let mut total = 0i64;
    let mut base = 0;
    while base < n {
        let end = (base + 8192).min(n);
        let mut s = _mm512_setzero_si512();
        while base < end {
            s = _mm512_dpbusd_epi32(
                s,
                _mm512_loadu_si512(a.add(base).cast()),
                _mm512_loadu_si512(b.add(base).cast()),
            );
            base += 64;
        }
        total += i64::from(_mm512_reduce_add_epi32(s));
    }
    total
}

fn int8_distance(q: &Packed8, qi: usize, db: &Packed8, id: usize, dot: i64) -> f32 {
    let qs = q.scales[qi];
    let xs = db.scales[id];
    let norms = qs * qs * q.norms[qi] as f64 + xs * xs * db.norms[id] as f64;
    let result = norms - 2.0 * qs * xs * dot as f64;
    if result <= 32.0 * f64::EPSILON * norms || !result.is_finite() {
        // Resolve cancellation in the original quantized domain.
        (0..db.dim)
            .map(|i| {
                let d = q.value(qi, i) - db.value(id, i);
                d * d
            })
            .sum::<f64>() as f32
    } else {
        result as f32
    }
}

#[derive(Default)]
pub(super) struct Prepared {
    half: Option<Packed16>,
    bf16: Option<Packed16>,
    int8: Option<Packed8>,
}
impl Prepared {
    pub(super) fn new(values: &[f32], dim: usize, kinds: &[Kind]) -> Result<Self, String> {
        let mut result = Self::default();
        if kinds.contains(&Kind::F16) {
            let t = Instant::now();
            let v = Packed16::encode(values, dim, false)?;
            println!(
                "[Encoding] FP16 database: {:.3} ms, {} bytes (excluded from query timing)",
                t.elapsed().as_secs_f64() * 1e3,
                v.data.len() * 2 + v.norms.len() * 8
            );
            result.half = Some(v);
        }
        if kinds.contains(&Kind::Bf16) {
            let t = Instant::now();
            let v = Packed16::encode(values, dim, true)?;
            println!(
                "[Encoding] BF16 database: {:.3} ms, {} bytes (excluded from query timing)",
                t.elapsed().as_secs_f64() * 1e3,
                v.data.len() * 2 + v.norms.len() * 8
            );
            result.bf16 = Some(v);
        }
        if kinds
            .iter()
            .any(|kind| matches!(kind, Kind::Vnni | Kind::Vnni256Evex | Kind::Vnni512))
        {
            let t = Instant::now();
            let v = Packed8::encode(values, dim)?;
            println!(
                "[Encoding] INT8 database: {:.3} ms, {} bytes (excluded from query timing)",
                t.elapsed().as_secs_f64() * 1e3,
                v.data.len() + v.scales.len() * 24
            );
            result.int8 = Some(v);
        }
        Ok(result)
    }

    pub(super) fn topk(&self, kind: Kind, queries: &[f32], dim: usize, k: usize) -> Vec<TopK> {
        assert!(kind.available(), "requested compressed ISA is unavailable");
        match kind {
            Kind::F16 | Kind::Bf16 => {
                let bf = kind == Kind::Bf16;
                let db = if bf {
                    self.bf16.as_ref().expect("BF16 database not encoded")
                } else {
                    self.half.as_ref().expect("FP16 database not encoded")
                };
                assert_eq!(db.dim, dim, "compressed query/database dimension mismatch");
                let q = Packed16::encode(queries, dim, bf).expect("query encoding failed");
                let mut results = Vec::with_capacity(q.norms.len());
                let neps = (dim as f64 + 16.0) * f32::EPSILON as f64;
                let factor = if neps < 0.5 {
                    16.0 * neps / (1.0 - neps)
                } else {
                    f64::INFINITY
                };
                for qi in 0..q.norms.len() {
                    let mut tk = TopK::new(k);
                    for id in 0..db.norms.len() {
                        let qa = unsafe { q.data.as_ptr().add(qi * q.stride) };
                        let xa = unsafe { db.data.as_ptr().add(id * db.stride) };
                        let distance = if bf {
                            let dot = unsafe { bf16_dot(qa, xa, db.stride) };
                            let norms = q.norms[qi] + db.norms[id];
                            let approx = norms - 2.0 * f64::from(dot);
                            let error =
                                factor * norms + 16.0 * dim as f64 * f32::MIN_POSITIVE as f64;
                            if approx.is_finite()
                                && error.is_finite()
                                && approx - error > f64::from(tk.cutoff())
                            {
                                continue;
                            }
                            unsafe { bf16_l2(qa, xa, db.stride) }
                        } else {
                            unsafe { half_l2(qa, xa, db.stride) }
                        };
                        tk.push(distance, id as u32);
                    }
                    results.push(tk);
                }
                results
            }
            Kind::Vnni | Kind::Vnni256Evex | Kind::Vnni512 => {
                let db = self.int8.as_ref().expect("INT8 database not encoded");
                assert_eq!(db.dim, dim, "compressed query/database dimension mismatch");
                let q = Packed8::encode(queries, dim).expect("query encoding failed");
                let unsigned: Vec<u8> =
                    q.data.iter().map(|&v| (i16::from(v) + 128) as u8).collect();
                let mut results = Vec::with_capacity(q.scales.len());
                for qi in 0..q.scales.len() {
                    let mut tk = TopK::new(k);
                    for id in 0..db.scales.len() {
                        let qa = unsafe { unsigned.as_ptr().add(qi * q.stride) };
                        let xa = unsafe { db.data.as_ptr().add(id * db.stride) };
                        let unsigned_dot = unsafe {
                            match kind {
                                Kind::Vnni => dot_vnni(qa, xa, db.stride),
                                Kind::Vnni256Evex => dot_vnni256_evex(qa, xa, db.stride),
                                Kind::Vnni512 => dot_vnni512(qa, xa, db.stride),
                                _ => unreachable!("expected an INT8 method"),
                            }
                        };
                        let dot = unsigned_dot - 128 * db.sums[id];
                        tk.push(int8_distance(&q, qi, db, id, dot), id as u32);
                    }
                    results.push(tk);
                }
                results
            }
        }
    }
    pub(super) fn search(&self, kind: Kind, queries: &[f32], dim: usize, k: usize) -> f64 {
        self.topk(kind, queries, dim, k)
            .iter()
            .map(TopK::checksum)
            .sum()
    }

    pub(super) fn validate(
        &self,
        kind: Kind,
        queries: &[f32],
        original: &[f32],
        dim: usize,
        k: usize,
        reference: &[TopK],
    ) {
        let actual = self.topk(kind, queries, dim, k);
        let q16 = if matches!(kind, Kind::F16 | Kind::Bf16) {
            Some(
                Packed16::encode(queries, dim, kind == Kind::Bf16)
                    .expect("validation query encoding"),
            )
        } else {
            None
        };
        let q8 = if matches!(kind, Kind::Vnni | Kind::Vnni256Evex | Kind::Vnni512) {
            Some(Packed8::encode(queries, dim).expect("validation query encoding"))
        } else {
            None
        };
        let mut hits = 0usize;
        let mut same_order = 0usize;
        let mut error = 0.0f64;
        let mut max_relative = 0.0f64;
        for (qi, tk) in actual.iter().enumerate() {
            same_order += usize::from(
                tk.buf
                    .iter()
                    .map(|x| x.1)
                    .eq(reference[qi].buf.iter().map(|x| x.1)),
            );
            for &(distance, id) in &tk.buf {
                if let Some(q) = &q16 {
                    let db = if kind == Kind::Bf16 {
                        self.bf16.as_ref().unwrap()
                    } else {
                        self.half.as_ref().unwrap()
                    };
                    let expected = x86::canonical_l2(&q.decoded(qi), &db.decoded(id as usize));
                    assert_eq!(
                        distance.to_bits(),
                        expected.to_bits(),
                        "compressed-domain distance mismatch"
                    );
                }
                if let Some(q) = &q8 {
                    let db = self.int8.as_ref().unwrap();
                    let expected = (0..dim)
                        .map(|i| {
                            let d = q.value(qi, i) - db.value(id as usize, i);
                            d * d
                        })
                        .sum::<f64>();
                    let error = 4.0 * f32::EPSILON as f64 * expected.abs()
                        + 64.0
                            * f64::EPSILON
                            * (q.norms[qi] as f64 * q.scales[qi].powi(2)
                                + db.norms[id as usize] as f64 * db.scales[id as usize].powi(2));
                    assert!(
                        (f64::from(distance) - expected).abs() <= error + f32::MIN_POSITIVE as f64,
                        "INT8-domain distance mismatch"
                    );
                }
                hits += usize::from(reference[qi].buf.iter().any(|x| x.1 == id));
                let q = &queries[qi * dim..(qi + 1) * dim];
                let row = &original[id as usize * dim..(id as usize + 1) * dim];
                let exact = unsafe { x86::l2_avx2(q.as_ptr(), row.as_ptr(), dim) };
                let absolute = (f64::from(distance) - f64::from(exact)).abs();
                error += absolute;
                max_relative = max_relative.max(if exact == 0.0 {
                    if absolute == 0.0 {
                        0.0
                    } else {
                        f64::INFINITY
                    }
                } else {
                    absolute / f64::from(exact).abs()
                });
            }
        }
        println!("[Accuracy] {kind:?}: queries={} recall@{k}={:.4}% same_order={}/{} mean_abs_distance_error={:.6e} max_relative_error={:.6e}",
            actual.len(),100.0*hits as f64/(actual.len()*k) as f64,same_order,actual.len(),error/(actual.len()*k) as f64,max_relative);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::gen;

    fn bits(v: &[TopK]) -> Vec<Vec<(u32, u32)>> {
        v.iter()
            .map(|t| t.buf.iter().map(|(d, id)| (d.to_bits(), *id)).collect())
            .collect()
    }
    fn kinds() -> Vec<Kind> {
        [
            Kind::F16,
            Kind::Bf16,
            Kind::Vnni,
            Kind::Vnni256Evex,
            Kind::Vnni512,
        ]
        .into_iter()
        .filter(|k| k.available())
        .collect()
    }

    #[test]
    fn half_and_bfloat_roundtrips_and_rounding_ties() {
        if Kind::F16.available() {
            let source: Vec<f32> = (0u32..=65535)
                .filter(|v| v & 0x7c00 != 0x7c00)
                .map(|v| decode_half(v as u16))
                .collect();
            let packed = Packed16::encode(&source, source.len(), false).unwrap();
            let expected: Vec<u16> = (0u32..=65535)
                .filter(|v| v & 0x7c00 != 0x7c00)
                .map(|v| v as u16)
                .collect();
            assert_eq!(&packed.data[..source.len()], expected.as_slice());
            let source = [
                1.0 + 2f32.powi(-11),
                1.0 + 3.0 * 2f32.powi(-11),
                2f32.powi(-25),
                3.0 * 2f32.powi(-25),
            ];
            let p = Packed16::encode(&source, 4, false).unwrap();
            assert_eq!(&p.data[..4], &[0x3c00, 0x3c02, 0, 2]);
        }
        if Kind::Bf16.available() {
            let codes: Vec<u16> = (0u32..=65535)
                .filter(|v| v & 0x7f80 != 0x7f80)
                .map(|v| v as u16)
                .collect();
            let source: Vec<_> = codes
                .iter()
                .map(|&v| f32::from_bits((v as u32) << 16))
                .collect();
            let p = Packed16::encode(&source, source.len(), true).unwrap();
            for (i, &v) in codes.iter().enumerate() {
                assert_eq!(p.data[i], if v & 0x7f80 == 0 { v & 0x8000 } else { v });
            }
            let v = [
                1.0 + 2f32.powi(-8),
                1.0 + 3.0 * 2f32.powi(-8),
                f32::from_bits(1),
                -f32::from_bits(1),
            ];
            let p = Packed16::encode(&v, 4, true).unwrap();
            assert_eq!(&p.data[..4], &[0x3f80, 0x3f82, 0, 0x8000]);
        }
    }

    #[test]
    fn compressed_input_errors_are_explicit() {
        for kind in kinds() {
            let p = Prepared::new(&[0.0; 4], 2, &[kind]).unwrap();
            assert!(std::panic::catch_unwind(|| p.topk(kind, &[0.0], 1, 1)).is_err());
        }
        for bf in [false, true] {
            if !(if bf {
                Kind::Bf16.available()
            } else {
                Kind::F16.available()
            }) {
                continue;
            }
            assert!(Packed16::encode(&[f32::NAN], 1, bf).is_err());
            assert!(Packed16::encode(&[f32::INFINITY], 1, bf).is_err());
            assert!(Packed16::encode(&[f32::MAX], 1, bf).is_err());
            assert!(Packed16::encode(&[1.0], 0, bf).is_err());
            assert!(Packed16::encode(&[1.0], 2, bf).is_err());
        }
        assert!(Packed8::encode(&[f32::NAN], 1).is_err());
        assert!(Packed8::encode(&[f32::INFINITY], 1).is_err());
        assert!(Packed8::encode(&[1.0], 0).is_err());
    }

    #[test]
    fn packed_float_kernels_match_decoded_oracles() {
        for dim in (1..=129).chain([255, 256, 257, 767, 768, 769, 8193]) {
            let a = gen(1, dim, 7);
            let b = gen(1, dim, 13);
            for bf in [false, true] {
                if !(if bf {
                    Kind::Bf16.available()
                } else {
                    Kind::F16.available()
                }) {
                    continue;
                }
                let qa = Packed16::encode(&a, dim, bf).unwrap();
                let qb = Packed16::encode(&b, dim, bf).unwrap();
                assert!(qa.data[dim..].iter().all(|v| *v == 0));
                let expected = x86::canonical_l2(&qa.decoded(0), &qb.decoded(0));
                let actual = unsafe {
                    if bf {
                        bf16_l2(qa.data.as_ptr(), qb.data.as_ptr(), qa.stride)
                    } else {
                        half_l2(qa.data.as_ptr(), qb.data.as_ptr(), qa.stride)
                    }
                };
                assert_eq!(actual.to_bits(), expected.to_bits(), "dim={dim} bf={bf}");
                if bf {
                    let dot = unsafe { bf16_dot(qa.data.as_ptr(), qb.data.as_ptr(), qa.stride) };
                    let exact: f64 = (0..dim)
                        .map(|i| f64::from(qa.value(0, i)) * f64::from(qb.value(0, i)))
                        .sum();
                    let abs: f64 = (0..dim)
                        .map(|i| (f64::from(qa.value(0, i)) * f64::from(qb.value(0, i))).abs())
                        .sum();
                    assert!(
                        (f64::from(dot) - exact).abs()
                            <= 8.0 * (dim + 16) as f64 * f32::EPSILON as f64 * abs
                                + dim as f64 * f32::MIN_POSITIVE as f64
                    );
                }
            }
        }
    }

    #[test]
    fn vnni_encodings_share_identical_quantization_and_topk() {
        assert_eq!(
            Kind::Vnni256Evex.available(),
            x86::avx512_available()
                && is_x86_feature_detected!("avx512vnni")
                && is_x86_feature_detected!("avx512vl")
        );
        let modes: Vec<_> = [Kind::Vnni, Kind::Vnni256Evex, Kind::Vnni512]
            .into_iter()
            .filter(|kind| kind.available())
            .collect();
        if modes.is_empty() {
            return;
        }
        for dim in [1, 31, 32, 33, 63, 64, 65, 768, 769, 8257] {
            let mut db = gen(19, dim, 31);
            let queries = gen(5, dim, 37);
            db[dim..2 * dim].copy_from_slice(&queries[..dim]);
            let prepared = Prepared::new(&db, dim, &modes).unwrap();
            assert!(prepared.half.is_none() && prepared.bf16.is_none());
            for k in [1, 10, 19] {
                let reference = bits(&prepared.topk(modes[0], &queries, dim, k));
                for &kind in &modes {
                    assert_eq!(
                        bits(&prepared.topk(kind, &queries, dim, k)),
                        reference,
                        "kind={kind:?} dim={dim} k={k}"
                    );
                }
            }
        }
    }

    #[test]
    fn vnni_exact_dots_include_large_overflow_stress() {
        for n in [64, 128, 192, 8192, 8256, 16384, 1_048_640] {
            let a = vec![255u8; n];
            let b = vec![127i8; n];
            let expected = n as i64 * 255 * 127;
            if Kind::Vnni.available() {
                assert_eq!(unsafe { dot_vnni(a.as_ptr(), b.as_ptr(), n) }, expected);
            }
            if Kind::Vnni256Evex.available() {
                assert_eq!(
                    unsafe { dot_vnni256_evex(a.as_ptr(), b.as_ptr(), n) },
                    expected
                );
            }
            if Kind::Vnni512.available() {
                assert_eq!(unsafe { dot_vnni512(a.as_ptr(), b.as_ptr(), n) }, expected);
            }
            let a: Vec<u8> = (0..n).map(|i| (i % 256) as u8).collect();
            let b: Vec<i8> = (0..n).map(|i| ((i % 255) as i16 - 127) as i8).collect();
            let expected: i64 = a
                .iter()
                .zip(&b)
                .map(|(&a, &b)| i64::from(a) * i64::from(b))
                .sum();
            if Kind::Vnni.available() {
                assert_eq!(unsafe { dot_vnni(a.as_ptr(), b.as_ptr(), n) }, expected);
            }
            if Kind::Vnni256Evex.available() {
                assert_eq!(
                    unsafe { dot_vnni256_evex(a.as_ptr(), b.as_ptr(), n) },
                    expected
                );
            }
            if Kind::Vnni512.available() {
                assert_eq!(unsafe { dot_vnni512(a.as_ptr(), b.as_ptr(), n) }, expected);
            }
        }
    }

    #[test]
    fn compressed_topk_matches_quantized_scalar_domains() {
        for dim in [1, 3, 17, 63, 64, 65, 127, 128, 129, 768, 769] {
            for center in [0.0f32, 8192.0] {
                let n = 37;
                let nq = 5;
                let mut db = gen(n, dim, 17);
                let mut queries = gen(nq, dim, 19);
                for x in db.iter_mut().chain(queries.iter_mut()) {
                    *x += center;
                }
                db[3 * dim..4 * dim].copy_from_slice(&queries[..dim]);
                for kind in kinds() {
                    let p = Prepared::new(&db, dim, &[kind]).unwrap();
                    for k in [1, 10, n] {
                        let actual = p.topk(kind, &queries, dim, k);
                        let mut expected: Vec<_> = (0..nq).map(|_| TopK::new(k)).collect();
                        if matches!(kind, Kind::F16 | Kind::Bf16) {
                            let bf = kind == Kind::Bf16;
                            let q = Packed16::encode(&queries, dim, bf).unwrap();
                            let x = if bf {
                                p.bf16.as_ref().unwrap()
                            } else {
                                p.half.as_ref().unwrap()
                            };
                            for qi in 0..nq {
                                for id in 0..n {
                                    expected[qi].push(
                                        x86::canonical_l2(&q.decoded(qi), &x.decoded(id)),
                                        id as u32,
                                    );
                                }
                            }
                        } else {
                            let q = Packed8::encode(&queries, dim).unwrap();
                            let x = p.int8.as_ref().unwrap();
                            for qi in 0..nq {
                                for id in 0..n {
                                    let dot: i64 = (0..dim)
                                        .map(|i| {
                                            i64::from(q.data[qi * q.stride + i])
                                                * i64::from(x.data[id * x.stride + i])
                                        })
                                        .sum();
                                    let d = int8_distance(&q, qi, x, id, dot);
                                    let exact: f64 = (0..dim)
                                        .map(|i| {
                                            let v = q.value(qi, i) - x.value(id, i);
                                            v * v
                                        })
                                        .sum();
                                    let allowance = 8.0 * f32::EPSILON as f64 * exact.abs()
                                        + 128.0
                                            * f64::EPSILON
                                            * (q.scales[qi].powi(2) * q.norms[qi] as f64
                                                + x.scales[id].powi(2) * x.norms[id] as f64);
                                    assert!(
                                        (f64::from(d) - exact).abs()
                                            <= allowance + f32::MIN_POSITIVE as f64
                                    );
                                    expected[qi].push(d, id as u32);
                                }
                            }
                        }
                        assert_eq!(
                            bits(&actual),
                            bits(&expected),
                            "kind={kind:?} dim={dim} k={k} center={center}"
                        );
                    }
                }
            }
        }
    }

    #[test]
    fn quantization_scales_cover_tiny_large_and_zero_vectors() {
        for scale in [0.0f32, 1e-44, 1e-20, 0.1, 1.0, 1e18, 1e30] {
            let mut v = gen(7, 129, 7);
            for x in &mut v {
                *x *= scale;
            }
            let p = Packed8::encode(&v, 129).unwrap();
            for row in 0..7 {
                let max = v[row * 129..(row + 1) * 129]
                    .iter()
                    .map(|x| f64::from(*x).abs())
                    .fold(0.0f64, f64::max);
                for i in 0..129 {
                    assert!(
                        (p.value(row, i) - f64::from(v[row * 129 + i])).abs()
                            <= max / 254.0 + max * 1e-12
                    );
                }
                assert!(p.data[row * p.stride + 129..(row + 1) * p.stride]
                    .iter()
                    .all(|x| *x == 0));
            }
        }
    }

    #[test]
    fn bfloat_refinement_handles_overflow_and_underflow() {
        if !Kind::Bf16.available() {
            return;
        }
        for dim in [1, 17, 129] {
            for (center, scale) in [
                (0.0f32, 1e-40f32),
                (0.0, 1e30),
                (1e30, 1e24),
                (f32::from_bits(0x7f7f0000), 0.0),
            ] {
                let mut db = gen(19, dim, 1);
                let mut queries = gen(5, dim, 3);
                for v in db.iter_mut().chain(queries.iter_mut()) {
                    *v = center + scale * *v;
                }
                let p = Prepared::new(&db, dim, &[Kind::Bf16]).unwrap();
                let q = Packed16::encode(&queries, dim, true).unwrap();
                let x = p.bf16.as_ref().unwrap();
                for k in [1, 10, 19] {
                    let actual = p.topk(Kind::Bf16, &queries, dim, k);
                    let mut expected: Vec<_> = (0..5).map(|_| TopK::new(k)).collect();
                    for qi in 0..5 {
                        for id in 0..19 {
                            expected[qi]
                                .push(x86::canonical_l2(&q.decoded(qi), &x.decoded(id)), id as u32);
                        }
                    }
                    assert_eq!(
                        bits(&actual),
                        bits(&expected),
                        "dim={dim} center={center} scale={scale}"
                    );
                }
            }
        }
    }
}
