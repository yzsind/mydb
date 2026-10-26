//! Shared compressed payloads and asymmetric scoring for native ANN indexes.
use super::{codec, distance::Metric};
use crate::engine::executor::mem_budget::{ControlledMemoryCategory, ControlledMemoryCharge};
use crate::{
    storage::{vector_config::QuantizationConfig, VectorIndexFile},
    types::QueryError,
    vector::{VectorDataType, VectorValue},
};
use serde::{Deserialize, Serialize};
use std::sync::Arc;

#[derive(Debug, Serialize, Deserialize)]
struct Model {
    version: u32,
    config: QuantizationConfig,
    datatype: VectorDataType,
    dimension: u32,
    metric: u8,
    trained_rows: usize,
    low: Vec<f64>,
    high: Vec<f64>,
    scale: Vec<f64>,
    centers: Vec<Vec<f64>>,
    offsets: Vec<usize>,
}
#[derive(Debug)]
pub(crate) struct Quantizer {
    model: Model,
    pq_centers: Vec<Vec<f64>>,
    _charge: ControlledMemoryCharge,
}
#[derive(Debug)]
pub(crate) struct Trainer {
    config: QuantizationConfig,
    datatype: VectorDataType,
    dimension: u32,
    metric: Metric,
    count: usize,
    low: Vec<f64>,
    high: Vec<f64>,
    mean: Vec<f64>,
    sample: Vec<Vec<f64>>,
    random: u64,
    charge: ControlledMemoryCharge,
}
fn dense(value: &VectorValue, metric: Metric) -> Vec<f64> {
    let mut values: Vec<_> = (0..value.dimension as usize)
        .map(|i| value.element(i))
        .collect();
    if metric == Metric::Cosine {
        let norm = values.iter().map(|x| x * x).sum::<f64>().sqrt();
        if norm > 0.0 {
            for v in &mut values {
                *v /= norm;
            }
        }
    }
    values
}
fn norm2(values: &[f64]) -> f64 {
    values.iter().map(|v| v * v).sum()
}
fn nibble(code: &[u8], i: usize) -> usize {
    ((code[i / 2] >> ((i % 2) * 4)) & 15) as usize
}
fn put_nibble(code: &mut [u8], i: usize, v: usize) {
    code[i / 2] |= (v as u8) << ((i % 2) * 4);
}
impl Trainer {
    pub(crate) fn new(
        config: QuantizationConfig,
        datatype: VectorDataType,
        dimension: u32,
        metric: Metric,
    ) -> Result<Self, QueryError> {
        if dimension == 0
            || dimension > datatype.max_dimension()
            || datatype == VectorDataType::Binary
            || !matches!(metric, Metric::L2 | Metric::Cosine | Metric::Dot)
        {
            return Err(codec::vector_err(
                "quantization requires a fixed numeric VECTOR and numeric metric",
            ));
        }
        if let QuantizationConfig::Pq {
            m,
            bits,
            train_samples,
            iterations,
        } = config
        {
            let parts = m.unwrap_or(dimension.div_ceil(8));
            if parts == 0
                || parts > dimension
                || !matches!(bits, 4 | 8)
                || train_samples < (1u32 << bits)
                || train_samples > 1_048_576
                || !(1..=1000).contains(&iterations)
            {
                return Err(codec::vector_err(
                    "invalid PQ training configuration for vector dimension",
                ));
            }
        }
        let charge = ControlledMemoryCharge::try_new_recoverable(
            ControlledMemoryCategory::StorageScratch,
            dimension as usize * 32,
        )?;
        Ok(Self {
            config,
            datatype,
            dimension,
            metric,
            count: 0,
            low: vec![f64::INFINITY; dimension as usize],
            high: vec![f64::NEG_INFINITY; dimension as usize],
            mean: vec![0.0; dimension as usize],
            sample: Vec::new(),
            random: 0x9173_abcd_5531_0001,
            charge,
        })
    }
    pub(crate) fn observe(&mut self, value: &VectorValue) -> Result<(), QueryError> {
        crate::engine::cancel::check_cancelled()?;
        let values = dense(value, self.metric);
        if values.iter().any(|v| !v.is_finite()) {
            return Err(codec::vector_err("non-finite quantizer input"));
        }
        self.count += 1;
        match self.config {
            QuantizationConfig::Sq8 | QuantizationConfig::Sq4 => {
                for (i, &x) in values.iter().enumerate() {
                    self.low[i] = self.low[i].min(x);
                    self.high[i] = self.high[i].max(x);
                }
            }
            QuantizationConfig::Bq => {
                for (i, &x) in values.iter().enumerate() {
                    self.mean[i] += (x - self.mean[i]) / self.count as f64;
                }
            }
            _ => {}
        }
        if let QuantizationConfig::Pq { train_samples, .. } = self.config {
            let limit = train_samples as usize;
            if self.sample.len() < limit {
                self.charge.try_grow_recoverable(values.len() * 8 + 32)?;
                self.sample.push(values);
            } else {
                self.random ^= self.random << 13;
                self.random ^= self.random >> 7;
                self.random ^= self.random << 17;
                let slot = (self.random % self.count as u64) as usize;
                if slot < limit {
                    self.sample[slot] = values;
                }
            }
        }
        Ok(())
    }
    pub(crate) fn finish(self) -> Result<Option<Arc<Quantizer>>, QueryError> {
        if self.count == 0 {
            return Ok(None);
        }
        let mut model = Model {
            version: 2,
            config: self.config.clone(),
            datatype: self.datatype,
            dimension: self.dimension,
            metric: self.metric.as_u8(),
            trained_rows: self.count,
            low: Vec::new(),
            high: Vec::new(),
            scale: Vec::new(),
            centers: Vec::new(),
            offsets: Vec::new(),
        };
        let mut model_charge =
            ControlledMemoryCharge::try_new_recoverable(ControlledMemoryCategory::DerivedCache, 0)?;
        match self.config {
            QuantizationConfig::Sq8 | QuantizationConfig::Sq4 => {
                model_charge.try_grow_recoverable(self.dimension as usize * 24)?;
                let levels = if matches!(self.config, QuantizationConfig::Sq8) {
                    255.0
                } else {
                    15.0
                };
                model.scale = self
                    .low
                    .iter()
                    .zip(&self.high)
                    .map(|(lo, hi)| (hi - lo) / levels)
                    .collect();
                model.low = self.low;
                model.high = self.high;
            }
            QuantizationConfig::Bq => {
                model_charge.try_grow_recoverable(self.dimension as usize * 8)?;
                model.low = self.mean;
            }
            QuantizationConfig::Pq {
                m,
                bits,
                iterations,
                ..
            } => {
                let k = 1usize << bits;
                if self.count < k {
                    return Ok(None);
                }
                let parts = m.unwrap_or(self.dimension.div_ceil(8)) as usize;
                let d = self.dimension as usize;
                model_charge.try_grow_recoverable(
                    k.saturating_mul(d).saturating_mul(8) + (parts + 1) * 8 + parts * 24,
                )?;
                let mut threads = std::thread::available_parallelism()
                    .map_or(1, |n| n.get())
                    .min(parts)
                    .min(16);
                let base_scratch =
                    self.sample.len() * 8 + k * d.div_ceil(parts) * 24 + k * 8 + 1024 * 1024;
                let row_scratch = self.sample.len() * d.div_ceil(parts) * 8;
                // Cache-friendly subspace copies are optional. Under pressure,
                // reduce concurrency, then train directly from the shared sample.
                let (compact_rows, _scratch) = loop {
                    if let Ok(charge) = ControlledMemoryCharge::try_new_recoverable(
                        ControlledMemoryCategory::StorageScratch,
                        threads * (base_scratch + row_scratch),
                    ) {
                        break (true, charge);
                    }
                    if threads > 1 {
                        threads = threads.div_ceil(2);
                        continue;
                    }
                    break (
                        false,
                        ControlledMemoryCharge::try_new_recoverable(
                            ControlledMemoryCategory::StorageScratch,
                            base_scratch,
                        )?,
                    );
                };
                model.offsets = Vec::with_capacity(parts + 1);
                model.offsets.push(0);
                for part in 0..parts {
                    model
                        .offsets
                        .push(model.offsets[part] + d / parts + usize::from(part < d % parts));
                }
                model.centers = vec![Vec::new(); parts];
                let cancellation = crate::engine::cancel::CancellationSnapshot::capture();
                std::thread::scope(|scope| -> Result<(), QueryError> {
                    let mut handles = Vec::new();
                    for (chunk, output) in model
                        .centers
                        .chunks_mut(parts.div_ceil(threads))
                        .enumerate()
                    {
                        let sample = &self.sample;
                        let offsets = &model.offsets;
                        let cancellation = &cancellation;
                        handles.push(scope.spawn(move || -> Result<(), QueryError> {
                            let _scope = cancellation.enter();
                            for (local, centers) in output.iter_mut().enumerate() {
                                let part = chunk * parts.div_ceil(threads) + local;
                                *centers = train_pq_part(
                                    sample,
                                    offsets[part],
                                    offsets[part + 1],
                                    k,
                                    iterations,
                                    compact_rows,
                                )?;
                            }
                            Ok(())
                        }));
                    }
                    for handle in handles {
                        handle
                            .join()
                            .map_err(|_| codec::vector_err("PQ training worker panicked"))??;
                    }
                    Ok(())
                })?;
            }
            QuantizationConfig::None => return Ok(None),
        }
        Ok(Some(Quantizer::from_model(model, model_charge)?))
    }
}

// Eight independent centers retain coordinate summation order while allowing
// SIMD across centers. The persisted model remains in its canonical row layout.
fn transpose_pq_centers(centers: &[f64], width: usize) -> Vec<f64> {
    let mut blocked = Vec::with_capacity(centers.len());
    for block in centers.chunks_exact(width * 8) {
        for dim in 0..width {
            for center in 0..8 {
                blocked.push(block[center * width + dim]);
            }
        }
    }
    blocked
}

fn nearest_pq_center(values: &[f64], centers: &[f64]) -> (f64, usize) {
    #[cfg(target_arch = "x86_64")]
    if std::arch::is_x86_feature_detected!("avx2") {
        // SAFETY: runtime dispatch checks the required CPU feature.
        return unsafe { nearest_pq_center_avx2(values, centers) };
    }
    nearest_pq_center_portable(values, centers)
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2")]
unsafe fn nearest_pq_center_avx2(values: &[f64], centers: &[f64]) -> (f64, usize) {
    nearest_pq_center_portable(values, centers)
}

#[inline(always)]
fn nearest_pq_center_portable(values: &[f64], centers: &[f64]) -> (f64, usize) {
    let mut best = (f64::INFINITY, 0);
    for (block, centers) in centers.chunks_exact(values.len() * 8).enumerate() {
        let mut distances = [0.0; 8];
        for (&x, coordinates) in values.iter().zip(centers.chunks_exact(8)) {
            for lane in 0..8 {
                let delta = x - coordinates[lane];
                distances[lane] += delta * delta;
            }
        }
        for (lane, distance) in distances.into_iter().enumerate() {
            if distance < best.0 {
                best = (distance, block * 8 + lane);
            }
        }
    }
    best
}

fn train_pq_part(
    sample: &[Vec<f64>],
    start: usize,
    end: usize,
    k: usize,
    iterations: u32,
    compact_rows: bool,
) -> Result<Vec<f64>, QueryError> {
    let width = end - start;
    let rows = compact_rows.then(|| {
        let mut rows = Vec::with_capacity(sample.len() * width);
        for row in sample {
            rows.extend_from_slice(&row[start..end]);
        }
        rows
    });
    let mut centers = Vec::with_capacity(k * width);
    for center in 0..k {
        centers.extend_from_slice(&sample[center * sample.len() / k][start..end]);
    }
    let mut assigned = vec![usize::MAX; sample.len()];
    let mut sums = vec![0.0; k * width];
    let mut counts = vec![0usize; k];
    for _ in 0..iterations {
        crate::engine::cancel::check_cancelled()?;
        let blocked = transpose_pq_centers(&centers, width);
        sums.fill(0.0);
        counts.fill(0);
        let mut changed = false;
        for (i, sample_row) in sample.iter().enumerate() {
            let row = rows.as_ref().map_or(&sample_row[start..end], |rows| {
                &rows[i * width..(i + 1) * width]
            });
            if i % 256 == 0 {
                crate::engine::cancel::check_cancelled()?;
            }
            let (_, best) = nearest_pq_center(row, &blocked);
            changed |= assigned[i] != best;
            assigned[i] = best;
            counts[best] += 1;
            for j in 0..width {
                sums[best * width + j] += row[j];
            }
        }
        for c in 0..k {
            if counts[c] > 0 {
                for j in 0..width {
                    centers[c * width + j] = sums[c * width + j] / counts[c] as f64;
                }
            }
        }
        if !changed {
            break;
        }
    }
    Ok(centers)
}

pub(crate) struct QuantQuery {
    values: Vec<f64>,
    norm2: f64,
    lut: Vec<[f64; 2]>,
    bq_lut: Vec<f64>,
    center_dot: f64,
    sq_weights: Vec<f64>,
    _charge: ControlledMemoryCharge,
}
/// Independent reductions allow LLVM to vectorize byte-to-f64 scoring on
/// NEON/AVX without reassociating a serial IEEE sum or narrowing native values.
fn sq8_weighted_dot(code: &[u8], weights: &[f64]) -> f64 {
    #[cfg(target_arch = "aarch64")]
    {
        unsafe { sq8_weighted_dot_neon(code, weights) }
    }
    #[cfg(not(target_arch = "aarch64"))]
    {
        sq8_weighted_dot_portable(code, weights)
    }
}

fn sq4_weighted_dot(code: &[u8], weights: &[f64]) -> f64 {
    #[cfg(target_arch = "x86_64")]
    if std::arch::is_x86_feature_detected!("avx2") {
        // SAFETY: dispatched only on CPUs with AVX2; the kernel bounds loads.
        return unsafe { sq4_weighted_dot_avx2(code, weights) };
    }
    sq4_weighted_dot_portable(code, weights)
}

fn sq4_weighted_dot_portable(code: &[u8], weights: &[f64]) -> f64 {
    let mut sums = [0.0; 8];
    for (block, values) in weights.chunks(8).enumerate() {
        for (lane, &weight) in values.iter().enumerate() {
            sums[lane] += nibble(code, block * 8 + lane) as f64 * weight;
        }
    }
    sums.into_iter().sum()
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2")]
unsafe fn sq4_weighted_dot_avx2(code: &[u8], weights: &[f64]) -> f64 {
    use std::arch::x86_64::*;
    let mut sums = [_mm256_setzero_pd(); 2];
    let mask = _mm_set1_epi8(15);
    let mut i = 0;
    while i + 8 <= weights.len() {
        // Four packed bytes become eight consecutive unsigned coordinates.
        // Reads are unaligned and bounded by ceil(dimension / 2) bytes.
        let packed = _mm_cvtsi32_si128(std::ptr::read_unaligned(code.as_ptr().add(i / 2).cast()));
        let low = _mm_and_si128(packed, mask);
        let high = _mm_and_si128(_mm_srli_epi16(packed, 4), mask);
        let codes = _mm256_cvtepu8_epi32(_mm_unpacklo_epi8(low, high));
        let a = _mm256_cvtepi32_pd(_mm256_castsi256_si128(codes));
        let b = _mm256_cvtepi32_pd(_mm256_extracti128_si256(codes, 1));
        sums[0] = _mm256_add_pd(
            sums[0],
            _mm256_mul_pd(a, _mm256_loadu_pd(weights.as_ptr().add(i))),
        );
        sums[1] = _mm256_add_pd(
            sums[1],
            _mm256_mul_pd(b, _mm256_loadu_pd(weights.as_ptr().add(i + 4))),
        );
        i += 8;
    }
    let mut lanes = [0.0; 8];
    _mm256_storeu_pd(lanes.as_mut_ptr(), sums[0]);
    _mm256_storeu_pd(lanes.as_mut_ptr().add(4), sums[1]);
    let mut result = lanes.into_iter().sum::<f64>();
    while i < weights.len() {
        result += nibble(code, i) as f64 * weights[i];
        i += 1;
    }
    result
}

#[cfg(target_arch = "aarch64")]
unsafe fn sq8_weighted_dot_neon(code: &[u8], weights: &[f64]) -> f64 {
    use std::arch::aarch64::*;
    let n = code.len().min(weights.len());
    let mut sums = [vdupq_n_f64(0.0); 4];
    let mut i = 0;
    while i + 8 <= n {
        // Widen whole groups instead of scalar lane extraction/shuffling.
        // Both slices are in bounds; NEON permits these unaligned loads.
        let bytes = vmovl_u8(vld1_u8(code.as_ptr().add(i)));
        let low = vmovl_u16(vget_low_u16(bytes));
        let high = vmovl_u16(vget_high_u16(bytes));
        let values = [
            vcvtq_f64_u64(vmovl_u32(vget_low_u32(low))),
            vcvtq_f64_u64(vmovl_u32(vget_high_u32(low))),
            vcvtq_f64_u64(vmovl_u32(vget_low_u32(high))),
            vcvtq_f64_u64(vmovl_u32(vget_high_u32(high))),
        ];
        for lane in 0..4 {
            sums[lane] = vfmaq_f64(
                sums[lane],
                values[lane],
                vld1q_f64(weights.as_ptr().add(i + lane * 2)),
            );
        }
        i += 8;
    }
    let mut result = vaddvq_f64(vaddq_f64(
        vaddq_f64(sums[0], sums[1]),
        vaddq_f64(sums[2], sums[3]),
    ));
    while i < n {
        result += code[i] as f64 * weights[i];
        i += 1;
    }
    result
}

#[cfg(not(target_arch = "aarch64"))]
fn sq8_weighted_dot_portable(code: &[u8], weights: &[f64]) -> f64 {
    let mut sums = [0.0; 8];
    let mut codes = code.chunks_exact(8);
    let mut values = weights.chunks_exact(8);
    for (code, weight) in codes.by_ref().zip(values.by_ref()) {
        for lane in 0..8 {
            sums[lane] += code[lane] as f64 * weight[lane];
        }
    }
    let tail = codes
        .remainder()
        .iter()
        .zip(values.remainder())
        .map(|(&c, &w)| c as f64 * w)
        .sum::<f64>();
    sums.into_iter().sum::<f64>() + tail
}

impl Quantizer {
    fn from_model(
        model: Model,
        mut charge: ControlledMemoryCharge,
    ) -> Result<Arc<Self>, QueryError> {
        charge.try_grow_recoverable(model.centers.iter().map(|v| v.len() * 8 + 24).sum())?;
        let pq_centers = model
            .centers
            .iter()
            .enumerate()
            .map(|(part, centers)| {
                transpose_pq_centers(centers, model.offsets[part + 1] - model.offsets[part])
            })
            .collect();
        Ok(Arc::new(Self {
            model,
            pq_centers,
            _charge: charge,
        }))
    }

    pub(crate) fn code_len(&self) -> usize {
        self.model.config.code_len(self.model.dimension)
    }
    pub(crate) fn record_len(&self) -> usize {
        self.code_len() + 9
    }
    pub(crate) fn trained_rows(&self) -> usize {
        self.model.trained_rows
    }
    pub(crate) fn name(&self) -> &'static str {
        self.model.config.name()
    }
    pub(crate) fn bytes(&self) -> usize {
        self.model.low.len() * 8
            + self.model.high.len() * 8
            + self.model.scale.len() * 8
            + self
                .model
                .centers
                .iter()
                .map(|v| v.len() * 8 + 24)
                .sum::<usize>()
            + self.model.offsets.len() * 8
            + self
                .pq_centers
                .iter()
                .map(|v| v.len() * 8 + 24)
                .sum::<usize>()
    }
    fn metric(&self) -> Metric {
        match self.model.metric {
            0 => Metric::L2,
            1 => Metric::Cosine,
            2 => Metric::Dot,
            _ => unreachable!("numeric quantization metric"),
        }
    }
    pub(crate) fn serialize(
        &self,
        config: Option<&crate::storage::vector_config::VectorIndexConfig>,
    ) -> Result<Vec<u8>, QueryError> {
        let mut bytes = Vec::new();
        let mut writer =
            super::persistence::Writer::new(&mut bytes, 8).map_err(super::persistence::io_error)?;
        writer.u32(2).map_err(super::persistence::io_error)?;
        writer
            .u8(self.model.datatype as u8)
            .map_err(super::persistence::io_error)?;
        writer
            .u32(self.model.dimension)
            .map_err(super::persistence::io_error)?;
        writer
            .u8(self.model.metric)
            .map_err(super::persistence::io_error)?;
        writer
            .u64(self.model.trained_rows as u64)
            .map_err(super::persistence::io_error)?;
        writer
            .bytes(
                &serde_json::to_vec(&self.model.config)
                    .map_err(|_| super::persistence::corrupt())?,
            )
            .map_err(super::persistence::io_error)?;
        writer
            .bytes(&serde_json::to_vec(&config).map_err(|_| super::persistence::corrupt())?)
            .map_err(super::persistence::io_error)?;
        fn put_floats(
            writer: &mut super::persistence::Writer<&mut Vec<u8>>,
            values: &[f64],
        ) -> Result<(), QueryError> {
            writer
                .count(values.len())
                .map_err(super::persistence::io_error)?;
            for value in values {
                if !value.is_finite() {
                    return Err(super::persistence::corrupt());
                }
                writer
                    .u64(value.to_bits())
                    .map_err(super::persistence::io_error)?;
            }
            Ok(())
        }
        for values in [&self.model.low, &self.model.high, &self.model.scale] {
            put_floats(&mut writer, values)?;
        }
        writer
            .count(self.model.centers.len())
            .map_err(super::persistence::io_error)?;
        for center in &self.model.centers {
            put_floats(&mut writer, center)?;
        }
        writer
            .count(self.model.offsets.len())
            .map_err(super::persistence::io_error)?;
        for offset in &self.model.offsets {
            writer
                .count(*offset)
                .map_err(super::persistence::io_error)?;
        }
        writer.finish().map_err(super::persistence::io_error)?;
        Ok(bytes)
    }

    pub(crate) fn matches_definition(
        &self,
        datatype: VectorDataType,
        dimension: u32,
        metric: Metric,
        config: &QuantizationConfig,
    ) -> bool {
        self.model.datatype == datatype
            && self.model.dimension == dimension
            && self.model.metric == metric.as_u8()
            && &self.model.config == config
    }

    pub(crate) fn deserialize(bytes: &[u8]) -> Result<Arc<Self>, QueryError> {
        let mut reader = super::persistence::Reader::new(bytes, 8)?;
        if reader.u32()? != 2 {
            return Err(super::persistence::corrupt());
        }
        let datatype =
            VectorDataType::from_tag(reader.u8()?).ok_or_else(super::persistence::corrupt)?;
        let dimension = reader.u32()?;
        let metric = reader.u8()?;
        let trained_rows =
            usize::try_from(reader.u64()?).map_err(|_| super::persistence::corrupt())?;
        if datatype == VectorDataType::Binary
            || dimension == 0
            || dimension > datatype.max_dimension()
            || metric > 2
            || trained_rows == 0
        {
            return Err(super::persistence::corrupt());
        }
        let config_bytes = reader.bytes()?;
        if config_bytes.len() > 8192 {
            return Err(super::persistence::corrupt());
        }
        let config: QuantizationConfig =
            serde_json::from_slice(config_bytes).map_err(|_| super::persistence::corrupt())?;
        let definition = reader.bytes()?;
        if definition.len() > 8192 {
            return Err(super::persistence::corrupt());
        }
        let definition: Option<crate::storage::vector_config::VectorIndexConfig> =
            serde_json::from_slice(definition).map_err(|_| super::persistence::corrupt())?;
        if definition
            .as_ref()
            .is_some_and(|definition| definition.quantization != config)
        {
            return Err(super::persistence::corrupt());
        }
        let d = dimension as usize;
        let (parts, k, scalars) = match &config {
            QuantizationConfig::Sq8 | QuantizationConfig::Sq4 => (0, 0, d * 3),
            QuantizationConfig::Bq => (0, 0, d),
            QuantizationConfig::Pq {
                m,
                bits,
                train_samples,
                iterations,
            } => {
                let parts = m.unwrap_or(dimension.div_ceil(8)) as usize;
                if !matches!(bits, 4 | 8)
                    || parts == 0
                    || parts > d
                    || *iterations == 0
                    || (*train_samples as usize) < (1usize << bits)
                    || trained_rows < (1usize << bits)
                {
                    return Err(super::persistence::corrupt());
                }
                let k = 1usize << bits;
                (parts, k, d * k)
            }
            QuantizationConfig::None => return Err(super::persistence::corrupt()),
        };
        let charge = ControlledMemoryCharge::try_new_recoverable(
            ControlledMemoryCategory::DerivedCache,
            scalars
                .saturating_mul(8)
                .saturating_add(parts.saturating_mul(32))
                .saturating_add(4096),
        )?;
        fn read_floats(
            reader: &mut super::persistence::Reader<'_>,
            expected: usize,
        ) -> Result<Vec<f64>, QueryError> {
            if reader.count(8)? != expected {
                return Err(super::persistence::corrupt());
            }
            let mut out = Vec::with_capacity(expected);
            for _ in 0..expected {
                let value = f64::from_bits(reader.u64()?);
                if !value.is_finite() {
                    return Err(super::persistence::corrupt());
                }
                out.push(value);
            }
            Ok(out)
        }
        let low = read_floats(
            &mut reader,
            if matches!(
                config,
                QuantizationConfig::Sq8 | QuantizationConfig::Sq4 | QuantizationConfig::Bq
            ) {
                d
            } else {
                0
            },
        )?;
        let high = read_floats(
            &mut reader,
            if matches!(config, QuantizationConfig::Sq8 | QuantizationConfig::Sq4) {
                d
            } else {
                0
            },
        )?;
        let scale = read_floats(
            &mut reader,
            if matches!(config, QuantizationConfig::Sq8 | QuantizationConfig::Sq4) {
                d
            } else {
                0
            },
        )?;
        if low.iter().zip(&high).any(|(lo, hi)| lo > hi) || scale.iter().any(|value| *value < 0.0) {
            return Err(super::persistence::corrupt());
        }
        if reader.count(4)? != parts {
            return Err(super::persistence::corrupt());
        }
        let mut centers = Vec::with_capacity(parts);
        for part in 0..parts {
            centers.push(read_floats(
                &mut reader,
                k * (d / parts + usize::from(part < d % parts)),
            )?);
        }
        let offsets_count = reader.count(4)?;
        if offsets_count != if parts == 0 { 0 } else { parts + 1 } {
            return Err(super::persistence::corrupt());
        }
        let mut offsets = Vec::with_capacity(offsets_count);
        let mut expected = 0;
        for part in 0..offsets_count {
            let offset = reader.u32()? as usize;
            if offset != expected {
                return Err(super::persistence::corrupt());
            }
            offsets.push(offset);
            if part < parts {
                expected += d / parts + usize::from(part < d % parts);
            }
        }
        reader.finish()?;
        let model = Model {
            version: 2,
            config,
            datatype,
            dimension,
            metric,
            trained_rows,
            low,
            high,
            scale,
            centers,
            offsets,
        };
        Self::from_model(model, charge)
    }

    pub(crate) fn prepare(&self, value: &VectorValue) -> Result<QuantQuery, QueryError> {
        self.prepare_values(|| dense(value, self.metric()))
    }
    fn prepare_values(
        &self,
        make_values: impl FnOnce() -> Vec<f64>,
    ) -> Result<QuantQuery, QueryError> {
        let extra = match self.model.config {
            QuantizationConfig::Pq { bits, .. } => self.model.centers.len() * (1usize << bits) * 16,
            QuantizationConfig::Bq => (self.model.dimension as usize).div_ceil(4) * 16 * 8,
            QuantizationConfig::Sq8 | QuantizationConfig::Sq4 => self.model.dimension as usize * 8,
            _ => 0,
        };
        let charge = ControlledMemoryCharge::try_new_recoverable(
            ControlledMemoryCategory::StorageScratch,
            self.model.dimension as usize * 8 + extra,
        )?;
        let values = make_values();
        let norm2 = norm2(&values);
        let mut q = QuantQuery {
            values,
            norm2,
            lut: Vec::new(),
            bq_lut: Vec::new(),
            center_dot: 0.0,
            sq_weights: Vec::new(),
            _charge: charge,
        };
        if matches!(
            self.model.config,
            QuantizationConfig::Sq8 | QuantizationConfig::Sq4
        ) && self.metric() != Metric::L2
        {
            q.sq_weights = q
                .values
                .iter()
                .zip(&self.model.scale)
                .map(|(x, scale)| x * scale)
                .collect();
            q.center_dot = q
                .values
                .iter()
                .zip(&self.model.low)
                .map(|(x, lo)| x * lo)
                .sum();
        }
        if let QuantizationConfig::Pq { bits, .. } = self.model.config {
            q.lut = Vec::with_capacity(self.model.centers.len() * (1usize << bits));
            for (part, centers) in self.model.centers.iter().enumerate() {
                let start = self.model.offsets[part];
                let end = self.model.offsets[part + 1];
                for center in centers.chunks_exact(end - start) {
                    let score = if self.metric() == Metric::L2 {
                        q.values[start..end]
                            .iter()
                            .zip(center)
                            .map(|(x, y)| (x - y) * (x - y))
                            .sum()
                    } else {
                        q.values[start..end]
                            .iter()
                            .zip(center)
                            .map(|(x, y)| x * y)
                            .sum()
                    };
                    q.lut.push([score, norm2_slice(center)]);
                }
            }
        }
        if matches!(self.model.config, QuantizationConfig::Bq) {
            q.bq_lut = Vec::with_capacity((self.model.dimension as usize).div_ceil(4) * 16);
            q.center_dot = q
                .values
                .iter()
                .zip(&self.model.low)
                .map(|(x, y)| x * y)
                .sum();
            for chunk in q.values.chunks(4) {
                for mask in 0..16 {
                    q.bq_lut.push(
                        chunk
                            .iter()
                            .enumerate()
                            .map(|(i, x)| if mask & (1 << i) != 0 { *x } else { -*x })
                            .sum(),
                    );
                }
            }
        }
        Ok(q)
    }
    /// Build graph probes directly from reconstructed f64 components. Narrowing
    /// a compressed node to FLOAT32 can erase subnormal BQ amplitudes.
    pub(crate) fn prepare_payload(
        &self,
        bytes: &[u8],
        file: Option<&Arc<VectorIndexFile>>,
    ) -> Result<QuantQuery, QueryError> {
        if bytes.first() == Some(&1) {
            return self.prepare(&self.exception(bytes, file)?);
        }
        if bytes.len() != self.record_len() || bytes[0] != 0 {
            return Err(crate::storage::error(
                crate::error::catalog::ER_GET_ERRNO,
                "corrupt compressed vector record",
            ));
        }
        self.prepare_values(|| {
            let mut values: Vec<_> = (0..self.model.dimension as usize)
                .map(|i| self.component(&bytes[9..], i))
                .collect();
            if self.metric() == Metric::Cosine {
                let norm = norm2(&values).sqrt();
                if norm > 0.0 {
                    for v in &mut values {
                        *v /= norm;
                    }
                }
            }
            values
        })
    }
    pub(crate) fn encode(&self, value: &VectorValue) -> Option<Vec<u8>> {
        let v = dense(value, self.metric());
        let mut code = vec![0; self.code_len()];
        match self.model.config {
            QuantizationConfig::Sq8 | QuantizationConfig::Sq4 => {
                let levels = if matches!(self.model.config, QuantizationConfig::Sq8) {
                    255.0
                } else {
                    15.0
                };
                for (i, &x) in v.iter().enumerate() {
                    let lo = self.model.low[i];
                    let scale = self.model.scale[i];
                    if x < lo || x > self.model.high[i] {
                        return None;
                    }
                    let c = if scale == 0.0 {
                        if x != lo {
                            return None;
                        }
                        0
                    } else {
                        ((x - lo) / scale).round_ties_even().clamp(0.0, levels) as usize
                    };
                    if levels == 255.0 {
                        code[i] = c as u8;
                    } else {
                        put_nibble(&mut code, i, c);
                    }
                }
            }
            QuantizationConfig::Pq { bits, .. } => {
                for part in 0..self.model.centers.len() {
                    let start = self.model.offsets[part];
                    let end = self.model.offsets[part + 1];
                    let best = nearest_pq_center(&v[start..end], &self.pq_centers[part]);
                    if bits == 8 {
                        code[part] = best.1 as u8;
                    } else {
                        put_nibble(&mut code, part, best.1);
                    }
                }
            }
            QuantizationConfig::Bq => {
                let offset = (self.model.dimension as usize).div_ceil(8);
                let a = v
                    .iter()
                    .zip(&self.model.low)
                    .map(|(x, c)| (x - c).abs())
                    .sum::<f64>()
                    / v.len() as f64;
                let mut cs = 0.0;
                let mut recon_norm = 0.0;
                for (i, (&x, &c)) in v.iter().zip(&self.model.low).enumerate() {
                    let s = if x >= c {
                        code[i / 8] |= 1 << (i % 8);
                        1.0
                    } else {
                        -1.0
                    };
                    cs += c * s;
                    let y = c + a * s;
                    if y.abs() > f32::MAX as f64 {
                        return None;
                    }
                    recon_norm += y * y;
                }
                code[offset..offset + 8].copy_from_slice(&a.to_le_bytes());
                code[offset + 8..offset + 16].copy_from_slice(&cs.to_le_bytes());
                code[offset + 16..offset + 24].copy_from_slice(&recon_norm.to_le_bytes());
            }
            QuantizationConfig::None => return None,
        }
        if self.metric() == Metric::Cosine && (0..v.len()).all(|i| self.component(&code, i) == 0.0)
        {
            return None;
        }
        Some(code)
    }
    fn component(&self, code: &[u8], i: usize) -> f64 {
        match self.model.config {
            QuantizationConfig::Sq8 => self.model.low[i] + self.model.scale[i] * code[i] as f64,
            QuantizationConfig::Sq4 => {
                self.model.low[i] + self.model.scale[i] * nibble(code, i) as f64
            }
            QuantizationConfig::Pq { bits, .. } => {
                let part = self.model.offsets.partition_point(|&offset| offset <= i) - 1;
                let width = self.model.offsets[part + 1] - self.model.offsets[part];
                let c = if bits == 8 {
                    code[part] as usize
                } else {
                    nibble(code, part)
                };
                self.model.centers[part][c * width + i - self.model.offsets[part]]
            }
            QuantizationConfig::Bq => {
                let offset = (self.model.dimension as usize).div_ceil(8);
                let a = f64::from_le_bytes(code[offset..offset + 8].try_into().unwrap());
                self.model.low[i]
                    + a * if code[i / 8] & (1 << (i % 8)) != 0 {
                        1.0
                    } else {
                        -1.0
                    }
            }
            QuantizationConfig::None => unreachable!(),
        }
    }
    pub(crate) fn score(&self, code: &[u8], query: &QuantQuery) -> f64 {
        let metric = self.metric();
        let mut score = 0.0;
        let mut norm = 0.0;
        match self.model.config {
            QuantizationConfig::Sq4 => {
                // Decode packed pairs directly. Calling component() for every
                // coordinate repeats the model dispatch and bounds checks in
                // the innermost navigation loop. Independent lanes expose the
                // same full-width affine arithmetic to SIMD, including tails.
                let mut scores = [0.0; 8];
                let mut norms = [0.0; 8];
                for (block, values) in query.values.chunks(8).enumerate() {
                    for (lane, &x) in values.iter().enumerate() {
                        let i = block * 8 + lane;
                        let c = ((code[i / 2] >> ((i % 2) * 4)) & 15) as f64;
                        let y = self.model.low[i] + self.model.scale[i] * c;
                        scores[lane] += if metric == Metric::L2 {
                            (x - y) * (x - y)
                        } else {
                            x * y
                        };
                        if metric == Metric::Cosine {
                            norms[lane] += y * y;
                        }
                    }
                }
                score = scores.into_iter().sum();
                norm = norms.into_iter().sum();
            }
            QuantizationConfig::Sq8 => {
                // Keep per-dimension affine scales and full f64 range. Explicit
                // independent lanes expose SIMD without a scalar component()
                // call or a metric/config dispatch for every coordinate.
                let mut scores = [0.0; 8];
                let mut norms = [0.0; 8];
                for (block, chunk) in code.chunks(8).enumerate() {
                    for (lane, &c) in chunk.iter().enumerate() {
                        let i = block * 8 + lane;
                        let y = self.model.low[i] + self.model.scale[i] * c as f64;
                        let x = query.values[i];
                        scores[lane] += if metric == Metric::L2 {
                            (x - y) * (x - y)
                        } else {
                            x * y
                        };
                        norms[lane] += y * y;
                    }
                }
                score = scores.into_iter().sum();
                norm = norms.into_iter().sum();
            }
            QuantizationConfig::Pq { bits, .. } => {
                let k = 1usize << bits;
                for part in 0..self.model.centers.len() {
                    let c = if bits == 8 {
                        code[part] as usize
                    } else {
                        nibble(code, part)
                    };
                    let entry = query.lut[part * k + c];
                    score += entry[0];
                    norm += entry[1];
                }
            }
            QuantizationConfig::Bq => {
                let d = self.model.dimension as usize;
                let offset = d.div_ceil(8);
                let a = f64::from_le_bytes(code[offset..offset + 8].try_into().unwrap());
                norm = f64::from_le_bytes(code[offset + 16..offset + 24].try_into().unwrap());
                let weighted = (0..d.div_ceil(4))
                    .map(|block| query.bq_lut[block * 16 + nibble(code, block)])
                    .sum::<f64>();
                score = query.center_dot + a * weighted;
                if metric == Metric::L2 {
                    let value = (query.norm2 + norm - 2.0 * score).max(0.0);
                    if value > 1e-12 * (query.norm2 + norm) {
                        return value;
                    }
                    return query
                        .values
                        .iter()
                        .enumerate()
                        .map(|(i, x)| (x - self.component(code, i)).powi(2))
                        .sum();
                }
            }
            _ => {
                for (i, &x) in query.values.iter().enumerate() {
                    let y = self.component(code, i);
                    if metric == Metric::L2 {
                        score += (x - y) * (x - y);
                    } else {
                        score += x * y;
                        norm += y * y;
                    }
                }
            }
        }
        match metric {
            Metric::L2 => score,
            Metric::Dot => -score,
            Metric::Cosine => {
                if norm > 0.0 && query.norm2 > 0.0 {
                    1.0 - (score / (query.norm2 * norm).sqrt()).clamp(-1.0, 1.0)
                } else {
                    f64::INFINITY
                }
            }
            _ => unreachable!(),
        }
    }
    pub(crate) fn store(
        &self,
        value: &VectorValue,
        file: Option<&Arc<VectorIndexFile>>,
    ) -> Result<VectorValue, QueryError> {
        let _scratch = ControlledMemoryCharge::try_new_recoverable(
            ControlledMemoryCategory::StorageScratch,
            self.model.dimension as usize * 8 + self.record_len() * 2 + value.len() + 12,
        )?;
        self.store_with_reserved_scratch(value, file)
    }

    /// The caller must hold admission for dense values, payloads and encoding
    /// workspace for the whole call. Bulk writers reuse that bounded workspace.
    pub(crate) fn store_with_reserved_scratch(
        &self,
        value: &VectorValue,
        file: Option<&Arc<VectorIndexFile>>,
    ) -> Result<VectorValue, QueryError> {
        let mut bytes = vec![0; self.record_len()];
        if let Some(code) = self.encode(value) {
            // The header's auxiliary word stores the reconstructed cosine norm.
            // Exception records use the same word for their blob pointer.
            if self.metric() == Metric::Cosine
                && matches!(
                    self.model.config,
                    QuantizationConfig::Sq8 | QuantizationConfig::Sq4
                )
            {
                let norm = self
                    .model
                    .low
                    .iter()
                    .zip(&self.model.scale)
                    .enumerate()
                    .map(|(i, (&lo, &scale))| {
                        let c = if matches!(self.model.config, QuantizationConfig::Sq4) {
                            nibble(&code, i) as f64
                        } else {
                            code[i] as f64
                        };
                        (lo + scale * c).powi(2)
                    })
                    .sum::<f64>();
                bytes[1..9].copy_from_slice(&norm.to_le_bytes());
            }
            bytes[9..].copy_from_slice(&code);
        } else {
            bytes[0] = 1;
            if let Some(file) = file {
                let raw = value.encode();
                let head = file.append_quantization_blob(&raw)?;
                bytes[1..9].copy_from_slice(&head.to_le_bytes());
            } else {
                bytes.truncate(9);
                bytes.extend(value.encode());
            }
        }
        Ok(VectorValue::new(
            VectorDataType::Int8,
            bytes.len() as u32,
            bytes,
        ))
    }
    fn exception(
        &self,
        bytes: &[u8],
        file: Option<&Arc<VectorIndexFile>>,
    ) -> Result<VectorValue, QueryError> {
        let _scratch = ControlledMemoryCharge::try_new_recoverable(
            ControlledMemoryCategory::StorageScratch,
            (self.model.datatype.payload_len(self.model.dimension) + 12) * 3,
        )?;
        let raw = if let Some(file) = file {
            file.read_quantization_blob(
                u64::from_le_bytes(bytes[1..9].try_into().unwrap()),
                self.model.datatype.payload_len(self.model.dimension) + 12,
            )?
        } else {
            bytes[9..].to_vec()
        };
        VectorValue::decode(&raw).ok_or_else(|| {
            crate::storage::error(
                crate::error::catalog::ER_GET_ERRNO,
                "corrupt quantization exception",
            )
        })
    }
    pub(crate) fn score_payload(
        &self,
        bytes: &[u8],
        file: Option<&Arc<VectorIndexFile>>,
        query: &VectorValue,
        prepared: Option<&QuantQuery>,
    ) -> Result<f64, QueryError> {
        if bytes.first() == Some(&1) {
            let value = self.exception(bytes, file)?;
            if let Some(prepared) = prepared {
                let norm = if self.metric() == Metric::Cosine {
                    (0..value.dimension as usize)
                        .map(|i| value.element(i).powi(2))
                        .sum::<f64>()
                        .sqrt()
                } else {
                    1.0
                };
                if norm == 0.0 {
                    return Ok(f64::INFINITY);
                }
                let mut result = 0.0;
                for (i, &x) in prepared.values.iter().enumerate() {
                    let y = value.element(i) / norm;
                    result += if self.metric() == Metric::L2 {
                        (x - y).powi(2)
                    } else {
                        x * y
                    };
                }
                return Ok(match self.metric() {
                    Metric::L2 => result,
                    Metric::Dot => -result,
                    Metric::Cosine => 1.0 - (result / prepared.norm2.sqrt()).clamp(-1.0, 1.0),
                    _ => unreachable!(),
                });
            }
            return super::distance::typed_ordering_distance(query, &value, self.metric());
        }
        if bytes.len() != self.record_len() || bytes[0] != 0 {
            return Err(crate::storage::error(
                crate::error::catalog::ER_GET_ERRNO,
                "corrupt compressed vector record",
            ));
        }
        let score = |prepared: &QuantQuery| {
            if !prepared.sq_weights.is_empty() {
                let norm = f64::from_le_bytes(bytes[1..9].try_into().unwrap());
                // Older records have a zero auxiliary word. Keep their exact
                // decoder as the fallback, including zero-norm validation.
                if self.metric() == Metric::Dot || (norm > 0.0 && norm.is_finite()) {
                    let weighted = if matches!(self.model.config, QuantizationConfig::Sq4) {
                        sq4_weighted_dot(&bytes[9..], &prepared.sq_weights)
                    } else {
                        sq8_weighted_dot(&bytes[9..], &prepared.sq_weights)
                    };
                    let dot = prepared.center_dot + weighted;
                    if dot.abs() <= (prepared.center_dot.abs() + weighted.abs()) * 1e-12 {
                        return self.score(&bytes[9..], prepared);
                    }
                    return match self.metric() {
                        Metric::Dot => -dot,
                        Metric::Cosine if prepared.norm2 > 0.0 => {
                            1.0 - (dot / (prepared.norm2 * norm).sqrt()).clamp(-1.0, 1.0)
                        }
                        _ => f64::INFINITY,
                    };
                }
            }
            self.score(&bytes[9..], prepared)
        };
        if let Some(prepared) = prepared {
            Ok(score(prepared))
        } else {
            Ok(score(&self.prepare(query)?))
        }
    }
    pub(crate) fn value_payload(
        &self,
        bytes: &[u8],
        file: Option<&Arc<VectorIndexFile>>,
    ) -> Result<VectorValue, QueryError> {
        if bytes.first() == Some(&1) {
            return self.exception(bytes, file);
        }
        if bytes.len() != self.record_len() || bytes[0] != 0 {
            return Err(crate::storage::error(
                crate::error::catalog::ER_GET_ERRNO,
                "corrupt compressed vector record",
            ));
        }
        let values: Vec<f32> = (0..self.model.dimension as usize)
            .map(|i| self.component(&bytes[9..], i) as f32)
            .collect();
        Ok(codec::from_f32(&values))
    }
}
fn norm2_slice(values: &[f64]) -> f64 {
    norm2(values)
}

#[cfg(test)]
mod tests {
    use super::*;
    fn configs() -> Vec<QuantizationConfig> {
        vec![
            QuantizationConfig::Sq8,
            QuantizationConfig::Sq4,
            QuantizationConfig::Pq {
                m: Some(3),
                bits: 4,
                train_samples: 32,
                iterations: 3,
            },
            QuantizationConfig::Bq,
        ]
    }
    #[test]
    fn pq_training_compact_and_shared_samples_are_identical_and_cancellable() {
        let sample: Vec<Vec<f64>> = (0..64)
            .map(|row| {
                (0..11)
                    .map(|dim| ((row * (dim + 3)) % 101) as f64)
                    .collect()
            })
            .collect();
        assert_eq!(
            train_pq_part(&sample, 1, 10, 16, 5, true).unwrap(),
            train_pq_part(&sample, 1, 10, 16, 5, false).unwrap()
        );
        let _deadline =
            crate::engine::cancel::install_statement_deadline(std::time::Duration::from_millis(1));
        std::thread::sleep(std::time::Duration::from_millis(2));
        let snapshot = crate::engine::cancel::CancellationSnapshot::capture();
        std::thread::scope(|scope| {
            scope
                .spawn(|| {
                    let _scope = snapshot.enter();
                    assert!(train_pq_part(&sample, 0, 11, 16, 5, true).is_err());
                })
                .join()
                .unwrap();
        });
    }

    #[test]
    fn pq_blocked_nearest_matches_scalar_including_ties() {
        for width in [1, 2, 3, 7, 8, 9, 17, 33, 128] {
            for k in [16, 256] {
                let centers: Vec<f64> = (0..k * width)
                    .map(|i| ((i * 7919 % 997) as f64 - 498.0) / 997.0)
                    .collect();
                let blocked = transpose_pq_centers(&centers, width);
                for row in 0..32 {
                    let values: Vec<f64> = (0..width)
                        .map(|i| ((i * 37 + row * 17) % 97) as f64 / 97.0)
                        .collect();
                    let mut expected = (f64::INFINITY, 0);
                    for (id, center) in centers.chunks_exact(width).enumerate() {
                        let distance = values
                            .iter()
                            .zip(center)
                            .map(|(x, y)| (x - y) * (x - y))
                            .sum::<f64>();
                        if distance < expected.0 {
                            expected = (distance, id);
                        }
                    }
                    assert_eq!(nearest_pq_center_portable(&values, &blocked), expected);
                    assert_eq!(nearest_pq_center(&values, &blocked), expected);
                }
                let zeros = vec![0.0; k * width];
                assert_eq!(nearest_pq_center(&vec![0.0; width], &zeros), (0.0, 0));
            }
        }
    }

    #[test]
    fn sq4_weighted_reduction_matches_scalar_with_odd_and_unaligned_codes() {
        for dimension in [1usize, 2, 7, 8, 9, 31, 128, 768, 769, 4096] {
            let mut buffer = vec![0xff; dimension.div_ceil(2) + 1];
            let code = &mut buffer[1..];
            for (i, byte) in code.iter_mut().enumerate() {
                *byte = i.wrapping_mul(71) as u8;
            }
            for magnitude in [1e-100, 1.0, 1e100] {
                let weights: Vec<_> = (0..dimension)
                    .map(|i| ((i * 13 % 119) as f64 - 59.0) * magnitude)
                    .collect();
                let reference = weights
                    .iter()
                    .enumerate()
                    .map(|(i, &w)| nibble(code, i) as f64 * w)
                    .sum::<f64>();
                for actual in [
                    sq4_weighted_dot(code, &weights),
                    sq4_weighted_dot_portable(code, &weights),
                ] {
                    assert!(
                        (actual - reference).abs() <= 1e-12 * magnitude * dimension as f64 * 15.0
                    );
                }
            }
        }
    }

    #[test]
    fn sq8_weighted_reduction_matches_scalar() {
        for dimension in [1, 7, 8, 9, 31, 128, 769, 4096] {
            let code: Vec<_> = (0..dimension).map(|i| (i * 71) as u8).collect();
            for magnitude in [1e-100, 1.0, 1e100] {
                let weights: Vec<_> = (0..dimension)
                    .map(|i| ((i * 13 % 119) as f64 - 59.0) * magnitude)
                    .collect();
                let reference = code
                    .iter()
                    .zip(&weights)
                    .map(|(&c, &w)| c as f64 * w)
                    .sum::<f64>();
                let actual = sq8_weighted_dot(&code, &weights);
                assert!((actual - reference).abs() <= 1e-12 * magnitude * dimension as f64 * 255.0);
            }
        }
    }

    #[test]
    fn quantized_asymmetric_scores_match_scalar_reconstruction() {
        for datatype in [
            VectorDataType::Float32,
            VectorDataType::Float16,
            VectorDataType::BFloat16,
            VectorDataType::Int8,
        ] {
            for metric in [Metric::L2, Metric::Cosine, Metric::Dot] {
                for config in configs() {
                    let mut trainer = Trainer::new(config, datatype, 7, metric).unwrap();
                    let values: Vec<_> = (0..32)
                        .map(|row| {
                            codec::parse_typed_text(
                                &format!(
                                    "[{}]",
                                    (0..7)
                                        .map(|j| ((row * (j + 3) + j * j) % 39 - 19).to_string())
                                        .collect::<Vec<_>>()
                                        .join(",")
                                ),
                                datatype,
                            )
                            .unwrap()
                        })
                        .collect();
                    for value in &values {
                        trainer.observe(value).unwrap();
                    }
                    let q = trainer.finish().unwrap().unwrap();
                    let query =
                        codec::parse_text("[0.25,1.125,-3.5,2.75,4.25,-0.125,6.5]").unwrap();
                    let prepared = q.prepare(&query).unwrap();
                    let mut encoded = 0;
                    for value in values {
                        if let Some(code) = q.encode(&value) {
                            encoded += 1;
                            assert_eq!(code.len(), q.code_len());
                            // This reference uses scalar geometry, independently of the PQ/BQ LUTs.
                            let source = dense(&query, metric);
                            let reconstructed: Vec<_> =
                                (0..7).map(|i| q.component(&code, i)).collect();
                            let reference = match metric {
                                Metric::L2 => source
                                    .iter()
                                    .zip(&reconstructed)
                                    .map(|(a, b)| (a - b).powi(2))
                                    .sum(),
                                Metric::Dot => -source
                                    .iter()
                                    .zip(&reconstructed)
                                    .map(|(a, b)| a * b)
                                    .sum::<f64>(),
                                Metric::Cosine => {
                                    1.0 - (source
                                        .iter()
                                        .zip(&reconstructed)
                                        .map(|(a, b)| a * b)
                                        .sum::<f64>()
                                        / (norm2(&source) * norm2(&reconstructed)).sqrt())
                                    .clamp(-1.0, 1.0)
                                }
                                _ => unreachable!(),
                            };
                            let mut stored = q.store(&value, None).unwrap();
                            let payload_score = q
                                .score_payload(&stored.payload, None, &query, Some(&prepared))
                                .unwrap();
                            assert!((payload_score - reference).abs() <= 1e-10 * reference.abs().max(1.0), "payload {datatype:?} {metric:?} {}: {payload_score} != {reference}", q.name());
                            if matches!(q.model.config, QuantizationConfig::Sq4)
                                && metric == Metric::Cosine
                            {
                                assert!(
                                    f64::from_le_bytes(stored.payload[1..9].try_into().unwrap())
                                        > 0.0
                                );
                                stored.payload[1..9].fill(0);
                                let legacy = q
                                    .score_payload(&stored.payload, None, &query, Some(&prepared))
                                    .unwrap();
                                assert!(
                                    (legacy - reference).abs() <= 1e-10 * reference.abs().max(1.0)
                                );
                            }
                            let actual = q.score(&code, &prepared);
                            assert!(
                                (actual - reference).abs() <= 1e-10 * reference.abs().max(1.0),
                                "{datatype:?} {metric:?} {}: {actual} != {reference}",
                                q.name()
                            );
                        }
                    }
                    assert!(encoded >= 24);
                }
            }
        }
    }
    #[test]
    fn bq_preserves_amplitude_for_l2_and_dot() {
        for metric in [Metric::L2, Metric::Dot] {
            let mut trainer =
                Trainer::new(QuantizationConfig::Bq, VectorDataType::Float32, 3, metric).unwrap();
            let values: Vec<_> = (1..=32)
                .map(|n| codec::parse_text(&format!("[{n},{n},{n}]")).unwrap())
                .collect();
            for v in &values {
                trainer.observe(v).unwrap();
            }
            let q = trainer.finish().unwrap().unwrap();
            let query = codec::parse_text("[31,31,31]").unwrap();
            let prepared = q.prepare(&query).unwrap();
            let a = q.score(&q.encode(&values[0]).unwrap(), &prepared);
            let b = q.score(&q.encode(&values[30]).unwrap(), &prepared);
            assert!(b < a, "{metric:?}: {a}, {b}");
        }
    }
    #[test]
    fn sq_outliers_use_lossless_exceptions_and_pq_waits_for_training() {
        let mut trainer = Trainer::new(
            QuantizationConfig::Sq4,
            VectorDataType::Float32,
            3,
            Metric::L2,
        )
        .unwrap();
        trainer
            .observe(&codec::parse_text("[1,1,1]").unwrap())
            .unwrap();
        let q = trainer.finish().unwrap().unwrap();
        let value = codec::parse_text("[2,3,4]").unwrap();
        let stored = q.store(&value, None).unwrap();
        assert_eq!(stored.payload[0], 1);
        assert_eq!(q.value_payload(&stored.payload, None).unwrap(), value);
        let mut trainer = Trainer::new(
            QuantizationConfig::Pq {
                m: Some(2),
                bits: 8,
                train_samples: 256,
                iterations: 2,
            },
            VectorDataType::Float32,
            3,
            Metric::Dot,
        )
        .unwrap();
        trainer.observe(&value).unwrap();
        assert!(trainer.finish().unwrap().is_none());
    }
}

#[cfg(test)]
mod failure_tests {
    use super::*;
    #[test]
    fn quantizer_cancellation_and_failed_admission_release_memory() {
        let before = crate::engine::executor::mem_budget::controlled_memory_snapshot().0;
        crate::engine::executor::mem_budget::with_test_effective_memory_limit(1024 * 1024, || {
            let error = Trainer::new(
                QuantizationConfig::Sq8,
                VectorDataType::Int8,
                65532,
                Metric::L2,
            )
            .unwrap_err();
            assert!(error.is(crate::error::catalog::ER_OUTOFMEMORY));
        });
        assert_eq!(
            crate::engine::executor::mem_budget::controlled_memory_snapshot().0,
            before
        );
        let mut trainer = Trainer::new(
            QuantizationConfig::Bq,
            VectorDataType::Float32,
            3,
            Metric::L2,
        )
        .unwrap();
        let token = crate::engine::cancel::QueryCancelToken::noop().fork_local();
        {
            let _scope = crate::engine::cancel::enter(token.clone());
            token.cancel_local();
            assert!(trainer
                .observe(&codec::parse_text("[1,2,3]").unwrap())
                .unwrap_err()
                .is(crate::error::catalog::ER_QUERY_INTERRUPTED));
        }
        trainer
            .observe(&codec::parse_text("[1,2,3]").unwrap())
            .unwrap();
        drop(trainer.finish().unwrap());
        assert_eq!(
            crate::engine::executor::mem_budget::controlled_memory_snapshot().0,
            before
        );
    }
}

#[cfg(test)]
mod subnormal_probes {
    use super::*;
    #[test]
    fn bq_code_to_code_probe_retains_subnormal_amplitudes() {
        let mut trainer = Trainer::new(
            QuantizationConfig::Bq,
            VectorDataType::Float32,
            129,
            Metric::L2,
        )
        .unwrap();
        let mut values = vec![0.0; 129];
        values[0] = f32::from_bits(1);
        let a = codec::from_f32(&values);
        values[0] = -values[0];
        let b = codec::from_f32(&values);
        trainer.observe(&a).unwrap();
        trainer.observe(&b).unwrap();
        let q = trainer.finish().unwrap().unwrap();
        let stored = q.store(&a, None).unwrap();
        let query = q.prepare_payload(&stored.payload, None).unwrap();
        assert!(query.values.iter().any(|x| *x != 0.0));
        assert_eq!(q.score(&q.encode(&a).unwrap(), &query), 0.0);
        assert!(q.score(&q.encode(&b).unwrap(), &query) > 0.0);
    }
}

#[cfg(test)]
mod calibration_regressions {
    use super::*;
    #[test]
    fn sq_calibration_encodes_every_observed_finite_endpoint() {
        let dim = 1536;
        let mut random = 0x1357_2468_ace0_0123u64;
        let values: Vec<_> = (0..257)
            .map(|_| {
                let v: Vec<_> = (0..dim)
                    .map(|j| {
                        random ^= random << 13;
                        random ^= random >> 7;
                        random ^= random << 17;
                        ((random >> 32) as f64 / u32::MAX as f64 * 2.0 - 1.0
                            + if j % 17 == 0 { 1000.0 } else { 0.0 }) as f32
                    })
                    .collect();
                codec::from_f32(&v)
            })
            .collect();
        for metric in [Metric::L2, Metric::Cosine, Metric::Dot] {
            for config in [QuantizationConfig::Sq8, QuantizationConfig::Sq4] {
                let mut trainer =
                    Trainer::new(config, VectorDataType::Float32, dim, metric).unwrap();
                for value in &values {
                    trainer.observe(value).unwrap();
                }
                let quantizer = trainer.finish().unwrap().unwrap();
                for (row, value) in values.iter().enumerate() {
                    assert!(
                        quantizer.encode(value).is_some(),
                        "{} {metric:?} rejected build row {row}",
                        quantizer.name()
                    );
                }
            }
        }
    }
    #[test]
    fn initial_cosine_exceptions_do_not_invalidate_the_new_model() {
        let dim = 1024;
        let mut trainer = Trainer::new(
            QuantizationConfig::Sq4,
            VectorDataType::Float32,
            dim,
            Metric::Cosine,
        )
        .unwrap();
        for axis in 0..dim as usize {
            let mut v = vec![0.0; dim as usize];
            v[axis] = 1.0;
            trainer.observe(&codec::from_f32(&v)).unwrap();
        }
        let dense = codec::from_f32(&vec![1.0; dim as usize]);
        for _ in 0..100 {
            trainer.observe(&dense).unwrap();
        }
        let quantizer = trainer.finish().unwrap().unwrap();
        assert!(quantizer.encode(&dense).is_none());
        let mut index = super::super::native_ivf::NativeIvf::new(
            VectorDataType::Float32,
            dim,
            Metric::Cosine,
            super::super::ivf::IvfParams::default(),
            None,
        )
        .unwrap();
        index.set_quantizer(true, Some(quantizer), 1);
        for axis in 0..dim as usize {
            let mut v = vec![0.0; dim as usize];
            v[axis] = 1.0;
            index
                .insert(
                    crate::storage::RowId {
                        page_id: axis as u32,
                        slot_id: 0,
                    },
                    codec::from_f32(&v),
                )
                .unwrap();
        }
        for row in 0..100 {
            index
                .insert(
                    crate::storage::RowId {
                        page_id: dim + row,
                        slot_id: 0,
                    },
                    dense.clone(),
                )
                .unwrap();
        }
        assert!(index.is_valid());
        assert_eq!(index.quantization_stats().2, 100);
    }
}

/// An untrained compressed index persists exact exceptions in the same container
/// format used by a trained model. It remains a complete index while training waits.
pub(crate) fn store_untrained(
    value: &VectorValue,
    file: Option<&Arc<VectorIndexFile>>,
) -> Result<VectorValue, QueryError> {
    let Some(file) = file else {
        return Ok(value.clone());
    };
    let length = file.native_payload_bytes()?;
    if length < 9 {
        return Err(super::persistence::corrupt());
    }
    let head = file.append_quantization_blob(&value.encode())?;
    let mut payload = vec![0; length];
    payload[0] = 1;
    payload[1..9].copy_from_slice(&head.to_le_bytes());
    Ok(VectorValue::new(
        VectorDataType::Int8,
        length as u32,
        payload,
    ))
}

pub(crate) fn load_untrained(
    payload: &[u8],
    file: &Arc<VectorIndexFile>,
    datatype: VectorDataType,
    dimension: u32,
) -> Result<VectorValue, QueryError> {
    if payload.len() < 9 || payload[0] != 1 {
        return Err(super::persistence::corrupt());
    }
    let head = u64::from_le_bytes(
        payload[1..9]
            .try_into()
            .map_err(|_| super::persistence::corrupt())?,
    );
    let bytes = file.read_quantization_blob(head, datatype.payload_len(dimension) + 12)?;
    let value = VectorValue::decode(&bytes).ok_or_else(super::persistence::corrupt)?;
    codec::validate_internal(&value)?;
    if value.datatype != datatype || value.dimension != dimension {
        return Err(super::persistence::corrupt());
    }
    Ok(value)
}
