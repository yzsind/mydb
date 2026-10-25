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
        for (i, &x) in values.iter().enumerate() {
            self.low[i] = self.low[i].min(x);
            self.high[i] = self.high[i].max(x);
            self.mean[i] += (x - self.mean[i]) / self.count as f64;
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
            version: 1,
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
                let _scratch = ControlledMemoryCharge::try_new_recoverable(
                    ControlledMemoryCategory::StorageScratch,
                    k.saturating_mul(d.div_ceil(parts)).saturating_mul(16) + self.sample.len() * 8,
                )?;
                model.offsets = Vec::with_capacity(parts + 1);
                model.centers = Vec::with_capacity(parts);
                model.offsets.push(0);
                for part in 0..parts {
                    let start = model.offsets[part];
                    let width = d / parts + usize::from(part < d % parts);
                    let end = start + width;
                    model.offsets.push(end);
                    let mut centers = Vec::with_capacity(k * width);
                    for center in 0..k {
                        centers.extend_from_slice(
                            &self.sample[center * self.sample.len() / k][start..end],
                        );
                    }
                    let mut assigned = vec![usize::MAX; self.sample.len()];
                    for _ in 0..iterations {
                        crate::engine::cancel::check_cancelled()?;
                        let mut sums = vec![0.0; k * width];
                        let mut counts = vec![0usize; k];
                        let mut changed = false;
                        for (i, row) in self.sample.iter().enumerate() {
                            let mut best = (f64::INFINITY, 0);
                            for (c, center) in centers.chunks_exact(width).enumerate() {
                                let dist = row[start..end]
                                    .iter()
                                    .zip(center)
                                    .map(|(x, y)| (x - y) * (x - y))
                                    .sum::<f64>();
                                if dist < best.0 {
                                    best = (dist, c);
                                }
                            }
                            changed |= assigned[i] != best.1;
                            assigned[i] = best.1;
                            counts[best.1] += 1;
                            for j in 0..width {
                                sums[best.1 * width + j] += row[start + j];
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
                    model.centers.push(centers);
                }
            }
            QuantizationConfig::None => return Ok(None),
        }
        Ok(Some(Arc::new(Quantizer {
            model,
            _charge: model_charge,
        })))
    }
}

pub(crate) struct QuantQuery {
    values: Vec<f64>,
    norm2: f64,
    lut: Vec<[f64; 2]>,
    bq_lut: Vec<f64>,
    center_dot: f64,
    _charge: ControlledMemoryCharge,
}
impl Quantizer {
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
        serde_json::to_vec(&(config, &self.model)).map_err(|e| codec::vector_err(e.to_string()))
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
            _charge: charge,
        };
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
                for (part, centers) in self.model.centers.iter().enumerate() {
                    let start = self.model.offsets[part];
                    let end = self.model.offsets[part + 1];
                    let mut best = (f64::INFINITY, 0);
                    for (c, center) in centers.chunks_exact(end - start).enumerate() {
                        let dist = v[start..end]
                            .iter()
                            .zip(center)
                            .map(|(x, y)| (x - y) * (x - y))
                            .sum::<f64>();
                        if dist < best.0 {
                            best = (dist, c);
                        }
                    }
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
        let mut bytes = vec![0; self.record_len()];
        if let Some(code) = self.encode(value) {
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
        if let Some(prepared) = prepared {
            Ok(self.score(&bytes[9..], prepared))
        } else {
            let prepared = self.prepare(query)?;
            Ok(self.score(&bytes[9..], &prepared))
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
