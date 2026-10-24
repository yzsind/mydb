//! Optional SQ8 navigation over native floating payloads.
use super::distance::Metric;
use crate::vector::VectorValue;

#[derive(Clone, Copy, Debug)]
pub(super) struct NativeQuant {
    pub mid: f64,
    pub scale: f64,
    pub metric: Metric,
}
impl NativeQuant {
    pub fn bounds(low: f64, high: f64, metric: Metric) -> Option<Self> {
        if !low.is_finite() || !high.is_finite() {
            return None;
        }
        Some(Self {
            mid: (low + high) * 0.5,
            scale: if high > low {
                (high - low) / 254.0
            } else {
                1.0
            },
            metric,
        })
    }
    pub fn norm(value: &VectorValue, metric: Metric) -> f64 {
        if metric == Metric::Cosine {
            (0..value.dimension as usize)
                .map(|i| value.element(i).powi(2))
                .sum::<f64>()
                .sqrt()
        } else {
            1.0
        }
    }
    pub fn observe(value: &VectorValue, metric: Metric, low: &mut f64, high: &mut f64) {
        let norm = Self::norm(value, metric);
        for i in 0..value.dimension as usize {
            let x = value.element(i) / norm;
            *low = low.min(x);
            *high = high.max(x);
        }
    }
    pub fn encode(self, value: &VectorValue) -> Option<Vec<i8>> {
        let norm = Self::norm(value, self.metric);
        if norm == 0.0 || !norm.is_finite() {
            return None;
        }
        let mut out = Vec::with_capacity(value.dimension as usize);
        for i in 0..value.dimension as usize {
            let x = (value.element(i) / norm - self.mid) / self.scale;
            if !x.is_finite() || !(-127.0..=127.0).contains(&x) {
                return None;
            }
            out.push(x.round_ties_even() as i8);
        }
        Some(out)
    }
    pub fn score(self, a: &[i8], b: &[i8]) -> f64 {
        let sum = super::distance::l2_ssd_i8(a, b) as f64;
        sum * self.scale
            * self.scale
            * if self.metric == Metric::Cosine {
                0.5
            } else {
                1.0
            }
    }
}
