//! Shared relational estimates and physical properties used by CBO v2.
//!
//! These types deliberately keep cardinality/width outside [`super::Cost`].
//! Plan nodes keep execution metadata, while `RelEstimate` is the single
//! optimizer-side source of truth for cardinality and width.

use std::collections::BTreeMap;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum EstimateSource {
    Exact,
    Analyzed,
    Histogram,
    MostCommonValue,
    Constraint,
    Derived,
    Fallback,
}

impl EstimateSource {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Exact => "exact",
            Self::Analyzed => "analyzed",
            Self::Histogram => "histogram",
            Self::MostCommonValue => "mcv",
            Self::Constraint => "constraint",
            Self::Derived => "derived",
            Self::Fallback => "fallback",
        }
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub struct ColumnEstimate {
    pub ndv: Option<f64>,
    pub null_fraction: Option<f64>,
    pub average_width: Option<f64>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct RelEstimate {
    pub rows: f64,
    pub average_width: f64,
    pub columns: BTreeMap<String, ColumnEstimate>,
    pub source: EstimateSource,
    /// Confidence in `[0, 1]`; used for diagnostics and conservative
    /// fallback selection, never as an opaque multiplier on cost.
    pub confidence: f64,
}

impl RelEstimate {
    pub fn new(rows: f64, average_width: f64, source: EstimateSource, confidence: f64) -> Self {
        Self {
            rows: rows.max(0.0),
            average_width: average_width.max(0.0),
            columns: BTreeMap::new(),
            source,
            confidence: confidence.clamp(0.0, 1.0),
        }
    }

    #[cfg(test)]
    pub fn is_valid(&self) -> bool {
        self.rows.is_finite()
            && self.rows >= 0.0
            && self.average_width.is_finite()
            && self.average_width >= 0.0
            && self.confidence.is_finite()
            && (0.0..=1.0).contains(&self.confidence)
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
pub enum ExecutionMode {
    #[default]
    Row,
    Vectorized,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Hash)]
pub struct PhysicalProperties {
    /// Canonical expression strings for the guaranteed output ordering.
    pub ordering: Vec<String>,
    pub execution_mode: ExecutionMode,
    pub parallel_degree: u32,
    /// Number of consecutive hash-join levels on the probe spine. The
    /// executor consumes such a spine as one streaming pipeline, so plans
    /// with different depths are not interchangeable memo alternatives even
    /// when their scalar costs are nearly equal.
    pub streaming_join_depth: u8,
    /// Canonical keys supplied by an outer relation for parameterized paths.
    pub parameterized_by: Vec<String>,
    pub rewindable: bool,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rel_estimate_allows_zero_but_rejects_non_finite_values() {
        assert!(RelEstimate::new(0.0, 0.0, EstimateSource::Exact, 1.0).is_valid());
        let mut invalid = RelEstimate::new(1.0, 8.0, EstimateSource::Fallback, 0.2);
        invalid.rows = f64::NAN;
        assert!(!invalid.is_valid());
    }
}
