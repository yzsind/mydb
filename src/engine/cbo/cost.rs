//! Structured cost vector used throughout the cost-based optimizer.
//!
//! Each `Cost` value carries:
//! - `startup`: the cost of producing the first output row (everything that
//!   must complete before a pipeline-breaking operator can yield), and
//! - `total`: the cost of producing all output rows.
//!
//! Diagnostic components (`io_seq_pages`, `io_rnd_pages`, `cpu_units`,
//! `mem_bytes`, `spill_bytes`) are surfaced by `EXPLAIN FORMAT=JSON`. They
//! do not participate in plan comparison directly; comparison is driven by
//! `total` (with `startup` used for LIMIT-aware path selection in later
//! phases).
//!
//! All unit-bearing numbers are expressed in *cost units*. The unit basis is
//! `cbo_seq_page_cost = 1.0` (see `super::consts`); every other coefficient
//! is calibrated relative to it. Switching hardware therefore only requires
//! re-running the calibration tool, not rewriting any formula.

use std::cmp::Ordering;

#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub struct Cost {
    /// Cost incurred before the first output row can be emitted. Equal to
    /// `total` for fully pipeline-breaking operators (Sort, HashJoin build
    /// side, HashAggregate without spill).
    pub startup: f64,
    /// Total cost of producing the entire result set. Used for plan
    /// comparison via [`Cost::cmp_total`].
    pub total: f64,
    // ---- diagnostic decomposition (surfaced in EXPLAIN FORMAT=JSON) ----
    pub io_seq_pages: f64,
    pub io_rnd_pages: f64,
    pub cpu_units: f64,
    pub mem_bytes: f64,
    pub spill_bytes: f64,
}

impl Cost {
    pub const ZERO: Cost = Cost {
        startup: 0.0,
        total: 0.0,
        io_seq_pages: 0.0,
        io_rnd_pages: 0.0,
        cpu_units: 0.0,
        mem_bytes: 0.0,
        spill_bytes: 0.0,
    };

    /// Construct a `Cost` from a single scalar `total`. Used during the
    /// Phase 1 transition where legacy callers still produce a single
    /// number; later phases populate the structured fields directly via
    /// `from_components`.
    #[inline]
    pub fn scalar(total: f64) -> Cost {
        Cost {
            startup: 0.0,
            total,
            ..Cost::ZERO
        }
    }

    #[cfg(test)]
    /// Build a fully-populated `Cost`. The decomposition fields exist for
    /// EXPLAIN diagnostics; `total` is what the optimizer compares.
    #[inline]
    pub fn from_components(
        startup: f64,
        total: f64,
        io_seq_pages: f64,
        io_rnd_pages: f64,
        cpu_units: f64,
        mem_bytes: f64,
        spill_bytes: f64,
    ) -> Cost {
        Cost {
            startup,
            total,
            io_seq_pages,
            io_rnd_pages,
            cpu_units,
            mem_bytes,
            spill_bytes,
        }
    }

    /// The scalar value used by the optimizer to compare two plans. Phase 1
    /// keeps this equal to `total`; later phases may introduce a weighted
    /// combination (e.g. memory pressure penalty), but all callers go
    /// through this one accessor so the policy stays in one place.
    #[inline]
    pub fn weighted_total(&self) -> f64 {
        if self.is_valid() {
            self.total
        } else {
            f64::INFINITY
        }
    }

    /// Whether this vector can safely participate in plan comparison.
    /// Invalid candidates are retained for diagnostics but always lose to a
    /// valid candidate.
    pub fn is_valid(&self) -> bool {
        self.startup.is_finite()
            && self.total.is_finite()
            && self.startup >= 0.0
            && self.total >= self.startup
            && self.io_seq_pages.is_finite()
            && self.io_seq_pages >= 0.0
            && self.io_rnd_pages.is_finite()
            && self.io_rnd_pages >= 0.0
            && self.cpu_units.is_finite()
            && self.cpu_units >= 0.0
            && self.mem_bytes.is_finite()
            && self.mem_bytes >= 0.0
            && self.spill_bytes.is_finite()
            && self.spill_bytes >= 0.0
    }

    #[inline]
    pub fn cmp_total(&self, other: &Cost) -> Ordering {
        self.weighted_total().total_cmp(&other.weighted_total())
    }

    /// Phase 1 transition helper: take an existing `Cost` (typically a
    /// child plan's cost) and bump its `total` by `extra`. Other fields
    /// are preserved. Disappears once Phase 3 wires real per-operator
    /// formulas through [`super::operator_cost`].
    #[inline]
    pub fn with_added_total(mut self, extra: f64) -> Cost {
        self.total += extra;
        self
    }

    #[cfg(test)]
    /// Apply a vectorization speedup to this cost. The vectorized
    /// expression engine and columnar operators process N rows per inner
    /// loop iteration rather than 1, so the CPU portion of the cost
    /// drops by roughly `1/factor`. IO is unchanged because the same
    /// pages are read; memory may go *up* slightly (column buffers) but
    /// not enough to flip plan choice in Phase 5, so we leave it alone
    /// and revisit when the spill budgets are wired in.
    ///
    /// `factor` should be in roughly `[2.0, 16.0]`. Phase 5 default is
    /// 8.0 — calibrated against the Phase 2 microbench (single
    /// expression, hot data, no SIMD). Calibration tooling under
    /// `src/bin/cbo_calibrate.rs` will refine this per platform.
    #[inline]
    pub fn vectorize_cpu(mut self, factor: f64) -> Cost {
        let cpu_savings = self.cpu_units - self.cpu_units / factor.max(1.0);
        self.cpu_units -= cpu_savings;
        self.total = (self.total - cpu_savings).max(0.0);
        self
    }

    /// Combine two sibling sub-plan costs (e.g. join inputs that both must
    /// be fully consumed). Both `total` and the diagnostic counters are
    /// summed; `startup` is the *max* because both inputs must reach their
    /// startup point before the parent can produce its first row. `rows`
    /// Relational cardinality and width are deliberately not part of this
    /// object; callers compose those through `RelEstimate`/plan properties.
    #[inline]
    pub fn add(&self, rhs: &Cost) -> Cost {
        Cost {
            startup: self.startup.max(rhs.startup),
            total: self.total + rhs.total,
            io_seq_pages: self.io_seq_pages + rhs.io_seq_pages,
            io_rnd_pages: self.io_rnd_pages + rhs.io_rnd_pages,
            cpu_units: self.cpu_units + rhs.cpu_units,
            mem_bytes: self.mem_bytes.max(rhs.mem_bytes),
            spill_bytes: self.spill_bytes + rhs.spill_bytes,
        }
    }

    /// Derive this node's exclusive work from a cumulative cost and its
    /// immediate children. Cost constructors remain the source of truth;
    /// this helper exists for diagnostics and the bottom-up verifier.
    pub fn exclusive_of(&self, children: &[Cost]) -> Cost {
        let child_total = children.iter().map(|c| c.total).sum::<f64>();
        let child_startup = children.iter().map(|c| c.startup).fold(0.0_f64, f64::max);
        let child_seq = children.iter().map(|c| c.io_seq_pages).sum::<f64>();
        let child_rnd = children.iter().map(|c| c.io_rnd_pages).sum::<f64>();
        let child_cpu = children.iter().map(|c| c.cpu_units).sum::<f64>();
        let child_mem = children.iter().map(|c| c.mem_bytes).fold(0.0_f64, f64::max);
        let child_spill = children.iter().map(|c| c.spill_bytes).sum::<f64>();
        Cost {
            startup: (self.startup - child_startup).max(0.0),
            total: (self.total - child_total).max(0.0),
            io_seq_pages: (self.io_seq_pages - child_seq).max(0.0),
            io_rnd_pages: (self.io_rnd_pages - child_rnd).max(0.0),
            cpu_units: (self.cpu_units - child_cpu).max(0.0),
            mem_bytes: (self.mem_bytes - child_mem).max(0.0),
            spill_bytes: (self.spill_bytes - child_spill).max(0.0),
        }
    }
}

impl std::fmt::Display for Cost {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Cost(startup={:.2} total={:.2})",
            self.startup, self.total
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn scalar_preserves_total() {
        let c = Cost::scalar(42.5);
        assert_eq!(c.total, 42.5);
        assert_eq!(c.startup, 0.0);
        assert_eq!(c.weighted_total(), 42.5);
    }

    #[test]
    fn cmp_total_orders_by_total() {
        let a = Cost::scalar(10.0);
        let b = Cost::scalar(20.0);
        assert_eq!(a.cmp_total(&b), Ordering::Less);
        assert_eq!(b.cmp_total(&a), Ordering::Greater);
        assert_eq!(a.cmp_total(&a), Ordering::Equal);
    }

    #[test]
    fn vectorize_cpu_lowers_total_proportionally() {
        let mut c = Cost::scalar(100.0);
        c.cpu_units = 64.0;
        c.total = 100.0;
        let v = c.vectorize_cpu(8.0);
        // cpu drops from 64 → 8, total drops by 56
        assert!((v.cpu_units - 8.0).abs() < 1e-6);
        assert!((v.total - 44.0).abs() < 1e-6);
    }

    #[test]
    fn vectorize_cpu_clamps_at_zero_total() {
        let mut c = Cost::scalar(2.0);
        c.cpu_units = 100.0;
        let v = c.vectorize_cpu(2.0);
        assert!(v.total >= 0.0);
    }

    #[test]
    fn cmp_total_handles_nan_safely() {
        // A poisoned candidate must deterministically lose to a valid one.
        let a = Cost::scalar(f64::NAN);
        let b = Cost::scalar(1.0);
        assert_eq!(a.cmp_total(&b), Ordering::Greater);
        assert!(!a.is_valid());
        assert!(b.is_valid());
    }

    #[test]
    fn invalid_startup_or_component_loses_comparison() {
        let invalid_startup = Cost {
            startup: 2.0,
            total: 1.0,
            ..Cost::ZERO
        };
        let invalid_component = Cost {
            total: 1.0,
            cpu_units: -1.0,
            ..Cost::ZERO
        };
        let valid = Cost::scalar(10.0);
        assert_eq!(invalid_startup.cmp_total(&valid), Ordering::Greater);
        assert_eq!(invalid_component.cmp_total(&valid), Ordering::Greater);
    }

    #[test]
    fn add_sums_total_and_max_startup() {
        let a = Cost {
            startup: 5.0,
            total: 10.0,
            io_seq_pages: 4.0,
            ..Cost::ZERO
        };
        let b = Cost {
            startup: 8.0,
            total: 20.0,
            io_seq_pages: 6.0,
            ..Cost::ZERO
        };
        let c = a.add(&b);
        assert_eq!(c.total, 30.0);
        assert_eq!(c.startup, 8.0); // max
        assert_eq!(c.io_seq_pages, 10.0); // sum
    }

    #[test]
    fn zero_is_identity_for_add() {
        let a = Cost::scalar(7.0);
        let r = a.add(&Cost::ZERO);
        assert_eq!(r.total, 7.0);
    }

    #[test]
    fn from_components_round_trips() {
        let c = Cost::from_components(1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0);
        assert_eq!(c.startup, 1.0);
        assert_eq!(c.total, 2.0);
        assert_eq!(c.io_seq_pages, 3.0);
        assert_eq!(c.io_rnd_pages, 4.0);
        assert_eq!(c.cpu_units, 5.0);
        assert_eq!(c.mem_bytes, 6.0);
        assert_eq!(c.spill_bytes, 7.0);
    }
}
