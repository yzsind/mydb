//! Cost-based optimizer (CBO) module.
//!
//! Phase 1 establishes the foundation: a structured [`Cost`] vector that
//! replaces the legacy `cost: f64` field on every [`PhysicalPlan`] variant,
//! and a tunable [`consts`] coefficient table (`cbo_*` GUCs) accessible via
//! `SHOW VARIABLES` / `SET GLOBAL`. Later phases plug real formulas,
//! selectivity, join enumeration, and EXPLAIN ANALYZE telemetry into this
//! foundation; the field shape will not change again.

pub mod cache_model;
pub mod consts;
pub mod cost;
pub mod function_cost;
pub mod join_order;
pub mod model;
pub mod operator_cost;
pub mod selectivity;
pub mod spill_model;
#[cfg(test)]
mod test_support;
pub mod trace;

#[cfg(test)]
pub use consts::is_cbo_variable;
pub use consts::{
    cbo_model, consts as cbo_consts, epoch as cbo_epoch, set_variable, variable_value,
    variables_snapshot, CostModelSnapshot,
};
pub use cost::Cost;
pub use function_cost::function_call_cost;
pub use model::{ColumnEstimate, EstimateSource, ExecutionMode, PhysicalProperties, RelEstimate};
pub use selectivity::{
    default_selectivity, Selectivity, SelectivitySource, StatsBackedSelectivity,
};

/// Page size (bytes) used by all CBO cost formulas. Must stay in sync
/// with `storage::core::PAGE_SIZE`; intentionally duplicated here so
/// the cost basis is stable even if the engine experiments with other
/// sizes in the future.
pub const PAGE_SIZE_BYTES: usize = 16 * 1024;

/// Stable cache-size baseline used by the optimizer. The executor still uses
/// the real buffer pool; planning deliberately does not, so the same schema
/// and statistics produce the same physical plan on hosts with different
/// memory targets.
pub const BASELINE_EFFECTIVE_CACHE_PAGES: u64 = 65_536;

/// Stable per-query memory baseline used only for costing hash tables and
/// sorts. Runtime memory governors may force spill earlier/later, but they do
/// not change plan shape.
pub const BASELINE_QUERY_MEM_BUDGET_BYTES: u64 = 256 * 1024 * 1024;

/// Version of the fixed planning-resource calibration contract. Increment
/// this only when deliberately recalibrating plan costs across a release.
pub const PLANNING_RESOURCE_BASELINE_VERSION: u32 = 2;

/// Canonical RID window used only to cost non-covering range scans. The
/// executor may use a different work-area-sized window, but that runtime
/// grant must never feed back into plan generation.
pub const PLANNING_MRR_WINDOW_ROWS: f64 = 131_072.0;

/// Canonical outer-key chunk used to cost streamed batched INLJ probes.
/// The executor may accept a diagnostic runtime override, but plan
/// generation always uses this stable value so live batching choices do not
/// change the physical plan.
pub const PLANNING_INLJ_CHUNK_ROWS: usize = 16_384;

/// Per-query / per-plan optimizer context.
///
/// Threaded through every Phase 3+ cost formula so they can read
/// tunable coefficients plus stable cache/work-memory baselines without
/// reaching for globals at call sites.
/// Phase 4 join enumeration adds a `Selectivity` instance here too.
#[derive(Debug, Clone, Copy)]
pub struct CboContext {
    /// Immutable model coefficients used by this planning invocation.
    pub model: CostModelSnapshot,
    /// Optimizer cache-size baseline in pages. This is intentionally not the
    /// live buffer-pool capacity.
    pub planning_cache_pages: u64,
    /// Effective cache pages used by the cache-hit-rate model. Driven by the
    /// stable baseline unless explicitly calibrated by
    /// `cbo_effective_cache_mb`.
    pub effective_cache_pages: u64,
    /// Optimizer memory baseline for hash tables / sorts. Runtime memory
    /// limits are enforced by the executor and may spill, but they do not
    /// re-cost or reshape plans.
    pub planning_mem_budget_bytes: u64,
}

impl CboContext {
    /// Build a context from raw inputs (typically called once at the
    /// top of `plan_select` and threaded down).
    pub fn new(planning_cache_pages: u64, planning_mem_budget_bytes: u64) -> Self {
        let model = consts::consts().snapshot();
        let cache_override_mb = model.effective_cache_mb;
        let baseline_pages = if planning_cache_pages > 0 {
            planning_cache_pages
        } else {
            BASELINE_EFFECTIVE_CACHE_PAGES
        };
        let effective_cache_pages = if cache_override_mb > 0.0 {
            (cache_override_mb * 1024.0 * 1024.0 / PAGE_SIZE_BYTES as f64) as u64
        } else {
            baseline_pages
        };
        CboContext {
            model,
            planning_cache_pages: baseline_pages,
            effective_cache_pages,
            planning_mem_budget_bytes,
        }
    }

    #[cfg(test)]
    /// Stable planning context used by production planning and tests.
    pub fn defaults_for_test() -> Self {
        Self::stable_for_planning()
    }

    /// Build the optimizer context for real planning. The `Database` handle is
    /// intentionally ignored for memory sizing: live buffer-pool and
    /// executor-memory settings are runtime resources, not plan inputs.
    pub fn from_db(_db: &crate::storage::Database) -> Self {
        Self::stable_for_planning()
    }

    pub fn stable_for_planning() -> Self {
        Self::new(
            BASELINE_EFFECTIVE_CACHE_PAGES,
            BASELINE_QUERY_MEM_BUDGET_BYTES,
        )
    }
}

#[cfg(test)]
mod context_tests {
    use super::*;
    use std::sync::Mutex;

    static CBO_GUC_TEST_LOCK: Mutex<()> = Mutex::new(());

    #[test]
    fn defaults_use_stable_planning_baseline() {
        let ctx = CboContext::defaults_for_test();
        assert_eq!(ctx.planning_cache_pages, BASELINE_EFFECTIVE_CACHE_PAGES);
        assert_eq!(ctx.effective_cache_pages, BASELINE_EFFECTIVE_CACHE_PAGES);
        assert_eq!(PLANNING_RESOURCE_BASELINE_VERSION, 2);
        assert_eq!(
            ctx.planning_mem_budget_bytes,
            BASELINE_QUERY_MEM_BUDGET_BYTES
        );
    }

    #[test]
    fn explicit_planning_cache_baseline_is_honored_without_override() {
        let _guard = CBO_GUC_TEST_LOCK.lock().unwrap();
        // Coexist with the override-takes-effect test which mutates
        // the same global GUC. Force-clear the override so this test
        // is robust under parallel execution.
        let prior = consts::consts().effective_cache_mb();
        consts::set_variable("cbo_effective_cache_mb", 0.0).unwrap();
        let small = CboContext::new(8192, 100 * 1024 * 1024);
        let huge = CboContext::new(8 * 1024 * 1024, 100 * 1024 * 1024);
        consts::set_variable("cbo_effective_cache_mb", prior).unwrap();
        assert_eq!(small.planning_cache_pages, 8192);
        assert_eq!(huge.planning_cache_pages, 8 * 1024 * 1024);
        assert_eq!(small.effective_cache_pages, 8192);
        assert_eq!(huge.effective_cache_pages, 8 * 1024 * 1024);
    }

    #[test]
    fn cbo_effective_cache_mb_override_takes_effect() {
        let _guard = CBO_GUC_TEST_LOCK.lock().unwrap();
        // Save existing override, set to 32 MiB, verify, restore. The
        // sibling new_propagates_buffer_pool_size test acquires the
        // same GUC, so this pair is order-sensitive only via the
        // process-global atomic — both tests restore the prior value
        // before exiting.
        let prior = consts::consts().effective_cache_mb();
        consts::set_variable("cbo_effective_cache_mb", 32.0).unwrap();
        let ctx = CboContext::new(4096, 100 * 1024 * 1024);
        consts::set_variable("cbo_effective_cache_mb", prior).unwrap();
        // 32 MiB / 16 KiB = 2048 pages.
        assert_eq!(ctx.effective_cache_pages, 2048);
    }

    #[test]
    fn cbo_effective_cache_mb_can_calibrate_above_baseline() {
        let _guard = CBO_GUC_TEST_LOCK.lock().unwrap();
        let prior = consts::consts().effective_cache_mb();
        consts::set_variable("cbo_effective_cache_mb", 2048.0).unwrap();
        let ctx = CboContext::stable_for_planning();
        consts::set_variable("cbo_effective_cache_mb", prior).unwrap();
        assert_eq!(ctx.effective_cache_pages, 131_072);
        assert_eq!(ctx.planning_cache_pages, BASELINE_EFFECTIVE_CACHE_PAGES);
    }
}
