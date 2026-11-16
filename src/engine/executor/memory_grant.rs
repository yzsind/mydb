//! One memory decision protocol shared by spill-capable executor operators.

use std::sync::Arc;

use super::mem_budget::{current_budget, MemBudget, OperatorCharge};
use super::work_area::{SpillHint, WorkAreaHandle};

/// Result of resizing an operator's resident-memory reservation.
pub(crate) enum GrantResize {
    Granted,
    /// Recoverable pressure: the operator must spill and resize downward.
    Spill,
}

/// RAII owner for both query-budget and work-area accounting.
///
/// `resize` accepts an absolute resident byte count. This makes accounting
/// idempotent and keeps release paths independent of how many incremental
/// container growth events occurred inside an operator batch.
pub(crate) struct OperatorMemoryGrant {
    budget: Option<Arc<MemBudget>>,
    work_area: Arc<WorkAreaHandle>,
    charged: usize,
}

impl OperatorMemoryGrant {
    pub(crate) fn register() -> Self {
        Self {
            budget: current_budget(),
            work_area: Arc::new(WorkAreaHandle::register()),
            charged: 0,
        }
    }

    pub(crate) fn with_budget(budget: Arc<MemBudget>) -> Self {
        Self {
            budget: Some(budget),
            work_area: Arc::new(WorkAreaHandle::register()),
            charged: 0,
        }
    }

    pub(crate) fn with_budget_and_work_area(
        budget: Arc<MemBudget>,
        work_area: Arc<WorkAreaHandle>,
    ) -> Self {
        Self {
            budget: Some(budget),
            work_area,
            charged: 0,
        }
    }

    pub(crate) fn resize(&mut self, resident_bytes: usize) -> GrantResize {
        if resident_bytes <= self.charged {
            let released = self.charged - resident_bytes;
            if let Some(budget) = &self.budget {
                budget.release(released);
            }
            self.work_area.release(released);
            self.charged = resident_bytes;
            if released != 0 {
                crate::engine::memory_reclaimer::reclaim_spill_allocator_if_requested();
            }
            return if self.work_area.must_spill() {
                GrantResize::Spill
            } else {
                GrantResize::Granted
            };
        }

        let delta = resident_bytes - self.charged;
        let work_area_result = self.work_area.try_charge(delta);
        if let Some(budget) = &self.budget {
            match budget.charge_for_spillable_operator(delta) {
                OperatorCharge::Granted => {}
                OperatorCharge::SoftLimit(_) => {
                    self.work_area.release(delta);
                    return GrantResize::Spill;
                }
            }
        }
        self.charged = resident_bytes;
        if matches!(work_area_result, SpillHint::Spill) {
            GrantResize::Spill
        } else {
            GrantResize::Granted
        }
    }

    pub(crate) fn charged_bytes(&self) -> usize {
        self.charged
    }

    pub(crate) fn resize_after_spill(&mut self, resident_bytes: usize) -> GrantResize {
        let freed = self.charged.saturating_sub(resident_bytes);
        let result = self.resize(resident_bytes);
        self.work_area.record_spill(freed);
        result
    }
}

impl Drop for OperatorMemoryGrant {
    fn drop(&mut self) {
        if let Some(budget) = &self.budget {
            budget.release(self.charged);
        }
        // Parallel grants share this handle. Releasing only when the last
        // Arc drops retains retired workers' usage during merge and forces
        // surviving workers to spill against memory that is already free.
        self.work_area.release(self.charged);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn query_budget_pressure_requests_spill_and_drop_releases_charge() {
        let budget = Arc::new(MemBudget::new(1024));

        let mut grant = OperatorMemoryGrant::with_budget(budget.clone());
        assert!(matches!(grant.resize(768), GrantResize::Granted));
        assert_eq!(budget.used(), 768);
        assert!(matches!(grant.resize(1536), GrantResize::Spill));
        assert_eq!(budget.used(), 768);
        drop(grant);
        assert_eq!(budget.used(), 0);
    }

    #[test]
    fn dropping_one_parallel_grant_releases_its_shared_work_area_usage() {
        let budget = Arc::new(MemBudget::new(4096));
        let area = Arc::new(WorkAreaHandle::register());
        let mut first =
            OperatorMemoryGrant::with_budget_and_work_area(budget.clone(), area.clone());
        let mut second =
            OperatorMemoryGrant::with_budget_and_work_area(budget.clone(), area.clone());
        assert!(matches!(first.resize(512), GrantResize::Granted));
        assert!(matches!(second.resize(256), GrantResize::Granted));
        assert_eq!(area.used_bytes(), 768);
        drop(first);
        assert_eq!(budget.used(), 256);
        assert_eq!(area.used_bytes(), 256);
        drop(second);
        assert_eq!(budget.used(), 0);
        assert_eq!(area.used_bytes(), 0);
    }

    #[test]
    fn shrinking_releases_both_accounting_layers() {
        let budget = Arc::new(MemBudget::new(4096));

        let mut grant = OperatorMemoryGrant::with_budget(budget.clone());
        assert!(matches!(grant.resize(1024), GrantResize::Granted));
        let _ = grant.resize(128);
        assert_eq!(grant.charged_bytes(), 128);
        assert_eq!(budget.used(), 128);
    }
}
