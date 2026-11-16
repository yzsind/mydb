//! Query-local candidate admission and bounded reranking validation rounds.
//! Stability is an ANN work heuristic, never a proof of exact recall.

pub(super) struct CandidateBudget {
    k: usize,
    fixed: Option<usize>,
    available: Option<usize>,
    accepted: usize,
    examined: usize,
    guard: usize,
    guard_cap: usize,
    round_rows: usize,
    round_changes: usize,
    stable_rounds: usize,
    checks: u64,
}

impl CandidateBudget {
    pub(super) fn new(k: usize, multiplier: usize, fixed: bool, available: Option<usize>) -> Self {
        let k = available.map_or(k, |n| k.min(n));
        let target = k.saturating_mul(multiplier).max(32).max(k);
        Self {
            k,
            fixed: fixed.then_some(available.map_or(target, |n| target.min(n))),
            available,
            accepted: 0,
            examined: 0,
            guard: k.max(8),
            guard_cap: target.max(k.max(8)),
            round_rows: 0,
            round_changes: 0,
            stable_rounds: 0,
            checks: 0,
        }
    }

    pub(super) fn done(&self) -> bool {
        self.k == 0
            || self.available.is_some_and(|n| self.accepted >= n)
            || self.fixed.map_or(
                self.accepted >= self.k && self.stable_rounds >= 2,
                |target| self.accepted >= target,
            )
    }

    pub(super) fn checks(&self) -> u64 {
        self.checks
    }

    pub(super) fn remaining_qualified(&self) -> usize {
        if let Some(target) = self.fixed {
            return target.saturating_sub(self.accepted);
        }
        if self.accepted < self.k {
            self.k.saturating_sub(self.accepted)
        } else {
            self.guard.saturating_sub(self.round_rows).max(1)
        }
    }

    pub(super) fn next_batch(&self, remaining: usize, single_pass: bool) -> usize {
        if single_pass {
            return self.fixed.unwrap_or(self.k).max(1).min(remaining);
        }
        // Smooth cold/empty batches without assuming table selectivity also
        // describes the query's vector neighborhood. Every request is bounded.
        let rate = (self.accepted as f64 + 1.0) / (self.examined as f64 + 2.0);
        let requested = (self.remaining_qualified() as f64 / rate).ceil() as usize;
        requested.clamp(self.k.clamp(1, 32), 256).min(remaining)
    }

    pub(super) fn observe(&mut self, examined: usize, accepted: usize, changes: usize) {
        let was_full = self.accepted >= self.k;
        self.examined = self.examined.saturating_add(examined);
        self.accepted = self.accepted.saturating_add(accepted);
        if self.fixed.is_some() || !was_full {
            return;
        }
        self.round_rows = self.round_rows.saturating_add(accepted);
        self.round_changes = self.round_changes.saturating_add(changes);
        if self.round_rows >= self.guard {
            self.checks += 1;
            if self.round_changes == 0 {
                self.stable_rounds += 1;
            } else {
                self.stable_rounds = 0;
                self.guard = self.guard.saturating_mul(2).min(self.guard_cap);
            }
            self.round_rows = 0;
            self.round_changes = 0;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn enough_rows_and_a_fixed_multiplier_do_not_hide_late_winners() {
        let mut budget = CandidateBudget::new(100, 4, false, None);
        budget.observe(200, 100, 0);
        assert!(!budget.done(), "K alone cannot complete validation");
        budget.observe(200, 100, 3);
        budget.observe(400, 200, 1);
        assert!(
            !budget.done(),
            "the old400-row cutoff still has new winners"
        );
        budget.observe(800, 400, 0);
        assert!(!budget.done(), "one stable round is insufficient");
        budget.observe(800, 400, 0);
        assert!(budget.done());
    }

    #[test]
    fn candidate_batches_follow_local_survival_and_stay_bounded() {
        let mut dense = CandidateBudget::new(100, 4, false, None);
        let mut sparse = CandidateBudget::new(100, 4, false, None);
        dense.observe(100, 90, 0);
        sparse.observe(100, 1, 0);
        assert!(dense.next_batch(1000, false) < sparse.next_batch(1000, false));
        assert_eq!(sparse.next_batch(1000, false), 256);
        assert_eq!(sparse.next_batch(7, false), 7);
        sparse.observe(256, 0, 0);
        assert!(!sparse.done());
    }

    #[test]
    fn explicit_pool_and_complete_visible_selection_keep_their_contracts() {
        let mut explicit = CandidateBudget::new(100, 4, true, None);
        assert_eq!(explicit.next_batch(1000, true), 400);
        explicit.observe(1000, 399, 100);
        assert!(!explicit.done());
        explicit.observe(10, 1, 1);
        assert!(explicit.done());
        let mut selected = CandidateBudget::new(100, 4, false, Some(12));
        selected.observe(12, 12, 12);
        assert!(selected.done(), "every visible selected row was scored");
        assert!(CandidateBudget::new(100, 4, false, Some(0)).done());
    }
}
