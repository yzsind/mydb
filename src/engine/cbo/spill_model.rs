//! Cost of spilling intermediate state to disk when an in-memory
//! algorithm exceeds its memory budget.
//!
//! mydb's executor today spills under three conditions:
//!
//! 1. **Grace hash join** ([`crate::engine::executor::select::grace`]):
//!    when the build side overflows the per-query memory budget the
//!    executor partitions both sides onto disk and joins each partition
//!    independently. Default 16 partitions, recursive up to depth 4.
//! 2. **External sort**: bounded runs followed by a fan-in merge; the
//!    cost is driven by the expected number of merge passes.
//! 3. **Hash aggregate**: partitioned state replay, with recursive
//!    repartitioning charged in the same shape as grace join.
//!
//! All three converge on the same "k-way merge with disk reads/writes"
//! cost shape, so this module exposes a single primitive and a few
//! named wrappers.

use super::consts::consts as cbo_consts;

/// Cost of spilling `bytes_to_spill` worth of state through `partitions`
/// partition files (Grace style). Each partition is written once and
/// read back once. The recursion factor `recursion_depth` accounts for
/// the case where partitions themselves overflow and have to be
/// re-partitioned.
///
/// `bytes_to_spill <= 0` returns 0 so callers don't have to check.
pub fn grace_spill_cost(bytes_to_spill: f64, partitions: f64, recursion_depth: f64) -> f64 {
    if bytes_to_spill <= 0.0 {
        return 0.0;
    }
    let pages = (bytes_to_spill / page_size_bytes() as f64).ceil();
    let parts = partitions.max(2.0);
    let depth = recursion_depth.max(1.0);
    // Each recursion level rewrites every page once and reads it back
    // — that's the 2x. Going one level deeper doubles the work, hence
    // multiplying by `depth`.
    let consts = cbo_consts();
    let io = pages * 2.0 * consts.spill_page_cost() * depth;
    let partition_cpu = parts * depth * consts.cpu_operator_cost();
    io + partition_cpu
}

/// Convenience wrapper for hash join: pass the build-side bytes and
/// budget. If `build_bytes <= budget`, returns 0 (no spill).
pub fn hash_join_spill_cost(build_bytes: f64, budget_bytes: f64) -> f64 {
    hash_join_spill_cost_with_probe(build_bytes, 0.0, budget_bytes)
}

/// Grace hash join writes and reads both build and probe partitions.  The
/// in-memory admission decision is still driven by build state, but once it
/// spills pricing only the build side systematically favours hash join for a
/// very large probe relation.
pub fn hash_join_spill_cost_with_probe(
    build_bytes: f64,
    probe_bytes: f64,
    budget_bytes: f64,
) -> f64 {
    if build_bytes <= 0.0 || (budget_bytes > 0.0 && build_bytes <= budget_bytes) {
        return 0.0;
    }
    // mydb's grace path defaults to 16 partitions and a max recursion
    // depth of 4 (see `MYDB_HASH_JOIN_GRACE_PARTITIONS` /
    // `MYDB_HASH_JOIN_GRACE_MAX_DEPTH`). We compute the *expected*
    // recursion depth as ceil(log_partitions(overflow / budget)).
    let partitions = 16.0;
    let effective_budget = budget_bytes.max(page_size_bytes() as f64);
    let depth = ((build_bytes / effective_budget).log(partitions).ceil()).clamp(1.0, 4.0);
    // Once Grace mode is entered the build relation is partitioned in full;
    // charging only the bytes above the budget badly underprices the
    // boundary immediately after an in-memory hash table overflows.
    grace_spill_cost(build_bytes + probe_bytes.max(0.0), partitions, depth)
}

/// HashAggregate partition spill. Safety decisions in the executor use
/// measured allocation and WorkArea grants only; this estimate is solely for
/// choosing between otherwise-correct physical plans.
pub fn hash_agg_spill_cost(state_bytes: f64, budget_bytes: f64) -> f64 {
    hash_join_spill_cost(state_bytes, budget_bytes)
}

/// External-sort spill. Modelled as 2 × pages × log_M(runs) where M is
/// a fixed merge fan-in of 32 (matches typical PG/MySQL defaults). When
/// the input fits in memory, returns 0.
pub fn sort_spill_cost(input_bytes: f64, budget_bytes: f64) -> f64 {
    if input_bytes <= 0.0 || (budget_bytes > 0.0 && input_bytes <= budget_bytes) {
        return 0.0;
    }
    let consts = cbo_consts();
    let pages = (input_bytes / page_size_bytes() as f64).ceil();
    let effective_budget = budget_bytes.max(page_size_bytes() as f64);
    let runs = (input_bytes / effective_budget).ceil().max(2.0);
    let merge_fanin = 32.0_f64;
    let passes = runs.log(merge_fanin).max(1.0).ceil();
    pages * 2.0 * consts.spill_page_cost() * passes
}

/// Page size in bytes. Storage uses 16 KiB pages; this constant is
/// intentionally redundant rather than imported from the storage
/// module so the optimizer keeps a stable cost basis even if the
/// engine experiments with different sizes.
#[inline]
const fn page_size_bytes() -> usize {
    16 * 1024
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn no_spill_when_under_budget() {
        assert_eq!(hash_join_spill_cost(100.0, 1000.0), 0.0);
        assert_eq!(hash_agg_spill_cost(100.0, 1000.0), 0.0);
        assert_eq!(sort_spill_cost(100.0, 1000.0), 0.0);
    }

    #[test]
    fn hash_join_spill_grows_with_overflow() {
        let budget = 1024.0 * 1024.0; // 1MB budget
        let small = hash_join_spill_cost(2.0 * budget, budget);
        let big = hash_join_spill_cost(10.0 * budget, budget);
        assert!(big > small, "big={big} small={small}");
        assert!(small > 0.0);
    }

    #[test]
    fn zero_budget_still_spills_nonempty_inputs() {
        assert!(hash_join_spill_cost(1024.0 * 1024.0, 0.0) > 0.0);
        assert!(sort_spill_cost(1024.0 * 1024.0, 0.0) > 0.0);
    }

    #[test]
    fn crossing_hash_budget_charges_the_full_build() {
        let budget = 1024.0 * 1024.0;
        let just_over = hash_join_spill_cost(budget + 1.0, budget);
        let full_build_floor = grace_spill_cost(budget, 16.0, 1.0);
        assert!(just_over >= full_build_floor);
    }

    #[test]
    fn sort_spill_grows_with_runs() {
        let budget = 1024.0 * 1024.0;
        let one_pass = sort_spill_cost(2.0 * budget, budget);
        // Push past the merge-fanin threshold so the formula adds an
        // additional merge pass.
        let two_pass = sort_spill_cost(64.0 * budget, budget);
        assert!(
            two_pass > one_pass,
            "two_pass={two_pass} one_pass={one_pass}"
        );
    }

    #[test]
    fn grace_spill_zero_bytes_zero_cost() {
        assert_eq!(grace_spill_cost(0.0, 16.0, 1.0), 0.0);
        assert_eq!(grace_spill_cost(-100.0, 16.0, 1.0), 0.0);
    }

    #[test]
    fn grace_spill_monotone_in_size() {
        let small = grace_spill_cost(1024.0 * 1024.0, 16.0, 1.0);
        let big = grace_spill_cost(10.0 * 1024.0 * 1024.0, 16.0, 1.0);
        assert!(big > small);
    }

    #[test]
    fn deeper_recursion_is_costlier() {
        let bytes = 100.0 * 1024.0 * 1024.0;
        let shallow = grace_spill_cost(bytes, 16.0, 1.0);
        let deep = grace_spill_cost(bytes, 16.0, 4.0);
        assert!(deep > shallow);
    }
}
