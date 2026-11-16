//! Buffer-pool-aware page IO cost.
//!
//! The optimizer doesn't know what's in the buffer pool at any given
//! moment, but it knows how big the pool is relative to the table being
//! scanned. We use that ratio to estimate a hit rate and discount IO
//! cost accordingly. The model is deliberately simple — Phase 6
//! calibration tunes the constants to whatever the host hardware
//! actually delivers.
//!
//! ## Model
//!
//! - **Sequential access** (table scan, B-tree leaf chain): uses the same
//!   cache-to-working-set residency estimate as other access patterns.
//!   Prefetch is represented by the calibrated `cbo_seq_page_cost`, not by
//!   claiming that cold pages were cache hits.
//!
//! - **Random access** (index descent, point heap fetch): uses the same
//!   residency estimate, with misses priced at `cbo_rnd_page_cost`.
//!
//! Cache hits are priced by the public `cbo_mem_page_cost` coefficient.

use super::consts::consts as cbo_consts;

/// Whether a page access pattern is sequential (prefetch friendly) or
/// random (point lookup, index descent, heap row fetch).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AccessKind {
    Sequential,
    Random,
    /// Pages visited in ascending order but *not* streamed by the
    /// prefetcher (batched INLJ heap fetches: sorted row_ids, demand
    /// reads). Sparse sorted RIDs still issue point page reads once their
    /// working set exceeds the stable planning cache; ordering improves queue
    /// locality but does not turn them into a scan or amortize syscall/page
    /// validation. A cache-resident working set retains sequential pricing so
    /// small lookup joins do not pay a cold-storage penalty.
    OrderedNoPrefetch,
}

/// Hit rate fraction applied when a hot page is found in the buffer
/// pool. Unknown working-set size is treated conservatively as cold.
pub(crate) fn hit_rate(access: AccessKind, effective_cache_pages: f64, table_pages: f64) -> f64 {
    let _ = access;
    if table_pages <= 0.0 {
        0.0
    } else {
        (effective_cache_pages / table_pages).clamp(0.0, 0.95)
    }
}

/// Cost in cost-units of touching `pages` pages with the given access
/// pattern, given a buffer pool effectively holding
/// `effective_cache_pages` of the working set and a table that consumes
/// `table_pages` total.
///
/// Returns 0 for `pages <= 0` so callers don't have to special-case
/// empty children.
pub fn page_io_cost(
    access: AccessKind,
    pages: f64,
    effective_cache_pages: f64,
    table_pages: f64,
) -> f64 {
    if pages <= 0.0 {
        return 0.0;
    }
    let consts = cbo_consts();
    let unit = match access {
        AccessKind::Sequential => consts.seq_page_cost(),
        AccessKind::Random => consts.rnd_page_cost(),
        AccessKind::OrderedNoPrefetch if table_pages > effective_cache_pages => {
            consts.rnd_page_cost()
        }
        AccessKind::OrderedNoPrefetch => consts.seq_page_cost(),
    };
    let h = hit_rate(access, effective_cache_pages, table_pages);
    let miss = 1.0 - h;
    pages * (miss * unit + h * consts.mem_page_cost())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn zero_pages_costs_zero() {
        assert_eq!(
            page_io_cost(AccessKind::Sequential, 0.0, 100.0, 1000.0),
            0.0
        );
        assert_eq!(page_io_cost(AccessKind::Random, -5.0, 100.0, 1000.0), 0.0);
    }

    #[test]
    fn sequential_is_cheaper_than_random_for_same_pages() {
        // With identical residency, random misses retain their higher
        // calibrated page price.
        let seq = page_io_cost(AccessKind::Sequential, 100.0, 1.0, 1_000_000.0);
        let rnd = page_io_cost(AccessKind::Random, 100.0, 1.0, 1_000_000.0);
        assert!(rnd > seq, "rnd={rnd} seq={seq}");
    }

    #[test]
    fn ordered_demand_reads_retain_point_read_cost() {
        let seq = page_io_cost(AccessKind::Sequential, 100.0, 1.0, 1_000_000.0);
        let ordered = page_io_cost(AccessKind::OrderedNoPrefetch, 100.0, 1.0, 1_000_000.0);
        let random = page_io_cost(AccessKind::Random, 100.0, 1.0, 1_000_000.0);
        assert!(ordered > seq, "ordered={ordered} seq={seq}");
        assert_eq!(ordered, random);

        let resident_seq = page_io_cost(AccessKind::Sequential, 100.0, 10_000.0, 100.0);
        let resident_ordered = page_io_cost(AccessKind::OrderedNoPrefetch, 100.0, 10_000.0, 100.0);
        assert_eq!(resident_ordered, resident_seq);
    }

    #[test]
    fn larger_cache_lowers_random_cost() {
        let cold = page_io_cost(AccessKind::Random, 100.0, 1.0, 10_000.0);
        let warm = page_io_cost(AccessKind::Random, 100.0, 5_000.0, 10_000.0);
        assert!(warm < cold, "warm={warm} cold={cold}");
    }

    #[test]
    fn hit_rate_clamps() {
        // Tiny table, huge cache: random hit rate caps at 0.95 (not 1.0)
        // so the cost stays a meaningful fraction of a miss.
        assert_eq!(hit_rate(AccessKind::Random, 1_000_000.0, 1.0), 0.95);
        // Tiny cache approaches zero residency; there is no optimistic floor.
        assert_eq!(hit_rate(AccessKind::Random, 0.0, 1_000_000.0), 0.0);
    }

    #[test]
    fn cost_is_monotone_in_pages() {
        let p10 = page_io_cost(AccessKind::Sequential, 10.0, 100.0, 1000.0);
        let p20 = page_io_cost(AccessKind::Sequential, 20.0, 100.0, 1000.0);
        let p100 = page_io_cost(AccessKind::Sequential, 100.0, 100.0, 1000.0);
        assert!(p10 < p20 && p20 < p100);
    }

    #[test]
    fn unknown_table_size_is_not_assumed_warm() {
        assert_eq!(hit_rate(AccessKind::Random, 100.0, 0.0), 0.0);
    }
}
