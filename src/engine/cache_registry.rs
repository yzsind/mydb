//! Unified observability over the engine's caches.
//!
//! Every cache keeps its own native counters (lock-free atomics or its
//! own guard); this module is a read-side aggregator — one row per
//! cache, one shape for all of them — feeding
//! `information_schema.mydb_caches` and the cache section of
//! `SHOW ENGINE MYDB STATUS`. It deliberately does NOT own any cache
//! lifecycle: registration happens through each cache's existing global
//! accessor, so there is no new lock on any hot path.
//!
//! Buffer-pool / storage-side caches are not listed here — they report
//! through `SHOW ENGINE MYDB STATUS`'s storage section with far richer
//! detail (per-kind residency, dirty ratios, prefetch efficiency).

/// Ownership of the reported byte estimate, independent of cache hit counters.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CacheMemoryAccounting {
    IndependentEstimate,
    GovernedSubset,
    /// Cache-local logical size can include Arc payloads shared with other owners.
    SharedEstimate,
}

impl CacheMemoryAccounting {
    pub(crate) fn label(self) -> &'static str {
        match self {
            Self::IndependentEstimate => "ESTIMATE",
            Self::GovernedSubset => "ESTIMATE_SUBSET",
            Self::SharedEstimate => "SHARED_ESTIMATE",
        }
    }

    pub(crate) fn parent(self) -> Option<&'static str> {
        match self {
            Self::GovernedSubset | Self::SharedEstimate => Some("controlled_derived"),
            Self::IndependentEstimate => None,
        }
    }
}

/// One cache's uniform statistics row.
#[derive(Debug, Clone)]
pub struct CacheStatsRow {
    pub name: &'static str,
    /// Coarse family: `sql` (statement/plan), `metadata` (view,
    /// interning), `executor` (compiled expressions), `function`
    /// (regex / JSON path compile caches).
    pub kind: &'static str,
    /// `process` for shared caches, `thread` for thread-local ones
    /// (sizes/capacities are then per-thread).
    pub scope: &'static str,
    pub hits: u64,
    pub misses: u64,
    pub size_entries: u64,
    /// 0 = unbounded / not applicable.
    pub capacity_entries: u64,
    /// Capacity-driven evictions (LRU).
    pub evictions: u64,
    /// Staleness-driven evictions (epoch bumps, dependency mismatches,
    /// explicit invalidation).
    pub invalidations: u64,
    /// Approximate cache bytes within `scope`; shared payloads may occur in
    /// multiple layers. Consult `memory_accounting` before summing. `None`
    /// means unavailable; `Some(0)` means measured/estimated and empty.
    pub bytes: Option<u64>,
    pub memory_accounting: CacheMemoryAccounting,
    /// None if actual eviction counts are not instrumented (fallbacks are not evictions).
    pub memory_evictions: Option<u64>,
}

impl CacheStatsRow {
    pub fn hit_pct(&self) -> f64 {
        let total = self.hits + self.misses;
        if total == 0 {
            0.0
        } else {
            (self.hits as f64) * 100.0 / (total as f64)
        }
    }
}

/// Snapshot every registered cache. Order is stable (grouped by layer)
/// so `information_schema.mydb_caches` reads consistently.
pub fn collect() -> Vec<CacheStatsRow> {
    let mut rows = Vec::with_capacity(8);

    if let Some(stats) = crate::engine::cache::global_stats() {
        rows.push(CacheStatsRow {
            name: "statement_ast",
            kind: "sql",
            scope: "process",
            hits: stats.hits,
            misses: stats.misses,
            size_entries: stats.size,
            capacity_entries: stats.capacity,
            evictions: stats.lru_evictions,
            invalidations: stats.stale_evictions,
            bytes: Some(stats.approx_bytes),
            memory_accounting: CacheMemoryAccounting::IndependentEstimate,
            memory_evictions: Some(stats.lru_evictions),
        });
    }
    if let Some(stats) = crate::engine::cache::global_plan_stats() {
        rows.push(CacheStatsRow {
            name: "physical_plan",
            kind: "sql",
            scope: "process",
            hits: stats.hits,
            misses: stats.misses,
            size_entries: stats.size,
            capacity_entries: stats.capacity,
            evictions: stats.lru_evictions,
            invalidations: stats.stale_evictions + stats.deps_invalidations,
            bytes: Some(stats.approx_bytes),
            memory_accounting: CacheMemoryAccounting::IndependentEstimate,
            memory_evictions: Some(stats.lru_evictions),
        });
    }
    if let Some(stats) = crate::engine::cache::global_prepared_dml_stats() {
        rows.push(CacheStatsRow {
            name: "prepared_dml_plan",
            kind: "sql",
            scope: "process",
            hits: stats.hits,
            misses: stats.misses,
            size_entries: stats.size,
            capacity_entries: stats.capacity,
            evictions: stats.lru_evictions,
            invalidations: stats.stale_evictions + stats.deps_invalidations,
            bytes: Some(stats.approx_bytes),
            memory_accounting: CacheMemoryAccounting::IndependentEstimate,
            memory_evictions: Some(stats.lru_evictions),
        });
    }
    if let Some(stats) = crate::engine::cache::global_prepared_dml_batch_stats() {
        rows.push(CacheStatsRow {
            name: "prepared_dml_batch",
            kind: "sql",
            scope: "process",
            hits: stats.hits,
            misses: stats.misses,
            size_entries: stats.size,
            capacity_entries: stats.capacity,
            evictions: stats.lru_evictions,
            invalidations: stats.stale_evictions + stats.deps_invalidations,
            bytes: Some(stats.approx_bytes),
            memory_accounting: CacheMemoryAccounting::IndependentEstimate,
            memory_evictions: Some(stats.lru_evictions),
        });
    }

    let expr = crate::engine::expr_program::expr_program_stats();
    rows.push(CacheStatsRow {
        name: "expr_program_l1",
        kind: "executor",
        scope: "thread",
        hits: expr.compile_cache_hits,
        misses: expr.compile_cache_l2_hits + expr.compile_cache_misses,
        size_entries: expr.l1_entries,
        capacity_entries: crate::engine::expr_program::ExprCompiler::MAX_PROGRAM_CACHE_ENTRIES
            as u64,
        evictions: 0,
        invalidations: 0,
        bytes: Some(expr.l1_bytes),
        memory_accounting: CacheMemoryAccounting::SharedEstimate,
        memory_evictions: None,
    });
    rows.push(CacheStatsRow {
        name: "expr_program_l2",
        kind: "executor",
        scope: "process",
        hits: expr.compile_cache_l2_hits,
        misses: expr.compile_cache_misses,
        size_entries: expr.l2_entries,
        capacity_entries: (crate::engine::expr_program::PROGRAM_CACHE_L2_SHARDS
            * crate::engine::expr_program::MAX_PROGRAM_CACHE_L2_SHARD_ENTRIES)
            as u64,
        evictions: 0,
        invalidations: 0,
        bytes: Some(expr.l2_bytes),
        memory_accounting: CacheMemoryAccounting::SharedEstimate,
        memory_evictions: None,
    });

    let func = crate::engine::func_cache::stats();
    rows.push(CacheStatsRow {
        name: "gtid_set_parse",
        kind: "function",
        scope: "thread",
        hits: func.gtid_set_hits,
        misses: func.gtid_set_misses,
        size_entries: func.gtid_set_entries,
        capacity_entries: crate::engine::func_cache::GTID_SET_CACHE_CAPACITY as u64,
        evictions: 0,
        invalidations: 0,
        bytes: Some(func.gtid_set_bytes),
        memory_accounting: CacheMemoryAccounting::GovernedSubset,
        memory_evictions: None,
    });
    rows.push(CacheStatsRow {
        name: "regex_compile",
        kind: "function",
        scope: "thread",
        hits: func.regex_hits,
        misses: func.regex_misses,
        size_entries: func.regex_entries,
        capacity_entries: crate::engine::func_cache::REGEX_CACHE_CAPACITY as u64,
        evictions: 0,
        invalidations: 0,
        bytes: Some(func.regex_bytes),
        memory_accounting: CacheMemoryAccounting::GovernedSubset,
        memory_evictions: None,
    });
    rows.push(CacheStatsRow {
        name: "json_path_compile",
        kind: "function",
        scope: "thread",
        hits: func.json_path_hits,
        misses: func.json_path_misses,
        size_entries: func.json_path_entries,
        capacity_entries: crate::engine::func_cache::JSON_PATH_CACHE_CAPACITY as u64,
        evictions: 0,
        invalidations: 0,
        bytes: Some(func.json_path_bytes),
        memory_accounting: CacheMemoryAccounting::GovernedSubset,
        memory_evictions: None,
    });
    rows.push(CacheStatsRow {
        name: "json_schema_compile",
        kind: "function",
        scope: "thread",
        hits: func.json_schema_hits,
        misses: func.json_schema_misses,
        size_entries: func.json_schema_entries,
        capacity_entries: crate::engine::func_cache::JSON_SCHEMA_CACHE_CAPACITY as u64,
        evictions: 0,
        invalidations: 0,
        bytes: Some(func.json_schema_bytes),
        memory_accounting: CacheMemoryAccounting::GovernedSubset,
        memory_evictions: None,
    });
    let (tz_entries, tz_bytes) = crate::engine::expr::tz_spec_cache_stats();
    rows.push(CacheStatsRow {
        name: "tz_spec_resolve",
        kind: "function",
        scope: "thread",
        hits: func.tz_hits,
        misses: func.tz_misses,
        size_entries: tz_entries,
        capacity_entries: crate::engine::expr::TZ_SPEC_CACHE_CAPACITY as u64,
        evictions: 0,
        invalidations: 0,
        bytes: Some(tz_bytes),
        memory_accounting: CacheMemoryAccounting::IndependentEstimate,
        memory_evictions: None,
    });
    rows.push(CacheStatsRow {
        // B9: LIKE plan classify cache — surfaced next to the other
        // per-thread compile caches so operators can watch its hit
        // rate on prepared / auto-param LIKE workloads.
        name: "like_plan",
        kind: "function",
        scope: "thread",
        hits: func.like_plan_hits,
        misses: func.like_plan_misses,
        size_entries: func.like_plan_entries,
        capacity_entries: crate::engine::func_cache::LIKE_PLAN_CACHE_CAPACITY as u64,
        evictions: 0,
        invalidations: 0,
        bytes: Some(func.like_plan_bytes),
        memory_accounting: CacheMemoryAccounting::GovernedSubset,
        memory_evictions: None,
    });

    let (intern_hits, intern_misses) = crate::engine::executor::col_meta_intern_pool_counters();
    let (intern_entries, intern_bytes) = crate::engine::executor::col_meta_intern_stats();
    rows.push(CacheStatsRow {
        name: "col_meta_intern",
        kind: "metadata",
        scope: "process",
        // Process-pool probes only, excluding its per-row TLS front cache.
        hits: intern_hits,
        misses: intern_misses,
        size_entries: intern_entries,
        capacity_entries: crate::engine::executor::col_meta_intern_capacity() as u64,
        // New shapes beyond the governed pool use reclaimable Arc storage.
        evictions: crate::engine::executor::col_meta_intern_fallbacks(),
        invalidations: 0,
        bytes: Some(intern_bytes),
        memory_accounting: CacheMemoryAccounting::GovernedSubset,
        memory_evictions: None,
    });

    rows
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn collect_returns_uniform_rows() {
        let rows = collect();
        // The always-present rows (thread-local + process aggregates).
        for expected in [
            "expr_program_l1",
            "expr_program_l2",
            "regex_compile",
            "json_path_compile",
            "col_meta_intern",
        ] {
            assert!(
                rows.iter().any(|r| r.name == expected),
                "missing cache row {expected}"
            );
        }
        for row in &rows {
            assert!(!row.kind.is_empty() && !row.scope.is_empty());
            let _ = row.hit_pct();
        }
    }
}
