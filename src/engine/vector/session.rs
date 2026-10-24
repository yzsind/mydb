//! HNSW-related session knobs surfaced to the planner and executor.
//!
//! Reads thread-local `SESSION_CTX_PTR` (set by
//! `engine::expr::with_session_expr_info`) and falls back to the documented
//! defaults when no session is installed (planner-only unit tests, internal
//! callers without a connection context). The HNSW knobs live on
//! `SessionSysVarState`; see [`crate::engine::api::SessionSysVarState`].

use crate::engine::expr;

/// Resolution policy for `iterative_scan` — controls how the executor widens
/// the candidate pool when filter/MVCC drops results below `k`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum IterativeScan {
    /// Single pass; accept short results.
    Off,
    /// Widen until k satisfied, do not re-sort by distance order.
    RelaxedOrder,
    /// Widen until k satisfied, preserve strict ascending distance order (default).
    StrictOrder,
}

impl IterativeScan {
    pub(crate) fn from_str(s: &str) -> Self {
        match s.trim().to_ascii_lowercase().as_str() {
            "off" => IterativeScan::Off,
            "relaxed_order" => IterativeScan::RelaxedOrder,
            _ => IterativeScan::StrictOrder,
        }
    }
}

/// HNSW knobs resolved from the current session (or defaults if none).
#[derive(Debug, Clone)]
pub(crate) struct HnswSessionConfig {
    pub ef_search: u64,
    pub ef_search_explicit: bool,
    pub ef_dynamic_factor: u64,
    pub iterative_scan: IterativeScan,
    pub max_scan_tuples: u64,
    pub brute_threshold: u64,
    pub brute_filter_rows: u64,
    /// IVF query-time nprobe (cells to probe). Shares this config struct
    /// with the HNSW knobs since both are "vector session state"; the executor
    /// reads whichever applies to the index's algorithm.
    pub ivf_nprobe: u64,
    pub ivf_nprobe_explicit: bool,
    pub ivf_rerank_mult: u64,
    pub ivf_iterative_scan: IterativeScan,
    pub ivf_max_scan_tuples: u64,
}

pub(crate) const DEFAULT_IVF_NPROBE: u64 = 8;

impl Default for HnswSessionConfig {
    fn default() -> Self {
        Self {
            ef_search: super::DEFAULT_EF_SEARCH as u64,
            ef_search_explicit: false,
            ef_dynamic_factor: 4,
            iterative_scan: IterativeScan::StrictOrder,
            max_scan_tuples: 20_000,
            brute_threshold: 10_000,
            brute_filter_rows: 1_000,
            ivf_nprobe: DEFAULT_IVF_NPROBE,
            ivf_nprobe_explicit: false,
            ivf_rerank_mult: 4,
            ivf_iterative_scan: IterativeScan::StrictOrder,
            ivf_max_scan_tuples: 20_000,
        }
    }
}

pub(crate) fn current_config() -> HnswSessionConfig {
    expr::session_hnsw_config().unwrap_or_default()
}

/// Compute the effective `ef_search` for a query: max of the resolved base, a
/// dynamic factor times `k`, and `k` itself (so the candidate pool always holds
/// at least the requested rows).
///
/// Base resolution priority (pgvector-aligned, with a mydb per-index default):
///   1. `/*+ HNSW_EF(n) */` HINT — wins per query.
///   2. session / GUC `mydb_hnsw_ef_search` — when explicitly `SET` (even to
///      the built-in value) or configured away from the built-in default.
///   3. per-index `EF_SEARCH=` (`index_default`) — the index's own default.
///   4. the built-in default (`mydb_hnsw_ef_search` left at 64).
pub(crate) fn effective_ef_search(
    cfg: &HnswSessionConfig,
    hint_override: Option<u64>,
    index_default: Option<u64>,
    k: usize,
) -> usize {
    let base = match hint_override {
        Some(v) if v > 0 => v,
        _ => {
            let session_tuned =
                cfg.ef_search_explicit || cfg.ef_search != super::DEFAULT_EF_SEARCH as u64;
            if session_tuned {
                cfg.ef_search
            } else {
                index_default.filter(|v| *v > 0).unwrap_or(cfg.ef_search)
            }
        }
    };
    let dynamic = cfg.ef_dynamic_factor.saturating_mul(k as u64);
    base.max(dynamic).max(k as u64).max(1) as usize
}

/// Compute the effective `nprobe` for an IVF query. Resolution priority
/// mirrors [`effective_ef_search`]:
///   1. `/*+ IVF_NPROBE(n) */` HINT — wins per query.
///   2. session / GUC `mydb_ivf_nprobe` — when tuned away from the default.
///   3. per-index `NPROBE=` (`index_default`) — the index's own default.
///   4. the built-in default (8).
///
/// Unlike ef, nprobe is *not* scaled by `k`: it selects how many inverted
/// cells to scan, and each cell already holds many candidates. The IVF search
/// clamps the result to `[1, nlist]`.
pub(crate) fn effective_nprobe(
    cfg: &HnswSessionConfig,
    hint_override: Option<u64>,
    index_default: Option<u64>,
) -> usize {
    let base = match hint_override {
        Some(v) if v > 0 => v,
        _ => {
            let session_tuned = cfg.ivf_nprobe_explicit || cfg.ivf_nprobe != DEFAULT_IVF_NPROBE;
            if session_tuned {
                cfg.ivf_nprobe
            } else {
                index_default.filter(|v| *v > 0).unwrap_or(cfg.ivf_nprobe)
            }
        }
    };
    base.max(1) as usize
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn explicit_default_ef_search_overrides_index_comment() {
        let mut cfg = HnswSessionConfig::default();
        assert_eq!(effective_ef_search(&cfg, None, Some(200), 1), 200);
        cfg.ef_search_explicit = true;
        assert_eq!(effective_ef_search(&cfg, None, Some(200), 1), 64);
        assert_eq!(effective_ef_search(&cfg, Some(16), Some(200), 1), 16);
    }

    #[test]
    fn explicit_default_nprobe_overrides_index_comment() {
        let mut cfg = HnswSessionConfig::default();
        assert_eq!(effective_nprobe(&cfg, None, Some(32)), 32);
        cfg.ivf_nprobe = DEFAULT_IVF_NPROBE;
        cfg.ivf_nprobe_explicit = true;
        assert_eq!(effective_nprobe(&cfg, None, Some(32)), 8);
        assert_eq!(effective_nprobe(&cfg, Some(3), Some(32)), 3);
    }
}
