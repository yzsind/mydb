//! Tunable cost coefficients (`cbo_*` GUCs).
//!
//! The full set of coefficients is held in a single process-global
//! `CboConsts` snapshot stored behind atomics, so reads in the planner are
//! lock-free and writes from `SET GLOBAL cbo_xxx = N` propagate immediately.
//!
//! Each coefficient is exposed via:
//! - an environment variable (read once at process start), and
//! - `SHOW VARIABLES LIKE 'cbo_%'` / `SET GLOBAL cbo_xxx = N` at runtime.
//!
//! The basis is `cbo_seq_page_cost = 1.0`. Every other coefficient is
//! relative to the cost of reading one page sequentially, so changing one
//! value never silently drifts the rest.

use std::sync::atomic::{AtomicU64, Ordering};

/// The only optimizer implementation shipped by mydb.
pub const CBO_MODEL: &str = "v2";

/// Read-only optimizer implementation identifier used by system variables,
/// EXPLAIN, and optimizer trace.
pub const fn cbo_model() -> &'static str {
    CBO_MODEL
}

/// Atomic store for one f64 coefficient.
struct AtomicF64(AtomicU64);

impl AtomicF64 {
    const fn new(v: f64) -> Self {
        AtomicF64(AtomicU64::new(v.to_bits()))
    }
    #[inline]
    fn load(&self) -> f64 {
        f64::from_bits(self.0.load(Ordering::Relaxed))
    }
    #[inline]
    fn store(&self, v: f64) {
        self.0.store(v.to_bits(), Ordering::Relaxed);
    }
}

/// Single source of truth for every `cbo_*` coefficient.
///
/// New fields go here, then get registered in [`ALL_COEFFICIENTS`] so the
/// `SHOW VARIABLES` and `SET GLOBAL` plumbing picks them up automatically.
pub struct CboConsts {
    seq_page_cost: AtomicF64,
    rnd_page_cost: AtomicF64,
    cpu_tuple_cost: AtomicF64,
    cpu_index_tuple_cost: AtomicF64,
    cpu_operator_cost: AtomicF64,
    hash_build_cost: AtomicF64,
    hash_probe_cost: AtomicF64,
    sort_cost_factor: AtomicF64,
    mem_page_cost: AtomicF64,
    spill_page_cost: AtomicF64,
    effective_cache_mb: AtomicF64,
    default_eq_selectivity: AtomicF64,
    default_range_selectivity: AtomicF64,
    default_like_selectivity: AtomicF64,
    /// B8: per-character selectivity used to refine `LIKE '%foo%'` /
    /// `LIKE '%foo'` / `LIKE '_foo'` estimates. PG uses the same idea
    /// with `default_ndim`-style formulas; each non-wildcard char
    /// shrinks the estimated match ratio geometrically. 0.15 mirrors
    /// PG's `DEFAULT_MATCH_SEL` after squaring for LIKE.
    like_char_selectivity: AtomicF64,
    default_join_selectivity: AtomicF64,
    join_planning_budget_ms: AtomicF64,
    /// Per-worker startup overhead (thread spawn + result merge) in
    /// cost units. Phase 2 uses this to keep the CBO from picking
    /// parallel for tiny scans where the overhead exceeds the savings.
    parallel_setup_cost: AtomicF64,
    /// When > 0 the equi-depth histogram builder closes buckets on the
    /// nearest natural calendar boundary (hour / day / week / month /
    /// year) for time-typed columns. Default 1.0 (on); set 0 to revert
    /// to pure equi-depth bucketing.
    time_aligned_histogram: AtomicF64,
    /// Per-byte CPU surcharge for deserializing an output tuple wider than
    /// [`DECODE_REF_WIDTH`](super::operator_cost::DECODE_REF_WIDTH). Lets a
    /// scan's per-row decode cost scale with the projected row width instead
    /// of a flat constant, so a wide heap scan is correctly costlier per row
    /// than a narrow covering-index scan that emits the same rows. Applied
    /// symmetrically to heap (`seq_scan`) and index (`index_fast_scan`)
    /// scans. Default 0.00005; set 0 to revert to a flat per-row decode cost.
    cpu_byte_decode_cost: AtomicF64,
}

/// Immutable coefficient set captured once at the start of planning. This
/// prevents a concurrent `SET GLOBAL cbo_*` from mixing old and new
/// coefficients inside one plan; the resulting plan is tagged with `epoch`
/// and will not survive a later model change in the shared cache.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct CostModelSnapshot {
    pub epoch: u64,
    pub seq_page_cost: f64,
    pub rnd_page_cost: f64,
    pub cpu_tuple_cost: f64,
    pub cpu_index_tuple_cost: f64,
    pub cpu_operator_cost: f64,
    pub hash_build_cost: f64,
    pub hash_probe_cost: f64,
    pub sort_cost_factor: f64,
    pub mem_page_cost: f64,
    pub spill_page_cost: f64,
    pub effective_cache_mb: f64,
    pub default_eq_selectivity: f64,
    pub default_range_selectivity: f64,
    pub default_like_selectivity: f64,
    pub like_char_selectivity: f64,
    pub default_join_selectivity: f64,
    pub join_planning_budget_ms: f64,
    pub parallel_setup_cost: f64,
    pub time_aligned_histogram: f64,
    pub cpu_byte_decode_cost: f64,
}

/// Public read-only accessor. Each `getter` reads the current atomic
/// snapshot. Phase 1 callers don't yet use these (they keep the legacy
/// formulas), but Phase 3 onwards reads them directly.
impl CboConsts {
    pub fn snapshot(&self) -> CostModelSnapshot {
        loop {
            let before = epoch();
            let snapshot = CostModelSnapshot {
                epoch: before,
                seq_page_cost: self.seq_page_cost(),
                rnd_page_cost: self.rnd_page_cost(),
                cpu_tuple_cost: self.cpu_tuple_cost(),
                cpu_index_tuple_cost: self.cpu_index_tuple_cost(),
                cpu_operator_cost: self.cpu_operator_cost(),
                hash_build_cost: self.hash_build_cost(),
                hash_probe_cost: self.hash_probe_cost(),
                sort_cost_factor: self.sort_cost_factor(),
                mem_page_cost: self.mem_page_cost(),
                spill_page_cost: self.spill_page_cost(),
                effective_cache_mb: self.effective_cache_mb(),
                default_eq_selectivity: self.default_eq_selectivity(),
                default_range_selectivity: self.default_range_selectivity(),
                default_like_selectivity: self.default_like_selectivity(),
                like_char_selectivity: self.like_char_selectivity(),
                default_join_selectivity: self.default_join_selectivity(),
                join_planning_budget_ms: self.join_planning_budget_ms(),
                parallel_setup_cost: self.parallel_setup_cost(),
                time_aligned_histogram: self.time_aligned_histogram(),
                cpu_byte_decode_cost: self.cpu_byte_decode_cost(),
            };
            if before % 2 == 0 && before == epoch() {
                return snapshot;
            }
        }
    }
    pub fn seq_page_cost(&self) -> f64 {
        self.seq_page_cost.load()
    }
    pub fn rnd_page_cost(&self) -> f64 {
        self.rnd_page_cost.load()
    }
    pub fn cpu_tuple_cost(&self) -> f64 {
        self.cpu_tuple_cost.load()
    }
    pub fn cpu_index_tuple_cost(&self) -> f64 {
        self.cpu_index_tuple_cost.load()
    }
    pub fn cpu_operator_cost(&self) -> f64 {
        self.cpu_operator_cost.load()
    }
    pub fn hash_build_cost(&self) -> f64 {
        self.hash_build_cost.load()
    }
    pub fn hash_probe_cost(&self) -> f64 {
        self.hash_probe_cost.load()
    }
    pub fn sort_cost_factor(&self) -> f64 {
        self.sort_cost_factor.load()
    }
    pub fn mem_page_cost(&self) -> f64 {
        self.mem_page_cost.load()
    }
    pub fn spill_page_cost(&self) -> f64 {
        self.spill_page_cost.load()
    }
    pub fn effective_cache_mb(&self) -> f64 {
        self.effective_cache_mb.load()
    }
    pub fn default_eq_selectivity(&self) -> f64 {
        self.default_eq_selectivity.load()
    }
    pub fn default_range_selectivity(&self) -> f64 {
        self.default_range_selectivity.load()
    }
    pub fn default_like_selectivity(&self) -> f64 {
        self.default_like_selectivity.load()
    }
    pub fn like_char_selectivity(&self) -> f64 {
        self.like_char_selectivity.load()
    }
    pub fn default_join_selectivity(&self) -> f64 {
        self.default_join_selectivity.load()
    }
    pub fn join_planning_budget_ms(&self) -> f64 {
        self.join_planning_budget_ms.load()
    }
    pub fn parallel_setup_cost(&self) -> f64 {
        self.parallel_setup_cost.load()
    }
    pub fn time_aligned_histogram(&self) -> f64 {
        self.time_aligned_histogram.load()
    }
    pub fn cpu_byte_decode_cost(&self) -> f64 {
        self.cpu_byte_decode_cost.load()
    }
}

/// Process-global snapshot. Initialized lazily on first access; values come
/// from `cbo_*` environment variables (uppercased) falling back to the
/// defaults below.
static GLOBAL: CboConsts = CboConsts {
    seq_page_cost: AtomicF64::new(1.0),
    rnd_page_cost: AtomicF64::new(4.0),
    cpu_tuple_cost: AtomicF64::new(0.01),
    cpu_index_tuple_cost: AtomicF64::new(0.005),
    cpu_operator_cost: AtomicF64::new(0.0025),
    hash_build_cost: AtomicF64::new(0.02),
    hash_probe_cost: AtomicF64::new(0.02),
    sort_cost_factor: AtomicF64::new(2.0),
    mem_page_cost: AtomicF64::new(0.2),
    spill_page_cost: AtomicF64::new(12.0),
    effective_cache_mb: AtomicF64::new(0.0), // 0 = use BASELINE_EFFECTIVE_CACHE_PAGES
    default_eq_selectivity: AtomicF64::new(0.005),
    default_range_selectivity: AtomicF64::new(0.333),
    default_like_selectivity: AtomicF64::new(0.05),
    like_char_selectivity: AtomicF64::new(0.15),
    default_join_selectivity: AtomicF64::new(0.005),
    join_planning_budget_ms: AtomicF64::new(50.0),
    parallel_setup_cost: AtomicF64::new(100.0),
    time_aligned_histogram: AtomicF64::new(1.0),
    cpu_byte_decode_cost: AtomicF64::new(0.00005),
};

static INIT: std::sync::Once = std::sync::Once::new();

/// Monotonic version of the process-wide cost model. Physical plans capture
/// this value when they enter the shared cache; changing any cost-affecting
/// CBO variable makes older plans stale without flushing parsed statements.
// This is also the seqlock sequence: stable snapshots must start even and
// coefficient writers advance through odd -> even. Bump releases by two.
static CBO_EPOCH: AtomicU64 = AtomicU64::new(4);

/// Current cost-model epoch used by the physical-plan cache.
#[inline]
pub fn epoch() -> u64 {
    CBO_EPOCH.load(Ordering::Acquire)
}

fn bump_epoch() {
    CBO_EPOCH.fetch_add(1, Ordering::AcqRel);
}

fn validate_variable(name: &str, value: f64) -> Result<(), String> {
    if !value.is_finite() {
        return Err(format!("variable '{name}' must be finite"));
    }

    let lower = name.to_ascii_lowercase();
    let is_selectivity = matches!(
        lower.as_str(),
        "cbo_default_eq_selectivity"
            | "cbo_default_range_selectivity"
            | "cbo_default_like_selectivity"
            | "cbo_like_char_selectivity"
            | "cbo_default_join_selectivity"
            | "cbo_time_aligned_histogram"
    );
    if is_selectivity && !(0.0..=1.0).contains(&value) {
        return Err(format!("variable '{name}' must be between 0 and 1"));
    }
    if lower == "cbo_join_planning_budget_ms" && value <= 0.0 {
        return Err(format!("variable '{name}' must be greater than 0"));
    }
    if value < 0.0 {
        return Err(format!(
            "variable '{name}' must be greater than or equal to 0"
        ));
    }
    Ok(())
}

fn init_from_env() {
    INIT.call_once(|| {
        for (name, atomic_fn) in coefficient_table() {
            let env_name = format!("CBO_{}", name.trim_start_matches("cbo_").to_uppercase());
            if let Ok(val) = std::env::var(&env_name) {
                match val
                    .parse::<f64>()
                    .map_err(|_| format!("variable '{name}' must be numeric"))
                    .and_then(|parsed| {
                        validate_variable(name, parsed)?;
                        Ok(parsed)
                    }) {
                    Ok(parsed) => atomic_fn().store(parsed),
                    Err(message) => {
                        eprintln!("mydb: ignoring invalid {env_name}={val:?}: {message}")
                    }
                }
            }
        }
    });
}

/// Read-only accessor for the process-global coefficient snapshot. Cheap;
/// every call is two relaxed atomic loads.
pub fn consts() -> &'static CboConsts {
    init_from_env();
    &GLOBAL
}

/// Iterate every (name, atomic) pair. Centralizing the registry here keeps
/// `SHOW VARIABLES` and `SET GLOBAL` automatically in sync with the struct.
static COEFFICIENT_TABLE: &[(&str, fn() -> &'static AtomicF64)] = &[
    ("cbo_seq_page_cost", || &GLOBAL.seq_page_cost),
    ("cbo_rnd_page_cost", || &GLOBAL.rnd_page_cost),
    ("cbo_cpu_tuple_cost", || &GLOBAL.cpu_tuple_cost),
    ("cbo_cpu_index_tuple_cost", || &GLOBAL.cpu_index_tuple_cost),
    ("cbo_cpu_operator_cost", || &GLOBAL.cpu_operator_cost),
    ("cbo_hash_build_cost", || &GLOBAL.hash_build_cost),
    ("cbo_hash_probe_cost", || &GLOBAL.hash_probe_cost),
    ("cbo_sort_cost_factor", || &GLOBAL.sort_cost_factor),
    ("cbo_mem_page_cost", || &GLOBAL.mem_page_cost),
    ("cbo_spill_page_cost", || &GLOBAL.spill_page_cost),
    ("cbo_effective_cache_mb", || &GLOBAL.effective_cache_mb),
    ("cbo_default_eq_selectivity", || {
        &GLOBAL.default_eq_selectivity
    }),
    ("cbo_default_range_selectivity", || {
        &GLOBAL.default_range_selectivity
    }),
    ("cbo_default_like_selectivity", || {
        &GLOBAL.default_like_selectivity
    }),
    ("cbo_like_char_selectivity", || {
        &GLOBAL.like_char_selectivity
    }),
    ("cbo_default_join_selectivity", || {
        &GLOBAL.default_join_selectivity
    }),
    ("cbo_join_planning_budget_ms", || {
        &GLOBAL.join_planning_budget_ms
    }),
    ("cbo_parallel_setup_cost", || &GLOBAL.parallel_setup_cost),
    ("cbo_time_aligned_histogram", || {
        &GLOBAL.time_aligned_histogram
    }),
    ("cbo_cpu_byte_decode_cost", || &GLOBAL.cpu_byte_decode_cost),
];

fn coefficient_table() -> &'static [(&'static str, fn() -> &'static AtomicF64)] {
    COEFFICIENT_TABLE
}

#[cfg(test)]
/// Recognised by `SET GLOBAL`. Returns true if the variable name (in
/// lowercase) matches one of the known `cbo_*` coefficients.
pub fn is_cbo_variable(name: &str) -> bool {
    let lower = name.to_ascii_lowercase();
    coefficient_table().iter().any(|(n, _)| *n == lower)
}

/// Apply a `SET GLOBAL cbo_xxx = N` write. Returns `Err` if the variable
/// name isn't a known cbo coefficient.
pub fn set_variable(name: &str, value: f64) -> Result<(), String> {
    let lower = name.to_ascii_lowercase();
    for (n, atomic_fn) in coefficient_table() {
        if *n == lower {
            validate_variable(name, value)?;
            let prior = atomic_fn().load();
            if prior.to_bits() != value.to_bits() {
                // Seqlock protocol: odd while the coefficient is changing,
                // even once readers may capture a coherent snapshot.
                bump_epoch();
                atomic_fn().store(value);
                bump_epoch();
            } else {
                atomic_fn().store(value);
            }
            return Ok(());
        }
    }
    Err(format!("unknown system variable '{name}'"))
}

/// Snapshot for `SHOW VARIABLES`. Returns `(name, value_string)` pairs in
/// stable order so EXPLAIN/snapshot tests stay deterministic.
pub fn variables_snapshot() -> Vec<(&'static str, String)> {
    init_from_env();
    coefficient_table()
        .iter()
        .map(|(name, atomic_fn)| (*name, format_f64(atomic_fn().load())))
        .collect()
}

/// Read a single coefficient as a string (used by `SELECT @@global.cbo_*`).
pub fn variable_value(name: &str) -> Option<String> {
    init_from_env();
    let lower = name.to_ascii_lowercase();
    coefficient_table()
        .iter()
        .find(|(n, _)| *n == lower)
        .map(|(_, atomic_fn)| format_f64(atomic_fn().load()))
}

fn format_f64(v: f64) -> String {
    // Keep integers integer-shaped so users see "1" not "1.0000000".
    if v.fract() == 0.0 && v.abs() < 1e15 {
        format!("{}", v as i64)
    } else {
        // Trim trailing zeros for compactness without losing precision.
        let s = format!("{:.6}", v);
        let trimmed = s.trim_end_matches('0').trim_end_matches('.').to_string();
        if trimmed.is_empty() {
            "0".to_string()
        } else {
            trimmed
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_are_set() {
        let c = consts();
        assert_eq!(c.seq_page_cost(), 1.0);
        assert_eq!(c.rnd_page_cost(), 4.0);
        assert_eq!(c.mem_page_cost(), 0.2);
        assert!(c.cpu_tuple_cost() > 0.0);
    }

    #[test]
    fn set_variable_round_trips() {
        // Read current, write new, verify, restore.
        let c = consts();
        let prior = c.rnd_page_cost();
        set_variable("cbo_rnd_page_cost", 7.5).unwrap();
        assert_eq!(c.rnd_page_cost(), 7.5);
        set_variable("cbo_rnd_page_cost", prior).unwrap();
    }

    #[test]
    fn set_variable_rejects_unknown() {
        let err = set_variable("cbo_does_not_exist", 1.0).unwrap_err();
        assert!(err.contains("unknown"));
    }

    #[test]
    fn set_variable_rejects_nan() {
        let err = set_variable("cbo_seq_page_cost", f64::NAN).unwrap_err();
        assert!(err.contains("finite"));
    }

    #[test]
    fn set_variable_validates_semantic_ranges() {
        assert!(set_variable("cbo_default_eq_selectivity", 1.01).is_err());
        assert!(set_variable("cbo_default_join_selectivity", -0.01).is_err());
        assert!(set_variable("cbo_seq_page_cost", -1.0).is_err());
        assert!(set_variable("cbo_join_planning_budget_ms", 0.0).is_err());
    }

    #[test]
    fn changing_a_coefficient_bumps_epoch() {
        let prior = consts().rnd_page_cost();
        let before = epoch();
        set_variable("cbo_rnd_page_cost", prior + 0.125).unwrap();
        assert!(epoch() > before);
        set_variable("cbo_rnd_page_cost", prior).unwrap();
    }

    #[test]
    fn snapshot_is_self_consistent() {
        let snapshot = consts().snapshot();
        assert!(snapshot.epoch > 0);
        assert_eq!(snapshot.epoch & 1, 0, "stable seqlock epoch must be even");
        assert_eq!(cbo_model(), "v2");
        assert!(snapshot.seq_page_cost.is_finite());
        assert!((0.0..=1.0).contains(&snapshot.default_join_selectivity));
    }

    #[test]
    fn snapshot_lists_all_coefficients() {
        let snap = variables_snapshot();
        assert!(snap.iter().any(|(n, _)| *n == "cbo_seq_page_cost"));
        assert!(snap.iter().any(|(n, _)| *n == "cbo_rnd_page_cost"));
        assert!(snap.iter().any(|(n, _)| *n == "cbo_default_eq_selectivity"));
        // Snapshot length should match the coefficient_table entries.
        assert_eq!(snap.len(), coefficient_table().len());
    }

    #[test]
    fn variable_value_is_case_insensitive() {
        assert!(variable_value("CBO_SEQ_PAGE_COST").is_some());
        assert!(variable_value("cbo_seq_page_cost").is_some());
        assert!(variable_value("nonsense").is_none());
    }

    #[test]
    fn is_cbo_variable_recognises_known() {
        assert!(is_cbo_variable("cbo_rnd_page_cost"));
        assert!(is_cbo_variable("CBO_RND_PAGE_COST"));
        assert!(!is_cbo_variable("mydb_buffer_pool_pages"));
    }

    #[test]
    fn format_f64_compacts() {
        assert_eq!(format_f64(1.0), "1");
        assert_eq!(format_f64(4.0), "4");
        assert_eq!(format_f64(0.005), "0.005");
        assert_eq!(format_f64(0.333), "0.333");
    }
}
