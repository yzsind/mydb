//! Common Table Expression (CTE) execution support.
//!
//! Materialised CTE results live in a thread-local registry that is installed
//! while a `WITH` statement's body is being executed. SELECT execution paths
//! consult this registry to resolve table references whose name matches a CTE.
//! For `WITH RECURSIVE`, this module also drives the iterative evaluation of
//! anchor + recursive parts.

use std::cell::RefCell;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use crate::engine::api::QueryResponse;
use crate::engine::api::{CteResult, SessionContext};
use crate::engine::expr::ValueKey;
use crate::engine::session_registry::SessionRegistry;
use crate::parser::{
    BinaryOp, CteDef, Expr, QuantifiedComparisonRight, Select, SelectItem, SetOp, Statement,
    SubQuery, TableRef,
};
use crate::storage::{error, Database, IsolationLevel, TxnView};
use crate::types::{Column, ColumnType, QueryError, Value};

thread_local! {
    /// Active CTE registry for the current thread/statement. The map is
    /// keyed by lowercase CTE name. Empty when no `WITH` is active.
    static CTE_REGISTRY: RefCell<HashMap<String, Arc<CteResult>>> =
        RefCell::new(HashMap::new());
    static CTE_MATERIALIZATION_ACTIVE: std::cell::Cell<bool> =
        const { std::cell::Cell::new(false) };
}

pub(crate) fn cte_materialization_active() -> bool {
    CTE_MATERIALIZATION_ACTIVE.with(|flag| flag.get())
}

fn with_cte_materialization<T>(f: impl FnOnce() -> Result<T, QueryError>) -> Result<T, QueryError> {
    CTE_MATERIALIZATION_ACTIVE.with(|flag| {
        let prev = flag.replace(true);
        let result = f();
        flag.set(prev);
        result
    })
}

/// Run `f` with the CTE-materialization-active flag forced to `active`,
/// restoring the previous value afterwards. Lets a worker thread (e.g. the
/// parallel set-operation branch executor in `dispatch`) re-establish the
/// parent thread's materialization context, since the flag is thread-local and
/// does not cross a spawned thread boundary on its own.
pub(crate) fn with_cte_materialization_active<T>(active: bool, f: impl FnOnce() -> T) -> T {
    CTE_MATERIALIZATION_ACTIVE.with(|flag| {
        let prev = flag.replace(active);
        let result = f();
        flag.set(prev);
        result
    })
}

/// Install `map` as the active CTE registry for the duration of `f`. Restores
/// the previous registry afterwards regardless of whether `f` errors.
pub(crate) fn with_cte_registry<T>(
    map: HashMap<String, Arc<CteResult>>,
    f: impl FnOnce() -> T,
) -> T {
    let prev = CTE_REGISTRY.with(|r| r.replace(map));
    let result = f();
    CTE_REGISTRY.with(|r| {
        *r.borrow_mut() = prev;
    });
    result
}

/// Like [`with_cte_registry`] but *extends* the currently-active registry with
/// `additional` entries instead of replacing it, so any enclosing `WITH`'s
/// CTEs stay visible. Used when a derived table is materialised into a single
/// registry entry and the statement is re-dispatched while an outer `WITH` is
/// still in scope (e.g. `WITH c AS (...) SELECT * FROM (SELECT FROM c) a, (SELECT
/// FROM c) b` — materialising `a` must not hide `c` from `b`). On a name
/// collision the new entry shadows the outer CTE, matching SQL scoping.
pub(crate) fn with_cte_registry_extended<T>(
    additional: HashMap<String, Arc<CteResult>>,
    f: impl FnOnce() -> T,
) -> T {
    let mut merged = CTE_REGISTRY.with(|r| r.borrow().clone());
    merged.extend(additional);
    with_cte_registry(merged, f)
}

/// Snapshot the currently-active CTE registry. Expression subqueries may be
/// evaluated from helper paths that re-enter SELECT execution, so they carry a
/// snapshot of the lexical CTE scope instead of relying on whichever registry
/// happens to be installed at the call site.
pub(crate) fn current_cte_registry() -> HashMap<String, Arc<CteResult>> {
    CTE_REGISTRY.with(|r| r.borrow().clone())
}

/// Look up a CTE by case-insensitive name in the active registry.
pub(crate) fn lookup_cte(name: &str) -> Option<Arc<CteResult>> {
    let key = name.to_ascii_lowercase();
    CTE_REGISTRY.with(|r| r.borrow().get(&key).cloned())
}

/// Allocate a registry identity for one materialized derived relation. SQL
/// aliases are local to a query block and must never key a shared spool map.
/// A NUL prefix keeps these internal names outside the SQL identifier namespace;
/// the process-wide counter also separates nested materializers and workers.
pub(super) fn next_inline_derived_name() -> String {
    static NEXT_ID: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let id = NEXT_ID.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    format!("\0mydb_inline_derived_{id}")
}

/// Update the named entry in the active registry. Used by the recursive CTE
/// loop to swap the working table between iterations.
pub(crate) fn set_cte_result(name: &str, result: Arc<CteResult>) {
    let key = name.to_ascii_lowercase();
    CTE_REGISTRY.with(|r| {
        r.borrow_mut().insert(key, result);
    });
}

/// True when the active registry contains a matching CTE name.
pub(crate) fn has_cte(name: &str) -> bool {
    let key = name.to_ascii_lowercase();
    CTE_REGISTRY.with(|r| r.borrow().contains_key(&key))
}

/// Per-CTE in-memory cap before its rows spill to disk. Keeps the
/// thread-local CTE registry bounded so a `WITH` with several large CTEs
/// (e.g. TPC-DS Q2/Q5/Q14/Q23) cannot accumulate the whole working set
/// in memory and raise 1037. `MYDB_CTE_SPILL_THRESHOLD_MB` overrides;
/// otherwise use the active per-query budget. `CteSpillBuilder` reserves each
/// resident capacity increment from the unified governor, so independent CTEs
/// share the real process headroom and spill on reservation pressure instead
/// of each assuming this whole threshold is privately available.
fn cte_spill_threshold_bytes() -> usize {
    if let Some(mb) = crate::util::env::var("MYDB_CTE_SPILL_THRESHOLD_MB")
        .ok()
        .and_then(|s| s.trim().parse::<usize>().ok())
    {
        return mb.saturating_mul(1024 * 1024);
    }
    let budget = crate::engine::executor::mem_budget::current_budget()
        .map(|b| b.limit())
        .unwrap_or_else(crate::engine::executor::mem_budget::query_mem_limit_bytes);
    budget.max(256 * 1024)
}

fn cte_plan_should_stream(plan: &crate::planner::PhysicalPlan) -> bool {
    let est_rows = crate::planner::estimated_rows(plan).max(0.0);
    let est_bytes = (est_rows as usize).saturating_mul(256);
    est_bytes >= cte_spill_threshold_bytes()
}

fn cte_union_branch_parallel_enabled() -> bool {
    // Default ON. UNION ALL CTE bodies (3 GROUP BY branches in TPC-DS Q4/Q11/
    // Q47/Q57/Q74) are otherwise consumed by `chained_union_all` (dispatch.rs)
    // strictly serially even though each branch is independent and bounded by
    // the same read snapshot — wasted parallelism. Off restores the serial
    // chained path for diffing.
    crate::util::env::var("MYDB_CTE_UNION_BRANCH_PARALLEL")
        .map(|v| {
            let v = v.trim();
            !(v == "0" || v.eq_ignore_ascii_case("off") || v.eq_ignore_ascii_case("false"))
        })
        .unwrap_or(true)
}

/// How many UNION-ALL CTE branches run concurrently in the parallel
/// materialization fast path.
///
/// Initial hypothesis: bound concurrency to 2 to avoid CPU oversubscription
/// (each branch internally uses `MYDB_PARALLEL_DEGREE=2` workers, so 3
/// concurrent branches × 2 = 6 workers on 4 P-cores look oversubscribed).
///
/// Empirically the heaviest branch finishes *sooner* when more siblings
/// share the run: TPC-DS Q4 store_sales branch (1.07 M rows INLJ → 76 k
/// groups) measured at 1905 ms wall with K=3 versus **2066 ms with K=2**.
/// Reason: under K=3 the heavy branch enjoys *falling* contention — once
/// the small web_sales branch (~820 ms) and catalog_sales (~1273 ms)
/// finish, the heavy branch runs the last ~600 ms alone, recouping cache /
/// memory bandwidth. Under K=2 the heavy branch contends with *something*
/// for its entire run (B1 followed by B2), so the constant 2-way pressure
/// hurts more than a brief 3-way burst.
///
/// Default 4 (= perf-core count on the target M-series; small enough to
/// avoid pathological 10-branch UNION ALL fanout, large enough to admit
/// every TPC-DS UNION-ALL CTE body in source order). Env override allows
/// tuning per workload.
fn cte_union_branch_concurrency() -> usize {
    crate::util::env::var("MYDB_CTE_UNION_BRANCH_CONCURRENCY")
        .ok()
        .and_then(|v| v.trim().parse::<usize>().ok())
        .filter(|v| *v > 0)
        .unwrap_or(4)
}

/// Builds a CTE result, spilling to a quota-protected disk file once the
/// accumulated rows exceed [`cte_spill_threshold_bytes`]. Small CTEs
/// stay in memory under the query/process budget; large ones become a
/// `CteResult::spilled`. CteScan streams them directly, while the legacy JOIN
/// collection adapter uses [`spilled_cte_read_all`] with a retained hard
/// charge.
pub(super) struct CteSpillBuilder {
    columns: Vec<crate::types::Column>,
    ncols: usize,
    data_dir: std::path::PathBuf,
    buffered: Vec<Vec<Value>>,
    buffered_bytes: usize,
    /// Admitted capacity not yet occupied by rows. Small prepayments amortize
    /// the global governor atomics without changing the resident threshold.
    unspent_bytes: usize,
    threshold: usize,
    memory_charge: Option<crate::engine::executor::mem_budget::ControlledMemoryCharge>,
    row_count: usize,
    spill: Option<CteSpillWriteState>,
    /// P1-4: per-column stats collector, present only on the CTE
    /// materialisation path (derived tables skip it to avoid the
    /// per-value hashing in hot derived-table flows).
    stats: Option<CteStatsBuilder>,
}

/// Distinct-value tracking cap per column. Once a column exceeds this many
/// distinct values it is recorded as high-cardinality (`ndv = None`) and
/// tracking stops, bounding both memory and the per-row hashing cost.
const CTE_NDV_CAP: usize = 4096;

/// Accumulates exact (capped) per-column statistics for small CTEs (P1-4).
/// Large CTEs abandon stats once they cross `MYDB_CTE_STATS_MAX_ROWS`; stats
/// only guide CBO selectivity, and wide large-CTE hashing can otherwise
/// dominate materialisation cost.
struct CteStatsBuilder {
    ncols: usize,
    seen_rows: usize,
    max_rows: usize,
    non_null: Vec<u64>,
    distinct: Vec<Option<std::collections::HashSet<crate::engine::expr::ValueKey>>>,
}

impl CteStatsBuilder {
    fn new(ncols: usize) -> Self {
        Self {
            ncols,
            seen_rows: 0,
            max_rows: cte_stats_max_rows(),
            non_null: vec![0; ncols],
            distinct: (0..ncols)
                .map(|_| Some(std::collections::HashSet::new()))
                .collect(),
        }
    }

    fn observe(&mut self, row: &[Value]) -> bool {
        self.seen_rows += 1;
        if self.seen_rows > self.max_rows {
            return false;
        }
        for (i, v) in row.iter().take(self.ncols).enumerate() {
            if matches!(v, Value::Null) {
                continue;
            }
            self.non_null[i] += 1;
            if let Some(set) = &mut self.distinct[i] {
                set.insert(crate::engine::expr::ValueKey::from(v));
                if set.len() > CTE_NDV_CAP {
                    self.distinct[i] = None;
                }
            }
        }
        true
    }

    fn finish(self, rows: u64) -> crate::engine::api::CteStats {
        let columns = (0..self.ncols)
            .map(|i| crate::engine::api::CteColumnStats {
                non_null: self.non_null[i],
                ndv: self.distinct[i].as_ref().map(|s| s.len() as u64),
            })
            .collect();
        crate::engine::api::CteStats { rows, columns }
    }
}

/// `MYDB_CTE_STATS` gate (default on). Stats only inform the planner, so
/// disabling never changes results.
fn cte_stats_enabled() -> bool {
    match crate::util::env::var("MYDB_CTE_STATS") {
        Ok(v) => !matches!(
            v.trim().to_ascii_lowercase().as_str(),
            "off" | "0" | "false"
        ),
        Err(_) => true,
    }
}

fn cte_stats_max_rows() -> usize {
    crate::util::env::var("MYDB_CTE_STATS_MAX_ROWS")
        .ok()
        .and_then(|s| s.trim().parse::<usize>().ok())
        .filter(|&n| n > 0)
        .unwrap_or(1)
}

struct CteSpillWriteState {
    dir: Arc<crate::engine::executor::select::grace::SpillTmpDir>,
    path: std::path::PathBuf,
    writer: crate::engine::executor::select::grace::ValueRowWriter,
}

impl CteSpillBuilder {
    pub(super) fn new(columns: Vec<crate::types::Column>, data_dir: std::path::PathBuf) -> Self {
        Self::new_inner(columns, data_dir, false)
    }

    /// Like [`new`] but also gathers per-column statistics (P1-4). Used by the
    /// CTE materialisation path; the derived-table path uses [`new`].
    pub(super) fn new_with_stats(
        columns: Vec<crate::types::Column>,
        data_dir: std::path::PathBuf,
    ) -> Self {
        Self::new_inner(columns, data_dir, true)
    }

    fn new_inner(
        columns: Vec<crate::types::Column>,
        data_dir: std::path::PathBuf,
        collect_stats: bool,
    ) -> Self {
        let ncols = columns.len();
        let stats = (collect_stats && cte_stats_enabled()).then(|| CteStatsBuilder::new(ncols));
        let memory_charge =
            crate::engine::executor::mem_budget::ControlledMemoryCharge::try_new_optional(
                crate::engine::executor::mem_budget::ControlledMemoryCategory::DerivedCache,
                0,
            );
        Self {
            columns,
            ncols,
            data_dir,
            buffered: Vec::new(),
            buffered_bytes: 0,
            unspent_bytes: 0,
            threshold: memory_charge
                .as_ref()
                .map(|_| cte_spill_threshold_bytes())
                .unwrap_or(0),
            memory_charge,
            row_count: 0,
            spill: None,
            stats,
        }
    }

    pub(super) fn push(&mut self, row: Vec<Value>) -> Result<(), QueryError> {
        self.row_count += 1;
        if let Some(stats) = self.stats.as_mut() {
            if !stats.observe(&row) {
                self.stats = None;
            }
        }
        if let Some(state) = self.spill.as_mut() {
            state.writer.append(&row)?;
            return Ok(());
        }

        use crate::engine::executor::memory_footprint::AllocatedBytes;
        let row_bytes = row
            .capacity()
            .saturating_mul(std::mem::size_of::<Value>())
            .saturating_add(row.iter().fold(0usize, |bytes, value| {
                bytes.saturating_add(value.allocated_bytes())
            }));
        let needs_outer_growth = self.buffered.len() == self.buffered.capacity();
        let predicted_outer_slots = if needs_outer_growth {
            self.buffered.capacity().max(4)
        } else {
            0
        };
        let predicted_outer_bytes =
            predicted_outer_slots.saturating_mul(std::mem::size_of::<Vec<Value>>());
        let extra = row_bytes.saturating_add(predicted_outer_bytes);
        if self.buffered_bytes.saturating_add(extra) > self.threshold
            || !self.reserve_resident(extra)
        {
            self.start_spill()?;
            self.spill
                .as_mut()
                .expect("spill initialized")
                .writer
                .append(&row)?;
            return Ok(());
        }

        if needs_outer_growth {
            let old_capacity = self.buffered.capacity();
            if self.buffered.try_reserve(1).is_err() {
                self.unspent_bytes = self.unspent_bytes.saturating_add(extra);
                return Err(crate::engine::executor::mem_budget::allocation_error(
                    "unable to reserve materialized CTE row storage",
                ));
            }
            let actual_outer_bytes = self
                .buffered
                .capacity()
                .saturating_sub(old_capacity)
                .saturating_mul(std::mem::size_of::<Vec<Value>>());
            if actual_outer_bytes > predicted_outer_bytes {
                let correction = actual_outer_bytes - predicted_outer_bytes;
                if !self.reserve_resident(correction) {
                    self.buffered_bytes = self
                        .memory_charge
                        .as_ref()
                        .map(|charge| charge.bytes())
                        .unwrap_or(0);
                    self.start_spill()?;
                    self.spill
                        .as_mut()
                        .expect("spill initialized")
                        .writer
                        .append(&row)?;
                    return Ok(());
                }
            } else {
                self.unspent_bytes = self
                    .unspent_bytes
                    .saturating_add(predicted_outer_bytes - actual_outer_bytes);
            }
        }
        self.buffered_bytes = self
            .memory_charge
            .as_ref()
            .map(|charge| charge.bytes().saturating_sub(self.unspent_bytes))
            .unwrap_or(0);
        self.buffered.push(row);
        Ok(())
    }

    pub(super) fn consume_cursor(
        &mut self,
        cursor: &mut dyn crate::engine::api::ResultCursor,
    ) -> Result<(), QueryError> {
        use crate::engine::api::ResultChunk;
        while let Some(chunk) = cursor.next_chunk()? {
            crate::engine::cancel::check_cancelled()?;
            match chunk {
                ResultChunk::Row(row) => self.push(row)?,
                ResultChunk::Columnar(owned) => {
                    let batch = &owned.batch;
                    if batch.columns().len() != self.ncols {
                        return Err(error(
                            crate::error::catalog::ER_INTERNAL_ERROR,
                            "CTE batch width differs from its result metadata",
                        ));
                    }
                    for row in batch.visible_indices() {
                        if self.stats.is_none() {
                            if let Some(spill) = self.spill.as_mut() {
                                spill.writer.append_batch_row(batch, row)?;
                                self.row_count += 1;
                                continue;
                            }
                        }
                        self.push(batch.row(row))?;
                    }
                    // `owned` retains the producer's reservation until every
                    // visible row has been moved into resident or spill storage.
                }
            }
        }
        Ok(())
    }

    fn reserve_resident(&mut self, extra: usize) -> bool {
        if self.unspent_bytes >= extra {
            self.unspent_bytes -= extra;
            return true;
        }
        let Some(charge) = self.memory_charge.as_mut() else {
            return false;
        };
        let needed = extra - self.unspent_bytes;
        let remaining = self.threshold.saturating_sub(charge.bytes());
        if needed > remaining {
            return false;
        }
        let quantum = needed.max(64 * 1024).min(remaining);
        let admitted = if charge.try_grow_optional(quantum) {
            quantum
        } else if quantum != needed && charge.try_grow_optional(needed) {
            needed
        } else {
            return false;
        };
        self.unspent_bytes = admitted - needed;
        true
    }

    /// Transition to disk: create the spill dir + writer and flush every
    /// buffered row, then drop the in-memory buffer.
    fn start_spill(&mut self) -> Result<(), QueryError> {
        let dir = Arc::new(
            crate::engine::executor::select::grace::SpillTmpDir::create_kind(
                &self.data_dir,
                "cte",
            )?,
        );
        let path = dir.path().join("cte-rows.bin");
        let mut writer = crate::engine::executor::select::grace::ValueRowWriter::create(
            &path,
            self.ncols,
            dir.charger(),
        )?;
        for row in self.buffered.drain(..) {
            writer.append(&row)?;
        }
        self.buffered.shrink_to_fit();
        if let Some(charge) = self.memory_charge.as_mut() {
            charge.shrink(charge.bytes());
        }
        self.buffered_bytes = 0;
        self.unspent_bytes = 0;
        self.spill = Some(CteSpillWriteState { dir, path, writer });
        Ok(())
    }

    pub(super) fn finish(mut self) -> Result<CteResult, QueryError> {
        let row_count = self.row_count;
        let stats = self
            .stats
            .take()
            .map(|s| std::sync::Arc::new(s.finish(row_count as u64)));
        if let Some(state) = self.spill.take() {
            state.writer.finish()?;
            let spill_file = Arc::new(crate::engine::api::CteSpillFile {
                _dir: state.dir,
                path: state.path,
                row_count,
            });
            crate::engine::status_counters::record_internal_temp_table(true);
            return Ok(CteResult::spilled(self.columns, spill_file).with_stats(stats));
        }
        let mut charge = self
            .memory_charge
            .take()
            .expect("in-memory CTE must retain its governor reservation");
        charge.shrink(self.unspent_bytes);
        crate::engine::status_counters::record_internal_temp_table(false);
        Ok(CteResult::materialized_charged(self.columns, self.buffered, charge).with_stats(stats))
    }
}

/// Materialise an already-built row set into a `CteResult`, spilling to
/// disk when it exceeds [`cte_spill_threshold_bytes`]. Shared by the CTE
/// path and the derived-table path (`execute_derived_select`) so a large
/// derived body / UNION result is bounded the same way a large CTE is.
pub(super) fn materialize_rows_with_spill(
    columns: Vec<crate::types::Column>,
    rows: Vec<Vec<Value>>,
    data_dir: std::path::PathBuf,
) -> Result<CteResult, QueryError> {
    let mut builder = CteSpillBuilder::new(columns, data_dir);
    for row in rows {
        builder.push(row)?;
    }
    builder.finish()
}

pub(crate) struct ChargedCteRows {
    pub(crate) rows: Arc<Vec<Vec<Value>>>,
    pub(crate) charge: Option<Arc<crate::engine::executor::mem_budget::OwnedMemCharge>>,
}

/// Compatibility collection API for JOIN-source consumers that still need a
/// random-access slice. Unlike the historical read-all path, every retained
/// row remains hard-budgeted for as long as the returned owner is alive.
pub(crate) fn spilled_cte_read_all(
    spill: &crate::engine::api::CteSpillFile,
) -> Result<ChargedCteRows, QueryError> {
    let mut out = Vec::with_capacity(spill.row_count.min(1 << 20));
    let mut charge = match crate::engine::executor::mem_budget::current_budget() {
        Some(budget) => Some(crate::engine::executor::mem_budget::OwnedMemCharge::new(
            budget, 0,
        )?),
        None => None,
    };
    if let Some(mut reader) = crate::engine::executor::select::grace::ValueRowReader::open(
        &spill.path,
        spill._dir.charger(),
    )? {
        while let Some(row) = reader.next()? {
            if let Some(charge) = charge.as_mut() {
                charge.add(crate::engine::executor::mem_budget::estimate_row_bytes(
                    &row,
                ))?;
            }
            out.push(row);
        }
    }
    Ok(ChargedCteRows {
        rows: Arc::new(out),
        charge: charge.map(Arc::new),
    })
}

/// Apply CTE column-list aliasing to a freshly materialised result.
fn apply_column_aliases(
    result: &mut CteResult,
    aliases: &[String],
    cte_name: &str,
) -> Result<(), QueryError> {
    if !aliases.is_empty() && result.columns.len() != aliases.len() {
        return Err(error(
            crate::error::catalog::ER_VIEW_WRONG_LIST,
            format!(
                "CTE '{}' column list has {} names but query produces {} columns",
                cte_name,
                aliases.len(),
                result.columns.len()
            ),
        ));
    }
    for (col, alias) in result.columns.iter_mut().zip(aliases.iter()) {
        col.name = alias.clone();
    }
    validate_output_names(&result.columns)
}

pub(super) fn validate_output_names(columns: &[crate::types::Column]) -> Result<(), QueryError> {
    // One output cannot have a duplicate name. In particular, recursive
    // deltas must not allocate a name set on every scalar iteration.
    if columns.len() < 2 {
        return Ok(());
    }
    let mut seen = HashSet::new();
    for column in columns {
        if !seen.insert(column.name.to_ascii_lowercase()) {
            return Err(error(
                crate::error::catalog::ER_DUP_FIELDNAME,
                format!("Duplicate column name '{}'", column.name),
            ));
        }
    }
    Ok(())
}

/// Materialise every CTE in `ctes` and return the resulting registry.
///
/// MySQL only allows earlier same-level CTE references; mydb deliberately
/// supports the more permissive non-recursive forward-reference shape by
/// materialising CTEs in dependency order. Self-recursive CTEs still use the
/// dedicated recursive evaluator, and mutually-recursive CTEs are rejected
/// until we have a fixpoint evaluator for multiple working tables.
pub(super) fn materialize_ctes(
    db: &Arc<Database>,
    session: &mut SessionContext,
    ctes: &[CteDef],
    body: &Statement,
    registry: &SessionRegistry,
) -> Result<HashMap<String, Arc<CteResult>>, QueryError> {
    let cte_keys = validate_cte_names(ctes)?;
    let dependencies = collect_cte_dependencies(ctes, &cte_keys);
    let lazy_union_plans = lazy_union_aggregate_plans(ctes, body, &cte_keys);
    let mut lazy_ready: HashMap<String, LazyUnionCte> = HashMap::new();
    let mut accum: HashMap<String, Arc<CteResult>> = HashMap::new();
    let mut cte_cache_keys: HashMap<String, String> = HashMap::new();
    let mut cte_cacheable: HashSet<String> = HashSet::new();
    let cte_cache_snapshot = db.visible_scn();
    let outer = current_cte_registry();
    // Session cache keys currently encode same-level dependencies only.
    // An enclosing working table can change every recursive iteration.
    let cte_cache_allowed = outer.is_empty() && cte_result_cache_allowed(session, body);
    let mut pending: Vec<usize> = (0..ctes.len()).collect();
    while !pending.is_empty() {
        let mut ready = Vec::new();
        for &idx in &pending {
            let key = &cte_keys[idx];
            let deps = &dependencies[idx];
            let self_ref = deps.contains(key);
            if self_ref && !ctes[idx].is_recursive {
                return Err(error(
                    crate::error::catalog::ER_NOT_SUPPORTED_YET,
                    format!("recursive CTE '{}' requires WITH RECURSIVE", ctes[idx].name),
                ));
            }
            let external_deps_ready = deps
                .iter()
                .filter(|dep| *dep != key)
                .all(|dep| accum.contains_key(dep) || lazy_ready.contains_key(dep));
            if external_deps_ready {
                ready.push(idx);
            }
        }
        if ready.is_empty() {
            let names = pending
                .iter()
                .map(|idx| ctes[*idx].name.as_str())
                .collect::<Vec<_>>()
                .join(", ");
            return Err(error(
                crate::error::catalog::ER_NOT_SUPPORTED_YET,
                format!("mutually recursive CTEs are not supported: {names}"),
            ));
        }

        let parallel_allowed = cte_parallel_materialize_allowed(session, body);
        let mut processed: HashSet<usize> = HashSet::new();
        let mut parallel_jobs = Vec::new();
        for idx in ready {
            let key = &cte_keys[idx];
            let deps = &dependencies[idx];
            if let Some(lazy) = lazy_union_plans.get(key).cloned() {
                lazy_ready.insert(key.clone(), lazy);
                processed.insert(idx);
                continue;
            }
            let mut snapshot = outer.clone();
            snapshot.extend(accum.clone());
            let cache_key = (cte_cache_allowed
                && cte_result_cache_cte_eligible(&ctes[idx])
                && deps
                    .iter()
                    .filter(|dep| *dep != key)
                    .all(|dep| cte_cacheable.contains(dep)))
            .then(|| cte_result_cache_key(session, &ctes[idx], deps, &cte_cache_keys));
            if let Some(cache_key) = cache_key.as_ref() {
                if let Some(cached) =
                    lookup_session_cte_result_cache(db, session, cache_key, cte_cache_snapshot)
                {
                    accum.insert(key.clone(), cached);
                    cte_cache_keys.insert(key.clone(), cache_key.clone());
                    cte_cacheable.insert(key.clone());
                    processed.insert(idx);
                    continue;
                }
            }
            let lazy_dep = deps.iter().find_map(|dep| {
                lazy_ready
                    .get(dep)
                    .filter(|lazy| lazy.consumer_key == *key)
                    .map(|lazy| (dep.as_str(), lazy.clone()))
            });
            let body_result = if let Some((dep, lazy)) = lazy_dep {
                match materialize_aggregate_over_lazy_union(
                    db,
                    session,
                    &ctes[idx],
                    dep,
                    &lazy,
                    snapshot.clone(),
                    registry,
                )? {
                    Some(result) => result,
                    None => materialize_single_cte(db, session, &ctes[idx], snapshot, registry)?,
                }
            } else if parallel_allowed && cte_parallel_materialize_cte_eligible(&ctes[idx]) {
                parallel_jobs.push(ParallelCteJob {
                    idx,
                    key: key.clone(),
                    cte: ctes[idx].clone(),
                    visible: snapshot,
                    cache_key,
                });
                continue;
            } else {
                materialize_single_cte(db, session, &ctes[idx], snapshot, registry)?
            };
            let body_result = Arc::new(body_result);
            if let Some(cache_key) = cache_key {
                let table_versions = collect_cte_dep_versions(db, session, &ctes[idx]);
                maybe_store_session_cte_result_cache(
                    session,
                    cache_key.clone(),
                    cte_cache_snapshot,
                    table_versions,
                    &body_result,
                );
                cte_cache_keys.insert(key.clone(), cache_key);
                cte_cacheable.insert(key.clone());
            }
            accum.insert(key.clone(), body_result);
            processed.insert(idx);
        }

        if parallel_jobs.len() == 1 {
            let job = parallel_jobs.pop().expect("one job");
            let result = materialize_single_cte(db, session, &job.cte, job.visible, registry)?;
            let body_result = Arc::new(result);
            if let Some(cache_key) = job.cache_key {
                let table_versions = collect_cte_dep_versions(db, session, &job.cte);
                maybe_store_session_cte_result_cache(
                    session,
                    cache_key.clone(),
                    cte_cache_snapshot,
                    table_versions,
                    &body_result,
                );
                cte_cache_keys.insert(job.key.clone(), cache_key);
                cte_cacheable.insert(job.key.clone());
            }
            accum.insert(job.key, body_result);
            processed.insert(job.idx);
        } else if parallel_jobs.len() > 1 {
            let results = materialize_cte_jobs_parallel(db, session, parallel_jobs, registry)?;
            for job_result in results {
                let body_result = Arc::new(job_result.result);
                if let Some(cache_key) = job_result.cache_key {
                    let table_versions =
                        collect_cte_dep_versions(db, session, &ctes[job_result.idx]);
                    maybe_store_session_cte_result_cache(
                        session,
                        cache_key.clone(),
                        cte_cache_snapshot,
                        table_versions,
                        &body_result,
                    );
                    cte_cache_keys.insert(job_result.key.clone(), cache_key);
                    cte_cacheable.insert(job_result.key.clone());
                }
                accum.insert(job_result.key, body_result);
                processed.insert(job_result.idx);
            }
        }
        pending.retain(|idx| !processed.contains(idx));
    }

    // Stamp every result with its consumer count so `try_hash_join` can gate
    // the cross-reference hash-build cache (the shared full-store build only
    // amortises when a CTE is consumed more than once). Done centrally here so
    // every construction path — single, parallel, lazy-union, and session-cache
    // hit — is covered uniformly. A session-cache hit returns a shared `Arc`
    // carrying a count from whichever query first built it, so clone-on-write to
    // set this query's count; the clone shares the underlying `Arc<CteStore>`
    // (and its build cache), preserving cross-reference reuse.
    let consumer_counts = compute_cte_consumer_counts(ctes, body, &cte_keys);
    for (key, result) in accum.iter_mut() {
        let count = consumer_counts.get(key).copied().unwrap_or(1).max(1);
        if result.consumer_count == count {
            continue;
        }
        match Arc::get_mut(result) {
            Some(r) => r.consumer_count = count,
            None => {
                let mut cloned = (**result).clone();
                cloned.consumer_count = count;
                *result = Arc::new(cloned);
            }
        }
    }

    Ok(accum)
}

/// Count how many times each CTE is referenced across the enclosing query
/// body and every *other* CTE body (mirrors the `ref_count` logic in the
/// output-domain pushdown rewrite). Keyed by the normalised CTE key so it
/// lines up with `materialize_ctes`'s `accum` map.
fn compute_cte_consumer_counts(
    ctes: &[CteDef],
    body: &Statement,
    cte_keys: &[String],
) -> HashMap<String, usize> {
    let mut out = HashMap::with_capacity(ctes.len());
    for (i, cte) in ctes.iter().enumerate() {
        let mut aliases = Vec::new();
        collect_cte_aliases_stmt(body, &cte.name, &mut aliases);
        for (j, other) in ctes.iter().enumerate() {
            if j != i {
                collect_cte_aliases_stmt(&other.query, &cte.name, &mut aliases);
            }
        }
        out.insert(cte_keys[i].clone(), aliases.len());
    }
    out
}

#[derive(Clone)]
struct ParallelCteJob {
    idx: usize,
    key: String,
    cte: CteDef,
    visible: HashMap<String, Arc<CteResult>>,
    cache_key: Option<String>,
}

struct ParallelCteResult {
    idx: usize,
    key: String,
    cache_key: Option<String>,
    result: CteResult,
}

fn cte_parallel_materialize_enabled() -> bool {
    crate::util::env::var("MYDB_CTE_PARALLEL_MATERIALIZE")
        .map(|v| {
            !matches!(
                v.trim().to_ascii_lowercase().as_str(),
                "0" | "off" | "false" | "no"
            )
        })
        .unwrap_or(true)
}

fn cte_parallel_materialize_allowed(session: &SessionContext, body: &Statement) -> bool {
    cte_parallel_materialize_enabled()
        && session.autocommit
        && !session.explicit_txn
        && session.current_txn.is_none()
        && session.temp_tables.is_empty()
        && session.explicit_table_locks.is_empty()
        && super::dispatch::statement_is_pure_read(body)
}

fn cte_parallel_materialize_cte_eligible(cte: &CteDef) -> bool {
    !cte.is_recursive
        && !statement_references_cte(&cte.query, &cte.name)
        && cte_result_cache_cte_eligible(cte)
}

fn materialize_cte_jobs_parallel(
    db: &Arc<Database>,
    session: &SessionContext,
    jobs: Vec<ParallelCteJob>,
    registry: &SessionRegistry,
) -> Result<Vec<ParallelCteResult>, QueryError> {
    let budget = crate::engine::executor::mem_budget::current_budget();
    let collector = super::select::analyze_stats::current_collector_handle();
    let cancellation = crate::engine::cancel::CancellationSnapshot::capture();
    let inherit_mdl = super::stream_mdl::StatementScope::active(db, session.connection_id as u64);
    let mut results = std::thread::scope(|scope| {
        let mut handles = Vec::with_capacity(jobs.len());
        for job in jobs {
            let db = Arc::clone(db);
            let budget = budget.clone();
            let collector = collector.clone();
            let cancellation = cancellation.clone();
            let mut worker_session = fork_cte_worker_session(session);
            handles.push(scope.spawn(move || {
                // Native CTE plans bypass session dispatch. Install the
                // parent's token and absolute deadline before planning or
                // capturing any nested operator/worker execution context.
                let _cancel_scope = cancellation.enter();
                // These workers are joined before the outer WITH returns.
                // Their nested dispatch may acquire shared tickets, but only
                // the parent may release or transfer the statement's grants.
                let _mdl_scope = inherit_mdl.then(|| {
                    super::stream_mdl::StatementScope::enter(
                        &db,
                        worker_session.connection_id as u64,
                    )
                });
                let run = || {
                    let result = materialize_single_cte(
                        &db,
                        &mut worker_session,
                        &job.cte,
                        job.visible,
                        registry,
                    )?;
                    Ok::<_, QueryError>(ParallelCteResult {
                        idx: job.idx,
                        key: job.key,
                        cache_key: job.cache_key,
                        result,
                    })
                };
                super::select::analyze_stats::with_collector_handle_if_present(collector, || {
                    match budget {
                        Some(budget) => {
                            crate::engine::executor::mem_budget::with_query_budget(budget, run)
                        }
                        None => run(),
                    }
                })
            }));
        }
        let mut out = Vec::with_capacity(handles.len());
        for handle in handles {
            match handle.join() {
                Ok(result) => out.push(result?),
                Err(_) => {
                    return Err(error(
                        crate::error::catalog::ER_UNKNOWN_ERROR,
                        "parallel CTE materialization worker panicked",
                    ));
                }
            }
        }
        Ok(out)
    })?;
    results.sort_by_key(|result| result.idx);
    Ok(results)
}

fn fork_cte_worker_session(parent: &SessionContext) -> SessionContext {
    let mut session = SessionContext {
        autocommit: parent.autocommit,
        isolation: parent.isolation,
        next_txn_isolation: parent.next_txn_isolation,
        next_txn_read_only: parent.next_txn_read_only,
        active_txn_isolation: parent.active_txn_isolation,
        current_database: parent.current_database.clone(),
        user: parent.user.clone(),
        host: parent.host.clone(),
        authenticated_as: parent.authenticated_as.clone(),
        login_user: parent.login_user.clone(),
        active_roles: parent.active_roles.clone(),
        auth_epoch: parent.auth_epoch,
        password_expired: parent.password_expired,
        account_locked: parent.account_locked,
        auth_sandbox: parent.auth_sandbox,
        auth_factors_done: parent.auth_factors_done,
        account_limits: parent.account_limits.clone(),
        current_user_is_system: parent.current_user_is_system,
        internal_system_session: parent.internal_system_session,
        connection_id: parent.connection_id,
        ssl_cipher: parent.ssl_cipher.clone(),
        ssl_version: parent.ssl_version.clone(),
        x509_subject: parent.x509_subject.clone(),
        x509_issuer: parent.x509_issuer.clone(),
        user_variables: parent.user_variables.clone(),
        cte_max_recursion_depth: parent.cte_max_recursion_depth,
        foreign_key_checks: parent.foreign_key_checks,
        sql_log_off: parent.sql_log_off,
        sql_log_bin: parent.sql_log_bin,
        redo_enabled: parent.redo_enabled,
        group_concat_max_len: parent.group_concat_max_len,
        sql_mode: parent.sql_mode.clone(),
        temporal_sql_mode_bits: parent.temporal_sql_mode_bits,
        character_set_client: parent.character_set_client.clone(),
        character_set_connection: parent.character_set_connection.clone(),
        character_set_results: parent.character_set_results.clone(),
        collation_connection: parent.collation_connection.clone(),
        explain_format: parent.explain_format,
        cancel_handle: parent.cancel_handle.clone(),
        query_cancel_generation: parent.query_cancel_generation,
        connection_kill_generation: parent.connection_kill_generation,
        current_query_token: parent.current_query_token.clone(),
        connection_attrs: parent.connection_attrs.clone(),
        cte_result_cache: Arc::clone(&parent.cte_result_cache),
        sysvars: parent.sysvars.clone(),
        ..SessionContext::default()
    };
    session.read_only_txn = true;
    session
}

/// Capture the parent statement's MVCC semantics for session-free CTE plans.
/// CTE workers bypass the ordinary SELECT transaction dispatcher, so they
/// must carry READ UNCOMMITTED's dirty-read bit explicitly (as well as an
/// active transaction's effective isolation level).
fn cte_statement_view(db: &Arc<Database>, session: &SessionContext) -> TxnView {
    let isolation = session.effective_isolation();
    let mut txn = db.session_statement_view(
        session.current_txn.unwrap_or(0),
        isolation,
        super::tx::session_read_only_snapshot(session),
    );
    txn.connection_id = session.connection_id;
    txn.dirty_read = isolation == IsolationLevel::ReadUncommitted;
    txn
}

fn cte_result_cache_enabled() -> bool {
    // Cross-statement result reuse is deliberately unsupported. A CTE may be
    // materialized and replayed inside one statement, but every new statement
    // must execute against its own MVCC snapshot even if a legacy environment
    // variable asks to enable the removed session cache.
    false
}

fn cte_result_cache_max_entries() -> usize {
    crate::util::env::var("MYDB_CTE_RESULT_CACHE_ENTRIES")
        .ok()
        .and_then(|v| v.trim().parse::<usize>().ok())
        .filter(|v| *v > 0)
        .unwrap_or(16)
}

fn cte_result_cache_max_rows() -> usize {
    crate::util::env::var("MYDB_CTE_RESULT_CACHE_ROWS")
        .ok()
        .and_then(|v| v.trim().parse::<usize>().ok())
        .unwrap_or(0)
}

fn cte_result_cache_max_bytes() -> usize {
    crate::util::env::var("MYDB_CTE_RESULT_CACHE_MB")
        .ok()
        .and_then(|v| v.trim().parse::<usize>().ok())
        .filter(|v| *v > 0)
        .unwrap_or(64)
        .saturating_mul(1024 * 1024)
}

fn estimate_cte_result_cache_bytes(result: &CteResult) -> usize {
    if let Some(bytes) = result.store.resident_memory_bytes() {
        return bytes;
    }
    result.rows().iter().fold(0usize, |acc, row| {
        acc.saturating_add(crate::engine::executor::mem_budget::estimate_row_bytes(row))
    })
}

fn cte_result_cache_allowed(session: &SessionContext, body: &Statement) -> bool {
    cte_result_cache_enabled()
        && session.autocommit
        && !session.explicit_txn
        && session.current_txn.is_none()
        && super::dispatch::statement_is_pure_read(body)
}

fn cte_result_cache_cte_eligible(cte: &CteDef) -> bool {
    !cte.is_recursive
        && super::dispatch::statement_is_pure_read(&cte.query)
        && statement_is_deterministic_for_cte_cache(&cte.query)
}

fn statement_is_deterministic_for_cte_cache<'q>(
    statement: impl Into<crate::parser::QueryRef<'q>>,
) -> bool {
    let statement = statement.into();
    match statement {
        crate::parser::QueryRef::Select(select) => !select_has_nondeterministic(select),
        crate::parser::QueryRef::SetOperation(set) => {
            statement_is_deterministic_for_cte_cache(&set.left)
                && statement_is_deterministic_for_cte_cache(&set.right)
        }
        crate::parser::QueryRef::WithCte(with) => {
            with.ctes
                .iter()
                .all(|cte| statement_is_deterministic_for_cte_cache(&cte.query))
                && statement_is_deterministic_for_cte_cache(&with.body)
        }
        _ => false,
    }
}

fn cte_result_cache_key(
    session: &SessionContext,
    cte: &CteDef,
    deps: &HashSet<String>,
    dep_keys: &HashMap<String, String>,
) -> String {
    let mut dep_parts = deps
        .iter()
        .filter(|dep| !dep.eq_ignore_ascii_case(&cte.name))
        .filter_map(|dep| dep_keys.get(dep).map(|key| format!("{dep}={key}")))
        .collect::<Vec<_>>();
    dep_parts.sort();
    let k = format!(
        "db={}|mode={}|cte_cols={:?}|deps=[{}]|query={:?}",
        session.current_database,
        session.sql_mode,
        cte.columns,
        dep_parts.join(";"),
        cte.query
    );
    k
}

thread_local! {
    /// Observability/test counter: session CTE result-cache hits on the
    /// current thread (thread-local so parallel test threads don't
    /// interfere). Bumped once per `lookup_session_cte_result_cache` that
    /// returns a cached result.
    pub(crate) static CTE_RESULT_CACHE_HITS: std::cell::Cell<u64> =
        const { std::cell::Cell::new(0) };
}

/// Snapshot `(table_id, schema_version, write_epoch)` for every base table
/// the CTE body reads. `None` (→ conservative SCN invalidation) when the
/// body isn't a plain analyzable Select or references other CTEs.
fn collect_cte_dep_versions(
    db: &Database,
    session: &SessionContext,
    cte: &CteDef,
) -> Option<Vec<(u64, u64, u64)>> {
    let crate::parser::QueryExpr::Select(select) = &cte.query else {
        return None;
    };
    // Qualify unqualified table names against the session's current database
    // (`FROM inventory` → `<db>.inventory`), exactly as the materialization
    // path does before planning — otherwise `plan_select` can't resolve them.
    let mut qualified = select.clone();
    super::qualification::qualify_select_tables(db, session, &mut qualified).ok()?;
    let deps = super::select::cte_body_base_table_versions(db, &qualified)?;
    Some(
        deps.iter()
            .map(|(_key, tid, sv)| (*tid, *sv, db.table_write_epoch(*tid)))
            .collect(),
    )
}

/// Whether a cache entry is still valid: table-version entries (`Some`) stay
/// fresh while every dependency table matches the live catalog + write epoch
/// (unrelated background commits don't evict them); `None` entries fall back
/// to snapshot-SCN equality.
fn cte_cache_entry_fresh(
    db: &Database,
    entry: &crate::engine::api::SessionCteResultCacheEntry,
    current_scn: u64,
) -> bool {
    match &entry.table_versions {
        Some(versions) => db.cte_dep_versions_match(versions),
        None => entry.snapshot_scn == current_scn,
    }
}

fn lookup_session_cte_result_cache(
    db: &Database,
    session: &SessionContext,
    key: &str,
    snapshot_scn: u64,
) -> Option<Arc<CteResult>> {
    let mut cache = session.cte_result_cache.lock().ok()?;
    // Per-entry invalidation: evict only the entries that are actually
    // stale, rather than clearing the whole cache when any one entry's
    // snapshot moved. This is what lets q39's second statement reuse the
    // `inv` CTE materialised by the first even after an unrelated
    // background commit advanced `visible_scn`.
    let stale: Vec<String> = cache
        .entries
        .iter()
        .filter(|(_, entry)| !cte_cache_entry_fresh(db, entry, snapshot_scn))
        .map(|(k, _)| k.clone())
        .collect();
    for k in stale {
        if let Some(old) = cache.entries.remove(&k) {
            cache.bytes = cache.bytes.saturating_sub(old.bytes);
            cache.order.retain(|o| o != &k);
        }
    }
    let hit = cache
        .entries
        .get(key)
        .map(|entry| Arc::clone(&entry.result));
    let live_bytes = cache.bytes as u64;
    drop(cache);
    if let Some(handle) = &session.cancel_handle {
        handle.set_cte_memory_bytes(live_bytes);
    }
    if hit.is_some() {
        CTE_RESULT_CACHE_HITS.with(|c| c.set(c.get() + 1));
    }
    hit
}

fn maybe_store_session_cte_result_cache(
    session: &SessionContext,
    key: String,
    snapshot_scn: u64,
    table_versions: Option<Vec<(u64, u64, u64)>>,
    result: &Arc<CteResult>,
) {
    let max_rows = cte_result_cache_max_rows();
    if result.spill.is_some() || (max_rows > 0 && result.nrows() > max_rows) {
        return;
    }
    let bytes = estimate_cte_result_cache_bytes(result);
    let max_bytes = cte_result_cache_max_bytes();
    if bytes == 0 || bytes > max_bytes {
        return;
    }
    let Ok(mut cache) = session.cte_result_cache.lock() else {
        return;
    };
    if let Some(old) = cache.entries.remove(&key) {
        cache.bytes = cache.bytes.saturating_sub(old.bytes);
        cache.order.retain(|k| k != &key);
    }
    cache.order.push_back(key.clone());
    cache.bytes = cache.bytes.saturating_add(bytes);
    cache.entries.insert(
        key,
        crate::engine::api::SessionCteResultCacheEntry {
            snapshot_scn,
            table_versions,
            result: Arc::clone(result),
            bytes,
        },
    );
    let max_entries = cte_result_cache_max_entries();
    while cache.entries.len() > max_entries || cache.bytes > max_bytes {
        let Some(oldest) = cache.order.pop_front() else {
            break;
        };
        if let Some(old) = cache.entries.remove(&oldest) {
            cache.bytes = cache.bytes.saturating_sub(old.bytes);
        }
    }
    let live_bytes = cache.bytes as u64;
    drop(cache);
    if let Some(handle) = &session.cancel_handle {
        handle.set_cte_memory_bytes(live_bytes);
    }
}

#[derive(Clone)]
struct LazyUnionCte {
    consumer_key: String,
    branches: Vec<Select>,
}

fn cte_union_agg_pushdown_disabled() -> bool {
    crate::util::env::var("MYDB_CTE_UNION_AGG_PUSHDOWN")
        .map(|v| {
            matches!(
                v.to_ascii_lowercase().as_str(),
                "0" | "off" | "false" | "no"
            )
        })
        .unwrap_or(false)
}

fn lazy_union_aggregate_plans(
    ctes: &[CteDef],
    body: &Statement,
    cte_keys: &[String],
) -> HashMap<String, LazyUnionCte> {
    let mut out = HashMap::new();
    if cte_union_agg_pushdown_disabled() {
        return out;
    }
    for (idx, cte) in ctes.iter().enumerate() {
        let key = &cte_keys[idx];
        if cte.is_recursive {
            continue;
        }
        let Some(branches) = flatten_lazy_union_all_branches(&cte.query) else {
            continue;
        };
        if !branches.iter().all(simple_union_branch_supported) {
            continue;
        }
        let mut refs = count_statement_refs(body, &cte.name);
        let mut consumer: Option<usize> = None;
        for (other_idx, other) in ctes.iter().enumerate() {
            if other_idx == idx {
                continue;
            }
            let n = count_statement_refs(&other.query, &cte.name);
            if n > 0 {
                if consumer.is_none() {
                    consumer = Some(other_idx);
                }
                refs += n;
            }
        }
        if refs != 1 {
            continue;
        }
        let Some(consumer_idx) = consumer else {
            continue;
        };
        if !aggregate_union_consumer_supported(&ctes[consumer_idx], &cte.name) {
            continue;
        }
        out.insert(
            key.clone(),
            LazyUnionCte {
                consumer_key: cte_keys[consumer_idx].clone(),
                branches,
            },
        );
    }
    out
}

fn flatten_lazy_union_all_branches<'q>(
    query: impl Into<crate::parser::QueryRef<'q>>,
) -> Option<Vec<Select>> {
    let query = query.into();
    if let Some(branches) = flatten_union_all_branches(query) {
        return Some(branches);
    }
    let crate::parser::QueryRef::Select(select) = query else {
        return None;
    };
    if select.values.is_some()
        || select.json_table.is_some()
        || select.from.len() > 1
        || !select.joins.is_empty()
        || select.selection.is_some()
        || !select.group_by.is_empty()
        || !select.grouping_sets.is_empty()
        || !matches!(
            select.group_by_modifier,
            crate::parser::GroupByModifier::None
        )
        || select.having.is_some()
        || !select.order_by.is_empty()
        || select.limit.is_some()
        || select.offset.is_some()
        || select.distinct
    {
        return None;
    }
    let Some(SubQuery::SetOp(setop)) = select.derived.as_ref() else {
        return None;
    };
    let branches = flatten_union_all_branches(setop)?;
    if !select.projection.iter().all(|item| match item {
        SelectItem::Expr { expr, alias } => alias.is_none() && matches!(expr, Expr::Identifier(_)),
        SelectItem::Wildcard | SelectItem::QualifiedWildcard(_) => false,
    }) {
        return None;
    }
    Some(branches)
}

fn simple_union_branch_supported(select: &Select) -> bool {
    select.values.is_none()
        && select.json_table.is_none()
        && select.derived.is_none()
        && !select.no_merge
        && select.from.len() == 1
        && select.joins.is_empty()
        && select.group_by.is_empty()
        && select.grouping_sets.is_empty()
        && matches!(
            select.group_by_modifier,
            crate::parser::GroupByModifier::None
        )
        && select.having.is_none()
        && select.order_by.is_empty()
        && select.limit.is_none()
        && select.offset.is_none()
        && !select.distinct
        && !select.for_update
        && select.lock_clause.is_none()
        && select.projection.iter().all(|item| match item {
            SelectItem::Expr { .. } => true,
            SelectItem::Wildcard | SelectItem::QualifiedWildcard(_) => false,
        })
}

fn aggregate_union_consumer_supported(cte: &CteDef, dep_name: &str) -> bool {
    let crate::parser::QueryExpr::Select(select) = &cte.query else {
        return false;
    };
    if select.group_by.is_empty()
        || select.having.is_some()
        || !select.grouping_sets.is_empty()
        || !matches!(
            select.group_by_modifier,
            crate::parser::GroupByModifier::None
        )
        || select.order_by.len() > 0
        || select.limit.is_some()
        || select.offset.is_some()
        || select.distinct
        || select.values.is_some()
        || select.json_table.is_some()
        || select.derived.is_some()
    {
        return false;
    }
    if select
        .from
        .iter()
        .filter(|t| t.name.eq_ignore_ascii_case(dep_name))
        .count()
        != 1
    {
        return false;
    }
    if select
        .joins
        .iter()
        .any(|j| j.table.name.eq_ignore_ascii_case(dep_name) || j.derived.is_some())
    {
        return false;
    }
    let group_cols = select.group_by.len();
    if select.projection.len() <= group_cols {
        return false;
    }
    select.projection[group_cols..]
        .iter()
        .all(is_mergeable_aggregate_item)
}

fn is_mergeable_aggregate_item(item: &SelectItem) -> bool {
    let SelectItem::Expr { expr, .. } = item else {
        return false;
    };
    let Expr::Function {
        name,
        args,
        distinct,
    } = expr
    else {
        return false;
    };
    !distinct && matches!(name.to_ascii_uppercase().as_str(), "SUM" | "COUNT") && !args.is_empty()
}

fn materialize_aggregate_over_lazy_union(
    db: &Arc<Database>,
    session: &mut SessionContext,
    consumer: &CteDef,
    dep_name: &str,
    lazy: &LazyUnionCte,
    visible: HashMap<String, Arc<CteResult>>,
    registry: &SessionRegistry,
) -> Result<Option<CteResult>, QueryError> {
    let crate::parser::QueryExpr::Select(select) = &consumer.query else {
        return Ok(None);
    };
    if !aggregate_union_consumer_supported(consumer, dep_name) {
        return Ok(None);
    }
    let group_cols = select.group_by.len();
    let mut columns = None;
    let mut partials = Vec::new();
    for branch in &lazy.branches {
        let Some(branch_select) = rewrite_union_aggregate_branch(select, dep_name, branch) else {
            return Ok(None);
        };
        let mut stmt = Statement::Select(branch_select);
        let response = with_cte_registry_extended(visible.clone(), || {
            with_cte_materialization(|| {
                super::dispatch::execute_for_cte(db, session, &mut stmt, registry)
            })
        })?;
        let QueryResponse::ResultSet(rs) = response.materialize()? else {
            return Ok(None);
        };
        if columns.is_none() {
            columns = Some(rs.columns.clone());
        }
        partials.push(rs.rows);
    }
    let columns = columns.unwrap_or_default();
    let rows = merge_union_aggregate_partials(partials, group_cols)?;
    crate::engine::status_counters::record_internal_temp_table(false);
    let mut result = CteResult::materialized(columns, rows);
    apply_column_aliases(&mut result, &consumer.columns, &consumer.name)?;
    Ok(Some(result))
}

fn rewrite_union_aggregate_branch(
    consumer: &Select,
    dep_name: &str,
    branch: &Select,
) -> Option<Select> {
    let map = branch_output_exprs(branch)?;
    let mut rewritten = consumer.clone();
    let mut replaced = false;
    for table in &mut rewritten.from {
        if table.name.eq_ignore_ascii_case(dep_name) {
            *table = branch.from[0].clone();
            replaced = true;
            break;
        }
    }
    if !replaced {
        return None;
    }
    for item in &mut rewritten.projection {
        if let SelectItem::Expr { expr, .. } = item {
            rewrite_dep_refs_expr(expr, dep_name, &map);
        }
    }
    for expr in &mut rewritten.group_by {
        rewrite_dep_refs_expr(expr, dep_name, &map);
    }
    if let Some(selection) = &mut rewritten.selection {
        rewrite_dep_refs_expr(selection, dep_name, &map);
    }
    for join in &mut rewritten.joins {
        if let Some(condition) = &mut join.condition {
            rewrite_dep_refs_expr(condition, dep_name, &map);
        }
    }
    if let Some(branch_sel) = &branch.selection {
        rewritten.selection = Some(match rewritten.selection.take() {
            Some(sel) => Expr::Binary {
                left: Box::new(sel),
                op: BinaryOp::And,
                right: Box::new(branch_sel.clone()),
            },
            None => branch_sel.clone(),
        });
    }
    Some(rewritten)
}

fn branch_output_exprs(branch: &Select) -> Option<HashMap<String, Expr>> {
    let mut out = HashMap::new();
    for item in &branch.projection {
        let SelectItem::Expr { expr, alias } = item else {
            return None;
        };
        let name = alias.clone().or_else(|| expr_output_name(expr))?;
        out.insert(name.to_ascii_lowercase(), expr.clone());
    }
    Some(out)
}

fn expr_output_name(expr: &Expr) -> Option<String> {
    match expr {
        Expr::Identifier(name) => Some(name.clone()),
        Expr::CompoundIdentifier(parts) => parts.last().cloned(),
        _ => None,
    }
}

fn rewrite_dep_refs_expr(expr: &mut Expr, dep_name: &str, map: &HashMap<String, Expr>) {
    match expr {
        Expr::Identifier(name) => {
            if let Some(replacement) = map.get(&name.to_ascii_lowercase()) {
                *expr = replacement.clone();
            }
        }
        Expr::CompoundIdentifier(parts) => {
            if parts.len() >= 2
                && parts[parts.len() - 2].eq_ignore_ascii_case(dep_name)
                && map.contains_key(&parts[parts.len() - 1].to_ascii_lowercase())
            {
                *expr = map[&parts[parts.len() - 1].to_ascii_lowercase()].clone();
            }
        }
        Expr::Binary { left, right, .. } => {
            rewrite_dep_refs_expr(left, dep_name, map);
            rewrite_dep_refs_expr(right, dep_name, map);
        }
        Expr::Unary { expr, .. }
        | Expr::IsNull(expr)
        | Expr::IsNotNull(expr)
        | Expr::GroupKeyRef { expr, .. } => rewrite_dep_refs_expr(expr, dep_name, map),
        Expr::Function { args, .. } => {
            for arg in args {
                rewrite_dep_refs_expr(arg, dep_name, map);
            }
        }
        Expr::Case {
            operand,
            conditions,
            results,
            else_result,
        } => {
            if let Some(operand) = operand {
                rewrite_dep_refs_expr(operand, dep_name, map);
            }
            for condition in conditions {
                rewrite_dep_refs_expr(condition, dep_name, map);
            }
            for result in results {
                rewrite_dep_refs_expr(result, dep_name, map);
            }
            if let Some(else_result) = else_result {
                rewrite_dep_refs_expr(else_result, dep_name, map);
            }
        }
        Expr::InList { expr, list, .. } => {
            rewrite_dep_refs_expr(expr, dep_name, map);
            for item in list {
                rewrite_dep_refs_expr(item, dep_name, map);
            }
        }
        Expr::Like { expr, pattern, .. } => {
            rewrite_dep_refs_expr(expr, dep_name, map);
            rewrite_dep_refs_expr(pattern, dep_name, map);
        }
        Expr::Between {
            expr, low, high, ..
        } => {
            rewrite_dep_refs_expr(expr, dep_name, map);
            rewrite_dep_refs_expr(low, dep_name, map);
            rewrite_dep_refs_expr(high, dep_name, map);
        }
        _ => {}
    }
}

fn merge_union_aggregate_partials(
    partials: Vec<Vec<Vec<Value>>>,
    group_cols: usize,
) -> Result<Vec<Vec<Value>>, QueryError> {
    struct State {
        group_values: Vec<Value>,
        aggs: Vec<Value>,
    }
    let mut groups: hashbrown::HashMap<Vec<ValueKey>, State, ahash::RandomState> =
        hashbrown::HashMap::with_hasher(ahash::RandomState::new());
    for rows in partials {
        for row in rows {
            if row.len() < group_cols {
                continue;
            }
            let group_values = row[..group_cols].to_vec();
            let key: Vec<ValueKey> = group_values.iter().map(ValueKey::from).collect();
            let agg_values = row[group_cols..].to_vec();
            use hashbrown::hash_map::Entry;
            match groups.entry(key) {
                Entry::Occupied(mut e) => {
                    let state = e.get_mut();
                    for (dst, src) in state.aggs.iter_mut().zip(agg_values.iter()) {
                        merge_sum_value(dst, src)?;
                    }
                }
                Entry::Vacant(e) => {
                    e.insert(State {
                        group_values,
                        aggs: agg_values,
                    });
                }
            }
        }
    }
    let mut out = Vec::with_capacity(groups.len());
    for (_, state) in groups {
        let mut row = state.group_values;
        row.extend(state.aggs);
        out.push(row);
    }
    Ok(out)
}

fn merge_sum_value(dst: &mut Value, src: &Value) -> Result<(), QueryError> {
    if matches!(src, Value::Null) {
        return Ok(());
    }
    if matches!(dst, Value::Null) {
        *dst = src.clone();
        return Ok(());
    }
    *dst = add_values_for_union_agg(dst, src)?;
    Ok(())
}

fn add_values_for_union_agg(left: &Value, right: &Value) -> Result<Value, QueryError> {
    match (left, right) {
        (Value::DecimalCompact(lm, ls), Value::DecimalCompact(rm, rs)) => {
            let scale = (*ls).max(*rs);
            let l = (*lm as i128)
                .checked_mul(10i128.pow((scale - *ls) as u32))
                .ok_or_else(|| {
                    error(
                        crate::error::catalog::ER_DATA_OUT_OF_RANGE,
                        "decimal overflow in UNION aggregate merge",
                    )
                })?;
            let r = (*rm as i128)
                .checked_mul(10i128.pow((scale - *rs) as u32))
                .ok_or_else(|| {
                    error(
                        crate::error::catalog::ER_DATA_OUT_OF_RANGE,
                        "decimal overflow in UNION aggregate merge",
                    )
                })?;
            let sum = l.checked_add(r).ok_or_else(|| {
                error(
                    crate::error::catalog::ER_DATA_OUT_OF_RANGE,
                    "decimal overflow in UNION aggregate merge",
                )
            })?;
            if let Ok(mag) = i64::try_from(sum) {
                Ok(Value::DecimalCompact(mag, scale))
            } else {
                Ok(Value::Double((sum as f64) / 10f64.powi(i32::from(scale))))
            }
        }
        (Value::Int(l), Value::Int(r)) => l.checked_add(*r).map(Value::Int).ok_or_else(|| {
            error(
                crate::error::catalog::ER_DATA_OUT_OF_RANGE,
                "integer overflow in UNION aggregate merge",
            )
        }),
        (Value::UInt(l), Value::UInt(r)) => l.checked_add(*r).map(Value::UInt).ok_or_else(|| {
            error(
                crate::error::catalog::ER_DATA_OUT_OF_RANGE,
                "integer overflow in UNION aggregate merge",
            )
        }),
        (Value::Double(l), Value::Double(r)) => Ok(Value::Double(l + r)),
        (Value::Float(l), Value::Float(r)) => Ok(Value::Float(l + r)),
        _ => {
            let l = crate::engine::expr::numeric(left)?;
            let r = crate::engine::expr::numeric(right)?;
            Ok(Value::Double(l + r))
        }
    }
}

pub(super) fn validate_cte_names(ctes: &[CteDef]) -> Result<Vec<String>, QueryError> {
    let mut seen = HashSet::new();
    let mut keys = Vec::with_capacity(ctes.len());
    for cte in ctes {
        let key = cte.name.to_ascii_lowercase();
        if !seen.insert(key.clone()) {
            return Err(error(
                crate::error::catalog::ER_NONUNIQ_TABLE,
                format!("not unique table/alias: '{}'", cte.name),
            ));
        }
        keys.push(key);
    }
    Ok(keys)
}

fn collect_cte_dependencies(ctes: &[CteDef], cte_keys: &[String]) -> Vec<HashSet<String>> {
    ctes.iter()
        .map(|cte| {
            cte_keys
                .iter()
                .zip(ctes.iter())
                .filter_map(|(key, candidate)| {
                    let inherited_self = !cte.is_recursive
                        && candidate.name.eq_ignore_ascii_case(&cte.name)
                        && has_cte(&cte.name);
                    (!inherited_self && statement_references_cte(&cte.query, &candidate.name))
                        .then(|| key.clone())
                })
                .collect()
        })
        .collect()
}

/// Materialise a single CTE with `visible` as the set of already-materialised
/// CTEs the body may reference. Used by `materialize_ctes` and by EXPLAIN
/// ANALYZE WITH-CTE, where per-CTE timing is collected by walking the list
/// here instead of inside `materialize_ctes`.
pub(super) fn materialize_single_cte(
    db: &Arc<Database>,
    session: &mut SessionContext,
    cte: &CteDef,
    visible: HashMap<String, Arc<CteResult>>,
    registry: &SessionRegistry,
) -> Result<CteResult, QueryError> {
    let depth_limit = session.cte_max_recursion_depth;
    with_cte_registry_extended(visible, || {
        let self_ref = statement_references_cte(&cte.query, &cte.name)
            && (cte.is_recursive || !has_cte(&cte.name));
        if self_ref && !cte.is_recursive {
            return Err(error(
                crate::error::catalog::ER_NOT_SUPPORTED_YET,
                format!("recursive CTE '{}' requires WITH RECURSIVE", cte.name),
            ));
        }
        if self_ref {
            materialize_recursive(db, session, cte, registry, depth_limit, None, None)
        } else {
            materialize_non_recursive(db, session, cte, registry)
        }
    })
}

pub(crate) mod streaming_aggregate;

fn materialize_non_recursive(
    db: &Arc<Database>,
    session: &mut SessionContext,
    cte: &CteDef,
    registry: &SessionRegistry,
) -> Result<CteResult, QueryError> {
    fn batch_graph_is_planner_visible<'q>(
        statement: impl Into<crate::parser::QueryRef<'q>>,
    ) -> bool {
        let statement = statement.into();
        match statement {
            // Derived tables and WindowAgg are ordinary physical nodes now;
            // neither is a reason to divert the whole CTE through the legacy
            // response materializer. Capability fallback, when needed, stays
            // inside the affected BatchOperator after this graph is lowered.
            crate::parser::QueryRef::Select(_) => true,
            crate::parser::QueryRef::SetOperation(set) => {
                batch_graph_is_planner_visible(&set.left)
                    && batch_graph_is_planner_visible(&set.right)
            }
            _ => false,
        }
    }

    // Lower the whole CTE query expression to the common BatchOperator graph
    // before opening any branch. Materialization is the intentional pipeline
    // boundary: native batches feed the bounded CTE spill builder through the
    // streaming cursor, and the query-level ExecutionContext carries the same
    // snapshot, parameters, CTE registry, cancellation and work-area budget.
    let mut physical_statement = cte.query.clone();
    // The batch-first materializer plans CTE bodies directly instead of
    // re-entering `execute_select`, so it must run the query-block constant
    // fold explicitly.  In particular, retaining a typed
    // DATE_ADD(CAST(...), INTERVAL ...) predicate here makes every storage
    // worker recompute the identical bound for each scanned row.  Predicate
    // result metadata is not observable, and `fold_select_constants` already
    // preserves metadata-bearing projection expressions.
    fold_query_block_constants(&mut physical_statement);
    // FROM/JOIN derived relations are pipeline boundaries just like an
    // explicitly materialised CTE.  Lower them before planning the owner so
    // `plan_query_physical` sees real CteScan leaves instead of the parser's
    // synthetic `derived` table name.  This is deliberately recursive and
    // accepts a complete query expression (including UNION/INTERSECT/EXCEPT),
    // unlike the old display-only rewriter which retained only the leftmost
    // SELECT and consequently had to re-run the whole owner through legacy
    // dispatch.
    //
    // Predicate pushdown must run before the derived source is detached and
    // materialised.  In particular, a shared CTE domain such as
    // `a.month = 2 AND b.month = 3` is first attached to the CTE's outer
    // SELECT; for a grouped derived source that predicate is useful only
    // after it moves onto the derived GROUP BY key.  Formatted EXPLAIN has
    // always performed this pass before stubbing derived sources.  Keeping
    // the normal materialiser in the same order prevents execution from
    // scanning and aggregating the full derived input shown as filtered in
    // the plan.
    // Catalog-backed derived pushdown resolves the filtered dimension's
    // schema. Qualify against the worker session first: `Database::table`
    // deliberately defaults an unqualified name to the `test` database, so a
    // CTE materialised while the session is using another database would
    // otherwise miss the rule and spool the complete UNION input.
    super::qualification::qualify_statement_tables(db, session, &mut physical_statement)?;
    pushdown_inline_derived_predicates(db, &mut physical_statement);
    if let Some(result) =
        streaming_aggregate::try_materialize(db, session, &physical_statement, cte)?
    {
        return Ok(result);
    }
    materialize_inline_derived_sources(db, session, &mut physical_statement, registry)?;
    super::qualification::qualify_statement_tables(db, session, &mut physical_statement)?;

    // A plain UNION ALL has two valid physical representations here: one
    // set-operation graph, or one independently planned graph per branch.
    // Prefer the latter while the branch-parallel gate is enabled.  Trying the
    // whole set-operation first makes the fast path below unreachable whenever
    // `plan_query_physical` can lower UNION ALL (the common case): its chained
    // cursor drains every branch serially even though the branches use the same
    // read snapshot and have no ordering dependency.  EXPLAIN ANALYZE already
    // plans the branches independently in `materialize_cte_from_union_branches`;
    // keeping normal execution on the same scheduling shape also makes its
    // operator timings representative.
    //
    // Run this after qualification/derived-source preparation so both choices
    // see the same rewritten query.  The helper retains all semantic guards
    // (plain SELECT branches, no set-level qualifiers/windows/self-reference)
    // and returns None for every unsupported shape.
    let prepared_cte = CteDef {
        name: cte.name.clone(),
        columns: cte.columns.clone(),
        is_recursive: cte.is_recursive,
        query: physical_statement.clone(),
    };
    if let Some(result) =
        try_materialize_non_recursive_union_branches_parallel(db, session, &prepared_cte, registry)?
    {
        return Ok(result);
    }

    if batch_graph_is_planner_visible(&physical_statement) {
        if let Some(plan) = crate::planner::plan_query_physical(db, &physical_statement)? {
            let plan = crate::planner::materialize_plan(plan, cte.name.clone());
            let txn = cte_statement_view(db, session);
            let budget =
                crate::engine::executor::mem_budget::current_budget().unwrap_or_else(|| {
                    Arc::new(crate::engine::executor::mem_budget::MemBudget::from_env())
                });
            let stream = with_cte_materialization(|| {
                let context =
                    super::select::execution_context::ExecutionContext::capture_with_budget(
                        txn, budget,
                    );
                super::select::vec_bridge::try_stream_native_plan(
                    Arc::clone(db),
                    txn,
                    Arc::new(plan),
                    context,
                )
            })?;
            if let Some(stream) = stream {
                return route_response_to_cte_result(
                    db,
                    QueryResponse::StreamingResultSet(stream),
                    cte,
                );
            }
        }
    }

    let mut stmt = cte.query.clone().into_statement();
    // P1-1: CTE body may run on the streaming path; materialise so we
    // can register the full row set into the CTE registry.
    let response = with_cte_materialization(|| {
        super::dispatch::execute_for_cte(db, session, &mut stmt, registry)
    })?;
    route_response_to_cte_result(db, response, cte)
}

/// Apply the ordinary SELECT constant-folding pass to every branch that the
/// CTE batch planner can lower directly.  Set-operation branches are separate
/// query blocks; nested WITH bodies are handled when their own materializer is
/// entered.
fn fold_query_block_constants<'q>(statement: impl Into<crate::parser::QueryMut<'q>>) {
    let statement = statement.into();
    match statement {
        crate::parser::QueryMut::Select(select) => super::select::fold_select_constants(select),
        crate::parser::QueryMut::SetOperation(set) => {
            fold_query_block_constants(&mut set.left);
            fold_query_block_constants(&mut set.right);
        }
        _ => {}
    }
}

/// Push safe predicates through inline-derived boundaries before those
/// boundaries are replaced by synthetic CTE spools. Set-operation branches
/// are independent query blocks, so apply the same rule recursively.
pub(super) fn pushdown_inline_derived_predicates<'q>(
    db: &Database,
    statement: impl Into<crate::parser::QueryMut<'q>>,
) {
    let statement = statement.into();
    match statement {
        crate::parser::QueryMut::Select(select) => {
            crate::planner_decorrelate::derived::pushdown_derived_predicates_with_db(db, select);
        }
        crate::parser::QueryMut::SetOperation(set) => {
            pushdown_inline_derived_predicates(db, &mut set.left);
            pushdown_inline_derived_predicates(db, &mut set.right);
        }
        _ => {}
    }
}

/// Replace every inline-derived FROM/JOIN source in `statement` with a
/// query-scoped CTE spool that is populated through the same batch-first CTE
/// materialiser.  Innermost sources are completed first, so a parent plan is
/// fully lowerable before it opens any storage source.
fn materialize_inline_derived_sources<'q>(
    db: &Arc<Database>,
    session: &mut SessionContext,
    statement: impl Into<crate::parser::QueryMut<'q>>,
    registry: &SessionRegistry,
) -> Result<(), QueryError> {
    let statement = statement.into();
    fn materialize_source(
        db: &Arc<Database>,
        session: &mut SessionContext,
        name: String,
        subquery: crate::parser::SubQuery,
        columns: Vec<String>,
        registry: &SessionRegistry,
    ) -> Result<(), QueryError> {
        let synthetic = CteDef {
            name: name.clone(),
            columns,
            is_recursive: false,
            query: subquery,
        };
        let result = materialize_non_recursive(db, session, &synthetic, registry)?;
        set_cte_result(&name, Arc::new(result));
        Ok(())
    }

    match statement {
        crate::parser::QueryMut::Select(select) => {
            // The batch-first CTE lowerer detaches derived inputs before the
            // window dispatcher sees them. Carry the same local ranking bound
            // used by execute_derived_select through this materialization
            // boundary, including the aggregate producer's existing Top-K.
            let _rollup_topk_guard = select
                .derived
                .as_ref()
                .and_then(|derived| {
                    super::dispatch::rollup_window_topk_hint_for_window_derived(select, derived)
                })
                .map(|hint| super::select::plan::set_rollup_window_topk_hint(Some(hint)));
            if let Some(subquery) = select.derived.take() {
                let alias = select
                    .from
                    .first()
                    .map(|table| table.alias.clone().unwrap_or_else(|| table.name.clone()))
                    .unwrap_or_else(|| "derived".to_string());
                let rank_source = matches!(&subquery, crate::parser::SubQuery::Select(inner)
                    if inner.limit.is_none() && inner.offset.is_none()
                        && super::dispatch::select_has_window_functions(inner));
                let _rank_guard =
                    (rank_source && select.joins.is_empty() && select.from.len() == 1)
                        .then(|| {
                            select
                                .selection
                                .as_ref()
                                .and_then(super::select::extract_window_topk_filter)
                        })
                        .flatten()
                        .map(|hint| super::dispatch::set_window_topk_hint(Some(hint)));
                let name = next_inline_derived_name();
                let columns = select
                    .from
                    .first()
                    .map(|table| table.column_aliases.clone())
                    .unwrap_or_default();
                materialize_source(db, session, name.clone(), subquery, columns, registry)?;
                if let Some(table) = select.from.first_mut() {
                    table.name = name;
                    table.alias = Some(alias);
                }
            }
            for join in &mut select.joins {
                let Some(subquery) = join.derived.take() else {
                    continue;
                };
                let alias = join
                    .table
                    .alias
                    .clone()
                    .unwrap_or_else(|| join.table.name.clone());
                let name = next_inline_derived_name();
                materialize_source(
                    db,
                    session,
                    name.clone(),
                    subquery,
                    join.table.column_aliases.clone(),
                    registry,
                )?;
                join.table.name = name;
                join.table.alias = Some(alias);
            }
            Ok(())
        }
        crate::parser::QueryMut::SetOperation(set) => {
            materialize_inline_derived_sources(db, session, &mut set.left, registry)?;
            materialize_inline_derived_sources(db, session, &mut set.right, registry)
        }
        _ => Ok(()),
    }
}

/// Runtime UNION ALL CTE branch parallelism.
///
/// Returns `Ok(Some(result))` when the body matches the parallel-eligible
/// shape (non-recursive UNION ALL of ≥2 plain-SELECT branches, no window
/// in any branch, no self-reference, gate on, EXPLAIN ANALYZE not active).
/// Returns `Ok(None)` to mean "fall back to the serial chained path."
///
/// Snapshot consistency: each worker runs against the same `TxnView`
/// derived from the session's current txn — under RR the frozen snapshot
/// SCN propagates through `session_read_only_snapshot`, so every branch
/// reads the same MVCC view as the serial path would have for the first
/// branch. Under RC, the serial path would re-pick `visible_scn()` per
/// branch; we tighten that to a single snapshot, which is strictly more
/// consistent (and matches what the EXPLAIN ANALYZE parallel path does).
fn try_materialize_non_recursive_union_branches_parallel(
    db: &Arc<Database>,
    session: &SessionContext,
    cte: &CteDef,
    registry: &SessionRegistry,
) -> Result<Option<CteResult>, QueryError> {
    if !cte_union_branch_parallel_enabled() {
        return Ok(None);
    }
    if cte.is_recursive {
        return Ok(None);
    }
    if statement_references_cte(&cte.query, &cte.name) {
        return Ok(None);
    }
    let Some(branches) = flatten_union_all_branches(&cte.query) else {
        return Ok(None);
    };
    if branches.len() < 2 {
        return Ok(None);
    }
    // A set-op-level `ORDER BY` / `LIMIT` / `OFFSET` (e.g. a parenthesised
    // `(... UNION ALL ...) ORDER BY a LIMIT 5` CTE body) applies *after* the
    // branches have been concatenated. `flatten_union_all_branches` strips
    // those qualifiers, so executing per-branch in parallel and concatenating
    // would silently drop the LIMIT. The serial `chained_union_all` dispatch
    // path guards on the same condition (only the unmodified UNION ALL takes
    // the streaming fast path; modified ones go through
    // `combine_set_operation_results` which applies ORDER/LIMIT/OFFSET).
    if union_all_has_setop_level_qualifiers(&cte.query) {
        return Ok(None);
    }
    // A windowed branch must route through dispatch's `execute_windowed_select`
    // (the `WindowAgg` operator is injected at dispatch time, not by
    // `plan_select` / `execute_select` directly). The session-free
    // `execute_select` here would mis-handle it.
    if branches
        .iter()
        .any(super::dispatch::select_has_window_functions)
    {
        return Ok(None);
    }
    // CTE-body table qualification happens *inside* `execute_for_cte` (the
    // serial path) via `execute_statement → qualify_statement_tables`. Our
    // session-free worker bypasses that, so unqualified leaf-branch names
    // (e.g. `from customer`) would surface as 1146 `table 'customer' does
    // not exist`. Clone + qualify here against the session's current
    // database, then re-flatten so workers see fully-qualified branches.
    let mut qualified_body = cte.query.clone();
    super::qualification::qualify_statement_tables(db, session, &mut qualified_body)?;
    let Some(branches) = flatten_union_all_branches(&qualified_body) else {
        return Ok(None);
    };
    let txn = cte_statement_view(db, session);
    let visible = current_cte_registry();
    let cte_name = cte.name.clone();
    // Bounded worker pool with a shared pull-based queue. Source-order
    // preserved via `slots[idx]`. The first worker error short-circuits the
    // queue so peers stop pulling instead of doing wasted work.
    let total_branches = branches.len();
    let worker_count = cte_union_branch_concurrency().min(total_branches).max(1);
    let queue: std::sync::Mutex<std::collections::VecDeque<(usize, crate::parser::Select)>> =
        std::sync::Mutex::new(
            branches
                .into_iter()
                .enumerate()
                .collect::<std::collections::VecDeque<_>>(),
        );
    let slots: std::sync::Mutex<Vec<Option<CteResult>>> =
        std::sync::Mutex::new((0..total_branches).map(|_| None).collect());
    let early_abort = std::sync::atomic::AtomicBool::new(false);
    let context = super::select::execution_context::ExecutionContext::capture(txn);
    std::thread::scope(|scope| -> Result<(), QueryError> {
        let mut handles = Vec::with_capacity(worker_count);
        for _ in 0..worker_count {
            let db_cloned = Arc::clone(db);
            let visible_outer = visible.clone();
            let cte_name_cloned = cte_name.clone();
            let queue_ref = &queue;
            let slots_ref = &slots;
            let abort_ref = &early_abort;
            let context = context.clone();
            handles.push(scope.spawn(move || -> Result<(), QueryError> {
                context.run(&db_cloned, || loop {
                    crate::engine::cancel::check_cancelled()?;
                    if abort_ref.load(std::sync::atomic::Ordering::Acquire) {
                        return Ok(());
                    }
                    let next = {
                        let mut q = queue_ref.lock().expect("queue lock");
                        q.pop_front()
                    };
                    let Some((idx, select)) = next else {
                        return Ok(());
                    };
                    let result = with_cte_registry_extended(visible_outer.clone(), || {
                        with_cte_materialization(|| {
                            let branch = CteDef {
                                name: cte_name_cloned.clone(),
                                columns: Vec::new(),
                                is_recursive: false,
                                query: crate::parser::QueryExpr::from(select.clone()),
                            };
                            let response =
                                super::select::execute_select(&db_cloned, txn, select, registry)?;
                            route_response_to_cte_result(&db_cloned, response, &branch)
                        })
                    });
                    match result {
                        Ok(rs) => {
                            slots_ref.lock().expect("slots lock")[idx] = Some(rs);
                        }
                        Err(e) => {
                            abort_ref.store(true, std::sync::atomic::Ordering::Release);
                            return Err(e);
                        }
                    }
                })
            }));
        }
        let mut first_err: Option<QueryError> = None;
        for handle in handles {
            match handle.join() {
                Ok(Ok(())) => {}
                Ok(Err(e)) => {
                    if first_err.is_none() {
                        first_err = Some(e);
                    }
                }
                Err(_) => {
                    if first_err.is_none() {
                        first_err = Some(error(
                            crate::error::catalog::ER_UNKNOWN_ERROR,
                            format!("CTE '{}' UNION ALL worker panicked", cte_name),
                        ));
                    }
                }
            }
        }
        if let Some(e) = first_err {
            return Err(e);
        }
        Ok(())
    })?;
    let branch_sets: Vec<CteResult> = slots
        .into_inner()
        .expect("slots poisoned")
        .into_iter()
        .map(|slot| slot.expect("every branch slot is filled on the success path"))
        .collect();
    let columns = union_output_columns(branch_sets.iter().map(|branch| branch.columns.as_slice()))?;
    let mut builder = CteSpillBuilder::new_with_stats(columns, db.data_dir().to_path_buf());
    for branch in branch_sets {
        append_union_branch_spool(branch, &mut builder)?;
    }
    let mut result = builder.finish()?;
    apply_column_aliases(&mut result, &cte.columns, &cte.name)?;
    Ok(Some(result))
}

/// Drain a CTE body's `QueryResponse` into a `CteResult`, routing rows
/// through `CteSpillBuilder` (small CTEs stay in memory, large ones spill
/// to a quota-protected disk file instead of raising 1037 — the TPC-DS
/// Q2/Q5/Q14/Q23 failure mode). The streaming arm pulls one row at a time
/// so even an oversized body never fully materialises before the spill
/// threshold trips. Shared by the re-planning path
/// (`materialize_non_recursive`) and the EXPLAIN ANALYZE pre-planned path
/// (`materialize_cte_from_physical`).
fn route_response_to_cte_result(
    db: &Arc<Database>,
    response: QueryResponse,
    cte: &CteDef,
) -> Result<CteResult, QueryError> {
    let data_dir = db.data_dir().to_path_buf();
    let mut result = match response {
        QueryResponse::ResultSet(rs) => {
            let mut builder = CteSpillBuilder::new_with_stats(rs.columns, data_dir);
            for row in rs.rows {
                builder.push(row)?;
            }
            builder.finish()?
        }
        QueryResponse::StreamingResultSet(mut stream) => {
            let mut builder = CteSpillBuilder::new_with_stats(stream.columns.clone(), data_dir);
            builder.consume_cursor(stream.cursor.as_mut())?;
            builder.finish()?
        }
        other => {
            let QueryResponse::ResultSet(rs) = other.materialize()? else {
                return Err(error(
                    crate::error::catalog::ER_NOT_SUPPORTED_YET,
                    format!("CTE '{}' body did not return rows", cte.name),
                ));
            };
            let mut builder = CteSpillBuilder::new_with_stats(rs.columns, data_dir);
            for row in rs.rows {
                builder.push(row)?;
            }
            builder.finish()?
        }
    };
    apply_column_aliases(&mut result, &cte.columns, &cte.name)?;
    Ok(result)
}

/// Materialise a non-recursive CTE by executing an *already-planned*
/// physical tree — the exact tree EXPLAIN's formatter will walk — instead
/// of re-planning the body. This keeps the per-operator stats collector's
/// node-pointer keys aligned with the display tree, so EXPLAIN ANALYZE can
/// attach `actual time/rows/loops` to the clauses *inside* the CTE body.
/// The caller must have the prerequisite CTE registry installed (so any
/// nested `CteScan` resolves) and, for ANALYZE, an active stats collector.
pub(super) fn materialize_cte_from_physical(
    db: &Arc<Database>,
    txn: crate::storage::TxnView,
    cte: &CteDef,
    select: &crate::parser::Select,
    plan: &crate::planner::PhysicalPlan,
) -> Result<CteResult, QueryError> {
    // `plan_select` binds GROUP BY projection references on an owned clone,
    // while this EXPLAIN-ANALYZE path executes the already-built physical
    // tree with the parser's original Select. Keep both halves in lockstep:
    // fused aggregates may emit only `$group.group_N` markers, so evaluating
    // an unbound projection such as `i_item_sk AS item_sk` against that row
    // incorrectly raises 1054. Normal SELECT execution performs the same bind
    // in `prepare_select_for_planning` before reaching the executor.
    let mut execution_select = select.clone();
    crate::planner::bind_rollup_select_for_execution(db, &mut execution_select)?;
    // A large estimate selects bounded consumption, not a different join
    // executor. Keep native batches for supported projections, including FULL
    // joins whose CTE-key cardinality estimate can greatly exceed real rows.
    if cte_plan_should_stream(plan)
        && !super::select::vec_bridge::should_route_result_to_batch(
            plan,
            &execution_select.projection,
        )
    {
        let columns = super::select::plan::empty_result_columns(&execution_select, plan)?;
        let data_dir = db.data_dir().to_path_buf();
        let mut builder = CteSpillBuilder::new_with_stats(columns, data_dir);
        with_cte_materialization(|| {
            super::select::plan::execute_plan_projected_rows_streaming(
                db,
                txn,
                &execution_select,
                plan,
                |row| builder.push(row),
            )
        })?;
        let mut result = builder.finish()?;
        apply_column_aliases(&mut result, &cte.columns, &cte.name)?;
        return Ok(result);
    }
    let response = with_cte_materialization(|| {
        let _stream = super::select::force_streaming_response_scope();
        super::select::plan::execute_physical_select(db, txn, &execution_select, plan)
    })?;
    route_response_to_cte_result(db, response, cte)
}

/// Materialise a synthetic CTE that represents an inline-derived table the
/// EXPLAIN ANALYZE display planner stripped out of a parent SELECT. The
/// `alias` is the derived table's alias (used only in error messages); the
/// caller must have installed the prerequisite CTE registry so any
/// `CteScan` inside `plan` resolves. No column-alias rename is applied —
/// inline-derived sources keep their inner projection names by design, and
/// the consumer references them by `alias.col`.
pub(super) fn materialize_inline_derived_from_physical(
    db: &Arc<Database>,
    txn: crate::storage::TxnView,
    alias: &str,
    select: &crate::parser::Select,
    plan: &crate::planner::PhysicalPlan,
) -> Result<CteResult, QueryError> {
    let synthetic = CteDef {
        name: alias.to_string(),
        columns: Vec::new(),
        is_recursive: false,
        query: crate::parser::QueryExpr::from(select.clone()),
    };
    materialize_cte_from_physical(db, txn, &synthetic, select, plan)
}

/// EXPLAIN ANALYZE inline-derived materialisation that is window-aware.
/// Window functions are wrapped into a `WindowAgg` operator at *dispatch*
/// time (`execute_windowed_select`), not by `planner::plan_select` — so the
/// display plan the inline-derived rewriter built carries no window node, and
/// executing it via `materialize_inline_derived_from_physical` would re-apply
/// the window projection in the legacy per-row evaluator with no window
/// context (error 1235). When the derived projection contains a window
/// function, materialise it through the same window-aware path normal
/// execution uses; per-node stats inside this derived are forfeited (the
/// window machinery builds its own plan), matching the lossy-CTE tradeoff.
pub(super) fn materialize_inline_derived_for_analyze(
    db: &Arc<Database>,
    txn: crate::storage::TxnView,
    alias: &str,
    select: &crate::parser::Select,
    plan: &crate::planner::PhysicalPlan,
    registry: &SessionRegistry,
) -> Result<CteResult, QueryError> {
    if super::dispatch::select_has_window_functions(select) {
        let synthetic = CteDef {
            name: alias.to_string(),
            columns: Vec::new(),
            is_recursive: false,
            query: crate::parser::QueryExpr::from(select.clone()),
        };
        let response = with_cte_materialization(|| {
            super::dispatch::execute_windowed_select(db, txn, select.clone(), registry)
        })?;
        return route_response_to_cte_result(db, response, &synthetic);
    }
    let started = std::time::Instant::now();
    let io_start = db.buffer_stats_counts();
    let result = materialize_inline_derived_from_physical(db, txn, alias, select, plan)?;
    super::select::analyze_stats::record_if_absent_with_io_snapshot(
        db,
        plan,
        started.elapsed().as_nanos(),
        result.estimated_rows.max(result.nrows() as f64) as u64,
        io_start,
    );
    Ok(result)
}

/// True iff any `SetOperation` node in the UNION-ALL chain carries a
/// trailing `ORDER BY` / `LIMIT` / `OFFSET` clause that applies to the
/// concatenated rows. These are stripped by [`flatten_union_all_branches`]
/// (it only inspects `op` / `all`), so the parallel CTE-materialization
/// fast path must skip such bodies — running the branches independently
/// and concatenating their rows would silently drop the qualifier.
pub(super) fn union_all_has_setop_level_qualifiers<'q>(
    query: impl Into<crate::parser::QueryRef<'q>>,
) -> bool {
    let query = query.into();
    match query {
        crate::parser::QueryRef::SetOperation(set) => {
            if !set.order_by.is_empty() || set.limit.is_some() || set.offset.is_some() {
                return true;
            }
            union_all_has_setop_level_qualifiers(&set.left)
                || union_all_has_setop_level_qualifiers(&set.right)
        }
        _ => false,
    }
}

/// Left-deep flatten a chain of `UNION ALL` set operations into the
/// source-order list of branch SELECTs. Returns `None` if any node in the
/// chain isn't `UNION ALL`, or if any leaf isn't a plain SELECT — matches
/// the eligibility check that produced the extra display plans.
pub(super) fn flatten_union_all_branches<'q>(
    query: impl Into<crate::parser::QueryRef<'q>>,
) -> Option<Vec<crate::parser::Select>> {
    fn walk<'q>(
        stmt: impl Into<crate::parser::QueryRef<'q>>,
        out: &mut Vec<crate::parser::Select>,
    ) -> bool {
        let stmt = stmt.into();
        match stmt {
            crate::parser::QueryRef::Select(select) => {
                out.push(select.clone());
                true
            }
            crate::parser::QueryRef::SetOperation(set) => {
                if set.op != crate::parser::SetOp::Union || !set.all {
                    return false;
                }
                walk(&set.left, out) && walk(&set.right, out)
            }
            _ => false,
        }
    }
    let mut branches = Vec::new();
    if walk(query.into(), &mut branches) && branches.len() >= 2 {
        Some(branches)
    } else {
        None
    }
}

/// EXPLAIN ANALYZE helper: materialise one CTE so its result lands in the
/// registry for downstream CTEs / the body to read. Non-recursive plain
/// SELECT CTEs and non-recursive `UNION ALL` CTEs with display plans run
/// *those exact plans* (giving per-operator stats inside the CTE body —
/// for every UNION ALL branch); recursive or other set-operation bodies
/// fall back to the re-planning path (`materialize_single_cte`) — correct
/// results, but their internal nodes show `(never executed)`.
#[allow(clippy::too_many_arguments)]
pub(super) fn analyze_materialize_cte(
    db: &Arc<Database>,
    session: &mut SessionContext,
    cte: &CteDef,
    cte_plans: &HashMap<String, crate::planner::PhysicalPlan>,
    cte_extras: Option<&[crate::planner::PhysicalPlan]>,
    key: &str,
    txn: crate::storage::TxnView,
    visible: HashMap<String, Arc<CteResult>>,
    registry: &SessionRegistry,
) -> Result<CteResult, QueryError> {
    // UNION ALL branch path: every branch's display plan was pre-planned
    // (LEFT in `cte_plans`, branches 1.. in `cte_extras`). Drive each
    // through `execute_physical_select` under the active stats collector
    // and concatenate row sets, so per-operator stats line up by node
    // pointer for ALL sides of the union (not just LEFT).
    // A CTE body whose projection holds a window function cannot run via its
    // display plan: the `WindowAgg` operator is injected at dispatch time, not
    // by `planner::plan_select`, so the display plan omits it and re-applying
    // the projection's window expression in the legacy evaluator raises 1235.
    // Route such bodies through the re-planning fallback (`materialize_single_cte`
    // → dispatch), which handles windows correctly (internal node stats are
    // then forfeited, as with recursive / set-op bodies).
    let body_has_window = match &cte.query {
        crate::parser::QueryExpr::Select(select) => {
            super::dispatch::select_has_window_functions(select)
        }
        _ => false,
    };
    let extras_len = cte_extras.map(|e| e.len()).unwrap_or(0);
    if !body_has_window
        && extras_len > 0
        && !cte.is_recursive
        && !statement_references_cte(&cte.query, &cte.name)
        && cte_plans.contains_key(key)
    {
        if let Some(branches) = flatten_union_all_branches(&cte.query) {
            if branches.len() == extras_len + 1 {
                let head_plan = cte_plans.get(key).expect("checked above");
                let extras = cte_extras.expect("len > 0");
                return with_cte_registry_extended(visible, || {
                    materialize_cte_from_union_branches(db, txn, cte, &branches, head_plan, extras)
                });
            }
        }
    }
    if cte.is_recursive {
        if let Some(plan) = cte_plans.get(key) {
            let started = std::time::Instant::now();
            let io_start = db.buffer_stats_counts();
            let depth_limit = session.cte_max_recursion_depth;
            let result = with_cte_registry_extended(visible, || {
                materialize_recursive(db, session, cte, registry, depth_limit, Some(plan), None)
            })?;
            super::select::analyze_stats::record_if_absent_with_io_snapshot(
                db,
                plan,
                started.elapsed().as_nanos(),
                result.estimated_rows.max(result.nrows() as f64) as u64,
                io_start,
            );
            return Ok(result);
        }
    }
    let eligible = !cte.is_recursive
        && !body_has_window
        && !statement_references_cte(&cte.query, &cte.name)
        && matches!(&cte.query, crate::parser::QueryExpr::Select(_))
        && cte_plans.contains_key(key);
    if eligible {
        if let (crate::parser::QueryExpr::Select(select), Some(plan)) =
            ((&cte.query), cte_plans.get(key))
        {
            let started = std::time::Instant::now();
            let io_start = db.buffer_stats_counts();
            let result = with_cte_registry_extended(visible, || {
                materialize_cte_from_physical(db, txn, cte, select, plan)
            })?;
            super::select::analyze_stats::record_if_absent_with_io_snapshot(
                db,
                plan,
                started.elapsed().as_nanos(),
                result.estimated_rows.max(result.nrows() as f64) as u64,
                io_start,
            );
            return Ok(result);
        }
    }
    materialize_single_cte(db, session, cte, visible, registry)
}

/// Execute every branch of a `UNION ALL` CTE body via its display plan and
/// concatenate the row sets through the spill-aware builder. The caller
/// must have installed the prerequisite CTE registry and (for stats) an
/// active analyze collector. `branches` is parallel to `[head_plan] ++
/// extras` (same length, branches[i] drives plans[i]).
fn materialize_cte_from_union_branches(
    db: &Arc<Database>,
    txn: crate::storage::TxnView,
    cte: &CteDef,
    branches: &[crate::parser::Select],
    head_plan: &crate::planner::PhysicalPlan,
    extras: &[crate::planner::PhysicalPlan],
) -> Result<CteResult, QueryError> {
    let plans: Vec<&crate::planner::PhysicalPlan> =
        std::iter::once(head_plan).chain(extras.iter()).collect();
    let descriptors = branches
        .iter()
        .zip(&plans)
        .map(|(select, plan)| {
            let mut bound = select.clone();
            crate::planner::bind_rollup_select_for_execution(db, &mut bound)?;
            super::select::plan::empty_result_columns(&bound, plan)
        })
        .collect::<Result<Vec<_>, QueryError>>()?;
    let columns = union_output_columns(descriptors.iter().map(Vec::as_slice))?;
    let mut builder = CteSpillBuilder::new_with_stats(columns, db.data_dir().to_path_buf());
    if cte_union_branch_parallel_enabled() && branches.len() > 1 {
        let visible = current_cte_registry();
        let context = super::select::execution_context::ExecutionContext::capture(txn);
        let branch_sets = std::thread::scope(|scope| {
            let mut handles = Vec::with_capacity(branches.len());
            for (select, plan) in branches.iter().zip(plans.iter()) {
                let db = Arc::clone(db);
                let visible = visible.clone();
                let context = context.clone();
                handles.push(scope.spawn(move || {
                    context.run(&db, || {
                        with_cte_registry_extended(visible, || {
                            with_cte_materialization(|| {
                                materialize_union_planned_branch(&db, txn, select, plan)
                            })
                        })
                    })
                }));
            }
            let mut branch_sets = Vec::with_capacity(handles.len());
            for handle in handles {
                match handle.join() {
                    Ok(result) => branch_sets.push(result?),
                    Err(_) => {
                        return Err(error(
                            crate::error::catalog::ER_UNKNOWN_ERROR,
                            format!("CTE '{}' UNION ALL branch execution panicked", cte.name),
                        ));
                    }
                }
            }
            Ok(branch_sets)
        })?;
        for branch in branch_sets {
            append_union_branch_spool(branch, &mut builder)?;
        }
    } else {
        for (select, plan) in branches.iter().zip(plans.iter()) {
            let branch = with_cte_materialization(|| {
                materialize_union_planned_branch(db, txn, select, plan)
            })?;
            append_union_branch_spool(branch, &mut builder)?;
        }
    }
    let mut result = builder.finish()?;
    apply_column_aliases(&mut result, &cte.columns, &cte.name)?;
    Ok(result)
}

/// Consume each UNION branch before publishing its result to the coordinator.
/// A branch result is a governed CTE store or a spill file, never an eagerly
/// collected ResultSet waiting outside the spill builder.
fn materialize_union_planned_branch(
    db: &Arc<Database>,
    txn: crate::storage::TxnView,
    select: &crate::parser::Select,
    plan: &crate::planner::PhysicalPlan,
) -> Result<CteResult, QueryError> {
    let response = super::select::plan::execute_physical_select(db, txn, select, plan)?;
    let branch = CteDef {
        name: "UNION ALL branch".into(),
        columns: Vec::new(),
        is_recursive: false,
        query: crate::parser::QueryExpr::from(select.clone()),
    };
    route_response_to_cte_result(db, response, &branch)
}

fn union_output_columns<'a>(
    mut branches: impl Iterator<Item = &'a [Column]>,
) -> Result<Vec<Column>, QueryError> {
    let mut output = branches
        .next()
        .ok_or_else(|| {
            error(
                crate::error::catalog::ER_NOT_SUPPORTED_YET,
                "UNION ALL has no branches",
            )
        })?
        .to_vec();
    for branch in branches {
        if branch.len() != output.len() {
            return Err(error(
                crate::error::catalog::ER_WRONG_NUMBER_OF_COLUMNS_IN_SELECT,
                "UNION branches have different column counts",
            ));
        }
        let mut branch = branch.to_vec();
        super::dispatch::unify_setop_declared_column_metadata(&mut output, &mut branch)?;
    }
    Ok(output)
}

fn union_column_needs_coercion(source: &Column, target: &Column) -> bool {
    let integer = |kind| {
        matches!(
            kind,
            ColumnType::Tiny
                | ColumnType::Short
                | ColumnType::MediumInt
                | ColumnType::Long
                | ColumnType::LongLong
                | ColumnType::Bit { .. }
        )
    };
    if integer(source.column_type) && integer(target.column_type) {
        return false;
    }
    if source.column_type == ColumnType::Vector && target.column_type == ColumnType::Vector {
        return source.source.vector_type != target.source.vector_type;
    }
    if source.column_type == target.column_type
        && matches!(
            target.column_type,
            ColumnType::Json | ColumnType::Geometry | ColumnType::Vector | ColumnType::Year
        )
    {
        return false;
    }
    true
}

fn normalize_union_row(
    mut row: Vec<Value>,
    columns: &[Column],
    conversions: &[usize],
) -> Result<Vec<Value>, QueryError> {
    if row.len() != columns.len() {
        return Err(error(
            crate::error::catalog::ER_WRONG_NUMBER_OF_COLUMNS_IN_SELECT,
            "UNION row has an invalid column count",
        ));
    }
    for &index in conversions {
        let value = &mut row[index];
        let column = &columns[index];
        // Integer widening preserves the payload; character lanes also avoid
        // copying already-normalized strings on the common CTE path.
        if matches!(column.column_type, ColumnType::VarString | ColumnType::Blob) {
            let binary = column.source.collation_id == Some(63);
            if (binary && matches!(value, Value::Bytes(_)))
                || (!binary && matches!(value, Value::String(_) | Value::SmallString(_)))
            {
                continue;
            }
        }
        let original = std::mem::replace(value, Value::Null);
        *value = super::dispatch::coerce_setop_value(original, column.column_type, &column.source)?;
    }
    Ok(row)
}

fn append_union_branch_spool(
    branch: CteResult,
    builder: &mut CteSpillBuilder,
) -> Result<(), QueryError> {
    if branch.columns.len() != builder.columns.len() {
        return Err(error(
            crate::error::catalog::ER_WRONG_NUMBER_OF_COLUMNS_IN_SELECT,
            "UNION branches have different column counts",
        ));
    }
    let conversions: Vec<usize> = branch
        .columns
        .iter()
        .zip(&builder.columns)
        .enumerate()
        .filter_map(|(index, (source, target))| {
            union_column_needs_coercion(source, target).then_some(index)
        })
        .collect();
    if let Some(spill) = &branch.spill {
        if let Some(mut reader) =
            super::select::grace::ValueRowReader::open(&spill.path, spill._dir.charger())?
        {
            while let Some(row) = reader.next()? {
                crate::engine::cancel::check_cancelled()?;
                let row = if conversions.is_empty() {
                    row
                } else {
                    normalize_union_row(row, &builder.columns, &conversions)?
                };
                builder.push(row)?;
            }
        }
    } else {
        for row in branch.store.dense_rows() {
            crate::engine::cancel::check_cancelled()?;
            let row = if conversions.is_empty() {
                row.clone()
            } else {
                normalize_union_row(row.clone(), &builder.columns, &conversions)?
            };
            builder.push(row)?;
        }
    }
    Ok(())
}

fn visit_cte_result_rows(
    result: &CteResult,
    mut visit: impl FnMut(Vec<Value>) -> Result<(), QueryError>,
) -> Result<(), QueryError> {
    if let Some(spill) = result.spill.as_ref() {
        if let Some(mut reader) = crate::engine::executor::select::grace::ValueRowReader::open(
            &spill.path,
            spill._dir.charger(),
        )? {
            while let Some(row) = reader.next()? {
                crate::engine::cancel::check_cancelled()?;
                visit(row)?;
            }
        }
    } else {
        for row in result.store.dense_rows() {
            crate::engine::cancel::check_cancelled()?;
            visit(row.clone())?;
        }
    }
    Ok(())
}

/// Pull cursor over either representation of a materialized CTE. It owns the
/// result handle, so a spill file and its quota reservation remain alive until
/// the last consumer finishes. In-memory rows are cloned one at a time; a
/// spilled result retains only the decoder buffer for its current row.
struct CteResultRowCursor {
    result: Arc<CteResult>,
    index: usize,
    spill: Option<crate::engine::executor::select::grace::ValueRowReader>,
}

impl CteResultRowCursor {
    fn open(result: Arc<CteResult>) -> Result<Self, QueryError> {
        let spill = match result.spill.as_ref() {
            Some(file) => crate::engine::executor::select::grace::ValueRowReader::open(
                &file.path,
                file._dir.charger(),
            )?,
            None => None,
        };
        Ok(Self {
            result,
            index: 0,
            spill,
        })
    }

    fn next_row(&mut self) -> Result<Option<Vec<Value>>, QueryError> {
        crate::engine::cancel::check_cancelled()?;
        if self.result.spill.is_some() {
            return match self.spill.as_mut() {
                Some(reader) => reader.next(),
                None => Ok(None),
            };
        }
        let row = self.result.store.dense_rows().get(self.index).cloned();
        self.index = self.index.saturating_add(usize::from(row.is_some()));
        Ok(row)
    }
}

/// Concatenate a recursion's prior delta spools without materializing them.
/// `UNION DISTINCT` uses this as the right side of a spillable EXCEPT to find
/// the genuinely-new delta for the next iteration.
struct CteHistoryRowCursor {
    history: std::vec::IntoIter<Arc<CteResult>>,
    current: Option<CteResultRowCursor>,
}

impl CteHistoryRowCursor {
    fn new(history: Vec<Arc<CteResult>>) -> Self {
        Self {
            history: history.into_iter(),
            current: None,
        }
    }

    fn next_row(&mut self) -> Result<Option<Vec<Value>>, QueryError> {
        loop {
            if let Some(current) = self.current.as_mut() {
                if let Some(row) = current.next_row()? {
                    return Ok(Some(row));
                }
            }
            let Some(result) = self.history.next() else {
                self.current = None;
                return Ok(None);
            };
            self.current = Some(CteResultRowCursor::open(result)?);
        }
    }
}

/// Split the left-associative UNION chain without executing or binding its
/// recursive side. Metadata comes exclusively from the nonrecursive prefix.
pub(super) fn recursive_parts(
    cte: &CteDef,
) -> Result<(crate::parser::QueryExpr, Vec<Select>, bool), QueryError> {
    fn split(
        stmt: &crate::parser::QueryExpr,
        name: &str,
        all: bool,
        parts: &mut Vec<Select>,
    ) -> Result<crate::parser::QueryExpr, QueryError> {
        let crate::parser::QueryExpr::SetOp(set) = stmt else {
            return Err(error(
                crate::error::catalog::ER_CTE_RECURSIVE_REQUIRES_UNION,
                format!("Recursive CTE '{name}' requires an anchor followed by UNION"),
            ));
        };
        if !statement_references_cte(&set.right, name) {
            return Err(error(
                crate::error::catalog::ER_CTE_RECURSIVE_REQUIRES_NONRECURSIVE_FIRST,
                format!("Recursive CTE '{name}' must have all nonrecursive branches first"),
            ));
        }
        if set.op != SetOp::Union
            || (all && !set.all)
            || !set.order_by.is_empty()
            || set.limit.is_some()
            || set.offset.is_some()
        {
            return Err(error(crate::error::catalog::ER_NOT_SUPPORTED_YET,
                format!("Recursive CTE '{name}' requires UNION branches without global ORDER BY/LIMIT; DISTINCT followed by ALL is not supported")));
        }
        let anchor = if statement_references_cte(&set.left, name) {
            if !matches!(&set.left, crate::parser::QueryExpr::SetOp(_)) {
                return Err(error(
                    crate::error::catalog::ER_CTE_RECURSIVE_REQUIRES_NONRECURSIVE_FIRST,
                    format!("Recursive CTE '{name}' must have all nonrecursive branches first"),
                ));
            }
            split(&set.left, name, all, parts)?
        } else {
            set.left.clone()
        };
        fn append<'q>(
            stmt: impl Into<crate::parser::QueryRef<'q>>,
            name: &str,
            all: bool,
            parts: &mut Vec<Select>,
        ) -> Result<(), QueryError> {
            let stmt = stmt.into();
            match stmt {
                crate::parser::QueryRef::Select(select) if statement_references_cte(stmt, name) => {
                    validate_recursive_part(select, name)?;
                    parts.push(select.clone());
                    Ok(())
                }
                crate::parser::QueryRef::SetOperation(set)
                    if set.op == SetOp::Union
                        && (!all || set.all)
                        && set.order_by.is_empty()
                        && set.limit.is_none()
                        && set.offset.is_none() =>
                {
                    append(&set.left, name, all, parts)?;
                    append(&set.right, name, all, parts)
                }
                _ => Err(error(
                    crate::error::catalog::ER_CTE_RECURSIVE_REQUIRES_NONRECURSIVE_FIRST,
                    format!("Recursive CTE '{name}' must have all nonrecursive branches first"),
                )),
            }
        }
        append(&set.right, name, all, parts)?;
        Ok(anchor)
    }
    let all = matches!( &cte.query, crate::parser::QueryExpr::SetOp(set) if set.all);
    let mut parts = Vec::new();
    let anchor = split(&cte.query, &cte.name, all, &mut parts)?;
    Ok((anchor, parts, all))
}

/// Describe in dependency order, retaining outer lexical scope and publishing
/// each recursive anchor before binding its recursive branches.
pub(crate) fn describe_definitions(
    ctes: &[CteDef],
    mut describe: impl FnMut(&crate::parser::QueryExpr) -> Result<Vec<crate::types::Column>, QueryError>,
) -> Result<HashMap<String, Arc<CteResult>>, QueryError> {
    let keys = validate_cte_names(ctes)?;
    let deps = collect_cte_dependencies(ctes, &keys);
    let mut stubs = HashMap::new();
    let mut pending: Vec<usize> = (0..ctes.len()).collect();
    while !pending.is_empty() {
        let Some(pos) = pending.iter().position(|&idx| {
            deps[idx]
                .iter()
                .all(|dep| dep == &keys[idx] || stubs.contains_key(dep))
        }) else {
            return Err(error(
                crate::error::catalog::ER_NOT_SUPPORTED_YET,
                "mutually recursive CTEs are not supported",
            ));
        };
        let idx = pending.remove(pos);
        let cte = &ctes[idx];
        let recursive = deps[idx].contains(&keys[idx]);
        if recursive && !cte.is_recursive {
            return Err(error(
                crate::error::catalog::ER_NOT_SUPPORTED_YET,
                format!("recursive CTE '{}' requires WITH RECURSIVE", cte.name),
            ));
        }
        let parts = if recursive {
            Some(recursive_parts(cte)?)
        } else {
            None
        };
        let query = parts
            .as_ref()
            .map(|(anchor, _, _)| anchor)
            .unwrap_or(&cte.query);
        let columns = with_cte_registry_extended(stubs.clone(), || describe(query))?;
        let mut result = CteResult::materialized(columns, Vec::new());
        apply_column_aliases(&mut result, &cte.columns, &cte.name)?;
        if recursive {
            for column in &mut result.columns {
                column.source.flags &= !crate::mysql::protocol::NOT_NULL_FLAG;
            }
        }
        let width = result.columns.len();
        stubs.insert(keys[idx].clone(), Arc::new(result));
        if let Some((_, branches, _)) = parts {
            for branch in branches {
                let columns = with_cte_registry_extended(stubs.clone(), || {
                    describe(&crate::parser::QueryExpr::from(branch))
                })?;
                if columns.len() != width {
                    return Err(error(
                        crate::error::catalog::ER_WRONG_NUMBER_OF_COLUMNS_IN_SELECT,
                        "The used SELECT statements have a different number of columns",
                    ));
                }
            }
        }
    }
    Ok(stubs)
}

/// The derived/subquery executor already owns a read view. Reuse it for all
/// branches rather than opening a transaction inside the parent statement.
pub(super) fn materialize_ctes_in_view(
    db: &Arc<Database>,
    txn: TxnView,
    ctes: &[CteDef],
    registry: &SessionRegistry,
) -> Result<HashMap<String, Arc<CteResult>>, QueryError> {
    let keys = validate_cte_names(ctes)?;
    let dependencies = collect_cte_dependencies(ctes, &keys);
    let mut pending: Vec<_> = (0..ctes.len()).collect();
    let mut results = HashMap::new();
    let mut session = crate::engine::expr::current_session_context_snapshot().unwrap_or_default();
    while !pending.is_empty() {
        let Some(pos) = pending.iter().position(|&idx| {
            dependencies[idx]
                .iter()
                .all(|dep| dep == &keys[idx] || results.contains_key(dep))
        }) else {
            return Err(error(
                crate::error::catalog::ER_NOT_SUPPORTED_YET,
                "mutually recursive CTEs are not supported",
            ));
        };
        let idx = pending.remove(pos);
        let cte = &ctes[idx];
        let result = with_cte_registry_extended(results.clone(), || {
            if dependencies[idx].contains(&keys[idx]) {
                if !cte.is_recursive {
                    return Err(error(
                        crate::error::catalog::ER_NOT_SUPPORTED_YET,
                        format!("recursive CTE '{}' requires WITH RECURSIVE", cte.name),
                    ));
                }
                let depth = session.cte_max_recursion_depth;
                materialize_recursive(db, &mut session, cte, registry, depth, None, Some(txn))
            } else {
                materialize_cte_in_view(db, txn, cte, registry)
            }
        })?;
        results.insert(keys[idx].clone(), Arc::new(result));
    }
    Ok(results)
}

fn materialize_cte_in_view(
    db: &Arc<Database>,
    txn: TxnView,
    cte: &CteDef,
    registry: &SessionRegistry,
) -> Result<CteResult, QueryError> {
    let response = with_cte_materialization(|| {
        super::dispatch::execute_set_operation_branch_response(db, txn, cte.query.clone(), registry)
    })?;
    route_response_to_cte_result(db, response, cte)
}

fn materialize_recursive(
    db: &Arc<Database>,
    session: &mut SessionContext,
    cte: &CteDef,
    registry: &SessionRegistry,
    depth_limit: u32,
    execution_plan: Option<&crate::planner::PhysicalPlan>,
    read_view: Option<TxnView>,
) -> Result<CteResult, QueryError> {
    let (anchor_stmt, recursive_selects, union_all) = recursive_parts(cte)?;
    let anchor_stmt = &anchor_stmt;
    // RecursiveUnion owns two explicit spools: `working` (the previous
    // iteration's delta) and `total`. Both use the ordinary spillable CTE
    // columnar store, so recursion no longer retains the accumulated result in
    // a `Vec<Vec<Value>>`. Anchor and recursive branches independently lower
    // through the same BatchOperator compiler used by non-recursive CTEs.
    let anchor_def = CteDef {
        name: cte.name.clone(),
        columns: cte.columns.clone(),
        is_recursive: false,
        query: anchor_stmt.clone(),
    };
    let supplied_branches = execution_plan
        .filter(|_| recursive_selects.len() == 1)
        .and_then(|plan| {
            let plan = match plan {
                crate::planner::PhysicalPlan::Materialize { input, .. } => input.as_ref(),
                other => other,
            };
            match plan {
                crate::planner::PhysicalPlan::RecursiveUnion {
                    anchor, recursive, ..
                } => Some((anchor.as_ref(), recursive.as_ref())),
                _ => None,
            }
        });
    let txn = read_view.unwrap_or_else(|| cte_statement_view(db, session));
    // Ordinary recursive branches are planned exactly once. The CteScan leaf
    // resolves the current delta from the registry at open time, so changing
    // delta cardinality cannot silently change join order or parallel
    // topology between iterations.
    let owned_anchor_plan = if supplied_branches.is_none() {
        crate::planner::plan_query_physical(db, anchor_stmt)?
    } else {
        None
    };
    let anchor_plan = supplied_branches
        .map(|(anchor, _)| anchor)
        .or(owned_anchor_plan.as_ref());
    let mut anchor = match (anchor_plan, anchor_stmt) {
        (Some(anchor_plan), crate::parser::QueryExpr::Select(anchor_select)) => {
            materialize_cte_from_physical(db, txn, &anchor_def, anchor_select, anchor_plan)?
        }
        _ if read_view.is_some() => materialize_cte_in_view(db, txn, &anchor_def, registry)?,
        _ => materialize_non_recursive(db, session, &anchor_def, registry)?,
    };
    for column in &mut anchor.columns {
        column.source.flags &= !crate::mysql::protocol::NOT_NULL_FLAG;
    }
    let anchor = Arc::new(anchor);
    let total_columns = anchor.columns.clone();
    // UNION DISTINCT first canonicalizes the anchor through the same
    // spillable hash-count state used by ordinary SetOp. This bounds the
    // duplicate state and establishes a unique first delta.
    let mut working = if union_all {
        anchor
    } else {
        let mut input = CteResultRowCursor::open(anchor)?;
        let mut distinct = crate::engine::executor::dispatch::combine_set_sources_spillable(
            &total_columns
                .iter()
                .map(|column| column.source.collation_id)
                .collect::<Vec<_>>(),
            SetOp::Union,
            false,
            total_columns.len(),
            db.data_dir(),
            || input.next_row(),
            || Ok(None),
        )?;
        let mut builder = CteSpillBuilder::new(total_columns.clone(), db.data_dir().to_path_buf());
        while let Some(row) = distinct.next_row()? {
            builder.push(row)?;
        }
        Arc::new(builder.finish()?)
    };
    let mut total_builder =
        CteSpillBuilder::new_with_stats(total_columns.clone(), db.data_dir().to_path_buf());
    visit_cte_result_rows(&working, |row| total_builder.push(row))?;
    // DISTINCT keeps each already-unique delta as a spillable history segment.
    // Membership is `produced EXCEPT DISTINCT history`; no unbounded process-
    // resident HashSet grows with recursion depth.
    let mut history = (!union_all).then(|| vec![Arc::clone(&working)]);
    set_cte_result(&cte.name, Arc::clone(&working));
    let target_types: Vec<_> = total_columns
        .iter()
        .map(crate::storage::schema::data_type_kind_for_result_column)
        .collect();
    let coerce_row = |mut row: Vec<Value>| -> Result<Vec<Value>, QueryError> {
        if row.len() != total_columns.len() {
            return Err(error(
                crate::error::catalog::ER_WRONG_NUMBER_OF_COLUMNS_IN_SELECT,
                "The used SELECT statements have a different number of columns",
            ));
        }
        for ((value, column), target) in row.iter_mut().zip(&total_columns).zip(&target_types) {
            *value = crate::storage::coerce_routine_typed_value(
                std::mem::replace(value, Value::Null),
                &column.name,
                target,
                column.source.flags & crate::mysql::protocol::UNSIGNED_FLAG != 0,
            )?;
        }
        Ok(row)
    };
    let owned_recursive_plans = recursive_selects
        .iter()
        .map(|select| {
            if supplied_branches.is_none()
                && select.derived.is_none()
                && select.joins.iter().all(|join| join.derived.is_none())
            {
                crate::planner::plan_query_physical(db, &Statement::Select(select.clone()))
            } else {
                Ok(None)
            }
        })
        .collect::<Result<Vec<_>, QueryError>>()?;
    // Step 2: iterate.
    let mut depth: u32 = 0;
    while working.estimated_rows > 0.0 {
        if depth >= depth_limit {
            return Err(error(
                crate::error::catalog::ER_CTE_MAX_RECURSION_DEPTH,
                format!(
                    "recursive CTE '{}' exceeded cte_max_recursion_depth ({})",
                    cte.name, depth_limit
                ),
            ));
        }
        // Working table = previous iteration's new rows. Install for the
        // recursive part to see.
        // P0-4: move the working table into the registry instead of cloning
        // it every iteration — `working_rows` is only read by the recursive
        // part (through the registry) and is reassigned at the loop tail.
        set_cte_result(&cte.name, Arc::clone(&working));
        // All branches read this iteration's immutable delta. Convert before
        // DISTINCT so equality and the next iteration use the declared type.
        let mut next_builder =
            CteSpillBuilder::new(total_columns.clone(), db.data_dir().to_path_buf());
        let mut produced_builder = (!union_all)
            .then(|| CteSpillBuilder::new(total_columns.clone(), db.data_dir().to_path_buf()));
        for (recursive_select, owned_plan) in recursive_selects.iter().zip(&owned_recursive_plans) {
            let recursive_plan = supplied_branches
                .map(|(_, plan)| plan)
                .or(owned_plan.as_ref());
            let recursive_def = CteDef {
                name: cte.name.clone(),
                columns: total_columns
                    .iter()
                    .map(|column| column.name.clone())
                    .collect(),
                is_recursive: false,
                query: crate::parser::QueryExpr::from(recursive_select.clone()),
            };
            let produced = match recursive_plan {
                Some(plan) => {
                    materialize_cte_from_physical(db, txn, &recursive_def, recursive_select, plan)?
                }
                None if read_view.is_some() => {
                    materialize_cte_in_view(db, txn, &recursive_def, registry)?
                }
                None => materialize_non_recursive(db, session, &recursive_def, registry)?,
            };
            if produced.columns.len() != total_columns.len() {
                return Err(error(
                    crate::error::catalog::ER_WRONG_NUMBER_OF_COLUMNS_IN_SELECT,
                    "The used SELECT statements have a different number of columns",
                ));
            }
            visit_cte_result_rows(&produced, |row| {
                let row = coerce_row(row)?;
                if let Some(builder) = produced_builder.as_mut() {
                    builder.push(row)
                } else {
                    total_builder.push(row.clone())?;
                    next_builder.push(row)
                }
            })?;
        }
        if let Some(produced_builder) = produced_builder {
            let produced = Arc::new(produced_builder.finish()?);
            let mut produced_rows = CteResultRowCursor::open(produced)?;
            let mut prior_rows = CteHistoryRowCursor::new(
                history
                    .as_ref()
                    .expect("DISTINCT recursion history initialized")
                    .clone(),
            );
            let mut new_rows = crate::engine::executor::dispatch::combine_set_sources_spillable(
                &total_columns
                    .iter()
                    .map(|column| column.source.collation_id)
                    .collect::<Vec<_>>(),
                SetOp::Except,
                false,
                total_columns.len(),
                db.data_dir(),
                || produced_rows.next_row(),
                || prior_rows.next_row(),
            )?;
            while let Some(row) = new_rows.next_row()? {
                total_builder.push(row.clone())?;
                next_builder.push(row)?;
            }
        }
        let next = Arc::new(next_builder.finish()?);
        if next.estimated_rows == 0.0 {
            break;
        }
        if let Some(history) = history.as_mut() {
            history.push(Arc::clone(&next));
        }
        working = next;
        depth += 1;
    }
    total_builder.finish()
}

fn validate_recursive_part(stmt: &Select, cte_name: &str) -> Result<(), QueryError> {
    if count_select_refs(stmt, cte_name) > 1 {
        return Err(error(crate::error::catalog::ER_CTE_RECURSIVE_REQUIRES_SINGLE_REFERENCE,
            format!("Recursive CTE '{cte_name}' must reference itself only once per recursive query block")));
    }
    if super::dispatch::select_has_window_functions(stmt) {
        return Err(error(
            crate::error::catalog::ER_CTE_RECURSIVE_FORBIDS_AGGREGATION,
            format!("Recursive CTE '{cte_name}' cannot use window functions"),
        ));
    }
    if !stmt.group_by.is_empty() {
        return Err(error(
            crate::error::catalog::ER_CTE_RECURSIVE_FORBIDS_AGGREGATION,
            format!("recursive CTE '{cte_name}' recursive part cannot use GROUP BY"),
        ));
    }
    if stmt.having.is_some() {
        return Err(error(
            crate::error::catalog::ER_NOT_SUPPORTED_YET,
            format!("recursive CTE '{cte_name}' recursive part cannot use HAVING"),
        ));
    }
    if stmt.distinct {
        return Err(error(
            crate::error::catalog::ER_NOT_SUPPORTED_YET,
            format!("recursive CTE '{cte_name}' recursive part cannot use DISTINCT"),
        ));
    }
    if !stmt.order_by.is_empty() {
        return Err(error(
            crate::error::catalog::ER_NOT_SUPPORTED_YET,
            format!("recursive CTE '{cte_name}' recursive part cannot use ORDER BY"),
        ));
    }
    if stmt.limit.is_some() || stmt.offset.is_some() {
        return Err(error(
            crate::error::catalog::ER_NOT_SUPPORTED_YET,
            format!("recursive CTE '{cte_name}' recursive part cannot use LIMIT/OFFSET"),
        ));
    }
    if projection_has_aggregate(&stmt.projection) {
        return Err(error(
            crate::error::catalog::ER_CTE_RECURSIVE_FORBIDS_AGGREGATION,
            format!("recursive CTE '{cte_name}' recursive part cannot use aggregate functions"),
        ));
    }
    Ok(())
}

fn projection_has_aggregate(items: &[SelectItem]) -> bool {
    items.iter().any(|item| match item {
        SelectItem::Expr { expr, .. } => expr_has_aggregate(expr),
        _ => false,
    })
}

fn expr_has_aggregate(expr: &Expr) -> bool {
    crate::parser::visit::has_block_aggregate(expr)
}

/// Does a statement reference the named CTE anywhere (FROM/JOIN/subquery)?
pub(crate) fn statement_references_cte<'q>(
    stmt: impl Into<crate::parser::QueryRef<'q>>,
    name: &str,
) -> bool {
    let stmt = stmt.into();
    match stmt {
        crate::parser::QueryRef::Select(select) => select_references_cte(select, name),
        crate::parser::QueryRef::SetOperation(s) => {
            statement_references_cte(&s.left, name) || statement_references_cte(&s.right, name)
        }
        crate::parser::QueryRef::WithCte(w) => {
            if let Some(shadow) = w
                .ctes
                .iter()
                .find(|cte| cte.name.eq_ignore_ascii_case(name))
            {
                !shadow.is_recursive && statement_references_cte(&shadow.query, name)
            } else {
                w.ctes
                    .iter()
                    .any(|cte| statement_references_cte(&cte.query, name))
                    || statement_references_cte(&w.body, name)
            }
        }
        _ => false,
    }
}

fn subquery_references_cte(sub: &SubQuery, name: &str) -> bool {
    match sub {
        SubQuery::Select(select) => select_references_cte(select, name),
        stmt @ (SubQuery::SetOp(_) | SubQuery::WithCte(_)) => statement_references_cte(stmt, name),
    }
}

fn select_references_cte(select: &Select, name: &str) -> bool {
    if let Some(derived) = &select.derived {
        if subquery_references_cte(derived, name) {
            return true;
        }
    }
    for table in &select.from {
        if table.name.eq_ignore_ascii_case(name) {
            return true;
        }
    }
    for join in &select.joins {
        if join.table.name.eq_ignore_ascii_case(name) {
            return true;
        }
        if join
            .derived
            .as_ref()
            .is_some_and(|derived| subquery_references_cte(derived, name))
        {
            return true;
        }
        if join
            .condition
            .as_ref()
            .is_some_and(|expr| expr_references_cte(expr, name))
        {
            return true;
        }
    }
    if select
        .group_by
        .iter()
        .any(|expr| expr_references_cte(expr, name))
    {
        return true;
    }
    if select
        .order_by
        .iter()
        .any(|item| expr_references_cte(&item.expr, name))
    {
        return true;
    }
    if let Some(sel) = &select.selection {
        if expr_references_cte(sel, name) {
            return true;
        }
    }
    if let Some(h) = &select.having {
        if expr_references_cte(h, name) {
            return true;
        }
    }
    select.projection.iter().any(|item| match item {
        SelectItem::Expr { expr, .. } => expr_references_cte(expr, name),
        _ => false,
    })
}

/// True when `stmt` references any CTE in the active registry, including
/// references hidden inside expression subqueries. Used as a conservative gate
/// for subquery plan caching: cached physical plans are not currently scoped by
/// CTE registry contents, so CTE-bearing subqueries must stay on the legacy
/// bind+execute path.
pub(crate) fn select_references_active_cte(stmt: &Select) -> bool {
    let keys: Vec<String> = CTE_REGISTRY.with(|r| r.borrow().keys().cloned().collect());
    keys.iter().any(|key| select_references_cte(stmt, key))
}

/// True when any FROM/JOIN target in `stmt` matches a CTE in the active
/// registry.
pub(crate) fn select_uses_cte_in_joins(stmt: &Select) -> bool {
    if !stmt.joins.is_empty() {
        let from_is_cte = stmt.from.first().map(|t| has_cte(&t.name)).unwrap_or(false);
        let join_has_cte = stmt.joins.iter().any(|j| has_cte(&j.table.name));
        if from_is_cte || join_has_cte {
            return true;
        }
    }
    false
}

/// True when any JOIN target carries a derived subquery
/// (e.g. `JOIN (SELECT ...) AS sub ON ...`). Such queries are routed
/// through the inline-derived bridge. Non-correlated inputs become bounded
/// CTE spools and then participate in the normal CBO/batch join graph.
pub(crate) fn select_has_join_derived(stmt: &Select) -> bool {
    stmt.joins.iter().any(|j| j.derived.is_some())
}

fn expr_references_cte(expr: &Expr, name: &str) -> bool {
    match expr {
        Expr::Subquery(s) => select_references_cte(s, name),
        Expr::InSubquery { subquery: s, .. } => select_references_cte(s, name),
        Expr::QuantifiedComparison { left, right, .. } => {
            expr_references_cte(left, name)
                || match right {
                    QuantifiedComparisonRight::Subquery(s) => select_references_cte(s, name),
                    QuantifiedComparisonRight::List(list) => {
                        list.iter().any(|item| expr_references_cte(item, name))
                    }
                }
        }
        Expr::Exists { subquery: s, .. } => select_references_cte(s, name),
        Expr::Binary { left, right, .. } => {
            expr_references_cte(left, name) || expr_references_cte(right, name)
        }
        Expr::Unary { expr, .. } => expr_references_cte(expr, name),
        Expr::Function { args, .. } => args.iter().any(|a| expr_references_cte(a, name)),
        Expr::WindowFunction { args, spec, .. } => {
            let crate::parser::WindowSpec {
                partition_by,
                order_by,
                frame,
                ..
            } = spec.as_ref();

            args.iter().any(|a| expr_references_cte(a, name))
                || partition_by.iter().any(|e| expr_references_cte(e, name))
                || order_by
                    .iter()
                    .any(|item| expr_references_cte(&item.expr, name))
                || frame
                    .as_ref()
                    .is_some_and(|frame| window_frame_references_cte(frame, name))
        }
        Expr::Case {
            operand,
            conditions,
            results,
            else_result,
        } => {
            operand
                .as_ref()
                .is_some_and(|e| expr_references_cte(e, name))
                || conditions.iter().any(|c| expr_references_cte(c, name))
                || results.iter().any(|r| expr_references_cte(r, name))
                || else_result
                    .as_ref()
                    .is_some_and(|e| expr_references_cte(e, name))
        }
        Expr::IsNull(expr) | Expr::IsNotNull(expr) | Expr::GroupKeyRef { expr, .. } => {
            expr_references_cte(expr, name)
        }
        Expr::InList { expr, list, .. } => {
            expr_references_cte(expr, name) || list.iter().any(|e| expr_references_cte(e, name))
        }
        Expr::Between {
            expr, low, high, ..
        } => {
            expr_references_cte(expr, name)
                || expr_references_cte(low, name)
                || expr_references_cte(high, name)
        }
        Expr::Like { expr, pattern, .. } => {
            expr_references_cte(expr, name) || expr_references_cte(pattern, name)
        }
        Expr::Tuple(items) => items.iter().any(|item| expr_references_cte(item, name)),
        _ => false,
    }
}

fn window_frame_references_cte(frame: &crate::parser::WindowFrame, name: &str) -> bool {
    window_frame_bound_references_cte(&frame.start, name)
        || window_frame_bound_references_cte(&frame.end, name)
}

fn window_frame_bound_references_cte(bound: &crate::parser::WindowFrameBound, name: &str) -> bool {
    match bound {
        crate::parser::WindowFrameBound::Preceding(expr)
        | crate::parser::WindowFrameBound::Following(expr) => expr_references_cte(expr, name),
        crate::parser::WindowFrameBound::UnboundedPreceding
        | crate::parser::WindowFrameBound::UnboundedFollowing
        | crate::parser::WindowFrameBound::CurrentRow => false,
    }
}

// ===========================================================================
// P1-1: single-use CTE inlining (MERGE)
//
// A non-recursive CTE referenced exactly once across the whole `WITH`
// statement is rewritten from a materialised result into a derived table at
// its single reference site, then dropped from the materialisation set. The
// derived table reuses the same AST shape the view-expansion path
// (`expand_views_in_select`) produces, so the existing derived-table planner —
// including `try_flatten_simple_derived_select` — transitively delivers
// projection and predicate pushdown for the inlined body, with that
// well-tested flatten guard as the correctness authority.
//
// Conservative fences (any failure ⇒ keep materialising, never wrong):
//   * only non-recursive, plain-`SELECT` bodies with no column-alias list;
//   * total reference count (FROM / JOIN / derived / expr-subquery, across the
//     body and every sibling CTE body) must be exactly 1 — over-counting only
//     skips inlining, so the count is always safe-by-construction;
//   * the single reference must be a top-level FROM/JOIN entry in the body
//     (references hidden in expression subqueries or sibling bodies are left
//     materialised);
//   * a body with non-deterministic functions is inlined with `no_merge` set
//     so flatten cannot duplicate a volatile expression across outer
//     references — it still materialises once, exactly like the CTE did.
// ===========================================================================

/// `MYDB_CTE_INLINE` gate (default on). `off`/`0`/`false` disables inlining so
/// the original full-materialisation path can serve as a differential oracle.
fn cte_inline_enabled() -> bool {
    match crate::util::env::var("MYDB_CTE_INLINE") {
        Ok(v) => !matches!(
            v.trim().to_ascii_lowercase().as_str(),
            "off" | "0" | "false"
        ),
        Err(_) => true,
    }
}

/// Rewrite every eligible single-use CTE in `ctes` into a derived table at its
/// reference site in `body`, removing it from `ctes`. Mutates in place; a CTE
/// that cannot be inlined is left untouched for materialisation.
pub(super) fn inline_single_use_ctes<'q>(
    ctes: &mut Vec<CteDef>,
    body: impl Into<crate::parser::QueryMut<'q>>,
) {
    let body = body.into();
    if !cte_inline_enabled() || ctes.is_empty() {
        return;
    }
    let crate::parser::QueryMut::Select(body_select) = body else {
        return;
    };
    let mut i = 0;
    while i < ctes.len() {
        if !cte_inline_eligible(&ctes[i]) {
            i += 1;
            continue;
        }
        let name = ctes[i].name.clone();
        // Total references across the body and every *other* CTE body.
        let mut total = count_select_refs(body_select, &name);
        for (j, other) in ctes.iter().enumerate() {
            if j != i {
                total += count_statement_refs(&other.query, &name);
            }
            if total > 1 {
                break;
            }
        }
        if total != 1 {
            i += 1;
            continue;
        }
        let crate::parser::QueryExpr::Select(cte_body) = &ctes[i].query else {
            i += 1;
            continue;
        };
        let no_merge = select_has_nondeterministic(cte_body);
        let cte_body = cte_body.clone();
        if try_inline_cte_into_select(body_select, &name, &cte_body, no_merge) {
            ctes.remove(i);
            // Vec shrank; the next CTE now sits at index `i`.
        } else {
            i += 1;
        }
    }
}

fn cte_inline_in_subquery_multi_enabled() -> bool {
    match crate::util::env::var("MYDB_CTE_INLINE_IN_SUBQUERY_MULTI") {
        Ok(v) => !matches!(
            v.trim().to_ascii_lowercase().as_str(),
            "off" | "0" | "false"
        ),
        Err(_) => true,
    }
}

/// O1 cost gate (`MYDB_CTE_INLINE_MAX_SCAN_ROWS`, default 100 000): the maximum
/// estimated base-table scan volume of a CTE body that may still be inlined
/// into each of its IN-subquery consumers. Inlining a multi-reference CTE
/// recomputes its body once per consumer, so it is only a win when the body is
/// cheap to recompute (a small dimension scan) — its sole upside over a shared
/// materialise-once plan is lazily skipping the body when an earlier semi-join
/// proves the consumer empty. When the body scans a large fact table (e.g.
/// TPC-DS q14's `cross_items`, ~5M rows across three fact tables), N× redundant
/// recompute dwarfs that, so we keep it materialised once and shared.
fn cte_inline_max_scan_rows() -> u64 {
    crate::util::env::var("MYDB_CTE_INLINE_MAX_SCAN_ROWS")
        .ok()
        .and_then(|v| v.trim().parse::<u64>().ok())
        .unwrap_or(100_000)
}

/// Sum the catalog row-count estimates of every base-table scan in `stmt`
/// (counting per occurrence, so a table joined under N aliases counts N×).
/// Names that do not resolve to an analysed base table — CTE references,
/// unanalysed/freshly-created tables, derived/`generate_series` sources —
/// contribute 0, so the estimate is a lower bound on scan volume. This biases
/// the inline decision toward the pre-O1 behaviour (inline) whenever we lack
/// positive evidence the body is large, keeping the change's blast radius to
/// cases with known-large fact scans.
fn cte_body_scan_volume<'q>(
    db: &Arc<Database>,
    current_db: &str,
    stmt: impl Into<crate::parser::QueryRef<'q>>,
) -> u64 {
    let stmt = stmt.into();
    let mut total = Some(0u64);
    accumulate_scan_volume_stmt(db, current_db, stmt, &mut total);
    total.unwrap_or(u64::MAX)
}

fn accumulate_scan_volume_stmt<'q>(
    db: &Arc<Database>,
    current_db: &str,
    stmt: impl Into<crate::parser::QueryRef<'q>>,
    total: &mut Option<u64>,
) {
    let stmt = stmt.into();
    match stmt {
        crate::parser::QueryRef::Select(select) => {
            accumulate_scan_volume_select(db, current_db, select, total)
        }
        crate::parser::QueryRef::SetOperation(set) => {
            accumulate_scan_volume_stmt(db, current_db, &set.left, total);
            accumulate_scan_volume_stmt(db, current_db, &set.right, total);
        }
        crate::parser::QueryRef::WithCte(with) => {
            for cte in with.ctes {
                accumulate_scan_volume_stmt(db, current_db, &cte.query, total);
            }
            accumulate_scan_volume_stmt(db, current_db, &with.body, total);
        }
        _ => {}
    }
}

fn accumulate_scan_volume_select(
    db: &Arc<Database>,
    current_db: &str,
    select: &Select,
    total: &mut Option<u64>,
) {
    let note = |t: &TableRef, total: &mut Option<u64>| {
        if t.generate_series.is_none() {
            let Some(accum) = total.as_mut() else {
                return;
            };
            match db.catalog_row_count_estimate(current_db, &t.name) {
                Some(rows) => *accum = accum.saturating_add(rows),
                None if db.is_base_table(current_db, &t.name) => *total = None,
                None => {}
            }
        }
    };
    if let Some(derived) = &select.derived {
        accumulate_scan_volume_subquery(db, current_db, derived, total);
    }
    for t in &select.from {
        note(t, total);
    }
    for join in &select.joins {
        if let Some(derived) = &join.derived {
            accumulate_scan_volume_subquery(db, current_db, derived, total);
        } else {
            note(&join.table, total);
        }
    }
}

fn accumulate_scan_volume_subquery(
    db: &Arc<Database>,
    current_db: &str,
    sub: &SubQuery,
    total: &mut Option<u64>,
) {
    match sub {
        SubQuery::Select(select) => accumulate_scan_volume_select(db, current_db, select, total),
        stmt @ (SubQuery::SetOp(_) | SubQuery::WithCte(_)) => {
            accumulate_scan_volume_stmt(db, current_db, stmt, total)
        }
    }
}

/// Defer multi-use CTEs that are referenced only as the FROM source of
/// IN-subqueries. This keeps them out of the eager WITH materialisation list
/// and lets normal predicate/join ordering avoid the CTE entirely when an
/// earlier semi-join proves the outer branch empty.
///
/// O1: inlining recomputes the body once per consumer, so it is gated on the
/// body's estimated scan volume staying under [`cte_inline_max_scan_rows`].
/// A large fact-scan CTE referenced N× stays materialised-once and shared.
pub(super) fn inline_in_subquery_only_ctes<'q>(
    db: &Arc<Database>,
    current_db: &str,
    ctes: &mut Vec<CteDef>,
    body: impl Into<crate::parser::QueryMut<'q>>,
) {
    let mut body = body.into();
    if !cte_inline_enabled() || !cte_inline_in_subquery_multi_enabled() || ctes.is_empty() {
        return;
    }
    let max_scan_rows = cte_inline_max_scan_rows();
    let mut i = 0;
    while i < ctes.len() {
        if !cte_inline_eligible(&ctes[i]) {
            i += 1;
            continue;
        }
        let name = ctes[i].name.clone();
        if count_cte_dependency_refs(&ctes[i].query, ctes, i) != 0 {
            i += 1;
            continue;
        }
        let mut sibling_refs = 0usize;
        for (j, other) in ctes.iter().enumerate() {
            if j != i {
                sibling_refs += count_statement_refs(&other.query, &name);
            }
        }
        if sibling_refs != 0 {
            i += 1;
            continue;
        }
        let total = count_statement_refs(body.as_ref(), &name);
        if total <= 1 || total != count_in_subquery_from_refs_stmt(body.as_ref(), &name) {
            i += 1;
            continue;
        }
        // O1: don't inline (and thus recompute N×) a body whose base-table scan
        // volume is large — keep it materialised once and shared instead.
        if cte_body_scan_volume(db, current_db, &ctes[i].query) > max_scan_rows {
            i += 1;
            continue;
        }
        let crate::parser::QueryExpr::Select(cte_body) = &ctes[i].query else {
            i += 1;
            continue;
        };
        if select_has_nondeterministic(cte_body) {
            i += 1;
            continue;
        }
        let cte_body = cte_body.clone();
        let replaced = inline_cte_into_in_subqueries_stmt(body.as_mut(), &name, &cte_body);
        if replaced == total {
            ctes.remove(i);
        } else {
            i += 1;
        }
    }
}

fn count_cte_dependency_refs<'q>(
    stmt: impl Into<crate::parser::QueryRef<'q>>,
    ctes: &[CteDef],
    self_idx: usize,
) -> usize {
    let stmt = stmt.into();
    ctes.iter()
        .enumerate()
        .filter(|(idx, _)| *idx != self_idx)
        .map(|(_, cte)| count_statement_refs(stmt, &cte.name))
        .sum()
}

fn cte_inline_eligible(cte: &CteDef) -> bool {
    (!cte.is_recursive || !statement_references_cte(&cte.query, &cte.name))
        && cte.columns.is_empty()
        && matches!(&cte.query, crate::parser::QueryExpr::Select(_))
}

// ===========================================================================
// P1-2: projection pushdown — narrow a materialised CTE body to the columns
// actually referenced across the whole WITH statement.
//
// Dropping a provably-unused output column shrinks the materialised rows for
// every consumer — including the in-memory CTE-join path, which copies all
// columns and gets no read-side pruning from the planner's
// `apply_column_pruning`.
//
// Safety is by over-approximation of "used":
//   * a CTE column is kept if its name appears as ANY bare identifier
//     anywhere, or as `<cte-alias>.col`, or if a wildcard might expand to
//     include it (`*` in any select, or `<cte-alias>.*`);
//   * narrowing only runs on a plain `SELECT` body whose projection items are
//     all simple/aliased columns with unique names and no DISTINCT / set-op /
//     super-aggregate / column-alias-list — so the output schema is known
//     statically and dropping an unreferenced item changes no semantics.
// Over-keeping is always safe; we only ever drop columns nothing can read.
// ===========================================================================

/// `MYDB_CTE_PUSHDOWN` gate (default on) for P1-2/P1-3. Off restores the
/// full-width materialisation path (differential oracle). Pushdown is
/// plan/structure-only-equivalent, so it never changes results.
fn cte_pushdown_enabled() -> bool {
    match crate::util::env::var("MYDB_CTE_PUSHDOWN") {
        Ok(v) => !matches!(
            v.trim().to_ascii_lowercase().as_str(),
            "off" | "0" | "false"
        ),
        Err(_) => true,
    }
}

pub(super) fn prune_unused_cte_columns(ctes: &mut [CteDef], body: &Statement) {
    if !cte_pushdown_enabled() || ctes.is_empty() {
        return;
    }
    // Collect c-aliases over body + every CTE body once per CTE below.
    for i in 0..ctes.len() {
        let name = ctes[i].name.clone();
        let Some(out_names) = narrowable_output_names(&ctes[i]) else {
            continue;
        };
        // Exchange c's reference aliases and used-column set across body + all
        // CTE bodies (including c's own body, harmless — it cannot reference
        // its own output columns by the CTE name).
        let mut aliases: Vec<String> = Vec::new();
        collect_cte_aliases_stmt(body, &name, &mut aliases);
        for (j, other) in ctes.iter().enumerate() {
            if j != i {
                collect_cte_aliases_stmt(&other.query, &name, &mut aliases);
            }
        }
        if aliases.is_empty() {
            continue;
        }
        let mut used = UsedCols::default();
        collect_used_stmt(body, &aliases, &mut used);
        for (j, other) in ctes.iter().enumerate() {
            if j != i {
                collect_used_stmt(&other.query, &aliases, &mut used);
            }
        }
        if used.wild {
            continue;
        }
        // Keep only output columns whose (lowercased) name was referenced.
        // When a CTE is consumed only for cardinality (for example
        // `SELECT COUNT(*) FROM cte`), no output column is needed by the
        // consumer. Keep a single cheap representative column so the
        // materialized CTE still has a stable row shape without forcing
        // expensive unused projection/group-key expressions to survive.
        let mut keep: Vec<bool> = out_names
            .iter()
            .map(|n| used.names.contains(&n.to_ascii_lowercase()))
            .collect();
        if keep.iter().all(|k| !*k) {
            if let Some(idx) = cheapest_projection_index(&ctes[i]) {
                keep[idx] = true;
            }
        }
        if keep.iter().all(|k| *k) {
            continue; // nothing to drop
        }
        retain_kept_projection(&mut ctes[i].query, &keep);
    }
}

fn cte_existence_keyset_enabled() -> bool {
    match crate::util::env::var("MYDB_CTE_EXISTENCE_KEYSET") {
        Ok(v) => !matches!(
            v.trim().to_ascii_lowercase().as_str(),
            "off" | "0" | "false"
        ),
        Err(_) => true,
    }
}

/// Shrink a materialized CTE whose consumers only need existence of one output
/// key. The high-payoff supported body today is a same-table self join proving
/// "there are at least two distinct marker values per key":
///
/// `SELECT a.k, ... FROM t a, t b WHERE a.k = b.k AND a.v <> b.v`
/// becomes
/// `SELECT a.k FROM t a WHERE a.k IS NOT NULL GROUP BY a.k
///  HAVING COUNT(DISTINCT a.v) >= 2`.
///
/// This is deliberately shape-driven, not benchmark-driven: no table or column
/// names are special-cased, and any context where CTE row multiplicity or
/// non-key columns are visible is left on the original materialization path.
pub(super) fn shrink_existence_key_ctes<'q>(
    ctes: &mut [CteDef],
    body: impl Into<crate::parser::QueryRef<'q>>,
) {
    let body = body.into();
    if !cte_existence_keyset_enabled() || ctes.is_empty() {
        return;
    }
    for idx in 0..ctes.len() {
        let rewrite = {
            let cte = &ctes[idx];
            existence_only_cte_key(cte, body, ctes, idx)
                .and_then(|key_col| build_self_neq_keyset_select(cte, &key_col))
        };
        if let Some(select) = rewrite {
            ctes[idx].query = crate::parser::QueryExpr::from(select);
        }
    }
}

#[derive(Default)]
struct ExistenceUseState {
    invalid: bool,
    refs: usize,
    used_cols: HashSet<String>,
    output_names: HashSet<String>,
}

fn existence_only_cte_key<'q>(
    cte: &CteDef,
    body: impl Into<crate::parser::QueryRef<'q>>,
    ctes: &[CteDef],
    self_idx: usize,
) -> Option<String> {
    let body = body.into();
    if cte.is_recursive || !cte.columns.is_empty() {
        return None;
    }
    let output_names = existence_cte_output_names(cte)?;
    let mut state = ExistenceUseState {
        output_names: output_names.clone(),
        ..Default::default()
    };
    collect_existence_uses_stmt(body, &cte.name, false, &mut state);
    for (idx, other) in ctes.iter().enumerate() {
        if idx != self_idx {
            collect_existence_uses_stmt(&other.query, &cte.name, false, &mut state);
        }
    }
    if state.invalid || state.refs == 0 || state.used_cols.len() != 1 {
        return None;
    }
    let key = state.used_cols.iter().next()?.clone();
    output_names.contains(&key).then_some(key)
}

fn existence_cte_output_names(cte: &CteDef) -> Option<HashSet<String>> {
    if cte.is_recursive || !cte.columns.is_empty() {
        return None;
    }
    let crate::parser::QueryExpr::Select(select) = &cte.query else {
        return None;
    };
    let mut names = HashSet::new();
    for item in &select.projection {
        let SelectItem::Expr { expr, alias } = item else {
            return None;
        };
        let name = alias.clone().or_else(|| expr_output_name(expr))?;
        if !names.insert(name.to_ascii_lowercase()) {
            return None;
        }
    }
    (!names.is_empty()).then_some(names)
}

fn collect_existence_uses_stmt<'q>(
    stmt: impl Into<crate::parser::QueryRef<'q>>,
    cte_name: &str,
    positive_in: bool,
    state: &mut ExistenceUseState,
) {
    let stmt = stmt.into();
    if state.invalid {
        return;
    }
    match stmt {
        crate::parser::QueryRef::Select(select) => {
            collect_existence_uses_select(select, cte_name, positive_in, state)
        }
        crate::parser::QueryRef::SetOperation(set) => {
            collect_existence_uses_stmt(&set.left, cte_name, positive_in, state);
            collect_existence_uses_stmt(&set.right, cte_name, positive_in, state);
        }
        // A nested WITH can shadow the name. Over-counting here only disables
        // the rewrite, so treating its body conservatively is safe.
        crate::parser::QueryRef::WithCte(with) => {
            collect_existence_uses_stmt(&with.body, cte_name, positive_in, state)
        }
        _ => {}
    }
}

fn collect_existence_uses_subquery(
    subquery: &SubQuery,
    cte_name: &str,
    positive_in: bool,
    state: &mut ExistenceUseState,
) {
    match subquery {
        SubQuery::Select(select) => {
            collect_existence_uses_select(select, cte_name, positive_in, state)
        }
        stmt @ (SubQuery::SetOp(_) | SubQuery::WithCte(_)) => {
            collect_existence_uses_stmt(stmt, cte_name, positive_in, state)
        }
    }
}

fn collect_existence_uses_select(
    select: &Select,
    cte_name: &str,
    positive_in: bool,
    state: &mut ExistenceUseState,
) {
    if state.invalid {
        return;
    }
    let aliases = cte_aliases_in_select(select, cte_name);
    if !aliases.is_empty() {
        state.refs = state.refs.saturating_add(aliases.len());
        if !positive_in || !select_is_safe_existence_consumer(select) {
            state.invalid = true;
            return;
        }
        collect_current_level_cte_cols_select(select, &aliases, state);
        if state.invalid {
            return;
        }
    }

    if let Some(derived) = &select.derived {
        collect_existence_uses_subquery(derived, cte_name, false, state);
    }
    for join in &select.joins {
        if let Some(derived) = &join.derived {
            collect_existence_uses_subquery(derived, cte_name, false, state);
        }
        if let Some(condition) = &join.condition {
            collect_existence_uses_expr_subqueries(condition, cte_name, positive_in, state);
        }
    }
    for item in &select.projection {
        if let SelectItem::Expr { expr, .. } = item {
            collect_existence_uses_expr_subqueries(expr, cte_name, positive_in, state);
        }
    }
    if let Some(selection) = &select.selection {
        collect_existence_uses_expr_subqueries(selection, cte_name, positive_in, state);
    }
    if let Some(having) = &select.having {
        collect_existence_uses_expr_subqueries(having, cte_name, positive_in, state);
    }
    for expr in &select.group_by {
        collect_existence_uses_expr_subqueries(expr, cte_name, positive_in, state);
    }
    for order in &select.order_by {
        collect_existence_uses_expr_subqueries(&order.expr, cte_name, positive_in, state);
    }
}

fn cte_aliases_in_select(select: &Select, cte_name: &str) -> Vec<String> {
    let mut aliases = Vec::new();
    for table in &select.from {
        if table.name.eq_ignore_ascii_case(cte_name) {
            aliases.push(table.alias.clone().unwrap_or_else(|| table.name.clone()));
        }
    }
    for join in &select.joins {
        if join.derived.is_none() && join.table.name.eq_ignore_ascii_case(cte_name) {
            aliases.push(
                join.table
                    .alias
                    .clone()
                    .unwrap_or_else(|| join.table.name.clone()),
            );
        }
    }
    aliases
}

fn select_is_safe_existence_consumer(select: &Select) -> bool {
    !select.distinct
        && select.group_by.is_empty()
        && select.grouping_sets.is_empty()
        && select.group_by_modifier == crate::parser::GroupByModifier::None
        && select.having.is_none()
        && select.order_by.is_empty()
        && select.limit.is_none()
        && select.offset.is_none()
        && !select.for_update
        && select.lock_clause.is_none()
        && select.values.is_none()
        && select.json_table.is_none()
        && select.derived.is_none()
        && select.named_windows.is_empty()
        && select.joins.iter().all(|join| {
            join.derived.is_none()
                && !join.natural
                && join.using_columns.is_empty()
                && !join.lateral
                && matches!(
                    join.join_type,
                    crate::parser::JoinType::Inner | crate::parser::JoinType::Cross
                )
        })
}

fn collect_current_level_cte_cols_select(
    select: &Select,
    aliases: &[String],
    state: &mut ExistenceUseState,
) {
    let sole_source = select.from.len() == 1 && select.joins.is_empty();
    for item in &select.projection {
        match item {
            SelectItem::Expr { expr, .. } => {
                collect_current_level_cte_cols_expr(expr, aliases, sole_source, state)
            }
            SelectItem::Wildcard => state.invalid = true,
            SelectItem::QualifiedWildcard(q) => {
                if aliases.iter().any(|alias| alias.eq_ignore_ascii_case(q)) {
                    state.invalid = true;
                }
            }
        }
    }
    if let Some(selection) = &select.selection {
        collect_current_level_cte_cols_expr(selection, aliases, sole_source, state);
    }
    for join in &select.joins {
        if let Some(condition) = &join.condition {
            collect_current_level_cte_cols_expr(condition, aliases, sole_source, state);
        }
    }
}

fn collect_current_level_cte_cols_expr(
    expr: &Expr,
    aliases: &[String],
    sole_source: bool,
    state: &mut ExistenceUseState,
) {
    crate::parser::visit::inspect_value_expr(
        expr,
        |expr| {
            if state.invalid {
                return false;
            }
            match expr {
                Expr::Identifier(name) if sole_source => {
                    state.used_cols.insert(name.to_ascii_lowercase());
                }
                Expr::Identifier(name) => {
                    if state.output_names.contains(&name.to_ascii_lowercase()) {
                        state.invalid = true;
                    }
                }
                Expr::CompoundIdentifier(parts) => {
                    if parts.len() >= 2 {
                        let qualifier = &parts[parts.len() - 2];
                        if aliases
                            .iter()
                            .any(|alias| alias.eq_ignore_ascii_case(qualifier))
                        {
                            if let Some(column) = parts.last() {
                                state.used_cols.insert(column.to_ascii_lowercase());
                            }
                        }
                    }
                }
                _ => {}
            }
            !matches!(
                expr,
                Expr::WindowFunction { .. } | Expr::QuantifiedComparison { .. }
            )
        },
        |_| {},
    );
}

fn collect_existence_uses_expr_subqueries(
    expr: &Expr,
    cte_name: &str,
    positive_in: bool,
    state: &mut ExistenceUseState,
) {
    if state.invalid {
        return;
    }
    match expr {
        Expr::InSubquery {
            expr,
            subquery,
            negated,
        } => {
            collect_existence_uses_expr_subqueries(expr, cte_name, positive_in, state);
            collect_existence_uses_select(subquery, cte_name, !*negated, state);
        }
        Expr::Subquery(select)
        | Expr::Exists {
            subquery: select, ..
        } => {
            collect_existence_uses_select(select, cte_name, false, state);
        }
        Expr::QuantifiedComparison { left, right, .. } => {
            collect_existence_uses_expr_subqueries(left, cte_name, positive_in, state);
            match right {
                QuantifiedComparisonRight::Subquery(select) => {
                    collect_existence_uses_select(select, cte_name, false, state);
                }
                QuantifiedComparisonRight::List(list) => {
                    for item in list {
                        collect_existence_uses_expr_subqueries(item, cte_name, positive_in, state);
                    }
                }
            }
        }
        Expr::Unary { expr, .. } | Expr::IsNull(expr) | Expr::IsNotNull(expr) => {
            collect_existence_uses_expr_subqueries(expr, cte_name, positive_in, state)
        }
        Expr::Binary { left, right, .. } => {
            collect_existence_uses_expr_subqueries(left, cte_name, positive_in, state);
            collect_existence_uses_expr_subqueries(right, cte_name, positive_in, state);
        }
        Expr::Function { args, .. } | Expr::Tuple(args) => {
            for arg in args {
                collect_existence_uses_expr_subqueries(arg, cte_name, positive_in, state);
            }
        }
        Expr::WindowFunction { args, spec, .. } => {
            let crate::parser::WindowSpec {
                partition_by,
                order_by,
                frame,
                ..
            } = spec.as_ref();

            for arg in args.iter().chain(partition_by.iter()) {
                collect_existence_uses_expr_subqueries(arg, cte_name, positive_in, state);
            }
            for order in order_by {
                collect_existence_uses_expr_subqueries(&order.expr, cte_name, positive_in, state);
            }
            if let Some(frame) = frame {
                collect_window_frame_cte_refs(frame, cte_name, positive_in, state);
            }
        }
        Expr::Case {
            operand,
            conditions,
            results,
            else_result,
        } => {
            if let Some(expr) = operand {
                collect_existence_uses_expr_subqueries(expr, cte_name, positive_in, state);
            }
            for expr in conditions.iter().chain(results.iter()) {
                collect_existence_uses_expr_subqueries(expr, cte_name, positive_in, state);
            }
            if let Some(expr) = else_result {
                collect_existence_uses_expr_subqueries(expr, cte_name, positive_in, state);
            }
        }
        Expr::InList { expr, list, .. } => {
            collect_existence_uses_expr_subqueries(expr, cte_name, positive_in, state);
            for item in list {
                collect_existence_uses_expr_subqueries(item, cte_name, positive_in, state);
            }
        }
        Expr::Between {
            expr, low, high, ..
        } => {
            collect_existence_uses_expr_subqueries(expr, cte_name, positive_in, state);
            collect_existence_uses_expr_subqueries(low, cte_name, positive_in, state);
            collect_existence_uses_expr_subqueries(high, cte_name, positive_in, state);
        }
        Expr::Like { expr, pattern, .. } => {
            collect_existence_uses_expr_subqueries(expr, cte_name, positive_in, state);
            collect_existence_uses_expr_subqueries(pattern, cte_name, positive_in, state);
        }
        Expr::GroupKeyRef { expr, .. } => {
            collect_existence_uses_expr_subqueries(expr, cte_name, positive_in, state)
        }
        Expr::Wildcard
        | Expr::Identifier(_)
        | Expr::CompoundIdentifier(_)
        | Expr::UserVariable(_)
        | Expr::SystemVariable { .. }
        | Expr::Literal(_)
        | Expr::BoundValue(_)
        | Expr::Placeholder(_)
        | Expr::Default(_)
        | Expr::DefaultQualified(_)
        | Expr::Grouping(_) => {}
    }
}

fn collect_window_frame_cte_refs(
    frame: &crate::parser::WindowFrame,
    cte_name: &str,
    positive_in: bool,
    state: &mut ExistenceUseState,
) {
    collect_window_frame_bound_cte_refs(&frame.start, cte_name, positive_in, state);
    collect_window_frame_bound_cte_refs(&frame.end, cte_name, positive_in, state);
}

fn collect_window_frame_bound_cte_refs(
    bound: &crate::parser::WindowFrameBound,
    cte_name: &str,
    positive_in: bool,
    state: &mut ExistenceUseState,
) {
    match bound {
        crate::parser::WindowFrameBound::Preceding(expr)
        | crate::parser::WindowFrameBound::Following(expr) => {
            collect_existence_uses_expr_subqueries(expr, cte_name, positive_in, state)
        }
        crate::parser::WindowFrameBound::CurrentRow
        | crate::parser::WindowFrameBound::UnboundedPreceding
        | crate::parser::WindowFrameBound::UnboundedFollowing => {}
    }
}

#[derive(Clone)]
struct SelfNeqKeysetSpec {
    base_table: TableRef,
    base_alias: String,
    key_column: String,
    neq_column: String,
}

fn build_self_neq_keyset_select(cte: &CteDef, key_col: &str) -> Option<Select> {
    let crate::parser::QueryExpr::Select(select) = &cte.query else {
        return None;
    };
    if !self_neq_body_is_rewritable(select) {
        return None;
    }
    let output_map = body_output_expr_map(cte)?;
    let key_expr = output_map.get(&key_col.to_ascii_lowercase())?;
    let spec = self_neq_keyset_spec(select, key_expr)?;
    let base_key = Expr::CompoundIdentifier(vec![spec.base_alias.clone(), spec.key_column]);
    let base_neq = Expr::CompoundIdentifier(vec![spec.base_alias.clone(), spec.neq_column]);
    let mut rewritten = select.as_ref().clone();
    rewritten.projection = vec![SelectItem::Expr {
        expr: base_key.clone(),
        alias: Some(key_col.to_string()),
    }];
    rewritten.table = Some(spec.base_table.name.clone());
    rewritten.from = vec![spec.base_table];
    rewritten.joins.clear();
    rewritten.selection = Some(Expr::IsNotNull(Box::new(base_key.clone())));
    rewritten.group_by = vec![base_key];
    rewritten.grouping_sets.clear();
    rewritten.group_by_modifier = crate::parser::GroupByModifier::None;
    rewritten.having = Some(Expr::Binary {
        left: Box::new(Expr::Function {
            name: "COUNT".to_string(),
            args: vec![base_neq],
            distinct: true,
        }),
        op: BinaryOp::GtEq,
        right: Box::new(Expr::Literal(crate::parser::Literal::Int(2))),
    });
    rewritten.order_by.clear();
    rewritten.limit = None;
    rewritten.offset = None;
    rewritten.distinct = false;
    rewritten
        .hint_warnings
        .push("CTE existence keyset rewrite".to_string());
    Some(rewritten)
}

fn self_neq_body_is_rewritable(select: &Select) -> bool {
    !select.distinct
        && select.group_by.is_empty()
        && select.grouping_sets.is_empty()
        && select.group_by_modifier == crate::parser::GroupByModifier::None
        && select.having.is_none()
        && select.order_by.is_empty()
        && select.limit.is_none()
        && select.offset.is_none()
        && !select.for_update
        && select.lock_clause.is_none()
        && select.values.is_none()
        && select.json_table.is_none()
        && select.derived.is_none()
        && select.named_windows.is_empty()
        && select.from.len() == 1
        && select.joins.len() == 1
        && select.joins[0].derived.is_none()
        && !select.joins[0].natural
        && select.joins[0].using_columns.is_empty()
        && !select.joins[0].lateral
        && matches!(
            select.joins[0].join_type,
            crate::parser::JoinType::Inner | crate::parser::JoinType::Cross
        )
}

fn self_neq_keyset_spec(select: &Select, key_expr: &Expr) -> Option<SelfNeqKeysetSpec> {
    let left = select.from.first()?;
    let right_join = select.joins.first()?;
    let right = &right_join.table;
    if !left.name.eq_ignore_ascii_case(&right.name) {
        return None;
    }
    let left_alias = table_alias_name(left).to_string();
    let right_alias = table_alias_name(right).to_string();
    if left_alias.eq_ignore_ascii_case(&right_alias) {
        return None;
    }
    let (key_alias, key_column) = qualified_column_ref(key_expr)?;
    if !key_alias.eq_ignore_ascii_case(&left_alias) && !key_alias.eq_ignore_ascii_case(&right_alias)
    {
        return None;
    }

    let mut conjuncts = Vec::new();
    if let Some(condition) = &right_join.condition {
        split_and(condition, &mut conjuncts);
    }
    if let Some(selection) = &select.selection {
        split_and(selection, &mut conjuncts);
    }
    let mut saw_key_eq = false;
    let mut neq_column: Option<String> = None;
    for conjunct in conjuncts {
        let Expr::Binary { left, op, right } = conjunct else {
            return None;
        };
        match op {
            BinaryOp::Eq => {
                let (l_alias, l_col) = qualified_column_ref(&left)?;
                let (r_alias, r_col) = qualified_column_ref(&right)?;
                if same_alias_col(&l_alias, &l_col, &left_alias, &key_column)
                    && same_alias_col(&r_alias, &r_col, &right_alias, &key_column)
                    || same_alias_col(&l_alias, &l_col, &right_alias, &key_column)
                        && same_alias_col(&r_alias, &r_col, &left_alias, &key_column)
                {
                    saw_key_eq = true;
                } else {
                    return None;
                }
            }
            BinaryOp::NotEq => {
                let (l_alias, l_col) = qualified_column_ref(&left)?;
                let (r_alias, r_col) = qualified_column_ref(&right)?;
                let same_column = l_col.eq_ignore_ascii_case(&r_col);
                let across_aliases = l_alias.eq_ignore_ascii_case(&left_alias)
                    && r_alias.eq_ignore_ascii_case(&right_alias)
                    || l_alias.eq_ignore_ascii_case(&right_alias)
                        && r_alias.eq_ignore_ascii_case(&left_alias);
                if !same_column || !across_aliases {
                    return None;
                }
                if neq_column
                    .as_ref()
                    .is_some_and(|existing| !existing.eq_ignore_ascii_case(&l_col))
                {
                    return None;
                }
                neq_column = Some(l_col);
            }
            _ => return None,
        }
    }
    Some(SelfNeqKeysetSpec {
        base_table: left.clone(),
        base_alias: left_alias,
        key_column,
        neq_column: neq_column.filter(|_| saw_key_eq)?,
    })
}

fn qualified_column_ref(expr: &Expr) -> Option<(String, String)> {
    match expr {
        Expr::CompoundIdentifier(parts) if parts.len() == 2 => {
            Some((parts[0].clone(), parts[1].clone()))
        }
        _ => None,
    }
}

fn same_alias_col(alias: &str, col: &str, expected_alias: &str, expected_col: &str) -> bool {
    alias.eq_ignore_ascii_case(expected_alias) && col.eq_ignore_ascii_case(expected_col)
}

/// Apply a positional keep-mask to a (possibly nested all-`UNION ALL`)
/// statement body. Every leaf SELECT shares the same output arity (verified
/// by [`narrowable_stmt_output_names`]), so the same mask is valid at each
/// branch — drop the i-th projection item from every branch consistently.
fn retain_kept_projection<'q>(stmt: impl Into<crate::parser::QueryMut<'q>>, keep: &[bool]) {
    let stmt = stmt.into();
    match stmt {
        crate::parser::QueryMut::Select(sel) => {
            let mut idx = 0;
            sel.projection.retain(|_| {
                let k = keep.get(idx).copied().unwrap_or(true);
                idx += 1;
                k
            });
        }
        crate::parser::QueryMut::SetOperation(set) => {
            retain_kept_projection(&mut set.left, keep);
            retain_kept_projection(&mut set.right, keep);
        }
        _ => {}
    }
}

/// Output column names of a CTE body iff it is safe to narrow: a plain
/// `SELECT` (no DISTINCT / GROUP BY super-aggregate / column-alias list)
/// whose projection is all simple/aliased columns with unique names and no
/// wildcards. Returns `None` (skip narrowing) otherwise.
fn narrowable_output_names(cte: &CteDef) -> Option<Vec<String>> {
    if cte.is_recursive || !cte.columns.is_empty() {
        return None;
    }
    narrowable_stmt_output_names(&cte.query)
}

/// Output names of a statement body iff it is safe to narrow. Handles a plain
/// `SELECT` (see [`narrowable_select_output_names`]) or an all-`UNION ALL` set
/// operation whose every leaf branch is itself narrowable with a matching
/// column count.
///
/// Only `UNION ALL` is narrowable: `UNION`/`INTERSECT`/`EXCEPT` *without* ALL
/// (and any per-branch `DISTINCT`) deduplicate over the full output tuple, so
/// dropping a column would change which rows survive. A set-op-level
/// `ORDER BY`/`LIMIT`/`OFFSET` can reference output columns positionally, so
/// those bail too.
fn narrowable_stmt_output_names<'q>(
    stmt: impl Into<crate::parser::QueryRef<'q>>,
) -> Option<Vec<String>> {
    let stmt = stmt.into();
    match stmt {
        crate::parser::QueryRef::Select(s) => narrowable_select_output_names(s),
        crate::parser::QueryRef::SetOperation(set) => {
            if !set.order_by.is_empty() || set.limit.is_some() || set.offset.is_some() {
                return None;
            }
            if !(set.op == crate::parser::SetOp::Union && set.all) {
                return None;
            }
            let left = narrowable_stmt_output_names(&set.left)?;
            let right = narrowable_stmt_output_names(&set.right)?;
            if left.len() != right.len() {
                return None;
            }
            // A set operation's output column names come from its leftmost
            // SELECT (SQL standard); the right branch aligns positionally.
            Some(left)
        }
        _ => None,
    }
}

/// Output names of a plain `SELECT` body iff narrowing is semantics-preserving:
/// no `DISTINCT` / `GROUP BY` super-aggregate / column-alias list, and a
/// projection of ≥2 simple/aliased columns with unique names and no wildcards
/// (so the output schema is known statically and dropping an unreferenced item
/// changes nothing). Returns `None` (skip narrowing) otherwise.
fn narrowable_select_output_names(s: &Select) -> Option<Vec<String>> {
    if s.distinct
        || s.group_by_modifier != crate::parser::GroupByModifier::None
        || !s.grouping_sets.is_empty()
        || s.projection.len() < 2
    {
        return None;
    }
    let mut names = Vec::with_capacity(s.projection.len());
    let mut seen = HashSet::new();
    for item in &s.projection {
        let SelectItem::Expr { expr, alias } = item else {
            return None; // wildcard in body → unknown static schema
        };
        let name = match alias {
            Some(a) => a.clone(),
            None => match expr {
                Expr::Identifier(n) => n.clone(),
                Expr::CompoundIdentifier(parts) => parts.last()?.clone(),
                _ => return None, // unaliased complex expr → name unclear
            },
        };
        if !seen.insert(name.to_ascii_lowercase()) {
            return None; // duplicate output names → ambiguous
        }
        names.push(name);
    }
    Some(names)
}

/// The leftmost leaf `SELECT` of a (possibly nested set-operation) body. A
/// set operation's output columns are positionally determined by its leftmost
/// branch, so cost decisions over output positions read from there.
fn leftmost_select_of_stmt<'q>(stmt: impl Into<crate::parser::QueryRef<'q>>) -> Option<&'q Select> {
    stmt.into().leftmost_select()
}

fn cheapest_projection_index(cte: &CteDef) -> Option<usize> {
    let s = leftmost_select_of_stmt(&cte.query)?;
    s.projection
        .iter()
        .enumerate()
        .filter_map(|(idx, item)| match item {
            SelectItem::Expr { expr, .. } => Some((idx, projection_expr_cost(expr))),
            _ => None,
        })
        .min_by_key(|(_, cost)| *cost)
        .map(|(idx, _)| idx)
}

fn projection_expr_cost(expr: &Expr) -> usize {
    match expr {
        Expr::Identifier(_) | Expr::CompoundIdentifier(_) | Expr::GroupKeyRef { .. } => 0,
        Expr::Literal(_)
        | Expr::BoundValue(_)
        | Expr::Placeholder(_)
        | Expr::Default(_)
        | Expr::DefaultQualified(_)
        | Expr::UserVariable(_)
        | Expr::SystemVariable { .. }
        | Expr::Grouping(_)
        | Expr::Wildcard => 1,
        Expr::Unary { expr, .. } | Expr::IsNull(expr) | Expr::IsNotNull(expr) => {
            1 + projection_expr_cost(expr)
        }
        Expr::Binary { left, right, .. } => {
            2 + projection_expr_cost(left) + projection_expr_cost(right)
        }
        Expr::Function { args, .. } => 10 + args.iter().map(projection_expr_cost).sum::<usize>(),
        Expr::WindowFunction { args, spec, .. } => {
            let crate::parser::WindowSpec {
                partition_by,
                order_by,
                ..
            } = spec.as_ref();

            20 + args.iter().map(projection_expr_cost).sum::<usize>()
                + partition_by.iter().map(projection_expr_cost).sum::<usize>()
                + order_by
                    .iter()
                    .map(|order| projection_expr_cost(&order.expr))
                    .sum::<usize>()
        }
        Expr::Case {
            operand,
            conditions,
            results,
            else_result,
        } => {
            5 + operand.as_deref().map_or(0, projection_expr_cost)
                + conditions.iter().map(projection_expr_cost).sum::<usize>()
                + results.iter().map(projection_expr_cost).sum::<usize>()
                + else_result.as_deref().map_or(0, projection_expr_cost)
        }
        Expr::InList { expr, list, .. } => {
            3 + projection_expr_cost(expr) + list.iter().map(projection_expr_cost).sum::<usize>()
        }
        Expr::Between {
            expr, low, high, ..
        } => {
            3 + projection_expr_cost(expr) + projection_expr_cost(low) + projection_expr_cost(high)
        }
        Expr::Like { expr, pattern, .. } => {
            3 + projection_expr_cost(expr) + projection_expr_cost(pattern)
        }
        Expr::Tuple(items) => 2 + items.iter().map(projection_expr_cost).sum::<usize>(),
        Expr::Subquery(_)
        | Expr::Exists { .. }
        | Expr::InSubquery { .. }
        | Expr::QuantifiedComparison { .. } => 100,
    }
}

fn fd_groupkey_minimize_enabled() -> bool {
    match crate::util::env::var("MYDB_FD_GROUPKEY_MINIMIZE") {
        Ok(v) => !matches!(
            v.trim().to_ascii_lowercase().as_str(),
            "off" | "0" | "false"
        ),
        Err(_) => true,
    }
}

/// Remove GROUP BY keys that are provably redundant after CTE output pruning.
///
/// A materialized CTE can project fewer columns than its original SELECT list.
/// If a now-unprojected GROUP BY expression is functionally determined by
/// retained GROUP BY keys from the same base table (currently PK/UNIQUE column
/// keys), dropping it does not merge SQL-visible groups. Q23's
/// `substr(i_item_desc,1,30)` is determined by retained `i_item_sk` through
/// item's primary key; removing it avoids per-row substring evaluation and
/// string group-key hashing while preserving the `item_sk,d_date` grouping.
pub(super) fn minimize_fd_group_by_keys(db: &Arc<Database>, current_db: &str, ctes: &mut [CteDef]) {
    if !cte_pushdown_enabled() || !fd_groupkey_minimize_enabled() {
        return;
    }
    for cte in ctes {
        let crate::parser::QueryExpr::Select(select) = &mut cte.query else {
            continue;
        };
        minimize_derived_fd_group_by_keys(db, current_db, select);
        if select.group_by.len() < 2
            || select.distinct
            || select.group_by_modifier != crate::parser::GroupByModifier::None
            || !select.grouping_sets.is_empty()
        {
            continue;
        }
        minimize_select_fd_group_by_keys(db, current_db, select);
    }
}

/// Projection pruning at a single-source derived boundary exposes unused
/// grouping keys to the same catalog-backed functional-dependency proof used
/// for CTE bodies. Retain every computed projection to preserve its evaluation.
fn minimize_derived_fd_group_by_keys(db: &Arc<Database>, current_db: &str, select: &mut Select) {
    if select.from.len() == 1 && select.joins.is_empty() {
        if let Some(inner) = select.derived.as_ref().and_then(SubQuery::as_select) {
            let names = narrowable_select_output_names(inner);
            if let Some(names) = names.filter(|_| inner.order_by.is_empty()) {
                let alias = table_alias_name(&select.from[0]);
                let mut scope = select.clone();
                scope.derived = None;
                let mut used = UsedCols::default();
                collect_used_select(&scope, &[alias.to_string()], &mut used);
                if !used.wild {
                    let mut keep = inner
                        .projection
                        .iter()
                        .zip(names)
                        .map(|(item, name)| {
                            used.names.contains(&name.to_ascii_lowercase())
                                || !matches!(
                                    item,
                                    SelectItem::Expr {
                                        expr: Expr::Identifier(_) | Expr::CompoundIdentifier(_),
                                        ..
                                    }
                                )
                        })
                        .collect::<Vec<_>>();
                    if !keep.iter().any(|&keep| keep) {
                        keep[0] = true;
                    }
                    let inner = select
                        .derived
                        .as_mut()
                        .and_then(SubQuery::as_select_mut)
                        .unwrap();
                    let mut index = 0;
                    inner.projection.retain(|_| {
                        let retained = keep[index];
                        index += 1;
                        retained
                    });
                }
            }
        }
    }
    if let Some(inner) = select.derived.as_mut().and_then(SubQuery::as_select_mut) {
        minimize_derived_fd_group_by_keys(db, current_db, inner);
        if inner.group_by.len() >= 2
            && !inner.distinct
            && inner.grouping_sets.is_empty()
            && inner.group_by_modifier == crate::parser::GroupByModifier::None
        {
            minimize_select_fd_group_by_keys(db, current_db, inner);
        }
    }
    for join in &mut select.joins {
        if let Some(inner) = join.derived.as_mut().and_then(SubQuery::as_select_mut) {
            minimize_derived_fd_group_by_keys(db, current_db, inner);
        }
    }
}

fn minimize_select_fd_group_by_keys(db: &Arc<Database>, current_db: &str, select: &mut Select) {
    let retained_projection_exprs: Vec<Expr> = select
        .projection
        .iter()
        .filter_map(|item| match item {
            SelectItem::Expr { expr, .. } => Some(expr.clone()),
            _ => None,
        })
        .collect();
    let removable: Vec<bool> = select
        .group_by
        .iter()
        .map(|group_expr| {
            !retained_projection_exprs
                .iter()
                .any(|expr| expr == group_expr)
                && select
                    .having
                    .as_ref()
                    .is_none_or(|having| !expr_contains_expr(having, group_expr))
                && select
                    .order_by
                    .iter()
                    .all(|order| !expr_contains_expr(&order.expr, group_expr))
                && group_expr_fd_determined_by_retained_keys(db, current_db, select, group_expr)
        })
        .collect();
    if removable.iter().any(|r| *r) && removable.iter().any(|r| !*r) {
        let mut idx = 0;
        select.group_by.retain(|_| {
            let keep = !removable[idx];
            idx += 1;
            keep
        });
    }
}

fn group_expr_fd_determined_by_retained_keys(
    db: &Arc<Database>,
    current_db: &str,
    select: &Select,
    group_expr: &Expr,
) -> bool {
    let Some((alias, columns, table)) =
        expr_columns_single_base_table(db, current_db, select, group_expr)
    else {
        return false;
    };
    if columns.is_empty() {
        return false;
    }
    if columns
        .iter()
        .any(|column| table.column_index(column).is_err())
    {
        return false;
    }
    unique_key_columns(&table).into_iter().any(|key_cols| {
        !key_cols.is_empty()
            && key_cols.iter().all(|key_col| {
                table
                    .column_index(key_col)
                    .ok()
                    .is_some_and(|index| !table.columns[index].nullable)
                    && select.group_by.iter().any(|candidate| {
                        candidate != group_expr
                            && expr_is_column_for_alias(candidate, &alias, key_col)
                    })
            })
    })
}

fn expr_columns_single_base_table(
    db: &Arc<Database>,
    current_db: &str,
    select: &Select,
    expr: &Expr,
) -> Option<(String, HashSet<String>, Arc<crate::storage::TableSchema>)> {
    let mut refs = Vec::new();
    collect_column_refs(expr, &mut refs);
    if refs.is_empty() {
        return None;
    }
    let mut resolved_alias: Option<String> = None;
    let mut resolved_table: Option<Arc<crate::storage::TableSchema>> = None;
    let mut columns = HashSet::new();
    for (qualifier, column) in refs {
        let (alias, table) =
            resolve_column_ref_table(db, current_db, select, qualifier.as_deref(), &column)?;
        if let Some(existing) = &resolved_alias {
            if !existing.eq_ignore_ascii_case(&alias) {
                return None;
            }
        } else {
            resolved_alias = Some(alias);
            resolved_table = Some(table);
        }
        columns.insert(column);
    }
    Some((resolved_alias?, columns, resolved_table?))
}

fn resolve_column_ref_table(
    db: &Arc<Database>,
    current_db: &str,
    select: &Select,
    qualifier: Option<&str>,
    column: &str,
) -> Option<(String, Arc<crate::storage::TableSchema>)> {
    let mut matches = Vec::new();
    for table_ref in select.from.iter().chain(
        select
            .joins
            .iter()
            .filter(|j| j.derived.is_none())
            .map(|j| &j.table),
    ) {
        let alias = table_alias_name(table_ref);
        if qualifier.is_some_and(|q| !alias.eq_ignore_ascii_case(q)) {
            continue;
        }
        let Ok(table) = db.table_in_database(current_db, &table_ref.name) else {
            continue;
        };
        if table.column_index(column).is_ok() {
            matches.push((alias.to_string(), table));
        }
    }
    (matches.len() == 1).then(|| matches.pop().expect("one match"))
}

fn unique_key_columns(table: &crate::storage::TableSchema) -> Vec<Vec<String>> {
    let mut keys = Vec::new();
    if !table.primary_key.is_empty() {
        keys.push(table.primary_key.clone());
    }
    for index in &table.indexes {
        if (index.primary || index.unique)
            && index.key_expressions.iter().all(|expr| expr.is_none())
            && !index.columns.is_empty()
        {
            keys.push(index.columns.clone());
        }
    }
    keys
}

fn table_alias_name(table: &TableRef) -> &str {
    table.alias.as_deref().unwrap_or(&table.name)
}

fn expr_is_column_for_alias(expr: &Expr, alias: &str, column: &str) -> bool {
    match expr {
        Expr::Identifier(name) => name.eq_ignore_ascii_case(column),
        Expr::CompoundIdentifier(parts) if parts.len() == 2 => {
            parts[0].eq_ignore_ascii_case(alias) && parts[1].eq_ignore_ascii_case(column)
        }
        Expr::GroupKeyRef { expr, .. } => expr_is_column_for_alias(expr, alias, column),
        _ => false,
    }
}

fn collect_column_refs(expr: &Expr, out: &mut Vec<(Option<String>, String)>) {
    crate::parser::visit::inspect_value_expr(
        expr,
        |expr| {
            match expr {
                Expr::Identifier(name) => out.push((None, name.clone())),
                Expr::CompoundIdentifier(parts) => {
                    if let Some(column) = parts.last() {
                        let qualifier = (parts.len() >= 2).then(|| parts[parts.len() - 2].clone());
                        out.push((qualifier, column.clone()));
                    }
                }
                Expr::WindowFunction { .. }
                | Expr::InSubquery { .. }
                | Expr::QuantifiedComparison { .. }
                | Expr::Subquery(_)
                | Expr::Exists { .. } => return false,
                _ => {}
            }
            true
        },
        |_| {},
    );
}

fn expr_contains_expr(haystack: &Expr, needle: &Expr) -> bool {
    let found = std::cell::Cell::new(false);
    crate::parser::visit::inspect_value_expr(
        haystack,
        |expr| {
            if found.get() {
                return false;
            }
            if expr == needle {
                found.set(true);
                return false;
            }
            match expr {
                Expr::Subquery(select)
                | Expr::InSubquery {
                    subquery: select, ..
                }
                | Expr::Exists {
                    subquery: select, ..
                } => {
                    found.set(select_contains_expr(select, needle));
                    false
                }
                _ => true,
            }
        },
        |select| {
            if !found.get() {
                found.set(select_contains_expr(select, needle));
            }
        },
    );
    found.get()
}

fn select_contains_expr(select: &Select, needle: &Expr) -> bool {
    select.projection.iter().any(
        |item| matches!(item, SelectItem::Expr { expr, .. } if expr_contains_expr(expr, needle)),
    ) || select
        .selection
        .as_ref()
        .is_some_and(|expr| expr_contains_expr(expr, needle))
        || select
            .having
            .as_ref()
            .is_some_and(|expr| expr_contains_expr(expr, needle))
        || select
            .group_by
            .iter()
            .any(|expr| expr_contains_expr(expr, needle))
        || select
            .order_by
            .iter()
            .any(|order| expr_contains_expr(&order.expr, needle))
}

#[derive(Default)]
struct UsedCols {
    names: HashSet<String>,
    wild: bool,
}

// ===========================================================================
// P1-3: predicate pushdown — push a filter into a materialised CTE body only
// when it is common to EVERY reference site.
//
// A CTE is materialised once and shared, so a filter may be pushed into its
// body only if every use applies it; otherwise pushing would drop rows a
// different use still needs. We therefore translate each (reference-site,
// alias) pair's single-source-on-CTE WHERE conjuncts into the body's own
// column terms and push only the *intersection* across all pairs. The
// canonical multi-reference shape (year_total self-join with a different
// filter per alias) has an empty intersection and is correctly left alone.
//
// Restricted to a plain, filterable body (no DISTINCT / GROUP BY / aggregate /
// HAVING / set-op): only then does a filter on an output column equal a WHERE
// filter on the body's source expression. Gated by `MYDB_CTE_PUSHDOWN`.
// ===========================================================================

pub(super) fn push_common_predicates_into_ctes<'q>(
    ctes: &mut [CteDef],
    body: impl Into<crate::parser::QueryRef<'q>>,
) {
    let body = body.into();
    if !cte_pushdown_enabled() || ctes.is_empty() {
        return;
    }
    for i in 0..ctes.len() {
        let name = ctes[i].name.clone();
        let Some(map) = body_output_expr_map(&ctes[i]) else {
            continue;
        };
        // One conjunct-set per (site, alias) pair across body + sibling bodies.
        let mut sites: Vec<Vec<Expr>> = Vec::new();
        collect_pushable_sites_stmt(body, &name, &map, &mut sites);
        for (j, other) in ctes.iter().enumerate() {
            if j != i {
                collect_pushable_sites_stmt(&other.query, &name, &map, &mut sites);
            }
        }
        if sites.len() < 2 {
            // 0 sites: nothing references it (shouldn't happen for a retained
            // CTE). 1 site: a single use — P1-1 handles single-ref via inline,
            // and a lone materialised use already filters at the CteScan; only
            // a genuinely shared (≥2-use) body benefits from body pushdown.
            continue;
        }
        // Intersection of all sites' conjunct sets (by structural equality).
        let mut common: Vec<Expr> = sites[0].clone();
        for set in &sites[1..] {
            common.retain(|e| set.contains(e));
            if common.is_empty() {
                break;
            }
        }
        if common.is_empty() {
            continue;
        }
        if let crate::parser::QueryExpr::Select(cte_body) = &mut ctes[i].query {
            for pred in common {
                cte_body.selection = Some(match cte_body.selection.take() {
                    Some(existing) => Expr::Binary {
                        op: crate::parser::BinaryOp::And,
                        left: Box::new(existing),
                        right: Box::new(pred),
                    },
                    None => pred,
                });
            }
        }
    }
}

/// Apply the pre-materialisation CTE rewrites used by normal execution and
/// EXPLAIN. Keeping the sequence centralised avoids EXPLAIN showing/executing
/// a different WITH plan than the real query path.
///
/// `inline` gates the two single-use / IN-subquery-only CTE inlining steps.
/// Execution always passes `inline = true`. EXPLAIN passes `true` as well so
/// the displayed plan reflects the inlined derived-subtree form that actually
/// executes — *except* for a FULL OUTER JOIN body, where it passes `false`:
/// the planner cannot fold a full outer join into one plan, so EXPLAIN falls
/// back to a synthetic CTE-in-JOIN display that cannot resolve an inlined
/// (orphaned) alias (1146 "table 'X' does not exist"). Keeping such bodies
/// non-inlined for display is the one remaining, documented divergence.
pub(super) fn apply_pre_materialization_rewrites(
    db: &Arc<Database>,
    current_db: &str,
    ctes: &mut Vec<CteDef>,
    body: &mut Statement,
    inline: bool,
) {
    // EXPLAIN and normal execution both enter here before a CTE body may be
    // planned directly by the batch materializer. Fold query-block constants
    // while the definitions are still explicit; after single-use inlining the
    // same SELECT can be nested below a derived source and would otherwise
    // retain per-row constant temporal arithmetic in the displayed plan.
    for cte in ctes.iter_mut() {
        fold_query_block_constants(&mut cte.query);
    }
    fold_query_block_constants(&mut *body);
    push_reference_domain_predicates_into_ctes(ctes, &mut *body);
    push_groupkey_predicates_into_ctes(ctes, &mut *body);
    if inline {
        inline_single_use_ctes(ctes, &mut *body);
    }
    prune_unused_cte_columns(ctes, &mut *body);
    shrink_existence_key_ctes(ctes, &mut *body);
    minimize_fd_group_by_keys(db, current_db, ctes);
    if inline {
        inline_in_subquery_only_ctes(db, current_db, ctes, &mut *body);
    }
    push_in_subquery_consumer_domains_into_ctes(ctes, &mut *body);
    push_common_predicates_into_ctes(ctes, &mut *body);
}

// ===========================================================================
// WS-A / WS-B: push group-key equality constants *through* aggregating CTE
// bodies, deriving them across the outer query's join-equality equivalence
// classes. Gated by `MYDB_PRED_PUSHDOWN_AGG` (default on; WS-C makes it safe).
//
// Motivating shape (TPC-DS Q78): three single-reference CTEs each `GROUP BY
// (d_year, item, customer)`, joined `ss LEFT JOIN ws LEFT JOIN cs` with an
// outer `WHERE ss_sold_year = 2002`. `ss_sold_year` is `ss`'s GROUP BY output
// of `d_year`, so the equality is semantically pushable *before* the aggregate
// (`σ_{P(g)}(γ_g f) ≡ γ_g(σ_{P(g)} f)`), turning the body's `date_dim` join
// into an indexed lookup and shrinking the aggregate input. The join equality
// `ws_sold_year = ss_sold_year` carries the constant to `ws`/`cs` as well.
//
// Why the existing P1-3 (`push_common_predicates_into_ctes`) misses this:
//  * `body_output_expr_map` returns None for any GROUP BY / aggregate body;
//  * it requires ≥2 reference sites; Q78's CTEs are single-reference;
//  * the outer predicate `ss_sold_year = 2002` is *unqualified*, which the
//    `single_source = false` (joined) translator conservatively rejects.
//
// Safety: a constant only enters an equivalence class from a WHERE-level
// equality (a post-join filter that eliminates violating output rows). For any
// CTE `x` whose output column is tied to that constant `k`, pushing `expr = k`
// into `x`'s body only removes rows that could never have contributed to a
// surviving output row (a surviving row has the class column = k; if `x` is
// matched its column = k by the equality chain, if unmatched no `x` row is
// involved). This holds regardless of join side, so it is sound to push into
// the (independently materialised) body — but we never synthesise an *outer*
// WHERE on a null-supplying column. We further restrict pushing to CTEs with
// exactly one reference across the whole statement, so a self-joined / shared
// body (year_total shape, different constant per alias) is never over-filtered.
// ===========================================================================

/// `MYDB_PRED_PUSHDOWN_AGG` gate (default on). Off restores the pre-pushdown
/// plan as a differential oracle — the rewrite is plan/structure-only-equivalent
/// so it never changes results, only shrinks the rows flowing through each body.
/// Default-on is gated on WS-C (the memory-safe streaming join path) being in
/// place: without it, a tiny-estimate pushed plan could materialise a large
/// join output and 1037. With WS-C the over-budget join streams instead, so the
/// pushdown is both faster and memory-safe at the default budget.
fn pred_pushdown_agg_enabled() -> bool {
    match crate::util::env::var("MYDB_PRED_PUSHDOWN_AGG") {
        Ok(v) => !matches!(
            v.trim().to_ascii_lowercase().as_str(),
            "off" | "0" | "false"
        ),
        Err(_) => true,
    }
}

/// Union-find over `(alias, column)` nodes carrying an optional constant per
/// class. Used to compute the equality-equivalence closure of the outer query
/// (WHERE + every join ON) and attach WHERE-level constants to whole classes.
struct EquivClasses {
    parent: Vec<usize>,
    constant: Vec<Option<Expr>>,
}

impl EquivClasses {
    fn new() -> Self {
        Self {
            parent: Vec::new(),
            constant: Vec::new(),
        }
    }
    fn make(&mut self) -> usize {
        let i = self.parent.len();
        self.parent.push(i);
        self.constant.push(None);
        i
    }
    fn find(&mut self, mut x: usize) -> usize {
        while self.parent[x] != x {
            self.parent[x] = self.parent[self.parent[x]];
            x = self.parent[x];
        }
        x
    }
    fn union(&mut self, a: usize, b: usize) {
        let (ra, rb) = (self.find(a), self.find(b));
        if ra == rb {
            return;
        }
        let carried = self.constant[rb].take();
        self.parent[rb] = ra;
        if self.constant[ra].is_none() {
            self.constant[ra] = carried;
        }
    }
    fn set_const(&mut self, x: usize, c: Expr) {
        let r = self.find(x);
        if self.constant[r].is_none() {
            self.constant[r] = Some(c);
        }
    }
    fn class_const(&mut self, x: usize) -> Option<Expr> {
        let r = self.find(x);
        self.constant[r].clone()
    }
}

/// A pushable literal-valued constant (`= literal` / bound value), or none.
fn as_pushable_constant(e: &Expr) -> Option<Expr> {
    match e {
        Expr::Literal(_) | Expr::BoundValue(_) => Some(e.clone()),
        _ => {
            let mut folded = e.clone();
            crate::engine::expr::fold_constant_subexprs(&mut folded);
            match folded {
                Expr::Literal(_) | Expr::BoundValue(_) => Some(folded),
                _ => None,
            }
        }
    }
}

/// WS-D: shared CTE reference-domain pushdown.
///
/// A self-joined/materialised CTE cannot accept a single alias-local predicate
/// (`a.month = 2`) because the same materialised rows also feed the other
/// aliases. It can, however, accept the union of all alias-local domains when
/// every direct reference constrains the same pushable output column:
///
/// ```sql
/// WITH inv AS (...) SELECT ...
/// FROM inv a, inv b
/// WHERE a.d_moy = 2 AND b.d_moy = 2 + 1
/// ```
///
/// becomes `d_moy IN (2, 3)` inside `inv`, while the outer predicates remain in
/// place. This is the shared-body counterpart of WS-A/B's single-reference
/// group-key pushdown: it is safe for grouped CTE bodies only for group-key
/// outputs, as determined by `body_pushable_output_map`.
/// Resolve direct CTE sources once for each pushdown pass. Unknown sources
/// make unqualified names unsafe; duplicate output names remain ambiguous.
struct CteSourceContext {
    alias_to_cte: HashMap<String, usize>,
    has_unknown_source: bool,
    col_owner: HashMap<String, Option<String>>,
}

impl CteSourceContext {
    fn new(ctes: &[CteDef], outer: &Select) -> Self {
        let mut alias_to_cte: HashMap<String, usize> = HashMap::new();
        let mut has_unknown_source = false;
        let note_source = |t: &TableRef,
                           alias_to_cte: &mut HashMap<String, usize>,
                           has_unknown_source: &mut bool| {
            if t.generate_series.is_some() {
                *has_unknown_source = true;
                return;
            }
            match ctes
                .iter()
                .position(|c| c.name.eq_ignore_ascii_case(&t.name))
            {
                Some(i) => {
                    let alias = t.alias.clone().unwrap_or_else(|| t.name.clone());
                    alias_to_cte.insert(alias.to_ascii_lowercase(), i);
                }
                None => *has_unknown_source = true,
            }
        };
        if outer.derived.is_some() {
            has_unknown_source = true;
        }
        for t in &outer.from {
            note_source(t, &mut alias_to_cte, &mut has_unknown_source);
        }
        for j in &outer.joins {
            if j.derived.is_some() {
                has_unknown_source = true;
            } else {
                note_source(&j.table, &mut alias_to_cte, &mut has_unknown_source);
            }
        }
        let mut col_owner: HashMap<String, Option<String>> = HashMap::new();
        for (alias, &i) in &alias_to_cte {
            for col in cte_output_column_names(&ctes[i]) {
                let key = col.to_ascii_lowercase();
                col_owner
                    .entry(key)
                    .and_modify(|e| *e = None)
                    .or_insert_with(|| Some(alias.clone()));
            }
        }
        Self {
            alias_to_cte,
            has_unknown_source,
            col_owner,
        }
    }

    fn resolve(&self, e: &Expr) -> Option<(String, String)> {
        let alias_to_cte = &self.alias_to_cte;
        let has_unknown_source = self.has_unknown_source;
        let col_owner = &self.col_owner;
        match e {
            Expr::CompoundIdentifier(parts) if parts.len() == 2 => {
                let a = parts[0].to_ascii_lowercase();
                if alias_to_cte.contains_key(&a) {
                    Some((a, parts[1].to_ascii_lowercase()))
                } else {
                    None
                }
            }
            Expr::Identifier(c) if !has_unknown_source => {
                let cl = c.to_ascii_lowercase();
                match col_owner.get(&cl) {
                    Some(Some(a)) => Some((a.clone(), cl)),
                    _ => None,
                }
            }
            _ => None,
        }
    }
}

pub(super) fn push_reference_domain_predicates_into_ctes<'q>(
    ctes: &mut [CteDef],
    body: impl Into<crate::parser::QueryRef<'q>>,
) {
    let body = body.into();
    if !pred_pushdown_agg_enabled() || ctes.is_empty() {
        return;
    }
    let crate::parser::QueryRef::Select(outer) = body else {
        return;
    };

    let sources = CteSourceContext::new(ctes, outer);
    let alias_to_cte = &sources.alias_to_cte;
    if alias_to_cte.is_empty() {
        return;
    }

    let mut direct_aliases_by_cte: Vec<Vec<String>> = vec![Vec::new(); ctes.len()];
    for (alias, &i) in alias_to_cte {
        direct_aliases_by_cte[i].push(alias.clone());
    }

    let resolve = |e: &Expr| sources.resolve(e);

    let mut alias_domains: HashMap<(String, String), Vec<Expr>> = HashMap::new();
    if let Some(sel) = &outer.selection {
        let mut conjuncts = Vec::new();
        split_and(sel, &mut conjuncts);
        for c in &conjuncts {
            if let Some(((alias, col), domain)) = extract_alias_domain(c, &resolve) {
                merge_alias_domain(&mut alias_domains, (alias, col), domain);
            }
        }
    }

    let mut ref_count: Vec<usize> = vec![0; ctes.len()];
    for (i, count) in ref_count.iter_mut().enumerate() {
        let mut aliases = Vec::new();
        collect_cte_aliases_stmt(body, &ctes[i].name, &mut aliases);
        for (j, other) in ctes.iter().enumerate() {
            if j != i {
                collect_cte_aliases_stmt(&other.query, &ctes[i].name, &mut aliases);
            }
        }
        *count = aliases.len();
    }

    let mut pushes: Vec<(usize, String, Vec<Expr>)> = Vec::new();
    for i in 0..ctes.len() {
        let aliases = &direct_aliases_by_cte[i];
        if aliases.len() < 2 || aliases.len() != ref_count[i] {
            continue;
        }
        for col in cte_output_column_names(&ctes[i]) {
            let mut union: Vec<Expr> = Vec::new();
            let mut every_alias_constrained = true;
            for alias in aliases {
                let key = (alias.clone(), col.clone());
                let Some(domain) = alias_domains.get(&key) else {
                    every_alias_constrained = false;
                    break;
                };
                if domain.is_empty() {
                    every_alias_constrained = false;
                    break;
                }
                extend_domain(&mut union, domain.iter().cloned());
            }
            if !every_alias_constrained || union.is_empty() {
                continue;
            }
            // Deterministic order: the alias-domain union can otherwise come
            // out in a different order across calls (q39's `d_moy IN (2,3)`
            // vs `(3,2)`), which destabilises any key derived from the CTE
            // body's Debug form — e.g. the session result cache — causing
            // spurious misses between two statements over the same CTE.
            union.sort_by(|a, b| format!("{a:?}").cmp(&format!("{b:?}")));
            pushes.push((i, col, union));
        }
    }

    for (i, col, domain) in pushes {
        push_output_domain_predicate(&mut ctes[i], &col, domain);
    }
}

fn extract_alias_domain(
    expr: &Expr,
    resolve: &dyn Fn(&Expr) -> Option<(String, String)>,
) -> Option<((String, String), Vec<Expr>)> {
    match expr {
        Expr::Binary {
            op: crate::parser::BinaryOp::Eq,
            left,
            right,
        } => match (resolve(left), resolve(right)) {
            (Some(col), None) => as_pushable_constant(right).map(|c| (col, vec![c])),
            (None, Some(col)) => as_pushable_constant(left).map(|c| (col, vec![c])),
            _ => None,
        },
        Expr::InList {
            expr,
            list,
            negated: false,
        } => {
            let col = resolve(expr)?;
            let mut domain = Vec::with_capacity(list.len());
            for item in list {
                domain.push(as_pushable_constant(item)?);
            }
            Some((col, dedup_domain(domain)))
        }
        _ => None,
    }
}

fn merge_alias_domain(
    domains: &mut HashMap<(String, String), Vec<Expr>>,
    key: (String, String),
    incoming: Vec<Expr>,
) {
    domains
        .entry(key)
        .and_modify(|existing| {
            existing.retain(|e| incoming.iter().any(|i| i == e));
        })
        .or_insert_with(|| dedup_domain(incoming));
}

fn domain_predicate(expr: Expr, values: Vec<Expr>) -> Expr {
    if values.len() == 1 {
        Expr::Binary {
            left: Box::new(expr),
            op: crate::parser::BinaryOp::Eq,
            right: Box::new(values.into_iter().next().unwrap()),
        }
    } else {
        Expr::InList {
            expr: Box::new(expr),
            list: values,
            negated: false,
        }
    }
}

fn extend_domain(out: &mut Vec<Expr>, items: impl IntoIterator<Item = Expr>) {
    for item in items {
        if !out.iter().any(|e| e == &item) {
            out.push(item);
        }
    }
}

fn dedup_domain(values: Vec<Expr>) -> Vec<Expr> {
    let mut out = Vec::new();
    extend_domain(&mut out, values);
    out
}

/// Push dynamic key domains from `outer_key IN (SELECT cte_key FROM cte)`
/// consumers into the consumed CTE. The rewrite is an over-approximation:
/// the generated domain subquery keeps the consumer SELECT's base FROM/JOIN
/// and non-CTE WHERE conjuncts, but drops every CTE-bearing predicate. That
/// can leave extra CTE groups in place, but never removes a group a surviving
/// consumer row could need.
pub(super) fn push_in_subquery_consumer_domains_into_ctes(ctes: &mut [CteDef], body: &Statement) {
    if !pred_pushdown_agg_enabled()
        || !in_subquery_consumer_domain_pushdown_enabled()
        || ctes.is_empty()
    {
        return;
    }
    let cte_names = ctes
        .iter()
        .map(|cte| cte.name.to_ascii_lowercase())
        .collect::<Vec<_>>();
    for idx in 0..ctes.len() {
        if ctes[idx].is_recursive {
            continue;
        }
        let Some(output_map) = body_pushable_output_map_for_in_domain(&ctes[idx]) else {
            continue;
        };
        let mut domains: HashMap<String, Vec<Select>> = HashMap::new();
        collect_in_subquery_consumer_domains_stmt(body, &ctes[idx].name, &cte_names, &mut domains);
        if domains.is_empty() {
            continue;
        }
        for (output_col, selects) in domains {
            let Some(body_expr) = output_map.get(&output_col) else {
                continue;
            };
            let mut disjuncts = Vec::new();
            for domain_select in selects {
                disjuncts.push(Expr::InSubquery {
                    expr: Box::new(body_expr.clone()),
                    subquery: Box::new(domain_select),
                    negated: false,
                });
            }
            let Some(predicate) = combine_or(disjuncts) else {
                continue;
            };
            push_body_where_conjunct(&mut ctes[idx], predicate);
        }
    }
}

fn in_subquery_consumer_domain_pushdown_enabled() -> bool {
    crate::util::env::var("MYDB_CTE_IN_SUBQUERY_DOMAIN_PUSHDOWN")
        .map(|v| {
            !matches!(
                v.trim().to_ascii_lowercase().as_str(),
                "0" | "off" | "false" | "no"
            )
        })
        .unwrap_or(true)
}

fn collect_in_subquery_consumer_domains_stmt<'q>(
    stmt: impl Into<crate::parser::QueryRef<'q>>,
    cte_name: &str,
    cte_names: &[String],
    out: &mut HashMap<String, Vec<Select>>,
) {
    let stmt = stmt.into();
    match stmt {
        crate::parser::QueryRef::Select(select) => {
            collect_in_subquery_consumer_domains_select(select, cte_name, cte_names, out)
        }
        crate::parser::QueryRef::SetOperation(set) => {
            collect_in_subquery_consumer_domains_stmt(&set.left, cte_name, cte_names, out);
            collect_in_subquery_consumer_domains_stmt(&set.right, cte_name, cte_names, out);
        }
        crate::parser::QueryRef::WithCte(with) => {
            collect_in_subquery_consumer_domains_stmt(&with.body, cte_name, cte_names, out);
        }
        _ => {}
    }
}

fn collect_in_subquery_consumer_domains_select(
    select: &Select,
    cte_name: &str,
    cte_names: &[String],
    out: &mut HashMap<String, Vec<Select>>,
) {
    if let Some(derived) = &select.derived {
        collect_in_subquery_consumer_domains_subquery(derived, cte_name, cte_names, out);
    }
    for join in &select.joins {
        if let Some(derived) = &join.derived {
            collect_in_subquery_consumer_domains_subquery(derived, cte_name, cte_names, out);
        }
        if let Some(condition) = &join.condition {
            collect_in_subquery_consumer_domains_expr(condition, cte_name, cte_names, out);
        }
    }
    if let Some(selection) = &select.selection {
        let mut conjuncts = Vec::new();
        split_and(selection, &mut conjuncts);
        for conjunct in &conjuncts {
            if let Some((output_col, outer_expr)) = simple_in_subquery_cte_key(conjunct, cte_name) {
                if expr_references_any_cte(&outer_expr, cte_names) {
                    continue;
                }
                if let Some(domain_select) =
                    build_consumer_domain_select(select, outer_expr, cte_names)
                {
                    out.entry(output_col).or_default().push(domain_select);
                }
            }
            collect_in_subquery_consumer_domains_expr(conjunct, cte_name, cte_names, out);
        }
    }
    if let Some(having) = &select.having {
        collect_in_subquery_consumer_domains_expr(having, cte_name, cte_names, out);
    }
}

fn collect_in_subquery_consumer_domains_subquery(
    subquery: &SubQuery,
    cte_name: &str,
    cte_names: &[String],
    out: &mut HashMap<String, Vec<Select>>,
) {
    match subquery {
        SubQuery::Select(select) => {
            collect_in_subquery_consumer_domains_select(select, cte_name, cte_names, out)
        }
        stmt @ (SubQuery::SetOp(_) | SubQuery::WithCte(_)) => {
            collect_in_subquery_consumer_domains_stmt(stmt, cte_name, cte_names, out)
        }
    }
}

fn collect_in_subquery_consumer_domains_expr(
    expr: &Expr,
    cte_name: &str,
    cte_names: &[String],
    out: &mut HashMap<String, Vec<Select>>,
) {
    match expr {
        Expr::Subquery(s) | Expr::Exists { subquery: s, .. } => {
            collect_in_subquery_consumer_domains_select(s, cte_name, cte_names, out)
        }
        Expr::InSubquery { subquery, .. } => {
            collect_in_subquery_consumer_domains_select(subquery, cte_name, cte_names, out)
        }
        Expr::QuantifiedComparison { left, right, .. } => {
            collect_in_subquery_consumer_domains_expr(left, cte_name, cte_names, out);
            match right {
                QuantifiedComparisonRight::Subquery(s) => {
                    collect_in_subquery_consumer_domains_select(s, cte_name, cte_names, out)
                }
                QuantifiedComparisonRight::List(items) => {
                    for item in items {
                        collect_in_subquery_consumer_domains_expr(item, cte_name, cte_names, out);
                    }
                }
            }
        }
        Expr::Binary { left, right, .. } => {
            collect_in_subquery_consumer_domains_expr(left, cte_name, cte_names, out);
            collect_in_subquery_consumer_domains_expr(right, cte_name, cte_names, out);
        }
        Expr::Unary { expr, .. }
        | Expr::IsNull(expr)
        | Expr::IsNotNull(expr)
        | Expr::GroupKeyRef { expr, .. } => {
            collect_in_subquery_consumer_domains_expr(expr, cte_name, cte_names, out)
        }
        Expr::Function { args, .. } => {
            for arg in args {
                collect_in_subquery_consumer_domains_expr(arg, cte_name, cte_names, out);
            }
        }
        Expr::WindowFunction { args, spec, .. } => {
            let crate::parser::WindowSpec {
                partition_by,
                order_by,
                ..
            } = spec.as_ref();

            for arg in args {
                collect_in_subquery_consumer_domains_expr(arg, cte_name, cte_names, out);
            }
            for expr in partition_by {
                collect_in_subquery_consumer_domains_expr(expr, cte_name, cte_names, out);
            }
            for item in order_by {
                collect_in_subquery_consumer_domains_expr(&item.expr, cte_name, cte_names, out);
            }
        }
        Expr::Case {
            operand,
            conditions,
            results,
            else_result,
        } => {
            if let Some(operand) = operand {
                collect_in_subquery_consumer_domains_expr(operand, cte_name, cte_names, out);
            }
            for expr in conditions.iter().chain(results.iter()) {
                collect_in_subquery_consumer_domains_expr(expr, cte_name, cte_names, out);
            }
            if let Some(expr) = else_result {
                collect_in_subquery_consumer_domains_expr(expr, cte_name, cte_names, out);
            }
        }
        Expr::InList { expr, list, .. } => {
            collect_in_subquery_consumer_domains_expr(expr, cte_name, cte_names, out);
            for item in list {
                collect_in_subquery_consumer_domains_expr(item, cte_name, cte_names, out);
            }
        }
        Expr::Between {
            expr, low, high, ..
        } => {
            collect_in_subquery_consumer_domains_expr(expr, cte_name, cte_names, out);
            collect_in_subquery_consumer_domains_expr(low, cte_name, cte_names, out);
            collect_in_subquery_consumer_domains_expr(high, cte_name, cte_names, out);
        }
        Expr::Like { expr, pattern, .. } => {
            collect_in_subquery_consumer_domains_expr(expr, cte_name, cte_names, out);
            collect_in_subquery_consumer_domains_expr(pattern, cte_name, cte_names, out);
        }
        Expr::Tuple(items) => {
            for item in items {
                collect_in_subquery_consumer_domains_expr(item, cte_name, cte_names, out);
            }
        }
        _ => {}
    }
}

fn simple_in_subquery_cte_key(expr: &Expr, cte_name: &str) -> Option<(String, Expr)> {
    let Expr::InSubquery {
        expr,
        subquery,
        negated: false,
    } = expr
    else {
        return None;
    };
    if subquery.from.len() != 1
        || !subquery.from[0].name.eq_ignore_ascii_case(cte_name)
        || subquery.derived.is_some()
        || !subquery.joins.is_empty()
        || subquery.selection.is_some()
        || !subquery.group_by.is_empty()
        || subquery.having.is_some()
        || subquery.distinct
    {
        return None;
    }
    let [SelectItem::Expr {
        expr: key_expr,
        alias: _,
    }] = subquery.projection.as_slice()
    else {
        return None;
    };
    let output_col = expr_output_name(key_expr)?.to_ascii_lowercase();
    Some((output_col, *expr.clone()))
}

fn build_consumer_domain_select(
    consumer: &Select,
    key_expr: Expr,
    cte_names: &[String],
) -> Option<Select> {
    if consumer.from.is_empty()
        || consumer.values.is_some()
        || consumer.json_table.is_some()
        || consumer.for_update
        || consumer.lock_clause.is_some()
    {
        return None;
    }
    let mut domain = consumer.clone();
    domain.projection = vec![SelectItem::Expr {
        expr: key_expr,
        alias: None,
    }];
    domain.group_by.clear();
    domain.grouping_sets.clear();
    domain.group_by_modifier = crate::parser::GroupByModifier::None;
    domain.having = None;
    domain.order_by.clear();
    domain.limit = None;
    domain.offset = None;
    domain.distinct = false;
    domain.sql_calc_found_rows = false;
    domain.named_windows.clear();
    domain.query_hints.clear();
    domain.hint_warnings.clear();
    if let Some(selection) = &consumer.selection {
        let mut kept = Vec::new();
        let mut conjuncts = Vec::new();
        split_and(selection, &mut conjuncts);
        for conjunct in conjuncts {
            if !expr_references_any_cte(&conjunct, cte_names) {
                kept.push(conjunct);
            }
        }
        domain.selection = combine_and(kept);
    }
    if select_references_any_cte(&domain, cte_names) {
        return None;
    }
    Some(domain)
}

fn body_pushable_output_map_for_in_domain(cte: &CteDef) -> Option<HashMap<String, Expr>> {
    if cte_body_is_pure_projection_wrapper(cte) {
        if let Some(map) = body_output_expr_map(cte) {
            return Some(map);
        }
    }
    if cte.is_recursive || !cte.columns.is_empty() {
        return None;
    }
    let crate::parser::QueryExpr::Select(s) = &cte.query else {
        return None;
    };
    if s.group_by.is_empty()
        || !s.grouping_sets.is_empty()
        || s.group_by_modifier != crate::parser::GroupByModifier::None
        || s.distinct
    {
        return None;
    }
    let mut map = HashMap::new();
    for item in &s.projection {
        let SelectItem::Expr { expr, alias } = item else {
            return None;
        };
        if !s.group_by.iter().any(|g| g == expr) {
            continue;
        }
        let name = alias
            .clone()
            .or_else(|| expr_output_name(expr))?
            .to_ascii_lowercase();
        map.insert(name, expr.clone());
    }
    (!map.is_empty()).then_some(map)
}

fn cte_body_is_pure_projection_wrapper(cte: &CteDef) -> bool {
    let crate::parser::QueryExpr::Select(s) = &cte.query else {
        return false;
    };
    s.from.is_empty()
        && s.table.is_none()
        && s.derived.is_none()
        && s.joins.is_empty()
        && s.selection.is_none()
        && s.values.is_none()
        && s.json_table.is_none()
        && s.order_by.is_empty()
        && s.limit.is_none()
        && s.offset.is_none()
        && !s.for_update
        && s.lock_clause.is_none()
}

fn combine_and(mut parts: Vec<Expr>) -> Option<Expr> {
    let first = parts.pop()?;
    Some(parts.into_iter().fold(first, |acc, expr| Expr::Binary {
        left: Box::new(expr),
        op: crate::parser::BinaryOp::And,
        right: Box::new(acc),
    }))
}

fn combine_or(mut parts: Vec<Expr>) -> Option<Expr> {
    let first = parts.pop()?;
    Some(parts.into_iter().fold(first, |acc, expr| Expr::Binary {
        left: Box::new(expr),
        op: crate::parser::BinaryOp::Or,
        right: Box::new(acc),
    }))
}

fn select_references_any_cte(select: &Select, cte_names: &[String]) -> bool {
    cte_names
        .iter()
        .any(|name| select_references_cte(select, name))
}

fn expr_references_any_cte(expr: &Expr, cte_names: &[String]) -> bool {
    cte_names.iter().any(|name| expr_references_cte(expr, name))
}

pub(super) fn push_groupkey_predicates_into_ctes<'q>(
    ctes: &mut [CteDef],
    body: impl Into<crate::parser::QueryRef<'q>>,
) {
    let body = body.into();
    if !pred_pushdown_agg_enabled() || ctes.is_empty() {
        return;
    }
    let crate::parser::QueryRef::Select(outer) = body else {
        return;
    };

    // 1. Outer sources that are CTEs: alias(lower) → index into `ctes`. Any
    //    non-CTE / derived / values source makes *unqualified* column
    //    resolution ambiguous (a base-table column could shadow a CTE one), so
    //    we only resolve unqualified identifiers when every outer source is a
    //    known CTE. Qualified `alias.col` references are always resolvable.
    let sources = CteSourceContext::new(ctes, outer);
    let alias_to_cte = &sources.alias_to_cte;
    if alias_to_cte.is_empty() {
        return;
    }

    // 2. Ownership map for unqualified-identifier resolution: output column
    //    name(lower) → unique owning alias, or `None` when produced by >1
    //    outer CTE source (ambiguous).
    // 3. Equality-equivalence closure over the outer WHERE + every join ON.
    let mut eq = EquivClasses::new();
    let mut node_id: HashMap<(String, String), usize> = HashMap::new();
    // WHERE: constants (seed) + column~column equivalences.
    let mut where_conjuncts: Vec<Expr> = Vec::new();
    if let Some(sel) = &outer.selection {
        split_and(sel, &mut where_conjuncts);
    }
    let resolve = |e: &Expr| sources.resolve(e);
    let intern = |eq: &mut EquivClasses,
                  node_id: &mut HashMap<(String, String), usize>,
                  k: (String, String)|
     -> usize {
        if let Some(&id) = node_id.get(&k) {
            id
        } else {
            let id = eq.make();
            node_id.insert(k, id);
            id
        }
    };
    let absorb_equality = |eq: &mut EquivClasses,
                           node_id: &mut HashMap<(String, String), usize>,
                           l: &Expr,
                           r: &Expr,
                           allow_const: bool| {
        match (resolve(l), resolve(r)) {
            (Some(lc), Some(rc)) => {
                let a = intern(eq, node_id, lc);
                let b = intern(eq, node_id, rc);
                eq.union(a, b);
            }
            (Some(lc), None) if allow_const => {
                if let Some(c) = as_pushable_constant(r) {
                    let a = intern(eq, node_id, lc);
                    eq.set_const(a, c);
                }
            }
            (None, Some(rc)) if allow_const => {
                if let Some(c) = as_pushable_constant(l) {
                    let b = intern(eq, node_id, rc);
                    eq.set_const(b, c);
                }
            }
            _ => {}
        }
    };
    for c in &where_conjuncts {
        if let Expr::Binary {
            op: crate::parser::BinaryOp::Eq,
            left,
            right,
        } = c
        {
            // WHERE equalities may seed constants (post-join filter).
            absorb_equality(&mut eq, &mut node_id, left, right, true);
        }
    }
    for j in &outer.joins {
        if let Some(cond) = &j.condition {
            let mut on_conjuncts = Vec::new();
            split_and(cond, &mut on_conjuncts);
            for c in &on_conjuncts {
                if let Expr::Binary {
                    op: crate::parser::BinaryOp::Eq,
                    left,
                    right,
                } = c
                {
                    // ON equalities only propagate equivalence, never seed a
                    // constant (an ON literal filters matches, not output rows).
                    absorb_equality(&mut eq, &mut node_id, left, right, false);
                }
            }
        }
    }

    // 4. Reference counts across body + sibling bodies: only push into a CTE
    //    referenced exactly once anywhere (a shared/self-joined body would be
    //    over-filtered if different aliases carried different constants).
    let mut ref_count: Vec<usize> = vec![0; ctes.len()];
    for (i, count) in ref_count.iter_mut().enumerate() {
        let mut aliases = Vec::new();
        collect_cte_aliases_stmt(body, &ctes[i].name, &mut aliases);
        for (j, other) in ctes.iter().enumerate() {
            if j != i {
                collect_cte_aliases_stmt(&other.query, &ctes[i].name, &mut aliases);
            }
        }
        *count = aliases.len();
    }

    // 5. For each single-reference outer CTE source, push the class constant of
    //    each pushable output column into its body.
    let pushes: Vec<(usize, String, Vec<Expr>)> = {
        let mut out = Vec::new();
        for (alias, &i) in alias_to_cte {
            if ref_count[i] != 1 {
                continue;
            }
            for col in cte_output_column_names(&ctes[i]) {
                let Some(&id) = node_id.get(&(alias.clone(), col.clone())) else {
                    continue;
                };
                if let Some(k) = eq.class_const(id) {
                    out.push((i, col, vec![k]));
                }
            }
        }
        out
    };
    for (i, col, domain) in pushes {
        push_output_domain_predicate(&mut ctes[i], &col, domain);
    }
}

/// Output column names of a CTE (explicit `WITH name(cols...)` list if present,
/// else derived from the leftmost SELECT's projection). Used only to build the
/// unqualified-identifier ownership map.
fn cte_output_column_names(cte: &CteDef) -> Vec<String> {
    if !cte.columns.is_empty() {
        return cte.columns.clone();
    }
    statement_output_column_names(&cte.query)
}

fn statement_output_column_names<'q>(stmt: impl Into<crate::parser::QueryRef<'q>>) -> Vec<String> {
    let stmt = stmt.into();
    let Some(s) = leftmost_select(stmt) else {
        return Vec::new();
    };
    let mut names = Vec::new();
    for item in &s.projection {
        if let SelectItem::Expr { expr, alias } = item {
            let name = match alias {
                Some(a) => Some(a.clone()),
                None => match expr {
                    Expr::Identifier(n) => Some(n.clone()),
                    Expr::CompoundIdentifier(parts) => parts.last().cloned(),
                    _ => None,
                },
            };
            if let Some(n) = name {
                names.push(n);
            }
        }
    }
    names
}

fn leftmost_select<'q>(stmt: impl Into<crate::parser::QueryRef<'q>>) -> Option<&'q Select> {
    stmt.into().leftmost_select()
}

/// Pushable output-column → body-source-expression map for WS-A/B. For a plain
/// (non-grouped) body this is `body_output_expr_map` (any output column). For a
/// grouped body it is restricted to *group-key* outputs (an equality on a
/// group key is equivalent before/after the aggregate); aggregate outputs are
/// excluded. ROLLUP / GROUPING SETS / HAVING bodies are rejected — a constant
/// pushed before a ROLLUP super-group would wrongly drop its synthetic NULL.
fn body_pushable_output_map(cte: &CteDef) -> Option<HashMap<String, Expr>> {
    if let Some(map) = body_output_expr_map(cte) {
        return Some(map);
    }
    if cte.is_recursive || !cte.columns.is_empty() {
        return None;
    }
    let crate::parser::QueryExpr::Select(s) = &cte.query else {
        return None;
    };
    if s.group_by.is_empty()
        || !s.grouping_sets.is_empty()
        || s.group_by_modifier != crate::parser::GroupByModifier::None
        || s.having.is_some()
        || s.distinct
        || !select_filter_commutes(s)
    {
        return None;
    }
    let mut map = HashMap::new();
    for item in &s.projection {
        let SelectItem::Expr { expr, alias } = item else {
            return None;
        };
        if expr_has_aggregate(expr) {
            continue; // aggregate output — not pushable through the aggregate
        }
        if !s.group_by.iter().any(|g| g == expr) {
            continue; // non-group-key scalar projection — conservatively skip
        }
        let name = match alias {
            Some(a) => a.clone(),
            None => match expr {
                Expr::Identifier(n) => n.clone(),
                Expr::CompoundIdentifier(parts) => parts.last()?.clone(),
                _ => continue,
            },
        };
        map.entry(name.to_ascii_lowercase())
            .or_insert_with(|| expr.clone());
    }
    if map.is_empty() {
        None
    } else {
        Some(map)
    }
}

/// AND a conjunct into a CTE body's WHERE, skipping a structurally identical
/// existing conjunct so repeated/already-pushed predicates don't accumulate.
fn push_body_where_conjunct(cte: &mut CteDef, pred: Expr) {
    let crate::parser::QueryExpr::Select(b) = &mut cte.query else {
        return;
    };
    push_select_where_conjunct(b, pred);
}

fn push_select_where_conjunct(select: &mut Select, pred: Expr) {
    let mut existing = Vec::new();
    if let Some(sel) = &select.selection {
        split_and(sel, &mut existing);
    }
    if existing.iter().any(|e| *e == pred) {
        return;
    }
    select.selection = Some(match select.selection.take() {
        Some(e) => Expr::Binary {
            op: crate::parser::BinaryOp::And,
            left: Box::new(e),
            right: Box::new(pred),
        },
        None => pred,
    });
}

fn push_output_domain_predicate(cte: &mut CteDef, output_col: &str, values: Vec<Expr>) -> bool {
    if values.is_empty() {
        return false;
    }
    if let Some(body_expr) = body_pushable_output_map(cte)
        .and_then(|map| map.get(&output_col.to_ascii_lowercase()).cloned())
    {
        push_body_where_conjunct(cte, domain_predicate(body_expr.clone(), values.clone()));
        // If the grouped/plain body sources this column from a derived
        // set-operation (`FROM (A UNION B) sd`), the conjunct we just added sits
        // *above* the materialised union and never reaches the branches' base
        // tables (it stalls as a post-union filter). Recurse the domain push
        // into each branch so the filter becomes an indexed scan there. This is
        // the TPC-DS Q75 / Q04 / Q11 / Q74 family: a multi-reference grouped CTE
        // whose body aggregates over a UNION of per-channel fact joins.
        if let Some(col) = body_expr_column_name(&body_expr) {
            push_domain_into_body_setop_sources(cte, &col, &values);
        }
        return true;
    }
    push_union_output_domain_predicate(cte, output_col, values)
}

fn push_union_output_domain_predicate(
    cte: &mut CteDef,
    output_col: &str,
    values: Vec<Expr>,
) -> bool {
    if cte.is_recursive || !cte.columns.is_empty() {
        return false;
    }
    push_union_domain_into_stmt(&mut cte.query, output_col, &values)
}

/// Push a domain predicate (`output_col IN values` / `output_col = v`) into every
/// branch of a UNION set-operation statement, mapping the output column to each
/// branch's projection expression at the matching ordinal. No-op (returns false)
/// unless the statement is a pushable UNION exposing `output_col` as a pushable
/// (group-key-safe, plain-column) output in *every* branch.
fn push_union_domain_into_stmt<'q>(
    stmt: impl Into<crate::parser::QueryMut<'q>>,
    output_col: &str,
    values: &[Expr],
) -> bool {
    let stmt = stmt.into();
    let Some(index) = union_output_column_index(stmt.as_ref(), output_col) else {
        return false;
    };
    let mut branch_exprs = Vec::new();
    if !collect_union_branch_pushable_exprs(stmt.as_ref(), index, &mut branch_exprs)
        || branch_exprs.len() < 2
    {
        return false;
    }
    let mut iter = branch_exprs.into_iter();
    apply_union_branch_domain_predicate(stmt, values, &mut iter) > 0
}

/// The bare lowercased column name of a body source expression, if it is a
/// plain column reference — so its values map 1:1 onto a derived source's
/// output column of the same name.
fn body_expr_column_name(expr: &Expr) -> Option<String> {
    match expr {
        Expr::Identifier(n) => Some(n.to_ascii_lowercase()),
        Expr::CompoundIdentifier(parts) => parts.last().map(|p| p.to_ascii_lowercase()),
        _ => None,
    }
}

/// Recurse a domain push into a CTE body's derived set-operation source(s).
/// Pushes only when *exactly one* derived set-op source of the body exposes
/// `output_col` (keeping the column reference unambiguous); a body joining two
/// union-derived sources that both expose the column is left untouched.
fn push_domain_into_body_setop_sources(cte: &mut CteDef, output_col: &str, values: &[Expr]) {
    let crate::parser::QueryExpr::Select(body) = &mut cte.query else {
        return;
    };
    let derived_match = matches!(
        &body.derived,
        Some(SubQuery::SetOp(s)) if union_output_column_index(s, output_col).is_some()
    );
    let join_matches: Vec<usize> = body
        .joins
        .iter()
        .enumerate()
        .filter(|(_, j)| {
            matches!(&j.derived, Some(SubQuery::SetOp(s)) if union_output_column_index(s, output_col).is_some())
        })
        .map(|(i, _)| i)
        .collect();
    if derived_match as usize + join_matches.len() != 1 {
        return;
    }
    if derived_match {
        if let Some(SubQuery::SetOp(s)) = body.derived.as_mut() {
            push_union_domain_into_stmt(&mut **s, output_col, values);
        }
    } else if let Some(&ji) = join_matches.first() {
        if let Some(SubQuery::SetOp(s)) = body.joins[ji].derived.as_mut() {
            push_union_domain_into_stmt(&mut **s, output_col, values);
        }
    }
}

fn union_output_column_index<'q>(
    stmt: impl Into<crate::parser::QueryRef<'q>>,
    output_col: &str,
) -> Option<usize> {
    let stmt = stmt.into();
    let select = leftmost_select(stmt)?;
    for (idx, item) in select.projection.iter().enumerate() {
        let SelectItem::Expr { expr, alias } = item else {
            continue;
        };
        let name = alias.clone().or_else(|| match expr {
            Expr::Identifier(n) => Some(n.clone()),
            Expr::CompoundIdentifier(parts) => parts.last().cloned(),
            _ => None,
        })?;
        if name.eq_ignore_ascii_case(output_col) {
            return Some(idx);
        }
    }
    None
}

fn collect_union_branch_pushable_exprs<'q>(
    stmt: impl Into<crate::parser::QueryRef<'q>>,
    index: usize,
    out: &mut Vec<Expr>,
) -> bool {
    let stmt = stmt.into();
    match stmt {
        crate::parser::QueryRef::Select(select) => {
            let Some(SelectItem::Expr { expr, .. }) = select.projection.get(index) else {
                return false;
            };
            let Some(pushable) = select_pushable_output_expr(select, expr) else {
                return false;
            };
            out.push(pushable);
            true
        }
        crate::parser::QueryRef::SetOperation(set)
            // A deterministic per-row filter commutes with both UNION ALL and
            // UNION DISTINCT (filtering before dedup yields the same set), so
            // domain pushdown is valid for either. Branch-level ORDER BY /
            // LIMIT / OFFSET would change which rows survive, so those still bar
            // the push.
            if set.op == crate::parser::SetOp::Union
                && set.order_by.is_empty()
                && set.limit.is_none()
                && set.offset.is_none() =>
        {
            collect_union_branch_pushable_exprs(&set.left, index, out)
                && collect_union_branch_pushable_exprs(&set.right, index, out)
        }
        _ => false,
    }
}

fn select_pushable_output_expr(select: &Select, expr: &Expr) -> Option<Expr> {
    if select.distinct
        || !select.grouping_sets.is_empty()
        || select.group_by_modifier != crate::parser::GroupByModifier::None
        || select.having.is_some()
        || select.limit.is_some()
        || select.offset.is_some()
        || expr_has_aggregate(expr)
        || expr_is_nondeterministic(expr)
    {
        return None;
    }
    if matches!(expr, Expr::Literal(_) | Expr::BoundValue(_)) {
        return Some(expr.clone());
    }
    let is_direct_column = matches!(expr, Expr::Identifier(_) | Expr::CompoundIdentifier(_));
    if !is_direct_column {
        return None;
    }
    if !select.group_by.is_empty() && !select.group_by.iter().any(|g| g == expr) {
        return None;
    }
    Some(expr.clone())
}

fn apply_union_branch_domain_predicate<'q>(
    stmt: impl Into<crate::parser::QueryMut<'q>>,
    values: &[Expr],
    branch_exprs: &mut std::vec::IntoIter<Expr>,
) -> usize {
    let stmt = stmt.into();
    match stmt {
        crate::parser::QueryMut::Select(select) => {
            let Some(branch_expr) = branch_exprs.next() else {
                return 0;
            };
            let predicate = domain_predicate(branch_expr, values.to_vec());
            push_select_where_conjunct(select, predicate);
            1
        }
        crate::parser::QueryMut::SetOperation(set)
            if set.op == crate::parser::SetOp::Union
                && set.order_by.is_empty()
                && set.limit.is_none()
                && set.offset.is_none() =>
        {
            apply_union_branch_domain_predicate(&mut set.left, values, branch_exprs)
                + apply_union_branch_domain_predicate(&mut set.right, values, branch_exprs)
        }
        _ => 0,
    }
}

/// Map of lowercased output column name → body source expression, for a body
/// that is safe to push a WHERE filter into (plain `SELECT`, no DISTINCT /
/// GROUP BY / aggregate / HAVING / set-op / column-alias-list / wildcard).
/// Can an outer filter on this block's output move into its WHERE? LIMIT /
/// OFFSET choose rows before the outer filter runs, a window function reads
/// rows the filter would remove, and a nondeterministic expression would be
/// evaluated over a different row set.
fn select_filter_commutes(s: &Select) -> bool {
    s.limit.is_none()
        && s.offset.is_none()
        && !super::dispatch::select_has_window_functions(s)
        && !select_has_nondeterministic(s)
}

fn body_output_expr_map(cte: &CteDef) -> Option<HashMap<String, Expr>> {
    if cte.is_recursive || !cte.columns.is_empty() {
        return None;
    }
    let crate::parser::QueryExpr::Select(s) = &cte.query else {
        return None;
    };
    if s.distinct
        || !s.group_by.is_empty()
        || !s.grouping_sets.is_empty()
        || s.group_by_modifier != crate::parser::GroupByModifier::None
        || s.having.is_some()
        || !select_filter_commutes(s)
    {
        return None;
    }
    let mut map = HashMap::new();
    for item in &s.projection {
        let SelectItem::Expr { expr, alias } = item else {
            return None;
        };
        if expr_has_aggregate(expr) {
            return None;
        }
        let name = match alias {
            Some(a) => a.clone(),
            None => match expr {
                Expr::Identifier(n) => n.clone(),
                Expr::CompoundIdentifier(parts) => parts.last()?.clone(),
                _ => return None,
            },
        };
        if map
            .insert(name.to_ascii_lowercase(), expr.clone())
            .is_some()
        {
            return None; // duplicate output name
        }
    }
    Some(map)
}

fn collect_pushable_sites_stmt<'q>(
    stmt: impl Into<crate::parser::QueryRef<'q>>,
    name: &str,
    map: &HashMap<String, Expr>,
    sites: &mut Vec<Vec<Expr>>,
) {
    let stmt = stmt.into();
    match stmt {
        crate::parser::QueryRef::Select(s) => collect_pushable_sites_select(s, name, map, sites),
        crate::parser::QueryRef::SetOperation(s) => {
            collect_pushable_sites_stmt(&s.left, name, map, sites);
            collect_pushable_sites_stmt(&s.right, name, map, sites);
        }
        crate::parser::QueryRef::WithCte(w) => {
            collect_pushable_sites_stmt(&w.body, name, map, sites)
        }
        _ => {}
    }
}

fn collect_pushable_sites_select(
    s: &Select,
    name: &str,
    map: &HashMap<String, Expr>,
    sites: &mut Vec<Vec<Expr>>,
) {
    // Direct FROM/JOIN sources matching the CTE → one (alias) site each.
    let single_source = s.from.len() == 1 && s.joins.is_empty() && s.derived.is_none();
    let mut aliases: Vec<String> = Vec::new();
    for t in &s.from {
        if t.name.eq_ignore_ascii_case(name) {
            aliases.push(t.alias.clone().unwrap_or_else(|| t.name.clone()));
        }
    }
    for j in &s.joins {
        if j.table.name.eq_ignore_ascii_case(name) {
            aliases.push(
                j.table
                    .alias
                    .clone()
                    .unwrap_or_else(|| j.table.name.clone()),
            );
        }
    }
    for alias in aliases {
        let mut conjuncts: Vec<Expr> = Vec::new();
        if let Some(sel) = &s.selection {
            let mut raw = Vec::new();
            split_and(sel, &mut raw);
            for c in &raw {
                if let Some(translated) = translate_single_source(c, &alias, single_source, map) {
                    conjuncts.push(translated);
                }
            }
        }
        sites.push(conjuncts);
    }
    // Recurse into nested selects (derived tables, expression subqueries) so
    // every use of the CTE is represented in the intersection.
    if let Some(sub) = &s.derived {
        collect_pushable_sites_subquery(sub, name, map, sites);
    }
    for j in &s.joins {
        if let Some(sub) = &j.derived {
            collect_pushable_sites_subquery(sub, name, map, sites);
        }
        if let Some(c) = &j.condition {
            collect_pushable_sites_expr(c, name, map, sites);
        }
    }
    for item in &s.projection {
        if let SelectItem::Expr { expr, .. } = item {
            collect_pushable_sites_expr(expr, name, map, sites);
        }
    }
    if let Some(e) = &s.selection {
        collect_pushable_sites_expr(e, name, map, sites);
    }
    if let Some(e) = &s.having {
        collect_pushable_sites_expr(e, name, map, sites);
    }
}

fn collect_pushable_sites_subquery(
    sub: &SubQuery,
    name: &str,
    map: &HashMap<String, Expr>,
    sites: &mut Vec<Vec<Expr>>,
) {
    match sub {
        SubQuery::Select(s) => collect_pushable_sites_select(s, name, map, sites),
        stmt @ (SubQuery::SetOp(_) | SubQuery::WithCte(_)) => {
            collect_pushable_sites_stmt(stmt, name, map, sites)
        }
    }
}

fn collect_pushable_sites_expr(
    expr: &Expr,
    name: &str,
    map: &HashMap<String, Expr>,
    sites: &mut Vec<Vec<Expr>>,
) {
    match expr {
        Expr::Subquery(s)
        | Expr::InSubquery { subquery: s, .. }
        | Expr::Exists { subquery: s, .. } => collect_pushable_sites_select(s, name, map, sites),
        Expr::Binary { left, right, .. } => {
            collect_pushable_sites_expr(left, name, map, sites);
            collect_pushable_sites_expr(right, name, map, sites);
        }
        Expr::Unary { expr, .. }
        | Expr::IsNull(expr)
        | Expr::IsNotNull(expr)
        | Expr::GroupKeyRef { expr, .. } => collect_pushable_sites_expr(expr, name, map, sites),
        Expr::Function { args, .. } => args
            .iter()
            .for_each(|a| collect_pushable_sites_expr(a, name, map, sites)),
        Expr::Case {
            operand,
            conditions,
            results,
            else_result,
        } => {
            if let Some(e) = operand {
                collect_pushable_sites_expr(e, name, map, sites);
            }
            conditions
                .iter()
                .for_each(|c| collect_pushable_sites_expr(c, name, map, sites));
            results
                .iter()
                .for_each(|r| collect_pushable_sites_expr(r, name, map, sites));
            if let Some(e) = else_result {
                collect_pushable_sites_expr(e, name, map, sites);
            }
        }
        Expr::InList { expr, list, .. } => {
            collect_pushable_sites_expr(expr, name, map, sites);
            list.iter()
                .for_each(|e| collect_pushable_sites_expr(e, name, map, sites));
        }
        Expr::Between {
            expr, low, high, ..
        } => {
            collect_pushable_sites_expr(expr, name, map, sites);
            collect_pushable_sites_expr(low, name, map, sites);
            collect_pushable_sites_expr(high, name, map, sites);
        }
        Expr::Like { expr, pattern, .. } => {
            collect_pushable_sites_expr(expr, name, map, sites);
            collect_pushable_sites_expr(pattern, name, map, sites);
        }
        Expr::Tuple(items) => items
            .iter()
            .for_each(|i| collect_pushable_sites_expr(i, name, map, sites)),
        _ => {}
    }
}

/// Split an expression tree on top-level `AND` into conjuncts.
fn split_and(expr: &Expr, out: &mut Vec<Expr>) {
    if let Expr::Binary {
        op: crate::parser::BinaryOp::And,
        left,
        right,
    } = expr
    {
        split_and(left, out);
        split_and(right, out);
    } else {
        out.push(expr.clone());
    }
}

/// Translate a conjunct into the CTE body's own column terms iff every column
/// reference is to the CTE (alias `alias`, or — only when the CTE is the
/// select's sole source — an unqualified output-column name) and contains no
/// subquery. Returns `None` for any reference outside the CTE (so the conjunct
/// is not single-source) or any unmapped/ambiguous column.
fn translate_single_source(
    expr: &Expr,
    alias: &str,
    single_source: bool,
    map: &HashMap<String, Expr>,
) -> Option<Expr> {
    match expr {
        Expr::Identifier(col) => {
            if single_source {
                map.get(&col.to_ascii_lowercase()).cloned()
            } else {
                None // ambiguous: not provably the CTE's column
            }
        }
        Expr::CompoundIdentifier(parts) => {
            if parts.len() == 2 && parts[0].eq_ignore_ascii_case(alias) {
                map.get(&parts[1].to_ascii_lowercase()).cloned()
            } else {
                None // qualified to a different source
            }
        }
        Expr::Literal(_) | Expr::Placeholder(_) => Some(expr.clone()),
        Expr::Binary { op, left, right } => {
            let l = translate_single_source(left, alias, single_source, map)?;
            let r = translate_single_source(right, alias, single_source, map)?;
            Some(Expr::Binary {
                op: *op,
                left: Box::new(l),
                right: Box::new(r),
            })
        }
        Expr::Unary { op, expr } => {
            let e = translate_single_source(expr, alias, single_source, map)?;
            Some(Expr::Unary {
                op: *op,
                expr: Box::new(e),
            })
        }
        Expr::IsNull(e) => Some(Expr::IsNull(Box::new(translate_single_source(
            e,
            alias,
            single_source,
            map,
        )?))),
        Expr::IsNotNull(e) => Some(Expr::IsNotNull(Box::new(translate_single_source(
            e,
            alias,
            single_source,
            map,
        )?))),
        Expr::Between {
            expr,
            low,
            high,
            negated,
        } => Some(Expr::Between {
            expr: Box::new(translate_single_source(expr, alias, single_source, map)?),
            low: Box::new(translate_single_source(low, alias, single_source, map)?),
            high: Box::new(translate_single_source(high, alias, single_source, map)?),
            negated: *negated,
        }),
        Expr::Like {
            expr,
            pattern,
            negated,
            escape_char,
        } => Some(Expr::Like {
            expr: Box::new(translate_single_source(expr, alias, single_source, map)?),
            pattern: Box::new(translate_single_source(pattern, alias, single_source, map)?),
            negated: *negated,
            escape_char: *escape_char,
        }),
        Expr::InList {
            expr,
            list,
            negated,
        } => {
            let e = translate_single_source(expr, alias, single_source, map)?;
            let mut tl = Vec::with_capacity(list.len());
            for it in list {
                tl.push(translate_single_source(it, alias, single_source, map)?);
            }
            Some(Expr::InList {
                expr: Box::new(e),
                list: tl,
                negated: *negated,
            })
        }
        Expr::Function {
            name,
            args,
            distinct,
        } => {
            // Pure scalar functions are fine; aggregates/subqueries are not
            // (and translate_single_source on a subquery arg returns None).
            if function_is_nondeterministic(name) {
                return None;
            }
            let mut ta = Vec::with_capacity(args.len());
            for a in args {
                ta.push(translate_single_source(a, alias, single_source, map)?);
            }
            Some(Expr::Function {
                name: name.clone(),
                args: ta,
                distinct: *distinct,
            })
        }
        // Subqueries, window functions, CASE, tuples, etc. — be conservative.
        _ => None,
    }
}

fn collect_cte_aliases_stmt<'q>(
    stmt: impl Into<crate::parser::QueryRef<'q>>,
    name: &str,
    out: &mut Vec<String>,
) {
    let stmt = stmt.into();
    match stmt {
        crate::parser::QueryRef::Select(s) => collect_cte_aliases_select(s, name, out),
        crate::parser::QueryRef::SetOperation(s) => {
            collect_cte_aliases_stmt(&s.left, name, out);
            collect_cte_aliases_stmt(&s.right, name, out);
        }
        crate::parser::QueryRef::WithCte(w) => collect_cte_aliases_stmt(&w.body, name, out),
        _ => {}
    }
}

fn collect_cte_aliases_select(s: &Select, name: &str, out: &mut Vec<String>) {
    for t in &s.from {
        if t.name.eq_ignore_ascii_case(name) {
            out.push(t.alias.clone().unwrap_or_else(|| t.name.clone()));
        }
    }
    for j in &s.joins {
        if j.table.name.eq_ignore_ascii_case(name) {
            out.push(
                j.table
                    .alias
                    .clone()
                    .unwrap_or_else(|| j.table.name.clone()),
            );
        }
        if let Some(sub) = &j.derived {
            collect_cte_aliases_subquery(sub, name, out);
        }
    }
    if let Some(sub) = &s.derived {
        collect_cte_aliases_subquery(sub, name, out);
    }
    // Expression subqueries can also reference the CTE.
    for item in &s.projection {
        if let SelectItem::Expr { expr, .. } = item {
            collect_cte_aliases_expr(expr, name, out);
        }
    }
    if let Some(e) = &s.selection {
        collect_cte_aliases_expr(e, name, out);
    }
    if let Some(e) = &s.having {
        collect_cte_aliases_expr(e, name, out);
    }
    for j in &s.joins {
        if let Some(c) = &j.condition {
            collect_cte_aliases_expr(c, name, out);
        }
    }
}

fn collect_cte_aliases_subquery(sub: &SubQuery, name: &str, out: &mut Vec<String>) {
    match sub {
        SubQuery::Select(s) => collect_cte_aliases_select(s, name, out),
        stmt @ (SubQuery::SetOp(_) | SubQuery::WithCte(_)) => {
            collect_cte_aliases_stmt(stmt, name, out)
        }
    }
}

fn collect_cte_aliases_expr(expr: &Expr, name: &str, out: &mut Vec<String>) {
    crate::parser::visit::inspect_value_expr(
        expr,
        |expr| match expr {
            Expr::Subquery(select)
            | Expr::InSubquery {
                subquery: select, ..
            }
            | Expr::Exists {
                subquery: select, ..
            } => {
                collect_cte_aliases_select(select, name, out);
                false
            }
            Expr::WindowFunction { .. } | Expr::QuantifiedComparison { .. } => false,
            _ => true,
        },
        |_| {},
    );
}

fn collect_used_stmt<'q>(
    stmt: impl Into<crate::parser::QueryRef<'q>>,
    aliases: &[String],
    used: &mut UsedCols,
) {
    let stmt = stmt.into();
    match stmt {
        crate::parser::QueryRef::Select(s) => collect_used_select(s, aliases, used),
        crate::parser::QueryRef::SetOperation(s) => {
            collect_used_stmt(&s.left, aliases, used);
            collect_used_stmt(&s.right, aliases, used);
        }
        crate::parser::QueryRef::WithCte(w) => collect_used_stmt(&w.body, aliases, used),
        _ => {}
    }
}

fn collect_used_select(s: &Select, aliases: &[String], used: &mut UsedCols) {
    for item in &s.projection {
        match item {
            // A bare `*` expands only over the current SELECT scope. A
            // sibling subquery's `SELECT * FROM other_cte` must not block
            // pruning for this CTE; Q23 has exactly that shape.
            SelectItem::Wildcard => {
                if select_scope_contains_alias(s, aliases) {
                    used.wild = true;
                }
            }
            SelectItem::QualifiedWildcard(q) => {
                if aliases.iter().any(|a| a.eq_ignore_ascii_case(q)) {
                    used.wild = true;
                }
            }
            SelectItem::Expr { expr, .. } => collect_used_expr(expr, aliases, used),
        }
    }
    if let Some(e) = &s.selection {
        collect_used_expr(e, aliases, used);
    }
    if let Some(e) = &s.having {
        collect_used_expr(e, aliases, used);
    }
    for e in &s.group_by {
        collect_used_expr(e, aliases, used);
    }
    for o in &s.order_by {
        collect_used_expr(&o.expr, aliases, used);
    }
    for j in &s.joins {
        if let Some(c) = &j.condition {
            collect_used_expr(c, aliases, used);
        }
        if let Some(sub) = &j.derived {
            collect_used_subquery(sub, aliases, used);
        }
    }
    if let Some(sub) = &s.derived {
        collect_used_subquery(sub, aliases, used);
    }
}

fn select_scope_contains_alias(s: &Select, aliases: &[String]) -> bool {
    s.from
        .iter()
        .any(|t| table_ref_matches_any_alias(t, aliases))
        || s.joins
            .iter()
            .any(|j| table_ref_matches_any_alias(&j.table, aliases))
}

fn table_ref_matches_any_alias(t: &TableRef, aliases: &[String]) -> bool {
    let alias = table_alias_name(t);
    aliases.iter().any(|a| a.eq_ignore_ascii_case(alias))
}

fn collect_used_subquery(sub: &SubQuery, aliases: &[String], used: &mut UsedCols) {
    match sub {
        SubQuery::Select(s) => collect_used_select(s, aliases, used),
        stmt @ (SubQuery::SetOp(_) | SubQuery::WithCte(_)) => {
            collect_used_stmt(stmt, aliases, used)
        }
    }
}

fn collect_used_expr(expr: &Expr, aliases: &[String], used: &mut UsedCols) {
    crate::parser::visit::inspect_value_expr(
        expr,
        |expr| {
            match expr {
                Expr::Identifier(name) => {
                    used.names.insert(name.to_ascii_lowercase());
                }
                Expr::CompoundIdentifier(parts) => {
                    if let Some(last) = parts.last() {
                        used.names.insert(last.to_ascii_lowercase());
                    }
                }
                Expr::Subquery(select)
                | Expr::InSubquery {
                    subquery: select, ..
                }
                | Expr::Exists {
                    subquery: select, ..
                } => {
                    collect_used_select(select, aliases, used);
                    return false;
                }
                Expr::QuantifiedComparison { left, right, .. } => {
                    collect_used_expr(left, aliases, used);
                    match right {
                        QuantifiedComparisonRight::Subquery(select) => {
                            collect_used_select(select, aliases, used)
                        }
                        QuantifiedComparisonRight::List(items) => {
                            for item in items {
                                collect_used_expr(item, aliases, used);
                            }
                        }
                    }
                    return false;
                }
                Expr::WindowFunction { .. } => return false,
                _ => {}
            }
            true
        },
        |_| {},
    );
}

/// Replace the single top-level FROM/JOIN reference to `name` in `body` with a
/// derived table carrying `cte_body`. Returns true on success.
fn try_inline_cte_into_select(
    body: &mut Select,
    name: &str,
    cte_body: &Select,
    no_merge: bool,
) -> bool {
    let make_inner = || {
        let mut inner = cte_body.clone();
        inner.no_merge = inner.no_merge || no_merge;
        SubQuery::Select(Box::new(inner))
    };
    // Primary FROM (only when the body has no explicit derived already — the
    // Select struct holds at most one primary `derived` slot).
    if let Some(first) = body.from.first() {
        if first.name.eq_ignore_ascii_case(name)
            && first.generate_series.is_none()
            && body.derived.is_none()
        {
            let alias = first.alias.clone().unwrap_or_else(|| first.name.clone());
            body.from[0] = placeholder_table_ref(alias);
            body.derived = Some(make_inner());
            return true;
        }
    }
    // JOIN list.
    for join in body.joins.iter_mut() {
        if join.derived.is_none()
            && join.table.name.eq_ignore_ascii_case(name)
            && join.table.generate_series.is_none()
        {
            let alias = join
                .table
                .alias
                .clone()
                .unwrap_or_else(|| join.table.name.clone());
            join.table = placeholder_table_ref(alias);
            join.derived = Some(make_inner());
            return true;
        }
    }
    false
}

fn count_in_subquery_from_refs_stmt<'q>(
    stmt: impl Into<crate::parser::QueryRef<'q>>,
    name: &str,
) -> usize {
    let stmt = stmt.into();
    match stmt {
        crate::parser::QueryRef::Select(select) => count_in_subquery_from_refs_select(select, name),
        crate::parser::QueryRef::SetOperation(set) => {
            count_in_subquery_from_refs_stmt(&set.left, name)
                + count_in_subquery_from_refs_stmt(&set.right, name)
        }
        crate::parser::QueryRef::WithCte(w) => count_in_subquery_from_refs_stmt(&w.body, name),
        _ => 0,
    }
}

fn count_in_subquery_from_refs_select(select: &Select, name: &str) -> usize {
    let mut n = 0;
    if let Some(derived) = &select.derived {
        n += count_in_subquery_from_refs_subquery(derived, name);
    }
    for join in &select.joins {
        if let Some(derived) = &join.derived {
            n += count_in_subquery_from_refs_subquery(derived, name);
        }
        if let Some(cond) = &join.condition {
            n += count_in_subquery_from_refs_expr(cond, name);
        }
    }
    if let Some(sel) = &select.selection {
        n += count_in_subquery_from_refs_expr(sel, name);
    }
    if let Some(h) = &select.having {
        n += count_in_subquery_from_refs_expr(h, name);
    }
    for expr in &select.group_by {
        n += count_in_subquery_from_refs_expr(expr, name);
    }
    for item in &select.order_by {
        n += count_in_subquery_from_refs_expr(&item.expr, name);
    }
    for item in &select.projection {
        if let SelectItem::Expr { expr, .. } = item {
            n += count_in_subquery_from_refs_expr(expr, name);
        }
    }
    n
}

fn count_in_subquery_from_refs_subquery(sub: &SubQuery, name: &str) -> usize {
    match sub {
        SubQuery::Select(select) => count_in_subquery_from_refs_select(select, name),
        stmt @ (SubQuery::SetOp(_) | SubQuery::WithCte(_)) => {
            count_in_subquery_from_refs_stmt(stmt, name)
        }
    }
}

fn count_direct_select_from_refs(select: &Select, name: &str) -> usize {
    let mut n = select
        .from
        .iter()
        .filter(|t| t.name.eq_ignore_ascii_case(name))
        .count();
    n += select
        .joins
        .iter()
        .filter(|j| j.table.name.eq_ignore_ascii_case(name))
        .count();
    n
}

fn count_in_subquery_from_refs_expr(expr: &Expr, name: &str) -> usize {
    match expr {
        Expr::InSubquery { subquery, .. } => {
            count_direct_select_from_refs(subquery, name)
                + count_in_subquery_from_refs_select(subquery, name)
        }
        Expr::Subquery(s) | Expr::Exists { subquery: s, .. } => {
            count_in_subquery_from_refs_select(s, name)
        }
        Expr::QuantifiedComparison { left, right, .. } => {
            count_in_subquery_from_refs_expr(left, name)
                + match right {
                    QuantifiedComparisonRight::Subquery(s) => {
                        count_in_subquery_from_refs_select(s, name)
                    }
                    QuantifiedComparisonRight::List(list) => list
                        .iter()
                        .map(|e| count_in_subquery_from_refs_expr(e, name))
                        .sum(),
                }
        }
        Expr::Binary { left, right, .. } => {
            count_in_subquery_from_refs_expr(left, name)
                + count_in_subquery_from_refs_expr(right, name)
        }
        Expr::Unary { expr, .. }
        | Expr::IsNull(expr)
        | Expr::IsNotNull(expr)
        | Expr::GroupKeyRef { expr, .. } => count_in_subquery_from_refs_expr(expr, name),
        Expr::Function { args, .. } => args
            .iter()
            .map(|a| count_in_subquery_from_refs_expr(a, name))
            .sum(),
        Expr::WindowFunction { args, spec, .. } => {
            let crate::parser::WindowSpec {
                partition_by,
                order_by,
                frame,
                ..
            } = spec.as_ref();

            args.iter()
                .map(|a| count_in_subquery_from_refs_expr(a, name))
                .sum::<usize>()
                + partition_by
                    .iter()
                    .map(|e| count_in_subquery_from_refs_expr(e, name))
                    .sum::<usize>()
                + order_by
                    .iter()
                    .map(|o| count_in_subquery_from_refs_expr(&o.expr, name))
                    .sum::<usize>()
                + frame
                    .as_ref()
                    .map(|f| count_in_subquery_from_refs_frame(f, name))
                    .unwrap_or(0)
        }
        Expr::Case {
            operand,
            conditions,
            results,
            else_result,
        } => {
            operand
                .as_ref()
                .map(|e| count_in_subquery_from_refs_expr(e, name))
                .unwrap_or(0)
                + conditions
                    .iter()
                    .map(|e| count_in_subquery_from_refs_expr(e, name))
                    .sum::<usize>()
                + results
                    .iter()
                    .map(|e| count_in_subquery_from_refs_expr(e, name))
                    .sum::<usize>()
                + else_result
                    .as_ref()
                    .map(|e| count_in_subquery_from_refs_expr(e, name))
                    .unwrap_or(0)
        }
        Expr::InList { expr, list, .. } => {
            count_in_subquery_from_refs_expr(expr, name)
                + list
                    .iter()
                    .map(|e| count_in_subquery_from_refs_expr(e, name))
                    .sum::<usize>()
        }
        Expr::Between {
            expr, low, high, ..
        } => {
            count_in_subquery_from_refs_expr(expr, name)
                + count_in_subquery_from_refs_expr(low, name)
                + count_in_subquery_from_refs_expr(high, name)
        }
        Expr::Like { expr, pattern, .. } => {
            count_in_subquery_from_refs_expr(expr, name)
                + count_in_subquery_from_refs_expr(pattern, name)
        }
        Expr::Tuple(items) => items
            .iter()
            .map(|e| count_in_subquery_from_refs_expr(e, name))
            .sum(),
        _ => 0,
    }
}

fn count_in_subquery_from_refs_frame(frame: &crate::parser::WindowFrame, name: &str) -> usize {
    count_frame_bound_refs(&frame.start, name) + count_frame_bound_refs(&frame.end, name)
}

fn inline_cte_into_in_subqueries_stmt<'q>(
    stmt: impl Into<crate::parser::QueryMut<'q>>,
    name: &str,
    cte_body: &Select,
) -> usize {
    let stmt = stmt.into();
    match stmt {
        crate::parser::QueryMut::Select(select) => {
            inline_cte_into_in_subqueries_select(select, name, cte_body)
        }
        crate::parser::QueryMut::SetOperation(set) => {
            inline_cte_into_in_subqueries_stmt(&mut set.left, name, cte_body)
                + inline_cte_into_in_subqueries_stmt(&mut set.right, name, cte_body)
        }
        crate::parser::QueryMut::WithCte(mut w) => {
            inline_cte_into_in_subqueries_stmt(&mut w.body, name, cte_body)
        }
        _ => 0,
    }
}

fn inline_cte_into_in_subqueries_select(
    select: &mut Select,
    name: &str,
    cte_body: &Select,
) -> usize {
    let mut n = 0;
    if let Some(derived) = &mut select.derived {
        n += inline_cte_into_in_subqueries_subquery(derived, name, cte_body);
    }
    for join in &mut select.joins {
        if let Some(derived) = &mut join.derived {
            n += inline_cte_into_in_subqueries_subquery(derived, name, cte_body);
        }
        if let Some(cond) = &mut join.condition {
            n += inline_cte_into_in_subqueries_expr(cond, name, cte_body);
        }
    }
    if let Some(sel) = &mut select.selection {
        n += inline_cte_into_in_subqueries_expr(sel, name, cte_body);
    }
    if let Some(h) = &mut select.having {
        n += inline_cte_into_in_subqueries_expr(h, name, cte_body);
    }
    for expr in &mut select.group_by {
        n += inline_cte_into_in_subqueries_expr(expr, name, cte_body);
    }
    for item in &mut select.order_by {
        n += inline_cte_into_in_subqueries_expr(&mut item.expr, name, cte_body);
    }
    for item in &mut select.projection {
        if let SelectItem::Expr { expr, .. } = item {
            n += inline_cte_into_in_subqueries_expr(expr, name, cte_body);
        }
    }
    n
}

fn inline_cte_into_in_subqueries_subquery(
    sub: &mut SubQuery,
    name: &str,
    cte_body: &Select,
) -> usize {
    match sub {
        SubQuery::Select(select) => inline_cte_into_in_subqueries_select(select, name, cte_body),
        stmt @ (SubQuery::SetOp(_) | SubQuery::WithCte(_)) => {
            inline_cte_into_in_subqueries_stmt(stmt, name, cte_body)
        }
    }
}

fn inline_cte_into_in_subqueries_expr(expr: &mut Expr, name: &str, cte_body: &Select) -> usize {
    match expr {
        Expr::InSubquery { subquery, .. } => {
            let mut n = 0;
            if try_inline_cte_into_select(subquery, name, cte_body, true) {
                n += 1;
            }
            n + inline_cte_into_in_subqueries_select(subquery, name, cte_body)
        }
        Expr::Subquery(s) | Expr::Exists { subquery: s, .. } => {
            inline_cte_into_in_subqueries_select(s, name, cte_body)
        }
        Expr::QuantifiedComparison { left, right, .. } => {
            inline_cte_into_in_subqueries_expr(left, name, cte_body)
                + match right {
                    QuantifiedComparisonRight::Subquery(s) => {
                        inline_cte_into_in_subqueries_select(s, name, cte_body)
                    }
                    QuantifiedComparisonRight::List(list) => list
                        .iter_mut()
                        .map(|e| inline_cte_into_in_subqueries_expr(e, name, cte_body))
                        .sum(),
                }
        }
        Expr::Binary { left, right, .. } => {
            inline_cte_into_in_subqueries_expr(left, name, cte_body)
                + inline_cte_into_in_subqueries_expr(right, name, cte_body)
        }
        Expr::Unary { expr, .. }
        | Expr::IsNull(expr)
        | Expr::IsNotNull(expr)
        | Expr::GroupKeyRef { expr, .. } => {
            inline_cte_into_in_subqueries_expr(expr, name, cte_body)
        }
        Expr::Function { args, .. } => args
            .iter_mut()
            .map(|a| inline_cte_into_in_subqueries_expr(a, name, cte_body))
            .sum(),
        Expr::WindowFunction { args, spec, .. } => {
            let crate::parser::WindowSpec {
                partition_by,
                order_by,
                ..
            } = spec.as_mut();

            args.iter_mut()
                .map(|a| inline_cte_into_in_subqueries_expr(a, name, cte_body))
                .sum::<usize>()
                + partition_by
                    .iter_mut()
                    .map(|e| inline_cte_into_in_subqueries_expr(e, name, cte_body))
                    .sum::<usize>()
                + order_by
                    .iter_mut()
                    .map(|o| inline_cte_into_in_subqueries_expr(&mut o.expr, name, cte_body))
                    .sum::<usize>()
        }
        Expr::Case {
            operand,
            conditions,
            results,
            else_result,
        } => {
            operand
                .as_mut()
                .map(|e| inline_cte_into_in_subqueries_expr(e, name, cte_body))
                .unwrap_or(0)
                + conditions
                    .iter_mut()
                    .map(|e| inline_cte_into_in_subqueries_expr(e, name, cte_body))
                    .sum::<usize>()
                + results
                    .iter_mut()
                    .map(|e| inline_cte_into_in_subqueries_expr(e, name, cte_body))
                    .sum::<usize>()
                + else_result
                    .as_mut()
                    .map(|e| inline_cte_into_in_subqueries_expr(e, name, cte_body))
                    .unwrap_or(0)
        }
        Expr::InList { expr, list, .. } => {
            inline_cte_into_in_subqueries_expr(expr, name, cte_body)
                + list
                    .iter_mut()
                    .map(|e| inline_cte_into_in_subqueries_expr(e, name, cte_body))
                    .sum::<usize>()
        }
        Expr::Between {
            expr, low, high, ..
        } => {
            inline_cte_into_in_subqueries_expr(expr, name, cte_body)
                + inline_cte_into_in_subqueries_expr(low, name, cte_body)
                + inline_cte_into_in_subqueries_expr(high, name, cte_body)
        }
        Expr::Like { expr, pattern, .. } => {
            inline_cte_into_in_subqueries_expr(expr, name, cte_body)
                + inline_cte_into_in_subqueries_expr(pattern, name, cte_body)
        }
        Expr::Tuple(items) => items
            .iter_mut()
            .map(|e| inline_cte_into_in_subqueries_expr(e, name, cte_body))
            .sum(),
        _ => 0,
    }
}

fn placeholder_table_ref(alias: String) -> TableRef {
    TableRef {
        name: alias.clone(),
        alias: Some(alias),
        system_time: None,
        index_hints: vec![],
        partitions: vec![],
        column_aliases: Vec::new(),
        generate_series: None,
    }
}

/// Functions whose value is not a pure function of their arguments. Inlining a
/// body containing one of these must not let flatten duplicate it across outer
/// references, so we force `no_merge` (materialise once).
fn function_is_nondeterministic(name: &str) -> bool {
    matches!(
        name.to_ascii_uppercase().as_str(),
        "RAND"
            | "NOW"
            | "CURRENT_TIMESTAMP"
            | "LOCALTIME"
            | "LOCALTIMESTAMP"
            | "SYSDATE"
            | "CURDATE"
            | "CURRENT_DATE"
            | "CURTIME"
            | "CURRENT_TIME"
            | "UNIX_TIMESTAMP"
            | "UTC_TIMESTAMP"
            | "UTC_DATE"
            | "UTC_TIME"
            | "UUID"
            | "UUID_SHORT"
            | "LAST_INSERT_ID"
            | "CONNECTION_ID"
            | "USER"
            | "CURRENT_USER"
            | "SESSION_USER"
            | "SYSTEM_USER"
            | "DATABASE"
            | "SCHEMA"
            | "VERSION"
            | "RANDOM_BYTES"
            | "SLEEP"
            | "GET_LOCK"
            | "RELEASE_LOCK"
            | "BENCHMARK"
            | "LOAD_FILE"
    )
}

fn select_has_nondeterministic(s: &Select) -> bool {
    s.projection.iter().any(|item| match item {
        SelectItem::Expr { expr, .. } => expr_is_nondeterministic(expr),
        _ => false,
    }) || s.selection.as_ref().is_some_and(expr_is_nondeterministic)
        || s.having.as_ref().is_some_and(expr_is_nondeterministic)
        || s.group_by.iter().any(expr_is_nondeterministic)
        || s.order_by.iter().any(|o| expr_is_nondeterministic(&o.expr))
        || s.joins
            .iter()
            .any(|j| j.condition.as_ref().is_some_and(expr_is_nondeterministic))
}

pub(in crate::engine::executor) fn expr_is_nondeterministic(expr: &Expr) -> bool {
    match expr {
        Expr::Function { name, args, .. } => {
            function_is_nondeterministic(name) || args.iter().any(expr_is_nondeterministic)
        }
        Expr::WindowFunction {
            name, args, spec, ..
        } => {
            let crate::parser::WindowSpec {
                partition_by,
                order_by,
                ..
            } = spec.as_ref();

            function_is_nondeterministic(name)
                || args.iter().any(expr_is_nondeterministic)
                || partition_by.iter().any(expr_is_nondeterministic)
                || order_by.iter().any(|o| expr_is_nondeterministic(&o.expr))
        }
        Expr::Binary { left, right, .. } => {
            expr_is_nondeterministic(left) || expr_is_nondeterministic(right)
        }
        Expr::Unary { expr, .. }
        | Expr::IsNull(expr)
        | Expr::IsNotNull(expr)
        | Expr::GroupKeyRef { expr, .. } => expr_is_nondeterministic(expr),
        Expr::Case {
            operand,
            conditions,
            results,
            else_result,
        } => {
            operand
                .as_ref()
                .is_some_and(|e| expr_is_nondeterministic(e))
                || conditions.iter().any(expr_is_nondeterministic)
                || results.iter().any(expr_is_nondeterministic)
                || else_result
                    .as_ref()
                    .is_some_and(|e| expr_is_nondeterministic(e))
        }
        Expr::InList { expr, list, .. } => {
            expr_is_nondeterministic(expr) || list.iter().any(expr_is_nondeterministic)
        }
        Expr::Between {
            expr, low, high, ..
        } => {
            expr_is_nondeterministic(expr)
                || expr_is_nondeterministic(low)
                || expr_is_nondeterministic(high)
        }
        Expr::Like { expr, pattern, .. } => {
            expr_is_nondeterministic(expr) || expr_is_nondeterministic(pattern)
        }
        Expr::Tuple(items) => items.iter().any(expr_is_nondeterministic),
        // A subquery's volatility is its own concern; conservatively treat it as
        // pure here (it is materialised/decorrelated independently).
        _ => false,
    }
}

// --- reference counting (mirrors the `*_references_cte` traversal) -----------

fn count_statement_refs<'q>(stmt: impl Into<crate::parser::QueryRef<'q>>, name: &str) -> usize {
    let stmt = stmt.into();
    match stmt {
        crate::parser::QueryRef::Select(select) => count_select_refs(select, name),
        crate::parser::QueryRef::SetOperation(s) => {
            count_statement_refs(&s.left, name) + count_statement_refs(&s.right, name)
        }
        crate::parser::QueryRef::WithCte(w) => {
            if let Some(shadow) = w
                .ctes
                .iter()
                .find(|cte| cte.name.eq_ignore_ascii_case(name))
            {
                if shadow.is_recursive {
                    0
                } else {
                    count_statement_refs(&shadow.query, name)
                }
            } else {
                w.ctes
                    .iter()
                    .map(|cte| count_statement_refs(&cte.query, name))
                    .sum::<usize>()
                    + count_statement_refs(&w.body, name)
            }
        }
        _ => 0,
    }
}

fn count_subquery_refs(sub: &SubQuery, name: &str) -> usize {
    match sub {
        SubQuery::Select(select) => count_select_refs(select, name),
        stmt @ (SubQuery::SetOp(_) | SubQuery::WithCte(_)) => count_statement_refs(stmt, name),
    }
}

fn count_select_refs(select: &Select, name: &str) -> usize {
    let mut n = 0;
    if let Some(derived) = &select.derived {
        n += count_subquery_refs(derived, name);
    }
    for table in &select.from {
        if table.name.eq_ignore_ascii_case(name) {
            n += 1;
        }
    }
    for join in &select.joins {
        if join.table.name.eq_ignore_ascii_case(name) {
            n += 1;
        }
        if let Some(derived) = &join.derived {
            n += count_subquery_refs(derived, name);
        }
        if let Some(cond) = &join.condition {
            n += count_expr_refs(cond, name);
        }
    }
    for expr in &select.group_by {
        n += count_expr_refs(expr, name);
    }
    for item in &select.order_by {
        n += count_expr_refs(&item.expr, name);
    }
    if let Some(sel) = &select.selection {
        n += count_expr_refs(sel, name);
    }
    if let Some(h) = &select.having {
        n += count_expr_refs(h, name);
    }
    for item in &select.projection {
        if let SelectItem::Expr { expr, .. } = item {
            n += count_expr_refs(expr, name);
        }
    }
    n
}

fn count_expr_refs(expr: &Expr, name: &str) -> usize {
    match expr {
        Expr::Subquery(s)
        | Expr::InSubquery { subquery: s, .. }
        | Expr::Exists { subquery: s, .. } => count_select_refs(s, name),
        Expr::QuantifiedComparison { left, right, .. } => {
            count_expr_refs(left, name)
                + match right {
                    QuantifiedComparisonRight::Subquery(s) => count_select_refs(s, name),
                    QuantifiedComparisonRight::List(list) => {
                        list.iter().map(|e| count_expr_refs(e, name)).sum()
                    }
                }
        }
        Expr::Binary { left, right, .. } => {
            count_expr_refs(left, name) + count_expr_refs(right, name)
        }
        Expr::Unary { expr, .. }
        | Expr::IsNull(expr)
        | Expr::IsNotNull(expr)
        | Expr::GroupKeyRef { expr, .. } => count_expr_refs(expr, name),
        Expr::Function { args, .. } => args.iter().map(|a| count_expr_refs(a, name)).sum(),
        Expr::WindowFunction { args, spec, .. } => {
            let crate::parser::WindowSpec {
                partition_by,
                order_by,
                frame,
                ..
            } = spec.as_ref();

            args.iter().map(|a| count_expr_refs(a, name)).sum::<usize>()
                + partition_by
                    .iter()
                    .map(|e| count_expr_refs(e, name))
                    .sum::<usize>()
                + order_by
                    .iter()
                    .map(|o| count_expr_refs(&o.expr, name))
                    .sum::<usize>()
                + frame
                    .as_ref()
                    .map(|f| count_frame_refs(f, name))
                    .unwrap_or(0)
        }
        Expr::Case {
            operand,
            conditions,
            results,
            else_result,
        } => {
            operand
                .as_ref()
                .map(|e| count_expr_refs(e, name))
                .unwrap_or(0)
                + conditions
                    .iter()
                    .map(|c| count_expr_refs(c, name))
                    .sum::<usize>()
                + results
                    .iter()
                    .map(|r| count_expr_refs(r, name))
                    .sum::<usize>()
                + else_result
                    .as_ref()
                    .map(|e| count_expr_refs(e, name))
                    .unwrap_or(0)
        }
        Expr::InList { expr, list, .. } => {
            count_expr_refs(expr, name)
                + list.iter().map(|e| count_expr_refs(e, name)).sum::<usize>()
        }
        Expr::Between {
            expr, low, high, ..
        } => count_expr_refs(expr, name) + count_expr_refs(low, name) + count_expr_refs(high, name),
        Expr::Like { expr, pattern, .. } => {
            count_expr_refs(expr, name) + count_expr_refs(pattern, name)
        }
        Expr::Tuple(items) => items.iter().map(|i| count_expr_refs(i, name)).sum(),
        _ => 0,
    }
}

fn count_frame_refs(frame: &crate::parser::WindowFrame, name: &str) -> usize {
    count_frame_bound_refs(&frame.start, name) + count_frame_bound_refs(&frame.end, name)
}

fn count_frame_bound_refs(bound: &crate::parser::WindowFrameBound, name: &str) -> usize {
    match bound {
        crate::parser::WindowFrameBound::Preceding(expr)
        | crate::parser::WindowFrameBound::Following(expr) => count_expr_refs(expr, name),
        _ => 0,
    }
}

#[cfg(test)]
mod spill_builder_reservation_tests {
    use super::*;

    #[test]
    fn prepaid_cte_reservations_track_rows_and_release_on_spill() {
        let dir = tempfile::tempdir().unwrap();
        let columns = vec![crate::types::Column {
            name: "v".into(),
            column_type: crate::types::ColumnType::LongLong,
            source: Default::default(),
        }];
        let mut builder = CteSpillBuilder::new(columns, dir.path().to_path_buf());
        builder.threshold = 128 * 1024;
        for value in 0..100 {
            builder.push(vec![Value::Int(value)]).unwrap();
            let reserved = builder.memory_charge.as_ref().unwrap().bytes();
            assert_eq!(reserved, builder.buffered_bytes + builder.unspent_bytes);
            assert!(reserved <= builder.threshold);
        }
        assert!(builder.unspent_bytes > 0);
        builder.start_spill().unwrap();
        assert_eq!(builder.memory_charge.as_ref().unwrap().bytes(), 0);
        assert_eq!(builder.unspent_bytes, 0);
        assert_eq!(builder.buffered_bytes, 0);
        builder.push(vec![Value::Int(100)]).unwrap();
        let result = builder.finish().unwrap();
        assert_eq!(result.estimated_rows, 101.0);
        assert!(result.spill.is_some());
    }
}

#[cfg(test)]
mod spill_contract_regressions {
    use super::*;
    use crate::engine::executor::mem_budget::{with_query_budget, MemBudget};

    #[test]
    fn unfinished_cte_spill_drop_releases_buffer_and_disk_quota() {
        std::fs::create_dir_all("/tmp/mydb").unwrap();
        let root = tempfile::tempdir_in("/tmp/mydb").unwrap();
        let budget = Arc::new(MemBudget::new(8 * 1024 * 1024));
        with_query_budget(budget.clone(), || {
            let mut builder = CteSpillBuilder::new(
                vec![crate::types::Column {
                    name: "v".into(),
                    column_type: crate::types::ColumnType::VarString,
                    source: Default::default(),
                }],
                root.path().to_path_buf(),
            );
            builder.threshold = 0;
            for _ in 0..64 {
                builder
                    .push(vec![Value::String("buffered tail".repeat(8))])
                    .unwrap();
            }
            let path = builder.spill.as_ref().unwrap().dir.path().to_path_buf();
            drop(builder);
            assert!(!path.exists());
            assert_eq!(budget.used(), 0);
            assert_eq!(budget.spill_bytes(), 0);
        });
    }
}
