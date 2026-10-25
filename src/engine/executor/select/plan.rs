use super::super::*;
use super::bloom::BloomFilter;
use super::eval::{precompute_order_keys, CompiledPlanPredicate};
use super::grace::{io_err, LazyPartitionWriterPool, SpillTmpDir};
use super::*;
pub(crate) use crate::engine::expr_program::CompiledNumericExpr;
use crate::engine::expr_program::{ExprCompiler, InputSchema};
use crate::planner::{
    AdaptiveJoinCandidate, AdaptiveJoinChoice, IndexAggOp, PlanNodeId, RuntimeFilter,
};
use crate::storage::schema::KeyPart;
use crate::storage::{HeapHandle, IndexHandle};
use crate::util::fast_hash::{
    ah_hash_map, ah_hash_map_with_capacity, ah_hash_set, AhHashMap, AhHashSet,
};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};

mod window;
#[cfg(test)]
pub(in crate::engine::executor) use window::WINDOW_RANK_PREFILTER_REJECTED;
pub(super) use window::*;
mod fulltext_source;
mod scan;
#[cfg(test)]
mod scan_routing_tests;
pub(crate) use scan::*;
mod nested_loop;
pub(super) use nested_loop::*;
mod index_join;
pub(crate) use index_join::*;
mod hash_join;
pub(super) use hash_join::*;
mod ordered;
pub(super) use ordered::*;
mod predicate;
pub(crate) use predicate::*;
mod aggregation;
pub(super) use aggregation::*;
mod output;
mod projected_distinct;
pub(crate) use output::*;

mod adaptive;
#[cfg(test)]
mod aggregate_tests;
mod inlj_pair;
use inlj_pair::emit_inlj_pair;
#[cfg(test)]
mod inlj_pair_tests;
mod inlj_read;
mod rollup;
mod row_aggregate;
use adaptive::*;
use inlj_read::{read_inlj_probe_rows_parallel, read_inlj_probe_rows_predicate_first_parallel};
pub(in crate::engine::executor::select) use rollup::*;
use row_aggregate::{RowAggregateGroup, RowAggregateGroups, RowAggregateProgram};

pub(crate) use super::row_schema::RowSchema;
use super::row_schema::RowSchemaRef;
pub(super) use super::row_schema::{field_lookup_hasher, get_field_qualified_ci, FieldLookup};

thread_local! {
    static STRICT_PLAN_ROW_OUTPUT_ACCOUNTING: std::cell::Cell<bool> =
        const { std::cell::Cell::new(false) };
    static ROLLUP_WINDOW_TOPK_HINT: std::cell::RefCell<Option<RollupWindowTopKHint>> =
        const { std::cell::RefCell::new(None) };
    /// Stack of per-operator-body transient-materialization charge
    /// accumulators. Each [`MaterializationChargeScope`] pushes a frame
    /// on enter and, on drop, releases everything charged into the frame
    /// back to the current query budget.
    ///
    /// This is what makes intermediate join / NLJ / scan output
    /// accounting *peak-concurrent* rather than *cumulative-ever*: a
    /// hash join's materialized output is charged while the join is
    /// building it (so a runaway cartesian product still trips the
    /// budget mid-build) and released when the producing operator body
    /// returns — before the next sequential operator (or the next CTE
    /// body) runs. Without this, sequential pipeline stages each leave
    /// their charge behind and the budget monotonically climbs even
    /// though only one intermediate is live at a time (the false-1037
    /// bug on tiny TPC-DS data).
    static MATERIALIZATION_CHARGE_STACK: std::cell::RefCell<Vec<crate::engine::executor::mem_budget::ScopedCharge>> =
        const { std::cell::RefCell::new(Vec::new()) };
    /// Prevents the post-probe reader from opening another worker fan-out
    /// inside an already partitioned INLJ outer worker.
    static INLJ_OUTER_WORKER_DEPTH: std::cell::Cell<u32> = const { std::cell::Cell::new(0) };
}

struct InljOuterWorkerGuard;

impl InljOuterWorkerGuard {
    fn enter() -> Self {
        INLJ_OUTER_WORKER_DEPTH.with(|depth| depth.set(depth.get().saturating_add(1)));
        Self
    }
}

impl Drop for InljOuterWorkerGuard {
    fn drop(&mut self) {
        INLJ_OUTER_WORKER_DEPTH.with(|depth| depth.set(depth.get().saturating_sub(1)));
    }
}

fn in_inlj_outer_worker() -> bool {
    INLJ_OUTER_WORKER_DEPTH.with(|depth| depth.get() > 0)
}

#[derive(Clone)]
pub(crate) struct RollupWindowTopKHint {
    pub(crate) partition_by: Vec<Expr>,
    pub(crate) order_by: Vec<OrderBy>,
    pub(crate) k: usize,
    pub(crate) kind: crate::engine::executor::dispatch::RankKind,
}

pub(crate) struct RollupWindowTopKGuard(Option<RollupWindowTopKHint>);

pub(crate) fn set_rollup_window_topk_hint(
    hint: Option<RollupWindowTopKHint>,
) -> RollupWindowTopKGuard {
    RollupWindowTopKGuard(ROLLUP_WINDOW_TOPK_HINT.with(|slot| slot.replace(hint)))
}

impl Drop for RollupWindowTopKGuard {
    fn drop(&mut self) {
        ROLLUP_WINDOW_TOPK_HINT.with(|slot| *slot.borrow_mut() = self.0.take());
    }
}

pub(in crate::engine::executor) fn current_rollup_window_topk_hint() -> Option<RollupWindowTopKHint>
{
    ROLLUP_WINDOW_TOPK_HINT.with(|slot| slot.borrow().clone())
}

/// RAII guard for a transient-materialization charge frame. Open one at
/// the top of an operator body that builds and returns an intermediate
/// `Vec<PlanRow>` (or runs worker threads that do); all
/// [`charge_transient_materialization`] charges issued while it is live
/// are released when it drops.
struct MaterializationChargeScope;

impl MaterializationChargeScope {
    fn enter() -> Self {
        MATERIALIZATION_CHARGE_STACK.with(|s| {
            s.borrow_mut()
                .push(crate::engine::executor::mem_budget::ScopedCharge::new(0))
        });
        MaterializationChargeScope
    }
}

impl Drop for MaterializationChargeScope {
    fn drop(&mut self) {
        // The frame captures its query owner at `enter`, so nested query
        // contexts and panic unwind cannot release against whichever TLS
        // budget happens to be current now.
        MATERIALIZATION_CHARGE_STACK.with(|s| drop(s.borrow_mut().pop()));
    }
}

/// Charge `bytes` of transient intermediate materialization against the
/// current query budget *and* record it in the active materialization
/// scope so it is released when that scope exits. If no scope is active
/// the charge is skipped: with no defined release point a charge would
/// leak into the budget counter, and the final result-set hand-off in
/// `execute_physical_select_inner` already accounts for the rows a
/// caller actually keeps.
fn charge_transient_materialization(bytes: usize) -> Result<(), QueryError> {
    if bytes == 0 {
        return Ok(());
    }
    MATERIALIZATION_CHARGE_STACK.with(|s| match s.borrow_mut().last_mut() {
        Some(frame) => frame.add(bytes),
        None => Ok(()),
    })
}

/// Move a speculative output reservation into the active materialization
/// frame.  No budget operation occurs here: the bytes were charged while the
/// speculative path was producing rows.  With no enclosing frame the guard is
/// intentionally dropped and releases its reservation, matching
/// `charge_transient_materialization`'s no-owner behavior.
pub(super) fn commit_transient_materialization_charge(
    mut charge: crate::engine::executor::mem_budget::ScopedCharge,
) {
    MATERIALIZATION_CHARGE_STACK.with(|stack| {
        if let Some(frame) = stack.borrow_mut().last_mut() {
            frame.absorb(&mut charge);
        }
    });
}

fn with_strict_plan_row_output_accounting<T>(
    enabled: bool,
    f: impl FnOnce() -> Result<T, QueryError>,
) -> Result<T, QueryError> {
    if !enabled {
        return f();
    }
    STRICT_PLAN_ROW_OUTPUT_ACCOUNTING.with(|flag| {
        let prev = flag.replace(true);
        let result = f();
        flag.set(prev);
        result
    })
}

fn strict_plan_row_output_accounting_enabled() -> bool {
    STRICT_PLAN_ROW_OUTPUT_ACCOUNTING.with(|flag| flag.get())
}

pub(crate) static HASH_JOIN_BLOOM_PROBE_FILTERED: AtomicU64 = AtomicU64::new(0);

// inline=1: single-column joins (`a.id = b.id`, FK lookups, the vast
// majority of BenchmarkSQL / sysbench / TPC-H joins) match the inline
// slot exactly; composite keys spill to one heap alloc per build/probe
// row. With inline=2 the struct was 88 bytes on the stack (and 80 bytes
// of dead inline storage when probing single-column joins), inflating
// every HashMap bucket and probe stack frame. See smallvec_vec_bench.
pub(super) type HashJoinKey = smallvec::SmallVec<[ValueKey; 1]>;

#[derive(Debug, Clone, Default)]
pub(super) struct PlanRow {
    pub(super) fields: Vec<PlanField>,
    pub(super) group: Option<Vec<PlanRow>>,
    /// Pre-computed aggregate values keyed by canonical aggregate
    /// expression text. Populated by `execute_hash_aggregate` (and the
    /// CTE-join GROUP BY path) so projection / HAVING / ORDER BY can
    /// look up aggregate results without iterating the original group
    /// rows. See `super::aggregate::aggregate_key`.
    pub(super) agg_values: Option<std::sync::Arc<std::collections::HashMap<u64, Value>>>,
    /// Shared immutable metadata and lazily-built field lookup. Values still
    /// live in `fields` during the staged row-layout migration; constructors
    /// populate `RowSchema::metas` so metadata no longer needs rebuilding.
    pub(super) schema: RowSchemaRef,
}

impl crate::engine::executor::memory_footprint::AllocatedBytes for PlanRow {
    fn allocated_bytes(&self) -> usize {
        super::vec_bridge::plan_row_owned_heap_bytes(self)
    }
}

impl PlanRow {
    /// Construct a plain row with a deferred schema. Hot producers should
    /// replace it with one operator-owned schema through `PlanRowBuilder`;
    /// the deferred form preserves correctness during staged migration.
    pub(super) fn from_fields(fields: Vec<PlanField>) -> Self {
        Self {
            fields,
            schema: RowSchemaRef::deferred(),
            ..Default::default()
        }
    }

    pub(super) fn builder(fields: Vec<PlanField>) -> PlanRowBuilder {
        PlanRowBuilder {
            row: Self::from_fields(fields),
        }
    }

    /// Append a field and invalidate the cache. The only sanctioned
    /// in-place fields mutation; direct `row.fields.push(...)` will
    /// leave stale schema metadata behind.
    pub(super) fn push_field(&mut self, field: PlanField) {
        self.fields.push(field);
        self.schema = RowSchemaRef::deferred();
    }

    pub(super) fn empty_schema() -> RowSchemaRef {
        RowSchemaRef::deferred()
    }
}

pub(super) struct PlanRowBuilder {
    row: PlanRow,
}

impl PlanRowBuilder {
    pub(super) fn schema(mut self, schema: std::sync::Arc<RowSchema>) -> Self {
        self.row.schema = schema.into();
        self
    }

    pub(super) fn build(self) -> PlanRow {
        self.row
    }
}

/// Column-name prefix for the hidden BM25 relevance pseudo-column a
/// `FulltextScan` injects per row (`__ft_score_<fingerprint>`). The
/// fingerprint ties the value back to the originating `MATCH … AGAINST`
/// expression so WHERE / SELECT / ORDER BY occurrences read the same
/// score. These fields must never surface through `*` / `t.*`.
pub(super) const FT_SCORE_PREFIX: &str = "__ft_score_";

/// True for a hidden full-text score pseudo-column; used to exclude it
/// from wildcard expansion and declared result columns.
#[inline]
pub(super) fn is_ft_score_column(name: &str) -> bool {
    name.starts_with(FT_SCORE_PREFIX)
}

/// O3 — column metadata (table / alias / column name / type) of a field.
/// Stable shapes are interned through [`intern_col_meta`]. The process pool
/// is hard-bounded and governed; shapes that cannot be admitted use an owned
/// `Arc` and are reclaimed with the last row/plan owner.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Default)]
pub(crate) struct ColMeta {
    pub(super) table: String,
    pub(super) alias: Option<String>,
    pub(super) column: String,
    pub(super) column_type: ColumnType,
    /// Catalog-derived wire metadata for this column: flags
    /// (NOT_NULL/PRI_KEY/...), declared length, and org_name. Constructed
    /// via [`crate::storage::schema::TableSchema::wire_column_source`]
    /// for columns that reference a real catalog row; default (all-zero
    /// flags, no length) for expression-derived columns. Interned along
    /// with the rest of the meta so the per-cell `PlanField` pays no
    /// extra allocation.
    pub(super) source: crate::types::ColumnSource,
    /// MySQL 8.0.23+ INVISIBLE column: scanned and addressable by name
    /// but skipped by `SELECT *` wildcard expansion (same filtering
    /// spot as the internal FT-score column).
    pub(super) invisible: bool,
}

impl ColMeta {
    /// Compiled-expression input for this column. Keeps the catalog
    /// collation from `source` so string comparisons follow the column (a
    /// `utf8mb4_bin` join residual stays binary), not the process default.
    pub(in crate::engine::executor::select) fn input_column(
        &self,
    ) -> crate::engine::expr_program::InputColumn {
        crate::engine::expr_program::InputColumn {
            table: self.table.clone(),
            alias: self.alias.clone(),
            name: self.column.clone(),
            column_type: self.column_type,
            unsigned: self.source.flags & crate::mysql::protocol::UNSIGNED_FLAG != 0
                || matches!(self.column_type, ColumnType::Year | ColumnType::Bit { .. }),
            collation_id: self.source.collation_id,
        }
    }

    /// Metadata for a physical table column, including its declared wire type
    /// and origin. Projection and visibility decisions belong to the caller.
    #[inline]
    pub(super) fn from_table(table: &PlanTable, index: usize) -> Self {
        let column = &table.schema.columns[index];
        Self {
            invisible: column.invisible,
            table: table.schema.name.clone(),
            alias: table.alias.clone(),
            column: column.name.clone(),
            column_type: table.schema.result_type(index),
            source: table
                .schema
                .wire_column_source_with_alias(index, table.alias.as_deref()),
        }
    }
}

#[derive(Debug)]
pub(crate) enum ColMetaRef {
    Interned(&'static ColMeta),
    Owned(std::sync::Arc<ColMeta>),
}

impl Clone for ColMetaRef {
    #[inline]
    fn clone(&self) -> Self {
        match self {
            Self::Interned(meta) => Self::Interned(meta),
            Self::Owned(meta) => Self::Owned(meta.clone()),
        }
    }
}

impl std::ops::Deref for ColMetaRef {
    type Target = ColMeta;

    #[inline]
    fn deref(&self) -> &Self::Target {
        match self {
            Self::Interned(meta) => meta,
            Self::Owned(meta) => meta,
        }
    }
}

impl ColMetaRef {
    #[inline]
    pub(crate) fn ptr_eq(&self, other: &Self) -> bool {
        std::ptr::eq::<ColMeta>(&**self, &**other)
    }

    #[inline]
    pub(crate) fn identity(&self) -> usize {
        (&**self as *const ColMeta) as usize
    }
}

/// Return a shared metadata handle whose contents equal `meta`. Identical
/// metadata admitted to the bounded process pool maps to the same pointer.
///
/// Hot path note: every TPC-H scan/INL row materialiser walks several
/// columns through here. A bare `Mutex<HashMap>` lookup per call
/// regresses Q9 by ~50 % because the lock is contended across worker
/// threads. We front the global table with a `thread_local!` cache so
/// the common hit case stays lock-free.
pub(super) fn intern_col_meta(meta: ColMeta) -> ColMetaRef {
    use std::cell::RefCell;
    use std::sync::{Mutex, OnceLock};

    // Same rationale as `FieldLookup`: SipHash is overkill for a small
    // intern table where the keys (ColMeta = a handful of short strings
    // + enum tags) are cheap to hash with ahash.
    thread_local! {
        static TLS_CACHE: RefCell<hashbrown::HashMap<ColMeta, &'static ColMeta, ahash::RandomState>> =
            RefCell::new(hashbrown::HashMap::with_hasher(field_lookup_hasher()));
    }

    if let Some(p) = TLS_CACHE.with(|c| c.borrow().get(&meta).copied()) {
        return ColMetaRef::Interned(p);
    }

    struct InternState {
        map: hashbrown::HashMap<ColMeta, &'static ColMeta, ahash::RandomState>,
        charge: crate::engine::executor::mem_budget::ControlledMemoryCharge,
    }

    static INTERN: OnceLock<Mutex<InternState>> = OnceLock::new();
    let state = INTERN.get_or_init(|| {
        Mutex::new(InternState {
            map: hashbrown::HashMap::with_hasher(field_lookup_hasher()),
            charge: crate::engine::executor::mem_budget::ControlledMemoryCharge::try_new_optional(
                crate::engine::executor::mem_budget::ControlledMemoryCategory::DerivedCache,
                0,
            )
            .expect("zero-byte metadata charge must succeed"),
        })
    });
    let interned = {
        let mut guard = state.lock().unwrap();
        if let Some(&p) = guard.map.get(&meta) {
            COL_META_INTERN_POOL_HITS.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            Some(p)
        } else {
            COL_META_INTERN_POOL_MISSES.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            let approx_bytes = 2
                * (std::mem::size_of::<ColMeta>()
                    + meta.table.len()
                    + meta.alias.as_deref().map_or(0, str::len)
                    + meta.column.len()
                    + 32);
            if guard.map.len() >= col_meta_intern_capacity()
                || !guard.charge.try_grow_optional(approx_bytes)
            {
                None
            } else {
                let p: &'static ColMeta = Box::leak(Box::new(meta.clone()));
                guard.map.insert(meta.clone(), p);
                COL_META_INTERN_ENTRIES.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                COL_META_INTERN_BYTES
                    .fetch_add(approx_bytes as u64, std::sync::atomic::Ordering::Relaxed);
                Some(p)
            }
        }
    };
    match interned {
        Some(interned) => {
            TLS_CACHE.with(|cache| {
                cache.borrow_mut().insert(meta, interned);
            });
            ColMetaRef::Interned(interned)
        }
        None => {
            COL_META_INTERN_FALLBACKS.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            ColMetaRef::Owned(std::sync::Arc::new(meta))
        }
    }
}

// Count process-pool probes only; the per-row TLS hit path remains free of
// shared atomic writes. These counters deliberately exclude TLS hits.
static COL_META_INTERN_POOL_HITS: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);
static COL_META_INTERN_POOL_MISSES: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);

pub(crate) fn col_meta_intern_pool_counters() -> (u64, u64) {
    (
        COL_META_INTERN_POOL_HITS.load(std::sync::atomic::Ordering::Relaxed),
        COL_META_INTERN_POOL_MISSES.load(std::sync::atomic::Ordering::Relaxed),
    )
}

static COL_META_INTERN_ENTRIES: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
static COL_META_INTERN_BYTES: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
static COL_META_INTERN_FALLBACKS: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);

pub(crate) fn col_meta_intern_capacity() -> usize {
    crate::util::env::var("MYDB_COL_META_INTERN_CAP")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(16_384)
        .clamp(1_024, 1_048_576)
}

/// (distinct interned column shapes, approximate resident bytes).
pub(crate) fn col_meta_intern_stats() -> (u64, u64) {
    (
        COL_META_INTERN_ENTRIES.load(std::sync::atomic::Ordering::Relaxed),
        COL_META_INTERN_BYTES.load(std::sync::atomic::Ordering::Relaxed),
    )
}

pub(crate) fn col_meta_intern_fallbacks() -> u64 {
    COL_META_INTERN_FALLBACKS.load(std::sync::atomic::Ordering::Relaxed)
}

/// Per-cell storage. Common metadata uses the non-refcounted interned arm;
/// overflow metadata owns an `Arc` and is reclaimed normally.
#[derive(Debug, Clone)]
pub(super) struct PlanField {
    pub(super) meta: ColMetaRef,
    pub(super) value: Value,
}

impl std::ops::Deref for PlanField {
    type Target = ColMeta;
    #[inline]
    fn deref(&self) -> &ColMeta {
        &self.meta
    }
}

impl PlanField {
    /// Build a field from owned metadata + an owned value. Interns the
    /// metadata into the global pool so future fields with the same
    /// column shape share storage.
    pub(super) fn new(
        table: impl Into<String>,
        alias: Option<String>,
        column: impl Into<String>,
        column_type: ColumnType,
        value: Value,
    ) -> Self {
        Self {
            meta: intern_col_meta(ColMeta {
                invisible: false,
                table: table.into(),
                alias,
                column: column.into(),
                column_type,
                source: Default::default(),
            }),
            value,
        }
    }

    /// Borrow the cell value. Mirrors the old `Arc<Value>::as_ref()`
    /// call sites from before the value was stored inline.
    #[inline]
    pub(super) fn value_ref(&self) -> &Value {
        &self.value
    }
}

fn snapshot_requires_replan(db: &Database, txn: TxnView, plan: &PhysicalPlan) -> bool {
    if !db.read_definition_changed_since(txn) {
        return false;
    }
    let mut unsafe_schema = false;
    super::walk_plan_tables(plan, &mut |table| {
        unsafe_schema |= table
            .schema
            .columns
            .iter()
            .any(|column| !column.nullable && column.not_null_from_scn > txn.snapshot_scn);
        unsafe_schema |= table.schema.indexes.iter().any(|index| {
            index.state == crate::storage::IndexState::Public
                && index.readable_from_scn > txn.snapshot_scn
        });
    });
    unsafe_schema
}

pub(crate) fn execute_physical_select(
    db: &Arc<Database>,
    txn: TxnView,
    stmt: &Select,
    plan: &PhysicalPlan,
) -> Result<QueryResponse, QueryError> {
    let _caller_partition_scope =
        crate::engine::executor::partition_router::suspend_pruned_partitions();
    let _explicit_scope = if stmt.from.len() == 1 && stmt.joins.is_empty() {
        crate::engine::executor::partition_clause::resolve_partition_clause(
            db,
            &stmt.from[0].name,
            &stmt.from[0].partitions,
        )?
        .map(crate::engine::executor::partition_router::push_pruned_partitions)
    } else {
        None
    };
    let _read_schema = crate::storage::read_schema::ReadSchemaScope::enter(
        (!txn.dirty_read).then_some(txn.snapshot_scn),
    );
    // Cached access paths can have been compiled for a newer snapshot. Rebind
    // before producing any rows, using descriptors that hide unsafe indexes.
    let rebound_stmt;
    let rebound_plan;
    let (stmt, plan) = if snapshot_requires_replan(db, txn, plan) {
        rebound_stmt = {
            let mut stmt = stmt.clone();
            super::prepare_select_for_planning(db, &mut stmt)?;
            stmt
        };
        rebound_plan = crate::planner::plan_prepared_physical_select(db, &rebound_stmt)?;
        (&rebound_stmt, &rebound_plan)
    } else {
        (stmt, plan)
    };
    crate::engine::cancel::check_cancelled()?;
    super::analyze_stats::register_execution_plan(plan)?;
    let execute = || {
        super::eval::with_subquery_context(db, txn, || {
            execute_physical_select_inner(db, txn, stmt, plan)
        })
    };
    // Install single-table partition pruning around the complete operator
    // tree. Heap scans already derive this locally, but index-only aggregate
    // and other fused roots do not carry the original WHERE expression. A
    // shared outer scope keeps every physical access path on the same leaf
    // storage-id set and is captured by streaming workers before this frame
    // returns.
    if stmt.joins.is_empty() {
        if let Some(table_name) = stmt.table.as_deref() {
            if let Ok(table) = db.table(table_name) {
                if table.is_partitioned() {
                    return run_scan_with_partition_pruning(&table, &stmt.selection, execute);
                }
            }
        }
    }
    execute()
}

/// Execute the immutable artifact produced by physical-plan compilation.
/// The presentation-bearing `stmt` comes from the current SQL text while all
/// access-path decisions come from `compiled`; this is what permits semantic
/// plan sharing without sharing expression-column spelling.
pub(crate) fn execute_compiled_select(
    db: &Arc<Database>,
    txn: TxnView,
    stmt: &Select,
    compiled: &crate::engine::cache::CompiledSelectPlan,
) -> Result<QueryResponse, QueryError> {
    let _read_schema = crate::storage::read_schema::ReadSchemaScope::enter(
        (!txn.dirty_read).then_some(txn.snapshot_scn),
    );
    if snapshot_requires_replan(db, txn, &compiled.physical) {
        return execute_physical_select(db, txn, stmt, &compiled.physical);
    }
    super::check_primary_definition_for_select(db, txn, stmt)?;
    use crate::engine::cache::CompiledOperator;
    let operator = compiled.executable.as_ref();
    super::analyze_stats::register_execution_plan(&compiled.physical)?;
    let force_batch = matches!(
        crate::engine::executor::vec::config::effective_mode(),
        crate::engine::executor::vec::config::VecMode::On
    );
    let _batch_route_hint = super::vec_bridge::enter_batch_route_hint(
        &compiled.physical,
        compiled.batch_route,
        compiled.native_output_supported,
    );

    // A unique point probe is already a complete, allocation-light result
    // kernel: parameter binding, one B-tree lookup and a projected heap
    // decode.  Sending it through the generic BatchOperator boundary first
    // rebuilds schemas/programs and creates a streaming worker for at most one
    // row, making the compiled operator below effectively unreachable in the
    // default auto mode.  Prefer the strictly narrower compiled kernel before
    // considering the general batch graph.  The kernel is plan-derived and
    // uses the same MVCC/storage primitives, so this changes only the
    // execution envelope, not the chosen access path or SQL semantics.
    if !force_batch {
        if let CompiledOperator::ProjectedUniquePointLookup(operator) = operator {
            if !super::analyze_stats::is_recording()
                && crate::engine::api::shared_result_metadata_allowed()
            {
                let rows = execute_compiled_projected_point_lookup_rows(db, txn, stmt, operator)?;
                return Ok(QueryResponse::SharedResultSet(
                    crate::engine::api::SharedResultSet {
                        columns: super::simple::projected_point_result_columns(stmt, operator)?,
                        rows,
                    },
                ));
            }
            if let Some(result) =
                run_compiled_select_kernel(db, txn, stmt, &compiled.physical, || {
                    execute_compiled_projected_point_lookup(db, txn, stmt, operator).map(Some)
                })?
            {
                return Ok(QueryResponse::ResultSet(result));
            }
        }
    }

    if let Some(recipe) = compiled.compact_range.as_ref() {
        if !force_batch
            && !super::analyze_stats::is_recording()
            && crate::engine::api::shared_result_metadata_allowed()
            && super::simple::runtime_unique_range_fits_batch(
                &compiled.physical,
                crate::engine::executor::vec::DEFAULT_BATCH_ROWS,
            )?
        {
            if let Some(result) = super::simple::execute_compact_range_recipe(
                db,
                txn,
                stmt,
                &compiled.physical,
                recipe,
            )? {
                return Ok(result);
            }
        }
    }

    // Building a complete vector graph and a top-level streaming worker costs
    // more than the useful work for a single-table range that provably fits in
    // one batch. Scalar aggregates retain the stable plan-estimate gate; plain,
    // ordered, and distinct projections require the stronger unique-integer
    // interval proof below. That keeps LIKE/non-unique ranges on their normal
    // streaming path. Neither gate depends on live memory/cache/concurrency
    // state, and forced vector mode remains an explicit override.
    if !force_batch
        && matches!(
            operator,
            CompiledOperator::ProjectedIndexRangeOrMultiLookup
                | CompiledOperator::FusedScanFilterProjectLimit
                | CompiledOperator::OrderedIndexScan
        )
        // Re-check the execution AST as well as the plan-cache semantic AST.
        // FD/group-aggregate lowering can intentionally make those two shapes
        // differ while sharing one compiled physical artifact.
        && super::simple::fused_single_table_shape_supported(stmt)
        && ((super::select_projection_has_aggregate(&stmt.projection)
            && super::simple::prefer_fused_single_table_operator(&compiled.physical))
            || super::simple::runtime_unique_range_fits_batch(
                &compiled.physical,
                crate::engine::executor::vec::DEFAULT_BATCH_ROWS,
            )?)
    {
        if let Some(result) = run_compiled_select_kernel(db, txn, stmt, &compiled.physical, || {
            execute_fused_single_table_operator(db, txn, stmt, &compiled.physical)
        })? {
            return Ok(QueryResponse::ResultSet(result));
        }
    }

    // Descriptor-only LOB projections must run before the generic batch
    // bridge: the latter materializes complete rows before evaluating scalar
    // functions, defeating LENGTH/range-read pushdown for multi-MB values.
    if !force_batch
        && super::simple::descriptor_lob_projection_shape_supported(stmt)
        && super::simple::fused_single_table_shape_supported(stmt)
    {
        if let Some(result) = run_compiled_select_kernel(db, txn, stmt, &compiled.physical, || {
            execute_fused_single_table_operator(db, txn, stmt, &compiled.physical)
        })? {
            return Ok(QueryResponse::ResultSet(result));
        }
    }

    // A compatible batch pipeline owns execution end-to-end through the
    // protocol cursor. This check is metadata-only and runs before the generic
    // physical executor, so no input can be consumed by one route and replayed
    // by the other. Auto mode still follows the CBO threshold; explicit `off`
    // keeps the scalar row oracle.
    if force_batch || compiled.batch_route {
        return execute_physical_select(db, txn, stmt, &compiled.physical);
    }

    // Preserve the existing scalar fallback for fused single-table variants
    // when the complete batch pipeline cannot own this result.
    if matches!(
        operator,
        CompiledOperator::ProjectedIndexRangeOrMultiLookup
            | CompiledOperator::FusedScanFilterProjectLimit
            | CompiledOperator::OrderedIndexScan
    ) {
        if let Some(result) = run_compiled_select_kernel(db, txn, stmt, &compiled.physical, || {
            execute_fused_single_table_operator(db, txn, stmt, &compiled.physical)
        })? {
            return Ok(QueryResponse::ResultSet(result));
        }
    }
    execute_physical_select(db, txn, stmt, &compiled.physical)
}

fn run_compiled_select_kernel(
    db: &Arc<Database>,
    _txn: TxnView,
    _stmt: &Select,
    plan: &PhysicalPlan,
    execute: impl FnOnce() -> Result<Option<ResultSet>, QueryError>,
) -> Result<Option<ResultSet>, QueryError> {
    let started = super::analyze_stats::is_recording().then(std::time::Instant::now);
    let io = started.map(|_| db.buffer_stats_counts());
    let (partition_observer, result) = observe_partitioned_scan(plan, || {
        super::analyze_stats::with_range_scan_plan(plan, execute)
    });
    let result = result?;
    if let (Some(result), Some(started)) = (result.as_ref(), started) {
        super::analyze_stats::record_with_io_snapshot(
            db,
            plan,
            started.elapsed().as_nanos(),
            result.rows.len() as u64,
            io.expect("analyze compiled-kernel I/O snapshot missing"),
        );
        if let Some((key, counter)) = partition_observer {
            super::analyze_stats::add_partitions_key(
                key,
                counter.load(std::sync::atomic::Ordering::Relaxed),
            );
        }
    }
    Ok(result)
}

/// Native batches retain the cached physical plan's metadata. At the SELECT
/// boundary, keep its types/origins but take display names from this execution
/// (COUNT/couNT and aliases must not inherit an earlier query's spelling).
fn refresh_native_result_column_names(
    columns: &mut [Column],
    stmt: &Select,
    plan: &PhysicalPlan,
) -> Result<(), QueryError> {
    if columns.len() == stmt.projection.len()
        && stmt
            .projection
            .iter()
            .all(|item| matches!(item, SelectItem::Expr { .. }))
    {
        for (column, item) in columns.iter_mut().zip(&stmt.projection) {
            if let SelectItem::Expr { expr, alias } = item {
                if let Some(alias) = alias {
                    column.name.clone_from(alias);
                } else {
                    column.name = expr_name(expr);
                }
            }
        }
    } else {
        // Wildcards can expand to several fields. Reuse the existing binder
        // for their positions instead of assuming one column per select item.
        let presentation = empty_result_columns(stmt, plan)?;
        if presentation.len() != columns.len() {
            return Err(error(
                crate::error::catalog::ER_UNKNOWN_ERROR,
                "native result column count does not match SELECT presentation",
            ));
        }
        for (column, current) in columns.iter_mut().zip(presentation) {
            column.name = current.name;
        }
    }
    Ok(())
}

fn execute_physical_select_inner(
    db: &Arc<Database>,
    txn: TxnView,
    stmt: &Select,
    plan: &PhysicalPlan,
) -> Result<QueryResponse, QueryError> {
    crate::engine::cancel::check_cancelled()?;
    let ordered_limit_window =
        !stmt.order_by.is_empty() && (stmt.limit.is_some() || stmt.offset.is_some());
    // Phase A/C: install a per-query memory budget. Operators consult
    // this via `crate::engine::executor::mem_budget::try_charge` to
    // fail fast (with `MemoryLimitExceeded`) before OOM-killing the
    // process. If the caller (e.g. a test or an outer query) has
    // already set a budget on this thread, inherit it rather than
    // replacing — this lets tests assert tight limits.
    let body = || -> Result<QueryResponse, QueryError> {
        let _grouping = (!stmt.group_by.is_empty()).then(|| {
            crate::engine::cancel::enter_process_stage(
                crate::engine::session_registry::ProcessState::SortingForGroup,
            )
        });
        let native_batch_result = !crate::planner::connection_thread_execution_required()
            && super::vec_bridge::should_route_result_to_batch(plan, &stmt.projection);
        // A top-level streaming response owns a producer on the process
        // blocking pool. Some physical operators (notably IndexInLookup)
        // execute and materialize a child SELECT from inside that producer.
        // Scheduling the child as another channel-backed producer can starve
        // forever when MYDB_MEMORY_TARGET is small enough to admit only one
        // blocking stack. Consume nested results synchronously on the current
        // worker instead; the caller needs the complete child result anyway.
        let nested_stream_consumer = super::stream::on_stream_worker();
        let requires_eager = nested_stream_consumer || select_requires_eager_response(stmt, plan);
        let eager_bounded_result = matches!(
            crate::engine::executor::vec::config::effective_mode(),
            crate::engine::executor::vec::config::VecMode::Auto
        ) && !super::force_streaming_response_active()
            && synchronously_bounded_result_plan(plan);
        let exact_rand_top_one_plan = super::vec_bridge::exact_rand_index_top_one_plan(plan);
        let eager_exact_rand_top_one = eager_bounded_result && exact_rand_top_one_plan;
        let eager_native_scalar_aggregate = matches!(
            crate::engine::executor::vec::config::effective_mode(),
            crate::engine::executor::vec::config::VecMode::Auto
        ) && native_batch_result
            && !super::force_streaming_response_active()
            && scalar_aggregate_result_plan(plan);
        // A scalar aggregate may scan an arbitrarily large input, so routing
        // it through the legacy eager PlanRow executor is a serious
        // regression. Its *native batch* graph, however, already streams and
        // spills its input while proving that the final result contains at
        // most one row. Drain that graph synchronously on the caller's
        // blocking worker: this preserves vector aggregation and bounded
        // memory while avoiding a fresh OS worker + mpsc channel per tiny
        // OLTP aggregate.
        if eager_native_scalar_aggregate {
            if let Some((mut columns, mut rows)) =
                super::vec_bridge::execute_native_plan_values(db, txn, plan)?
            {
                refresh_native_result_column_names(&mut columns, stmt, plan)?;
                let normalizer =
                    super::stream::SelectResultNormalizer::new(&columns, &stmt.projection);
                for row in &mut rows {
                    normalizer.apply(row);
                }
                return Ok(QueryResponse::ResultSet(ResultSet { columns, rows }));
            }
        }
        // A semantic LIMIT 0/1 over a native batch graph is also safe to
        // drain on this blocking caller, but must stay in that graph. Falling
        // through to the legacy eager adapter can replace a governed native
        // hash join with the standalone materialising bridge and bypass its
        // WorkArea lifetime. Exact unique probes intentionally keep their
        // compiled/row kernel selected above; this branch is only for the
        // additional LIMIT cardinality proof.
        if eager_bounded_result
            && !bounded_result_plan(plan)
            && (native_batch_result || eager_exact_rand_top_one)
        {
            if let Some((mut columns, mut rows)) =
                super::vec_bridge::execute_native_plan_values(db, txn, plan)?
            {
                refresh_native_result_column_names(&mut columns, stmt, plan)?;
                let normalizer =
                    super::stream::SelectResultNormalizer::new(&columns, &stmt.projection);
                for row in &mut rows {
                    normalizer.apply(row);
                }
                return Ok(QueryResponse::ResultSet(ResultSet { columns, rows }));
            }
        }
        // Preserve the native batch implementation for a nested result when
        // it is available. Only the response envelope changes: batches are
        // drained by the current worker instead of sending them through a
        // second producer/consumer channel on the same bounded pool.
        if nested_stream_consumer && native_batch_result {
            if let Some((mut columns, mut rows)) =
                super::vec_bridge::execute_native_plan_values(db, txn, plan)?
            {
                refresh_native_result_column_names(&mut columns, stmt, plan)?;
                let normalizer =
                    super::stream::SelectResultNormalizer::new(&columns, &stmt.projection);
                for row in &mut rows {
                    normalizer.apply(row);
                }
                return Ok(QueryResponse::ResultSet(ResultSet { columns, rows }));
            }
        }
        // A fully preflighted native result pipeline owns projection and the
        // protocol boundary, including blocking aggregate/sort/top-K nodes.
        // Those nodes may be eager internally, but wrapping their Batch output
        // in a cursor must not force a PlanRow materialization.  The legacy
        // eager guards below apply only when no end-to-end batch graph exists.
        if !nested_stream_consumer
            && !eager_bounded_result
            && (native_batch_result || (!requires_eager && !ordered_limit_window))
        {
            // A streaming response costs a producer worker, a bounded channel
            // and a consumer hand-off per statement, which dominates a lookup
            // that returns a handful of rows. When the optimizer expects a
            // small result and re-running the statement is unobservable,
            // drain the native graph here under a row cap; a larger result
            // discards the partial work (and its diagnostics) and streams
            // from scratch with the same snapshot.
            if native_batch_result && small_result_probe_allowed(db, stmt, plan) {
                let warnings = crate::engine::api::statement_warning_marks();
                match super::vec_bridge::execute_native_plan_values_capped(
                    db,
                    txn,
                    plan,
                    SMALL_RESULT_PROBE_MAX_ROWS,
                )? {
                    super::vec_bridge::NativePlanValues::Complete(native, mut rows) => {
                        // Present exactly the result metadata the streaming
                        // response would have used (declared precision,
                        // origins, flags), not the batch template's.
                        let columns = empty_result_columns(stmt, plan)
                            .or_else(|_| super::describe_select_columns(db, stmt))?;
                        if columns.len() == native.len() {
                            let normalizer = super::stream::SelectResultNormalizer::new(
                                &columns,
                                &stmt.projection,
                            );
                            for row in &mut rows {
                                normalizer.apply(row);
                            }
                            return Ok(QueryResponse::ResultSet(ResultSet { columns, rows }));
                        }
                        crate::engine::api::truncate_statement_warnings(warnings);
                    }
                    super::vec_bridge::NativePlanValues::Exceeded => {
                        crate::engine::api::truncate_statement_warnings(warnings);
                    }
                    super::vec_bridge::NativePlanValues::Unsupported => {}
                }
            }
            return super::stream::streaming_result_set_for_select(
                Arc::clone(db),
                txn,
                stmt.clone(),
                plan,
            );
        }
        // Bound transient intermediate materialization to this SELECT
        // body. Charges issued by blocking operators below (hash join /
        // NLJ / scan output) release when this scope drops, so a query
        // that materialises several CTE bodies in sequence does not
        // accumulate every body's intermediate footprint into one
        // monotonically-climbing budget total.
        let _materialization_scope = MaterializationChargeScope::enter();
        let strict_plan_row_output = plan_contains_cte_scan(plan)
            || crate::engine::executor::cte::cte_materialization_active();
        let mut rows = with_strict_plan_row_output_accounting(strict_plan_row_output, || {
            if ordered_limit_window {
                execute_plan_rows_inner(db, txn, plan)
            } else {
                execute_plan_rows(db, txn, plan)
            }
        })?;
        // O1 — Share one schema-built `FieldLookup` across the whole eager
        // result set. The legacy projection / ORDER-BY interpreter resolves
        // column references by name (`resolve_plan_field`), which lazily
        // builds a per-row `FieldLookup` the first time a row is touched.
        // When the producing operator does not propagate a shared lookup
        // Arc (e.g. the materialised CTE-join path), every row arrives with
        // a fresh empty `OnceLock`, so each one rebuilds the map — N lowercase
        // String allocations + two HashMap fills per row. For TPC-DS Q78
        // (2.45M wide rows reaching a legacy projection) this rebuild was
        // ~32% of total query time. All rows in an eager result set share one
        // schema (projection `compile_once` already assumes `rows.first()`
        // represents every row), so binding them to one pre-built lookup is
        // semantically identical and turns N rebuilds into one.
        share_field_lookup_across_rows(&mut rows);
        // Best-effort process charge for the materialized PlanRow set.
        // The actual projected Value rows below are charged strictly as
        // they are built, which covers internal SELECT materialization
        // for CTEs / derived tables before a caller gets a chance to
        // inspect the finished ResultSet.
        //
        // Use the *actual* per-row footprint, not `rows.len() ×
        // ESTIMATED_ROW_BYTES` (256). For wide intermediate rows the two
        // agree, but for the narrow key/aggregate rows that dominate
        // TPC-DS IN-subquery and union intermediates (e.g. a single
        // `item_sk` column ≈ 8–40 B/row) the 256 B/row estimate
        // over-charges by up to ~32×, holding hundreds of MB of phantom
        // budget and leaving no room for the projection's strict charge —
        // a false 1037 (Q14/Q23). The accurate sum is one O(rows) pass we
        // already pay for during projection below.
        let result_charge_bytes = rows.iter().fold(0usize, |acc, row| {
            acc.saturating_add(materialized_plan_row_charge(row))
        });
        let _release_on_exit =
            crate::engine::executor::mem_budget::force_charge_scoped(result_charge_bytes)?;
        let columns = result_columns(db, stmt, &rows, plan)?;
        let source_order = wildcard_source_order(stmt);
        let ordered_wildcard =
            projection_uses_unqualified_wildcard(&stmt.projection) && !source_order.is_empty();
        let projection_program = if ordered_wildcard {
            None
        } else {
            rows.first()
                .map(|row| PlanProjectionProgram::compile_once(row, &stmt.projection))
        };
        let mut projected_rows = crate::engine::executor::mem_budget::try_vec_with_capacity(
            rows.len(),
            "projected result rows",
        )?;
        let mut projected_rows_charge = crate::engine::executor::mem_budget::ScopedCharge::new(0);
        for row in rows {
            let projected = if let Some(program) = projection_program.as_ref() {
                program.project_row_owned(row)?
            } else {
                stmt.projection
                    .iter()
                    .flat_map(|item| project_item_with_source_order(&row, item, &source_order))
                    .collect::<Result<Vec<_>, _>>()?
            };
            let projected_charge =
                crate::engine::executor::mem_budget::estimate_row_bytes(&projected);
            // This API has already materialized the projected row and must
            // return an owned ResultSet. Its reservation is mandatory, even
            // when a blocking parent occupies the query's soft spill quota.
            // The unified process hard governor remains non-bypassable.
            projected_rows_charge.add_force(projected_charge)?;
            projected_rows.push(projected);
        }
        Ok(QueryResponse::ResultSet(ResultSet {
            columns,
            rows: projected_rows,
        }))
    };
    if crate::engine::executor::mem_budget::current_budget().is_some() {
        body()
    } else {
        let budget =
            std::sync::Arc::new(crate::engine::executor::mem_budget::MemBudget::from_env());
        crate::engine::executor::mem_budget::with_query_budget(budget, body)
    }
}

/// Whether a physical tree can emit at most one row from exact unique probes.
///
/// Such plans are a poor fit for the top-level streaming envelope: the
/// envelope creates an OS worker and an mpsc channel before producing at most
/// one row. Executing the already selected physical tree eagerly on the
/// caller's blocking worker avoids that fixed cost while retaining the same
/// operator kernels. This predicate is intentionally structural rather than
/// benchmark- or table-name-specific; it never trusts cardinality estimates.
/// Unbounded scans/ranges and aggregates therefore stay streaming.
pub(super) fn bounded_result_plan(plan: &PhysicalPlan) -> bool {
    match plan {
        PhysicalPlan::Projection { input, .. }
        | PhysicalPlan::Filter { input, .. }
        | PhysicalPlan::Sort { input, .. }
        | PhysicalPlan::IncrementalSort { input, .. }
        | PhysicalPlan::Materialize { input, .. }
        | PhysicalPlan::Exchange { input, .. } => bounded_result_plan(input),
        PhysicalPlan::IndexLookup { index, exact, .. } => *exact && (index.primary || index.unique),
        PhysicalPlan::NestedLoopJoin { left, right, .. }
        | PhysicalPlan::HashJoin { left, right, .. } => {
            bounded_result_plan(left) && bounded_result_plan(right)
        }
        PhysicalPlan::IndexNestedLoopJoin {
            left,
            right_index,
            eq_prefix_len,
            ..
        } => {
            bounded_result_plan(left)
                && (right_index.primary || right_index.unique)
                && *eq_prefix_len >= right_index.columns.len()
        }
        _ => false,
    }
}

/// Whether the complete response is semantically capped at one row.
///
/// Besides exact unique probes, a resolved `LIMIT 0/1` is an equally strong
/// cardinality proof even when its input is an unbounded range. The existing
/// limit executor keeps that input streaming (or uses bounded Top-N), so
/// finishing on the caller's blocking worker does not materialise the range;
/// it only removes the otherwise redundant top-level OS worker and channel.
/// Parameterized limits are read from the active binding context, never from
/// estimates. Wrappers below cannot multiply their input's rows.
/// Optimizer row estimate up to which a streamable SELECT is first drained
/// synchronously (see `small_result_probe_allowed`).
const SMALL_RESULT_PROBE_MAX_ESTIMATE: f64 = 64.0;
/// Hard cap on rows a synchronous small-result probe may buffer before it
/// falls back to the streaming response.
const SMALL_RESULT_PROBE_MAX_ROWS: usize = 256;

/// Whether a streamable SELECT may first be drained synchronously and, on an
/// oversized result, executed again for streaming. That requires a small
/// estimated result and a statement whose evaluation has no observable side
/// effects or volatility beyond diagnostics: only explicit plain column/
/// constant/parameter expressions and the standard aggregates, no wildcards,
/// subqueries, variables, window functions, row locks or FOUND_ROWS
/// bookkeeping.
fn small_result_probe_allowed(db: &Database, stmt: &Select, plan: &PhysicalPlan) -> bool {
    if matches!(
        crate::engine::executor::vec::config::effective_mode(),
        crate::engine::executor::vec::config::VecMode::Off
    ) || super::force_streaming_response_active()
        || super::analyze_stats::is_recording()
        || !(crate::planner::estimated_rows(plan) <= SMALL_RESULT_PROBE_MAX_ESTIMATE)
        || stmt.for_update
        || stmt.lock_clause.is_some()
        || stmt.sql_calc_found_rows
        || stmt.derived.is_some()
        || stmt.values.is_some()
        || stmt.json_table.is_some()
        || !stmt.named_windows.is_empty()
        || plan_contains_cte_scan(plan)
        // Views and CTEs carry their own expressions; only base tables keep
        // every evaluated expression visible in this statement.
        || !stmt
            .from
            .iter()
            .chain(stmt.joins.iter().map(|join| &join.table))
            .all(|table| db.table(&table.name).is_ok())
    {
        return false;
    }
    fn pure(expr: &Expr) -> bool {
        !crate::parser::visit::any_expr(expr, |node| match node {
            Expr::Identifier(_)
            | Expr::CompoundIdentifier(_)
            | Expr::Literal(_)
            | Expr::BoundValue(_)
            | Expr::Placeholder(_)
            | Expr::Wildcard
            | Expr::Unary { .. }
            | Expr::Binary { .. }
            | Expr::IsNull(_)
            | Expr::IsNotNull(_)
            | Expr::InList { .. }
            | Expr::Between { .. }
            | Expr::Like { .. }
            | Expr::Case { .. }
            | Expr::Tuple(_) => false,
            Expr::Function { name, .. } => !["COUNT", "SUM", "MIN", "MAX", "AVG"]
                .iter()
                .any(|aggregate| name.eq_ignore_ascii_case(aggregate)),
            _ => true,
        })
    }
    // Wildcards keep the streaming path's source-order expansion.
    stmt.projection.iter().all(|item| match item {
        SelectItem::Expr { expr, .. } => pure(expr),
        SelectItem::Wildcard | SelectItem::QualifiedWildcard(_) => false,
    }) && stmt.selection.as_ref().is_none_or(pure)
        && stmt.having.as_ref().is_none_or(pure)
        && stmt.group_by.iter().all(pure)
        && stmt.order_by.iter().all(|order| pure(&order.expr))
        && stmt
            .joins
            .iter()
            .all(|join| join.condition.as_ref().is_none_or(pure))
}

fn synchronously_bounded_result_plan(plan: &PhysicalPlan) -> bool {
    if bounded_result_plan(plan) {
        return true;
    }
    match plan {
        PhysicalPlan::Projection { input, .. }
        | PhysicalPlan::Filter { input, .. }
        | PhysicalPlan::Sort { input, .. }
        | PhysicalPlan::IncrementalSort { input, .. }
        | PhysicalPlan::Materialize { input, .. }
        | PhysicalPlan::Exchange { input, .. } => synchronously_bounded_result_plan(input),
        PhysicalPlan::Limit { limit, binding, .. } => binding
            .resolve_limit(*limit)
            .is_some_and(|limit| limit <= 1),
        PhysicalPlan::TopWindow { limit, binding, .. } => binding
            .resolve_limit(Some(*limit))
            .is_some_and(|limit| limit <= 1),
        _ => false,
    }
}

/// Whether a physical tree has a scalar aggregate as its sole cardinality
/// boundary. Unlike estimates, an empty GROUP BY is a semantic proof that the
/// aggregate emits at most one row (HAVING may reduce that to zero). The
/// predicate admits only wrappers that cannot multiply rows.
fn scalar_aggregate_result_plan(plan: &PhysicalPlan) -> bool {
    match plan {
        PhysicalPlan::Projection { input, .. }
        | PhysicalPlan::Filter { input, .. }
        | PhysicalPlan::Sort { input, .. }
        | PhysicalPlan::IncrementalSort { input, .. }
        | PhysicalPlan::Materialize { input, .. }
        | PhysicalPlan::Limit { input, .. } => scalar_aggregate_result_plan(input),
        PhysicalPlan::HashAggregate { group_by, .. }
        | PhysicalPlan::StreamingAggregate { group_by, .. }
        | PhysicalPlan::ParallelHashAggregate { group_by, .. }
        | PhysicalPlan::CoveringIndexAggregate { group_by, .. }
        | PhysicalPlan::ParallelIndexAggregate { group_by, .. } => group_by.is_empty(),
        PhysicalPlan::IndexAggregate { .. } => true,
        _ => false,
    }
}

fn select_requires_eager_response(stmt: &Select, plan: &PhysicalPlan) -> bool {
    // A FULLTEXT scan injects a hidden relevance pseudo-column that
    // downstream clauses resolve by fingerprint; that resolution only
    // runs on the eager path, so it always forces eager.
    if plan_contains_fulltext_scan(plan) {
        return true;
    }
    // TopWindow is itself a blocking, page-sized operator: it must read
    // all input rows before quickselect can isolate the requested window,
    // and then emits only LIMIT rows. Wrapping it in the top-level streaming
    // cursor buys no early-row delivery and moves the expensive scan/window
    // work onto a worker/channel path that is measurably slower than the
    // eager executor used by EXPLAIN ANALYZE.
    if plan_contains_top_window(plan) {
        return true;
    }
    // Session-mutating expressions (`@v := ...`, sequence functions and
    // LOAD_FILE's live security context) must execute on the connection
    // thread. This guard must precede the CTE/derived cursor preferences:
    // those paths otherwise move projection evaluation to a worker with an
    // owned session snapshot, so COUNT(*) can observe the rows while the
    // assignment side effects disappear from the real session.
    if crate::planner::connection_thread_execution_required() {
        return true;
    }
    // When materialising a CTE body, the consumer (`materialize_non_recursive`)
    // drains the result row-by-row into a spill-capable `CteSpillBuilder`.
    // Streaming is therefore strictly preferable: it avoids the eager
    // full-result materialisation + strict per-row charge that OOMs large
    // CTE bodies (TPC-DS Q78's 2.45M-group aggregates, Q95's 6.64M-row
    // self-join) before a single row reaches the spill builder. Params are
    // re-bound on the streaming worker and the parent's CTE registry is
    // propagated to it, so aggregate / param bodies stream correctly; the
    // operator-internal output `Vec` is not budget-charged, so the spill
    // builder is what bounds memory.
    //
    // Exclude bodies that themselves scan a CTE (`CteScan`): a recursive
    // CTE mutates its working-table entry in the thread-local registry
    // between iterations, but the streaming worker captures a registry
    // *snapshot* at spawn — it would read a stale working table and the
    // recursion would stall ("recursive part did not return rows"). Those
    // bodies stay eager; they reference already-materialised CTE state.
    //
    if crate::engine::executor::cte::cte_materialization_active() && !plan_contains_cte_scan(plan) {
        return false;
    }
    stmt.derived.is_some()
        || (crate::engine::params::has_active_params()
            && !crate::engine::params::streaming_param_source_active()
            && !super::force_streaming_response_active())
        || !stmt.group_by.is_empty()
        || stmt.having.is_some()
        || super::select_projection_has_aggregate(&stmt.projection)
        || plan_emits_aggregate_rows(plan)
        || plan_contains_cte_scan(plan)
}

fn plan_contains_top_window(plan: &PhysicalPlan) -> bool {
    plan.any(|node| matches!(node, PhysicalPlan::TopWindow { .. }))
}

/// Whether `plan` contains a `FulltextScan` anywhere. Such plans are
/// materialised eagerly: the scan injects a hidden relevance pseudo-column
/// that downstream WHERE / SELECT / ORDER BY resolve by fingerprint, and
/// eager execution keeps that resolution on the simple, well-tested path.
fn plan_contains_fulltext_scan(plan: &PhysicalPlan) -> bool {
    plan.any(|node| matches!(node, PhysicalPlan::FulltextScan { .. }))
}

pub(super) fn plan_contains_cte_scan(plan: &PhysicalPlan) -> bool {
    plan.any(|node| matches!(node, PhysicalPlan::CteScan { .. }))
}

/// True when `plan`'s output `PlanRow`s carry aggregate metadata
/// (`group` / `agg_values`) that a downstream SELECT projection still
/// needs to resolve `Expr::Function` aggregate calls. Pass-through
/// operators (non-distinct Projection, Exchange, Limit) preserve that
/// metadata, so we peel them to find the row-producing operator.
///
/// The vectorized Top-N bridge uses this to refuse upstreams whose rows
/// it would round-trip through a `Batch` — that conversion only keeps
/// column values and silently drops `group` / `agg_values`.
pub(super) fn plan_emits_aggregate_rows(plan: &PhysicalPlan) -> bool {
    match plan {
        PhysicalPlan::HashAggregate { .. }
        | PhysicalPlan::HashRollupAggregate { .. }
        | PhysicalPlan::HashGroupingSetsAggregate { .. }
        | PhysicalPlan::StreamingAggregate { .. }
        | PhysicalPlan::StreamingRollupAggregate { .. }
        | PhysicalPlan::StreamingGroupingSetsAggregate { .. }
        | PhysicalPlan::ParallelHashAggregate { .. }
        | PhysicalPlan::ParallelRollupAggregate { .. }
        | PhysicalPlan::ParallelGroupingSetsAggregate { .. }
        | PhysicalPlan::IndexAggregate { .. }
        | PhysicalPlan::CoveringIndexAggregate { .. }
        | PhysicalPlan::ParallelIndexAggregate { .. } => true,
        PhysicalPlan::Projection {
            input,
            distinct: false,
            ..
        }
        | PhysicalPlan::Materialize { input, .. }
        | PhysicalPlan::Exchange { input, .. }
        | PhysicalPlan::Limit { input, .. } => plan_emits_aggregate_rows(input),
        _ => false,
    }
}

/// Execute a physical plan and return the result as `(columns, rows)`.
/// Used by code outside the `select` module (e.g. the window-function
/// dispatch path) that doesn't have access to the internal `PlanRow` type.
pub(in crate::engine::executor) fn execute_plan_to_columns_and_values(
    db: &Arc<Database>,
    txn: TxnView,
    plan: &PhysicalPlan,
) -> Result<(Vec<Column>, Vec<Vec<Value>>), QueryError> {
    let body = || -> Result<(Vec<Column>, Vec<Vec<Value>>), QueryError> {
        if let Some(super::vec_bridge::VecBatchInput {
            mut stream,
            template,
        }) = super::vec_bridge::try_native_vec_input_stream(db, txn, plan)?
        {
            let columns = template
                .iter()
                .map(|field| Column {
                    source: field.source.clone(),
                    name: field.column.clone(),
                    column_type: field.column_type,
                })
                .collect();
            let mut row_values = Vec::new();
            while let Some(batch) = stream.next_batch()? {
                crate::engine::cancel::check_cancelled()?;
                row_values.reserve(batch.visible_rows());
                for row_idx in batch.visible_indices() {
                    row_values.push(batch.row(row_idx));
                }
            }
            return Ok((columns, row_values));
        }
        let rows = execute_plan_rows(db, txn, plan)?;
        let columns: Vec<Column> = if let Some(first) = rows.first() {
            first
                .fields
                .iter()
                .map(|f| Column {
                    // Plumb the catalog-derived wire metadata from
                    // ColMeta.source so clients reached through the
                    // window-function / EXPLAIN ANALYZE dispatch path
                    // see the real flags / length / org_name. Mirrors
                    // result_columns() on the main SELECT path.
                    source: f.source.clone(),
                    name: f.column.clone(),
                    column_type: f.column_type,
                })
                .collect()
        } else {
            Vec::new()
        };
        let row_values: Vec<Vec<Value>> = rows
            .iter()
            .map(|r| r.fields.iter().map(|f| (f.value).clone()).collect())
            .collect();
        Ok((columns, row_values))
    };
    // Mirror execute_select's budget setup so callers that go through
    // this entry (window-function dispatch, EXPLAIN ANALYZE, etc.)
    // also benefit from per-query / process-level memory accounting.
    if crate::engine::executor::mem_budget::current_budget().is_some() {
        body()
    } else {
        let budget =
            std::sync::Arc::new(crate::engine::executor::mem_budget::MemBudget::from_env());
        crate::engine::executor::mem_budget::with_query_budget(budget, body)
    }
}

pub(super) fn execute_plan_rows(
    db: &Arc<Database>,
    txn: TxnView,
    plan: &PhysicalPlan,
) -> Result<Vec<PlanRow>, QueryError> {
    crate::engine::cancel::check_cancelled()?;
    // Collection is an output adapter, not an alternative physical executor.
    // Always use the same stream construction policy; an active ANALYZE trace
    // may observe that stream but must never select a different route.
    super::stream::collect_plan_stream(db, txn, plan)
}

pub(super) fn zone_map_runtime_snapshot() -> (u64, u64, u64, u64, u64, u64, u64) {
    use std::sync::atomic::Ordering::Relaxed;
    (
        crate::storage::ZONE_MAP_METADATA_DECODE_NS.load(Relaxed),
        super::parallel::ZONE_MAP_CANDIDATE_BUILD_NS
            .load(Relaxed)
            .saturating_add(crate::storage::ZONE_MAP_RUNTIME_CANDIDATE_NS.load(Relaxed)),
        super::parallel::ZONE_MAP_SCAN_TASKS_SCHEDULED
            .load(Relaxed)
            .saturating_add(crate::storage::ZONE_MAP_RUNTIME_TASKS.load(Relaxed)),
        super::parallel::ZONE_MAP_SCAN_PAGES_SCHEDULED
            .load(Relaxed)
            .saturating_add(crate::storage::ZONE_MAP_RUNTIME_PAGES.load(Relaxed)),
        super::parallel::ZONE_MAP_SCAN_PAGES_SKIPPED
            .load(Relaxed)
            .saturating_add(crate::storage::ZONE_MAP_RUNTIME_PAGES_SKIPPED.load(Relaxed)),
        super::parallel::ZONE_MAP_RUNTIME_FALLBACKS.load(Relaxed),
        super::parallel::PARALLEL_WORKERS_SPAWNED.load(Relaxed),
    )
}

fn scan_node_has_partitions(plan: &PhysicalPlan) -> bool {
    match plan {
        PhysicalPlan::SeqScan { table, .. }
        | PhysicalPlan::IndexLookup { table, .. }
        | PhysicalPlan::IndexMultiLookup { table, .. }
        | PhysicalPlan::IndexRangeScan { table, .. }
        | PhysicalPlan::IndexFastScan { table, .. }
        | PhysicalPlan::FulltextScan { table, .. }
        | PhysicalPlan::SpatialScan { table, .. }
        | PhysicalPlan::VectorIndexScan { table, .. }
        | PhysicalPlan::VectorBruteScan { table, .. }
        | PhysicalPlan::ParallelSeqScan { table, .. }
        | PhysicalPlan::ParallelIndexFastScan { table, .. }
        | PhysicalPlan::ParallelIndexRangeScan { table, .. } => table.schema.partitioning.is_some(),
        _ => false,
    }
}

fn partitioned_scan_observer(
    plan: &PhysicalPlan,
) -> Option<(PlanNodeId, std::sync::Arc<std::sync::atomic::AtomicU64>)> {
    if scan_node_has_partitions(plan) {
        return Some((
            super::analyze_stats::plan_key(plan)?,
            std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0)),
        ));
    }
    plan_stats_children(plan)
        .into_iter()
        .find_map(partitioned_scan_observer)
}

fn observe_partitioned_scan<F, R>(
    plan: &PhysicalPlan,
    f: F,
) -> (
    Option<(PlanNodeId, std::sync::Arc<std::sync::atomic::AtomicU64>)>,
    R,
)
where
    F: FnOnce() -> R,
{
    let observer = super::analyze_stats::is_recording()
        .then(|| partitioned_scan_observer(plan))
        .flatten();
    let result = crate::engine::executor::partition_router::with_partition_visit_collector(
        observer.as_ref().map(|(_, counter)| counter.clone()),
        f,
    );
    (observer, result)
}

/// Append one `generate_series` row (value column + optional ordinality),
/// applying the residual filter. Shared by the integer and numeric loops.
fn push_generate_series_row(
    out: &mut Vec<PlanRow>,
    templates: &[PlanField],
    with_ordinality: bool,
    ordinal: i64,
    value: Value,
    filter: Option<&Expr>,
) -> Result<(), QueryError> {
    let mut fields = templates.to_vec();
    fields[0].value = value;
    if with_ordinality {
        fields[1].value = Value::Int(ordinal);
    }
    let plan_row = PlanRow {
        fields,
        group: None,
        agg_values: None,
        schema: PlanRow::empty_schema(),
    };
    let keep = match filter {
        Some(expr) => eval_plan_bool(expr, &plan_row)?,
        None => true,
    };
    if keep {
        out.push(plan_row);
    }
    Ok(())
}

/// Render a fixed-point magnitude as the narrowest decimal `Value`.
fn generate_series_decimal_value(mag: i128, scale: u8) -> Value {
    match i64::try_from(mag) {
        Ok(compact) => Value::DecimalCompact(compact, scale),
        Err(_) => Value::Decimal(crate::util::decimal::format_decimal_parts(mag, scale)),
    }
}

/// Single-table B-tree index-scan nodes whose execution reads
/// partition-local indexes and consumes per-partition `RowId`s. On a
/// partitioned table each partition keeps its own physical index and a
/// `RowId` is only unique within one partition, so a scan spanning
/// several partitions returns ambiguous ids. Returns the target table
/// when `plan` is one of these nodes over a partitioned table so the
/// dispatcher can fan the scan out one partition at a time.
///
/// Vector/fulltext/spatial scans are intentionally excluded: the vector
/// scan already fans out internally, and spatial/fulltext fall back to a
/// sequential scan for partitioned tables in the planner.
fn partitioned_btree_index_scan_table(plan: &PhysicalPlan) -> Option<&PlanTable> {
    let table = match plan {
        PhysicalPlan::IndexMerge { table, .. }
        | PhysicalPlan::IndexLookup { table, .. }
        | PhysicalPlan::IndexMultiLookup { table, .. }
        | PhysicalPlan::IndexRangeScan { table, .. }
        | PhysicalPlan::IndexFastScan { table, .. }
        | PhysicalPlan::IndexInLookup { table, .. } => table,
        _ => return None,
    };
    table.schema.is_partitioned().then_some(table)
}

fn synthetic_plan_metas(relation: &str, columns: &[crate::types::Column]) -> Vec<ColMetaRef> {
    columns
        .iter()
        .map(|column| {
            intern_col_meta(ColMeta {
                invisible: false,
                table: relation.to_string(),
                alias: None,
                column: column.name.clone(),
                column_type: column.column_type,
                source: column.source.clone(),
            })
        })
        .collect()
}

fn materialize_apply_predicate(
    db: &Arc<Database>,
    txn: TxnView,
    input: &PhysicalPlan,
    predicate: &Expr,
) -> Result<Expr, QueryError> {
    let mut metas = Vec::new();
    collect_plan_fields(input, &mut metas);
    let template = PlanRow::from_fields(
        metas
            .into_iter()
            .map(|meta| PlanField {
                meta: intern_col_meta(meta),
                value: Value::Null,
            })
            .collect(),
    );
    Ok(
        super::eval::resolve_batch_constant_scalar_subqueries(db, txn, predicate, &template)?
            .map_or_else(|| predicate.clone(), |resolved| resolved.expr),
    )
}

pub(super) fn execute_plan_rows_inner(
    db: &Arc<Database>,
    txn: TxnView,
    plan: &PhysicalPlan,
) -> Result<Vec<PlanRow>, QueryError> {
    crate::engine::cancel::check_cancelled()?;
    // Partition fan-out: run a partitioned B-tree index scan once per
    // in-scope partition with a single-`storage_id` prune context
    // installed. Inside that context the index read touches exactly one
    // partition's local B-tree and every `RowId` is unambiguous, so the
    // heap read (via `read_row` / the partition-resolved heap handle)
    // routes to the right partition. When only one partition is in
    // scope we fall through and execute directly under the existing
    // context.
    if let Some(table) = partitioned_btree_index_scan_table(plan) {
        let _scope = table.resolved_partition_storage_ids(None).map(|_| {
            crate::engine::executor::partition_router::push_pruned_partitions(
                table.in_scope_storage_ids(),
            )
        });
        let in_scope = table.in_scope_storage_ids();
        if in_scope.is_empty() {
            return Ok(Vec::new());
        }
        if in_scope.len() > 1 {
            let mut out = Vec::new();
            for sid in in_scope {
                let part = crate::engine::executor::partition_router::with_pruned_partitions(
                    vec![sid],
                    || execute_plan_rows_inner(db, txn, plan),
                )?;
                out.extend(part);
            }
            return Ok(out);
        }
    }
    match plan {
        PhysicalPlan::NullExtend { input, table, .. } => {
            let mut rows = execute_plan_rows(db, txn, input)?;
            let fields = null_extended_table_fields(table)
                .into_iter()
                .map(|meta| PlanField {
                    meta: intern_col_meta(meta),
                    value: Value::Null,
                })
                .collect::<Vec<_>>();
            for row in &mut rows {
                row.fields.extend(fields.iter().cloned());
                row.schema = PlanRow::empty_schema();
            }
            Ok(rows)
        }

        PhysicalPlan::RecursiveUnion { .. } => Err(crate::storage::error(
            crate::error::catalog::ER_UNKNOWN_ERROR,
            "RecursiveUnion must be opened by the recursive CTE operator",
        )),
        PhysicalPlan::RrfFusion { .. } => {
            let mut stream = super::hybrid::open_plan(db, txn, plan)?;
            let mut rows = Vec::new();
            while let Some(row) = stream.next()? {
                rows.push(row);
            }
            Ok(rows)
        }
        PhysicalPlan::Materialize { input, .. } => execute_plan_rows(db, txn, input),
        PhysicalPlan::SetOp {
            left,
            right,
            op,
            all,
            ..
        } => {
            let left_rows = execute_plan_rows(db, txn, left)?;
            let right_rows = execute_plan_rows(db, txn, right)?;
            let mut metas = Vec::new();
            collect_plan_fields(left, &mut metas);
            let left_values = left_rows
                .into_iter()
                .map(|row| row.fields.into_iter().map(|field| field.value).collect())
                .collect::<Vec<Vec<Value>>>();
            let right_values = right_rows
                .into_iter()
                .map(|row| row.fields.into_iter().map(|field| field.value).collect())
                .collect::<Vec<Vec<Value>>>();
            let width = metas.len();
            if right_values.first().is_some_and(|row| row.len() != width) {
                return Err(crate::storage::error(
                    crate::error::catalog::ER_WRONG_NUMBER_OF_COLUMNS_IN_SELECT,
                    "the used SELECT statements have a different number of columns",
                ));
            }
            let (rows, _rows_charge) =
                crate::engine::executor::dispatch::combine_set_rows_spillable_charged(
                    &metas
                        .iter()
                        .map(|field| field.source.collation_id)
                        .collect::<Vec<_>>(),
                    *op,
                    *all,
                    left_values,
                    right_values,
                    width,
                    db.data_dir(),
                )?;
            let metas = metas.into_iter().map(intern_col_meta).collect::<Vec<_>>();
            Ok(rows
                .into_iter()
                .map(|values| PlanRow {
                    fields: metas
                        .iter()
                        .cloned()
                        .zip(values)
                        .map(|(meta, value)| PlanField { meta, value })
                        .collect(),
                    ..Default::default()
                })
                .collect())
        }
        PhysicalPlan::ValuesScan {
            rows,
            columns,
            filter,
            ..
        } => {
            let metas = synthetic_plan_metas("values", columns);
            let empty = PlanRow::default();
            let mut eval_ctx = super::eval::EvalQueryContext::default();
            let mut out = Vec::with_capacity(rows.len());
            for raw in rows {
                let mut fields = Vec::with_capacity(raw.len());
                for (expr, meta) in raw.iter().zip(&metas) {
                    let value =
                        super::eval::eval_plan_expr_with_db(db, txn, expr, &empty, &mut eval_ctx)?;
                    fields.push(PlanField {
                        meta: meta.clone(),
                        value,
                    });
                }
                let row = PlanRow {
                    fields,
                    ..Default::default()
                };
                let keep = match filter {
                    Some(predicate) => super::eval::eval_plan_bool(predicate, &row)?,
                    None => true,
                };
                if keep {
                    out.push(row);
                }
            }
            Ok(out)
        }
        PhysicalPlan::JsonTableScan {
            source,
            relation,
            columns,
            filter,
            ..
        } => {
            let metas = synthetic_plan_metas(relation, columns);
            let value = crate::engine::expr::eval_const(&source.expr)?;
            let roots = if matches!(value, Value::Null) {
                Vec::new()
            } else {
                let doc = super::json_table_doc_arg(&value)?;
                super::json_table_eval_path(&doc, &source.path)?
            };
            let mut out = Vec::new();
            for (index, root) in roots.iter().enumerate() {
                for values in
                    super::json_table_rows_for_columns(root, &source.columns, (index + 1) as u64)?
                {
                    let row = PlanRow {
                        fields: metas
                            .iter()
                            .zip(values)
                            .map(|(meta, value)| PlanField {
                                meta: meta.clone(),
                                value,
                            })
                            .collect(),
                        ..Default::default()
                    };
                    let keep = match filter {
                        Some(predicate) => super::eval::eval_plan_bool(predicate, &row)?,
                        None => true,
                    };
                    if keep {
                        out.push(row);
                    }
                }
            }
            Ok(out)
        }
        PhysicalPlan::SystemScan {
            name,
            relation,
            columns,
            filter,
            projected_columns,
            ..
        } => {
            let registry = super::current_session_registry().ok_or_else(|| {
                crate::storage::error(
                    crate::error::catalog::ER_UNKNOWN_ERROR,
                    "system scan opened without a statement SessionRegistry",
                )
            })?;
            let projected = projected_columns
                .as_deref()
                .map(|projection| projection.to_vec())
                .unwrap_or_else(|| (0..columns.len()).collect());
            let metas = projected
                .iter()
                .filter_map(|index| columns.get(*index).cloned())
                .collect::<Vec<_>>();
            let metas = synthetic_plan_metas(relation, &metas);
            let governed = super::system_table_rows(
                db,
                name,
                &registry,
                &super::vec_bridge::system_scan_request(filter.as_ref()),
            )?;
            let (values, _system_metadata_charge) = governed.into_parts();
            let mut out = Vec::with_capacity(values.len());
            for values in values {
                let values = projected
                    .iter()
                    .map(|index| values.get(*index).cloned().unwrap_or(Value::Null));
                let row = PlanRow {
                    fields: metas
                        .iter()
                        .zip(values)
                        .map(|(meta, value)| PlanField {
                            meta: meta.clone(),
                            value,
                        })
                        .collect(),
                    ..Default::default()
                };
                if filter
                    .as_ref()
                    .map(|predicate| super::eval::eval_plan_bool(predicate, &row))
                    .transpose()?
                    .unwrap_or(true)
                {
                    out.push(row);
                }
            }
            Ok(out)
        }
        PhysicalPlan::SeqScan {
            table,
            filter,
            runtime_filters,
            projected_columns,
            ..
        } => {
            let scan_txn = crate::engine::executor::temporal::txn_for_table(txn, table);
            if scan_txn.historical {
                db.record_temporal_heap_fallback();
            }
            scan_plan_table(
                db,
                scan_txn,
                table,
                filter,
                runtime_filters,
                projected_columns.as_deref().map(|v| v.as_slice()),
                false,
            )
        }
        PhysicalPlan::ZoneMapScan {
            table,
            filter,
            runtime_filters,
            projected_columns,
            ..
        } => {
            let scan_txn = crate::engine::executor::temporal::txn_for_table(txn, table);
            if scan_txn.historical {
                db.record_temporal_heap_fallback();
            }
            scan_plan_table(
                db,
                scan_txn,
                table,
                filter,
                runtime_filters,
                projected_columns.as_deref().map(|v| v.as_slice()),
                true,
            )
        }
        PhysicalPlan::IndexMerge {
            table,
            root,
            filter,
            projected_columns,
            ..
        } => {
            let txn = crate::engine::executor::temporal::txn_for_table(txn, table);
            let ids = execute_index_merge_row_ids(db, txn, &table.schema, root)?;
            charge_transient_materialization(
                ids.capacity().saturating_mul(std::mem::size_of::<RowId>()),
            )?;
            read_plan_table_ids(
                db,
                txn,
                table,
                ids,
                filter,
                projected_columns.as_deref().map(|v| v.as_slice()),
            )
        }
        PhysicalPlan::IndexLookup {
            table,
            index,
            key,
            exact,
            filter,
            covering,
            projected_columns,
            ..
        } => {
            let txn = crate::engine::executor::temporal::txn_for_table(txn, table);
            let projection = projected_columns.as_deref().map(|v| v.as_slice());
            let residual_filter = match filter.as_ref() {
                None => None,
                Some(expr) if db.has_active_write_txns() => Some(expr.clone()),
                Some(expr) => {
                    let access = EngineIndexAccess::Lookup {
                        key: key.clone(),
                        exact: *exact,
                    };
                    crate::engine::executor::indexes::index_access_residual_filter(
                        &table.schema,
                        index,
                        &access,
                        expr,
                    )
                }
            };
            if *covering {
                let (entries, page_max_trx_id) = if *exact {
                    db.lookup_index_entries_visible_with_bound(
                        &table.schema,
                        index,
                        key.clone(),
                        txn,
                    )?
                } else {
                    db.prefix_index_entries_visible_with_bound(
                        &table.schema,
                        index,
                        key.clone(),
                        txn,
                    )?
                };
                read_plan_table_entries_with_visibility_bound(
                    db,
                    txn,
                    table,
                    index,
                    entries,
                    page_max_trx_id,
                    &residual_filter,
                    projection,
                )
            } else {
                let ids = if *exact {
                    db.lookup_index_visible(&table.schema, index, key.clone(), txn)?
                } else {
                    db.prefix_index_visible(&table.schema, index, key.clone(), txn)?
                };
                read_plan_table_ids(db, txn, table, ids, &residual_filter, projection)
            }
        }
        PhysicalPlan::IndexMultiLookup {
            table,
            index,
            keys,
            exact,
            filter,
            covering,
            projected_columns,
            access_rows,
            ..
        } => {
            let txn = crate::engine::executor::temporal::txn_for_table(txn, table);
            let projection = projected_columns.as_deref().map(|v| v.as_slice());
            if *covering {
                let entries = execute_index_multi_lookup_entries(
                    db,
                    txn,
                    &table.schema,
                    index,
                    keys,
                    *exact,
                )?;
                read_plan_table_entries(db, txn, table, index, entries, filter, projection)
            } else {
                // Parallel fan-out for non-covering IndexMultiLookup. For
                // Q21-shape probes (small build × big fact joined on a
                // secondary index, ~240K row fetches) splitting the keyset
                // across workers turns the serial heap pass into ~N
                // parallel passes. Falls back transparently when the
                // probe is small / partitioned / parallelism disabled.
                super::parallel::execute_parallel_index_multi_lookup(
                    db,
                    txn,
                    table,
                    index,
                    keys,
                    *exact,
                    filter,
                    projection,
                    *access_rows,
                )
            }
        }
        PhysicalPlan::IndexRangeScan {
            table,
            index,
            low,
            high,
            filter,
            covering,
            reverse,
            projected_columns,
            ..
        } => {
            let txn = crate::engine::executor::temporal::txn_for_table(txn, table);
            let projection = projected_columns.as_deref().map(|v| v.as_slice());
            if *covering {
                read_plan_table_entries_streamed(
                    db,
                    txn,
                    table,
                    index,
                    low.clone(),
                    high.clone(),
                    *reverse,
                    filter,
                    projection,
                )
            } else {
                super::analyze_stats::with_range_scan_plan(plan, || {
                    read_plan_table_ids_range_chunked(
                        db,
                        txn,
                        table,
                        index,
                        low.clone(),
                        high.clone(),
                        *reverse,
                        filter,
                        projection,
                    )
                })
            }
        }
        PhysicalPlan::FulltextScan {
            table,
            index,
            mode,
            query,
            query_param,
            columns,
            match_fingerprint,
            filter,
            projected_columns,
            needs_score,
            restrict_to_matches,
            additional_matches,
            like_prefilter,
            base_access,
            ..
        } => {
            let txn = crate::engine::executor::temporal::txn_for_table(txn, table);
            execute_fulltext_scan(
                db,
                txn,
                table,
                index,
                *mode,
                query,
                *query_param,
                columns,
                *match_fingerprint,
                filter,
                projected_columns.as_ref(),
                *needs_score,
                *restrict_to_matches,
                additional_matches,
                like_prefilter.as_ref(),
                base_access.as_ref(),
            )
        }
        PhysicalPlan::SpatialScan {
            table,
            index,
            query_probe,
            filter,
            ..
        } => {
            let txn = crate::engine::executor::temporal::txn_for_table(txn, table);
            execute_spatial_scan(db, txn, table, index, query_probe, filter)
        }
        PhysicalPlan::VectorIndexScan {
            table,
            index,
            query_vector,
            query_vector_expr,
            metric,
            k,
            baked_offset,
            binding,
            ef_search,
            ef_hint,
            filter,
            order_by,
            ..
        } => {
            let txn = crate::engine::executor::temporal::txn_for_table(txn, table);
            let (k, _) = binding.resolve_k_offset(*k, *baked_offset);
            // Re-resolve `ef_search` from the *live* session, not the value
            // baked at plan time: the plan cache key doesn't include
            // `mydb_hnsw_ef_search`, so a cached VectorIndexScan would otherwise
            // pin whatever ef the first execution saw and silently ignore a
            // later `SET mydb_hnsw_ef_search`. An explicit `/*+ HNSW_EF(n) */`
            // (baked in `ef_hint`) still wins. Falls back to the baked value if
            // no session is installed (internal callers).
            let cfg = crate::engine::vector::session::current_config();
            let _ = ef_search; // baked planning-time value; superseded by the live re-resolution
                               // Resolve the algorithm-appropriate search effort from the *live*
                               // session: an ef candidate-pool size for HNSW, an nprobe cell count
                               // for IVF. Both flow through the same `ef` argument — the
                               // backend (`VectorGraph::search`) interprets it per its algorithm.
            let ef = match index.vector_algo() {
                crate::storage::schema::VectorAlgo::Ivf => {
                    let index_default = index.vector_meta().default_nprobe.map(|v| v as u64);
                    crate::engine::vector::session::effective_nprobe(&cfg, *ef_hint, index_default)
                }
                crate::storage::schema::VectorAlgo::Hnsw => {
                    let index_default_ef = index.vector_meta().default_ef_search.map(|v| v as u64);
                    crate::engine::vector::session::effective_ef_search(
                        &cfg,
                        *ef_hint,
                        index_default_ef,
                        k,
                    )
                }
            };
            let vector_col = table.schema.column_index(&index.columns[0])?;
            let query_vector =
                resolve_knn_query_vector(table, vector_col, query_vector, query_vector_expr)?;
            let metric = crate::engine::vector::Metric::parse(metric).unwrap_or_default();
            validate_knn_query_vector(&query_vector, metric)?;
            execute_vector_index_scan(
                db,
                txn,
                table,
                index,
                &query_vector,
                // The metric name was validated at plan time; default to L2 if a
                // cached plan ever carries an unrecognised string.
                metric,
                k,
                ef,
                filter,
                order_by,
            )
        }
        PhysicalPlan::VectorBruteScan {
            table,
            column_index,
            query_vector,
            query_vector_expr,
            metric,
            k,
            baked_offset,
            binding,
            ..
        } => {
            let txn = crate::engine::executor::temporal::txn_for_table(txn, table);
            let (k, _) = binding.resolve_k_offset(*k, *baked_offset);
            let query_vector =
                resolve_knn_query_vector(table, *column_index, query_vector, query_vector_expr)?;
            let metric = crate::engine::vector::Metric::parse(metric).unwrap_or_default();
            validate_knn_query_vector(&query_vector, metric)?;
            execute_vector_brute_scan(db, txn, table, *column_index, &query_vector, metric, k)
        }
        PhysicalPlan::IndexFastScan {
            table,
            index,
            low,
            high,
            filter,
            reverse,
            projected_columns,
            ..
        } => {
            let txn = crate::engine::executor::temporal::txn_for_table(txn, table);
            // Index-only path: walk leaf entries directly, never touching
            // the heap. Equivalent to the covering branch of IndexRangeScan
            // above, exposed as a distinct operator so cost and EXPLAIN
            // reflect the absence of heap I/O.
            read_plan_table_entries_streamed(
                db,
                txn,
                table,
                index,
                low.clone(),
                high.clone(),
                *reverse,
                filter,
                projected_columns.as_deref().map(|v| v.as_slice()),
            )
        }
        PhysicalPlan::IndexInLookup {
            table,
            index,
            key_prefix,
            in_column,
            subquery,
            subquery_plan,
            filter,
            covering,
            projected_columns,
            ..
        } => {
            let txn = crate::engine::executor::temporal::txn_for_table(txn, table);
            let projection = projected_columns.as_deref().map(|v| v.as_slice());
            if *covering {
                let entries = execute_index_in_lookup_entries(
                    db,
                    txn,
                    &table.schema,
                    index,
                    key_prefix,
                    in_column,
                    subquery,
                    subquery_plan,
                )?;
                read_plan_table_entries(db, txn, table, index, entries, filter, projection)
            } else {
                let (ids, _ids_charge) = execute_index_in_lookup_ids(
                    db,
                    txn,
                    &table.schema,
                    index,
                    key_prefix,
                    in_column,
                    subquery,
                    subquery_plan,
                )?;
                read_plan_table_ids(db, txn, table, ids, filter, projection)
            }
        }
        PhysicalPlan::NestedLoopJoin {
            left,
            right,
            join_type,
            condition,
            ..
        } => execute_nested_loop_join(db, txn, left, right, *join_type, condition.as_ref()),
        PhysicalPlan::IndexNestedLoopJoin {
            left,
            right_table,
            right_index,
            join_type,
            condition,
            right_covering,
            right_projected_columns,
            right_runtime_filters,
            ..
        } => execute_index_nested_loop_join(
            db,
            txn,
            left,
            right_table,
            right_index,
            *join_type,
            condition.as_ref(),
            *right_covering,
            right_projected_columns.as_deref().map(|v| v.as_slice()),
            right_runtime_filters,
        ),
        PhysicalPlan::HashJoin {
            left,
            right,
            join_type,
            left_keys,
            right_keys,
            null_safe,
            build_side,
            residual,
            output_projection,
            ..
        } => execute_hash_join(
            db,
            txn,
            left,
            right,
            *join_type,
            left_keys,
            right_keys,
            null_safe,
            *build_side,
            residual.as_ref(),
            output_projection.as_deref().map(|v| v.as_slice()),
        ),
        PhysicalPlan::AdaptiveJoin {
            left,
            right,
            join_type,
            candidates,
            preferred,
            ..
        } => execute_adaptive_join(db, txn, left, right, *join_type, candidates, *preferred),
        PhysicalPlan::HashAggregate {
            input,
            group_by,
            having,
            aggregates,
            ..
        } => execute_hash_aggregate_keyed(
            db,
            txn,
            input,
            group_by,
            having.as_ref(),
            aggregates,
            Some(plan),
        ),
        PhysicalPlan::HashRollupAggregate {
            input,
            group_by,
            having,
            aggregates,
            ..
        } => execute_hash_rollup_aggregate(db, txn, input, group_by, having.as_ref(), aggregates),
        PhysicalPlan::HashGroupingSetsAggregate {
            input,
            group_by,
            grouping_sets,
            having,
            aggregates,
            ..
        } => execute_hash_grouping_sets_aggregate(
            db,
            txn,
            input,
            group_by,
            grouping_sets,
            having.as_ref(),
            aggregates,
        ),
        PhysicalPlan::ParallelHashAggregate {
            input,
            group_by,
            having,
            aggregates,
            ..
        } => execute_hash_aggregate_keyed(
            db,
            txn,
            input,
            group_by,
            having.as_ref(),
            aggregates,
            Some(plan),
        ),
        PhysicalPlan::ParallelRollupAggregate {
            input,
            group_by,
            having,
            aggregates,
            degree,
            estimated_rows,
            ..
        } => super::parallel::execute_parallel_rollup_aggregate(
            db,
            txn,
            input,
            group_by,
            having.as_ref(),
            aggregates,
            *degree,
            *estimated_rows,
        ),
        PhysicalPlan::ParallelGroupingSetsAggregate {
            input,
            group_by,
            grouping_sets,
            having,
            aggregates,
            ..
        } => execute_hash_grouping_sets_aggregate(
            db,
            txn,
            input,
            group_by,
            grouping_sets,
            having.as_ref(),
            aggregates,
        ),
        PhysicalPlan::ParallelSort {
            input,
            order_by,
            degree,
            ..
        } => super::parallel::execute_parallel_sort(db, txn, input, order_by, *degree),
        PhysicalPlan::Sort {
            input, order_by, ..
        } => {
            let rows = execute_plan_rows(db, txn, input)?;
            super::external_sort::sort_plan_rows_for_plan(db.data_dir(), rows, order_by, Some(plan))
        }
        PhysicalPlan::IncrementalSort {
            input,
            order_by,
            presorted_prefix,
            ..
        } => execute_incremental_sort(db, txn, input, order_by, *presorted_prefix),
        PhysicalPlan::TopK {
            input,
            order_by,
            dedup_projection,
            k,
            offset,
            binding,
            ..
        } => {
            let (k, _) = binding.resolve_k_offset(*k, *offset);
            match dedup_projection {
                Some(projection) => execute_distinct_topk(db, txn, input, order_by, projection, k),
                None => execute_topk(db, txn, input, order_by, k),
            }
        }
        PhysicalPlan::TopWindow {
            input,
            order_by,
            limit,
            offset,
            binding,
            ..
        } => {
            let (limit, offset) = binding.resolve_limit_offset(*limit, *offset);
            execute_topwindow(db, txn, input, order_by, limit, offset)
        }
        PhysicalPlan::ParallelTopK {
            input,
            order_by,
            dedup_projection,
            k,
            offset,
            binding,
            degree,
            ..
        } => {
            let (k, _) = binding.resolve_k_offset(*k, *offset);
            match dedup_projection {
                Some(projection) => super::parallel::execute_parallel_distinct_topk(
                    db, txn, input, order_by, projection, k, *degree,
                ),
                None => {
                    super::parallel::execute_parallel_topk(db, txn, input, order_by, k, *degree)
                }
            }
        }
        PhysicalPlan::Filter {
            input, predicate, ..
        } => {
            // Genuine post-join WHERE filter: keep only rows where the
            // predicate is TRUE, dropping NULL-extended outer-join rows that
            // fail it (the residual-fold bug this node fixes). `_with_db`
            // supports a predicate carrying a subquery.
            let rows = execute_plan_rows(db, txn, input)?;
            let mut eval_ctx = EvalQueryContext::default();
            let mut out = Vec::with_capacity(rows.len());
            // The input schema is stable for the lifetime of this Filter.
            // Bind identifiers, type metadata and collation once instead of
            // walking the AST and resolving names for every row. Unsupported
            // or subquery-bearing shapes keep the DB-aware legacy evaluator
            // through CompiledPlanPredicate::eval_bool_with_db.
            let compiled = rows
                .first()
                .map(|row| CompiledPlanPredicate::compile_once(predicate, row));
            for row in rows {
                let keep = match compiled.as_ref() {
                    Some(predicate) => predicate.eval_bool_with_db(db, txn, &row, &mut eval_ctx)?,
                    None => eval_plan_bool_with_db(db, txn, predicate, &row, &mut eval_ctx)?,
                };
                if keep {
                    out.push(row);
                }
            }
            Ok(out)
        }
        PhysicalPlan::Apply {
            outer, predicate, ..
        } => {
            let predicate = materialize_apply_predicate(db, txn, outer, predicate)?;
            let rows = execute_plan_rows(db, txn, outer)?;
            let mut eval_ctx = EvalQueryContext::default();
            let mut out = Vec::with_capacity(rows.len());
            let compiled = rows
                .first()
                .map(|row| CompiledPlanPredicate::compile_once(&predicate, row));
            for row in rows {
                let keep = match compiled.as_ref() {
                    Some(compiled) => compiled.eval_bool_with_db(db, txn, &row, &mut eval_ctx)?,
                    None => eval_plan_bool_with_db(db, txn, &predicate, &row, &mut eval_ctx)?,
                };
                if keep {
                    out.push(row);
                }
            }
            Ok(out)
        }
        PhysicalPlan::Projection {
            input,
            projection,
            distinct,
            ..
        } => {
            let mut rows = execute_plan_rows(db, txn, input)?;
            if *distinct {
                dedup_projected_rows(&mut rows, projection, db.data_dir())?;
            }
            Ok(rows)
        }
        PhysicalPlan::Limit {
            input,
            limit,
            offset,
            binding,
            ..
        } => execute_limit_plan_rows(
            db,
            txn,
            input,
            binding.resolve_limit(*limit),
            binding.resolve_offset(*offset),
        ),
        PhysicalPlan::WindowAgg { input, windows, .. } => {
            let rows = execute_plan_rows(db, txn, input)?;
            execute_window_agg(db, rows, windows, plan)
        }
        PhysicalPlan::CteScan { .. } => {
            let mut input = open_cte_scan_stream(plan)?;
            let mut rows = Vec::new();
            while let Some(row) = input.next()? {
                rows.push(row);
            }
            Ok(rows)
        }
        PhysicalPlan::GenerateSeries {
            series,
            relation,
            with_ordinality,
            columns,
            filter,
            ..
        } => {
            // Interned field templates built once; the per-row loop only swaps
            // in values. Integer family (M2); a streaming / vectorized leaf is
            // M5, so a very large range is materialised here for now.
            let templates: Vec<PlanField> = columns
                .iter()
                .map(|col| PlanField {
                    meta: intern_col_meta(ColMeta {
                        invisible: false,
                        table: relation.clone(),
                        alias: Some(relation.clone()),
                        column: col.name.clone(),
                        column_type: col.column_type,
                        source: Default::default(),
                    }),
                    value: Value::Null,
                })
                .collect();
            let mut out: Vec<PlanRow> = Vec::new();
            let mut ordinal: i64 = 0;
            match series {
                crate::planner::SeriesKind::Int { start, stop, step } => {
                    let (stop, step) = (*stop, *step);
                    let mut current = *start;
                    loop {
                        let in_range = if step > 0 {
                            current <= stop
                        } else {
                            current >= stop
                        };
                        if !in_range {
                            break;
                        }
                        ordinal += 1;
                        if ordinal % 8192 == 0 {
                            crate::engine::cancel::check_cancelled()?;
                        }
                        push_generate_series_row(
                            &mut out,
                            &templates,
                            *with_ordinality,
                            ordinal,
                            Value::Int(current),
                            filter.as_ref(),
                        )?;
                        match current.checked_add(step) {
                            Some(next) => current = next,
                            None => break, // i64 overflow at boundary: stop cleanly
                        }
                    }
                }
                crate::planner::SeriesKind::Dec {
                    start,
                    stop,
                    step,
                    scale,
                } => {
                    let (stop, step, scale) = (*stop, *step, *scale);
                    let mut current = *start;
                    loop {
                        let in_range = if step > 0 {
                            current <= stop
                        } else {
                            current >= stop
                        };
                        if !in_range {
                            break;
                        }
                        ordinal += 1;
                        if ordinal % 8192 == 0 {
                            crate::engine::cancel::check_cancelled()?;
                        }
                        push_generate_series_row(
                            &mut out,
                            &templates,
                            *with_ordinality,
                            ordinal,
                            generate_series_decimal_value(current, scale),
                            filter.as_ref(),
                        )?;
                        match current.checked_add(step) {
                            Some(next) => current = next,
                            None => break, // i128 overflow at boundary: stop cleanly
                        }
                    }
                }
                crate::planner::SeriesKind::Ts {
                    start,
                    stop,
                    amount,
                    unit,
                    ascending,
                } => {
                    let (stop, ascending) = (*stop, *ascending);
                    let mut current = *start;
                    loop {
                        let in_range = if ascending {
                            current <= stop
                        } else {
                            current >= stop
                        };
                        if !in_range {
                            break;
                        }
                        ordinal += 1;
                        if ordinal % 8192 == 0 {
                            crate::engine::cancel::check_cancelled()?;
                        }
                        push_generate_series_row(
                            &mut out,
                            &templates,
                            *with_ordinality,
                            ordinal,
                            Value::DateTime(current),
                            filter.as_ref(),
                        )?;
                        let next = crate::engine::expr::add_interval_to_micros(
                            current, amount, unit, false,
                        )?;
                        if next == current {
                            break; // zero-length interval safety
                        }
                        current = next;
                    }
                }
            }
            Ok(out)
        }
        PhysicalPlan::StreamingAggregate {
            input,
            group_by,
            having,
            aggregates,
            ..
        } => execute_streaming_aggregate(db, txn, input, group_by, having.as_ref(), aggregates),
        PhysicalPlan::StreamingRollupAggregate {
            input,
            group_by,
            having,
            aggregates,
            ..
        } => execute_streaming_rollup_aggregate(
            db,
            txn,
            input,
            group_by,
            having.as_ref(),
            aggregates,
        ),
        PhysicalPlan::StreamingGroupingSetsAggregate {
            input,
            group_by,
            grouping_sets,
            having,
            aggregates,
            ..
        } => execute_hash_grouping_sets_aggregate(
            db,
            txn,
            input,
            group_by,
            grouping_sets,
            having.as_ref(),
            aggregates,
        ),
        PhysicalPlan::StreamingDistinct {
            input, projection, ..
        } => execute_streaming_distinct(db, txn, input, projection),
        PhysicalPlan::IndexAggregate {
            table,
            index,
            low,
            high,
            filter,
            op,
            agg_key,
            output_alias,
            output_type,
            ..
        } => execute_index_aggregate(
            db,
            txn,
            table,
            index,
            low.clone(),
            high.clone(),
            filter.as_ref(),
            op,
            *agg_key,
            output_alias,
            *output_type,
        ),
        PhysicalPlan::CoveringIndexAggregate {
            table,
            index,
            low,
            high,
            group_by,
            having,
            aggregates,
            ..
        } => execute_covering_index_aggregate(
            db,
            txn,
            plan,
            table,
            index,
            low.clone(),
            high.clone(),
            group_by,
            having.as_ref(),
            aggregates,
        ),
        PhysicalPlan::ParallelIndexAggregate {
            table,
            index,
            low,
            high,
            group_by,
            having,
            aggregates,
            degree,
            ..
        } => super::parallel::execute_parallel_index_aggregate(
            db,
            txn,
            plan,
            table,
            index,
            low.clone(),
            high.clone(),
            group_by,
            having.as_ref(),
            aggregates,
            *degree,
        ),
        PhysicalPlan::ParallelSeqScan {
            table,
            filter,
            runtime_filters,
            projected_columns,
            degree,
            ..
        } => super::parallel::execute_parallel_seq_scan(
            db,
            txn,
            table,
            filter.as_ref(),
            runtime_filters,
            projected_columns.as_deref().map(|v| v.as_slice()),
            *degree,
            false,
        ),
        PhysicalPlan::ParallelZoneMapScan {
            table,
            filter,
            runtime_filters,
            projected_columns,
            degree,
            ..
        } => super::parallel::execute_parallel_seq_scan(
            db,
            txn,
            table,
            filter.as_ref(),
            runtime_filters,
            projected_columns.as_deref().map(|v| v.as_slice()),
            *degree,
            true,
        ),
        PhysicalPlan::ParallelIndexFastScan {
            table,
            index,
            low,
            high,
            filter,
            projected_columns,
            degree,
            ..
        } => super::parallel::execute_parallel_index_fast_scan(
            db,
            txn,
            table,
            index,
            low.clone(),
            high.clone(),
            filter.as_ref(),
            projected_columns.as_deref().map(|v| v.as_slice()),
            *degree,
        ),
        PhysicalPlan::ParallelIndexRangeScan {
            table,
            index,
            low,
            high,
            filter,
            projected_columns,
            degree,
            ..
        } => super::parallel::execute_parallel_index_range_scan(
            db,
            txn,
            table,
            index,
            low.clone(),
            high.clone(),
            filter.as_ref(),
            projected_columns.as_deref().map(|v| v.as_slice()),
            *degree,
        ),
        PhysicalPlan::Exchange {
            input,
            degree,
            output_order,
            merge_keys,
            ..
        } => super::parallel::execute_exchange(db, txn, input, *degree, *output_order, merge_keys),
    }
}

fn range_index_entries_visible_ordered(
    db: &Arc<Database>,
    txn: TxnView,
    table: &PlanTable,
    index: &IndexSchema,
    low: Option<IndexBound>,
    high: Option<IndexBound>,
    reverse: bool,
) -> Result<Vec<(IndexKey, RowId)>, QueryError> {
    if !reverse {
        if db.has_pending_index_repairs_for(table.schema.id, &index.name) {
            return db.range_index_bounds_entries_visible(&table.schema, index, low, high, txn);
        }
        const CHUNK: usize = 4096;
        let mut out = Vec::new();
        let heap = index_scan_heap(db, table)?;
        let mut cursor =
            db.range_index_snapshot_cursor(&table.schema, index, low.clone(), high.clone(), txn)?;
        let mut seen = (!index.unique).then(std::collections::HashSet::new);
        while !cursor.is_finished() {
            let (chunk, page_max_trx_id) = cursor.next_leaf_chunk(CHUNK)?;
            if chunk.is_empty() {
                debug_assert!(
                    cursor.is_finished(),
                    "empty leaf chunk must exhaust the scan"
                );
                break;
            }
            // InnoDB-style PAGE_MAX_TRX_ID shortcut: if the leaf's
            // writer bound proves visibility to `txn` and no pending
            // index repairs are queued, every entry on this leaf is
            // authoritative and the per-entry heap-header probe is skipped.
            let visible_mask = db
                .try_index_page_max_trx_shortcut_for(
                    table.schema.id,
                    &index.name,
                    page_max_trx_id,
                    txn,
                    chunk.len(),
                )
                .map(Ok::<_, QueryError>)
                .unwrap_or_else(|| {
                    db.filter_visible_index_entries_mask_with_handle(
                        &heap,
                        &table.schema,
                        index,
                        &chunk,
                        txn,
                    )
                })?;
            for ((key, row_id), visible) in chunk.into_iter().zip(visible_mask.into_iter()) {
                if !visible {
                    continue;
                }
                if seen.as_mut().is_some_and(|seen| !seen.insert(row_id)) {
                    continue;
                }
                out.push((key, row_id));
            }
        }
        return Ok(out);
    }
    const REV_CHUNK: usize = 4096;
    let mut out = Vec::new();
    let heap = index_scan_heap(db, table)?;
    let mut cursor = db.range_index_snapshot_reverse_cursor(
        &table.schema,
        index,
        low.clone(),
        high.clone(),
        txn,
    )?;
    let mut seen = (!index.unique).then(std::collections::HashSet::new);
    while !cursor.is_finished() {
        let (chunk, page_max_trx_id) = cursor.next_leaf_chunk(REV_CHUNK)?;
        if chunk.is_empty() {
            break;
        }
        let mut entries = Vec::with_capacity(chunk.len());
        for entry in chunk {
            if seen.as_mut().is_some_and(|seen| !seen.insert(entry.1)) {
                continue;
            }
            entries.push(entry);
        }
        let visible_mask = db
            .try_index_page_max_trx_shortcut_for(
                table.schema.id,
                &index.name,
                page_max_trx_id,
                txn,
                entries.len(),
            )
            .map(Ok::<_, QueryError>)
            .unwrap_or_else(|| {
                db.filter_visible_index_entries_mask_with_handle(
                    &heap,
                    &table.schema,
                    index,
                    &entries,
                    txn,
                )
            })?;
        for ((key, row_id), visible) in entries.into_iter().zip(visible_mask.into_iter()) {
            if visible {
                out.push((key, row_id));
            }
        }
    }
    Ok(out)
}

fn range_index_ids_visible_ordered(
    db: &Arc<Database>,
    txn: TxnView,
    table: &PlanTable,
    index: &IndexSchema,
    low: Option<IndexBound>,
    high: Option<IndexBound>,
    reverse: bool,
) -> Result<Vec<RowId>, QueryError> {
    if !reverse {
        if db.has_pending_index_repairs_for(table.schema.id, &index.name) {
            return db.range_index_bounds_visible(&table.schema, index, low, high, txn);
        }
        const CHUNK: usize = 4096;
        let mut out = Vec::new();
        let heap = index_scan_heap(db, table)?;
        let mut cursor =
            db.range_index_snapshot_cursor(&table.schema, index, low.clone(), high.clone(), txn)?;
        let mut seen = (!index.unique).then(std::collections::HashSet::new);
        while !cursor.is_finished() {
            // Ids-only consumer: skip the per-entry IndexKey clone.
            let (chunk, page_max_trx_id) = cursor.next_leaf_chunk_row_ids(CHUNK)?;
            if chunk.is_empty() {
                break;
            }
            let visible_mask = db
                .try_index_page_max_trx_shortcut_for(
                    table.schema.id,
                    &index.name,
                    page_max_trx_id,
                    txn,
                    chunk.len(),
                )
                .map(Ok::<_, QueryError>)
                .unwrap_or_else(|| {
                    db.filter_visible_row_ids_mask_with_handle(&heap, &table.schema, &chunk, txn)
                })?;
            for (row_id, visible) in chunk.into_iter().zip(visible_mask.into_iter()) {
                if !visible {
                    continue;
                }
                if seen.as_mut().is_some_and(|seen| !seen.insert(row_id)) {
                    continue;
                }
                out.push(row_id);
            }
        }
        return Ok(out);
    }
    range_index_entries_visible_ordered(db, txn, table, index, low, high, true)
        .map(|entries| entries.into_iter().map(|(_, row_id)| row_id).collect())
}

pub(super) trait PlanStream {
    fn next(&mut self) -> Result<Option<PlanRow>, QueryError>;

    fn install_runtime_filter(&mut self, _filter: RuntimeFilter) -> bool {
        false
    }
}

pub(super) struct LimitedSeqScanStream {
    rows: std::vec::IntoIter<PlanRow>,
}

impl LimitedSeqScanStream {
    fn new(
        db: &Arc<Database>,
        txn: TxnView,
        table: &PlanTable,
        filter: &Option<Expr>,
        runtime_filters: &[RuntimeFilter],
        projected_columns: Option<&[usize]>,
        max_rows: usize,
    ) -> Result<Self, QueryError> {
        if max_rows == 0 {
            return Ok(Self {
                rows: Vec::new().into_iter(),
            });
        }
        if filter
            .as_ref()
            .is_some_and(super::simple::expr_has_subquery)
        {
            return Ok(Self {
                rows: scan_plan_table(
                    db,
                    txn,
                    table,
                    filter,
                    runtime_filters,
                    projected_columns,
                    false,
                )?
                .into_iter(),
            });
        }
        if let Some(projection) = projected_columns {
            let templates: Vec<PlanField> = projection
                .iter()
                .map(|&i| PlanField {
                    meta: intern_col_meta(ColMeta {
                        invisible: table.schema.columns[i].invisible,
                        table: table.schema.name.clone(),
                        alias: table.alias.clone(),
                        column: table.schema.columns[i].name.clone(),
                        column_type: table.schema.result_type(i),
                        source: Default::default(),
                    }),
                    value: Value::Null,
                })
                .collect();
            let qualified = db.storage_key_for_table(&table.schema);
            let mut rows = Vec::with_capacity(max_rows.min(64));
            let mut eval_ctx = EvalQueryContext::default();
            run_plan_scan_with_partition_pruning(table, filter, || {
                db.scan_rows_columns_streaming_until(&qualified, txn, projection, |values| {
                    crate::engine::cancel::check_cancelled()?;
                    let mut fields = templates.clone();
                    for (i, field) in fields.iter_mut().enumerate() {
                        field.value = values[i].clone();
                    }
                    let row = PlanRow {
                        fields,
                        group: None,
                        agg_values: None,
                        schema: PlanRow::empty_schema(),
                    };
                    if !runtime_filters_admit(runtime_filters, &row)? {
                        HASH_JOIN_BLOOM_PROBE_FILTERED.fetch_add(1, Ordering::Relaxed);
                        return Ok(false);
                    }
                    let keep = match filter {
                        Some(expr) => eval_plan_bool_with_db(db, txn, expr, &row, &mut eval_ctx)?,
                        None => true,
                    };
                    if keep {
                        rows.push(row);
                    }
                    Ok(rows.len() >= max_rows)
                })
            })?;
            return Ok(Self {
                rows: rows.into_iter(),
            });
        }
        let qualified = db.storage_key_for_table(&table.schema);
        let mut eval_ctx = EvalQueryContext::default();
        let stored_rows = run_plan_scan_with_partition_pruning(table, filter, || {
            db.scan_rows_limited(
                &qualified,
                txn,
                |stored| {
                    let row = stored_row_to_plan_row(table, stored.values.clone())?;
                    match filter {
                        Some(expr) => eval_plan_bool_with_db(db, txn, expr, &row, &mut eval_ctx),
                        None => Ok(true),
                    }
                },
                0,
                Some(max_rows),
            )
        })?;
        let mut rows = Vec::with_capacity(stored_rows.len());
        for stored in stored_rows {
            rows.push(stored_row_to_plan_row(table, stored.values)?);
        }
        Ok(Self {
            rows: rows.into_iter(),
        })
    }

    fn len(&self) -> usize {
        self.rows.len()
    }
}

impl PlanStream for LimitedSeqScanStream {
    fn next(&mut self) -> Result<Option<PlanRow>, QueryError> {
        Ok(self.rows.next())
    }
}

pub(super) struct MaterializedStream {
    iter: std::vec::IntoIter<PlanRow>,
}

impl MaterializedStream {
    pub(super) fn new(rows: Vec<PlanRow>) -> Self {
        Self {
            iter: rows.into_iter(),
        }
    }
}

impl PlanStream for MaterializedStream {
    fn next(&mut self) -> Result<Option<PlanRow>, QueryError> {
        Ok(self.iter.next())
    }
}

/// EXPLAIN ANALYZE wrapper for an operator whose result is consumed through
/// `PlanStream`. Collection-mode operators are timed by `execute_plan_rows`;
/// without this adapter a streaming Sort was displayed as "never executed"
/// even though its rows were consumed by a StreamingAggregate.
struct AnalyzedPlanStream {
    plan_key: PlanNodeId,
    transparent_child_key: Option<PlanNodeId>,
    open_ns: u128,
    active_ns: u128,
    loop_started: Option<std::time::Instant>,
    first_row_ns: Option<u128>,
    activated: bool,
    operator_io: std::sync::Arc<crate::storage::QueryBufferStats>,
    loops_before: u64,
    rows: u64,
    finished: bool,
    range_plan_key: Option<PlanNodeId>,
    partition_visits: Option<(PlanNodeId, std::sync::Arc<std::sync::atomic::AtomicU64>)>,
    runtime_filter_rejected: Option<(
        PlanNodeId,
        Vec<std::sync::Arc<std::sync::atomic::AtomicU64>>,
    )>,
    zone_before: Option<(u64, u64, u64, u64, u64, u64, u64)>,
    inner: Option<Box<dyn PlanStream>>,
}

impl AnalyzedPlanStream {
    fn new(
        plan: &PhysicalPlan,
        started: std::time::Instant,
        operator_io: std::sync::Arc<crate::storage::QueryBufferStats>,
        loops_before: u64,
        partition_visits: Option<(PlanNodeId, std::sync::Arc<std::sync::atomic::AtomicU64>)>,
        runtime_filter_rejected: Option<(
            PlanNodeId,
            Vec<std::sync::Arc<std::sync::atomic::AtomicU64>>,
        )>,
        inner: Box<dyn PlanStream>,
    ) -> Self {
        let open_ns = started.elapsed().as_nanos();
        Self {
            plan_key: super::analyze_stats::plan_key(plan)
                .expect("analyzed stream requires an active stats collector"),
            transparent_child_key: match plan {
                PhysicalPlan::Projection {
                    input,
                    distinct: false,
                    ..
                } => super::analyze_stats::plan_key(input),
                _ => None,
            },
            open_ns,
            active_ns: 0,
            loop_started: None,
            first_row_ns: None,
            activated: false,
            operator_io,
            loops_before,
            rows: 0,
            finished: false,
            range_plan_key: matches!(
                plan,
                PhysicalPlan::IndexMerge { .. }
                    | PhysicalPlan::IndexRangeScan { .. }
                    | PhysicalPlan::ParallelIndexRangeScan { .. }
            )
            .then(|| super::analyze_stats::plan_key(plan))
            .flatten(),
            partition_visits,
            runtime_filter_rejected,
            zone_before: matches!(
                plan,
                PhysicalPlan::ZoneMapScan { .. } | PhysicalPlan::ParallelZoneMapScan { .. }
            )
            .then(zone_map_runtime_snapshot),
            inner: Some(inner),
        }
    }

    fn finish(&mut self) {
        if self.finished {
            return;
        }
        self.finished = true;

        // Self-instrumented streaming operators publish from Drop, while
        // materialized fallbacks may already have published during stream
        // construction. Finalize the inner stream first, then use this
        // adapter only if no lower layer completed a loop for this node.
        let operator_io = std::sync::Arc::clone(&self.operator_io);
        crate::storage::with_shared_operator_buffer_stats(operator_io, || {
            drop(self.inner.take());
        });
        if !self.activated {
            return;
        }
        self.active_ns = self
            .loop_started
            .map(|started| started.elapsed().as_nanos())
            .unwrap_or(0);
        super::analyze_stats::record_key_if_loops_unchanged_timing(
            self.plan_key,
            self.loops_before,
            self.first_row_ns.unwrap_or(self.open_ns + self.active_ns),
            self.open_ns + self.active_ns,
            self.rows,
            {
                let (cache_reads, physical_reads) =
                    crate::storage::query_buffer_stats_counts(&self.operator_io);
                super::analyze_stats::NodeIoStats {
                    cache_reads,
                    physical_reads,
                }
            },
        );
        if let Some(child_key) = self.transparent_child_key {
            super::analyze_stats::mirror_key_if_absent(self.plan_key, child_key);
        }
        if let Some(before) = self.zone_before {
            let after = zone_map_runtime_snapshot();
            let candidate_ns = after.1.saturating_sub(before.1);
            let observed_fallbacks = after.5.saturating_sub(before.5);
            super::analyze_stats::add_zone_map_runtime_key(
                self.plan_key,
                super::analyze_stats::ZoneMapRuntimeStats {
                    metadata_ns: after.0.saturating_sub(before.0),
                    candidate_ns,
                    heap_scan_ns: self
                        .open_ns
                        .saturating_add(self.active_ns)
                        .min(u64::MAX as u128)
                        .saturating_sub(candidate_ns as u128)
                        as u64,
                    tasks: after.2.saturating_sub(before.2),
                    pages_scanned: after.3.saturating_sub(before.3),
                    pages_skipped: after.4.saturating_sub(before.4),
                    fallbacks: observed_fallbacks + u64::from(candidate_ns == 0),
                    workers: after.6.saturating_sub(before.6),
                },
            );
        }
        if let Some((key, counter)) = &self.partition_visits {
            super::analyze_stats::add_partitions_key(
                *key,
                counter.load(std::sync::atomic::Ordering::Relaxed),
            );
        }
        if let Some((key, counters)) = self.runtime_filter_rejected.take() {
            let rejected = counters
                .iter()
                .map(|counter| counter.load(std::sync::atomic::Ordering::Relaxed))
                .sum();
            super::analyze_stats::add_runtime_filter_rejected_key(key, rejected);
        }
    }
}

impl PlanStream for AnalyzedPlanStream {
    fn next(&mut self) -> Result<Option<PlanRow>, QueryError> {
        if self.finished {
            return Ok(None);
        }
        self.activated = true;
        let loop_started = *self
            .loop_started
            .get_or_insert_with(std::time::Instant::now);
        let mut inner = || {
            self.inner
                .as_mut()
                .expect("unfinished analyzed stream is missing its inner stream")
                .next()
        };
        let operator_io = std::sync::Arc::clone(&self.operator_io);
        let result = crate::storage::with_shared_operator_buffer_stats(operator_io, || {
            match (
                self.range_plan_key,
                self.partition_visits
                    .as_ref()
                    .map(|(_, counter)| counter.clone()),
            ) {
                (None, None) => inner(),
                (range_key, partition_counter) => {
                    super::analyze_stats::with_range_scan_plan_key_if_present(range_key, || {
                        crate::engine::executor::partition_router::with_partition_visit_collector(
                            partition_counter,
                            inner,
                        )
                    })
                }
            }
        });
        if self.first_row_ns.is_none() {
            self.first_row_ns = Some(
                self.open_ns
                    .saturating_add(loop_started.elapsed().as_nanos()),
            );
        }
        match result {
            Ok(Some(row)) => {
                self.rows = self.rows.saturating_add(1);
                Ok(Some(row))
            }
            Ok(None) => {
                self.finish();
                Ok(None)
            }
            Err(err) => {
                self.finish();
                Err(err)
            }
        }
    }
}

impl Drop for AnalyzedPlanStream {
    fn drop(&mut self) {
        // LIMIT and error paths may stop before EOF. Record the work that was
        // actually performed instead of leaving the node as never executed.
        self.finish();
    }
}

/// Stream-aware projection: pulls one row at a time from `inner` and
/// reshapes it through `projection`. `distinct=true` is NOT handled
/// here — see `dedup_projected_rows` for the materialised dedup path.
/// Stream-aware LIMIT/OFFSET: pulls from `inner`, drops the first
/// `skip` rows, then yields up to `remaining` rows before terminating.
pub(super) struct LimitStream {
    inner: Box<dyn PlanStream>,
    skip: usize,
    remaining: usize,
}

impl PlanStream for LimitStream {
    fn next(&mut self) -> Result<Option<PlanRow>, QueryError> {
        while self.skip > 0 {
            match self.inner.next()? {
                Some(_) => self.skip -= 1,
                None => return Ok(None),
            }
        }
        if self.remaining == 0 {
            return Ok(None);
        }
        match self.inner.next()? {
            Some(row) => {
                self.remaining -= 1;
                Ok(Some(row))
            }
            None => Ok(None),
        }
    }
}

/// Stream-aware post-join WHERE filter: pulls one row at a time from
/// `inner`, keeping only rows whose predicate evaluates TRUE (mirrors
/// the materialised `Filter` arm exactly — same `eval_plan_bool_with_db`,
/// dropping NULL-extended outer-join rows that fail). Lets a TopK /
/// Limit consumer bound memory when the join beneath streams: rows that
/// fail the filter never accumulate.
pub(super) struct FilterStream {
    db: Arc<Database>,
    txn: TxnView,
    predicate: Expr,
    compiled: Option<CompiledPlanPredicate>,
    inner: Box<dyn PlanStream>,
    eval_ctx: EvalQueryContext,
}

impl PlanStream for FilterStream {
    fn next(&mut self) -> Result<Option<PlanRow>, QueryError> {
        while let Some(row) = self.inner.next()? {
            let predicate = self
                .compiled
                .get_or_insert_with(|| CompiledPlanPredicate::compile_once(&self.predicate, &row));
            if predicate.eval_bool_with_db(&self.db, self.txn, &row, &mut self.eval_ctx)? {
                return Ok(Some(row));
            }
        }
        Ok(None)
    }
}

/// Build a `PlanStream` for `plan`. Streamable wrappers are produced
/// for Projection (non-distinct), Filter, Limit, and chain-streamable
/// HashJoin spines (see `join_stream`); everything else falls
/// back to `execute_plan_rows` and is wrapped as a
/// `MaterializedStream`.
///
/// The returned stream is fully owned (`'static`): every streaming
/// constructor clones what it needs into its worker (see
/// `stream::seq_scan_stream`), and the materialised fallback owns its
/// rows. Chain-streaming relies on this — a join level opens its probe
/// stream from a locally-cloned (runtime-filter-injected) plan and the
/// stream legitimately outlives that clone.
pub(super) fn execute_plan_stream(
    db: &Arc<Database>,
    txn: TxnView,
    plan: &PhysicalPlan,
) -> Result<Box<dyn PlanStream>, QueryError> {
    // Register the complete immutable tree before any constructor can open a
    // child worker. This fixes IDs to deterministic preorder independent of
    // which operator happens to publish its first runtime observation.
    let plan_key = super::analyze_stats::plan_key(plan);
    let started = super::analyze_stats::is_recording().then(std::time::Instant::now);
    let loops_before = plan_key
        .map(super::analyze_stats::recorded_loops_key)
        .unwrap_or(0);
    // Install scan sidecars before opening the inner stream. Parallel stream
    // constructors spawn workers immediately and must inherit this exact
    // collector; installing it only in `next()` is too late for them.
    let partition_visits = started
        .is_some()
        .then(|| partitioned_scan_observer(plan))
        .flatten();
    let runtime_filter_rejected = started
        .is_some()
        .then(|| runtime_filter_observer(plan))
        .flatten();
    // Native batch operators already carry per-node ANALYZE envelopes and
    // publish rows/timing once per batch. Wrapping their final PlanRow cursor
    // again would add one observability call per result row (dominant for
    // large scans/joins) and double-own the same node. Keep the row adapter
    // only for the explicit scalar oracle; AUTO/ON native paths report from
    // AnalyzedBatchOperator, while unsupported fused nodes are declared by
    // the trace completion pass.
    let mut aggregate_metrics_adapter = false;
    plan.walk(|node| {
        aggregate_metrics_adapter |= matches!(
            node,
            PhysicalPlan::HashAggregate { .. }
                | PhysicalPlan::HashRollupAggregate { .. }
                | PhysicalPlan::HashGroupingSetsAggregate { .. }
                | PhysicalPlan::StreamingAggregate { .. }
                | PhysicalPlan::StreamingRollupAggregate { .. }
                | PhysicalPlan::StreamingGroupingSetsAggregate { .. }
                | PhysicalPlan::ParallelHashAggregate { .. }
                | PhysicalPlan::ParallelRollupAggregate { .. }
                | PhysicalPlan::ParallelGroupingSetsAggregate { .. }
        );
    });
    let scalar_metrics_adapter = aggregate_metrics_adapter
        || matches!(
            crate::engine::executor::vec::config::effective_mode(),
            crate::engine::executor::vec::config::VecMode::Off
        );
    let instrument_stream = started.is_some() && scalar_metrics_adapter;
    let operator_io =
        instrument_stream.then(|| std::sync::Arc::new(crate::storage::QueryBufferStats::default()));
    let build_stream = || {
        crate::engine::executor::partition_router::with_partition_visit_collector(
            partition_visits
                .as_ref()
                .map(|(_, counter)| counter.clone()),
            || execute_plan_stream_inner(db, txn, plan),
        )
    };
    let stream = crate::storage::with_shared_operator_buffer_stats_if_present(
        operator_io.clone(),
        build_stream,
    )?;
    if let Some(started) = started.filter(|_| instrument_stream) {
        Ok(Box::new(AnalyzedPlanStream::new(
            plan,
            started,
            operator_io.expect("analyze operator I/O sink missing"),
            loops_before,
            partition_visits,
            runtime_filter_rejected,
            stream,
        )))
    } else {
        Ok(stream)
    }
}

fn runtime_filter_observer(
    plan: &PhysicalPlan,
) -> Option<(
    PlanNodeId,
    Vec<std::sync::Arc<std::sync::atomic::AtomicU64>>,
)> {
    fn scan_counters(
        plan: &PhysicalPlan,
    ) -> Option<Vec<std::sync::Arc<std::sync::atomic::AtomicU64>>> {
        match plan {
            PhysicalPlan::SeqScan {
                runtime_filters, ..
            }
            | PhysicalPlan::ZoneMapScan {
                runtime_filters, ..
            }
            | PhysicalPlan::ParallelSeqScan {
                runtime_filters, ..
            }
            | PhysicalPlan::ParallelZoneMapScan {
                runtime_filters, ..
            } => Some(
                runtime_filters
                    .iter()
                    .filter_map(|filter| match filter {
                        RuntimeFilter::BloomKey {
                            rejected: Some(counter),
                            ..
                        } => Some(counter.clone()),
                        _ => None,
                    })
                    .collect(),
            )
            .filter(|counters: &Vec<_>| !counters.is_empty()),
            _ => None,
        }
    }

    let (target, counters) = if let Some(counters) = scan_counters(plan) {
        (plan, counters)
    } else if let PhysicalPlan::Exchange { input, .. } = plan {
        (input.as_ref(), scan_counters(input)?)
    } else {
        return None;
    };
    Some((super::analyze_stats::plan_key(target)?, counters))
}

/// The scalar predicate fallback must remain incremental too: a function
/// filter on a spilled CTE is not permission to read the entire spool into
/// PlanRows before downstream work-area admission.
fn open_cte_scan_stream(plan: &PhysicalPlan) -> Result<Box<dyn PlanStream>, QueryError> {
    let PhysicalPlan::CteScan {
        name,
        alias,
        columns,
        filter,
        projected_columns,
        ..
    } = plan
    else {
        unreachable!("CTE stream requires a CteScan");
    };
    let cte = crate::engine::executor::cte::lookup_cte(name).ok_or_else(|| {
        error(
            crate::error::catalog::ER_NOT_SUPPORTED_YET,
            format!("CTE '{name}' is not active in this scope"),
        )
    })?;
    let alias_str = alias.clone().unwrap_or_else(|| name.clone());
    let bare_table = name.clone();
    // Build PlanField templates once (interned `ColMeta` is cheap to clone),
    // so the per-row loop only swaps in the value. When `projected_columns`
    // is set, the template — and therefore each emitted `PlanRow.fields` —
    // contains only the referenced subset.
    let proj_indices: Option<&[usize]> = projected_columns.as_deref().map(|v| v.as_slice());
    let templates: Vec<PlanField> = match proj_indices {
        Some(indices) => indices
            .iter()
            .map(|&i| PlanField {
                meta: intern_col_meta(ColMeta {
                    invisible: false,
                    table: bare_table.clone(),
                    alias: Some(alias_str.clone()),
                    column: columns[i].name.clone(),
                    column_type: columns[i].column_type,
                    source: columns[i].source.clone(),
                }),
                value: Value::Null,
            })
            .collect(),
        None => columns
            .iter()
            .map(|col| PlanField {
                meta: intern_col_meta(ColMeta {
                    invisible: false,
                    table: bare_table.clone(),
                    alias: Some(alias_str.clone()),
                    column: col.name.clone(),
                    column_type: col.column_type,
                    source: col.source.clone(),
                }),
                value: Value::Null,
            })
            .collect(),
    };
    let predicate = filter
        .as_ref()
        .and_then(|expr| compile_cte_row_predicate(expr, columns));
    let row_schema = RowSchema::from_fields(&templates);
    let reader = if let Some(spill) = &cte.spill {
        let reader = super::grace::ValueRowReader::open(&spill.path, spill._dir.charger())?;
        if reader.is_none() && spill.row_count != 0 {
            return Err(crate::error::catalog::ER_GET_ERRNO
                .error("materialized CTE spill file is missing or truncated"));
        }
        reader
    } else {
        None
    };
    Ok(Box::new(CteScanPlanStream {
        cte,
        templates,
        projected_columns: projected_columns.clone(),
        predicate,
        filter: filter.clone(),
        predicate_program: None,
        row_schema,
        reader,
        position: 0,
    }))
}

struct CteScanPlanStream {
    cte: Arc<crate::engine::api::CteResult>,
    templates: Vec<PlanField>,
    projected_columns: Option<Arc<Vec<usize>>>,
    predicate: Option<CteRowPredicate>,
    filter: Option<Expr>,
    predicate_program: Option<super::eval::CompiledPlanPredicate>,
    row_schema: Arc<RowSchema>,
    reader: Option<super::grace::ValueRowReader>,
    position: usize,
}

impl PlanStream for CteScanPlanStream {
    fn next(&mut self) -> Result<Option<PlanRow>, QueryError> {
        loop {
            crate::engine::cancel::check_cancelled()?;
            let values = if self.cte.spill.is_some() {
                let Some(reader) = self.reader.as_mut() else {
                    return Ok(None);
                };
                let Some(row) = reader.next()? else {
                    self.reader = None;
                    return Ok(None);
                };
                std::borrow::Cow::Owned(row)
            } else {
                let Some(row) = self.cte.rows().get(self.position) else {
                    return Ok(None);
                };
                self.position += 1;
                std::borrow::Cow::Borrowed(row.as_slice())
            };
            if let Some(predicate) = &self.predicate {
                if !eval_cte_row_predicate(predicate, &values)? {
                    continue;
                }
            }
            let fields = self
                .templates
                .iter()
                .enumerate()
                .map(|(output, template)| {
                    let source = self
                        .projected_columns
                        .as_ref()
                        .map_or(output, |indices| indices[output]);
                    PlanField {
                        meta: template.meta.clone(),
                        value: values.get(source).cloned().unwrap_or(Value::Null),
                    }
                })
                .collect();
            let row = PlanRow {
                fields,
                group: None,
                agg_values: None,
                schema: self.row_schema.clone().into(),
            };
            if self.predicate.is_none() {
                if let Some(filter) = &self.filter {
                    let program = self.predicate_program.get_or_insert_with(|| {
                        super::eval::CompiledPlanPredicate::compile_once(filter, &row)
                    });
                    if !program.eval_bool(&row)? {
                        continue;
                    }
                }
            }
            return Ok(Some(row));
        }
    }
}

fn execute_plan_stream_inner(
    db: &Arc<Database>,
    txn: TxnView,
    plan: &PhysicalPlan,
) -> Result<Box<dyn PlanStream>, QueryError> {
    match plan {
        PhysicalPlan::RrfFusion { .. } => super::hybrid::open_plan(db, txn, plan),
        PhysicalPlan::CteScan { .. } => open_cte_scan_stream(plan),
        PhysicalPlan::ParallelSeqScan {
            table,
            filter,
            runtime_filters,
            projected_columns,
            degree,
            ..
        }
        | PhysicalPlan::ParallelZoneMapScan {
            table,
            filter,
            runtime_filters,
            projected_columns,
            degree,
            ..
        } => super::stream::parallel_exchange_unordered_stream(
            db,
            txn,
            table,
            filter,
            runtime_filters,
            projected_columns
                .as_deref()
                .map(|columns| columns.as_slice()),
            *degree,
            matches!(plan, PhysicalPlan::ParallelZoneMapScan { .. }),
        ),
        PhysicalPlan::IndexNestedLoopJoin { .. } if inlj_chunk_stream_applicable(plan) => {
            open_inlj_chunk_stream(db, txn, plan.clone())
        }

        PhysicalPlan::HashJoin { .. } if super::join_stream::chain_applicable(db, plan) => {
            super::join_stream::open_chain_stream(db, txn, plan)
        }
        // Under memory pressure: chain_applicable declines (CTE chain / build-deep depth 1)
        // but the single-level shape is supported and the estimated output would hit the query budget →
        // a same-thread streaming core lets bounded consumers above (TopK / Limit / top-level Sort) pull row by row, without materializing the join output.
        PhysicalPlan::HashJoin { .. } if super::join_stream::spill_stream_applicable(db, plan) => {
            super::join_stream::open_single_level_stream(db, txn, plan)
        }
        PhysicalPlan::SeqScan {
            table,
            filter,
            runtime_filters,
            projected_columns,
            ..
        } => {
            if crate::planner::connection_thread_execution_required() {
                return Ok(Box::new(MaterializedStream::new(scan_plan_table(
                    db,
                    txn,
                    table,
                    filter,
                    runtime_filters,
                    projected_columns.as_deref().map(|v| v.as_slice()),
                    false,
                )?)));
            }
            super::stream::seq_scan_stream(
                db,
                txn,
                table,
                filter,
                runtime_filters,
                projected_columns.as_deref().map(|v| v.as_slice()),
                false,
            )
        }
        PhysicalPlan::ZoneMapScan {
            table,
            filter,
            runtime_filters,
            projected_columns,
            ..
        } => {
            if crate::planner::connection_thread_execution_required() {
                return Ok(Box::new(MaterializedStream::new(scan_plan_table(
                    db,
                    txn,
                    table,
                    filter,
                    runtime_filters,
                    projected_columns.as_deref().map(|v| v.as_slice()),
                    true,
                )?)));
            }
            super::stream::seq_scan_stream(
                db,
                txn,
                table,
                filter,
                runtime_filters,
                projected_columns.as_deref().map(|v| v.as_slice()),
                true,
            )
        }
        // Streaming post-join WHERE filter: rows are filtered one by one as input streams, so failing rows never pile up.
        PhysicalPlan::Filter {
            input, predicate, ..
        } => {
            let inner = execute_plan_stream(db, txn, input)?;
            Ok(Box::new(FilterStream {
                db: db.clone(),
                txn,
                predicate: predicate.clone(),
                compiled: None,
                inner,
                eval_ctx: EvalQueryContext::default(),
            }))
        }
        PhysicalPlan::Apply {
            outer, predicate, ..
        } => {
            let predicate = materialize_apply_predicate(db, txn, outer, predicate)?;
            let inner = execute_plan_stream(db, txn, outer)?;
            Ok(Box::new(FilterStream {
                db: db.clone(),
                txn,
                predicate,
                compiled: None,
                inner,
                eval_ctx: EvalQueryContext::default(),
            }))
        }
        PhysicalPlan::Projection {
            input,
            distinct: false,
            ..
        } => execute_plan_stream(db, txn, input),
        PhysicalPlan::Limit {
            input,
            limit,
            offset,
            binding,
            ..
        } => {
            let limit = binding.resolve_limit(*limit);
            let offset = binding.resolve_offset(*offset);
            if let (
                Some(limit),
                PhysicalPlan::WindowAgg {
                    input: window_input,
                    windows,
                    ..
                },
            ) = (limit, input.as_ref())
            {
                let target_rows = limit.saturating_add(offset.unwrap_or(0));
                if target_rows <= 4096 {
                    let started =
                        super::analyze_stats::is_recording().then(std::time::Instant::now);
                    let io_start = started.map(|_| db.buffer_stats_counts());
                    let rows = execute_window_agg_limited(
                        db,
                        txn,
                        window_input,
                        windows,
                        limit,
                        offset.unwrap_or(0),
                    )?;
                    if let Some(started) = started {
                        super::analyze_stats::record_with_io_snapshot(
                            db,
                            input,
                            started.elapsed().as_nanos(),
                            rows.len() as u64,
                            io_start.expect("window I/O snapshot missing"),
                        );
                    }
                    super::analyze_stats::add_window_runtime(
                        input,
                        "range_state",
                        0,
                        target_rows as u64,
                    );
                    return Ok(Box::new(MaterializedStream::new(rows)));
                }
            }
            if let Some(rows) =
                try_execute_index_ordered_limit_window(db, txn, input, limit, offset)?
            {
                return Ok(Box::new(MaterializedStream::new(rows)));
            }
            let target = offset
                .unwrap_or(0)
                .saturating_add(limit.unwrap_or(usize::MAX));
            let inner = if limit.is_some() {
                execute_plan_stream_limited(db, txn, input, target)?
            } else {
                execute_plan_stream(db, txn, input)?
            };
            Ok(Box::new(LimitStream {
                inner,
                skip: offset.unwrap_or(0),
                remaining: limit.unwrap_or(usize::MAX),
            }))
        }
        // Exchange over a ParallelSeqScan (Unordered) is the one
        // parallel boundary we can stream end-to-end today: workers
        // push row batches into a sync_channel, the main thread
        // pulls a batch at a time. Anything else (sorted gather,
        // parallel agg/sort/join, ParallelIndexFastScan, etc.) still
        // produces a materialised result and is wrapped below.
        PhysicalPlan::Exchange {
            input,
            degree,
            output_order: crate::planner::ExchangeOrder::Unordered,
            ..
        } => {
            let scan = match input.as_ref() {
                PhysicalPlan::ParallelSeqScan {
                    table,
                    filter,
                    runtime_filters,
                    projected_columns,
                    ..
                } => Some((table, filter, runtime_filters, projected_columns, false)),
                PhysicalPlan::ParallelZoneMapScan {
                    table,
                    filter,
                    runtime_filters,
                    projected_columns,
                    ..
                } => Some((table, filter, runtime_filters, projected_columns, true)),
                _ => None,
            };
            if let Some((table, filter, runtime_filters, projected_columns, use_zone_map)) = scan {
                let has_subquery_filter = filter
                    .as_ref()
                    .is_some_and(super::simple::expr_has_subquery);
                if !has_subquery_filter {
                    return super::stream::parallel_exchange_unordered_stream(
                        db,
                        txn,
                        table,
                        filter,
                        runtime_filters,
                        projected_columns.as_deref().map(|v| v.as_slice()),
                        *degree,
                        use_zone_map,
                    );
                }
            }
            // Apply/Filter can sit between an Exchange and its parallel scan.
            // Preserve their incremental predicate evaluation instead of
            // materializing every scan row before the predicate sees it.
            execute_plan_stream(db, txn, input)
        }
        // Top-level ORDER BY consumes its child as a stream and builds
        // bounded runs directly; neither input nor final merge is collected.
        PhysicalPlan::Sort {
            input, order_by, ..
        } => {
            let input_stream = match execute_plan_stream(db, txn, input) {
                Ok(stream) => stream,
                Err(err) => return Err(err),
            };
            let stream = match super::external_sort::sort_plan_input_stream(
                db.data_dir(),
                input_stream,
                order_by,
                Some(plan),
            ) {
                Ok(stream) => stream,
                Err(err) => return Err(err),
            };
            Ok(stream)
        }
        // HashAggregate, joins, distinct projection, and all leaf
        // scans currently produce their result fully materialised.
        // Wrap so the API is uniform.
        _ => {
            let rows = execute_plan_rows_inner(db, txn, plan)?;
            Ok(Box::new(MaterializedStream::new(rows)))
        }
    }
}

fn execute_plan_stream_limited(
    db: &Arc<Database>,
    txn: TxnView,
    plan: &PhysicalPlan,
    max_rows: usize,
) -> Result<Box<dyn PlanStream>, QueryError> {
    match plan {
        PhysicalPlan::NestedLoopJoin {
            left,
            right,
            join_type,
            condition,
            ..
        } if matches!(
            join_type,
            crate::parser::JoinType::Inner | crate::parser::JoinType::Cross
        ) =>
        {
            nested_loop::open_nested_loop_stream(db, txn, left, right, condition.as_ref())
        }
        PhysicalPlan::Projection {
            input,
            distinct: false,
            ..
        } => execute_plan_stream_limited(db, txn, input, max_rows),
        PhysicalPlan::Limit { input, .. } => execute_plan_stream_limited(db, txn, input, max_rows),
        // Streaming post-join WHERE filter (limit-aware variant): propagates LIMIT early-stop upward after filtering.
        PhysicalPlan::Filter {
            input, predicate, ..
        } => {
            let inner = execute_plan_stream_limited(db, txn, input, usize::MAX)?;
            Ok(Box::new(FilterStream {
                db: db.clone(),
                txn,
                predicate: predicate.clone(),
                compiled: None,
                inner,
                eval_ctx: EvalQueryContext::default(),
            }))
        }
        PhysicalPlan::Apply {
            outer, predicate, ..
        } => {
            let predicate = materialize_apply_predicate(db, txn, outer, predicate)?;
            let inner = execute_plan_stream_limited(db, txn, outer, usize::MAX)?;
            Ok(Box::new(FilterStream {
                db: db.clone(),
                txn,
                predicate,
                compiled: None,
                inner,
                eval_ctx: EvalQueryContext::default(),
            }))
        }
        // join → LIMIT early-stop: the chained streaming join produces output on demand; once the
        // LIMIT is satisfied it stops, and upstream halts naturally via channel backpressure (S1c).
        PhysicalPlan::HashJoin { .. } if super::join_stream::chain_applicable(db, plan) => {
            super::join_stream::open_chain_stream(db, txn, plan)
        }
        PhysicalPlan::HashJoin { .. } if super::join_stream::spill_stream_applicable(db, plan) => {
            super::join_stream::open_single_level_stream(db, txn, plan)
        }
        PhysicalPlan::SeqScan {
            table,
            filter,
            runtime_filters,
            projected_columns,
            ..
        } => {
            let start = super::analyze_stats::is_recording().then(std::time::Instant::now);
            let io_start = start.map(|_| db.buffer_stats_counts());
            let stream = LimitedSeqScanStream::new(
                db,
                txn,
                table,
                filter,
                runtime_filters,
                projected_columns.as_deref().map(|v| v.as_slice()),
                max_rows,
            )?;
            if let Some(start) = start {
                super::analyze_stats::record_with_io_snapshot(
                    db,
                    plan,
                    start.elapsed().as_nanos(),
                    stream.len() as u64,
                    io_start.unwrap(),
                );
            }
            Ok(Box::new(stream))
        }
        PhysicalPlan::ZoneMapScan {
            table,
            filter,
            runtime_filters,
            projected_columns,
            ..
        } => super::stream::seq_scan_stream(
            db,
            txn,
            table,
            filter,
            runtime_filters,
            projected_columns.as_deref().map(|v| v.as_slice()),
            true,
        ),
        PhysicalPlan::IndexRangeScan {
            table,
            index,
            low,
            high,
            filter,
            covering,
            reverse,
            projected_columns,
            ..
        } => {
            let projection = projected_columns.as_deref().map(|v| v.as_slice());
            let start = super::analyze_stats::is_recording().then(std::time::Instant::now);
            let io_start = start.map(|_| db.buffer_stats_counts());
            let rows = if *covering {
                read_plan_table_entries_chunked_limited(
                    db,
                    txn,
                    table,
                    index,
                    low.clone(),
                    high.clone(),
                    *reverse,
                    filter,
                    projection,
                    max_rows,
                )?
            } else {
                super::analyze_stats::with_range_scan_plan(plan, || {
                    read_plan_table_ids_range_chunked_limited(
                        db,
                        txn,
                        table,
                        index,
                        low.clone(),
                        high.clone(),
                        *reverse,
                        filter,
                        max_rows,
                        projection,
                        true,
                    )
                })?
            };
            if let Some(start) = start {
                super::analyze_stats::record_with_io_snapshot(
                    db,
                    plan,
                    start.elapsed().as_nanos(),
                    rows.len() as u64,
                    io_start.unwrap(),
                );
            }
            Ok(Box::new(MaterializedStream::new(rows)))
        }
        PhysicalPlan::IndexFastScan {
            table,
            index,
            low,
            high,
            filter,
            reverse,
            projected_columns,
            ..
        } => {
            let start = super::analyze_stats::is_recording().then(std::time::Instant::now);
            let io_start = start.map(|_| db.buffer_stats_counts());
            let rows = read_plan_table_entries_chunked_limited(
                db,
                txn,
                table,
                index,
                low.clone(),
                high.clone(),
                *reverse,
                filter,
                projected_columns.as_deref().map(|v| v.as_slice()),
                max_rows,
            )?;
            if let Some(start) = start {
                super::analyze_stats::record_with_io_snapshot(
                    db,
                    plan,
                    start.elapsed().as_nanos(),
                    rows.len() as u64,
                    io_start.unwrap(),
                );
            }
            Ok(Box::new(MaterializedStream::new(rows)))
        }
        PhysicalPlan::Exchange {
            input,
            degree,
            output_order,
            merge_keys,
            ..
        } if matches!(output_order, crate::planner::ExchangeOrder::Unordered) => {
            let start = super::analyze_stats::is_recording().then(std::time::Instant::now);
            let io_start = start.map(|_| db.buffer_stats_counts());
            let rows = super::parallel::execute_exchange_limited(
                db,
                txn,
                input,
                *degree,
                *output_order,
                merge_keys,
                max_rows,
            )?;
            if let Some(start) = start {
                super::analyze_stats::record_with_io_snapshot(
                    db,
                    plan,
                    start.elapsed().as_nanos(),
                    rows.len() as u64,
                    io_start.unwrap(),
                );
            }
            Ok(Box::new(MaterializedStream::new(rows)))
        }
        _ => execute_plan_stream(db, txn, plan),
    }
}

/// True when `plan` can be advanced row-at-a-time without any
/// blocking operator above the leaf scan. Used to gate the LIMIT
/// stream-shortcut in `execute_limit_plan_rows`.
fn input_is_streamable(plan: &PhysicalPlan) -> bool {
    match plan {
        PhysicalPlan::NestedLoopJoin { join_type, .. } => matches!(
            join_type,
            crate::parser::JoinType::Inner | crate::parser::JoinType::Cross
        ),
        PhysicalPlan::Projection {
            input,
            distinct: false,
            ..
        }
        | PhysicalPlan::Limit { input, .. } => input_is_streamable(input),
        PhysicalPlan::SeqScan { .. }
        | PhysicalPlan::IndexLookup { .. }
        | PhysicalPlan::IndexRangeScan { .. }
        | PhysicalPlan::IndexFastScan { .. }
        | PhysicalPlan::Exchange { .. }
        | PhysicalPlan::CteScan { .. } => true,
        // Anything blocking (sort/agg/join/distinct/IndexInLookup/
        // window) requires full materialisation before LIMIT can
        // be pushed down through the rest of the tree.
        _ => false,
    }
}

pub(super) fn execute_limit_plan_rows(
    db: &Arc<Database>,
    txn: TxnView,
    input: &PhysicalPlan,
    limit: Option<usize>,
    offset: Option<usize>,
) -> Result<Vec<PlanRow>, QueryError> {
    let warning_checkpoint = crate::engine::api::statement_warning_count();
    let group_concat_slots = group_concat_slots_per_output_group(input);
    if let Some(rows) = try_execute_index_ordered_limit_window(db, txn, input, limit, offset)? {
        cap_limited_group_concat_warnings(warning_checkpoint, group_concat_slots, rows.len());
        return Ok(rows);
    }
    if let (
        Some(limit),
        PhysicalPlan::WindowAgg {
            input: window_input,
            windows,
            ..
        },
    ) = (limit, input)
    {
        let target_rows = limit.saturating_add(offset.unwrap_or(0));
        if target_rows <= 4096 {
            let started = super::analyze_stats::is_recording().then(std::time::Instant::now);
            let io_start = started.map(|_| db.buffer_stats_counts());
            let output = execute_window_agg_limited(
                db,
                txn,
                window_input,
                windows,
                limit,
                offset.unwrap_or(0),
            )?;
            if let Some(started) = started {
                super::analyze_stats::record_with_io_snapshot(
                    db,
                    input,
                    started.elapsed().as_nanos(),
                    output.len() as u64,
                    io_start.expect("window I/O snapshot missing"),
                );
            }
            super::analyze_stats::add_window_runtime(input, "range_state", 0, target_rows as u64);
            return Ok(output);
        }
    }
    let rows = match input {
        PhysicalPlan::Sort {
            input, order_by, ..
        } => {
            // Phase 7: try the vectorized Top-N heap before the legacy
            // full-materialise + quickselect path. The bridge returns
            // `None` when LIMIT isn't set, K is too large, or ORDER BY
            // contains non-column expressions.
            if let Some(rows) =
                super::vec_bridge::try_vec_topn(db, txn, input, order_by, limit, offset)?
            {
                return Ok(rows);
            }
            let mut rows = execute_plan_rows(db, txn, input)?;
            apply_ordered_limit(&mut rows, order_by, limit, offset)?;
            Ok(rows)
        }
        PhysicalPlan::Projection {
            input,
            distinct: false,
            ..
        } => {
            if let PhysicalPlan::Sort {
                input, order_by, ..
            } = input.as_ref()
            {
                if let Some(rows) =
                    super::vec_bridge::try_vec_topn(db, txn, input, order_by, limit, offset)?
                {
                    return Ok(rows);
                }
                let mut rows = execute_plan_rows(db, txn, input)?;
                apply_ordered_limit(&mut rows, order_by, limit, offset)?;
                Ok(rows)
            } else if input_is_streamable(input) {
                // Phase B #B3: pull at most offset+limit rows through
                // the stream and stop. Once leaf scans actually stream
                // (B3 phase 2), this avoids materialising the full
                // table; today it caps the work done by intermediate
                // projections / filters above the materialised scan.
                drain_via_stream(db, txn, input, limit, offset)
            } else {
                apply_plain_limit(execute_plan_rows(db, txn, input)?, limit, offset)
            }
        }
        other if input_is_streamable(other) => drain_via_stream(db, txn, input, limit, offset),
        _ => apply_plain_limit(execute_plan_rows(db, txn, input)?, limit, offset),
    }?;
    cap_limited_group_concat_warnings(warning_checkpoint, group_concat_slots, rows.len());
    Ok(rows)
}

pub(super) fn group_concat_slots_per_output_group(plan: &PhysicalPlan) -> usize {
    let aggregate_count = |aggregates: &[Expr]| {
        super::aggregate::collect_aggregate_slots(&aggregates.iter().collect::<Vec<_>>())
            .into_iter()
            .filter(|slot| slot.name.eq_ignore_ascii_case("GROUP_CONCAT"))
            .count()
    };
    match plan {
        PhysicalPlan::HashAggregate { aggregates, .. }
        | PhysicalPlan::HashRollupAggregate { aggregates, .. }
        | PhysicalPlan::HashGroupingSetsAggregate { aggregates, .. }
        | PhysicalPlan::StreamingAggregate { aggregates, .. }
        | PhysicalPlan::StreamingRollupAggregate { aggregates, .. }
        | PhysicalPlan::StreamingGroupingSetsAggregate { aggregates, .. }
        | PhysicalPlan::ParallelHashAggregate { aggregates, .. }
        | PhysicalPlan::ParallelRollupAggregate { aggregates, .. }
        | PhysicalPlan::ParallelGroupingSetsAggregate { aggregates, .. } => {
            aggregate_count(aggregates)
        }
        PhysicalPlan::Projection { input, .. }
        | PhysicalPlan::Sort { input, .. }
        | PhysicalPlan::IncrementalSort { input, .. }
        | PhysicalPlan::Filter { input, .. }
        | PhysicalPlan::Exchange { input, .. }
        | PhysicalPlan::Limit { input, .. } => group_concat_slots_per_output_group(input),
        _ => 0,
    }
}

pub(super) fn cap_limited_group_concat_warnings(
    warning_checkpoint: usize,
    group_concat_slots: usize,
    output_rows: usize,
) {
    if group_concat_slots == 0 {
        return;
    }
    crate::engine::api::cap_statement_warnings_since(
        warning_checkpoint,
        crate::error::catalog::ER_CUT_VALUE_GROUP_CONCAT
            .code()
            .get(),
        output_rows.saturating_mul(group_concat_slots),
    );
}

fn drain_via_stream(
    db: &Arc<Database>,
    txn: TxnView,
    input: &PhysicalPlan,
    limit: Option<usize>,
    offset: Option<usize>,
) -> Result<Vec<PlanRow>, QueryError> {
    let target = offset
        .unwrap_or(0)
        .saturating_add(limit.unwrap_or(usize::MAX));
    let mut stream = execute_plan_stream_limited(db, txn, input, target)?;
    let mut rows = Vec::with_capacity(target.min(64));
    while rows.len() < target {
        match stream.next()? {
            Some(row) => rows.push(row),
            None => break,
        }
    }
    apply_plain_limit(rows, limit, offset)
}

fn try_execute_index_ordered_limit_window(
    db: &Arc<Database>,
    txn: TxnView,
    input: &PhysicalPlan,
    limit: Option<usize>,
    offset: Option<usize>,
) -> Result<Option<Vec<PlanRow>>, QueryError> {
    let Some(limit) = limit else {
        return Ok(None);
    };
    let offset = offset.unwrap_or(0);
    match input {
        PhysicalPlan::Projection {
            input,
            distinct: false,
            ..
        }
        | PhysicalPlan::Limit { input, .. } => {
            try_execute_index_ordered_limit_window(db, txn, input, Some(limit), Some(offset))
        }
        PhysicalPlan::IndexFastScan {
            table,
            index,
            low,
            high,
            filter,
            reverse,
            projected_columns,
            ..
        } => {
            if db.has_pending_index_repairs_for(table.schema.id, &index.name) {
                return Ok(None);
            }
            // Partitioned tables scan one index per partition; this
            // window path pushes the LIMIT into a single ordered index
            // walk (the planner already elided the Sort). Per-partition
            // index order is not global order, so let the ordinary
            // Sort+Limit run instead of pushing the window down.
            if table.schema.is_partitioned() {
                return Ok(None);
            }
            read_plan_table_entries_windowed(
                db,
                txn,
                table,
                index,
                low.clone(),
                high.clone(),
                *reverse,
                filter,
                projected_columns.as_deref().map(|v| v.as_slice()),
                offset,
                limit,
            )
            .map(Some)
        }
        PhysicalPlan::IndexRangeScan {
            table,
            index,
            low,
            high,
            filter,
            covering,
            reverse,
            projected_columns,
            ..
        } => {
            if db.has_pending_index_repairs_for(table.schema.id, &index.name) {
                return Ok(None);
            }
            if table.schema.is_partitioned() {
                return Ok(None);
            }
            let projection = projected_columns.as_deref().map(|v| v.as_slice());
            if *covering {
                return read_plan_table_entries_windowed(
                    db,
                    txn,
                    table,
                    index,
                    low.clone(),
                    high.clone(),
                    *reverse,
                    filter,
                    projection,
                    offset,
                    limit,
                )
                .map(Some);
            }
            if index_window_filter_blocks_fast_path(table, filter) {
                return Ok(None);
            }
            super::analyze_stats::with_range_scan_plan(input, || {
                read_plan_table_ids_range_windowed(
                    db,
                    txn,
                    table,
                    index,
                    low.clone(),
                    high.clone(),
                    *reverse,
                    projection,
                    offset,
                    limit,
                )
            })
            .map(Some)
        }
        _ => Ok(None),
    }
}

fn index_window_filter_blocks_fast_path(table: &PlanTable, filter: &Option<Expr>) -> bool {
    let Some(filter) = filter else {
        return false;
    };
    !is_nonnull_integer_min_lower_bound(table, filter)
}

fn is_nonnull_integer_min_lower_bound(table: &PlanTable, expr: &Expr) -> bool {
    let Expr::Binary { left, op, right } = expr else {
        return false;
    };
    if *op != BinaryOp::GtEq {
        return false;
    }
    let Some(column_name) = simple_order_column_name(left) else {
        return false;
    };
    let Some(column) = table
        .schema
        .columns
        .iter()
        .find(|col| col.name.eq_ignore_ascii_case(column_name))
    else {
        return false;
    };
    if column.nullable {
        return false;
    }
    match (right.as_ref(), &column.data_type, column.unsigned) {
        (
            Expr::Literal(crate::parser::Literal::Int(v)),
            crate::storage::DataTypeKind::TinyInt
            | crate::storage::DataTypeKind::SmallInt
            | crate::storage::DataTypeKind::MediumInt
            | crate::storage::DataTypeKind::Int
            | crate::storage::DataTypeKind::BigInt
            | crate::storage::DataTypeKind::Bool,
            false,
        ) => *v == i64::MIN,
        (
            Expr::Literal(crate::parser::Literal::UInt(v)),
            crate::storage::DataTypeKind::TinyInt
            | crate::storage::DataTypeKind::SmallInt
            | crate::storage::DataTypeKind::MediumInt
            | crate::storage::DataTypeKind::Int
            | crate::storage::DataTypeKind::BigInt
            | crate::storage::DataTypeKind::Bool,
            true,
        ) => *v == 0,
        _ => false,
    }
}

pub(super) fn apply_ordered_limit(
    rows: &mut Vec<PlanRow>,
    order_by: &[OrderBy],
    limit: Option<usize>,
    offset: Option<usize>,
) -> Result<(), QueryError> {
    // O12 — Schwartzian sort/partition: each helper precomputes ORDER BY
    // keys once per row, eliminating the per-comparison expression eval
    // cost for non-trivial sort expressions.
    let offset = offset.unwrap_or(0);
    let Some(limit) = limit else {
        sort_plan_rows_by_order(rows, order_by)?;
        rows.drain(0..offset.min(rows.len()));
        return Ok(());
    };
    let needed = offset.saturating_add(limit).min(rows.len());
    if needed < rows.len() {
        select_nth_plan_rows_by_order(rows, order_by, needed)?;
        rows.truncate(needed);
    }
    sort_plan_rows_by_order(rows, order_by)?;
    let offset = offset.min(rows.len());
    let end = offset.saturating_add(limit).min(rows.len());
    rows.drain(end..);
    rows.drain(0..offset);
    Ok(())
}

fn execute_incremental_sort(
    db: &Arc<Database>,
    txn: TxnView,
    input: &PhysicalPlan,
    order_by: &[OrderBy],
    presorted_prefix: usize,
) -> Result<Vec<PlanRow>, QueryError> {
    if presorted_prefix == 0 || presorted_prefix >= order_by.len() {
        let rows = execute_plan_rows(db, txn, input)?;
        return super::external_sort::sort_plan_rows(db.data_dir(), rows, order_by);
    }
    let prefix_order = &order_by[..presorted_prefix];
    let prefix_fields = crate::engine::executor::sort::fields_from_order_by(prefix_order);
    let mut out = Vec::new();
    let mut group = Vec::new();
    let mut current_prefix: Option<Vec<Value>> = None;
    for row in execute_plan_rows(db, txn, input)? {
        crate::engine::cancel::check_cancelled()?;
        let prefix = precompute_order_keys(&row, prefix_order)?;
        let same_group = current_prefix.as_ref().is_some_and(|current| {
            crate::engine::executor::sort::compare_sort_keys(&prefix_fields, current, &prefix)
                == std::cmp::Ordering::Equal
        });
        if current_prefix.is_some() && !same_group {
            let sorted =
                sort_incremental_group(db.data_dir(), std::mem::take(&mut group), order_by)?;
            out.extend(sorted);
        }
        if current_prefix.is_none() || !same_group {
            current_prefix = Some(prefix);
        }
        group.push(row);
    }
    if !group.is_empty() {
        let sorted = sort_incremental_group(db.data_dir(), group, order_by)?;
        out.extend(sorted);
    }
    Ok(out)
}

/// Sort one `IncrementalSort` prefix-group. Groups under
/// `INCREMENTAL_SORT_SMALL_GROUP_LIMIT` rows take a fast path that
/// bypasses the full `external_sort::sort_plan_rows` pipeline.
///
/// The expensive part of `sort_plan_rows` is
/// `estimate_sort_work_bytes`, which evaluates every ORDER BY
/// expression on every row just to compute a byte estimate for the
/// budget / spill decision — those evals are then *redone* by the
/// in-memory Schwartzian. For an `IncrementalSort` with high-
/// cardinality prefix (many small groups), that wasted pre-eval can
/// dominate the operator. At this size the group will never spill
/// regardless of the query budget, so it's safe to skip the
/// estimate/charge entirely.
const INCREMENTAL_SORT_SMALL_GROUP_LIMIT: usize = 4096;

fn sort_incremental_group(
    data_dir: &std::path::Path,
    rows: Vec<PlanRow>,
    order_by: &[OrderBy],
) -> Result<Vec<PlanRow>, QueryError> {
    if rows.len() <= INCREMENTAL_SORT_SMALL_GROUP_LIMIT {
        let mut rows = rows;
        super::eval::sort_plan_rows_by_order(&mut rows, order_by)?;
        return Ok(rows);
    }
    super::external_sort::sort_plan_rows(data_dir, rows, order_by)
}

pub(super) fn apply_plain_limit(
    rows: Vec<PlanRow>,
    limit: Option<usize>,
    offset: Option<usize>,
) -> Result<Vec<PlanRow>, QueryError> {
    let offset = offset.unwrap_or(0).min(rows.len());
    let end = limit
        .map(|limit| offset.saturating_add(limit).min(rows.len()))
        .unwrap_or(rows.len());
    Ok(rows[offset..end].to_vec())
}

/// Runtime thresholds for hash join, read once from environment variables and cached.
#[derive(Clone, Copy)]
pub(super) struct HashJoinLimits {
    pub(super) mem_limit_bytes: u64,
    /// Whether `mem_limit_bytes` came from an explicit operator/query budget.
    /// Vector hash join may borrow beyond the ordinary session soft quota only
    /// when the WorkAreaManager grants the resident bytes; explicit caps must
    /// always remain authoritative.
    pub(super) mem_limit_explicit: bool,
    pub(super) max_build_rows: u64,
    fallback_error: bool,
    bloom_max_build_rows: u64,
    bloom_min_build_rows: u64,
    bloom_small_build_probe_ratio: f64,
    pub(super) grace_partitions: u32,
    pub(super) grace_max_depth: u32,
    parallelism: u32,
    parallelism_threshold: u64,
}

impl HashJoinLimits {
    /// Whether a probe-side bloom filter is worth building for a hash
    /// join with `build_len` build rows.
    ///
    /// A bloom check only pays off when the build-side hash table is
    /// large enough that a probe miss cache-misses in the table — for a
    /// small build the table is cache-resident and a miss is already as
    /// cheap as the bloom probe, so the bloom is pure overhead. Measured
    /// on TPC-H Q3 (build = 20k rows): the bloom cost ~160ms with zero
    /// net benefit. The lower bound (`MYDB_HASH_JOIN_BLOOM_MIN_BUILD_ROWS`)
    /// keeps the bloom only for genuinely large builds; semi/anti keyset
    /// joins, which use the bloom differently, are gated separately.
    #[inline]
    pub(super) fn bloom_worthwhile(&self, build_len: usize) -> bool {
        let n = build_len as u64;
        self.bloom_max_build_rows > 0
            && n <= self.bloom_max_build_rows
            && n >= self.bloom_min_build_rows
    }

    /// Inject-time gate: whether to build a bloom on the *build* side
    /// and push it into the *probe* side's scan as a `RuntimeFilter`.
    /// Strictly more permissive than `bloom_worthwhile` because a
    /// scan-time bloom saves more than just hash-table cache misses:
    /// it avoids materialising and channeling rows the join could
    /// never have matched.
    ///
    /// The "big build" regime is the same as `bloom_worthwhile`. The
    /// additional "tiny build, enormous probe" regime catches
    /// small-dim × big-fact joins (star schema, TPC-H Q17
    /// `part(1k) ⋈ lineitem(30M)`) where the build hash table is
    /// cache-resident but rejecting 99% of probe rows at the scan
    /// still pays for itself many times over.
    /// `MYDB_HASH_JOIN_BLOOM_SMALL_BUILD_PROBE_RATIO` (default 100)
    /// controls how lopsided the ratio has to be; the probe must
    /// additionally exceed `bloom_max_build_rows` so the pressure is
    /// real and not just an estimate artefact on a small table.
    #[inline]
    pub(super) fn inject_bloom_worthwhile(&self, build_len: usize, probe_est: f64) -> bool {
        let n = build_len as u64;
        if self.bloom_max_build_rows == 0 || n == 0 || n > self.bloom_max_build_rows {
            return false;
        }
        if n >= self.bloom_min_build_rows {
            return true;
        }
        probe_est >= (build_len as f64) * self.bloom_small_build_probe_ratio
            && probe_est >= self.bloom_max_build_rows as f64
    }
}

#[derive(Clone, Copy)]
struct HashJoinStaticConfig {
    mem_limit_bytes: Option<u64>,
    max_build_rows: u64,
    fallback_error: bool,
    bloom_max_build_rows: u64,
    bloom_min_build_rows: u64,
    bloom_small_build_probe_ratio: f64,
    grace_partitions: u32,
    grace_max_depth: u32,
    parallelism: u32,
    parallelism_threshold: u64,
}

fn load_hash_join_static_config() -> HashJoinStaticConfig {
    HashJoinStaticConfig {
        mem_limit_bytes: crate::util::env::var("MYDB_HASH_JOIN_MEM_LIMIT_MB")
            .ok()
            .and_then(|s| s.parse::<u64>().ok())
            .map(|mb| mb.saturating_mul(1024 * 1024)),
        max_build_rows: crate::util::env::var("MYDB_HASH_JOIN_MAX_BUILD_ROWS")
            .ok()
            .and_then(|s| s.parse::<u64>().ok())
            // The byte grant is the real memory bound.  A fixed default row
            // cap made narrow, budget-resident builds spill solely because
            // they crossed 5M rows (for example, one BIGINT join key), even
            // when MYDB_MEMORY_TARGET had ample room.  Keep the explicit env
            // knob as an operator guard/forced-spill control, while the
            // default only protects the u32 row handles used by hash buckets.
            .map(|rows| rows.min(u64::from(u32::MAX - 1)))
            .unwrap_or(u64::from(u32::MAX - 1)),
        fallback_error: crate::util::env::var("MYDB_HASH_JOIN_FALLBACK")
            .map(|v| v.eq_ignore_ascii_case("error"))
            .unwrap_or(false),
        bloom_max_build_rows: crate::util::env::var("MYDB_HASH_JOIN_BLOOM_MAX_BUILD_ROWS")
            .ok()
            .and_then(|s| s.parse::<u64>().ok())
            .unwrap_or(1_000_000),
        bloom_min_build_rows: crate::util::env::var("MYDB_HASH_JOIN_BLOOM_MIN_BUILD_ROWS")
            .ok()
            .and_then(|s| s.parse::<u64>().ok())
            .unwrap_or(128_000),
        bloom_small_build_probe_ratio: crate::util::env::var(
            "MYDB_HASH_JOIN_BLOOM_SMALL_BUILD_PROBE_RATIO",
        )
        .ok()
        .and_then(|s| s.parse::<f64>().ok())
        .filter(|&v| v >= 1.0)
        .unwrap_or(100.0),
        grace_partitions: crate::util::env::var("MYDB_HASH_JOIN_GRACE_PARTITIONS")
            .ok()
            .and_then(|s| s.parse::<u32>().ok())
            .filter(|&v| (2..=256).contains(&v))
            .unwrap_or(128),
        grace_max_depth: crate::util::env::var("MYDB_HASH_JOIN_GRACE_MAX_DEPTH")
            .ok()
            .and_then(|s| s.parse::<u32>().ok())
            .filter(|&v| (1..=8).contains(&v))
            .unwrap_or(4),
        parallelism: crate::util::env::var("MYDB_HASH_JOIN_PARALLELISM")
            .ok()
            .and_then(|s| s.parse::<u32>().ok())
            .filter(|&v| (1..=64).contains(&v))
            .unwrap_or_else(|| crate::planner::default_parallel_degree().min(8)),
        parallelism_threshold: crate::util::env::var("MYDB_HASH_JOIN_PARALLELISM_THRESHOLD")
            .ok()
            .and_then(|s| s.parse::<u64>().ok())
            .unwrap_or(10_000),
    }
}

fn hash_join_static_config() -> HashJoinStaticConfig {
    #[cfg(not(test))]
    {
        static CONFIG: std::sync::OnceLock<HashJoinStaticConfig> = std::sync::OnceLock::new();
        *CONFIG.get_or_init(load_hash_join_static_config)
    }
    #[cfg(test)]
    {
        // Unit tests scope environment overrides within one process.
        load_hash_join_static_config()
    }
}

pub(super) fn hash_join_limits() -> HashJoinLimits {
    let config = hash_join_static_config();
    let current_budget = crate::engine::executor::mem_budget::current_budget();
    let mem_limit_bytes = config.mem_limit_bytes.unwrap_or_else(|| {
        current_budget
            .as_ref()
            .map(|budget| budget.remaining() as u64)
            .unwrap_or_else(|| crate::engine::executor::mem_budget::query_mem_limit_bytes() as u64)
    });
    HashJoinLimits {
        mem_limit_bytes,
        mem_limit_explicit: config.mem_limit_bytes.is_some()
            || current_budget.is_some_and(|budget| !budget.tracks_process()),
        max_build_rows: config.max_build_rows,
        fallback_error: config.fallback_error,
        bloom_max_build_rows: config.bloom_max_build_rows,
        bloom_min_build_rows: config.bloom_min_build_rows,
        bloom_small_build_probe_ratio: config.bloom_small_build_probe_ratio,
        grace_partitions: config.grace_partitions,
        grace_max_depth: config.grace_max_depth,
        parallelism: config.parallelism,
        parallelism_threshold: config.parallelism_threshold,
    }
}

pub(super) fn estimate_row_bytes(row: &PlanRow) -> u64 {
    let mut bytes: u64 = 64; // PlanRow / Vec overhead
    for f in &row.fields {
        bytes = bytes.saturating_add(32);
        bytes = bytes.saturating_add(match &f.value {
            Value::Null => 0,
            Value::Int(_) | Value::UInt(_) | Value::Float(_) | Value::Double(_) => 8,
            Value::String(s) => 16 + s.len() as u64,
            Value::SmallString(b) => b.len() as u64, // inline — no heap
            Value::Bytes(b) => 16 + b.len() as u64,
            Value::Decimal(s) => 16 + s.len() as u64,
            // Binary temporal / decimal variants are tiny on the heap
            // (no String box), so their memory footprint is just the
            // primitive payload.
            Value::DecimalCompact(..) => 9,
            Value::Decimal128(..) => 17,
            Value::DecimalWide(_) => 16 + f.value.decimal_heap_bytes() as u64,
            Value::Date(_) => 4,
            Value::Time(_) | Value::DateTime(_) | Value::Timestamp(_) => 8,
            Value::Year(_) => 1,
            Value::Json(b) => 16 + b.len() as u64,
            Value::Geometry(b) => 16 + b.len() as u64,
            Value::Vector(b) => 16 + b.len() as u64,
        });
    }
    bytes
}

pub(super) fn estimate_rows_bytes(rows: &[PlanRow]) -> u64 {
    rows.iter().map(estimate_row_bytes).sum()
}

/// NLJ fallback path when the memory threshold is exceeded: reuses the already-materialized build/probe data without re-running the subplans.
/// Semantics are equivalent to hash join (multi-key + null_safe + residual + outer build flip bitmap).
fn execute_hash_join_nlj_fallback(
    left: &PhysicalPlan,
    right: &PhysicalPlan,
    join_type: crate::parser::JoinType,
    left_keys: &[Expr],
    right_keys: &[Expr],
    null_safe: &[bool],
    build_side: BuildSide,
    residual: Option<&Expr>,
    build_rows: Vec<PlanRow>,
    probe_rows: Vec<PlanRow>,
) -> Result<Vec<PlanRow>, QueryError> {
    let build_is_left = matches!(build_side, BuildSide::Left);
    let build_is_outer_preserved = match (join_type, build_is_left) {
        (crate::parser::JoinType::Left, true) => true,
        (crate::parser::JoinType::Right, false) => true,
        (crate::parser::JoinType::Full, _) => true,
        _ => false,
    };
    let probe_is_outer_preserved = match (join_type, build_is_left) {
        (crate::parser::JoinType::Left, false) => true,
        (crate::parser::JoinType::Right, true) => true,
        (crate::parser::JoinType::Full, _) => true,
        _ => false,
    };
    let mut build_matched = if build_is_outer_preserved {
        vec![false; build_rows.len()]
    } else {
        Vec::new()
    };
    let semi_or_anti = matches!(
        join_type,
        crate::parser::JoinType::Semi | crate::parser::JoinType::Anti
    );
    let mut out = Vec::new();
    let join_lk = new_join_lookup();
    for probe_row in &probe_rows {
        let mut probe_matched = false;
        for (bi, build_row) in build_rows.iter().enumerate() {
            let (l_row, r_row) = if build_is_left {
                (build_row, probe_row)
            } else {
                (probe_row, build_row)
            };
            if !rows_match_on_keys(left_keys, right_keys, null_safe, l_row, r_row)? {
                continue;
            }
            let joined = if build_is_left {
                join_rows(build_row, probe_row, &join_lk)
            } else {
                join_rows(probe_row, build_row, &join_lk)
            };
            if residual
                .map(|expr| eval_plan_bool(expr, &joined))
                .transpose()?
                .unwrap_or(true)
            {
                probe_matched = true;
                if semi_or_anti {
                    if join_type == crate::parser::JoinType::Semi {
                        out.push(probe_row.clone());
                    }
                    break;
                }
                if build_is_outer_preserved {
                    build_matched[bi] = true;
                }
                out.push(joined);
            }
        }
        if !probe_matched {
            if semi_or_anti {
                if join_type == crate::parser::JoinType::Anti {
                    out.push(probe_row.clone());
                }
            } else if probe_is_outer_preserved {
                if build_is_left {
                    out.push(null_extend_left(probe_row, left));
                } else {
                    out.push(null_extend_right(probe_row, right));
                }
            }
        }
    }
    if build_is_outer_preserved {
        for (bi, matched) in build_matched.iter().enumerate() {
            if !*matched {
                let build_row = &build_rows[bi];
                if build_is_left {
                    out.push(null_extend_right(build_row, right));
                } else {
                    out.push(null_extend_left(build_row, left));
                }
            }
        }
    }
    Ok(out)
}

/// Evaluates whether two rows are equal on all join keys (including null_safe semantics).
fn rows_match_on_keys(
    left_keys: &[Expr],
    right_keys: &[Expr],
    null_safe: &[bool],
    left_row: &PlanRow,
    right_row: &PlanRow,
) -> Result<bool, QueryError> {
    for i in 0..left_keys.len() {
        let l = eval_plan_expr(&left_keys[i], left_row)?;
        let r = eval_plan_expr(&right_keys[i], right_row)?;
        let ns = null_safe.get(i).copied().unwrap_or(false);
        let matched = if ns {
            crate::engine::expr::null_safe_eq(&l, &r)?
        } else {
            !matches!(l, Value::Null)
                && !matches!(r, Value::Null)
                && compare_values(&l, &r)? == std::cmp::Ordering::Equal
        };
        if !matched {
            return Ok(false);
        }
    }
    Ok(true)
}

// ---------- Hash join runtime counters (Phase 2.4) ----------

pub(crate) static HASH_JOIN_COUNT: AtomicU64 = AtomicU64::new(0);
pub(crate) static BOUNDED_INNER_JOIN_HITS: AtomicU64 = AtomicU64::new(0);
pub(crate) static HASH_JOIN_BUILD_ROWS: AtomicU64 = AtomicU64::new(0);
pub(crate) static HASH_JOIN_PROBE_ROWS: AtomicU64 = AtomicU64::new(0);
pub(crate) static HASH_JOIN_NULL_SKIPPED: AtomicU64 = AtomicU64::new(0);
pub(crate) static HASH_JOIN_MEM_EXCEEDED: AtomicU64 = AtomicU64::new(0);
pub(crate) static HASH_JOIN_FALLBACK_TO_NLJ: AtomicU64 = AtomicU64::new(0);
pub(crate) static HASH_JOIN_BLOOM_BUILT: AtomicU64 = AtomicU64::new(0);
pub(crate) static HASH_JOIN_BLOOM_FILTERED: AtomicU64 = AtomicU64::new(0);
pub(crate) static HASH_JOIN_PARALLEL_PARTITIONS: AtomicU64 = AtomicU64::new(0);
pub(crate) static HASH_JOIN_GRACE_PARTITIONS: AtomicU64 = AtomicU64::new(0);
pub(crate) static HASH_JOIN_SPILL_BYTES: AtomicU64 = AtomicU64::new(0);
pub(crate) static HASH_JOIN_SPILL_READ_BYTES: AtomicU64 = AtomicU64::new(0);
pub(crate) static HASH_JOIN_GRACE_MAX_DEPTH: AtomicU64 = AtomicU64::new(0);
pub(crate) static HASH_JOIN_GRACE_REPARTITIONS: AtomicU64 = AtomicU64::new(0);
pub(crate) static HASH_JOIN_SKEW_CHUNKS: AtomicU64 = AtomicU64::new(0);
pub(crate) static HASH_JOIN_PEAK_CHARGED_BYTES: AtomicU64 = AtomicU64::new(0);
pub(crate) static HASH_JOIN_CHARGED_BYTES: AtomicU64 = AtomicU64::new(0);
pub(crate) static HASH_JOIN_MEM_DENIED_OPERATOR: AtomicU64 = AtomicU64::new(0);
pub(crate) static HASH_JOIN_MEM_DENIED_QUERY: AtomicU64 = AtomicU64::new(0);
pub(crate) static HASH_JOIN_MEM_DENIED_WORK_AREA: AtomicU64 = AtomicU64::new(0);

/// Couples the query-budget reservation with Hash Join's operator-local
/// observability. The current-charge gauge is decremented on every normal,
/// error, cancellation and unwind path by Drop.
struct HashJoinBudgetCharge {
    _budget: crate::engine::executor::mem_budget::ScopedCharge,
    bytes: u64,
}

impl HashJoinBudgetCharge {
    fn new(bytes: usize) -> Self {
        let bytes = bytes as u64;
        let previous = HASH_JOIN_CHARGED_BYTES.fetch_add(bytes, Ordering::Relaxed);
        HASH_JOIN_PEAK_CHARGED_BYTES.fetch_max(previous.saturating_add(bytes), Ordering::Relaxed);
        Self {
            _budget: crate::engine::executor::mem_budget::ScopedCharge::new(bytes as usize),
            bytes,
        }
    }
}

impl Drop for HashJoinBudgetCharge {
    fn drop(&mut self) {
        HASH_JOIN_CHARGED_BYTES.fetch_sub(self.bytes, Ordering::Relaxed);
    }
}
pub(crate) static HASH_JOIN_TYPED_KEY_FAST: AtomicU64 = AtomicU64::new(0);
pub(crate) static HASH_JOIN_TYPED_KEY_ARITY_1: AtomicU64 = AtomicU64::new(0);
pub(crate) static HASH_JOIN_TYPED_KEY_ARITY_2: AtomicU64 = AtomicU64::new(0);
pub(crate) static HASH_JOIN_TYPED_KEY_ARITY_3: AtomicU64 = AtomicU64::new(0);
pub(crate) static HASH_JOIN_TYPED_KEY_ARITY_4: AtomicU64 = AtomicU64::new(0);
pub(crate) static HASH_JOIN_TYPED_KEY_ARITY_5_PLUS: AtomicU64 = AtomicU64::new(0);
pub(crate) static HASH_JOIN_TYPED_KEY_FALLBACK_UINT_RANGE: AtomicU64 = AtomicU64::new(0);
pub(crate) static HASH_JOIN_TYPED_KEY_FALLBACK_RUNTIME_TYPE: AtomicU64 = AtomicU64::new(0);
/// WS-P1-1: times a build-side-preserved LEFT/RIGHT join entered the parallel probe path.
pub(crate) static HASH_JOIN_PARALLEL_BUILD_PRESERVED: AtomicU64 = AtomicU64::new(0);
pub(crate) static HASH_JOIN_RESIDUAL_PRECHECK: AtomicU64 = AtomicU64::new(0);
/// S2 fused projection: times the join output produced narrowed rows per the projection directly
/// at the stitch point (counted per join; both the materialized path and streaming-inner count; the chained streaming path keeps its own emit stats).
pub(crate) static HASH_JOIN_PROJECTION_FUSED: AtomicU64 = AtomicU64::new(0);
/// WS-2/3a: times the partitioned parallel path computed scatter pids via parallel chunking (counted per side).
pub(crate) static HASH_JOIN_PARALLEL_SCATTER: AtomicU64 = AtomicU64::new(0);
/// WS-P0-4: cumulative count of non-null-safe NULL-key rows scattered via row_idx to a partition other than 0.
/// 0 = the old "NULL → partition 0" semantics still hold (or the workload has no NULL keys); >0 = scatter is in effect.
pub(crate) static HASH_JOIN_PARALLEL_NULL_SCATTERED: AtomicU64 = AtomicU64::new(0);
/// Hits of the executor-level NLJ→hash equi fast path (the plan node remains NestedLoopJoin).
pub(crate) static NLJ_HASH_FAST_PATH: AtomicU64 = AtomicU64::new(0);
/// Hits of the covering-batch INLJ unique-key existence / derived-column fast path (P0-2).
pub(crate) static INLJ_COVERING_EXISTENCE_FAST: AtomicU64 = AtomicU64::new(0);
pub(crate) static ADAPTIVE_JOIN_COUNT: AtomicU64 = AtomicU64::new(0);
pub(crate) static ADAPTIVE_JOIN_CHOSEN_HASH: AtomicU64 = AtomicU64::new(0);
pub(crate) static ADAPTIVE_JOIN_CHOSEN_INDEX_NLJ: AtomicU64 = AtomicU64::new(0);
pub(crate) static ADAPTIVE_JOIN_CHOSEN_NLJ: AtomicU64 = AtomicU64::new(0);
pub(crate) static ADAPTIVE_JOIN_SWITCHED_FROM_PLANNER: AtomicU64 = AtomicU64::new(0);

// Non-covering index-range MRR runtime diagnostics. These describe execution
// mechanics only and are never consulted by the planner.
pub(crate) static MRR_WINDOWS: AtomicU64 = AtomicU64::new(0);
pub(crate) static MRR_RIDS: AtomicU64 = AtomicU64::new(0);
pub(crate) static MRR_UNIQUE_PAGES: AtomicU64 = AtomicU64::new(0);
pub(crate) static MRR_PAGE_REUSES: AtomicU64 = AtomicU64::new(0);
pub(crate) static MRR_WORKAREA_FLUSHES: AtomicU64 = AtomicU64::new(0);
pub(crate) static MRR_PEAK_BYTES: AtomicU64 = AtomicU64::new(0);

// WS-N8 NLJ/INLJ observability additions
//
// Bare NLJ: from_rows main-loop executions, cumulative pairs compared, pairs kept (condition hits)
// + Block-NL batched pairs evaluated / pairs kept / inner rescans (for N1, stubs for now)
// + outer parallel chunk count (for N2, stubs for now).
pub(crate) static NLJ_EXECUTIONS: AtomicU64 = AtomicU64::new(0);
pub(crate) static NLJ_PAIRS_COMPARED: AtomicU64 = AtomicU64::new(0);
pub(crate) static NLJ_PAIRS_KEPT: AtomicU64 = AtomicU64::new(0);
pub(crate) static NLJ_BLOCK_EXECUTIONS: AtomicU64 = AtomicU64::new(0);
pub(crate) static NLJ_BLOCK_PAIRS_EVALUATED: AtomicU64 = AtomicU64::new(0);
pub(crate) static NLJ_BLOCK_PAIRS_KEPT: AtomicU64 = AtomicU64::new(0);
pub(crate) static NLJ_OUTER_PARALLEL_CHUNKS: AtomicU64 = AtomicU64::new(0);
// WS-N3: outer blocks whose mask fill took the SIMD vectorized path (at least one inner row
// used the const kernel mask rather than the whole block falling back to row-wise). Differential
// tests assert it increments, confirming the vectorized path truly runs instead of silently falling back.
pub(crate) static NLJ_SIMD_FILL_BLOCKS: AtomicU64 = AtomicU64::new(0);
// INLJ: per-row main-loop fallback executions; total chunks triggered by outer parallelism;
// total chunks triggered by parallel batch-filter passes; ungrouped/grouped fused hits;
// N4 range-promote triggers (for N4, stubs for now); N5 MRR cross-chunk share hits (for N5, stubs for now).
pub(crate) static INLJ_PER_ROW_EXECUTIONS: AtomicU64 = AtomicU64::new(0);
pub(crate) static INLJ_OUTER_PARALLEL_CHUNKS: AtomicU64 = AtomicU64::new(0);
pub(crate) static INLJ_BATCH_PARALLEL_CHUNKS: AtomicU64 = AtomicU64::new(0);
pub(crate) static INLJ_PROBE_PARALLEL_DECISIONS: AtomicU64 = AtomicU64::new(0);
pub(crate) static INLJ_PROBE_PARALLEL_TASKS: AtomicU64 = AtomicU64::new(0);
pub(crate) static INLJ_PROBE_PARALLEL_ROWS: AtomicU64 = AtomicU64::new(0);
pub(crate) static INLJ_BATCH_RANGE_PROMOTED: AtomicU64 = AtomicU64::new(0);
pub(crate) static INLJ_MRR_CHUNK_SHARED_HITS: AtomicU64 = AtomicU64::new(0);
/// Cumulative non-covering batched-INLJ cardinalities.  These make estimate
/// errors and residual selectivity observable without enabling a profiler.
pub(crate) static INLJ_PROBE_KEYS: AtomicU64 = AtomicU64::new(0);
pub(crate) static INLJ_INDEX_ENTRIES: AtomicU64 = AtomicU64::new(0);
pub(crate) static INLJ_CANDIDATE_ROW_IDS: AtomicU64 = AtomicU64::new(0);
pub(crate) static INLJ_PREDICATE_FIRST_BATCHES: AtomicU64 = AtomicU64::new(0);
pub(crate) static INLJ_PREDICATE_FIRST_INPUT_ROWS: AtomicU64 = AtomicU64::new(0);
pub(crate) static INLJ_PREDICATE_FIRST_OUTPUT_ROWS: AtomicU64 = AtomicU64::new(0);
/// Runtime-KeySet secondary-index intersection decisions. These distinguish a
/// genuinely selective index intersection from the candidate-first fallback
/// and expose stale-statistics budget aborts.
pub(crate) static INLJ_RUNTIME_KEYSET_INTERSECT_ATTEMPTS: AtomicU64 = AtomicU64::new(0);
pub(crate) static INLJ_RUNTIME_KEYSET_INTERSECT_CHOSEN: AtomicU64 = AtomicU64::new(0);
pub(crate) static INLJ_RUNTIME_KEYSET_INTERSECT_COST_REJECTED: AtomicU64 = AtomicU64::new(0);
pub(crate) static INLJ_RUNTIME_KEYSET_INTERSECT_BUDGET_REJECTED: AtomicU64 = AtomicU64::new(0);
pub(crate) static INLJ_RUNTIME_KEYSET_INTERSECT_INDEX_ENTRIES: AtomicU64 = AtomicU64::new(0);
pub(crate) static INLJ_RUNTIME_KEYSET_CANDIDATE_REJECTED: AtomicU64 = AtomicU64::new(0);

pub(super) fn increment_hash_join_count() {
    HASH_JOIN_COUNT.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
}
fn record_nlj_hash_fast_path() {
    NLJ_HASH_FAST_PATH.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
}
fn record_inlj_covering_existence_fast() {
    INLJ_COVERING_EXISTENCE_FAST.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
}
pub(super) fn record_hash_join_build_rows(n: u64) {
    HASH_JOIN_BUILD_ROWS.fetch_add(n, std::sync::atomic::Ordering::Relaxed);
}
pub(super) fn record_hash_join_probe_rows(n: u64) {
    HASH_JOIN_PROBE_ROWS.fetch_add(n, std::sync::atomic::Ordering::Relaxed);
}
fn record_hash_join_null_skipped() {
    HASH_JOIN_NULL_SKIPPED.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
}
pub(super) fn record_hash_join_mem_exceeded() {
    HASH_JOIN_MEM_EXCEEDED.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
}
fn record_hash_join_fallback() {
    HASH_JOIN_FALLBACK_TO_NLJ.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
}
pub(super) fn record_hash_join_bloom_built() {
    HASH_JOIN_BLOOM_BUILT.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
}
pub(super) fn record_hash_join_bloom_filtered() {
    record_hash_join_bloom_filtered_n(1);
}
#[inline]
pub(super) fn record_hash_join_bloom_filtered_n(count: u64) {
    HASH_JOIN_BLOOM_FILTERED.fetch_add(count, std::sync::atomic::Ordering::Relaxed);
}
pub(super) fn record_hash_join_parallel_partitions(n: u64) {
    HASH_JOIN_PARALLEL_PARTITIONS.fetch_add(n, std::sync::atomic::Ordering::Relaxed);
}
fn record_hash_join_grace_partitions(n: u64) {
    HASH_JOIN_GRACE_PARTITIONS.fetch_add(n, std::sync::atomic::Ordering::Relaxed);
}
/// Count of times the streaming row hash aggregate overflowed the
/// in-memory budget at runtime and retried via the disk-spilling path
/// (a CBO cardinality under-estimate that the adaptive fallback caught).
static ROW_HASH_AGG_ADAPTIVE_SPILL: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);
pub(super) fn record_row_hash_agg_adaptive_spill() {
    ROW_HASH_AGG_ADAPTIVE_SPILL.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
}
pub(crate) fn row_hash_agg_adaptive_spill_count() -> u64 {
    ROW_HASH_AGG_ADAPTIVE_SPILL.load(std::sync::atomic::Ordering::Relaxed)
}
fn record_hash_join_spill_bytes(n: u64) {
    HASH_JOIN_SPILL_BYTES.fetch_add(n, std::sync::atomic::Ordering::Relaxed);
}
pub(super) fn record_hash_join_typed_key_fast(arity: usize) {
    HASH_JOIN_TYPED_KEY_FAST.fetch_add(1, Ordering::Relaxed);
    match arity {
        1 => &HASH_JOIN_TYPED_KEY_ARITY_1,
        2 => &HASH_JOIN_TYPED_KEY_ARITY_2,
        3 => &HASH_JOIN_TYPED_KEY_ARITY_3,
        4 => &HASH_JOIN_TYPED_KEY_ARITY_4,
        _ => &HASH_JOIN_TYPED_KEY_ARITY_5_PLUS,
    }
    .fetch_add(1, Ordering::Relaxed);
}
#[inline]
pub(super) fn record_hash_join_parallel_build_preserved() {
    HASH_JOIN_PARALLEL_BUILD_PRESERVED.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
}
pub(super) fn record_hash_join_residual_precheck() {
    HASH_JOIN_RESIDUAL_PRECHECK.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
}
pub(super) fn record_hash_join_projection_fused() {
    HASH_JOIN_PROJECTION_FUSED.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
}
pub(super) fn record_hash_join_parallel_scatter() {
    HASH_JOIN_PARALLEL_SCATTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
}
#[inline]
pub(super) fn record_hash_join_parallel_null_scattered() {
    HASH_JOIN_PARALLEL_NULL_SCATTERED.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
}

fn record_adaptive_join_count() {
    ADAPTIVE_JOIN_COUNT.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
}

#[inline]
fn record_nlj_execution() {
    NLJ_EXECUTIONS.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
}
#[inline]
fn record_nlj_pairs(compared: u64, kept: u64) {
    if compared > 0 {
        NLJ_PAIRS_COMPARED.fetch_add(compared, std::sync::atomic::Ordering::Relaxed);
    }
    if kept > 0 {
        NLJ_PAIRS_KEPT.fetch_add(kept, std::sync::atomic::Ordering::Relaxed);
    }
}
#[inline]
#[allow(dead_code)] // wired by WS-N1 Block-NL
fn record_nlj_block_execution(pairs_evaluated: u64, pairs_kept: u64) {
    NLJ_BLOCK_EXECUTIONS.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    if pairs_evaluated > 0 {
        NLJ_BLOCK_PAIRS_EVALUATED.fetch_add(pairs_evaluated, std::sync::atomic::Ordering::Relaxed);
    }
    if pairs_kept > 0 {
        NLJ_BLOCK_PAIRS_KEPT.fetch_add(pairs_kept, std::sync::atomic::Ordering::Relaxed);
    }
}
#[inline]
fn record_nlj_outer_parallel_chunks(chunks: u64) {
    if chunks > 0 {
        NLJ_OUTER_PARALLEL_CHUNKS.fetch_add(chunks, std::sync::atomic::Ordering::Relaxed);
    }
}
#[inline]
fn record_inlj_per_row_execution() {
    INLJ_PER_ROW_EXECUTIONS.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
}
#[inline]
fn record_inlj_outer_parallel_chunks(chunks: u64) {
    if chunks > 0 {
        INLJ_OUTER_PARALLEL_CHUNKS.fetch_add(chunks, std::sync::atomic::Ordering::Relaxed);
    }
}
#[inline]
fn record_inlj_batch_parallel_chunks(chunks: u64) {
    if chunks > 0 {
        INLJ_BATCH_PARALLEL_CHUNKS.fetch_add(chunks, std::sync::atomic::Ordering::Relaxed);
    }
}
#[inline]
fn record_inlj_probe_parallel(tasks: u64, rows: u64) {
    INLJ_PROBE_PARALLEL_DECISIONS.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    INLJ_PROBE_PARALLEL_TASKS.fetch_add(tasks, std::sync::atomic::Ordering::Relaxed);
    INLJ_PROBE_PARALLEL_ROWS.fetch_add(rows, std::sync::atomic::Ordering::Relaxed);
}
#[inline]
fn record_inlj_probe_batch(keys: u64, entries: u64, candidates: u64) {
    INLJ_PROBE_KEYS.fetch_add(keys, std::sync::atomic::Ordering::Relaxed);
    INLJ_INDEX_ENTRIES.fetch_add(entries, std::sync::atomic::Ordering::Relaxed);
    INLJ_CANDIDATE_ROW_IDS.fetch_add(candidates, std::sync::atomic::Ordering::Relaxed);
}
#[inline]
fn record_inlj_predicate_first_rows(input: u64, output: u64) {
    INLJ_PREDICATE_FIRST_BATCHES.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    INLJ_PREDICATE_FIRST_INPUT_ROWS.fetch_add(input, std::sync::atomic::Ordering::Relaxed);
    INLJ_PREDICATE_FIRST_OUTPUT_ROWS.fetch_add(output, std::sync::atomic::Ordering::Relaxed);
}
#[inline]
fn record_inlj_batch_range_promoted() {
    INLJ_BATCH_RANGE_PROMOTED.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
}
#[inline]
fn record_inlj_mrr_chunk_shared_hit() {
    INLJ_MRR_CHUNK_SHARED_HITS.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
}

fn record_adaptive_choice(chosen: AdaptiveJoinChoice, preferred: AdaptiveJoinChoice) {
    if chosen != preferred {
        ADAPTIVE_JOIN_SWITCHED_FROM_PLANNER.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }
    match chosen {
        AdaptiveJoinChoice::Hash => {
            ADAPTIVE_JOIN_CHOSEN_HASH.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        }
        AdaptiveJoinChoice::IndexNestedLoop => {
            ADAPTIVE_JOIN_CHOSEN_INDEX_NLJ.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        }
        AdaptiveJoinChoice::NestedLoop => {
            ADAPTIVE_JOIN_CHOSEN_NLJ.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        }
    }
}

#[cfg(test)]
mod compiled_fast_path_tests;
