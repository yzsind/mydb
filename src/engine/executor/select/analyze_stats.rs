//! Per-operator runtime statistics for `EXPLAIN ANALYZE`.
//!
//! When EXPLAIN ANALYZE FORMAT=TREE/JSON runs a query, we want to attach
//! the actual rows/time/loops to *each* plan node, not just the root.
//! The collector lives in a thread-local so `execute_plan_rows`
//! (`super::plan`) can record measurements without threading an extra
//! parameter through every recursive call. Operators are keyed by stable,
//! deterministic [`PlanNodeId`] values. Raw addresses are contained inside
//! the execution overlay and are used only to bind immutable plan instances
//! (including execution clones) to those IDs.

use crate::planner::{PhysicalPlan, PlanNodeId};
use std::cell::{Cell, RefCell};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub(crate) enum VectorRequestedMode {
    #[default]
    Unknown,
    Off,
    On,
    Auto,
}

impl From<crate::engine::executor::vec::config::VecMode> for VectorRequestedMode {
    fn from(mode: crate::engine::executor::vec::config::VecMode) -> Self {
        use crate::engine::executor::vec::config::VecMode;
        match mode {
            VecMode::Off => Self::Off,
            VecMode::On => Self::On,
            VecMode::Auto => Self::Auto,
        }
    }
}

impl VectorRequestedMode {
    fn as_str(self) -> &'static str {
        match self {
            Self::Unknown => "unknown",
            Self::Off => "off",
            Self::On => "on",
            Self::Auto => "auto",
        }
    }
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub(crate) enum VectorFallbackReason {
    #[default]
    None,
    UnsupportedPlan,
}

impl VectorFallbackReason {
    fn as_str(self) -> &'static str {
        match self {
            Self::None => "none",
            Self::UnsupportedPlan => "unsupported_plan",
        }
    }
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub(crate) enum StatsSource {
    /// Sidecar counters exist, but no completed operator scope was observed.
    #[default]
    Unmeasured,
    /// Rows/time/loops were published by the operator that executed.
    Measured,
    /// Exact measurements were rebound from an execution clone of this node.
    Mirrored,
    /// This display node was fused into another measured operator.  Runtime
    /// values are never copied into it; `included_in` identifies the owner.
    Included,
}

impl StatsSource {
    pub(super) fn as_str(self) -> &'static str {
        match self {
            Self::Unmeasured => "unmeasured",
            Self::Measured => "measured",
            Self::Mirrored => "mirrored",
            Self::Included => "included",
        }
    }
}

#[derive(Debug, Default, Clone, Copy)]
pub(crate) struct NodeStats {
    pub source: StatsSource,
    /// Sum, over all loops, of the active time needed to produce the first
    /// iterator result.  EOF is the first result for an empty loop.
    pub first_row_ns: u128,
    /// Inclusive active time summed over all loops.  Kept as `time_ns` to
    /// avoid bloating every executor call site; renderers expose the precise
    /// completion/total names.
    pub time_ns: u128,
    /// Active time in the operator itself, excluding synchronous child calls.
    /// Executors which cannot split the interval conservatively leave this at
    /// zero rather than fabricating a value.
    pub self_time_ns: u128,
    pub rows: u64,
    pub loops: u64,
    /// Stable preorder id of the measured owner when this node was fused.
    pub included_in: Option<PlanNodeId>,
    pub cache_reads: u64,
    pub physical_reads: u64,
    pub io_is_self: bool,
    /// Rows rejected by a scan-local runtime Bloom filter before they reached
    /// the owning operator's output.
    pub runtime_filter_rejected: u64,
    pub build_rows: u64,
    pub probe_rows: u64,
    pub match_rows: u64,
    pub index_entries_examined: u64,
    pub index_merge_branches: u64,
    pub index_merge_candidate_rowids: u64,
    pub index_merge_duplicates_removed: u64,
    pub heap_rows_fetched: u64,
    pub heap_page_probes: u64,
    pub mrr_windows: u64,
    pub mrr_rids: u64,
    pub mrr_unique_pages: u64,
    pub mrr_page_reuses: u64,
    pub mrr_workarea_flushes: u64,
    pub mrr_peak_bytes: u64,
    pub row_goal: u64,
    pub candidate_batches: u64,
    pub candidate_window_peak: u64,
    /// High-water mark of bytes the operator's own state held in memory.
    /// Set by streaming operators (e.g. HashAggregate) via `add_peak_memory`.
    pub peak_memory_bytes: u64,
    /// Total bytes the operator wrote to spill files. 0 means no spill.
    pub spill_bytes: u64,
    pub spill_bytes_read: u64,
    /// Unified spill shape across blocking operators. The legacy sort fields
    /// below remain populated for compatibility with existing diagnostics.
    pub spill_runs: u64,
    pub spill_passes: u64,
    pub spill_partitions: u64,
    pub spill_file_opens: u64,
    pub spill_repartitions: u64,
    pub spill_max_depth: u64,
    pub spill_reason: Option<&'static str>,
    pub spill_input_rows: u64,
    pub spill_partial_rows: u64,
    /// Number of external-sort runs generated by this node.
    pub sort_runs: u64,
    /// Number of external-sort merge passes before final output.
    pub sort_merge_passes: u64,
    /// True if this execution ran through a real streaming path (the
    /// operator produced batches incrementally). False means the operator
    /// fell back to a fully-materialised legacy implementation.
    pub is_streaming: bool,
    /// True when this node executed inside the columnar pipeline.
    pub is_vectorized: bool,
    /// Session request and the reason a requested vector path stayed row.
    pub vector_requested_mode: VectorRequestedMode,
    pub vector_fallback_reason: VectorFallbackReason,
    /// Input/output batches processed by the vector operator.
    pub batches: u64,
    /// Rows converted across the PlanRow/Batch boundary (zero for native
    /// storage/CTE pipelines and final protocol materialization).
    pub conversion_rows: u64,
    /// Expression kernel actually used inside the operator. Observability
    /// only; these fields never participate in plan or execution routing.
    pub expression_mode: Option<&'static str>,
    pub native_expression_rows: u64,
    pub fallback_expression_rows: u64,
    /// Aggregate kernel selected inside the physical aggregate node.
    /// Observability only; never read by CBO or routing.
    pub aggregate_mode: Option<&'static str>,
    pub native_aggregate_rows: u64,
    pub fallback_aggregate_rows: u64,
    pub distinct_spill_bytes: u64,
    pub window_mode: Option<&'static str>,
    pub window_spill_bytes: u64,
    pub window_peak_partition_rows: u64,
    /// Maximum worker count used by this node.
    pub workers: u32,
    /// Number of physical table partitions actually opened by this node.
    /// Populated for partitioned scan nodes from the storage-layer visit
    /// counter; zero means either non-partitioned or not applicable.
    pub partitions: u64,
    pub zone_metadata_ns: u64,
    pub zone_candidate_ns: u64,
    pub zone_heap_scan_ns: u64,
    pub zone_tasks: u64,
    pub zone_pages_scanned: u64,
    pub zone_pages_skipped: u64,
    pub zone_fallbacks: u64,
    pub zone_workers: u64,
}

#[derive(Debug, Default)]
pub(super) struct NodeStatsMap {
    stats: HashMap<PlanNodeId, NodeStats>,
    address_to_id: HashMap<usize, AddressBinding>,
    query_io: NodeIoStats,
    unassigned_io: NodeIoStats,
    executed_plan: Option<Box<PhysicalPlan>>,
    // Keeps the unified-governor reservation alive through rendering.
    _reservation: Option<crate::engine::executor::mem_budget::ControlledMemoryCharge>,
}

impl NodeStatsMap {
    pub(super) fn query_io(&self) -> NodeIoStats {
        self.query_io
    }

    pub(super) fn unassigned_io(&self) -> NodeIoStats {
        self.unassigned_io
    }

    pub(super) fn take_executed_plan(&mut self) -> Option<Box<PhysicalPlan>> {
        self.executed_plan.take()
    }

    pub(super) fn get_by_id(&self, id: PlanNodeId) -> Option<&NodeStats> {
        self.stats.get(&id)
    }

    pub(super) fn get_for_plan(&self, plan: &PhysicalPlan) -> Option<&NodeStats> {
        self.address_to_id
            .get(&plan_address(plan))
            .filter(|binding| binding.kind == std::mem::discriminant(plan))
            .and_then(|binding| self.stats.get(&binding.id))
    }

    pub(super) fn insert_for_plan(&mut self, plan: &PhysicalPlan, stats: NodeStats) {
        if let Some(binding) = self
            .address_to_id
            .get(&plan_address(plan))
            .filter(|binding| binding.kind == std::mem::discriminant(plan))
        {
            self.stats.insert(binding.id, stats);
        }
    }

    pub(super) fn id_for_plan(&self, plan: &PhysicalPlan) -> Option<PlanNodeId> {
        self.address_to_id
            .get(&plan_address(plan))
            .filter(|binding| binding.kind == std::mem::discriminant(plan))
            .map(|binding| binding.id)
    }

    /// Bind a display-only tree after execution. WITH/CTE fallbacks may have
    /// to execute a freshly planned graph for correctness, so those display
    /// node addresses never passed through the live collector. Assigning
    /// stable overlay IDs here lets the caller attach the measured block root
    /// and mark its descendants as fused instead of rendering a completely
    /// misleading `(never executed)` tree.
    pub(super) fn ensure_tree(&mut self, plan: &PhysicalPlan) {
        let mut next_id = self
            .address_to_id
            .values()
            .map(|binding| binding.id.0)
            .max()
            .map_or(0, |id| id.saturating_add(1));
        plan.walk(|node| {
            let address = plan_address(node);
            let kind = std::mem::discriminant(node);
            if !self
                .address_to_id
                .get(&address)
                .is_some_and(|binding| binding.kind == kind)
            {
                self.address_to_id.insert(
                    address,
                    AddressBinding {
                        id: PlanNodeId(next_id),
                        kind,
                    },
                );
                next_id = next_id.saturating_add(1);
            }
        });
    }

    /// Aggregate measurements from execution-only SetOp plan instances. A
    /// lossy WITH/derived display plan is rebuilt after execution, so pointer
    /// identity cannot be mirrored one-for-one; the aggregate is attached to
    /// the visible SetOp boundary while its children remain explicitly fused.
    pub(super) fn aggregate_setop_stats(&self, exemplar: &PhysicalPlan) -> Option<NodeStats> {
        debug_assert!(matches!(exemplar, PhysicalPlan::SetOp { .. }));
        let setop_kind = std::mem::discriminant(exemplar);
        let ids = self
            .address_to_id
            .values()
            .filter(|binding| binding.kind == setop_kind)
            .map(|binding| binding.id)
            .collect::<std::collections::HashSet<_>>();
        let mut combined = NodeStats::default();
        let mut found = false;
        for id in ids {
            let Some(stats) = self.stats.get(&id).copied() else {
                continue;
            };
            if stats.loops == 0 {
                continue;
            }
            found = true;
            combined.source = StatsSource::Measured;
            combined.first_row_ns = combined.first_row_ns.saturating_add(stats.first_row_ns);
            combined.time_ns = combined.time_ns.saturating_add(stats.time_ns);
            combined.self_time_ns = combined.self_time_ns.saturating_add(stats.self_time_ns);
            combined.rows = combined.rows.saturating_add(stats.rows);
            combined.loops = combined.loops.saturating_add(stats.loops);
            combined.peak_memory_bytes = combined.peak_memory_bytes.max(stats.peak_memory_bytes);
            combined.spill_bytes = combined.spill_bytes.saturating_add(stats.spill_bytes);
            combined.spill_bytes_read = combined
                .spill_bytes_read
                .saturating_add(stats.spill_bytes_read);
            combined.spill_runs = combined.spill_runs.saturating_add(stats.spill_runs);
            combined.spill_passes = combined.spill_passes.saturating_add(stats.spill_passes);
            combined.spill_partitions = combined.spill_partitions.max(stats.spill_partitions);
            combined.spill_file_opens = combined
                .spill_file_opens
                .saturating_add(stats.spill_file_opens);
            combined.spill_repartitions = combined
                .spill_repartitions
                .saturating_add(stats.spill_repartitions);
            combined.spill_max_depth = combined.spill_max_depth.max(stats.spill_max_depth);
            combined.spill_reason = combined.spill_reason.or(stats.spill_reason);
        }
        found.then_some(combined)
    }
}

#[derive(Debug, Clone, Copy)]
struct AddressBinding {
    id: PlanNodeId,
    kind: std::mem::Discriminant<PhysicalPlan>,
}

#[derive(Debug, Default)]
struct ExecutionOverlay {
    address_to_id: HashMap<usize, AddressBinding>,
    next_id: u32,
}

impl ExecutionOverlay {
    fn ensure_tree(&mut self, plan: &PhysicalPlan) -> PlanNodeId {
        let root_address = plan_address(plan);
        // A few dispatch paths insert a WindowAgg after preparing a cached
        // root. Always walk the current finalized shape: existing bindings
        // stay stable while newly introduced descendants receive IDs before
        // clone binding tries to resolve them.
        plan.walk(|node| {
            let address = plan_address(node);
            let kind = std::mem::discriminant(node);
            if !self
                .address_to_id
                .get(&address)
                .is_some_and(|binding| binding.kind == kind)
            {
                let id = PlanNodeId(self.next_id);
                self.next_id = self
                    .next_id
                    .checked_add(1)
                    .expect("execution overlay exhausted PlanNodeId space");
                self.address_to_id
                    .insert(address, AddressBinding { id, kind });
            }
        });
        self.address_to_id[&root_address].id
    }

    fn bind_clone(&mut self, executed: &PhysicalPlan, display: &PhysicalPlan) {
        self.ensure_tree(display);
        fn bind(overlay: &mut ExecutionOverlay, executed: &PhysicalPlan, display: &PhysicalPlan) {
            assert_eq!(
                std::mem::discriminant(executed),
                std::mem::discriminant(display),
                "cannot bind ANALYZE clone with a different operator kind"
            );
            let id = overlay.address_to_id[&plan_address(display)].id;
            overlay.address_to_id.insert(
                plan_address(executed),
                AddressBinding {
                    id,
                    kind: std::mem::discriminant(executed),
                },
            );
            let executed_children = executed.children();
            let display_children = display.children();
            assert_eq!(
                executed_children.len(),
                display_children.len(),
                "cannot bind ANALYZE clone with different topology"
            );
            for (executed_child, display_child) in
                executed_children.into_iter().zip(display_children)
            {
                bind(overlay, executed_child, display_child);
            }
        }
        bind(self, executed, display);
    }

    fn bind_key(&mut self, executed: &PhysicalPlan, key: PlanNodeId) {
        self.address_to_id.insert(
            plan_address(executed),
            AddressBinding {
                id: key,
                kind: std::mem::discriminant(executed),
            },
        );
    }

    fn bind_snapshot(&mut self, snapshot: &PhysicalPlan, executed: &PhysicalPlan) {
        self.ensure_tree(executed);
        fn bind(overlay: &mut ExecutionOverlay, snapshot: &PhysicalPlan, executed: &PhysicalPlan) {
            debug_assert_eq!(
                std::mem::discriminant(snapshot),
                std::mem::discriminant(executed),
                "execution-plan snapshot changed node kind"
            );
            let executed_binding = overlay.address_to_id[&plan_address(executed)];
            overlay.address_to_id.insert(
                plan_address(snapshot),
                AddressBinding {
                    id: executed_binding.id,
                    kind: std::mem::discriminant(snapshot),
                },
            );
            let snapshot_children = snapshot.children();
            let executed_children = executed.children();
            debug_assert_eq!(
                snapshot_children.len(),
                executed_children.len(),
                "execution-plan snapshot changed topology"
            );
            for (snapshot_child, executed_child) in
                snapshot_children.into_iter().zip(executed_children)
            {
                bind(overlay, snapshot_child, executed_child);
            }
        }
        bind(self, snapshot, executed);
    }
}

#[derive(Debug)]
pub(crate) struct StatsCollector {
    stats: Mutex<HashMap<PlanNodeId, NodeStats>>,
    overlay: Mutex<ExecutionOverlay>,
    query_io: Arc<crate::storage::QueryBufferStats>,
    executed_plan: Mutex<Option<Box<PhysicalPlan>>>,
    reservation: Mutex<Option<crate::engine::executor::mem_budget::ControlledMemoryCharge>>,
}

impl StatsCollector {
    const TRACE_BASE_BYTES: usize = 64 * 1024;
    const TRACE_BYTES_PER_NODE: usize = 1024;

    fn try_new() -> Result<Self, crate::error::QueryError> {
        use crate::engine::executor::mem_budget::{
            ControlledMemoryCategory, ControlledMemoryCharge,
        };
        let reservation = ControlledMemoryCharge::try_new(
            ControlledMemoryCategory::Executor,
            Self::TRACE_BASE_BYTES,
        )?;
        Ok(Self {
            stats: Mutex::new(HashMap::with_capacity(64)),
            overlay: Mutex::new(ExecutionOverlay::default()),
            query_io: Arc::new(crate::storage::QueryBufferStats::default()),
            executed_plan: Mutex::new(None),
            reservation: Mutex::new(Some(reservation)),
        })
    }

    fn plan_key(&self, plan: &PhysicalPlan) -> PlanNodeId {
        self.overlay
            .lock()
            .expect("stats overlay lock poisoned")
            .ensure_tree(plan)
    }

    fn snapshot(&self) -> NodeStatsMap {
        let (cache_reads, physical_reads) =
            crate::storage::query_buffer_stats_counts(&self.query_io);
        let stats = self.stats.lock().map(|map| map.clone()).unwrap_or_default();
        let attributed_cache = stats
            .values()
            .filter(|value| value.io_is_self)
            .fold(0u64, |sum, value| sum.saturating_add(value.cache_reads));
        let attributed_physical = stats
            .values()
            .filter(|value| value.io_is_self)
            .fold(0u64, |sum, value| sum.saturating_add(value.physical_reads));
        NodeStatsMap {
            stats,
            address_to_id: self
                .overlay
                .lock()
                .map(|overlay| overlay.address_to_id.clone())
                .unwrap_or_default(),
            query_io: NodeIoStats {
                cache_reads,
                physical_reads,
            },
            unassigned_io: NodeIoStats {
                cache_reads: cache_reads.saturating_sub(attributed_cache),
                physical_reads: physical_reads.saturating_sub(attributed_physical),
            },
            // Move the pinned snapshot out without cloning it: address
            // bindings intentionally refer to the nodes inside this Box and
            // remain valid when the Box itself is moved into NodeStatsMap.
            executed_plan: self
                .executed_plan
                .lock()
                .ok()
                .and_then(|mut plan| plan.take()),
            _reservation: self
                .reservation
                .lock()
                .ok()
                .and_then(|mut charge| charge.take()),
        }
    }
}

type SharedStatsCollector = Arc<StatsCollector>;

/// Sendable ANALYZE context. The operator I/O sink is captured together with
/// the statement collector so manually spawned executor workers preserve both
/// the trace and the deepest active operator attribution scope.
#[derive(Clone)]
pub(crate) struct CollectorHandle {
    collector: SharedStatsCollector,
    operator_io: Option<Arc<crate::storage::QueryBufferStats>>,
}

#[derive(Debug, Default, Clone, Copy)]
pub(super) struct NodeIoStats {
    pub cache_reads: u64,
    pub physical_reads: u64,
}

thread_local! {
    static STATS_COLLECTOR: RefCell<Option<SharedStatsCollector>> = const { RefCell::new(None) };
    static RANGE_SCAN_PLAN_KEY: Cell<Option<PlanNodeId>> = const { Cell::new(None) };
    /// Hash-join probe misses produced inside the generic batch kernel. The
    /// enclosing HashJoin ANALYZE wrapper drains this thread-local counter to
    /// its stable plan key, avoiding a global counter that would mix queries.
    static PENDING_RUNTIME_FILTER_REJECTED: Cell<u64> = const { Cell::new(0) };
    /// Spill produced by the generic hash-join kernel before control returns
    /// to the enclosing stable plan-node wrapper.
    static PENDING_HASH_JOIN_SPILL_BYTES: Cell<u64> = const { Cell::new(0) };
    static PENDING_HASH_JOIN_SPILL_PARTITIONS: Cell<u64> = const { Cell::new(0) };
    static PENDING_SETOP_SPILL_BYTES: Cell<u64> = const { Cell::new(0) };
    static PENDING_SETOP_SPILL_PARTITIONS: Cell<u64> = const { Cell::new(0) };
    static PENDING_SETOP_SPILL_FILE_OPENS: Cell<u64> = const { Cell::new(0) };
    static PENDING_SETOP_PEAK_MEMORY: Cell<u64> = const { Cell::new(0) };
    static PENDING_SETOP_REPARTITIONS: Cell<u64> = const { Cell::new(0) };
    static PENDING_SETOP_MAX_DEPTH: Cell<u64> = const { Cell::new(0) };
    static PENDING_SETOP_MERGE_PASSES: Cell<u64> = const { Cell::new(0) };
    static PENDING_NATIVE_EXPRESSION_ROWS: Cell<u64> = const { Cell::new(0) };
    static PENDING_SCALAR_EXPRESSION_ROWS: Cell<u64> = const { Cell::new(0) };
    static PENDING_CONVERSION_ROWS: Cell<u64> = const { Cell::new(0) };
    static PENDING_JOIN_BUILD_ROWS: Cell<u64> = const { Cell::new(0) };
    static PENDING_JOIN_PROBE_ROWS: Cell<u64> = const { Cell::new(0) };
    static PENDING_JOIN_MATCH_ROWS: Cell<u64> = const { Cell::new(0) };
}

struct CollectorRestore(Option<SharedStatsCollector>);

impl Drop for CollectorRestore {
    fn drop(&mut self) {
        STATS_COLLECTOR.with(|cell| *cell.borrow_mut() = self.0.take());
    }
}

struct RangeScanKeyRestore(Option<PlanNodeId>);

impl Drop for RangeScanKeyRestore {
    fn drop(&mut self) {
        RANGE_SCAN_PLAN_KEY.with(|cell| cell.set(self.0));
    }
}

#[inline]
fn plan_address(plan: &PhysicalPlan) -> usize {
    std::ptr::from_ref(plan).addr()
}

/// Whether an execution trace sink is installed.  This is intentionally an
/// observability predicate only; executor admission/routing code must not use
/// it to choose a physical implementation.
pub(crate) fn is_recording() -> bool {
    STATS_COLLECTOR.with(|cell| cell.borrow().is_some())
}

pub(crate) fn current_collector_handle() -> Option<CollectorHandle> {
    STATS_COLLECTOR.with(|cell| {
        cell.borrow()
            .as_ref()
            .cloned()
            .map(|collector| CollectorHandle {
                collector,
                operator_io: crate::storage::current_shared_operator_buffer_stats(),
            })
    })
}

pub(super) fn current_range_scan_plan_key() -> Option<PlanNodeId> {
    RANGE_SCAN_PLAN_KEY.with(Cell::get)
}

pub(super) fn plan_key(plan: &PhysicalPlan) -> Option<PlanNodeId> {
    STATS_COLLECTOR.with(|cell| {
        cell.borrow()
            .as_ref()
            .map(|collector| collector.plan_key(plan))
    })
}

pub(super) fn bind_plan_clone(executed: &PhysicalPlan, display: &PhysicalPlan) {
    STATS_COLLECTOR.with(|cell| {
        if let Some(collector) = cell.borrow().as_ref() {
            collector
                .overlay
                .lock()
                .expect("stats overlay lock poisoned")
                .bind_clone(executed, display);
        }
    })
}

pub(super) fn bind_plan_key(executed: &PhysicalPlan, key: PlanNodeId) {
    STATS_COLLECTOR.with(|cell| {
        if let Some(collector) = cell.borrow().as_ref() {
            collector
                .overlay
                .lock()
                .expect("stats overlay lock poisoned")
                .bind_key(executed, key);
        }
    });
}

/// Capture the exact finalized plan handed to the ordinary executor. The
/// snapshot is formatted after execution and is bound to the same stable
/// preorder ids before any operator can publish a measurement.
pub(super) fn register_execution_plan(plan: &PhysicalPlan) -> Result<(), crate::error::QueryError> {
    STATS_COLLECTOR.with(|cell| {
        let Some(collector) = cell.borrow().as_ref().cloned() else {
            return Ok(());
        };
        let Ok(mut slot) = collector.executed_plan.lock() else {
            return Ok(());
        };
        if slot.is_some() {
            return Ok(());
        }
        let node_bytes = plan
            .node_count()
            .saturating_mul(StatsCollector::TRACE_BYTES_PER_NODE);
        let required = StatsCollector::TRACE_BASE_BYTES.max(node_bytes);
        if required > StatsCollector::TRACE_BASE_BYTES {
            if let Ok(mut reservation) = collector.reservation.lock() {
                if let Some(charge) = reservation.as_mut() {
                    charge.try_grow(required - StatsCollector::TRACE_BASE_BYTES)?;
                }
            }
        }
        let snapshot = Box::new(plan.clone());
        if let Ok(mut overlay) = collector.overlay.lock() {
            overlay.bind_snapshot(&snapshot, plan);
        }
        *slot = Some(snapshot);
        Ok(())
    })
}

pub(super) fn with_range_scan_plan<F, R>(plan: &PhysicalPlan, f: F) -> R
where
    F: FnOnce() -> R,
{
    // Single-table fast paths are invoked with the prepared plan root, which
    // is commonly a transparent Projection/Filter/Limit wrapper around the
    // range scan. Resolve the collector key from that immutable tree instead
    // of requiring the caller to execute a different, scan-rooted plan.
    fn range_scan_key(plan: &PhysicalPlan) -> Option<PlanNodeId> {
        if matches!(
            plan,
            PhysicalPlan::IndexMerge { .. }
                | PhysicalPlan::IndexLookup { .. }
                | PhysicalPlan::IndexMultiLookup { .. }
                | PhysicalPlan::IndexRangeScan { .. }
                | PhysicalPlan::IndexFastScan { .. }
                | PhysicalPlan::ParallelIndexRangeScan { .. }
                | PhysicalPlan::ParallelIndexFastScan { .. }
        ) {
            return plan_key(plan);
        }
        super::plan::plan_stats_children(plan)
            .into_iter()
            .find_map(range_scan_key)
    }

    let key = range_scan_key(plan);
    with_range_scan_plan_key_if_present(key, f)
}

pub(super) fn with_range_scan_plan_key_if_present<F, R>(key: Option<PlanNodeId>, f: F) -> R
where
    F: FnOnce() -> R,
{
    let _restore = RangeScanKeyRestore(RANGE_SCAN_PLAN_KEY.with(|cell| cell.replace(key)));
    f()
}

pub(crate) fn add_range_scan_work(
    index_entries_examined: u64,
    heap_rows_fetched: u64,
    heap_page_probes: u64,
) {
    let Some(key) = current_range_scan_plan_key() else {
        return;
    };
    add_range_scan_work_key(
        key,
        index_entries_examined,
        heap_rows_fetched,
        heap_page_probes,
    );
}

pub(crate) fn add_range_scan_work_for_plan(
    plan: &PhysicalPlan,
    index_entries_examined: u64,
    heap_rows_fetched: u64,
    heap_page_probes: u64,
) {
    let Some(key) = plan_key(plan) else {
        return;
    };
    add_range_scan_work_key(
        key,
        index_entries_examined,
        heap_rows_fetched,
        heap_page_probes,
    );
}

fn add_range_scan_work_key(
    key: PlanNodeId,
    index_entries_examined: u64,
    heap_rows_fetched: u64,
    heap_page_probes: u64,
) {
    STATS_COLLECTOR.with(|cell| {
        if let Some(collector) = cell.borrow().as_ref().cloned() {
            if let Ok(mut map) = collector.stats.lock() {
                let entry = map.entry(key).or_default();
                entry.index_entries_examined = entry
                    .index_entries_examined
                    .saturating_add(index_entries_examined);
                entry.heap_rows_fetched = entry.heap_rows_fetched.saturating_add(heap_rows_fetched);
                entry.heap_page_probes = entry.heap_page_probes.saturating_add(heap_page_probes);
            }
        }
    });
}

pub(crate) fn add_index_candidate_batch(row_goal: u64, rows: u64, window: u64) {
    let Some(key) = current_range_scan_plan_key() else {
        return;
    };
    STATS_COLLECTOR.with(|cell| {
        if let Some(collector) = cell.borrow().as_ref().cloned() {
            if let Ok(mut map) = collector.stats.lock() {
                let entry = map.entry(key).or_default();
                entry.row_goal = entry.row_goal.max(row_goal);
                entry.candidate_batches = entry.candidate_batches.saturating_add(1);
                entry.candidate_window_peak = entry.candidate_window_peak.max(window.max(rows));
            }
        }
    });
}

pub(crate) fn add_index_merge_work(branches: u64, candidates: u64, duplicates: u64) {
    let Some(key) = current_range_scan_plan_key() else {
        return;
    };
    STATS_COLLECTOR.with(|cell| {
        if let Some(collector) = cell.borrow().as_ref().cloned() {
            if let Ok(mut map) = collector.stats.lock() {
                let entry = map.entry(key).or_default();
                entry.index_merge_branches = entry.index_merge_branches.max(branches);
                entry.index_merge_candidate_rowids = entry
                    .index_merge_candidate_rowids
                    .saturating_add(candidates);
                entry.index_merge_duplicates_removed = entry
                    .index_merge_duplicates_removed
                    .saturating_add(duplicates);
            }
        }
    });
}

pub(super) fn add_runtime_filter_rejected_key(key: PlanNodeId, rows: u64) {
    if rows == 0 {
        return;
    }
    STATS_COLLECTOR.with(|cell| {
        if let Some(collector) = cell.borrow().as_ref().cloned() {
            if let Ok(mut map) = collector.stats.lock() {
                let entry = map.entry(key).or_default();
                entry.runtime_filter_rejected = entry.runtime_filter_rejected.saturating_add(rows);
            }
        }
    });
}

pub(crate) fn add_pending_runtime_filter_rejected(rows: u64) {
    if rows != 0 && is_recording() {
        PENDING_RUNTIME_FILTER_REJECTED.with(|cell| {
            cell.set(cell.get().saturating_add(rows));
        });
    }
}

pub(super) fn take_pending_runtime_filter_rejected() -> u64 {
    PENDING_RUNTIME_FILTER_REJECTED.with(|cell| cell.replace(0))
}

pub(crate) fn add_pending_hash_join_spill(bytes: u64, partitions: u64) {
    if !is_recording() {
        return;
    }
    PENDING_HASH_JOIN_SPILL_BYTES.with(|cell| {
        cell.set(cell.get().saturating_add(bytes));
    });
    PENDING_HASH_JOIN_SPILL_PARTITIONS.with(|cell| {
        cell.set(cell.get().saturating_add(partitions));
    });
}

pub(in crate::engine::executor) fn add_pending_setop_spill(
    bytes: u64,
    partitions: u64,
    file_opens: u64,
    peak_memory: u64,
    repartitions: u64,
    max_depth: u64,
    merge_passes: u64,
) {
    if !is_recording() {
        return;
    }
    PENDING_SETOP_SPILL_BYTES.with(|cell| cell.set(cell.get().saturating_add(bytes)));
    PENDING_SETOP_SPILL_PARTITIONS.with(|cell| {
        cell.set(cell.get().max(partitions));
    });
    PENDING_SETOP_SPILL_FILE_OPENS.with(|cell| {
        cell.set(cell.get().saturating_add(file_opens));
    });
    PENDING_SETOP_PEAK_MEMORY.with(|cell| cell.set(cell.get().max(peak_memory)));
    PENDING_SETOP_REPARTITIONS.with(|cell| {
        cell.set(cell.get().saturating_add(repartitions));
    });
    PENDING_SETOP_MAX_DEPTH.with(|cell| cell.set(cell.get().max(max_depth)));
    PENDING_SETOP_MERGE_PASSES.with(|cell| {
        cell.set(cell.get().saturating_add(merge_passes));
    });
}

pub(in crate::engine::executor) fn take_pending_setop_spill() -> (u64, u64, u64, u64, u64, u64, u64)
{
    (
        PENDING_SETOP_SPILL_BYTES.with(|cell| cell.replace(0)),
        PENDING_SETOP_SPILL_PARTITIONS.with(|cell| cell.replace(0)),
        PENDING_SETOP_SPILL_FILE_OPENS.with(|cell| cell.replace(0)),
        PENDING_SETOP_PEAK_MEMORY.with(|cell| cell.replace(0)),
        PENDING_SETOP_REPARTITIONS.with(|cell| cell.replace(0)),
        PENDING_SETOP_MAX_DEPTH.with(|cell| cell.replace(0)),
        PENDING_SETOP_MERGE_PASSES.with(|cell| cell.replace(0)),
    )
}

pub(super) fn add_setop_spill_runtime(
    plan: &PhysicalPlan,
    file_opens: u64,
    repartitions: u64,
    max_depth: u64,
    merge_passes: u64,
) {
    if file_opens == 0 && repartitions == 0 && max_depth == 0 && merge_passes == 0 {
        return;
    }
    let Some(key) = plan_key(plan) else {
        return;
    };
    STATS_COLLECTOR.with(|cell| {
        if let Some(collector) = cell.borrow().as_ref().cloned() {
            if let Ok(mut map) = collector.stats.lock() {
                let entry = map.entry(key).or_default();
                entry.spill_file_opens = entry.spill_file_opens.saturating_add(file_opens);
                entry.spill_repartitions = entry.spill_repartitions.saturating_add(repartitions);
                entry.spill_max_depth = entry.spill_max_depth.max(max_depth);
                entry.spill_passes = entry.spill_passes.saturating_add(merge_passes);
            }
        }
    });
}

pub(super) fn take_pending_hash_join_spill() -> (u64, u64) {
    let bytes = PENDING_HASH_JOIN_SPILL_BYTES.with(|cell| cell.replace(0));
    let partitions = PENDING_HASH_JOIN_SPILL_PARTITIONS.with(|cell| cell.replace(0));
    (bytes, partitions)
}

pub(crate) fn add_pending_expression_rows(native_rows: u64, scalar_rows: u64) {
    if !is_recording() {
        return;
    }
    PENDING_NATIVE_EXPRESSION_ROWS.with(|cell| {
        cell.set(cell.get().saturating_add(native_rows));
    });
    PENDING_SCALAR_EXPRESSION_ROWS.with(|cell| {
        cell.set(cell.get().saturating_add(scalar_rows));
    });
}

pub(super) fn take_pending_expression_rows() -> (u64, u64) {
    let native = PENDING_NATIVE_EXPRESSION_ROWS.with(|cell| cell.replace(0));
    let scalar = PENDING_SCALAR_EXPRESSION_ROWS.with(|cell| cell.replace(0));
    (native, scalar)
}

pub(crate) fn add_pending_conversion_rows(rows: u64) {
    if rows > 0 && is_recording() {
        PENDING_CONVERSION_ROWS.with(|cell| {
            cell.set(cell.get().saturating_add(rows));
        });
    }
}

pub(super) fn take_pending_conversion_rows() -> u64 {
    PENDING_CONVERSION_ROWS.with(|cell| cell.replace(0))
}

pub(crate) fn add_pending_join_runtime(build_rows: u64, probe_rows: u64, match_rows: u64) {
    if !is_recording() {
        return;
    }
    PENDING_JOIN_BUILD_ROWS.with(|cell| cell.set(cell.get().saturating_add(build_rows)));
    PENDING_JOIN_PROBE_ROWS.with(|cell| cell.set(cell.get().saturating_add(probe_rows)));
    PENDING_JOIN_MATCH_ROWS.with(|cell| cell.set(cell.get().saturating_add(match_rows)));
}

pub(super) fn take_pending_join_runtime() -> (u64, u64, u64) {
    let build = PENDING_JOIN_BUILD_ROWS.with(|cell| cell.replace(0));
    let probe = PENDING_JOIN_PROBE_ROWS.with(|cell| cell.replace(0));
    let matches = PENDING_JOIN_MATCH_ROWS.with(|cell| cell.replace(0));
    (build, probe, matches)
}

pub(super) fn add_join_runtime_key(
    key: PlanNodeId,
    build_rows: u64,
    probe_rows: u64,
    match_rows: u64,
) {
    STATS_COLLECTOR.with(|cell| {
        if let Some(collector) = cell.borrow().as_ref().cloned() {
            if let Ok(mut map) = collector.stats.lock() {
                let entry = map.entry(key).or_default();
                entry.build_rows = entry.build_rows.saturating_add(build_rows);
                entry.probe_rows = entry.probe_rows.saturating_add(probe_rows);
                entry.match_rows = entry.match_rows.saturating_add(match_rows);
            }
        }
    });
}

pub(super) fn add_mrr_work(
    windows: u64,
    rids: u64,
    unique_pages: u64,
    workarea_flushes: u64,
    peak_bytes: u64,
) {
    let Some(key) = current_range_scan_plan_key() else {
        return;
    };
    STATS_COLLECTOR.with(|cell| {
        if let Some(collector) = cell.borrow().as_ref().cloned() {
            if let Ok(mut map) = collector.stats.lock() {
                let entry = map.entry(key).or_default();
                entry.mrr_windows = entry.mrr_windows.saturating_add(windows);
                entry.mrr_rids = entry.mrr_rids.saturating_add(rids);
                entry.mrr_unique_pages = entry.mrr_unique_pages.saturating_add(unique_pages);
                entry.mrr_page_reuses = entry
                    .mrr_page_reuses
                    .saturating_add(rids.saturating_sub(unique_pages));
                entry.mrr_workarea_flushes =
                    entry.mrr_workarea_flushes.saturating_add(workarea_flushes);
                entry.mrr_peak_bytes = entry.mrr_peak_bytes.max(peak_bytes);
            }
        }
    });
}

pub(crate) fn with_collector_handle_if_present<F, R>(collector: Option<CollectorHandle>, f: F) -> R
where
    F: FnOnce() -> R,
{
    if let Some(collector) = collector {
        with_collector_handle(collector, f)
    } else {
        f()
    }
}

/// Install a fresh collector for the duration of `f`, then return the
/// populated map together with `f`'s result. Nested invocations are not
/// expected here (subqueries route through `execute_select`, which
/// re-enters `execute_plan_rows`, but they share the same collector).
pub(super) fn with_collector<F, R>(f: F) -> (NodeStatsMap, Result<R, crate::error::QueryError>)
where
    F: FnOnce() -> Result<R, crate::error::QueryError>,
{
    let collector = match StatsCollector::try_new() {
        Ok(collector) => Arc::new(collector),
        Err(error) => return (NodeStatsMap::default(), Err(error)),
    };
    let prev = STATS_COLLECTOR.with(|cell| cell.borrow_mut().replace(collector.clone()));
    let restore = CollectorRestore(prev);
    let result = crate::storage::with_query_buffer_stats(collector.query_io.clone(), f);
    drop(restore);
    // Streaming cursors and joined workers may still hold a short-lived
    // collector handle after their last row. Requiring Arc uniqueness here
    // used to discard the *entire* trace in that case. Snapshot the completed
    // map under the mutex instead; lingering readers cannot mutate it once
    // their execution scope has ended.
    let map = collector.snapshot();
    (map, result)
}

pub(super) fn with_collector_handle<F, R>(handle: CollectorHandle, f: F) -> R
where
    F: FnOnce() -> R,
{
    let CollectorHandle {
        collector,
        operator_io,
    } = handle;
    let query_io = collector.query_io.clone();
    let prev = STATS_COLLECTOR.with(|cell| cell.borrow_mut().replace(collector));
    let _restore = CollectorRestore(prev);
    let result = crate::storage::with_query_buffer_stats(query_io, || {
        crate::storage::with_shared_operator_buffer_stats_if_present(operator_io, f)
    });
    result
}

/// Record one execution of the given plan node. Multiple calls accumulate
/// (loops += 1, time_ns += elapsed, rows += rows_produced) — this is what
/// produces the `loops=N` count for inner sides of nested-loop joins.
pub(super) fn record(plan: &PhysicalPlan, elapsed_ns: u128, rows_produced: u64, io: NodeIoStats) {
    if let Some(key) = plan_key(plan) {
        record_key(key, elapsed_ns, rows_produced, io);
    }
}

/// Record an operator by its stable plan-node ID. Pull-based streams are
/// required to be `'static`, so they retain the compact ID rather than a
/// reference to the display plan.
pub(super) fn record_key(key: PlanNodeId, elapsed_ns: u128, rows_produced: u64, io: NodeIoStats) {
    record_key_timing(key, elapsed_ns, elapsed_ns, 0, rows_produced, io, false);
}

/// Publish one real iterator loop. `first_row_ns` and `completion_ns` are
/// inclusive active durations for that loop; the collector stores totals and
/// the formatter derives MySQL's per-loop averages. `self_ns == 0` means the
/// executor could not split synchronous child work precisely.
pub(super) fn record_key_timing(
    key: PlanNodeId,
    first_row_ns: u128,
    completion_ns: u128,
    self_ns: u128,
    rows_produced: u64,
    io: NodeIoStats,
    io_is_self: bool,
) {
    STATS_COLLECTOR.with(|cell| {
        if let Some(collector) = cell.borrow().as_ref().cloned() {
            if let Ok(mut map) = collector.stats.lock() {
                let entry = map.entry(key).or_default();
                // A precompiled native input can remain deliberately unopened
                // when an enclosing exact fast path answers the query from
                // storage metadata (for example bounded-index RAND Top-1).
                // Its Drop wrapper publishes an all-zero observation. If the
                // fast path already recorded the same node, that observation
                // is not another execution loop and must not halve the
                // reported rows/time-per-loop.
                if completion_ns == 0
                    && rows_produced == 0
                    && io.cache_reads == 0
                    && io.physical_reads == 0
                    && entry.loops > 0
                {
                    return;
                }
                entry.first_row_ns = entry.first_row_ns.saturating_add(first_row_ns);
                entry.time_ns = entry.time_ns.saturating_add(completion_ns);
                entry.self_time_ns = entry.self_time_ns.saturating_add(self_ns);
                entry.rows = entry.rows.saturating_add(rows_produced);
                entry.loops = entry.loops.saturating_add(1);
                entry.source = StatsSource::Measured;
                entry.cache_reads = entry.cache_reads.saturating_add(io.cache_reads);
                entry.physical_reads = entry.physical_reads.saturating_add(io.physical_reads);
                entry.io_is_self |= io_is_self;
            }
        }
    });
}

pub(super) fn io_delta(before: (u64, u64), after: (u64, u64)) -> NodeIoStats {
    NodeIoStats {
        cache_reads: after.0.saturating_sub(before.0),
        physical_reads: after.1.saturating_sub(before.1),
    }
}

pub(super) fn record_with_io_snapshot(
    db: &crate::storage::Database,
    plan: &PhysicalPlan,
    elapsed_ns: u128,
    rows_produced: u64,
    io_start: (u64, u64),
) {
    let io_end = db.buffer_stats_counts();
    record(plan, elapsed_ns, rows_produced, io_delta(io_start, io_end));
}

/// Record a plan-root observation only when no lower execution path has
/// already recorded that exact operator.  This closes transparent fast-path
/// gaps without changing which path is selected or double-counting normal
/// execution.
pub(in crate::engine::executor) fn record_if_absent_with_io_snapshot(
    db: &crate::storage::Database,
    plan: &PhysicalPlan,
    time_ns: u128,
    rows: u64,
    io_start: (u64, u64),
) {
    let Some(key) = plan_key(plan) else {
        return;
    };
    let (cache_now, physical_now) = db.buffer_stats_counts();
    STATS_COLLECTOR.with(|cell| {
        if let Some(collector) = cell.borrow().as_ref().cloned() {
            if let Ok(mut map) = collector.stats.lock() {
                map.entry(key).or_insert_with(|| NodeStats {
                    source: StatsSource::Measured,
                    first_row_ns: time_ns,
                    time_ns,
                    rows,
                    loops: 1,
                    cache_reads: cache_now.saturating_sub(io_start.0),
                    physical_reads: physical_now.saturating_sub(io_start.1),
                    ..NodeStats::default()
                });
            }
        }
    });
}

pub(super) fn record_key_with_io_snapshot(
    db: &crate::storage::Database,
    key: PlanNodeId,
    elapsed_ns: u128,
    rows_produced: u64,
    io_start: (u64, u64),
) {
    let io_end = db.buffer_stats_counts();
    record_key(key, elapsed_ns, rows_produced, io_delta(io_start, io_end));
}

/// Return the number of completed execution scopes already published for
/// `key`. Stream adapters capture this before opening their inner operator so
/// they can tell whether that operator recorded the same execution itself.
pub(super) fn recorded_loops_key(key: PlanNodeId) -> u64 {
    STATS_COLLECTOR.with(|cell| {
        cell.borrow()
            .as_ref()
            .and_then(|collector| collector.stats.lock().ok())
            .and_then(|map| map.get(&key).map(|stats| stats.loops))
            .unwrap_or(0)
    })
}

/// Publish a stream-adapter observation only when the wrapped operator did
/// not publish a completed loop after the adapter was opened.
///
/// Many materialized operators record during stream construction, while
/// streaming joins record from their `Drop` implementation. The adapter is a
/// coverage fallback for operators without either hook; it must not become a
/// second owner of the same execution statistics.
/// Timing-aware stream-adapter fallback. Unlike the legacy snapshot helper,
/// this consumes I/O already accumulated only while the iterator was active,
/// so parent think-time and sibling worker traffic are not folded into it.
pub(super) fn record_key_if_loops_unchanged_timing(
    key: PlanNodeId,
    loops_before: u64,
    first_row_ns: u128,
    completion_ns: u128,
    rows_produced: u64,
    io: NodeIoStats,
) {
    STATS_COLLECTOR.with(|cell| {
        if let Some(collector) = cell.borrow().as_ref().cloned() {
            if let Ok(mut map) = collector.stats.lock() {
                let entry = map.entry(key).or_default();
                if entry.loops == loops_before {
                    entry.first_row_ns = entry.first_row_ns.saturating_add(first_row_ns);
                    entry.time_ns = entry.time_ns.saturating_add(completion_ns);
                    entry.rows = entry.rows.saturating_add(rows_produced);
                    entry.loops = entry.loops.saturating_add(1);
                    entry.source = StatsSource::Measured;
                    entry.cache_reads = entry.cache_reads.saturating_add(io.cache_reads);
                    entry.physical_reads = entry.physical_reads.saturating_add(io.physical_reads);
                    entry.io_is_self = true;
                }
            }
        }
    });
}

/// Bump the peak-memory water mark for a plan node. Multiple operators may
/// charge bytes against the same node across the query lifetime; we keep the
/// max across all calls so EXPLAIN ANALYZE reports the worst-case footprint.
pub(crate) fn add_peak_memory(plan: &PhysicalPlan, bytes: u64) {
    if bytes == 0 {
        return;
    }
    let Some(key) = plan_key(plan) else {
        return;
    };
    STATS_COLLECTOR.with(|cell| {
        if let Some(collector) = cell.borrow().as_ref().cloned() {
            if let Ok(mut map) = collector.stats.lock() {
                let entry = map.entry(key).or_default();
                if bytes > entry.peak_memory_bytes {
                    entry.peak_memory_bytes = bytes;
                }
            }
        }
    });
}

/// Accumulate `bytes` spilled by a plan node.
pub(crate) fn add_spill_bytes(plan: &PhysicalPlan, bytes: u64) {
    if bytes == 0 {
        return;
    }
    let Some(key) = plan_key(plan) else {
        return;
    };
    STATS_COLLECTOR.with(|cell| {
        if let Some(collector) = cell.borrow().as_ref().cloned() {
            if let Ok(mut map) = collector.stats.lock() {
                let entry = map.entry(key).or_default();
                entry.spill_bytes = entry.spill_bytes.saturating_add(bytes);
                entry.spill_reason.get_or_insert("memory_limit");
            }
        }
    });
}

pub(super) fn add_spill_bytes_read_key(key: PlanNodeId, bytes: u64) {
    if bytes == 0 {
        return;
    }
    STATS_COLLECTOR.with(|cell| {
        if let Some(collector) = cell.borrow().as_ref().cloned() {
            if let Ok(mut map) = collector.stats.lock() {
                let entry = map.entry(key).or_default();
                entry.spill_bytes_read = entry.spill_bytes_read.saturating_add(bytes);
            }
        }
    });
}

/// Record external-sort run shape for EXPLAIN ANALYZE.
pub(crate) fn add_sort_runs(plan: &PhysicalPlan, runs: u64, merge_passes: u64) {
    if runs == 0 && merge_passes == 0 {
        return;
    }
    crate::engine::status_counters::record_sort_runs(runs, merge_passes);
    let Some(key) = plan_key(plan) else {
        return;
    };
    STATS_COLLECTOR.with(|cell| {
        if let Some(collector) = cell.borrow().as_ref().cloned() {
            if let Ok(mut map) = collector.stats.lock() {
                let entry = map.entry(key).or_default();
                entry.sort_runs = entry.sort_runs.saturating_add(runs);
                entry.sort_merge_passes = entry.sort_merge_passes.saturating_add(merge_passes);
                entry.spill_runs = entry.spill_runs.saturating_add(runs);
                entry.spill_passes = entry.spill_passes.saturating_add(merge_passes);
                entry.spill_reason.get_or_insert("memory_limit");
            }
        }
    });
}

pub(super) fn add_spill_shape(
    plan: &PhysicalPlan,
    runs: u64,
    passes: u64,
    partitions: u64,
    reason: &'static str,
) {
    let Some(key) = plan_key(plan) else {
        return;
    };
    STATS_COLLECTOR.with(|cell| {
        if let Some(collector) = cell.borrow().as_ref().cloned() {
            if let Ok(mut map) = collector.stats.lock() {
                let entry = map.entry(key).or_default();
                entry.spill_runs = entry.spill_runs.saturating_add(runs);
                entry.spill_passes = entry.spill_passes.saturating_add(passes);
                entry.spill_partitions = entry.spill_partitions.max(partitions);
                entry.spill_reason.get_or_insert(reason);
            }
        }
    });
}

/// Accumulate physical partition visits observed while a scan node ran.
pub(super) fn add_partitions_key(key: PlanNodeId, partitions: u64) {
    if partitions == 0 {
        return;
    }
    STATS_COLLECTOR.with(|cell| {
        if let Some(collector) = cell.borrow().as_ref().cloned() {
            if let Ok(mut map) = collector.stats.lock() {
                let entry = map.entry(key).or_default();
                entry.partitions = entry.partitions.saturating_add(partitions);
            }
        }
    });
}

#[derive(Debug, Default, Clone, Copy)]
pub(super) struct ZoneMapRuntimeStats {
    pub metadata_ns: u64,
    pub candidate_ns: u64,
    pub heap_scan_ns: u64,
    pub tasks: u64,
    pub pages_scanned: u64,
    pub pages_skipped: u64,
    pub fallbacks: u64,
    pub workers: u64,
}

pub(super) fn add_zone_map_runtime_key(key: PlanNodeId, stats: ZoneMapRuntimeStats) {
    STATS_COLLECTOR.with(|cell| {
        if let Some(collector) = cell.borrow().as_ref().cloned() {
            if let Ok(mut map) = collector.stats.lock() {
                let entry = map.entry(key).or_default();
                entry.zone_metadata_ns = entry.zone_metadata_ns.saturating_add(stats.metadata_ns);
                entry.zone_candidate_ns =
                    entry.zone_candidate_ns.saturating_add(stats.candidate_ns);
                entry.zone_heap_scan_ns =
                    entry.zone_heap_scan_ns.saturating_add(stats.heap_scan_ns);
                entry.zone_tasks = entry.zone_tasks.saturating_add(stats.tasks);
                entry.zone_pages_scanned =
                    entry.zone_pages_scanned.saturating_add(stats.pages_scanned);
                entry.zone_pages_skipped =
                    entry.zone_pages_skipped.saturating_add(stats.pages_skipped);
                entry.zone_fallbacks = entry.zone_fallbacks.saturating_add(stats.fallbacks);
                entry.zone_workers = entry.zone_workers.saturating_add(stats.workers);
            }
        }
    });
}

/// Mark a plan node as having executed through a true streaming path.
/// Idempotent — once set, additional calls keep the flag `true`.
pub(super) fn set_streaming(plan: &PhysicalPlan, streaming: bool) {
    let Some(key) = plan_key(plan) else {
        return;
    };
    STATS_COLLECTOR.with(|cell| {
        if let Some(collector) = cell.borrow().as_ref().cloned() {
            if let Ok(mut map) = collector.stats.lock() {
                let entry = map.entry(key).or_default();
                if streaming {
                    entry.is_streaming = true;
                }
            }
        }
    });
}

/// Attach vector execution counters to a node. Calls accumulate batch and
/// conversion work while worker count records the widest parallel phase.
pub(super) fn add_vector_runtime(
    plan: &PhysicalPlan,
    batches: u64,
    conversion_rows: u64,
    workers: u32,
) {
    let Some(key) = plan_key(plan) else {
        return;
    };
    STATS_COLLECTOR.with(|cell| {
        if let Some(collector) = cell.borrow().as_ref().cloned() {
            if let Ok(mut map) = collector.stats.lock() {
                let entry = map.entry(key).or_default();
                entry.is_vectorized = true;
                entry.vector_requested_mode =
                    crate::engine::executor::vec::config::effective_mode().into();
                entry.vector_fallback_reason = VectorFallbackReason::None;
                entry.batches = entry.batches.saturating_add(batches);
                entry.conversion_rows = entry.conversion_rows.saturating_add(conversion_rows);
                entry.workers = entry.workers.max(workers.max(1));
            }
        }
    });
}

pub(super) fn add_expression_runtime(
    plan: &PhysicalPlan,
    mode: &'static str,
    native_rows: u64,
    fallback_rows: u64,
) {
    let Some(key) = plan_key(plan) else {
        return;
    };
    STATS_COLLECTOR.with(|cell| {
        if let Some(collector) = cell.borrow().as_ref().cloned() {
            if let Ok(mut map) = collector.stats.lock() {
                let entry = map.entry(key).or_default();
                entry.expression_mode = Some(mode);
                entry.native_expression_rows =
                    entry.native_expression_rows.saturating_add(native_rows);
                entry.fallback_expression_rows =
                    entry.fallback_expression_rows.saturating_add(fallback_rows);
            }
        }
    });
}

pub(super) fn add_aggregate_runtime(
    plan: &PhysicalPlan,
    mode: &'static str,
    native_rows: u64,
    fallback_rows: u64,
    distinct_spill_bytes: u64,
) {
    let Some(key) = plan_key(plan) else {
        return;
    };
    STATS_COLLECTOR.with(|cell| {
        if let Some(collector) = cell.borrow().as_ref().cloned() {
            if let Ok(mut map) = collector.stats.lock() {
                let entry = map.entry(key).or_default();
                entry.aggregate_mode = Some(mode);
                entry.native_aggregate_rows =
                    entry.native_aggregate_rows.saturating_add(native_rows);
                entry.fallback_aggregate_rows =
                    entry.fallback_aggregate_rows.saturating_add(fallback_rows);
                entry.distinct_spill_bytes = entry
                    .distinct_spill_bytes
                    .saturating_add(distinct_spill_bytes);
            }
        }
    });
}

pub(super) fn add_window_runtime(
    plan: &PhysicalPlan,
    mode: &'static str,
    spill_bytes: u64,
    peak_partition_rows: u64,
) {
    let Some(key) = plan_key(plan) else {
        return;
    };
    STATS_COLLECTOR.with(|cell| {
        if let Some(collector) = cell.borrow().as_ref().cloned() {
            if let Ok(mut map) = collector.stats.lock() {
                let entry = map.entry(key).or_default();
                entry.window_mode = Some(mode);
                entry.window_spill_bytes = entry.window_spill_bytes.saturating_add(spill_bytes);
                entry.window_peak_partition_rows =
                    entry.window_peak_partition_rows.max(peak_partition_rows);
            }
        }
    });
}

/// Record the no-I/O capability/cost decision made before a vector subtree is
/// opened. A later successful vector execution overwrites the fallback reason.
pub(super) fn set_vector_fallback(plan: &PhysicalPlan, reason: VectorFallbackReason) {
    let Some(key) = plan_key(plan) else {
        return;
    };
    STATS_COLLECTOR.with(|cell| {
        if let Some(collector) = cell.borrow().as_ref().cloned() {
            if let Ok(mut map) = collector.stats.lock() {
                let entry = map.entry(key).or_default();
                entry.vector_requested_mode =
                    crate::engine::executor::vec::config::effective_mode().into();
                if !entry.is_vectorized
                    && entry.vector_fallback_reason == VectorFallbackReason::None
                {
                    entry.vector_fallback_reason = reason;
                }
            }
        }
    });
}

/// Mirror already-recorded stats from one plan node to another while the
/// collector is active. Useful when execution ran against a cloned plan tree
/// (for example after runtime-filter injection) but EXPLAIN formatting still
/// walks the original tree.
pub(super) fn mirror(from: &PhysicalPlan, to: &PhysicalPlan) {
    if let (Some(from_key), Some(to_key)) = (plan_key(from), plan_key(to)) {
        mirror_key(from_key, to_key);
    }
}

/// Mirror stats from an executed cloned node to a captured display-plan key.
///
/// Streaming join pipelines own cloned plan fragments (and may move their
/// root enum values while opening the stream), so callers cannot always keep
/// a `&PhysicalPlan` for the display node alive here.  The display tree itself
/// remains alive for the whole EXPLAIN ANALYZE call, making its captured raw
/// key just as stable as the key used by [`record_key`].
pub(super) fn mirror_key(from_key: PlanNodeId, to_key: PlanNodeId) {
    if from_key == to_key {
        return;
    }
    STATS_COLLECTOR.with(|cell| {
        if let Some(collector) = cell.borrow().as_ref().cloned() {
            if let Ok(mut map) = collector.stats.lock() {
                if let Some(stats) = map.get(&from_key).copied() {
                    let entry = map.entry(to_key).or_default();
                    entry.source = StatsSource::Mirrored;
                    entry.first_row_ns = entry.first_row_ns.saturating_add(stats.first_row_ns);
                    entry.time_ns = entry.time_ns.saturating_add(stats.time_ns);
                    entry.self_time_ns = entry.self_time_ns.saturating_add(stats.self_time_ns);
                    entry.rows = entry.rows.saturating_add(stats.rows);
                    entry.loops = entry.loops.saturating_add(stats.loops);
                    entry.cache_reads = entry.cache_reads.saturating_add(stats.cache_reads);
                    entry.physical_reads =
                        entry.physical_reads.saturating_add(stats.physical_reads);
                    entry.io_is_self |= stats.io_is_self;
                    entry.runtime_filter_rejected = entry
                        .runtime_filter_rejected
                        .saturating_add(stats.runtime_filter_rejected);
                    entry.build_rows = entry.build_rows.saturating_add(stats.build_rows);
                    entry.probe_rows = entry.probe_rows.saturating_add(stats.probe_rows);
                    entry.match_rows = entry.match_rows.saturating_add(stats.match_rows);
                    entry.index_entries_examined = entry
                        .index_entries_examined
                        .saturating_add(stats.index_entries_examined);
                    entry.index_merge_branches =
                        entry.index_merge_branches.max(stats.index_merge_branches);
                    entry.index_merge_candidate_rowids = entry
                        .index_merge_candidate_rowids
                        .saturating_add(stats.index_merge_candidate_rowids);
                    entry.index_merge_duplicates_removed = entry
                        .index_merge_duplicates_removed
                        .saturating_add(stats.index_merge_duplicates_removed);
                    entry.heap_rows_fetched = entry
                        .heap_rows_fetched
                        .saturating_add(stats.heap_rows_fetched);
                    entry.heap_page_probes = entry
                        .heap_page_probes
                        .saturating_add(stats.heap_page_probes);
                    entry.mrr_windows = entry.mrr_windows.saturating_add(stats.mrr_windows);
                    entry.mrr_rids = entry.mrr_rids.saturating_add(stats.mrr_rids);
                    entry.mrr_unique_pages = entry
                        .mrr_unique_pages
                        .saturating_add(stats.mrr_unique_pages);
                    entry.mrr_page_reuses =
                        entry.mrr_page_reuses.saturating_add(stats.mrr_page_reuses);
                    entry.mrr_workarea_flushes = entry
                        .mrr_workarea_flushes
                        .saturating_add(stats.mrr_workarea_flushes);
                    entry.mrr_peak_bytes = entry.mrr_peak_bytes.max(stats.mrr_peak_bytes);
                    entry.row_goal = entry.row_goal.max(stats.row_goal);
                    entry.candidate_batches = entry
                        .candidate_batches
                        .saturating_add(stats.candidate_batches);
                    entry.candidate_window_peak =
                        entry.candidate_window_peak.max(stats.candidate_window_peak);
                    if stats.peak_memory_bytes > entry.peak_memory_bytes {
                        entry.peak_memory_bytes = stats.peak_memory_bytes;
                    }
                    entry.spill_bytes = entry.spill_bytes.saturating_add(stats.spill_bytes);
                    entry.spill_bytes_read = entry
                        .spill_bytes_read
                        .saturating_add(stats.spill_bytes_read);
                    entry.spill_runs = entry.spill_runs.saturating_add(stats.spill_runs);
                    entry.spill_passes = entry.spill_passes.saturating_add(stats.spill_passes);
                    entry.spill_partitions = entry.spill_partitions.max(stats.spill_partitions);
                    entry.spill_file_opens = entry
                        .spill_file_opens
                        .saturating_add(stats.spill_file_opens);
                    entry.spill_repartitions = entry
                        .spill_repartitions
                        .saturating_add(stats.spill_repartitions);
                    entry.spill_max_depth = entry.spill_max_depth.max(stats.spill_max_depth);
                    entry.spill_input_rows = entry
                        .spill_input_rows
                        .saturating_add(stats.spill_input_rows);
                    entry.spill_partial_rows = entry
                        .spill_partial_rows
                        .saturating_add(stats.spill_partial_rows);
                    if entry.spill_reason.is_none() {
                        entry.spill_reason = stats.spill_reason;
                    }
                    entry.sort_runs = entry.sort_runs.saturating_add(stats.sort_runs);
                    entry.sort_merge_passes = entry
                        .sort_merge_passes
                        .saturating_add(stats.sort_merge_passes);
                    entry.partitions = entry.partitions.saturating_add(stats.partitions);
                    entry.zone_metadata_ns = entry
                        .zone_metadata_ns
                        .saturating_add(stats.zone_metadata_ns);
                    entry.zone_candidate_ns = entry
                        .zone_candidate_ns
                        .saturating_add(stats.zone_candidate_ns);
                    entry.zone_heap_scan_ns = entry
                        .zone_heap_scan_ns
                        .saturating_add(stats.zone_heap_scan_ns);
                    entry.zone_tasks = entry.zone_tasks.saturating_add(stats.zone_tasks);
                    entry.zone_pages_scanned = entry
                        .zone_pages_scanned
                        .saturating_add(stats.zone_pages_scanned);
                    entry.zone_pages_skipped = entry
                        .zone_pages_skipped
                        .saturating_add(stats.zone_pages_skipped);
                    entry.zone_fallbacks =
                        entry.zone_fallbacks.saturating_add(stats.zone_fallbacks);
                    entry.zone_workers = entry.zone_workers.saturating_add(stats.zone_workers);
                    if stats.is_streaming {
                        entry.is_streaming = true;
                    }
                    if stats.is_vectorized {
                        entry.is_vectorized = true;
                    }
                    if stats.vector_requested_mode != VectorRequestedMode::Unknown {
                        entry.vector_requested_mode = stats.vector_requested_mode;
                    }
                    if stats.vector_fallback_reason != VectorFallbackReason::None {
                        entry.vector_fallback_reason = stats.vector_fallback_reason;
                    }
                    entry.batches = entry.batches.saturating_add(stats.batches);
                    entry.conversion_rows =
                        entry.conversion_rows.saturating_add(stats.conversion_rows);
                    if stats.expression_mode.is_some() {
                        entry.expression_mode = stats.expression_mode;
                    }
                    entry.native_expression_rows = entry
                        .native_expression_rows
                        .saturating_add(stats.native_expression_rows);
                    entry.fallback_expression_rows = entry
                        .fallback_expression_rows
                        .saturating_add(stats.fallback_expression_rows);
                    if stats.aggregate_mode.is_some() {
                        entry.aggregate_mode = stats.aggregate_mode;
                    }
                    entry.native_aggregate_rows = entry
                        .native_aggregate_rows
                        .saturating_add(stats.native_aggregate_rows);
                    entry.fallback_aggregate_rows = entry
                        .fallback_aggregate_rows
                        .saturating_add(stats.fallback_aggregate_rows);
                    entry.distinct_spill_bytes = entry
                        .distinct_spill_bytes
                        .saturating_add(stats.distinct_spill_bytes);
                    if stats.window_mode.is_some() {
                        entry.window_mode = stats.window_mode;
                    }
                    entry.window_spill_bytes = entry
                        .window_spill_bytes
                        .saturating_add(stats.window_spill_bytes);
                    entry.window_peak_partition_rows = entry
                        .window_peak_partition_rows
                        .max(stats.window_peak_partition_rows);
                    entry.workers = entry.workers.max(stats.workers);
                }
            }
        }
    });
}

/// Declare that a visible child was fused into a measured owner. Runtime
/// values are deliberately not copied: doing so made estimates appear to have
/// exact per-node observations when only the enclosing kernel was measured.
pub(super) fn mirror_key_if_absent(from_key: PlanNodeId, to_key: PlanNodeId) {
    if from_key == to_key {
        return;
    }
    STATS_COLLECTOR.with(|cell| {
        if let Some(collector) = cell.borrow().as_ref().cloned() {
            if let Ok(mut map) = collector.stats.lock() {
                if !map.contains_key(&to_key) {
                    if map.get(&from_key).is_some_and(|stats| stats.loops > 0) {
                        map.insert(
                            to_key,
                            NodeStats {
                                source: StatsSource::Included,
                                included_in: Some(from_key),
                                ..NodeStats::default()
                            },
                        );
                    }
                }
            }
        }
    });
}

/// Look up stats for a plan node through its execution-overlay binding.
pub(super) fn lookup(stats: &NodeStatsMap, plan: &PhysicalPlan) -> Option<NodeStats> {
    stats.get_for_plan(plan).copied()
}

fn format_per_loop(value: f64) -> String {
    if value.is_finite() && (value - value.round()).abs() < 0.000_5 {
        format!("{:.0}", value)
    } else {
        let rendered = format!("{value:.3}");
        rendered
            .trim_end_matches('0')
            .trim_end_matches('.')
            .to_string()
    }
}

/// Format MySQL-compatible iterator measurements. Times and rows are averages
/// per loop; cumulative values remain available in JSON. A fused display node
/// never inherits its owner's values.
pub(super) fn format_tree_suffix(stats: Option<NodeStats>) -> String {
    match stats {
        None => String::from(" (never executed)"),
        Some(s) if s.source == StatsSource::Included => match s.included_in {
            Some(owner) => format!(" (included in operator #{})", owner.0),
            None => String::from(" (included in fused operator)"),
        },
        Some(s) if s.loops == 0 => String::from(" (never executed)"),
        Some(s) => {
            let loops = s.loops as f64;
            let completion_ms = s.time_ns as f64 / loops / 1_000_000.0;
            let first_total = if s.first_row_ns == 0 {
                s.time_ns
            } else {
                s.first_row_ns
            };
            let first_ms = first_total as f64 / loops / 1_000_000.0;
            let rows_per_loop = s.rows as f64 / loops;
            let total_reads = s.cache_reads.saturating_add(s.physical_reads);
            let mut suffix = format!(
                " (actual time={first_ms:.3}..{completion_ms:.3} rows={} loops={}",
                format_per_loop(rows_per_loop),
                s.loops,
            );
            if s.source != StatsSource::Measured {
                suffix.push_str(&format!(" stats_source={}", s.source.as_str()));
            }
            if total_reads > 0 {
                let hit_rate = (s.cache_reads as f64 * 100.0) / total_reads as f64;
                if s.physical_reads > 0 {
                    suffix.push_str(&format!(" physical_reads={}", s.physical_reads));
                }
                if s.cache_reads > 0 {
                    suffix.push_str(&format!(" cache_reads={}", s.cache_reads));
                }
                let io_scope = if s.io_is_self {
                    "operator_self"
                } else {
                    "operator_interval_inclusive"
                };
                suffix.push_str(&format!(
                    " cache_hit_rate={hit_rate:.2}% io_scope={io_scope}"
                ));
            }
            if s.index_entries_examined > 0 {
                suffix.push_str(&format!(
                    " index_entries_examined={}",
                    s.index_entries_examined
                ));
            }
            if s.index_merge_branches > 0 {
                suffix.push_str(&format!(
                    " index_merge_branches={} candidate_rowids={} duplicates_removed={}",
                    s.index_merge_branches,
                    s.index_merge_candidate_rowids,
                    s.index_merge_duplicates_removed
                ));
            }
            if s.heap_rows_fetched > 0 {
                suffix.push_str(&format!(" heap_rows_fetched={}", s.heap_rows_fetched));
            }
            if s.heap_page_probes > 0 {
                suffix.push_str(&format!(" heap_page_probes={}", s.heap_page_probes));
            }
            if s.runtime_filter_rejected > 0 {
                suffix.push_str(&format!(
                    " runtime_filter_rejected={}",
                    s.runtime_filter_rejected
                ));
            }
            if s.build_rows > 0 {
                suffix.push_str(&format!(" build_rows={}", s.build_rows));
            }
            if s.probe_rows > 0 {
                suffix.push_str(&format!(" probe_rows={}", s.probe_rows));
            }
            if s.match_rows > 0 {
                suffix.push_str(&format!(" match_rows={}", s.match_rows));
            }
            for (name, value) in [
                ("mrr_windows", s.mrr_windows),
                ("mrr_rids", s.mrr_rids),
                ("mrr_unique_pages", s.mrr_unique_pages),
                ("mrr_page_reuses", s.mrr_page_reuses),
                ("mrr_workarea_flushes", s.mrr_workarea_flushes),
                ("mrr_peak_bytes", s.mrr_peak_bytes),
                ("row_goal", s.row_goal),
                ("candidate_batches", s.candidate_batches),
                ("candidate_window_peak", s.candidate_window_peak),
            ] {
                if value > 0 {
                    suffix.push_str(&format!(" {name}={value}"));
                }
            }
            if s.peak_memory_bytes > 0 {
                suffix.push_str(&format!(" peak_mem={}", s.peak_memory_bytes));
            }
            if s.spill_bytes > 0 {
                suffix.push_str(&format!(" spill_bytes={}", s.spill_bytes));
            }
            if s.spill_bytes_read > 0 {
                suffix.push_str(&format!(" spill_bytes_read={}", s.spill_bytes_read));
            }
            if s.sort_runs > 0 {
                suffix.push_str(&format!(" sort_runs={}", s.sort_runs));
            }
            if s.sort_merge_passes > 0 {
                suffix.push_str(&format!(" sort_merge_passes={}", s.sort_merge_passes));
            }
            if s.spill_runs > 0 {
                suffix.push_str(&format!(" spill_runs={}", s.spill_runs));
            }
            if s.spill_passes > 0 {
                suffix.push_str(&format!(" spill_passes={}", s.spill_passes));
            }
            if s.spill_partitions > 0 {
                suffix.push_str(&format!(" spill_partitions={}", s.spill_partitions));
            }
            if s.spill_file_opens > 0 {
                suffix.push_str(&format!(" spill_file_opens={}", s.spill_file_opens));
            }
            if s.spill_repartitions > 0 {
                suffix.push_str(&format!(" spill_repartitions={}", s.spill_repartitions));
            }
            if s.spill_max_depth > 0 {
                suffix.push_str(&format!(" spill_max_depth={}", s.spill_max_depth));
            }
            if let Some(reason) = s.spill_reason {
                suffix.push_str(&format!(" spill_reason={reason}"));
            }
            if s.spill_input_rows > 0 {
                suffix.push_str(&format!(" spill_input_rows={}", s.spill_input_rows));
            }
            if s.spill_partial_rows > 0 {
                suffix.push_str(&format!(" spill_partial_rows={}", s.spill_partial_rows));
            }
            if s.partitions > 0 {
                suffix.push_str(&format!(" partitions={}", s.partitions));
            }
            for (name, ns) in [
                ("zone_metadata_ms", s.zone_metadata_ns),
                ("zone_candidate_ms", s.zone_candidate_ns),
                ("zone_heap_ms", s.zone_heap_scan_ns),
            ] {
                if ns > 0 {
                    suffix.push_str(&format!(" {name}={:.3}", ns as f64 / 1_000_000.0));
                }
            }
            for (name, value) in [
                ("zone_tasks", s.zone_tasks),
                ("zone_pages", s.zone_pages_scanned),
                ("zone_skipped", s.zone_pages_skipped),
                ("zone_fallbacks", s.zone_fallbacks),
                ("zone_workers", s.zone_workers),
            ] {
                if value > 0 {
                    suffix.push_str(&format!(" {name}={value}"));
                }
            }
            if s.is_streaming {
                suffix.push_str(" streaming=true");
            }
            if !matches!(
                s.vector_requested_mode,
                VectorRequestedMode::Unknown | VectorRequestedMode::Auto
            ) {
                suffix.push_str(&format!(
                    " requested_mode={}",
                    s.vector_requested_mode.as_str()
                ));
            }
            if s.vector_fallback_reason != VectorFallbackReason::None {
                suffix.push_str(&format!(
                    " fallback_reason={}",
                    s.vector_fallback_reason.as_str()
                ));
            }
            if s.vector_requested_mode != VectorRequestedMode::Unknown && !s.is_vectorized {
                suffix.push_str(" execution_mode=row");
            }
            if s.is_vectorized {
                let scalar_rows = s
                    .fallback_expression_rows
                    .saturating_add(s.fallback_aggregate_rows);
                suffix.push_str(" execution_mode=vector");
                if s.batches > 0 {
                    suffix.push_str(&format!(" batches={}", s.batches));
                }
                if s.conversion_rows > 0 {
                    suffix.push_str(&format!(" conversion_rows={}", s.conversion_rows));
                }
                if scalar_rows > 0 {
                    suffix.push_str(&format!(" scalar_rows={scalar_rows}"));
                }
                if s.workers > 1 {
                    suffix.push_str(&format!(" workers={}", s.workers));
                }
            }
            if let Some(mode) = s.expression_mode {
                suffix.push_str(&format!(" expression_mode={mode}"));
                if s.native_expression_rows > 0 {
                    suffix.push_str(&format!(
                        " native_expression_rows={}",
                        s.native_expression_rows
                    ));
                }
                if s.fallback_expression_rows > 0 {
                    suffix.push_str(&format!(
                        " fallback_expression_rows={}",
                        s.fallback_expression_rows
                    ));
                }
            }
            if let Some(mode) = s.aggregate_mode {
                suffix.push_str(&format!(" aggregate_mode={mode}"));
                if s.native_aggregate_rows > 0 {
                    suffix.push_str(&format!(
                        " native_aggregate_rows={}",
                        s.native_aggregate_rows
                    ));
                }
                if s.fallback_aggregate_rows > 0 {
                    suffix.push_str(&format!(
                        " fallback_aggregate_rows={}",
                        s.fallback_aggregate_rows
                    ));
                }
                if s.distinct_spill_bytes > 0 {
                    suffix.push_str(&format!(" distinct_spill_bytes={}", s.distinct_spill_bytes));
                }
            }
            if let Some(mode) = s.window_mode {
                suffix.push_str(&format!(" window_mode={mode}"));
                if s.window_spill_bytes > 0 {
                    suffix.push_str(&format!(" window_spill_bytes={}", s.window_spill_bytes));
                }
                if s.window_peak_partition_rows > 0 {
                    suffix.push_str(&format!(
                        " window_peak_partition_rows={}",
                        s.window_peak_partition_rows
                    ));
                }
            }
            suffix.push(')');
            suffix
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tree_suffix_omits_default_runtime_attributes() {
        let suffix = format_tree_suffix(Some(NodeStats {
            source: StatsSource::Measured,
            first_row_ns: 500_000,
            time_ns: 2_500_000,
            rows: 3,
            loops: 1,
            ..NodeStats::default()
        }));

        assert_eq!(suffix, " (actual time=0.500..2.500 rows=3 loops=1)");
    }

    #[test]
    fn tree_suffix_keeps_non_default_runtime_attributes() {
        let suffix = format_tree_suffix(Some(NodeStats {
            source: StatsSource::Mirrored,
            first_row_ns: 2_000_000,
            time_ns: 8_000_000,
            rows: 6,
            loops: 2,
            physical_reads: 1,
            cache_reads: 3,
            peak_memory_bytes: 1024,
            spill_bytes: 256,
            vector_requested_mode: VectorRequestedMode::On,
            vector_fallback_reason: VectorFallbackReason::UnsupportedPlan,
            ..NodeStats::default()
        }));

        assert!(suffix.contains("actual time=1.000..4.000 rows=3 loops=2"));
        assert!(suffix.contains("stats_source=mirrored"));
        assert!(suffix.contains("physical_reads=1 cache_reads=3 cache_hit_rate=75.00%"));
        assert!(suffix.contains("peak_mem=1024"));
        assert!(suffix.contains("spill_bytes=256"));
        assert!(suffix.contains("fallback_reason=unsupported_plan"));
        assert!(suffix.contains("execution_mode=row"));
        assert!(!suffix.contains("peak_memory_bytes="));
        assert!(!suffix.contains("spill_bytes_written="));
        assert!(suffix.contains("io_scope=operator_interval_inclusive"));
    }

    #[test]
    fn tree_suffix_labels_self_io_and_omits_zero_io() {
        let stats = NodeStats {
            loops: 1,
            cache_reads: 4,
            io_is_self: true,
            ..NodeStats::default()
        };
        let suffix = format_tree_suffix(Some(stats));
        assert!(suffix.contains("cache_reads=4 cache_hit_rate=100.00% io_scope=operator_self"));
        let suffix = format_tree_suffix(Some(NodeStats {
            cache_reads: 0,
            ..stats
        }));
        assert!(!suffix.contains("io_scope=") && !suffix.contains("cache_hit_rate="));
    }

    #[test]
    fn tree_suffix_distinguishes_never_executed_and_fused_nodes() {
        assert_eq!(format_tree_suffix(None), " (never executed)");
        assert_eq!(
            format_tree_suffix(Some(NodeStats {
                source: StatsSource::Included,
                included_in: Some(PlanNodeId(7)),
                ..NodeStats::default()
            })),
            " (included in operator #7)"
        );
    }
}
