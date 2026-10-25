//! Lower a finalized `PhysicalPlan` into the common columnar
//! `BatchOperator` graph.
//!
//! Capability discovery happens before a source is opened. Expressions that
//! lack a typed kernel execute through a scalar lane inside the current batch;
//! they never consume input and restart the statement. A temporary `None`
//! outcome remains only for physical node families still being migrated and
//! is reported through the general fallback diagnostics.

use crate::engine::expr_program::expr_semantically_eq;
use std::{
    borrow::Cow,
    io::{Read, Write},
    sync::Arc,
};

use super::analyze_stats;
use super::plan::{ColMeta, ColMetaRef, PlanField, PlanRow};
use crate::engine::executor::mem_budget;
use crate::engine::executor::vec::{
    batch::{Batch, ColumnAccumulator, ColumnVector, Selection, DEFAULT_BATCH_ROWS},
    bitmap::Bitmap,
    distinct as vec_distinct, expr_batch, hash_agg as vec_hash_agg, hash_join as vec_hash_join,
    index_scan as vec_index_scan,
    schema::{BatchField, BatchSchema},
    sort as vec_sort,
    stream::{
        BatchOperator, BatchOrdering, BatchPartitioning, BatchProperties, BatchSortKey,
        MaterializedBatchOperator,
    },
    topn as vec_topn,
};
use crate::engine::expr_program::ExprProgram;
use crate::parser::{BinaryOp, Expr, Literal, OrderBy, SelectItem};
use crate::planner::{BuildSide, PhysicalPlan};
use crate::storage::{Database, TxnView};
use crate::types::{ColumnType, QueryError, Value};

mod grace_join;
pub(crate) use grace_join::*;
mod sources;
pub(super) use sources::*;
mod window;
use window::*;
mod fused_inlj;
#[cfg(test)]
pub(crate) use fused_inlj::FUSED_INLJ_BATCH_HITS;
mod hash_join;
pub(super) use hash_join::try_vec_hash_join;
#[cfg(test)]
pub(crate) use hash_join::HashJoinBatchOperator;
use hash_join::{try_build_vec_hash_join_input, try_build_vec_nested_loop_join_input};
mod topn;
#[cfg(test)]
pub(crate) use topn::EXACT_RAND_INDEX_TOP_ONE_HITS;
pub(super) use topn::*;
mod scan;
pub(crate) use scan::*;
mod scalar_aggregate;
use scalar_aggregate::*;
mod setop;
use setop::*;
mod aggregation;
pub(super) use aggregation::*;
mod compute;
use compute::*;

mod compact_window;
mod scan_batch;
mod scan_workers;
mod spooled_prefix_window;
use scan_batch::ScanBatchBuilder;
use scan_workers::ScanWorkers;

pub(crate) static COMPACT_WINDOW_BATCH_HITS: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);

/// Native storage-to-Batch Zone Map leaves that actually entered the
/// vectorized pipeline. Tests and status diagnostics use this to distinguish
/// real vector execution from a result-equivalent row fallback.
pub(crate) static ZONE_MAP_BATCH_SCAN_HITS: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);
pub(crate) static SETOP_BATCH_HITS: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);
pub(crate) static HIDDEN_FIRST_VALUE_TOPN_HITS: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);
pub(crate) static HIDDEN_FIRST_VALUE_FD_GROUP_ROWS: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);
/// Exact high-NDV aggregate Top-N kernels entered since process start.
/// Kept next to the other batch-routing counters so regression tests can
/// prove that a result did not merely fall back to the ordinary spilling
/// HashAggregate.
pub(crate) static DUPLICATE_AWARE_AGG_TOPN_HITS: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);
#[cfg(test)]
pub(crate) static DUPLICATE_AWARE_AGG_TOPN_SPILLS: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);
#[cfg(test)]
pub(crate) static DUPLICATE_AWARE_AGG_TOPN_GROWS: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);
#[cfg(test)]
pub(crate) static DUPLICATE_AWARE_AGG_TOPN_RESTART_SPILLS: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);
/// Exact stats-seeded COUNT Top-K proofs accepted since process start.
pub(crate) static STATS_PROVEN_COUNT_TOPK_HITS: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);
pub(crate) static STATS_PROVEN_COUNT_TOPK_FALLBACKS: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);
pub(crate) static AGG_COUNT_TOPK_PUSHDOWN_HITS: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);
pub(crate) static JOIN_AGG_MULTIPLICITY_ROWS_SAVED: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);

thread_local! {
    /// Statement-scoped reuse of the immutable plan/projection capability
    /// decision stored on `CompiledSelectPlan`. This is execution context, not
    /// a resident cache: nested statements restore the previous hint on drop.
    static BATCH_ROUTE_HINT: std::cell::Cell<Option<(usize, bool, Option<bool>)>> = const { std::cell::Cell::new(None) };
}

pub(crate) struct BatchRouteHintGuard(Option<(usize, bool, Option<bool>)>);

// Each wrapper explicitly selects its forwarding capabilities; expansion
// keeps the original virtual-call and property behavior.
macro_rules! batch_wrapper_method {
    (schema) => {
        fn schema(&self) -> &Arc<BatchSchema> {
            &self.schema
        }
    };
    (properties) => {
        fn properties(&self) -> BatchProperties {
            self.properties.clone()
        }
    };
    (install_runtime_filter) => {
        fn install_runtime_filter(&mut self, filter: crate::planner::RuntimeFilter) -> bool {
            self.upstream.install_runtime_filter(filter)
        }
    };
    (install_runtime_index_bound) => {
        fn install_runtime_index_bound(&mut self, predicate: &Expr) -> bool {
            self.upstream.install_runtime_index_bound(predicate)
        }
    };
    (prefer_key_locality) => {
        fn prefer_key_locality(&mut self, columns: &[usize]) -> bool {
            self.upstream.prefer_key_locality(columns)
        }
    };
}

impl Drop for BatchRouteHintGuard {
    fn drop(&mut self) {
        BATCH_ROUTE_HINT.with(|hint| hint.set(self.0));
    }
}

pub(crate) fn enter_batch_route_hint(
    plan: &PhysicalPlan,
    batch_route: bool,
    output_supported: Option<bool>,
) -> BatchRouteHintGuard {
    let key = plan as *const PhysicalPlan as usize;
    let previous =
        BATCH_ROUTE_HINT.with(|hint| hint.replace(Some((key, batch_route, output_supported))));
    BatchRouteHintGuard(previous)
}

/// Scalar/vector is a kernel choice inside the common BatchOperator graph.
/// Compatibility is decided by the compiler, never by cardinality or mode.
fn vectorize_enabled_for_plan(_plan: &PhysicalPlan) -> bool {
    true
}

/// CTE joins use the same batch HashJoin as base-table joins.
fn cte_join_enabled() -> bool {
    true
}

/// True when every scan leaf under `plan` is a `CteScan` — the whole join
/// subtree reads only materialised CTE references, with nothing but
/// projections / filters / CTE-to-CTE joins in between. A real-table leaf
/// (`SeqScan` / `IndexScan` / `GenerateSeries` / …) or any non-pass-through
/// operator (aggregate, sort, set-op, window) returns false. This is used only
/// to normalize the preserved side for legacy CTE metadata, not for routing.
fn plan_is_cte_only(plan: &PhysicalPlan) -> bool {
    match plan {
        PhysicalPlan::CteScan { .. } => true,
        PhysicalPlan::Projection { input, .. } | PhysicalPlan::Filter { input, .. } => {
            plan_is_cte_only(input)
        }
        PhysicalPlan::HashJoin { left, right, .. }
        | PhysicalPlan::NestedLoopJoin { left, right, .. } => {
            plan_is_cte_only(left) && plan_is_cte_only(right)
        }
        _ => false,
    }
}

/// The build side the vec hash join must use for a CTE-only join. The vec
/// preserved-probe kernel can only preserve the *probe* side, so an outer join
/// must build on its inner side (LEFT → build=right, RIGHT → build=left),
/// regardless of what the CBO chose: the CBO's build side is a cost decision,
/// and for a materialised CTE self-join driven by an under-estimate it is often
/// the *wrong* one (q78: CBO builds on the 489 k-row `ss` because it thinks it's
/// 18 rows). Forcing the inner side is always correct for an outer join and is
/// also the cheaper build here. INNER/SEMI/ANTI keep the CBO's choice (INNER is
/// symmetric; SEMI/ANTI are always planned build=right).
fn cte_vec_build_side(join_type: crate::parser::JoinType, cbo: BuildSide) -> BuildSide {
    use crate::parser::JoinType;
    match join_type {
        JoinType::Left => BuildSide::Right,
        JoinType::Right => BuildSide::Left,
        _ => cbo,
    }
}

/// Returns true if `expr` is a plain column reference (Identifier or
/// CompoundIdentifier whose last component is a column name). Used as
/// the static gate before we start the upstream stream — operators
/// that can't be translated should bail out **without** kicking off a
/// background scan worker, because that workers's I/O is wasted when
/// the legacy path then re-executes the same plan from scratch.
fn is_simple_column_ref(expr: &Expr) -> bool {
    matches!(expr, Expr::Identifier(_) | Expr::CompoundIdentifier(_))
}

// ---------------------------------------------------------------------
// Schema inference helpers
// ---------------------------------------------------------------------

const TEMPLATE_SCHEMA_CACHE_SLOTS: usize = 128;

struct TemplateSchemaCacheEntry {
    fingerprint: u64,
    metas: Box<[ColMetaRef]>,
    schema: Arc<BatchSchema>,
}

struct TemplateSchemaCache {
    slots: Box<[Option<TemplateSchemaCacheEntry>]>,
    _charge: crate::engine::executor::mem_budget::ControlledMemoryCharge,
}

impl TemplateSchemaCache {
    fn try_new() -> Option<Self> {
        let bytes = std::mem::size_of::<Self>().saturating_add(
            TEMPLATE_SCHEMA_CACHE_SLOTS
                .saturating_mul(std::mem::size_of::<Option<TemplateSchemaCacheEntry>>()),
        );
        let charge = crate::engine::executor::mem_budget::ControlledMemoryCharge::try_new_optional(
            crate::engine::executor::mem_budget::ControlledMemoryCategory::DerivedCache,
            bytes,
        )?;
        let slots = std::iter::repeat_with(|| None)
            .take(TEMPLATE_SCHEMA_CACHE_SLOTS)
            .collect::<Vec<_>>()
            .into_boxed_slice();
        Some(Self {
            slots,
            _charge: charge,
        })
    }

    fn get(&self, fingerprint: u64, template: &[PlanField]) -> Option<Arc<BatchSchema>> {
        let entry = self.slots[fingerprint as usize % self.slots.len()].as_ref()?;
        (entry.fingerprint == fingerprint
            && entry.metas.len() == template.len()
            && entry
                .metas
                .iter()
                .zip(template)
                .all(|(cached, field)| cached.ptr_eq(&field.meta)))
        .then(|| Arc::clone(&entry.schema))
    }

    fn insert(&mut self, entry: TemplateSchemaCacheEntry) {
        let slot = entry.fingerprint as usize % self.slots.len();
        self.slots[slot] = Some(entry);
    }
}

thread_local! {
    /// Stable plan templates repeatedly cross the row/batch boundary. Cache
    /// their immutable schema by exact interned metadata identity so a hit is
    /// allocation-free and never confuses a fingerprint collision. A direct-
    /// mapped bounded table avoids a mutex in short OLTP statements; optional
    /// DerivedCache admission makes memory pressure simply disable the cache.
    static TEMPLATE_SCHEMA_CACHE: std::cell::RefCell<Option<Box<TemplateSchemaCache>>> =
        std::cell::RefCell::new(TemplateSchemaCache::try_new().map(Box::new));
}

#[inline]
fn template_schema_fingerprint(template: &[PlanField]) -> u64 {
    let mut hash = 0xcbf2_9ce4_8422_2325_u64 ^ template.len() as u64;
    for field in template {
        let identity = field.meta.identity() as u64;
        hash ^= identity.wrapping_add(0x9e37_79b9_7f4a_7c15);
        hash = hash.wrapping_mul(0x1000_0000_01b3);
        hash ^= hash >> 32;
    }
    hash
}

fn build_batch_schema(template: &[PlanField]) -> BatchSchema {
    let fields: Vec<BatchField> = template
        .iter()
        .map(|f| BatchField {
            table: f.table.clone(),
            alias: f.alias.clone(),
            name: f.column.clone(),
            column_type: f.column_type,
            source: f.source.clone(),
        })
        .collect();
    BatchSchema::new(fields)
}

fn template_to_batch_schema(template: &[PlanField]) -> Arc<BatchSchema> {
    let fingerprint = template_schema_fingerprint(template);
    if let Some(schema) = TEMPLATE_SCHEMA_CACHE.with(|cache| {
        cache
            .borrow()
            .as_deref()
            .and_then(|cache| cache.get(fingerprint, template))
    }) {
        return schema;
    }
    if !TEMPLATE_SCHEMA_CACHE.with(|cache| cache.borrow().is_some()) {
        return Arc::new(build_batch_schema(template));
    }

    let mut schema = build_batch_schema(template);
    let input = Arc::new(expr_batch::batch_schema_to_input(&schema));
    let metas: Box<[ColMetaRef]> = template
        .iter()
        .map(|field| field.meta.clone())
        .collect::<Vec<_>>()
        .into_boxed_slice();
    let retained_owned_meta_bytes = metas.iter().fold(0usize, |bytes, meta| {
        bytes.saturating_add(match meta {
            ColMetaRef::Interned(_) => 0,
            ColMetaRef::Owned(meta) => std::mem::size_of::<ColMeta>()
                .saturating_add(meta.table.capacity())
                .saturating_add(meta.alias.as_ref().map_or(0, String::capacity))
                .saturating_add(meta.column.capacity())
                .saturating_add(meta.source.schema.capacity())
                .saturating_add(meta.source.table.capacity())
                .saturating_add(meta.source.org_table.capacity())
                .saturating_add(meta.source.org_name.capacity()),
        })
    });
    let resident_bytes = std::mem::size_of::<BatchSchema>()
        .saturating_add(schema.heap_capacity_bytes())
        .saturating_add(std::mem::size_of::<crate::engine::expr_program::InputSchema>())
        .saturating_add(input.heap_capacity_bytes())
        .saturating_add(
            metas
                .len()
                .saturating_mul(std::mem::size_of::<ColMetaRef>()),
        )
        .saturating_add(retained_owned_meta_bytes)
        .saturating_add(std::mem::size_of::<TemplateSchemaCacheEntry>());
    let Some(charge) =
        crate::engine::executor::mem_budget::ControlledMemoryCharge::try_new_optional(
            crate::engine::executor::mem_budget::ControlledMemoryCategory::DerivedCache,
            resident_bytes,
        )
    else {
        return Arc::new(schema);
    };
    schema.install_cached_input(input, charge);
    let schema = Arc::new(schema);
    TEMPLATE_SCHEMA_CACHE.with(|cache| {
        if let Some(cache) = cache.borrow_mut().as_deref_mut() {
            cache.insert(TemplateSchemaCacheEntry {
                fingerprint,
                metas,
                schema: Arc::clone(&schema),
            });
        }
    });
    schema
}

/// Convert a single vectorized output row (a `Vec<Value>` produced by a
/// vec operator that finalised) back into a `PlanRow` carrying the
/// template metadata.
pub(super) fn values_to_plan_row(values: Vec<Value>, template: &[PlanField]) -> PlanRow {
    let fields: Vec<PlanField> = template
        .iter()
        .zip(values.into_iter())
        .map(|(template, value)| PlanField {
            // The template already owns the exact interned catalog metadata.
            // Rebuilding an owned ColMeta here cloned table/column strings for
            // every cell before the intern lookup (millions of temporary
            // allocations for a Batch→INLJ boundary).
            meta: template.meta.clone(),
            value: match (template.column_type, value) {
                (ColumnType::Float, crate::types::Value::Double(value)) => {
                    crate::types::Value::Float(value as f32)
                }
                (_, value) => value,
            },
        })
        .collect();
    PlanRow {
        fields,
        ..Default::default()
    }
}

fn vec_hash_agg_group_fields(
    group_template: &[PlanField],
    cols: &[ColumnVector],
    row_idx: usize,
) -> Vec<PlanField> {
    let mut fields: Vec<PlanField> = group_template
        .iter()
        .enumerate()
        .map(|(i, t)| PlanField {
            meta: crate::engine::executor::select::plan::intern_col_meta(ColMeta {
                invisible: false,
                table: t.table.clone(),
                alias: t.alias.clone(),
                column: t.column.clone(),
                column_type: t.column_type,
                source: t.source.clone(),
            }),
            value: cols[i].typed_value_at(row_idx, t.column_type),
        })
        .collect();
    for (idx, col) in cols.iter().take(group_template.len()).enumerate() {
        fields.push(super::aggregate::group_marker_field(
            idx,
            col.typed_value_at(row_idx, group_template[idx].column_type),
        ));
    }
    fields
}

fn vec_hash_agg_group_fields_from_values(
    group_template: &[PlanField],
    row: &[Value],
) -> Vec<PlanField> {
    let mut fields: Vec<PlanField> = group_template
        .iter()
        .enumerate()
        .map(|(i, t)| PlanField {
            meta: crate::engine::executor::select::plan::intern_col_meta(ColMeta {
                invisible: false,
                table: t.table.clone(),
                alias: t.alias.clone(),
                column: t.column.clone(),
                column_type: t.column_type,
                source: t.source.clone(),
            }),
            value: row[i].clone(),
        })
        .collect();
    for (idx, value) in row.iter().take(group_template.len()).enumerate() {
        fields.push(super::aggregate::group_marker_field(idx, value.clone()));
    }
    fields
}

/// Bind metadata-only rows without materializing any input or cloning its AST.
fn plan_output_template(plan: &PhysicalPlan) -> Vec<PlanField> {
    let mut metas = Vec::new();
    super::plan::collect_plan_fields(plan, &mut metas);
    metas
        .into_iter()
        .map(|meta| PlanField {
            meta: super::plan::intern_col_meta(meta),
            value: Value::Null,
        })
        .collect()
}

/// Template → interned `&'static ColMeta`, once per join (the old
/// `values_to_plan_row` interned per row per column).
fn template_metas(template: &[PlanField]) -> Vec<super::plan::ColMetaRef> {
    template
        .iter()
        .map(|t| {
            crate::engine::executor::select::plan::intern_col_meta(ColMeta {
                invisible: false,
                table: t.table.clone(),
                alias: t.alias.clone(),
                column: t.column.clone(),
                column_type: t.column_type,
                source: t.source.clone(),
            })
        })
        .collect()
}

/// Columnar join output → row PlanRow (respecting the Selection mask). This is
/// the sole boundary conversion between the vec join and row consumers: one
/// `value_at` per cell, metas pre-interned.
fn batches_to_plan_rows(
    batches: Vec<Batch>,
    template: &[PlanField],
) -> Result<Vec<PlanRow>, QueryError> {
    materialize_join_batches(batches.into_iter().map(Ok), template)
}

fn materialize_join_batches(
    batches: impl Iterator<Item = Result<Batch, QueryError>>,
    template: &[PlanField],
) -> Result<Vec<PlanRow>, QueryError> {
    let metas = template_metas(template);
    let mut out = Vec::new();
    let mut charge = mem_budget::ScopedCharge::new(0);
    for batch in batches {
        let batch = batch?;
        crate::engine::cancel::check_cancelled()?;
        debug_assert_eq!(batch.columns().len(), metas.len());
        let mut emit = |r: usize| -> Result<(), QueryError> {
            let fields: Vec<PlanField> = metas
                .iter()
                .enumerate()
                .map(|(c, meta)| PlanField {
                    meta: meta.clone(),
                    value: batch.columns()[c].typed_value_at(r, metas[c].column_type),
                })
                .collect();
            let row = PlanRow {
                fields,
                ..Default::default()
            };
            charge.add(super::plan::materialized_plan_row_charge(&row))?;
            out.push(row);
            Ok(())
        };
        for row in batch.visible_indices() {
            emit(row)?;
        }
    }
    super::plan::commit_transient_materialization_charge(charge);
    Ok(out)
}

fn concat_templates(left: &[PlanField], right: &[PlanField]) -> Vec<PlanField> {
    let mut out = Vec::with_capacity(left.len() + right.len());
    out.extend(left.iter().cloned());
    out.extend(right.iter().cloned());
    out
}

// ---------------------------------------------------------------------
// Hash join
// ---------------------------------------------------------------------

fn value_row_slice_to_batches(schema: Arc<BatchSchema>, rows: &[Vec<Value>]) -> Vec<Batch> {
    let mut out = Vec::new();
    for chunk in rows.chunks(DEFAULT_BATCH_ROWS) {
        if chunk.is_empty() {
            continue;
        }
        let mut columns: Vec<Vec<Value>> = (0..schema.fields().len())
            .map(|_| Vec::with_capacity(chunk.len()))
            .collect();
        for row in chunk {
            for (column, value) in columns.iter_mut().zip(row) {
                column.push(value.clone());
            }
        }
        out.push(Batch::with_row_count(
            schema.clone(),
            columns.into_iter().map(ColumnVector::from_values).collect(),
            chunk.len(),
        ));
    }
    out
}

fn value_rows_to_batches(schema: Arc<BatchSchema>, rows: Vec<Vec<Value>>) -> Vec<Batch> {
    value_row_slice_to_batches(schema, &rows)
}

fn synthetic_source_template(relation: &str, columns: &[crate::types::Column]) -> Vec<PlanField> {
    columns
        .iter()
        .map(|column| PlanField {
            meta: super::plan::intern_col_meta(ColMeta {
                invisible: false,
                table: relation.to_string(),
                alias: None,
                column: column.name.clone(),
                column_type: column.column_type,
                source: column.source.clone(),
            }),
            value: Value::Null,
        })
        .collect()
}

/// Move owned rows into columnar batches without cloning variable-width
/// payloads. Blocking operators use this at their output boundary so the
/// reservation can follow the resulting batches instead of briefly covering
/// both a row copy and a column copy.
fn value_rows_into_batches(schema: Arc<BatchSchema>, rows: Vec<Vec<Value>>) -> Vec<Batch> {
    let width = schema.fields().len();
    let mut out = Vec::with_capacity(rows.len().div_ceil(DEFAULT_BATCH_ROWS));
    let mut rows = rows.into_iter();
    loop {
        let mut columns: Vec<Vec<Value>> = (0..width)
            .map(|_| Vec::with_capacity(DEFAULT_BATCH_ROWS))
            .collect();
        let mut row_count = 0usize;
        for _ in 0..DEFAULT_BATCH_ROWS {
            let Some(row) = rows.next() else {
                break;
            };
            debug_assert_eq!(row.len(), width);
            for (column, value) in columns.iter_mut().zip(row) {
                column.push(value);
            }
            row_count += 1;
        }
        if row_count == 0 {
            break;
        }
        out.push(Batch::with_row_count(
            schema.clone(),
            columns.into_iter().map(ColumnVector::from_values).collect(),
            row_count,
        ));
    }
    out
}

/// Compatibility lane for operators whose canonical kernel is still
/// row-shaped (FULLTEXT, spatial, vector, subquery-driven index IN and the
/// runtime candidate selector in AdaptiveJoin). The
/// node itself is nevertheless a leaf in the common BatchOperator graph: it
/// is constructed during preflight, opens only on the first pull after the
/// complete graph exists, and can never return a capability miss after I/O.
/// Values are moved into typed batches immediately; no upper operator sees a
/// `PlanRow` and no scan is restarted.
struct DeferredRowStorageBatchOperator<'a> {
    db: &'a Arc<Database>,
    txn: TxnView,
    plan: &'a PhysicalPlan,
    schema: Arc<BatchSchema>,
    input: Option<Box<dyn super::plan::PlanStream>>,
    field_mapping: Option<Vec<Option<usize>>>,
    finished: bool,
}

#[cfg(not(test))]
const PARALLEL_INDEX_IN_MIN_ROWS: f64 = 4096.0;
// Keep the integration regression small; production retains the 4K
// thread-start amortisation gate above.
#[cfg(test)]
const PARALLEL_INDEX_IN_MIN_ROWS: f64 = 256.0;

/// Observability and regression hook for the filtered native source.  This is
/// intentionally separate from ordinary parallel IndexIn executions so tests
/// can prove a residual predicate did not force the compatibility lane.
pub(crate) static PARALLEL_FILTERED_INDEX_IN_HITS: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);

/// Native non-covering `IndexInLookup` source.  The subquery and B-tree seeks
/// stay on the connection thread, while the much larger heap projection is
/// split into page-local chunks and decoded directly into bounded batches.
/// This avoids both the serial heap bottleneck and the intermediate
/// `Vec<PlanRow>` used by the compatibility lane.
struct ParallelIndexInBatchStream<'a> {
    db: Arc<Database>,
    txn: TxnView,
    pending_plan: Option<&'a PhysicalPlan>,
    schema: Arc<BatchSchema>,
    columns: Arc<Vec<usize>>,
    /// Compiled residual over `columns`.  Keeping this beside the native
    /// source lets filtered IndexIn scans retain their parallel heap path
    /// instead of falling back to serial `Vec<PlanRow>` materialisation.
    predicate: Option<Arc<super::plan::CompiledRowPredicate>>,
    execution: ScanWorkers,
}

fn projected_values_batch(
    schema: &Arc<BatchSchema>,
    rows: Vec<(crate::storage::RowId, Vec<Value>)>,
) -> Option<Batch> {
    if rows.is_empty() {
        return None;
    }
    let row_count = rows.len();
    let mut accumulators = schema
        .fields()
        .iter()
        .map(|field| ColumnAccumulator::for_field(field, row_count))
        .collect::<Vec<_>>();
    for (_, values) in rows {
        for (accumulator, value) in accumulators.iter_mut().zip(&values) {
            accumulator.push(value);
        }
    }
    Some(Batch::with_row_count(
        schema.clone(),
        accumulators
            .into_iter()
            .map(ColumnAccumulator::finish)
            .collect(),
        row_count,
    ))
}

impl ParallelIndexInBatchStream<'_> {
    fn ensure_started(&mut self) -> Result<(), QueryError> {
        let Some(plan) = self.pending_plan.take() else {
            return Ok(());
        };
        if self.predicate.is_some() {
            PARALLEL_FILTERED_INDEX_IN_HITS.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        }
        let PhysicalPlan::IndexInLookup {
            table,
            index,
            key_prefix,
            in_column,
            subquery,
            subquery_plan,
            ..
        } = plan
        else {
            unreachable!("parallel IndexIn source created for another plan node");
        };
        let (row_ids, row_ids_charge) = super::plan::execute_index_in_lookup_ids(
            &self.db,
            self.txn,
            &table.schema,
            index,
            key_prefix,
            in_column,
            subquery,
            subquery_plan,
        )?;

        let requested = crate::planner::default_parallel_degree().max(1) as usize;
        let task_count = row_ids.len().div_ceil(DEFAULT_BATCH_ROWS).max(1);
        let degree = requested.min(task_count);
        let rows_per_worker = row_ids.len().div_ceil(degree).max(1);
        // Keep the sorted RowId allocation single-owned and share immutable
        // ranges with workers.  Copying into one Vec per partition would
        // transiently double this query-sized allocation and make its
        // lifetime/accounting harder to follow under memory pressure.
        let row_ids_charge = Arc::new(row_ids_charge);
        let row_ids = Arc::new(row_ids);

        let (sender, receiver) = std::sync::mpsc::sync_channel(degree.saturating_mul(2).max(2));
        let table = table.clone();
        for worker in 0..degree {
            let start = worker.saturating_mul(rows_per_worker);
            let end = row_ids.len().min(start.saturating_add(rows_per_worker));
            if start == end {
                continue;
            }
            let sender = sender.clone();
            let context = self.execution.worker_context.clone();
            let db = Arc::clone(&self.db);
            let table = table.clone();
            let columns = Arc::clone(&self.columns);
            let schema = Arc::clone(&self.schema);
            let predicate = self.predicate.clone();
            let row_ids = Arc::clone(&row_ids);
            let row_ids_charge = Arc::clone(&row_ids_charge);
            self.execution.workers.push(std::thread::spawn(move || {
                let _row_ids_charge = row_ids_charge;
                context.run(&db, || {
                    let result = (|| -> Result<(), QueryError> {
                        let heap = db.cached_heap_handle(table.schema.id)?;
                        // Heap Value rows coexist with the converted Batch.
                        // Reserve before decoding and leave half this worker's
                        // buffer share for conversion and queued output.
                        let batch_rows = (scan_decode_batch_rows(&schema) / 2).max(1);
                        for ids in row_ids[start..end].chunks(batch_rows) {
                            context.cancellation().check()?;
                            let row_slots =
                                std::mem::size_of::<(crate::storage::RowId, Vec<Value>)>();
                            let predicted = ids.len().saturating_mul(row_slots.saturating_add(
                                columns.len().saturating_mul(std::mem::size_of::<Value>()),
                            ));
                            let mut rows_charge = mem_budget::force_charge_scoped(predicted)?;
                            let mut rows = db.read_row_columns_many_with_handle(
                                &heap,
                                &table.schema,
                                ids,
                                context.txn(),
                                &columns,
                            )?;
                            let actual = rows.iter().fold(
                                rows.capacity().saturating_mul(row_slots),
                                |bytes, (_, values)| {
                                    bytes
                                        .saturating_add(
                                            mem_budget::estimate_row_bytes(values)
                                                .saturating_sub(std::mem::size_of::<Vec<Value>>()),
                                        )
                                        .saturating_add(
                                            values
                                                .capacity()
                                                .saturating_sub(values.len())
                                                .saturating_mul(std::mem::size_of::<Value>()),
                                        )
                                },
                            );
                            if actual > predicted {
                                rows_charge.add_force(actual - predicted)?;
                            } else {
                                rows_charge.release_bytes(predicted - actual);
                            }
                            if let Some(predicate) = predicate.as_deref() {
                                let mut eval_error = None;
                                rows.retain(|(_, values)| {
                                    if eval_error.is_some() {
                                        return false;
                                    }
                                    match predicate.eval(values) {
                                        Ok(keep) => keep,
                                        Err(error) => {
                                            eval_error = Some(error);
                                            false
                                        }
                                    }
                                });
                                if let Some(error) = eval_error {
                                    return Err(error);
                                }
                            }
                            if let Some(batch) = projected_values_batch(&schema, rows) {
                                if !send_parallel_scan_message(&sender, &context, Ok(batch)) {
                                    return Ok(());
                                }
                            }
                        }
                        Ok(())
                    })();
                    if let Err(error) = result {
                        let error = context.terminal_error(error);
                        let _ = send_parallel_scan_message(&sender, &context, Err(error));
                    }
                });
            }));
        }
        drop(sender);
        self.execution.receiver = Some(receiver);
        Ok(())
    }
}

impl BatchOperator for ParallelIndexInBatchStream<'_> {
    batch_wrapper_method!(schema);

    fn properties(&self) -> BatchProperties {
        BatchProperties {
            ordering: BatchOrdering::Unordered,
            partitioning: BatchPartitioning::RoundRobin {
                partitions: crate::planner::default_parallel_degree().max(1) as usize,
            },
        }
    }

    fn next_batch(&mut self) -> Result<Option<Batch>, QueryError> {
        if self.execution.finished {
            return Ok(None);
        }
        self.ensure_started()?;
        self.execution.next_batch()
    }
}

fn try_build_parallel_index_in_batch<'a>(
    db: &'a Arc<Database>,
    txn: TxnView,
    plan: &'a PhysicalPlan,
) -> Result<Option<VecBatchInput<'a>>, QueryError> {
    let PhysicalPlan::IndexInLookup {
        table,
        filter,
        access_rows,
        covering,
        projected_columns,
        ..
    } = plan
    else {
        return Ok(None);
    };
    if *covering
        || table.schema.is_partitioned()
        // Heap work scales with index candidates, not post-filter rows.
        || *access_rows < PARALLEL_INDEX_IN_MIN_ROWS
        || crate::planner::default_parallel_degree() <= 1
    {
        return Ok(None);
    }
    let columns = projected_columns.clone().unwrap_or_else(|| {
        Arc::new(
            table
                .schema
                .iter_visible_columns()
                .map(|(index, _)| index)
                .collect(),
        )
    });
    let schema = build_scan_batch_schema(table, &columns);
    let template = schema.fields().iter().map(batch_field_template).collect();
    // Compile while still on the connection thread so auto/prepared
    // parameters are resolved once. Unsupported shapes (subqueries,
    // collation-sensitive forms, etc.) keep the established row evaluator
    // as the correctness fallback.
    let predicate = match filter.as_ref() {
        Some(expr) => {
            let projected_schema = super::plan::projected_schema_view(&table.schema, &columns);
            let Some(predicate) =
                super::plan::compile_projected_row_predicate(expr, &projected_schema)
            else {
                return Ok(None);
            };
            Some(Arc::new(predicate))
        }
        None => None,
    };
    Ok(Some(VecBatchInput {
        stream: Box::new(ParallelIndexInBatchStream {
            db: Arc::clone(db),
            txn,
            pending_plan: Some(plan),
            schema,
            columns,
            predicate,
            execution: ScanWorkers::new(
                super::execution_context::ExecutionContext::capture(txn).fork_worker_cancellation(),
                "parallel IndexIn heap worker panicked",
            ),
        }),
        template,
    }))
}

impl BatchOperator for DeferredRowStorageBatchOperator<'_> {
    batch_wrapper_method!(schema);

    fn properties(&self) -> BatchProperties {
        BatchProperties::UNORDERED_SINGLE
    }

    fn next_batch(&mut self) -> Result<Option<Batch>, QueryError> {
        if self.finished {
            return Ok(None);
        }
        if self.input.is_none() {
            self.input = Some(super::plan::execute_plan_stream(
                self.db, self.txn, self.plan,
            )?);
        }

        // Keep the compatibility lane pull-bounded for FULLTEXT. A large
        // decode batch would consume thousands of heap rows before a small
        // downstream LIMIT can stop the incremental posting source.
        let target = if matches!(self.plan, PhysicalPlan::FulltextScan { .. }) {
            scan_decode_batch_rows(&self.schema).min(super::plan::FULLTEXT_BATCH_ROWS)
        } else {
            scan_decode_batch_rows(&self.schema)
        };
        let mut accumulators: Vec<ColumnAccumulator> = self
            .schema
            .fields()
            .iter()
            .map(|field| ColumnAccumulator::for_field(field, target))
            .collect();
        let mut rows = 0usize;
        while rows < target {
            let Some(row) = self.input.as_mut().expect("row stream opened").next()? else {
                self.finished = true;
                break;
            };
            let mapping = self.field_mapping.get_or_insert_with(|| {
                deferred_row_field_mapping(self.schema.fields(), &row.fields)
            });
            for (accumulator, source) in accumulators.iter_mut().zip(mapping) {
                accumulator.push(
                    source
                        .and_then(|column| row.fields.get(column))
                        .map(|field| &field.value)
                        .unwrap_or(&Value::Null),
                );
            }
            rows += 1;
        }
        if rows == 0 {
            return Ok(None);
        }
        analyze_stats::add_pending_conversion_rows(rows as u64);
        Ok(Some(Batch::with_row_count(
            self.schema.clone(),
            accumulators
                .into_iter()
                .map(ColumnAccumulator::finish)
                .collect(),
            rows,
        )))
    }
}

/// Map each planned batch column to its source field in the row stream.
/// A relation alias identifies the field (a self-join emits `q0.id` and
/// `q1.id` from the same base table), and every source field is claimed at
/// most once so duplicate names keep their positional order.
fn deferred_row_field_mapping(planned: &[BatchField], fields: &[PlanField]) -> Vec<Option<usize>> {
    let qualifier_matches = |planned: &BatchField, field: &PlanField| match &planned.alias {
        Some(alias) => match &field.alias {
            Some(field_alias) => field_alias.eq_ignore_ascii_case(alias),
            None => field.table.eq_ignore_ascii_case(alias),
        },
        None => {
            planned.table.is_empty()
                || field.table.eq_ignore_ascii_case(&planned.table)
                || field
                    .alias
                    .as_ref()
                    .is_some_and(|alias| alias.eq_ignore_ascii_case(&planned.table))
        }
    };
    let mut claimed = vec![false; fields.len()];
    planned
        .iter()
        .map(|planned| {
            let position = fields.iter().enumerate().position(|(index, field)| {
                !claimed[index]
                    && field.column.eq_ignore_ascii_case(&planned.name)
                    && qualifier_matches(planned, field)
            })?;
            claimed[position] = true;
            Some(position)
        })
        .collect()
}

fn try_build_row_storage_batch<'a>(
    db: &'a Arc<Database>,
    txn: TxnView,
    plan: &'a PhysicalPlan,
) -> Result<Option<VecBatchInput<'a>>, QueryError> {
    // Parameter/type errors belong to open-time validation. Resolving these
    // expressions does not touch storage, so reject an invalid prepared KNN
    // probe before returning a cursor instead of surfacing it on first pull.
    match plan {
        PhysicalPlan::VectorIndexScan {
            table,
            index,
            query_vector,
            query_vector_expr,
            metric,
            ..
        } => {
            let column = table.schema.column_index(&index.columns[0])?;
            let query = super::plan::resolve_knn_query_vector(
                table,
                column,
                query_vector,
                query_vector_expr,
            )?;
            super::plan::validate_knn_query_vector(
                &query,
                crate::engine::vector::Metric::parse(metric).unwrap_or_default(),
            )?;
        }
        PhysicalPlan::VectorBruteScan {
            table,
            column_index,
            query_vector,
            query_vector_expr,
            metric,
            ..
        } => {
            let query = super::plan::resolve_knn_query_vector(
                table,
                *column_index,
                query_vector,
                query_vector_expr,
            )?;
            super::plan::validate_knn_query_vector(
                &query,
                crate::engine::vector::Metric::parse(metric).unwrap_or_default(),
            )?;
        }
        _ => {}
    }
    let mut fields = Vec::new();
    super::plan::collect_plan_fields(plan, &mut fields);
    let template = fields
        .into_iter()
        .map(|meta| PlanField {
            meta: super::plan::intern_col_meta(meta),
            value: Value::Null,
        })
        .collect::<Vec<_>>();
    let schema = template_to_batch_schema(&template);
    Ok(Some(VecBatchInput {
        stream: Box::new(DeferredRowStorageBatchOperator {
            db,
            txn,
            plan,
            schema,
            input: None,
            field_mapping: None,
            finished: false,
        }),
        template,
    }))
}

pub(super) fn preflight_runtime_parameters(plan: &PhysicalPlan) -> Result<(), QueryError> {
    let mut failure = None;
    plan.any(|node| {
        if failure.is_some() {
            return false;
        }
        let result = match node {
            PhysicalPlan::VectorIndexScan {
                table,
                index,
                query_vector,
                query_vector_expr,
                metric,
                ..
            } => table
                .schema
                .column_index(&index.columns[0])
                .and_then(|column| {
                    super::plan::resolve_knn_query_vector(
                        table,
                        column,
                        query_vector,
                        query_vector_expr,
                    )
                })
                .and_then(|query| {
                    super::plan::validate_knn_query_vector(
                        &query,
                        crate::engine::vector::Metric::parse(metric).unwrap_or_default(),
                    )
                }),
            PhysicalPlan::VectorBruteScan {
                table,
                column_index,
                query_vector,
                query_vector_expr,
                metric,
                ..
            } => super::plan::resolve_knn_query_vector(
                table,
                *column_index,
                query_vector,
                query_vector_expr,
            )
            .and_then(|query| {
                super::plan::validate_knn_query_vector(
                    &query,
                    crate::engine::vector::Metric::parse(metric).unwrap_or_default(),
                )
            }),
            _ => Ok(()),
        };
        if let Err(error) = result {
            failure = Some(error);
        }
        false
    });
    failure.map_or(Ok(()), Err)
}

/// ROLLUP / GROUPING SETS historically exposed their finalized values through
/// `PlanRow::agg_values` and their GROUPING bitmap through another hidden map
/// entry.  That representation cannot cross a Batch boundary.  This adapter
/// makes the complete aggregate result an ordinary typed relation:
///
///   [group keys..., finalized aggregates..., grouping mask]
///
/// The physical aggregate still owns its spill/parallel strategy; downstream
/// Projection, HAVING, Sort and Window operators consume the same columns and
/// never need to reconstruct a `PlanRow` aggregate context.
struct DeferredGroupingBatchOperator<'a> {
    db: &'a Arc<Database>,
    txn: TxnView,
    plan: &'a PhysicalPlan,
    schema: Arc<BatchSchema>,
    group_count: usize,
    aggregate_keys: Vec<u64>,
    output: Option<std::vec::IntoIter<Batch>>,
}

impl BatchOperator for DeferredGroupingBatchOperator<'_> {
    batch_wrapper_method!(schema);

    fn properties(&self) -> BatchProperties {
        BatchProperties::UNORDERED_SINGLE
    }

    fn next_batch(&mut self) -> Result<Option<Batch>, QueryError> {
        if self.output.is_none() {
            // `execute_plan_rows` dispatches this exact aggregate node to its
            // canonical implementation. Its children may themselves be native
            // BatchOperators; there is no capability decision or input replay
            // after this point.
            // The enclosing BatchOperator owns this node's timing envelope;
            // entering the row stream wrapper here records the same execution twice.
            let rows = super::plan::execute_plan_rows_inner(self.db, self.txn, self.plan)?;
            forward_vec_hash_agg_metrics(self.plan);
            let group_names = (0..self.group_count)
                .map(|index| format!("group_{index}"))
                .collect::<Vec<_>>();
            let mut values = Vec::with_capacity(rows.len());
            for mut row in rows {
                let mut output =
                    Vec::with_capacity(self.group_count + self.aggregate_keys.len() + 1);
                // Normal finalized groups contain only ordered group markers.
                // Move their payloads; avoid per-cell names, searches and string
                // clones while retaining the general representative-row path.
                if row.fields.len() == self.group_count
                    && row
                        .fields
                        .iter()
                        .zip(&group_names)
                        .all(|(field, name)| field.table == "$group" && field.column == *name)
                {
                    output.extend(row.fields.into_iter().map(|field| field.value));
                } else {
                    for name in &group_names {
                        output.push(
                            row.fields
                                .iter_mut()
                                .find(|field| field.table == "$group" && field.column == *name)
                                .map(|field| std::mem::replace(&mut field.value, Value::Null))
                                .unwrap_or(Value::Null),
                        );
                    }
                }
                match row.agg_values {
                    Some(cache) => match Arc::try_unwrap(cache) {
                        Ok(mut cache) => {
                            output.extend(
                                self.aggregate_keys
                                    .iter()
                                    .map(|key| cache.remove(key).unwrap_or(Value::Null)),
                            );
                            output.push(
                                cache
                                    .remove(&super::aggregate::ROLLUP_MASK_AGG_KEY)
                                    .unwrap_or(Value::UInt(0)),
                            );
                        }
                        Err(cache) => {
                            output.extend(
                                self.aggregate_keys
                                    .iter()
                                    .map(|key| cache.get(key).cloned().unwrap_or(Value::Null)),
                            );
                            output.push(
                                cache
                                    .get(&super::aggregate::ROLLUP_MASK_AGG_KEY)
                                    .cloned()
                                    .unwrap_or(Value::UInt(0)),
                            );
                        }
                    },
                    None => {
                        output.extend((0..self.aggregate_keys.len()).map(|_| Value::Null));
                        output.push(Value::UInt(0));
                    }
                }
                values.push(output);
            }
            self.output = Some(value_rows_into_batches(self.schema.clone(), values).into_iter());
        }
        Ok(self.output.as_mut().and_then(Iterator::next))
    }
}

fn grouping_output_preflight(
    input: &PhysicalPlan,
    group_by: &[Expr],
    aggregates: &[Expr],
) -> Option<HashAggPreflight> {
    // HAVING is evaluated by the grouping operator while the full grouping
    // context is still available. Downstream expressions are rewritten onto
    // the explicit output relation below.
    let mut layout = hash_agg_output_preflight(input, group_by, None, aggregates)?;
    let mut fields = layout.output_schema.fields().to_vec();
    fields.push(BatchField {
        table: "__agg".into(),
        alias: None,
        name: "grouping_mask".into(),
        column_type: ColumnType::LongLong,
        source: crate::types::ColumnSource::default(),
    });
    layout.output_schema = Arc::new(BatchSchema::new(fields));
    layout.output_template = layout
        .output_schema
        .fields()
        .iter()
        .map(batch_field_template)
        .collect();
    Some(layout)
}

fn try_build_grouping_batch<'a>(
    db: &'a Arc<Database>,
    txn: TxnView,
    plan: &'a PhysicalPlan,
) -> Result<Option<VecBatchInput<'a>>, QueryError> {
    let Some(aggregate) = plan
        .aggregate_input()
        .filter(|aggregate| aggregate.grouping)
    else {
        return Ok(None);
    };
    let (input, group_by, aggregates) = (aggregate.input, aggregate.group_by, aggregate.aggregates);
    let Some(layout) = grouping_output_preflight(input, group_by, aggregates) else {
        return Ok(None);
    };
    let group_count = layout.group_count;
    let aggregate_keys = layout.aggregate_keys;
    let schema = layout.output_schema;
    let template = layout.output_template;
    Ok(Some(VecBatchInput {
        stream: Box::new(DeferredGroupingBatchOperator {
            db,
            txn,
            plan,
            schema,
            group_count,
            aggregate_keys,
            output: None,
        }),
        template,
    }))
}

struct DeferredFinalizedAggregateBatchOperator<'a> {
    db: &'a Arc<Database>,
    txn: TxnView,
    plan: &'a PhysicalPlan,
    schema: Arc<BatchSchema>,
    group_count: usize,
    aggregate_keys: Vec<u64>,
    output: Option<std::vec::IntoIter<Batch>>,
}

impl BatchOperator for DeferredFinalizedAggregateBatchOperator<'_> {
    batch_wrapper_method!(schema);

    fn properties(&self) -> BatchProperties {
        BatchProperties::UNORDERED_SINGLE
    }

    fn next_batch(&mut self) -> Result<Option<Batch>, QueryError> {
        if self.output.is_none() {
            let rows = super::plan::execute_plan_rows(self.db, self.txn, self.plan)?;
            let mut values = Vec::with_capacity(rows.len());
            for row in rows {
                let mut output = Vec::with_capacity(self.group_count + self.aggregate_keys.len());
                for index in 0..self.group_count {
                    let column = format!("group_{index}");
                    output.push(
                        row.fields
                            .iter()
                            .find(|field| field.table == "$group" && field.column == column)
                            .map(|field| field.value.clone())
                            .unwrap_or(Value::Null),
                    );
                }
                for key in &self.aggregate_keys {
                    output.push(
                        row.agg_values
                            .as_deref()
                            .and_then(|values| values.get(key))
                            .cloned()
                            .unwrap_or(Value::Null),
                    );
                }
                values.push(output);
            }
            self.output = Some(value_rows_into_batches(self.schema.clone(), values).into_iter());
        }
        Ok(self.output.as_mut().and_then(Iterator::next))
    }
}

fn covering_index_agg_layout(
    table: &crate::planner::PlanTable,
    group_by: &[Expr],
    having: Option<&Expr>,
    aggregates: &[Expr],
) -> Option<HashAggPreflight> {
    let mut metas = Vec::new();
    super::plan::push_table_fields(&mut metas, table, None);
    let template = metas
        .into_iter()
        .map(|meta| PlanField {
            meta: super::plan::intern_col_meta(meta),
            value: Value::Null,
        })
        .collect();
    hash_agg_output_preflight_for_template(template, group_by, having, aggregates, None)
}

fn index_agg_layout(plan: &PhysicalPlan) -> Option<HashAggPreflight> {
    match plan {
        PhysicalPlan::IndexAggregate {
            agg_key,
            output_type,
            ..
        } => {
            let output_schema = Arc::new(BatchSchema::new(vec![BatchField {
                table: "__agg".into(),
                alias: None,
                name: "a0".into(),
                column_type: *output_type,
                source: crate::types::ColumnSource::default(),
            }]));
            Some(HashAggPreflight {
                output_template: output_schema
                    .fields()
                    .iter()
                    .map(batch_field_template)
                    .collect(),
                output_schema,
                group_count: 0,
                group_exprs: Vec::new(),
                aggregate_keys: vec![*agg_key],
                having_program: None,
            })
        }
        PhysicalPlan::CoveringIndexAggregate {
            table,
            group_by,
            having,
            aggregates,
            ..
        }
        | PhysicalPlan::ParallelIndexAggregate {
            table,
            group_by,
            having,
            aggregates,
            ..
        } => covering_index_agg_layout(table, group_by, having.as_ref(), aggregates),
        _ => None,
    }
}

fn try_build_index_agg_batch<'a>(
    db: &'a Arc<Database>,
    txn: TxnView,
    plan: &'a PhysicalPlan,
) -> Result<Option<VecBatchInput<'a>>, QueryError> {
    let Some(layout) = index_agg_layout(plan) else {
        return Ok(None);
    };
    let group_count = layout.group_count;
    let aggregate_keys = layout.aggregate_keys;
    let schema = layout.output_schema;
    let template = layout.output_template;
    Ok(Some(VecBatchInput {
        stream: Box::new(DeferredFinalizedAggregateBatchOperator {
            db,
            txn,
            plan,
            schema,
            group_count,
            aggregate_keys,
            output: None,
        }),
        template,
    }))
}

/// Count of times the materializing vec hash join (`try_vec_hash_join`)
/// actually produced a result — bumped once per completed join, not per row,
/// so the overhead is negligible. Read by tests to prove a CTE self-join went
/// through the vec join (and consumed the columnar CTE leaves) rather than
/// silently falling back to the row hash join. Mirrors `CTE_BATCH_SCAN_HITS`.
pub(crate) static VEC_HASH_JOIN_HITS: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);

/// Native HashJoin streams constructed as children of another vector
/// operator (for example HashAgg→HashJoin). Kept separate from the standalone
/// materialising counter so routing tests can detect a hidden nested vec join.
pub(crate) static VEC_HASH_JOIN_STREAM_HITS: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);

/// Whether the streaming vec HashJoin path is enabled. Today this is
/// gated by `MYDB_VECTORIZED` (the streaming path is only safe
/// for inner equi-joins without residual predicates / NULL-safe
/// equality; the bridge enforces those preconditions before constructing
/// the stream). The legacy row-based HashJoin remains the correctness
/// owner for everything outside that envelope.
fn vec_hash_join_streaming_enabled(plan: &PhysicalPlan) -> bool {
    vectorize_enabled_for_plan(plan)
}

/// A native `BatchOperator` over `plan`, plus the statically derived
/// `PlanField` template used to resolve downstream column references.
pub(crate) struct VecBatchInput<'a> {
    pub stream: Box<dyn BatchOperator + 'a>,
    pub(super) template: Vec<PlanField>,
}

/// A native batch subtree ready for the protocol/result boundary. Projection
/// indices are validated from immutable plan metadata before the subtree is
/// opened, so an unsupported SELECT expression cannot consume input and then
/// restart through `PlanStream`.
pub(crate) struct VecResultBatchInput<'a> {
    pub stream: Box<dyn BatchOperator + 'a>,
    pub projection: Vec<VecResultProjection>,
    pub(super) template: Vec<PlanField>,
    /// Multiple expressions that may warn/error must be evaluated in SQL
    /// row-major order (row 0 projection 0..N, then row 1), rather than the
    /// usual faster expression-major column evaluation.
    pub row_major_projection: bool,
}

pub(crate) enum VecResultProjection {
    Column(usize),
    Expr(Arc<ExprProgram>),
    Scalar(Expr),
}

pub(crate) static NATIVE_RESULT_BATCH_HITS: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);

fn hash_aggregate_node(plan: &PhysicalPlan) -> Option<&PhysicalPlan> {
    match plan {
        node @ (PhysicalPlan::HashAggregate { .. }
        | PhysicalPlan::StreamingAggregate { .. }
        | PhysicalPlan::ParallelHashAggregate { .. }
        | PhysicalPlan::HashRollupAggregate { .. }
        | PhysicalPlan::HashGroupingSetsAggregate { .. }
        | PhysicalPlan::StreamingRollupAggregate { .. }
        | PhysicalPlan::StreamingGroupingSetsAggregate { .. }
        | PhysicalPlan::ParallelRollupAggregate { .. }
        | PhysicalPlan::ParallelGroupingSetsAggregate { .. }
        | PhysicalPlan::IndexAggregate { .. }
        | PhysicalPlan::CoveringIndexAggregate { .. }
        | PhysicalPlan::ParallelIndexAggregate { .. }) => Some(node),
        PhysicalPlan::Projection {
            input,
            distinct: false,
            ..
        }
        | PhysicalPlan::Limit { input, .. }
        | PhysicalPlan::Sort { input, .. }
        | PhysicalPlan::IncrementalSort { input, .. }
        | PhysicalPlan::ParallelSort { input, .. }
        | PhysicalPlan::TopK { input, .. }
        | PhysicalPlan::ParallelTopK { input, .. }
        | PhysicalPlan::TopWindow { input, .. }
        | PhysicalPlan::Materialize { input, .. }
        | PhysicalPlan::Exchange { input, .. } => hash_aggregate_node(input),
        _ => None,
    }
}

fn preflight_hash_aggregate_result_projection(
    plan: &PhysicalPlan,
    projection: &[SelectItem],
) -> Option<(Vec<PlanField>, Vec<VecResultProjection>, bool)> {
    let aggregate_node = hash_aggregate_node(plan)?;
    if matches!(
        aggregate_node,
        PhysicalPlan::IndexAggregate { .. }
            | PhysicalPlan::CoveringIndexAggregate { .. }
            | PhysicalPlan::ParallelIndexAggregate { .. }
    ) {
        return preflight_aggregate_result_layout(index_agg_layout(aggregate_node)?, projection);
    }
    let aggregate = aggregate_node
        .aggregate_input()
        .expect("hash_aggregate_node returned a non-aggregate");
    let (input, group_by, having, aggregates, grouping_output) = (
        aggregate.input,
        aggregate.group_by,
        aggregate.having,
        aggregate.aggregates,
        aggregate.grouping,
    );
    let layout = if grouping_output {
        grouping_output_preflight(input, group_by, aggregates)?
    } else {
        hash_agg_output_preflight(input, group_by, having, aggregates)?
    };
    preflight_aggregate_result_layout(layout, projection)
}

fn preflight_aggregate_result_layout(
    layout: HashAggPreflight,
    projection: &[SelectItem],
) -> Option<(Vec<PlanField>, Vec<VecResultProjection>, bool)> {
    let schema = layout.output_schema.clone();
    let mut output = Vec::with_capacity(projection.len());
    let mut expression_count = 0usize;
    let mut has_erroring_expression = false;
    for item in projection {
        let SelectItem::Expr { expr, .. } = item else {
            return None;
        };
        if let Expr::GroupKeyRef { index, .. } = expr {
            if *index >= layout.group_count {
                return None;
            }
            output.push(VecResultProjection::Column(*index));
            continue;
        }
        if let Some(column) =
            expr_column_index_in_template(expr, &layout.output_template[..layout.group_count])
        {
            output.push(VecResultProjection::Column(column));
            continue;
        }
        if super::expr_has_aggregate(expr) {
            let key = super::aggregate::aggregate_key(expr);
            if let Some(slot) = layout
                .aggregate_keys
                .iter()
                .position(|candidate| *candidate == key)
            {
                output.push(VecResultProjection::Column(layout.group_count + slot));
                continue;
            }
        }
        let rewritten = rewrite_hash_agg_output_expr(
            expr,
            &layout.output_schema,
            layout.group_count,
            &layout.group_exprs,
            &layout.aggregate_keys,
        )?;
        let program = expr_batch::compile_post_aggregate(&rewritten, &schema).ok()??;
        let capability = program.capability();
        expression_count += 1;
        has_erroring_expression |= capability.requires_ordered_evaluation();
        output.push(VecResultProjection::Expr(program));
    }
    let row_major = output
        .iter()
        .any(|item| matches!(item, VecResultProjection::Scalar(_)))
        || (has_erroring_expression && expression_count > 1);
    Some((layout.output_template, output, row_major))
}

fn preflight_result_projection(
    plan: &PhysicalPlan,
    projection: &[SelectItem],
    wildcard_source_order: Option<&[String]>,
) -> Option<(Vec<PlanField>, Vec<VecResultProjection>, bool)> {
    if hash_aggregate_node(plan).is_some() {
        return preflight_hash_aggregate_result_projection(plan, projection);
    }
    let planned_template = plan_output_template(plan);
    let schema = template_to_batch_schema(&planned_template);
    let mut output = Vec::new();
    let mut expression_count = 0usize;
    let mut has_erroring_expression = false;
    for item in projection {
        match item {
            SelectItem::Wildcard => {
                let mut used = vec![false; planned_template.len()];
                if let Some(source_order) = wildcard_source_order {
                    for source in source_order {
                        for (index, field) in planned_template.iter().enumerate() {
                            if !used[index]
                                && !field.invisible
                                && super::eval::field_matches_qualifier(field, source)
                            {
                                used[index] = true;
                                output.push(VecResultProjection::Column(index));
                            }
                        }
                    }
                }
                output.extend(
                    planned_template
                        .iter()
                        .enumerate()
                        .filter(|(index, field)| !used[*index] && !field.invisible)
                        .map(|(index, _)| VecResultProjection::Column(index)),
                );
            }
            SelectItem::QualifiedWildcard(qualifier) => {
                let start = output.len();
                let qualifier = qualifier.to_ascii_lowercase();
                output.extend(
                    planned_template
                        .iter()
                        .enumerate()
                        .filter_map(|(idx, field)| {
                            (!field.invisible
                                && (field.table.to_ascii_lowercase() == qualifier
                                    || field.alias.as_ref().is_some_and(|alias| {
                                        alias.to_ascii_lowercase() == qualifier
                                    })))
                            .then_some(VecResultProjection::Column(idx))
                        }),
                );
                if output.len() == start {
                    return None;
                }
            }
            SelectItem::Expr { expr, .. } => {
                if let Some(index) = expr_column_index_in_template(expr, &planned_template) {
                    output.push(VecResultProjection::Column(index));
                    continue;
                }
                // GREATEST/LEAST must aggregate MySQL coercibility and
                // character-set metadata across every operand. That metadata
                // can originate at a derived/CTE boundary and is richer than
                // the value-only function kernel. Evaluate the expression
                // row-wise against the PlanField template inside this batch;
                // the input is still consumed exactly once.
                if matches!(
                    expr,
                    Expr::Function { name, .. }
                        if name.eq_ignore_ascii_case("GREATEST")
                            || name.eq_ignore_ascii_case("LEAST")
                ) {
                    expression_count += 1;
                    has_erroring_expression = true;
                    output.push(VecResultProjection::Scalar(expr.clone()));
                    continue;
                }
                let Some(mut program) = expr_batch::compile_for_batch(expr, &schema).ok()? else {
                    expression_count += 1;
                    has_erroring_expression = true;
                    output.push(VecResultProjection::Scalar(expr.clone()));
                    continue;
                };
                let mut source_sensitive_string_coercion = false;
                let mut source_sensitive_number_rendering = false;
                crate::planner::walk_expr_for_idents(expr, &mut |node| {
                    let Some(index) = expr_column_index_in_template(node, &planned_template) else {
                        return;
                    };
                    let field = &planned_template[index];
                    source_sensitive_number_rendering |= matches!(
                        field.column_type,
                        ColumnType::Float | ColumnType::Double | ColumnType::Bit { .. }
                    );
                    source_sensitive_string_coercion |= match field.column_type {
                        // BLOB uses MYSQL_TYPE_BLOB for both binary and text
                        // families; the explicit BINARY flag distinguishes a
                        // real BLOB from the silent TEXT/BLOB legacy lane.
                        ColumnType::Blob => {
                            field.source.flags & crate::mysql::protocol::BINARY_FLAG == 0
                        }
                        // TEXT is represented as VarString at the result
                        // boundary. A real catalog TEXT has no bounded length;
                        // expression-derived strings have no origin name.
                        ColumnType::VarString => {
                            field.source.length.is_none() && !field.source.org_name.is_empty()
                        }
                        _ => false,
                    };
                });
                let value_string_operations = !source_sensitive_number_rendering
                    && program.has_only_value_string_operations();
                // Numeric coercions retain the catalog-aware oracle (TEXT/BLOB
                // conversions are silent while bounded strings publish 1292).
                // Proven value-only string operations can use compiled kernels;
                // `requires_ordered_evaluation` below still preserves diagnostic
                // precedence, including packet-size warnings and slicing errors.
                // This is a kernel choice in the same pipeline: input is not
                // replayed and the projection remains batch-driven.
                let typed_value_operations = program.has_column_contract(&schema);
                if crate::engine::expr::strict_numeric_materialization_active()
                    || (!typed_value_operations
                        && (((source_sensitive_string_coercion || program.capability().may_error)
                            && !value_string_operations)
                            || !program.capability().deterministic
                            || program.capability().depends_on_session))
                {
                    let Some(legacy) = expr_batch::compile_legacy_for_batch(expr, &schema).ok()?
                    else {
                        expression_count += 1;
                        has_erroring_expression = true;
                        output.push(VecResultProjection::Scalar(expr.clone()));
                        continue;
                    };
                    program = legacy;
                }
                let capability = program.capability();
                expression_count += 1;
                // TEXT expressions previously ran row-major. Keep that order
                // for multiple result expressions, including CONCAT's packet
                // warnings, while replacing only their interpreter kernels.
                has_erroring_expression |= capability.requires_ordered_evaluation()
                    || (source_sensitive_string_coercion && value_string_operations);
                output.push(VecResultProjection::Expr(program));
            }
        }
    }
    // `eval_to_column` walks rows in input order. More than one expression
    // that may error switches the result boundary to its row-major evaluator
    // so warning/error precedence remains identical to the scalar oracle.
    let row_major = output
        .iter()
        .any(|item| matches!(item, VecResultProjection::Scalar(_)))
        || (has_erroring_expression && expression_count > 1);
    Some((planned_template, output, row_major))
}

/// Conservative no-I/O route used ahead of legacy compiled single-table
/// kernels. Auto mode keeps the CBO threshold; explicit `on` selects this
/// protocol for compatible scans.
pub(crate) fn should_route_result_to_batch(plan: &PhysicalPlan, projection: &[SelectItem]) -> bool {
    let plan_key = plan as *const PhysicalPlan as usize;
    if let Some(route) = BATCH_ROUTE_HINT.with(|hint| {
        hint.get()
            .and_then(|(key, route, _)| (key == plan_key).then_some(route))
    }) {
        return route;
    }
    fn supported_scan_spine(plan: &PhysicalPlan) -> bool {
        if let Some(aggregate) = plan.aggregate_input() {
            let layout = if aggregate.grouping {
                grouping_output_preflight(aggregate.input, aggregate.group_by, aggregate.aggregates)
            } else {
                hash_agg_output_preflight(
                    aggregate.input,
                    aggregate.group_by,
                    aggregate.having,
                    aggregate.aggregates,
                )
            };
            return layout.is_some() && supported_scan_spine(aggregate.input);
        }
        match plan {
            PhysicalPlan::Projection {
                input,
                distinct: false,
                ..
            }
            | PhysicalPlan::NullExtend { input, .. }
            | PhysicalPlan::Limit { input, .. }
            | PhysicalPlan::Filter { input, .. }
            | PhysicalPlan::Apply { outer: input, .. }
            | PhysicalPlan::Materialize { input, .. } => supported_scan_spine(input),
            PhysicalPlan::Projection {
                input,
                projection,
                distinct: true,
                ..
            }
            | PhysicalPlan::StreamingDistinct {
                input, projection, ..
            } => distinct_key_preflight(input, projection).is_some() && supported_scan_spine(input),
            PhysicalPlan::TopK {
                input,
                order_by,
                dedup_projection,
                k,
                offset,
                binding,
                ..
            }
            | PhysicalPlan::ParallelTopK {
                input,
                order_by,
                dedup_projection,
                k,
                offset,
                binding,
                ..
            } => {
                let (k, _) = binding.resolve_k_offset(*k, *offset);
                dedup_projection.is_none()
                    && !order_by.is_empty()
                    && k <= vec_topn::max_topn_k_for_budget()
                    && topn_key_preflight(input, order_by).is_ok_and(|keys| keys.is_some())
                    && supported_scan_spine(input)
            }
            PhysicalPlan::Sort {
                input, order_by, ..
            }
            | PhysicalPlan::IncrementalSort {
                input, order_by, ..
            }
            | PhysicalPlan::ParallelSort {
                input, order_by, ..
            } => {
                !order_by.is_empty()
                    && topn_key_preflight(input, order_by).is_ok_and(|keys| keys.is_some())
                    && supported_scan_spine(input)
            }
            PhysicalPlan::HashJoin {
                left,
                right,
                join_type,
                left_keys,
                right_keys,
                residual,
                ..
            } => {
                let keys_compile = |input: &PhysicalPlan, keys: &[Expr]| {
                    let mut fields = Vec::new();
                    super::plan::collect_plan_fields(input, &mut fields);
                    let template = fields
                        .into_iter()
                        .map(|meta| PlanField {
                            meta: super::plan::intern_col_meta(meta),
                            value: Value::Null,
                        })
                        .collect::<Vec<_>>();
                    let schema = template_to_batch_schema(&template);
                    keys.iter().all(|key| {
                        expr_column_index_in_template(key, &template).is_some()
                            || expr_batch::compile_for_batch(key, &schema)
                                .is_ok_and(|program| program.is_some())
                    })
                };
                matches!(
                    join_type,
                    crate::parser::JoinType::Inner
                        | crate::parser::JoinType::Left
                        | crate::parser::JoinType::Right
                        | crate::parser::JoinType::Semi
                        | crate::parser::JoinType::Anti
                        | crate::parser::JoinType::Full
                ) && residual.as_ref().is_none_or(|predicate| {
                    let mut fields = Vec::new();
                    super::plan::collect_plan_fields(left, &mut fields);
                    super::plan::collect_plan_fields(right, &mut fields);
                    let template = fields
                        .into_iter()
                        .map(|meta| PlanField {
                            meta: super::plan::intern_col_meta(meta),
                            value: Value::Null,
                        })
                        .collect::<Vec<_>>();
                    expr_batch::compile_for_batch(predicate, &template_to_batch_schema(&template))
                        .is_ok_and(|program| program.is_some())
                }) && !left_keys.is_empty()
                    && left_keys.len() == right_keys.len()
                    && keys_compile(left, left_keys)
                    && keys_compile(right, right_keys)
                    && supported_scan_spine(left)
                    && supported_scan_spine(right)
            }
            PhysicalPlan::NestedLoopJoin {
                left,
                right,
                join_type,
                condition,
                ..
            } => {
                matches!(
                    join_type,
                    crate::parser::JoinType::Inner
                        | crate::parser::JoinType::Cross
                        | crate::parser::JoinType::Left
                        | crate::parser::JoinType::Right
                        | crate::parser::JoinType::Full
                        | crate::parser::JoinType::Semi
                        | crate::parser::JoinType::Anti
                ) && condition.as_ref().is_none_or(|predicate| {
                    let mut fields = Vec::new();
                    super::plan::collect_plan_fields(left, &mut fields);
                    super::plan::collect_plan_fields(right, &mut fields);
                    let template = fields
                        .into_iter()
                        .map(|meta| PlanField {
                            meta: super::plan::intern_col_meta(meta),
                            value: Value::Null,
                        })
                        .collect::<Vec<_>>();
                    expr_batch::compile_for_batch(predicate, &template_to_batch_schema(&template))
                        .is_ok_and(|program| program.is_some())
                }) && supported_scan_spine(left)
                    && supported_scan_spine(right)
            }
            PhysicalPlan::IndexNestedLoopJoin { left, .. } => {
                // The bounded INLJ kernel evaluates residual subqueries in
                // the captured session/CTE context. Reject only subqueries in
                // lookup bounds, exactly as the operator's own preflight does;
                // rejecting every residual here sent the entire result graph
                // through eager row scans despite a supported native kernel.
                super::plan::inlj_chunk_stream_applicable(plan) && supported_scan_spine(left)
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
                let k = offset.saturating_add(limit);
                !order_by.is_empty()
                    && k <= vec_topn::max_topn_k_for_budget()
                    && topn_key_preflight(input, order_by).is_ok_and(|keys| keys.is_some())
                    && supported_scan_spine(input)
            }
            PhysicalPlan::WindowAgg { input, .. } => supported_scan_spine(input),
            PhysicalPlan::SeqScan { .. }
            | PhysicalPlan::ZoneMapScan { .. }
            | PhysicalPlan::ParallelSeqScan { .. }
            | PhysicalPlan::ParallelZoneMapScan { .. } => true,
            PhysicalPlan::IndexMerge { .. }
            | PhysicalPlan::IndexLookup { .. }
            | PhysicalPlan::IndexMultiLookup { .. }
            | PhysicalPlan::IndexRangeScan { .. }
            | PhysicalPlan::IndexFastScan { .. }
            | PhysicalPlan::ParallelIndexRangeScan { .. }
            | PhysicalPlan::ParallelIndexFastScan { .. } => true,
            PhysicalPlan::Exchange { input, .. } => supported_scan_spine(input),
            PhysicalPlan::SetOp { left, right, .. } => {
                supported_scan_spine(left) && supported_scan_spine(right)
            }
            PhysicalPlan::RrfFusion { .. }
            | PhysicalPlan::GenerateSeries { .. }
            | PhysicalPlan::ValuesScan { .. }
            | PhysicalPlan::JsonTableScan { .. }
            | PhysicalPlan::SystemScan { .. }
            | PhysicalPlan::CteScan { .. }
            | PhysicalPlan::FulltextScan { .. }
            | PhysicalPlan::SpatialScan { .. }
            | PhysicalPlan::VectorIndexScan { .. }
            | PhysicalPlan::VectorBruteScan { .. }
            | PhysicalPlan::IndexInLookup { .. }
            | PhysicalPlan::AdaptiveJoin { .. }
            | PhysicalPlan::IndexAggregate { .. }
            | PhysicalPlan::CoveringIndexAggregate { .. }
            | PhysicalPlan::ParallelIndexAggregate { .. } => true,
            _ => false,
        }
    }
    supported_scan_spine(plan)
        && preflight_result_projection(plan, projection, None).is_some()
        && vectorize_enabled_for_plan(plan)
}

pub(crate) fn try_native_result_batch_input<'a>(
    db: &'a Arc<Database>,
    txn: TxnView,
    plan: &'a PhysicalPlan,
    projection: &[SelectItem],
    wildcard_source_order: &[String],
) -> Result<Option<VecResultBatchInput<'a>>, QueryError> {
    if !vectorize_enabled_for_plan(plan) {
        return Ok(None);
    }

    let Some((planned_template, output, row_major_projection)) =
        preflight_result_projection(plan, projection, Some(wildcard_source_order))
    else {
        return Ok(None);
    };

    let Some(VecBatchInput { stream, template }) = try_native_vec_input_stream(db, txn, plan)?
    else {
        return Ok(None);
    };
    if template.len() != planned_template.len()
        || output.iter().any(|projection| {
            matches!(projection, VecResultProjection::Column(index) if *index >= template.len())
        })
    {
        return Err(crate::storage::error(
            crate::error::catalog::ER_UNKNOWN_ERROR,
            "batch result schema diverged from its preflight metadata",
        ));
    }
    NATIVE_RESULT_BATCH_HITS.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    Ok(Some(VecResultBatchInput {
        stream,
        projection: output,
        template,
        row_major_projection,
    }))
}

enum NativeCompileOutcome<'a> {
    Native(VecBatchInput<'a>),
    Fallback(analyze_stats::VectorFallbackReason),
}

struct NativeVecCompiler;

impl NativeVecCompiler {
    fn compile<'a>(
        db: &'a Arc<Database>,
        txn: TxnView,
        plan: &'a PhysicalPlan,
    ) -> Result<NativeCompileOutcome<'a>, QueryError> {
        let mut compiled = match plan {
            PhysicalPlan::NullExtend { .. } => try_build_null_extend_input(db, txn, plan)?,
            PhysicalPlan::Projection {
                distinct: false, ..
            } => try_build_vec_projection_input(db, txn, plan)?,
            PhysicalPlan::Projection { distinct: true, .. }
            | PhysicalPlan::StreamingDistinct { .. } => {
                try_build_vec_distinct_input(db, txn, plan)?
            }
            PhysicalPlan::Limit { .. } => try_build_vec_limit_input(db, txn, plan)?,
            PhysicalPlan::Sort { .. }
            | PhysicalPlan::IncrementalSort { .. }
            | PhysicalPlan::ParallelSort { .. } => try_build_vec_sort_input(db, txn, plan)?,
            PhysicalPlan::TopK { .. }
            | PhysicalPlan::TopWindow { .. }
            | PhysicalPlan::ParallelTopK { .. } => try_build_vec_topn_input(db, txn, plan)?,
            PhysicalPlan::HashAggregate { .. }
            | PhysicalPlan::StreamingAggregate { .. }
            | PhysicalPlan::ParallelHashAggregate { .. } => {
                try_build_vec_hash_agg_input(db, txn, plan)?
            }
            PhysicalPlan::HashRollupAggregate { .. }
            | PhysicalPlan::HashGroupingSetsAggregate { .. }
            | PhysicalPlan::StreamingRollupAggregate { .. }
            | PhysicalPlan::StreamingGroupingSetsAggregate { .. }
            | PhysicalPlan::ParallelRollupAggregate { .. }
            | PhysicalPlan::ParallelGroupingSetsAggregate { .. } => {
                try_build_grouping_batch(db, txn, plan)?
            }
            PhysicalPlan::IndexAggregate { .. }
            | PhysicalPlan::CoveringIndexAggregate { .. }
            | PhysicalPlan::ParallelIndexAggregate { .. } => {
                try_build_index_agg_batch(db, txn, plan)?
            }
            PhysicalPlan::HashJoin { .. } => try_build_vec_hash_join_input(db, txn, plan)?,
            PhysicalPlan::NestedLoopJoin { .. } => {
                try_build_vec_nested_loop_join_input(db, txn, plan)?
            }
            PhysicalPlan::IndexNestedLoopJoin { .. } => try_build_inlj_batch_input(db, txn, plan)?,
            PhysicalPlan::Filter { .. } | PhysicalPlan::Apply { .. } => {
                try_build_vec_filter_input(db, txn, plan)?
            }
            PhysicalPlan::WindowAgg { .. } => try_build_window_batch(db, txn, plan)?,
            PhysicalPlan::GenerateSeries { .. } => try_build_generate_series_batch(plan)?,
            PhysicalPlan::ValuesScan { .. } => try_build_values_batch(db, txn, plan)?,
            PhysicalPlan::JsonTableScan { .. } => try_build_json_table_batch(plan)?,
            PhysicalPlan::SystemScan { .. } => try_build_system_batch(db, plan)?,
            PhysicalPlan::IndexMerge { .. } => try_build_direct_index_merge_batch(db, txn, plan)?,
            PhysicalPlan::RrfFusion { .. }
            | PhysicalPlan::FulltextScan { .. }
            | PhysicalPlan::SpatialScan { .. }
            | PhysicalPlan::VectorIndexScan { .. }
            | PhysicalPlan::VectorBruteScan { .. }
            | PhysicalPlan::AdaptiveJoin { .. } => try_build_row_storage_batch(db, txn, plan)?,
            PhysicalPlan::IndexInLookup { .. } => try_build_parallel_index_in_batch(db, txn, plan)?,
            PhysicalPlan::SetOp { .. } => try_build_setop_batch(db, txn, plan)?,
            PhysicalPlan::Materialize { input, .. } => try_native_vec_input_stream(db, txn, input)?,
            // RecursiveUnion owns its working/delta spools in the canonical
            // row kernel today, but it still enters the same pull graph via a
            // bounded row-to-batch compatibility lane. The lane is selected
            // before open and never replays an iteration after input starts.
            PhysicalPlan::RecursiveUnion { .. } => try_build_row_storage_batch(db, txn, plan)?,
            PhysicalPlan::CteScan { .. } => try_build_cte_batch_scan(plan)?,
            PhysicalPlan::Exchange { input, .. } => try_native_vec_input_stream(db, txn, input)?,
            PhysicalPlan::SeqScan { .. }
            | PhysicalPlan::ZoneMapScan { .. }
            | PhysicalPlan::ParallelSeqScan { .. }
            | PhysicalPlan::ParallelZoneMapScan { .. } => {
                try_build_direct_batch_scan(db, txn, plan)?
            }
            PhysicalPlan::IndexLookup { .. }
            | PhysicalPlan::IndexMultiLookup { .. }
            | PhysicalPlan::IndexRangeScan { .. }
            | PhysicalPlan::IndexFastScan { .. }
            | PhysicalPlan::ParallelIndexRangeScan { .. }
            | PhysicalPlan::ParallelIndexFastScan { .. } => {
                try_build_direct_index_batch_scan(db, txn, plan)?
            }
        };
        // Every non-aggregate physical node has a semantic-complete scalar
        // lane in the same BatchOperator graph. This compatibility operator
        // is selected before I/O and consumes its plan once; it prevents one
        // missing typed kernel from restarting the whole statement in the
        // legacy executor. Aggregate rows carry out-of-band state and are
        // deliberately handled only by their explicit finalized-column
        // adapters above.
        if compiled.is_none() && !super::plan::plan_emits_aggregate_rows(plan) {
            compiled = try_build_row_storage_batch(db, txn, plan)?;
        }
        Ok(match compiled {
            Some(input) => NativeCompileOutcome::Native(input),
            None => {
                NativeCompileOutcome::Fallback(analyze_stats::VectorFallbackReason::UnsupportedPlan)
            }
        })
    }
}

/// Per-node ANALYZE envelope for the common batch graph. Every recursively
/// compiled child gets its own wrapper, so native scans/filters/sorts no
/// longer disappear behind a synthetic root-only timing record.
struct AnalyzedBatchOperator<'a> {
    plan: &'a PhysicalPlan,
    inner: Box<dyn BatchOperator + 'a>,
    // Only time spent inside this operator's `next_batch` calls belongs to
    // the node. A wall clock started at `open` also counts downstream work
    // performed while the returned batch is borrowed by the parent, which
    // can make a cheap CTE/scan leaf appear slower than its materializer.
    active_ns: u128,
    first_row_ns: Option<u128>,
    activated: bool,
    operator_io: std::sync::Arc<crate::storage::QueryBufferStats>,
    // Kept separately for ZoneMap's metadata/candidate/heap decomposition.
    started: std::time::Instant,
    output_rows: u64,
    batches: u64,
    recorded: bool,
    runtime_filter_key: Option<crate::planner::PlanNodeId>,
    range_plan_key: Option<crate::planner::PlanNodeId>,
    zone_before: Option<(u64, u64, u64, u64, u64, u64, u64)>,
    partition_visits: Option<(
        crate::planner::PlanNodeId,
        std::sync::Arc<std::sync::atomic::AtomicU64>,
    )>,
}

impl AnalyzedBatchOperator<'_> {
    fn record(&mut self) {
        if self.recorded {
            return;
        }
        if !self.activated {
            return;
        }
        self.recorded = true;
        if let Some(key) = self.runtime_filter_key {
            analyze_stats::add_runtime_filter_rejected_key(
                key,
                analyze_stats::take_pending_runtime_filter_rejected(),
            );
            let (spill_bytes, spill_partitions) = analyze_stats::take_pending_hash_join_spill();
            if spill_bytes > 0 {
                analyze_stats::add_spill_bytes(self.plan, spill_bytes);
                analyze_stats::add_spill_shape(
                    self.plan,
                    spill_partitions,
                    1,
                    spill_partitions,
                    "memory_limit",
                );
            }
            let (build_rows, probe_rows, match_rows) = analyze_stats::take_pending_join_runtime();
            analyze_stats::add_join_runtime_key(key, build_rows, probe_rows, match_rows);
        }
        if let Some(key) = analyze_stats::plan_key(self.plan) {
            analyze_stats::record_key_timing(
                key,
                self.first_row_ns.unwrap_or(self.active_ns),
                self.active_ns,
                0,
                self.output_rows,
                {
                    let (cache_reads, physical_reads) =
                        crate::storage::query_buffer_stats_counts(&self.operator_io);
                    analyze_stats::NodeIoStats {
                        cache_reads,
                        physical_reads,
                    }
                },
                true,
            );
        }
        let conversion_rows = analyze_stats::take_pending_conversion_rows();
        analyze_stats::add_vector_runtime(
            self.plan,
            self.batches,
            conversion_rows,
            match self.plan {
                PhysicalPlan::Exchange { degree, .. }
                | PhysicalPlan::ParallelSeqScan { degree, .. }
                | PhysicalPlan::ParallelZoneMapScan { degree, .. }
                | PhysicalPlan::ParallelIndexRangeScan { degree, .. }
                | PhysicalPlan::ParallelIndexFastScan { degree, .. }
                | PhysicalPlan::ParallelHashAggregate { degree, .. } => *degree,
                _ => 1,
            },
        );
        let (native_expression_rows, scalar_expression_rows) =
            analyze_stats::take_pending_expression_rows();
        if native_expression_rows > 0 || scalar_expression_rows > 0 {
            analyze_stats::add_expression_runtime(
                self.plan,
                if native_expression_rows > 0 && scalar_expression_rows > 0 {
                    "hybrid"
                } else if scalar_expression_rows > 0 {
                    "scalar"
                } else {
                    "decoded"
                },
                native_expression_rows,
                scalar_expression_rows,
            );
        }
        if let Some(before) = self.zone_before {
            let after = super::plan::zone_map_runtime_snapshot();
            let candidate_ns = after.1.saturating_sub(before.1);
            if let Some(key) = analyze_stats::plan_key(self.plan) {
                analyze_stats::add_zone_map_runtime_key(
                    key,
                    analyze_stats::ZoneMapRuntimeStats {
                        metadata_ns: after.0.saturating_sub(before.0),
                        candidate_ns,
                        heap_scan_ns: self
                            .started
                            .elapsed()
                            .as_nanos()
                            .min(u64::MAX as u128)
                            .saturating_sub(candidate_ns as u128)
                            as u64,
                        tasks: after.2.saturating_sub(before.2),
                        pages_scanned: after.3.saturating_sub(before.3),
                        pages_skipped: after.4.saturating_sub(before.4),
                        fallbacks: after.5.saturating_sub(before.5) + u64::from(candidate_ns == 0),
                        workers: after.6.saturating_sub(before.6),
                    },
                );
            }
        }
        if let Some((key, counter)) = &self.partition_visits {
            analyze_stats::add_partitions_key(
                *key,
                counter.load(std::sync::atomic::Ordering::Relaxed),
            );
        }
    }
}

impl BatchOperator for AnalyzedBatchOperator<'_> {
    fn prefer_key_locality(&mut self, columns: &[usize]) -> bool {
        self.inner.prefer_key_locality(columns)
    }

    fn limit_column_prefixes(&mut self, limits: &[(usize, usize)]) {
        self.inner.limit_column_prefixes(limits);
    }
    fn schema(&self) -> &Arc<BatchSchema> {
        self.inner.schema()
    }

    fn properties(&self) -> BatchProperties {
        self.inner.properties()
    }

    fn sample_scan_pages(&mut self, max_pages: usize) -> bool {
        self.inner.sample_scan_pages(max_pages)
    }

    fn install_runtime_filter(&mut self, mut filter: crate::planner::RuntimeFilter) -> bool {
        // ANALYZE is observational only and must never form an execution
        // boundary. In particular, a parent hash join installs its build-side
        // filter before the first probe batch is pulled; forward it through
        // the metrics wrapper so an INLJ/scan below can apply it predicate-
        // first instead of producing rows merely to reject them again here.
        filter.enable_rejection_counter();
        self.inner.install_runtime_filter(filter)
    }

    fn install_runtime_index_bound(&mut self, predicate: &Expr) -> bool {
        self.inner.install_runtime_index_bound(predicate)
    }

    fn consume_into_hash_aggregate(
        &mut self,
        aggregate: &mut vec_hash_agg::HashAggregateCore,
    ) -> Result<bool, QueryError> {
        self.activated = true;
        let before = aggregate.metrics();
        let started = std::time::Instant::now();
        let operator_io = std::sync::Arc::clone(&self.operator_io);
        let consumed = crate::storage::with_shared_operator_buffer_stats(operator_io, || {
            self.inner.consume_into_hash_aggregate(aggregate)
        })?;
        if consumed {
            let after = aggregate.metrics();
            self.active_ns = self.active_ns.saturating_add(started.elapsed().as_nanos());
            self.batches = self
                .batches
                .saturating_add(after.batches.saturating_sub(before.batches));
            self.output_rows = self.output_rows.saturating_add(
                after
                    .native_aggregate_rows
                    .saturating_sub(before.native_aggregate_rows),
            );
            self.first_row_ns.get_or_insert(self.active_ns);
            self.record();
        }
        Ok(consumed)
    }

    fn consume_computed_into_hash_aggregate(
        &mut self,
        aggregate: &mut vec_hash_agg::HashAggregateCore,
        expressions: &[Expr],
    ) -> Result<bool, QueryError> {
        self.activated = true;
        let before = aggregate.metrics();
        let started = std::time::Instant::now();
        let operator_io = std::sync::Arc::clone(&self.operator_io);
        let consumed = crate::storage::with_shared_operator_buffer_stats(operator_io, || {
            self.inner
                .consume_computed_into_hash_aggregate(aggregate, expressions)
        })?;
        if consumed {
            let after = aggregate.metrics();
            self.active_ns = self.active_ns.saturating_add(started.elapsed().as_nanos());
            self.batches = self
                .batches
                .saturating_add(after.batches.saturating_sub(before.batches));
            self.output_rows = self.output_rows.saturating_add(
                after
                    .native_aggregate_rows
                    .saturating_sub(before.native_aggregate_rows),
            );
            self.first_row_ns.get_or_insert(self.active_ns);
            self.record();
        }
        Ok(consumed)
    }

    fn next_batch(&mut self) -> Result<Option<Batch>, QueryError> {
        self.activated = true;
        let key = self.range_plan_key;
        let collector = self
            .partition_visits
            .as_ref()
            .map(|(_, counter)| std::sync::Arc::clone(counter));
        let started = std::time::Instant::now();
        let pull = || {
            crate::engine::executor::partition_router::with_partition_visit_collector(
                collector,
                || {
                    analyze_stats::with_range_scan_plan_key_if_present(key, || {
                        self.inner.next_batch()
                    })
                },
            )
        };
        let operator_io = std::sync::Arc::clone(&self.operator_io);
        let batch = crate::storage::with_shared_operator_buffer_stats(operator_io, pull)?;
        self.active_ns = self.active_ns.saturating_add(started.elapsed().as_nanos());
        if let Some(batch) = batch.as_ref() {
            self.batches = self.batches.saturating_add(1);
            self.output_rows = self.output_rows.saturating_add(batch.visible_rows() as u64);
        } else {
            self.record();
        }
        self.first_row_ns.get_or_insert(self.active_ns);
        Ok(batch)
    }
}

impl Drop for AnalyzedBatchOperator<'_> {
    fn drop(&mut self) {
        self.record();
    }
}

/// Compile the currently supported native columnar subtree. `None` is
/// returned before starting the input when any node is unsupported, so
/// the caller can select the legacy executor without a partial scan or a
/// PlanRow→Batch→PlanRow round trip.
pub(crate) fn try_native_vec_input_stream<'a>(
    db: &'a Arc<Database>,
    txn: TxnView,
    plan: &'a PhysicalPlan,
) -> Result<Option<VecBatchInput<'a>>, QueryError> {
    if matches!(plan, PhysicalPlan::SetOp { .. }) {
        crate::util::stack::with_stack(
            crate::engine::executor::mem_budget::ControlledMemoryCategory::Executor,
            || try_native_vec_input_stream_checked(db, txn, plan),
        )?
    } else {
        try_native_vec_input_stream_checked(db, txn, plan)
    }
}

fn try_native_vec_input_stream_checked<'a>(
    db: &'a Arc<Database>,
    txn: TxnView,
    plan: &'a PhysicalPlan,
) -> Result<Option<VecBatchInput<'a>>, QueryError> {
    let analyze_started = std::time::Instant::now();
    let zone_before = (analyze_stats::is_recording()
        && matches!(
            plan,
            PhysicalPlan::ZoneMapScan { .. } | PhysicalPlan::ParallelZoneMapScan { .. }
        ))
    .then(super::plan::zone_map_runtime_snapshot);
    let partition_visits = if analyze_stats::is_recording() {
        match plan {
            PhysicalPlan::SeqScan { table, .. }
            | PhysicalPlan::ZoneMapScan { table, .. }
            | PhysicalPlan::ParallelSeqScan { table, .. }
            | PhysicalPlan::ParallelZoneMapScan { table, .. }
                if table.schema.is_partitioned() =>
            {
                analyze_stats::plan_key(plan).map(|key| {
                    (
                        key,
                        std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0)),
                    )
                })
            }
            _ => None,
        }
    } else {
        None
    };
    let compile_collector = partition_visits
        .as_ref()
        .map(|(_, counter)| std::sync::Arc::clone(counter));
    let compiled = crate::engine::executor::partition_router::with_partition_visit_collector(
        compile_collector,
        || NativeVecCompiler::compile(db, txn, plan),
    )?;
    match compiled {
        NativeCompileOutcome::Native(mut input) => {
            // DeferredHashAgg and SetOp already own equivalent envelopes
            // because they must begin after capability preflight / branch
            // initialization. All other nodes use the uniform wrapper here,
            // including recursively built leaves.
            if analyze_stats::is_recording()
                && !matches!(
                    plan,
                    PhysicalPlan::HashAggregate { .. }
                        | PhysicalPlan::StreamingAggregate { .. }
                        | PhysicalPlan::ParallelHashAggregate { .. }
                        | PhysicalPlan::SetOp { .. }
                )
            {
                input.stream = Box::new(AnalyzedBatchOperator {
                    plan,
                    inner: input.stream,
                    active_ns: analyze_started.elapsed().as_nanos(),
                    first_row_ns: None,
                    activated: false,
                    operator_io: std::sync::Arc::new(crate::storage::QueryBufferStats::default()),
                    started: analyze_started,
                    output_rows: 0,
                    batches: 0,
                    recorded: false,
                    runtime_filter_key: matches!(plan, PhysicalPlan::HashJoin { .. })
                        .then(|| analyze_stats::plan_key(plan))
                        .flatten(),
                    range_plan_key: matches!(
                        plan,
                        PhysicalPlan::IndexMerge { .. }
                            | PhysicalPlan::IndexLookup { .. }
                            | PhysicalPlan::IndexMultiLookup { .. }
                            | PhysicalPlan::IndexRangeScan { .. }
                            | PhysicalPlan::IndexFastScan { .. }
                            | PhysicalPlan::ParallelIndexRangeScan { .. }
                            | PhysicalPlan::ParallelIndexFastScan { .. }
                    )
                    .then(|| analyze_stats::plan_key(plan))
                    .flatten(),
                    zone_before,
                    partition_visits,
                });
            }
            Ok(Some(input))
        }
        NativeCompileOutcome::Fallback(reason) => {
            analyze_stats::set_vector_fallback(plan, reason);
            Ok(None)
        }
    }
}

/// Execute a completely preflighted physical graph to the final SQL value
/// boundary. Used by query-expression parents (SetOp/Materialize/CTAS) that
/// need to consume a child inside the same statement rather than through the
/// protocol row cursor. `None` is returned before I/O when a node family has
/// not yet been lowered.
pub(crate) fn execute_native_plan_values(
    db: &Arc<Database>,
    txn: TxnView,
    plan: &PhysicalPlan,
) -> Result<Option<(Vec<crate::types::Column>, Vec<Vec<Value>>)>, QueryError> {
    match execute_native_plan_values_capped(db, txn, plan, usize::MAX)? {
        NativePlanValues::Complete(columns, rows) => Ok(Some((columns, rows))),
        NativePlanValues::Unsupported | NativePlanValues::Exceeded => Ok(None),
    }
}

pub(crate) enum NativePlanValues {
    /// The plan has no end-to-end native batch graph.
    Unsupported,
    /// More than the requested number of rows was produced; the partial
    /// result was discarded.
    Exceeded,
    Complete(Vec<crate::types::Column>, Vec<Vec<Value>>),
}

/// Drain a native batch graph on the caller's thread, giving up as soon as
/// it has produced more than `max_rows` rows.
pub(crate) fn execute_native_plan_values_capped(
    db: &Arc<Database>,
    txn: TxnView,
    plan: &PhysicalPlan,
    max_rows: usize,
) -> Result<NativePlanValues, QueryError> {
    preflight_runtime_parameters(plan)?;
    let cached = BATCH_ROUTE_HINT.with(|hint| {
        hint.get().and_then(|(key, _, supported)| {
            (key == plan as *const PhysicalPlan as usize)
                .then_some(supported)
                .flatten()
        })
    });
    let supported = match cached {
        Some(supported) => {
            #[cfg(test)]
            NATIVE_OUTPUT_PREFLIGHT_REUSES.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            supported
        }
        None => preflight_query_output_supported(plan)?,
    };
    if !supported {
        return Ok(NativePlanValues::Unsupported);
    }
    let Some(mut input) = try_build_query_output_batch(db, txn, plan)? else {
        return Err(crate::storage::error(
            crate::error::catalog::ER_UNKNOWN_ERROR,
            "physical batch graph changed capability after preflight",
        ));
    };
    NATIVE_RESULT_BATCH_HITS.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    if plan.any(|node| matches!(node, PhysicalPlan::SetOp { .. })) {
        SETOP_BATCH_HITS.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }
    let columns = input
        .template
        .iter()
        .map(|field| crate::types::Column {
            name: field.column.clone(),
            column_type: field.column_type,
            source: field.source.clone(),
        })
        .collect();
    let mut rows = Vec::new();
    while let Some(batch) = input.stream.next_batch()? {
        crate::engine::cancel::check_cancelled()?;
        if batch.visible_rows() > max_rows.saturating_sub(rows.len()) {
            return Ok(NativePlanValues::Exceeded);
        }
        rows.reserve(batch.visible_rows());
        for row in batch.visible_indices() {
            rows.push(batch.row(row));
        }
    }
    Ok(NativePlanValues::Complete(columns, rows))
}

struct NativePlanBatchCursor {
    rx: Option<std::sync::mpsc::Receiver<Result<QueuedScanBatch, QueryError>>>,
    worker: Option<std::thread::JoinHandle<()>>,
    cancellation: crate::engine::cancel::CancellationSnapshot,
    budget: Arc<mem_budget::MemBudget>,
    current: Option<Batch>,
    current_charge: Option<mem_budget::ScopedCharge>,
    current_rows: std::vec::IntoIter<usize>,
    warning_sink: crate::engine::api::StatementWarningSink,
}

impl NativePlanBatchCursor {
    #[inline]
    fn check_cancelled(&self) -> Result<(), QueryError> {
        // Workers publish terminal OOM before cancelling. Resolve that primary
        // error only on failure, without a budget lookup on successful rows.
        self.cancellation
            .check()
            .map_err(|error| self.budget.terminal_oom().unwrap_or(error))
    }

    fn finish(&mut self) -> Result<(), QueryError> {
        let result = self
            .worker
            .take()
            .map(super::stream::join_stream_thread)
            .unwrap_or(Ok(()));
        match self.budget.terminal_oom() {
            Some(error) => Err(error),
            None => result,
        }
    }
}

impl std::fmt::Debug for NativePlanBatchCursor {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("NativePlanBatchCursor")
            .field("worker_active", &self.worker.is_some())
            .finish_non_exhaustive()
    }
}

impl crate::engine::api::ResultCursor for NativePlanBatchCursor {
    fn next_chunk(&mut self) -> Result<Option<crate::engine::api::ResultChunk>, QueryError> {
        use crate::engine::api::{ResultBatch, ResultChunk};
        self.check_cancelled()?;
        // Support callers switching from row consumption partway through a
        // batch. Preserve the remaining physical indices, not a dense prefix.
        if let Some(batch) = self.current.take() {
            let indices = self
                .current_rows
                .by_ref()
                .map(|row| row as u32)
                .collect::<Vec<_>>();
            if !indices.is_empty() {
                return Ok(Some(ResultChunk::Columnar(ResultBatch {
                    batch: batch.with_selection(Selection::Indices(indices)),
                    charge: self.current_charge.take(),
                })));
            }
        }
        self.current_charge = None;
        match self
            .rx
            .as_ref()
            .expect("native batch receiver present")
            .recv()
        {
            Ok(Ok(QueuedScanBatch { batch, _charge })) => {
                Ok(Some(ResultChunk::Columnar(ResultBatch {
                    batch,
                    charge: Some(_charge),
                })))
            }
            Ok(Err(error)) => Err(self.budget.terminal_oom().unwrap_or(error)),
            Err(_) => {
                self.finish()?;
                Ok(None)
            }
        }
    }

    fn next_row(&mut self) -> Result<Option<Vec<Value>>, QueryError> {
        loop {
            self.check_cancelled()?;
            if let Some(row) = self.current_rows.next() {
                return Ok(Some(
                    self.current
                        .as_ref()
                        .expect("native batch present while row indices remain")
                        .row(row),
                ));
            }
            // Release the consumed batch before waiting for its successor.
            // CTE materializers may spill while the native producer is ahead;
            // queued and current batches both retain their real reservations.
            self.current = None;
            self.current_charge = None;
            match self
                .rx
                .as_ref()
                .expect("native batch receiver present")
                .recv()
            {
                Ok(Ok(queued)) => {
                    let QueuedScanBatch { batch, _charge } = queued;
                    self.current_charge = Some(_charge);
                    self.current_rows = batch.visible_indices().collect::<Vec<_>>().into_iter();
                    self.current = Some(batch);
                }
                Ok(Err(error)) => return Err(self.budget.terminal_oom().unwrap_or(error)),
                Err(_) => {
                    self.finish()?;
                    return Ok(None);
                }
            }
        }
    }

    fn take_warnings(&mut self) -> crate::engine::api::SqlWarnings {
        std::mem::take(&mut *self.warning_sink.lock())
    }
}

impl Drop for NativePlanBatchCursor {
    fn drop(&mut self) {
        self.cancellation
            .finish_channel_worker(&mut self.rx, &mut self.worker);
    }
}

/// Convert a fully lowered physical graph to the sole protocol row boundary.
/// The graph is preflighted before the worker is spawned; once opened, native
/// batches cross a bounded channel and become rows only as the client pulls
/// them. Early cursor close cancels the worker and releases its snapshot,
/// reservations and spill files.
pub(in crate::engine::executor) fn try_stream_native_plan(
    db: Arc<Database>,
    txn: TxnView,
    plan: Arc<PhysicalPlan>,
    execution_context: super::execution_context::ExecutionContext,
) -> Result<Option<crate::engine::api::StreamingResultSet>, QueryError> {
    preflight_runtime_parameters(&plan)?;
    let Some(template) = preflight_query_output_template(&plan)? else {
        return Ok(None);
    };
    // This entry point is used by complete query expressions (notably set
    // operations) without passing through execute_physical_select. Pin the
    // exact finalized tree before its worker starts so EXPLAIN ANALYZE formats
    // the same nodes that publish runtime and I/O statistics.
    analyze_stats::register_execution_plan(&plan)?;
    let columns = template
        .iter()
        .map(|field| crate::types::Column {
            name: field.column.clone(),
            column_type: field.column_type,
            source: field.source.clone(),
        })
        .collect::<Vec<_>>();
    NATIVE_RESULT_BATCH_HITS.fetch_add(1, std::sync::atomic::Ordering::Relaxed);

    let execution_context = execution_context.fork_worker_cancellation();
    let cancellation = execution_context.cancellation().clone();
    let budget = execution_context.budget().clone();
    let warning_sink = execution_context.warning_sink().clone();
    let snapshot_guard = db.register_statement_snapshot(txn.snapshot_scn);
    let (tx, rx) = std::sync::mpsc::sync_channel::<Result<QueuedScanBatch, QueryError>>(
        super::stream::stream_channel_batches(),
    );
    let worker_plan = Arc::clone(&plan);
    let worker = std::thread::spawn(move || {
        let _snapshot_guard = snapshot_guard;
        let _active_worker = super::stream::ActiveStreamWorker::enter();
        let result = execution_context.run(&db, || -> Result<(), QueryError> {
            let Some(mut input) = try_build_query_output_batch(&db, txn, &worker_plan)? else {
                return Err(crate::storage::error(
                    crate::error::catalog::ER_UNKNOWN_ERROR,
                    "physical batch graph changed capability after preflight",
                ));
            };
            if worker_plan.any(|node| matches!(node, PhysicalPlan::SetOp { .. })) {
                SETOP_BATCH_HITS.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            }
            while let Some(batch) = input.stream.next_batch()? {
                crate::engine::cancel::check_cancelled()?;
                if batch.visible_rows() != 0
                    && !send_parallel_scan_message(&tx, &execution_context, Ok(batch))
                {
                    return Ok(());
                }
            }
            Ok(())
        });
        if let Err(error) = result {
            let _ = tx.send(Err(execution_context.terminal_error(error)));
        }
    });
    Ok(Some(crate::engine::api::StreamingResultSet {
        columns,
        found_rows: None,
        cursor: Box::new(NativePlanBatchCursor {
            rx: Some(rx),
            worker: Some(worker),
            cancellation,
            budget,
            current: None,
            current_charge: None,
            current_rows: Vec::new().into_iter(),
            warning_sink,
        }),
    }))
}

/// Append constant NULL vectors without decoding or cloning row payloads.
struct NullExtendBatchOperator<'a> {
    input: Box<dyn BatchOperator + 'a>,
    schema: Arc<BatchSchema>,
    extra_columns: usize,
}

impl BatchOperator for NullExtendBatchOperator<'_> {
    batch_wrapper_method!(schema);
    fn properties(&self) -> BatchProperties {
        self.input.properties()
    }
    fn next_batch(&mut self) -> Result<Option<Batch>, QueryError> {
        let Some(batch) = self.input.next_batch()? else {
            return Ok(None);
        };
        let rows = batch.num_rows();
        let selection = batch.selection().cloned();
        let mut columns = batch.into_columns()?;
        columns.extend((0..self.extra_columns).map(|_| ColumnVector::Null { len: rows }));
        let output = Batch::from_owned_columns(self.schema.clone(), columns);
        Ok(Some(match selection {
            Some(selection) => output.with_selection(selection),
            None => output,
        }))
    }
}

fn try_build_null_extend_input<'a>(
    db: &'a Arc<Database>,
    txn: TxnView,
    plan: &'a PhysicalPlan,
) -> Result<Option<VecBatchInput<'a>>, QueryError> {
    let PhysicalPlan::NullExtend { input, table, .. } = plan else {
        return Ok(None);
    };
    let Some(mut compiled) = try_native_vec_input_stream(db, txn, input)? else {
        return Ok(None);
    };
    let fields = super::plan::null_extended_table_fields(table);
    let extra_columns = fields.len();
    compiled
        .template
        .extend(fields.into_iter().map(|meta| PlanField {
            meta: super::plan::intern_col_meta(meta),
            value: Value::Null,
        }));
    let schema = template_to_batch_schema(&compiled.template);
    Ok(Some(VecBatchInput {
        stream: Box::new(NullExtendBatchOperator {
            input: compiled.stream,
            schema,
            extra_columns,
        }),
        template: compiled.template,
    }))
}

/// `Projection(distinct=false)` is a transparent execution node in the
/// legacy plan-row pipeline: final SELECT expression materialisation happens
/// at the protocol/result boundary, while this node only preserves the
/// planner's projection shape. Treat it the same way in a native Batch subtree
/// so a harmless projection between a scan/join and an aggregate does not
/// force a PlanRow round-trip. DISTINCT remains owned by `try_vec_distinct`.
fn try_build_vec_projection_input<'a>(
    db: &'a Arc<Database>,
    txn: TxnView,
    plan: &'a PhysicalPlan,
) -> Result<Option<VecBatchInput<'a>>, QueryError> {
    let PhysicalPlan::Projection {
        input,
        distinct: false,
        ..
    } = plan
    else {
        return Ok(None);
    };
    let compiled = try_native_vec_input_stream(db, txn, input)?;
    if compiled.is_some() {
        VEC_PROJECTION_INPUT_HITS.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }
    Ok(compiled)
}

pub(super) fn final_projection(plan: &PhysicalPlan) -> Option<&[SelectItem]> {
    match plan {
        PhysicalPlan::Projection { projection, .. }
        | PhysicalPlan::StreamingDistinct { projection, .. } => Some(projection),
        PhysicalPlan::Limit { input, .. }
        | PhysicalPlan::Sort { input, .. }
        | PhysicalPlan::IncrementalSort { input, .. }
        | PhysicalPlan::ParallelSort { input, .. }
        | PhysicalPlan::TopK { input, .. }
        | PhysicalPlan::TopWindow { input, .. }
        | PhysicalPlan::ParallelTopK { input, .. }
        | PhysicalPlan::Materialize { input, .. } => final_projection(input),
        _ => None,
    }
}

/// Shared by nested projections and the terminal SELECT cursor. Retain only
/// successful, diagnostic-free columns and own their memory until emission.
pub(super) fn precompute_read_only_projection(
    projection: &[VecResultProjection],
    batch: &Batch,
) -> (Vec<Option<ColumnVector>>, mem_budget::ScopedCharge) {
    let mut retained = batch.memory_charge();
    if matches!(
        crate::engine::executor::vec::config::effective_mode(),
        crate::engine::executor::vec::config::VecMode::Off
    ) {
        return (Vec::new(), retained);
    }
    let cap = crate::engine::executor::vec::evaluated_batch::byte_target().saturating_mul(2);
    let Some(metadata) = projection
        .len()
        .checked_mul(std::mem::size_of::<Option<ColumnVector>>())
    else {
        return (Vec::new(), retained);
    };
    if !retained.add_within(metadata, cap) {
        return (Vec::new(), retained);
    }
    // Session-stable expressions may run ahead only if no sibling can mutate
    // session state. Unknown routines keep their original observable order.
    let read_only = projection.iter().all(|item| match item {
        VecResultProjection::Column(_) => true,
        VecResultProjection::Expr(program) => program.has_column_contract(batch.schema()),
        VecResultProjection::Scalar(_) => false,
    });
    let columns = projection
        .iter()
        .map(|item| {
            let VecResultProjection::Expr(program) = item else {
                return None;
            };
            if !read_only && !program.column_parallel_safe(batch.schema()) {
                return None;
            }
            let reservation = program.column_output_bound(batch)?;
            if !retained.add_within(reservation, cap) {
                return None;
            }
            if let Some(column) = program.try_eval_column(batch) {
                let bytes = column.memory_bytes();
                if bytes <= reservation {
                    retained.release_bytes(reservation - bytes);
                    return Some(column);
                }
            }
            retained.release_bytes(reservation);
            None
        })
        .collect();
    (columns, retained)
}

struct QueryProjectionBatchOperator<'a> {
    db: &'a Arc<Database>,
    txn: TxnView,
    input: Box<dyn BatchOperator + 'a>,
    input_template: Vec<PlanField>,
    projection: Vec<VecResultProjection>,
    row_major: bool,
    schema: Arc<BatchSchema>,
    eval_ctx: super::eval::EvalQueryContext,
}

impl BatchOperator for QueryProjectionBatchOperator<'_> {
    batch_wrapper_method!(schema);

    fn properties(&self) -> BatchProperties {
        BatchProperties {
            ordering: BatchOrdering::Unknown,
            partitioning: self.input.properties().partitioning,
        }
    }

    fn next_batch(&mut self) -> Result<Option<Batch>, QueryError> {
        let Some(batch) = self.input.next_batch()? else {
            return Ok(None);
        };
        let visible = batch.visible_indices().collect::<Vec<_>>();
        let mut output = (0..self.projection.len())
            .map(|_| Vec::with_capacity(visible.len()))
            .collect::<Vec<Vec<Value>>>();
        if self.row_major {
            let (precomputed, _retained) =
                precompute_read_only_projection(&self.projection, &batch);
            let needs_row = self
                .projection
                .iter()
                .enumerate()
                .any(|(index, projection)| {
                    matches!(projection, VecResultProjection::Scalar(_))
                        || matches!(projection, VecResultProjection::Expr(_))
                            && precomputed.get(index).and_then(Option::as_ref).is_none()
                });
            let mut row_values = Vec::with_capacity(batch.columns().len());
            for row in visible {
                if needs_row {
                    row_values.clear();
                    row_values.extend(
                        batch
                            .columns()
                            .iter()
                            .zip(batch.schema().fields())
                            .map(|(column, field)| column.typed_value_at(row, field.column_type)),
                    );
                }
                let view = crate::engine::expr_program::RowView::from_values(&row_values);
                for (index, projection) in self.projection.iter().enumerate() {
                    output[index].push(match projection {
                        VecResultProjection::Column(column) => batch.columns()[*column]
                            .typed_value_at(row, self.input_template[*column].column_type),
                        VecResultProjection::Expr(program) => {
                            if let Some(column) = precomputed.get(index).and_then(Option::as_ref) {
                                column.value_at(row)
                            } else {
                                crate::engine::expr_program::with_eval_scratch(|scratch| {
                                    program.eval_row(&view, scratch)
                                })?
                            }
                        }
                        VecResultProjection::Scalar(expr) => {
                            let plan_row =
                                values_to_plan_row(row_values.clone(), &self.input_template);
                            super::eval::eval_plan_expr_with_db(
                                self.db,
                                self.txn,
                                expr,
                                &plan_row,
                                &mut self.eval_ctx,
                            )?
                        }
                    });
                }
            }
        } else {
            let computed = self
                .projection
                .iter()
                .map(|projection| match projection {
                    VecResultProjection::Column(_) => Ok(None),
                    VecResultProjection::Expr(program) => {
                        expr_batch::eval_to_column(program, &batch).map(Some)
                    }
                    VecResultProjection::Scalar(_) => {
                        unreachable!("scalar query projection must select the row-major lane")
                    }
                })
                .collect::<Result<Vec<_>, QueryError>>()?;
            for row in visible {
                for (index, projection) in self.projection.iter().enumerate() {
                    output[index].push(match projection {
                        VecResultProjection::Column(column) => batch.columns()[*column]
                            .typed_value_at(row, self.input_template[*column].column_type),
                        VecResultProjection::Expr(_) => computed[index]
                            .as_ref()
                            .expect("query projection column missing")
                            .value_at(row),
                        VecResultProjection::Scalar(_) => {
                            unreachable!("scalar query projection must select the row-major lane")
                        }
                    });
                }
            }
        }
        let row_count = output.first().map_or(0, Vec::len);
        Ok(Some(Batch::with_row_count(
            self.schema.clone(),
            output.into_iter().map(ColumnVector::from_values).collect(),
            row_count,
        )))
    }
}

fn query_projection_template(
    projection: &[SelectItem],
    input: &[PlanField],
    lowered: &[VecResultProjection],
) -> Option<Vec<PlanField>> {
    let mut output = Vec::with_capacity(lowered.len());
    let mut lowered_index = 0usize;
    for item in projection {
        match item {
            SelectItem::Wildcard => {
                for field in input.iter().filter(|field| !field.invisible) {
                    output.push(field.clone());
                    lowered_index += 1;
                }
            }
            SelectItem::QualifiedWildcard(qualifier) => {
                for field in input.iter().filter(|field| {
                    !field.invisible
                        && (field.table.eq_ignore_ascii_case(qualifier)
                            || field
                                .alias
                                .as_deref()
                                .is_some_and(|alias| alias.eq_ignore_ascii_case(qualifier)))
                }) {
                    output.push(field.clone());
                    lowered_index += 1;
                }
            }
            SelectItem::Expr { expr, alias } => {
                let lowered = lowered.get(lowered_index)?;
                let name = alias
                    .clone()
                    .unwrap_or_else(|| crate::engine::expr::expr_name(expr));
                let (table, table_alias, column_type, source) = match lowered {
                    VecResultProjection::Column(index) => {
                        let field = input.get(*index)?;
                        (
                            field.table.clone(),
                            field.alias.clone(),
                            field.column_type,
                            field.source.clone(),
                        )
                    }
                    VecResultProjection::Expr(program) => {
                        let descriptor = program.result_type();
                        let mut source = crate::types::ColumnSource::default();
                        descriptor.apply_to_source(&mut source);
                        (String::new(), None, descriptor.column_type(), source)
                    }
                    VecResultProjection::Scalar(expr) => {
                        let resolve_column = |expr: &Expr| {
                            let field = input.get(expr_column_index_in_template(expr, input)?)?;
                            Some(crate::engine::expr_type::ExprType::from_column_type(
                                field.column_type,
                                &field.source,
                            ))
                        };
                        let resolve_placeholder = |_: usize| None;
                        let context = crate::engine::expr_type::TypeContext::context_free(
                            &resolve_column,
                            &resolve_placeholder,
                        );
                        let descriptor = crate::engine::expr_type::infer_expr_type(expr, &context);
                        let mut source = crate::types::ColumnSource::default();
                        descriptor.apply_to_source(&mut source);
                        (String::new(), None, descriptor.column_type(), source)
                    }
                };
                output.push(PlanField {
                    meta: super::plan::intern_col_meta(ColMeta {
                        invisible: false,
                        table,
                        alias: table_alias,
                        column: name,
                        column_type,
                        source,
                    }),
                    value: Value::Null,
                });
                lowered_index += 1;
            }
        }
    }
    (lowered_index == lowered.len()).then_some(output)
}

fn unify_setop_templates(
    left: Vec<PlanField>,
    right: Vec<PlanField>,
) -> Result<Vec<PlanField>, QueryError> {
    if left.len() != right.len() {
        return Err(crate::storage::error(
            crate::error::catalog::ER_WRONG_NUMBER_OF_COLUMNS_IN_SELECT,
            "the used SELECT statements have a different number of columns",
        ));
    }
    let mut left_columns = left
        .iter()
        .map(|field| crate::types::Column {
            name: field.column.clone(),
            column_type: field.column_type,
            source: field.source.clone(),
        })
        .collect::<Vec<_>>();
    let mut right_columns = right
        .iter()
        .map(|field| crate::types::Column {
            name: field.column.clone(),
            column_type: field.column_type,
            source: field.source.clone(),
        })
        .collect::<Vec<_>>();
    crate::engine::executor::dispatch::unify_setop_declared_column_metadata(
        &mut left_columns,
        &mut right_columns,
    )?;
    Ok(left
        .into_iter()
        .zip(left_columns)
        .map(|(field, column)| PlanField {
            meta: super::plan::intern_col_meta(ColMeta {
                invisible: field.invisible,
                table: field.table.clone(),
                alias: field.alias.clone(),
                column: column.name,
                column_type: column.column_type,
                source: column.source,
            }),
            value: Value::Null,
        })
        .collect())
}

/// Pure metadata/capability lowering for a relation-producing query graph.
/// It must remain free of storage access and worker creation: callers use it
/// to select the BatchOperator protocol before opening any input. The runtime
/// builder is then invoked exactly once and a capability mismatch is treated
/// as an internal error rather than replaying the statement in another
/// executor.
pub(crate) fn preflight_query_output_supported(plan: &PhysicalPlan) -> Result<bool, QueryError> {
    Ok(preflight_query_output_template(plan)?.is_some())
}

#[cfg(test)]
pub(crate) static NATIVE_OUTPUT_PREFLIGHT_REUSES: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);

fn preflight_query_output_template(
    plan: &PhysicalPlan,
) -> Result<Option<Vec<PlanField>>, QueryError> {
    if matches!(plan, PhysicalPlan::SetOp { .. }) {
        crate::util::stack::with_stack(
            crate::engine::executor::mem_budget::ControlledMemoryCategory::Executor,
            || preflight_query_output_template_checked(plan),
        )?
    } else {
        preflight_query_output_template_checked(plan)
    }
}

fn preflight_query_output_template_checked(
    plan: &PhysicalPlan,
) -> Result<Option<Vec<PlanField>>, QueryError> {
    match plan {
        PhysicalPlan::SetOp { left, right, .. } => {
            let Some(left) = preflight_query_output_template(left)? else {
                return Ok(None);
            };
            let Some(right) = preflight_query_output_template(right)? else {
                return Ok(None);
            };
            return unify_setop_templates(left, right).map(Some);
        }
        PhysicalPlan::Limit { input, .. }
        | PhysicalPlan::Materialize { input, .. }
        | PhysicalPlan::Exchange { input, .. } => {
            return preflight_query_output_template(input);
        }
        PhysicalPlan::Sort {
            input, order_by, ..
        }
        | PhysicalPlan::IncrementalSort {
            input, order_by, ..
        }
        | PhysicalPlan::ParallelSort {
            input, order_by, ..
        } => {
            if topn_key_preflight(input, order_by)?.is_none() {
                return Ok(None);
            }
            return preflight_query_output_template(input);
        }
        PhysicalPlan::TopK {
            input,
            order_by,
            dedup_projection,
            ..
        }
        | PhysicalPlan::ParallelTopK {
            input,
            order_by,
            dedup_projection,
            ..
        } => {
            if dedup_projection.is_some() {
                return Ok(None);
            }
            if topn_key_preflight(input, order_by)?.is_none() {
                return Ok(None);
            }
            return preflight_query_output_template(input);
        }
        PhysicalPlan::TopWindow {
            input, order_by, ..
        } => {
            if topn_key_preflight(input, order_by)?.is_none() {
                return Ok(None);
            }
            return preflight_query_output_template(input);
        }
        _ => {}
    }

    if let Some(projection) = final_projection(plan) {
        if !should_route_result_to_batch(plan, projection) {
            return Ok(None);
        }
        let Some((planned_template, lowered, _)) =
            preflight_result_projection(plan, projection, None)
        else {
            return Ok(None);
        };
        return Ok(query_projection_template(
            projection,
            &planned_template,
            &lowered,
        ));
    }

    let wildcard = [SelectItem::Wildcard];
    if !should_route_result_to_batch(plan, &wildcard) {
        return Ok(None);
    }
    let Some((planned_template, lowered, _)) = preflight_result_projection(plan, &wildcard, None)
    else {
        return Ok(None);
    };
    Ok(query_projection_template(
        &wildcard,
        &planned_template,
        &lowered,
    ))
}

/// Compile a SELECT branch as a relation-producing child. Unlike the
/// protocol boundary, a parent SetOp/Materialize must see the SELECT's actual
/// projected columns rather than the transparent legacy Projection marker.
fn try_build_query_output_batch<'a>(
    db: &'a Arc<Database>,
    txn: TxnView,
    plan: &'a PhysicalPlan,
) -> Result<Option<VecBatchInput<'a>>, QueryError> {
    let Some(projection) = final_projection(plan) else {
        return try_native_vec_input_stream(db, txn, plan);
    };
    let Some((planned_template, lowered, row_major)) =
        preflight_result_projection(plan, projection, None)
    else {
        return Ok(None);
    };
    let Some(input) = try_native_vec_input_stream(db, txn, plan)? else {
        return Ok(None);
    };
    let Some(template) = query_projection_template(projection, &planned_template, &lowered) else {
        return Ok(None);
    };
    let schema = template_to_batch_schema(&template);
    Ok(Some(VecBatchInput {
        stream: Box::new(QueryProjectionBatchOperator {
            db,
            txn,
            input: input.stream,
            input_template: planned_template,
            projection: lowered,
            row_major,
            schema,
            eval_ctx: super::eval::EvalQueryContext::default(),
        }),
        template,
    }))
}

/// Successful transparent Projection nodes inside a native Batch subtree.
/// This is a compile-time hit counter used by differential tests to ensure the
/// intended boundary was crossed rather than silently taking the row path.
pub(crate) static VEC_PROJECTION_INPUT_HITS: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);

struct DistinctBatchOperator<'a> {
    upstream: Box<dyn BatchOperator + 'a>,
    schema: Arc<BatchSchema>,
    properties: BatchProperties,
    data_dir: std::path::PathBuf,
    key_columns: Vec<usize>,
    output: Option<Box<dyn BatchOperator>>,
}

#[cfg(test)]
pub(crate) static DIRECT_ROLLUP_HITS: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);

/// Adjacent-key DISTINCT for an input whose physical plan already guarantees
/// ordering by every DISTINCT key. It carries only the previous encoded key
/// across batches and returns the original column vectors with a selection,
/// so memory is O(key width) and input ordering is preserved.
struct StreamingDistinctBatchOperator<'a> {
    upstream: Box<dyn BatchOperator + 'a>,
    schema: Arc<BatchSchema>,
    properties: BatchProperties,
    key_columns: Vec<usize>,
    last_key: Vec<u8>,
    has_last_key: bool,
    key_scratch: Vec<u8>,
    aggregate_plan: Option<&'a PhysicalPlan>,
}

impl StreamingDistinctBatchOperator<'_> {
    fn encode_key(&mut self, batch: &Batch, row: usize) {
        self.key_scratch.clear();
        for &column in &self.key_columns {
            crate::engine::executor::vec::key_encode::encode_from_column_row_with_collation(
                &batch.columns()[column],
                row,
                self.schema.fields()[column].source.collation_id,
                &mut self.key_scratch,
            );
        }
    }
}

impl BatchOperator for StreamingDistinctBatchOperator<'_> {
    batch_wrapper_method!(schema);

    batch_wrapper_method!(properties);

    fn next_batch(&mut self) -> Result<Option<Batch>, QueryError> {
        loop {
            crate::engine::cancel::check_cancelled()?;
            let Some(batch) = self.upstream.next_batch()? else {
                return Ok(None);
            };
            let visible = batch.visible_rows();
            let mut kept = Vec::with_capacity(visible);
            for row in batch.visible_indices() {
                self.encode_key(&batch, row);
                if self.has_last_key && self.key_scratch == self.last_key {
                    continue;
                }
                self.last_key.clear();
                self.last_key.extend_from_slice(&self.key_scratch);
                self.has_last_key = true;
                kept.push(row as u32);
            }
            if let Some(plan) = self.aggregate_plan {
                analyze_stats::set_streaming(plan, true);
                analyze_stats::add_peak_memory(
                    plan,
                    (self.last_key.capacity()
                        + self.key_scratch.capacity()
                        + kept.capacity() * std::mem::size_of::<u32>()) as u64,
                );
                analyze_stats::add_aggregate_runtime(
                    plan,
                    "ordered_distinct",
                    visible as u64,
                    0,
                    0,
                );
            }
            if kept.is_empty() {
                continue;
            }
            if kept.len() == visible {
                return Ok(Some(batch));
            }
            return Ok(Some(batch.with_selection(Selection::Indices(kept))));
        }
    }
}

impl DistinctBatchOperator<'_> {
    fn initialize(&mut self) -> Result<(), QueryError> {
        if self.output.is_some() {
            return Ok(());
        }
        let output_columns: Vec<usize> = (0..self.schema.len()).collect();
        self.output = Some(vec_distinct::run_distinct_stream(
            self.upstream.as_mut(),
            &self.key_columns,
            &output_columns,
            &self.data_dir,
        )?);
        Ok(())
    }
}

impl BatchOperator for DistinctBatchOperator<'_> {
    batch_wrapper_method!(schema);

    batch_wrapper_method!(properties);

    fn next_batch(&mut self) -> Result<Option<Batch>, QueryError> {
        self.initialize()?;
        self.output
            .as_mut()
            .expect("distinct initialized")
            .next_batch()
    }
}

fn try_build_vec_distinct_input<'a>(
    db: &'a Arc<Database>,
    txn: TxnView,
    plan: &'a PhysicalPlan,
) -> Result<Option<VecBatchInput<'a>>, QueryError> {
    let (input, projection, streaming) = match plan {
        PhysicalPlan::Projection {
            input,
            projection,
            distinct: true,
            ..
        } => (input.as_ref(), projection.as_slice(), false),
        PhysicalPlan::StreamingDistinct {
            input, projection, ..
        } => (input.as_ref(), projection.as_slice(), true),
        _ => return Ok(None),
    };
    let Some((planned_template, key_columns)) = distinct_key_preflight(input, projection) else {
        return Ok(None);
    };
    let Some(VecBatchInput { stream, template }) = try_native_vec_input_stream(db, txn, input)?
    else {
        return Ok(None);
    };
    if template.len() != planned_template.len() {
        return Err(crate::storage::error(
            crate::error::catalog::ER_UNKNOWN_ERROR,
            "batch DISTINCT schema diverged from its preflight metadata",
        ));
    }
    let schema = stream.schema().clone();
    // Stable first-occurrence DISTINCT only removes rows, so it preserves the
    // ordering and partitioning guarantees published by its input, including
    // after hash-partition spill.
    let properties = stream.properties();
    if streaming {
        return Ok(Some(VecBatchInput {
            stream: Box::new(StreamingDistinctBatchOperator {
                upstream: stream,
                schema,
                properties,
                key_columns,
                last_key: Vec::new(),
                has_last_key: false,
                key_scratch: Vec::new(),
                aggregate_plan: None,
            }),
            template,
        }));
    }
    Ok(Some(VecBatchInput {
        stream: Box::new(DistinctBatchOperator {
            upstream: stream,
            schema,
            properties,
            data_dir: db.data_dir().to_path_buf(),
            key_columns,
            output: None,
        }),
        template,
    }))
}

/// Streaming LIMIT/OFFSET over a native Batch source. It never asks the
/// upstream for another batch after the requested window is complete and
/// carries partial-batch windows as a zero-copy selection vector.
struct LimitBatchStream<'a> {
    upstream: Box<dyn BatchOperator + 'a>,
    schema: Arc<BatchSchema>,
    properties: BatchProperties,
    skip: usize,
    remaining: usize,
}

impl BatchOperator for LimitBatchStream<'_> {
    batch_wrapper_method!(schema);

    batch_wrapper_method!(properties);

    batch_wrapper_method!(install_runtime_filter);

    fn next_batch(&mut self) -> Result<Option<Batch>, QueryError> {
        use crate::engine::executor::vec::batch::Selection;
        if self.remaining == 0 {
            return Ok(None);
        }
        while let Some(batch) = self.upstream.next_batch()? {
            crate::engine::cancel::check_cancelled()?;
            let visible = batch.visible_rows();
            if self.skip >= visible {
                self.skip -= visible;
                continue;
            }
            if self.skip == 0 && self.remaining >= visible {
                self.remaining = self.remaining.saturating_sub(visible);
                return Ok(Some(batch));
            }
            let take = self.remaining.min(visible - self.skip);
            let selection = match batch.selection() {
                Some(Selection::Range { start, .. }) => Selection::Range {
                    start: start.saturating_add(self.skip as u32),
                    len: take as u32,
                },
                Some(selection) => Selection::Indices(
                    selection
                        .iter()
                        .skip(self.skip)
                        .take(take)
                        .map(|idx| idx as u32)
                        .collect(),
                ),
                None => Selection::Range {
                    start: self.skip as u32,
                    len: take as u32,
                },
            };
            self.skip = 0;
            self.remaining = self.remaining.saturating_sub(take);
            if selection.count() == 0 {
                continue;
            }
            return Ok(Some(batch.with_selection(selection)));
        }
        Ok(None)
    }
}

fn try_build_vec_limit_input<'a>(
    db: &'a Arc<Database>,
    txn: TxnView,
    plan: &'a PhysicalPlan,
) -> Result<Option<VecBatchInput<'a>>, QueryError> {
    let PhysicalPlan::Limit {
        input,
        limit,
        offset,
        binding,
        ..
    } = plan
    else {
        return Ok(None);
    };
    let skip = binding.resolve_offset(*offset).unwrap_or(0);
    let remaining = binding.resolve_limit(*limit).unwrap_or(usize::MAX);
    let target = skip.saturating_add(remaining);
    // A small unordered LIMIT should not make the leaf fill the ordinary
    // sixteen-page throughput morsel before the window can stop it. This is a
    // batch-size/prefetch hint only: it does not alter access choice or plan
    // topology and is consumed while the scan source is constructed.
    let row_target = (target <= DEFAULT_BATCH_ROWS && scan_row_goal_pushdown_safe(input))
        .then_some(target.max(1));
    let Some(VecBatchInput { stream, template }) =
        with_scan_limit_hint(row_target, || try_native_vec_input_stream(db, txn, input))?
    else {
        return Ok(None);
    };
    let schema = stream.schema().clone();
    let properties = stream.properties();
    Ok(Some(VecBatchInput {
        stream: Box::new(LimitBatchStream {
            upstream: stream,
            schema,
            properties,
            skip,
            remaining,
        }),
        template,
    }))
}

/// A LIMIT row goal may reach a streaming scan through transparent wrappers,
/// but must stop at blocking operators such as Sort/TopK/Aggregate. Pushing a
/// one-row hint through TopK made ORDER BY RAND() scan a complete relation in
/// one-row source batches even though the sort cannot stop early.
fn scan_row_goal_pushdown_safe(plan: &PhysicalPlan) -> bool {
    match plan {
        PhysicalPlan::SeqScan { .. }
        | PhysicalPlan::ZoneMapScan { .. }
        | PhysicalPlan::IndexLookup { .. }
        | PhysicalPlan::IndexMultiLookup { .. }
        | PhysicalPlan::IndexRangeScan { .. }
        | PhysicalPlan::IndexFastScan { .. }
        | PhysicalPlan::ParallelSeqScan { .. }
        | PhysicalPlan::ParallelZoneMapScan { .. }
        | PhysicalPlan::ParallelIndexRangeScan { .. }
        | PhysicalPlan::ParallelIndexFastScan { .. } => true,
        PhysicalPlan::Projection { input, .. } | PhysicalPlan::Filter { input, .. } => {
            scan_row_goal_pushdown_safe(input)
        }
        PhysicalPlan::Apply { outer, .. } => scan_row_goal_pushdown_safe(outer),
        _ => false,
    }
}

/// A `BatchOperator` that applies a compiled predicate to every upstream batch,
/// attaching the surviving rows as a `Selection::Mask` (the same convention the
/// hash-join residual uses — see `apply_residual` in `vec/hash_join.rs`). No
/// row data is copied: the mask rides along on the batch and downstream
/// operators iterate only the visible rows. Empty batches are skipped.
struct FilterBatchStream<'a> {
    upstream: Box<dyn BatchOperator + 'a>,
    program: Arc<crate::engine::executor::vec::expr_batch::BatchExprProgram>,
    schema: Arc<BatchSchema>,
    properties: BatchProperties,
    analyze_plan: Option<&'a PhysicalPlan>,
    analyze_recorded: bool,
}

impl FilterBatchStream<'_> {
    fn record_expression_stats(&mut self) {
        if self.analyze_recorded {
            return;
        }
        self.analyze_recorded = true;
        let Some(plan) = self.analyze_plan else {
            return;
        };
        let (native_rows, fallback_rows) = self.program.expression_rows();
        let mode = match (native_rows > 0, fallback_rows > 0) {
            (true, false) => "decoded",
            (false, true) => "scalar",
            (true, true) => "hybrid",
            (false, false) => "decoded",
        };
        analyze_stats::add_expression_runtime(plan, mode, native_rows, fallback_rows);
    }
}

impl Drop for FilterBatchStream<'_> {
    fn drop(&mut self) {
        self.record_expression_stats();
    }
}

/// Repack sparse filtered scan batches before downstream compute/aggregate
/// operators. The scan filter itself remains vectorized over the original
/// physical batch; only surviving rows are copied, and the pending payload is
/// bounded below 1.5 ordinary batches because each appended input is at most
/// half full. Dense batches keep the zero-copy path.
struct SparseSelectionCoalesceStream<'a> {
    upstream: Box<dyn BatchOperator + 'a>,
    schema: Arc<BatchSchema>,
    properties: BatchProperties,
    pending_columns: Option<Vec<ColumnVector>>,
    pending_rows: usize,
    deferred: Option<Batch>,
}

impl<'a> SparseSelectionCoalesceStream<'a> {
    fn new(upstream: Box<dyn BatchOperator + 'a>) -> Self {
        let schema = upstream.schema().clone();
        let properties = upstream.properties();
        Self {
            upstream,
            schema,
            properties,
            pending_columns: None,
            pending_rows: 0,
            deferred: None,
        }
    }

    fn take_pending(&mut self) -> Option<Batch> {
        if self.pending_rows == 0 {
            return None;
        }
        let rows = std::mem::take(&mut self.pending_rows);
        let columns = self
            .pending_columns
            .take()
            .expect("sparse batch columns accompany pending rows");
        Some(Batch::with_row_count(self.schema.clone(), columns, rows))
    }
}

impl BatchOperator for SparseSelectionCoalesceStream<'_> {
    batch_wrapper_method!(schema);

    batch_wrapper_method!(properties);

    batch_wrapper_method!(install_runtime_filter);

    fn next_batch(&mut self) -> Result<Option<Batch>, QueryError> {
        if let Some(batch) = self.deferred.take() {
            return Ok(Some(batch));
        }
        loop {
            let Some(batch) = self.upstream.next_batch()? else {
                return Ok(self.take_pending());
            };
            let visible_rows = batch.visible_rows();
            let sparse =
                batch.selection().is_some() && visible_rows.saturating_mul(2) <= batch.num_rows();
            if !sparse {
                if self.pending_rows > 0 {
                    self.deferred = Some(batch);
                    return Ok(self.take_pending());
                }
                return Ok(Some(batch));
            }
            if visible_rows == 0 {
                continue;
            }
            let rows = batch.visible_indices().collect::<Vec<_>>();
            let pending = self.pending_columns.get_or_insert_with(|| {
                batch
                    .columns()
                    .iter()
                    .map(|_| ColumnVector::Null { len: 0 })
                    .collect()
            });
            for (output, input) in pending.iter_mut().zip(batch.columns()) {
                output.append_rows_from(input, &rows);
            }
            self.pending_rows = self.pending_rows.saturating_add(visible_rows);
            if self.pending_rows >= DEFAULT_BATCH_ROWS {
                return Ok(self.take_pending());
            }
        }
    }
}

/// Zero-copy column pruning/reordering inside a batch pipeline. Column vectors
/// are ref-count-free owned buffers today, so this clones only the selected
/// vector handles/buffers; it still avoids rebuilding row values or PlanRows.
struct ProjectColumnsBatchStream<'a> {
    upstream: Box<dyn BatchOperator + 'a>,
    columns: Vec<usize>,
    schema: Arc<BatchSchema>,
}

impl BatchOperator for ProjectColumnsBatchStream<'_> {
    fn prefer_key_locality(&mut self, columns: &[usize]) -> bool {
        let Some(columns) = columns
            .iter()
            .map(|column| self.columns.get(*column).copied())
            .collect::<Option<Vec<_>>>()
        else {
            return false;
        };
        self.upstream.prefer_key_locality(&columns)
    }

    batch_wrapper_method!(schema);

    fn properties(&self) -> BatchProperties {
        BatchProperties {
            ordering: BatchOrdering::Unknown,
            partitioning: self.upstream.properties().partitioning,
        }
    }

    batch_wrapper_method!(install_runtime_filter);

    fn consume_into_hash_aggregate(
        &mut self,
        aggregate: &mut vec_hash_agg::HashAggregateCore,
    ) -> Result<bool, QueryError> {
        let Some(saved) = aggregate.remap_direct_input_columns(&self.columns) else {
            return Ok(false);
        };
        let result = self.upstream.consume_into_hash_aggregate(aggregate);
        aggregate.restore_direct_input_columns(saved);
        result
    }

    fn consume_computed_into_hash_aggregate(
        &mut self,
        aggregate: &mut vec_hash_agg::HashAggregateCore,
        expressions: &[Expr],
    ) -> Result<bool, QueryError> {
        let inner_width = self.upstream.schema().len();
        let mut mapping = self.columns.clone();
        mapping.extend((0..expressions.len()).map(|index| inner_width + index));
        let Some(saved) = aggregate.remap_direct_input_columns(&mapping) else {
            return Ok(false);
        };
        let result = self
            .upstream
            .consume_computed_into_hash_aggregate(aggregate, expressions);
        aggregate.restore_direct_input_columns(saved);
        result
    }

    fn next_batch(&mut self) -> Result<Option<Batch>, QueryError> {
        let Some(batch) = self.upstream.next_batch()? else {
            return Ok(None);
        };
        let selection = batch.selection().cloned();
        let row_count = batch.num_rows();
        let columns = self
            .columns
            .iter()
            .map(|&index| batch.columns()[index].clone())
            .collect();
        let mut projected = Batch::with_row_count(self.schema.clone(), columns, row_count);
        if let Some(selection) = selection {
            projected = projected.with_selection(selection);
        }
        Ok(Some(projected))
    }
}

/// Semantic-complete scalar predicate lane for expressions that cannot be
/// represented by ExprProgram (notably correlated subqueries / EXISTS / IN).
/// It evaluates only the current batch and returns a selection vector; the
/// upstream graph remains open exactly once, so an Apply-shaped predicate can
/// never trigger a scan replay or whole-query materialization.
struct ScalarFilterBatchStream<'a> {
    db: &'a Arc<Database>,
    txn: TxnView,
    upstream: Box<dyn BatchOperator + 'a>,
    predicate: Cow<'a, Expr>,
    template: Vec<PlanField>,
    schema: Arc<BatchSchema>,
    properties: BatchProperties,
    eval_ctx: super::eval::EvalQueryContext,
    analyze_plan: Option<&'a PhysicalPlan>,
    scalar_rows: u64,
    analyze_recorded: bool,
}

impl ScalarFilterBatchStream<'_> {
    fn record_expression_stats(&mut self) {
        if self.analyze_recorded {
            return;
        }
        self.analyze_recorded = true;
        if let Some(plan) = self.analyze_plan {
            analyze_stats::add_expression_runtime(plan, "scalar", 0, self.scalar_rows);
        }
    }
}

impl Drop for ScalarFilterBatchStream<'_> {
    fn drop(&mut self) {
        self.record_expression_stats();
    }
}

impl BatchOperator for ScalarFilterBatchStream<'_> {
    batch_wrapper_method!(prefer_key_locality);

    batch_wrapper_method!(schema);

    batch_wrapper_method!(properties);

    batch_wrapper_method!(install_runtime_filter);

    batch_wrapper_method!(install_runtime_index_bound);

    fn next_batch(&mut self) -> Result<Option<Batch>, QueryError> {
        while let Some(batch) = self.upstream.next_batch()? {
            crate::engine::cancel::check_cancelled()?;
            let mut mask =
                crate::engine::executor::vec::bitmap::Bitmap::new_all_invalid(batch.num_rows());
            let mut evaluated = 0u64;
            for index in batch.visible_indices() {
                let row = values_to_plan_row(batch.row(index), &self.template);
                if super::eval::eval_plan_bool_with_db(
                    self.db,
                    self.txn,
                    self.predicate.as_ref(),
                    &row,
                    &mut self.eval_ctx,
                )? {
                    mask.set_true(index);
                }
                evaluated = evaluated.saturating_add(1);
            }
            self.scalar_rows = self.scalar_rows.saturating_add(evaluated);
            let selection = Selection::Mask(mask);
            if selection.count() == 0 {
                continue;
            }
            return Ok(Some(batch.with_selection(selection)));
        }
        self.record_expression_stats();
        Ok(None)
    }
}

/// Native vec leaf for a `Filter` over a vec-eligible input: build the input as
/// a columnar `BatchOperator` and apply the predicate per-batch as a selection
/// mask, so a `Filter` sitting between a vec join and a vec TopN/aggregate keeps
/// the pipeline columnar. The join's wide output is never materialised as
/// `PlanRow`s — only the rows that survive the filter (and, downstream, the
/// TopN's top-k) ever leave the vec world. This is what removes q78's ~489 k
/// wide-row materialisation: `TopK → Filter → ss⋈ws⋈cs` runs end-to-end vec.
///
/// Gated on (a) the input having a native batch operator and (b) the predicate
/// compiling through `BatchExprProgram`. Typed leaves use column kernels;
/// long-tail leaves stay inside this batch operator and invoke the scalar
/// `ExprProgram` oracle only for their selected rows.
fn try_build_vec_filter_input<'a>(
    db: &'a Arc<Database>,
    txn: TxnView,
    plan: &'a PhysicalPlan,
) -> Result<Option<VecBatchInput<'a>>, QueryError> {
    let (input, predicate, is_apply) = match plan {
        PhysicalPlan::Filter {
            input, predicate, ..
        } => (input.as_ref(), predicate, false),
        PhysicalPlan::Apply {
            outer, predicate, ..
        } => (outer.as_ref(), predicate, true),
        _ => return Ok(None),
    };
    // Compile the predicate from immutable plan metadata before opening the
    // input subtree. In particular, a HashJoin child can start scan workers
    // while constructing its BatchOperator; discovering an unsupported
    // expression after that point would violate the no-I/O fallback contract.
    let mut planned_metas = Vec::new();
    super::plan::collect_plan_fields(input, &mut planned_metas);
    let planned_template: Vec<PlanField> = planned_metas
        .into_iter()
        .map(|meta| PlanField {
            meta: super::plan::intern_col_meta(meta),
            value: Value::Null,
        })
        .collect();
    let schema = template_to_batch_schema(&planned_template);
    let resolved_predicate = if is_apply {
        let outer_template = PlanRow::from_fields(planned_template.clone());
        super::eval::resolve_batch_constant_scalar_subqueries(db, txn, predicate, &outer_template)?
    } else {
        None
    };
    let runtime_index_bound = resolved_predicate
        .as_ref()
        .map(|resolved| resolved.expr.clone());
    let (program, scalar_predicate) = match resolved_predicate {
        Some(resolved) if resolved.charge.is_none() => {
            let program = crate::engine::executor::vec::expr_batch::BatchExprProgram::compile_runtime_constants(
                &resolved.expr,
                &schema,
                None,
            )?;
            let scalar = program.is_none().then_some(Cow::Owned(resolved.expr));
            (program, scalar)
        }
        Some(resolved) => (
            crate::engine::executor::vec::expr_batch::BatchExprProgram::compile_runtime_constants(
                &resolved.expr,
                &schema,
                resolved.charge,
            )?,
            None,
        ),
        None => (
            crate::engine::executor::vec::expr_batch::BatchExprProgram::compile(
                predicate, &schema,
            )?,
            None,
        ),
    };
    let Some(VecBatchInput {
        mut stream,
        template,
    }) = try_native_vec_input_stream(db, txn, input)?
    else {
        return Ok(None);
    };
    if let Some(bound) = runtime_index_bound.as_ref() {
        let _ = stream.install_runtime_index_bound(bound);
    }
    let properties = stream.properties();
    if template.is_empty() {
        return Ok(Some(VecBatchInput { stream, template }));
    }
    debug_assert_eq!(template.len(), planned_template.len());
    VEC_FILTER_INPUT_HITS.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let stream: Box<dyn BatchOperator + 'a> = if let Some(program) = program {
        Box::new(FilterBatchStream {
            upstream: stream,
            program,
            schema,
            properties,
            analyze_plan: Some(plan),
            analyze_recorded: false,
        })
    } else {
        Box::new(ScalarFilterBatchStream {
            db,
            txn,
            upstream: stream,
            predicate: scalar_predicate.unwrap_or(Cow::Borrowed(predicate)),
            template: template.clone(),
            schema,
            properties,
            eval_ctx: super::eval::EvalQueryContext::default(),
            analyze_plan: Some(plan),
            scalar_rows: 0,
            analyze_recorded: false,
        })
    };
    Ok(Some(VecBatchInput { stream, template }))
}

/// Count of times the native vec `Filter`-over-join leaf engaged (once per
/// qualifying `Filter` node, not per row). Read by tests to prove the columnar
/// filter fired rather than the query silently taking the row pipeline. Mirrors
/// `CTE_BATCH_SCAN_HITS` / `VEC_HASH_JOIN_HITS`.
pub(crate) static VEC_FILTER_INPUT_HITS: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);

/// Bounded adapter for the existing batched/MRR INLJ probe source. The INLJ
/// owns one left chunk at a time and never materializes the complete join;
/// rows are transposed immediately into a native Batch so every downstream
/// operator remains in the common pull pipeline. `conversion_rows` makes this
/// temporary compatibility boundary explicit in EXPLAIN ANALYZE until the
/// index MRR API accepts column vectors directly.
struct InljMrrBatchOperator<'a> {
    input: Box<dyn super::plan::PlanStream + 'a>,
    schema: Arc<BatchSchema>,
    finished: bool,
}

/// One bounded Batch→PlanRow boundary at the bottom of an INLJ chain. Nested
/// INLJs consume the resulting row stream directly, avoiding a PlanRow→Batch→
/// PlanRow round trip at every level while the hash-join source remains native.
struct BatchPlanRowStream<'a> {
    input: Box<dyn BatchOperator + 'a>,
    template: Vec<PlanField>,
    current: Option<Batch>,
    visible: Vec<usize>,
    position: usize,
    finished: bool,
}

impl super::plan::PlanStream for BatchPlanRowStream<'_> {
    fn next(&mut self) -> Result<Option<PlanRow>, QueryError> {
        loop {
            if let Some(batch) = self.current.as_ref() {
                if let Some(&row) = self.visible.get(self.position) {
                    self.position += 1;
                    return Ok(Some(values_to_plan_row(batch.row(row), &self.template)));
                }
            }
            if self.finished {
                return Ok(None);
            }
            let Some(batch) = self.input.next_batch()? else {
                self.finished = true;
                return Ok(None);
            };
            self.visible.clear();
            self.visible.extend(batch.visible_indices());
            analyze_stats::add_pending_conversion_rows(self.visible.len() as u64);
            self.position = 0;
            self.current = Some(batch);
        }
    }

    fn install_runtime_filter(&mut self, filter: crate::planner::RuntimeFilter) -> bool {
        self.input.install_runtime_filter(filter)
    }
}

/// Row-shaped compatibility kernel for one INLJ level. Its input may be
/// another instance of this type, so an entire consecutive INLJ chain keeps
/// `PlanRow`s only inside one bounded MRR chunk and converts to Batch once at
/// the final boundary.
struct NativeInljPlanRowStream<'a> {
    db: &'a Arc<Database>,
    txn: TxnView,
    left_plan: &'a PhysicalPlan,
    left: Box<dyn super::plan::PlanStream + 'a>,
    right_table: crate::planner::PlanTable,
    right_index: crate::storage::IndexSchema,
    join_type: crate::parser::JoinType,
    lookup_plan: super::plan::IndexLookupPlan,
    right_covering: bool,
    right_projected_columns: Option<Arc<Vec<usize>>>,
    right_runtime_filters: Vec<crate::planner::RuntimeFilter>,
    shared_mrr: Option<super::plan::InljMrrSharedCaches>,
    current: std::vec::IntoIter<PlanRow>,
    current_charge: Option<InljChunkCharge>,
    unique_bounded_output: bool,
    left_finished: bool,
    chunk_rows: usize,
}

/// Bounded adapter for a consecutive SEMI/ANTI INLJ pair that the row executor
/// can fuse into one same-index probe. The original adapter drained the entire
/// deepest-left input before its first output row. On wide Q21-shaped plans
/// that retained millions of `PlanRow`s outside the query budget even though
/// the surrounding vector graph was bounded. Keep fusion local to one
/// resource-sized chunk and retain a conservative charge until that chunk's
/// output has been consumed.
struct FusedInljPlanRowStream<'a> {
    db: &'a Arc<Database>,
    txn: TxnView,
    plan: &'a PhysicalPlan,
    left: Box<dyn super::plan::PlanStream + 'a>,
    current: std::vec::IntoIter<PlanRow>,
    current_charge: Option<InljChunkCharge>,
    left_finished: bool,
    chunk_rows: usize,
}

pub(super) fn plan_row_owned_heap_bytes(row: &PlanRow) -> usize {
    use crate::engine::executor::memory_footprint::AllocatedBytes;

    let mut bytes = row
        .fields
        .capacity()
        .saturating_mul(std::mem::size_of::<PlanField>());
    for field in &row.fields {
        bytes = bytes.saturating_add(field.value.allocated_bytes());
    }
    if let Some(group) = &row.group {
        bytes = bytes.saturating_add(
            group
                .capacity()
                .saturating_mul(std::mem::size_of::<PlanRow>()),
        );
        for nested in group {
            bytes = bytes.saturating_add(plan_row_owned_heap_bytes(nested));
        }
    }
    // `agg_values` and `schema` are shared Arcs whose allocations follow the
    // producing operator; this adapter only owns their inline Arc handles.
    bytes
}

/// Short-lived probe work shares the executor arbiter with blocking producers.
/// A producer may borrow past the query soft watermark while still fitting in
/// that pool; using only `MemBudget::remaining` here then serializes MRR into
/// one-row probes. Keep actual capacities charged to the unified governor and
/// let the work-area grant bound the overlap instead.
const INLJ_BATCH_WORK_BYTES: usize = 64 * 1024 * 1024;

struct InljChunkCharge {
    charge: crate::engine::executor::mem_budget::ScopedCharge,
    area: crate::engine::executor::work_area::WorkAreaHandle,
}

impl InljChunkCharge {
    fn new() -> Self {
        use crate::engine::executor::work_area::WorkAreaHandle;
        Self {
            charge: crate::engine::executor::mem_budget::ScopedCharge::new(0),
            area: WorkAreaHandle::register(),
        }
    }

    fn add(&mut self, bytes: usize) -> Result<bool, QueryError> {
        use crate::engine::executor::work_area::SpillHint;
        let admitted = matches!(self.area.try_charge(bytes), SpillHint::Continue);
        self.charge.add_force(bytes)?;
        Ok(admitted)
    }

    #[cfg(test)]
    fn charged(&self) -> usize {
        self.charge.charged()
    }
}

fn pull_fused_inlj_left_chunk(
    left: &mut dyn super::plan::PlanStream,
    chunk_rows: usize,
    left_finished: &mut bool,
) -> Result<(Vec<PlanRow>, InljChunkCharge), QueryError> {
    crate::engine::cancel::check_cancelled()?;
    let mut chunk_charge = InljChunkCharge::new();
    // Input, worker output and the final concatenated Vec can overlap.
    let row_slots_bytes = std::mem::size_of::<PlanRow>().saturating_mul(3);
    // Probe scratch must coexist with input/output. Bound the batch to a
    // 64-MiB envelope and the currently unassigned executor work area.
    let manager = chunk_charge.area.manager();
    let headroom = manager
        .target_bytes()
        .saturating_sub(manager.allocated_bytes());
    let chunk_rows = chunk_rows
        .max(1)
        .min((headroom.min(INLJ_BATCH_WORK_BYTES) / 4096).max(1));
    let slot_bytes = chunk_rows.saturating_mul(row_slots_bytes);
    if !chunk_charge.add(slot_bytes)? {
        // No grant for a batch: retain just the mandatory progress row.
        drop(chunk_charge);
        let mut charge = InljChunkCharge::new();
        charge.add(row_slots_bytes)?;
        let row = left.next()?;
        let rows = if let Some(row) = row {
            charge.add(plan_row_owned_heap_bytes(&row).saturating_mul(2))?;
            vec![row]
        } else {
            *left_finished = true;
            Vec::new()
        };
        return Ok((rows, charge));
    }
    let mut left_rows = Vec::with_capacity(chunk_rows);
    while left_rows.len() < chunk_rows {
        let Some(row) = left.next()? else {
            *left_finished = true;
            break;
        };
        crate::engine::cancel::check_cancelled()?;
        let admitted = chunk_charge.add(plan_row_owned_heap_bytes(&row).saturating_mul(2))?;
        left_rows.push(row);
        if !admitted {
            break;
        }
    }
    Ok((left_rows, chunk_charge))
}

impl super::plan::PlanStream for FusedInljPlanRowStream<'_> {
    fn next(&mut self) -> Result<Option<PlanRow>, QueryError> {
        loop {
            if let Some(row) = self.current.next() {
                return Ok(Some(row));
            }
            // Drop the exhausted allocation before reserving the next chunk.
            self.current = Vec::new().into_iter();
            self.current_charge = None;
            if self.left_finished {
                return Ok(None);
            }

            let chunk_rows = self.chunk_rows;
            let (left_rows, chunk_charge) = pull_fused_inlj_left_chunk(
                self.left.as_mut(),
                chunk_rows,
                &mut self.left_finished,
            )?;
            if left_rows.is_empty() {
                return Ok(None);
            }
            let rows = super::plan::execute_fused_semi_anti_inlj_from_left_rows(
                self.db, self.txn, self.plan, &left_rows,
            );
            // Free the input before a failed probe unwinds its reservation.
            drop(left_rows);
            let rows = rows?;
            self.current = rows.into_iter();
            self.current_charge = Some(chunk_charge);
        }
    }
}

fn fused_inlj_stream_chunk_rows() -> usize {
    super::plan::inlj_chunk_stream_rows().clamp(1, 65_536)
}

impl super::plan::PlanStream for NativeInljPlanRowStream<'_> {
    fn install_runtime_filter(&mut self, filter: crate::planner::RuntimeFilter) -> bool {
        if super::plan::runtime_filter_schema_indices(&filter, &self.right_table).is_some() {
            self.right_runtime_filters.push(filter);
            true
        } else {
            self.left.install_runtime_filter(filter)
        }
    }

    fn next(&mut self) -> Result<Option<PlanRow>, QueryError> {
        loop {
            if let Some(row) = self.current.next() {
                return Ok(Some(row));
            }
            self.current = Vec::new().into_iter();
            self.current_charge = None;
            let chunk_rows = if self.unique_bounded_output {
                self.chunk_rows
            } else {
                super::plan::inlj_fanout_chunk_rows(
                    self.chunk_rows,
                    &self.right_table,
                    &self.right_index,
                    &self.lookup_plan,
                )
            };
            let (left_rows, chunk_charge) = if self.unique_bounded_output {
                {
                    let (rows, charge) = pull_fused_inlj_left_chunk(
                        self.left.as_mut(),
                        chunk_rows,
                        &mut self.left_finished,
                    )?;
                    (rows, Some(charge))
                }
            } else {
                let mut left_rows = Vec::with_capacity(chunk_rows);
                while left_rows.len() < chunk_rows && !self.left_finished {
                    let Some(row) = self.left.next()? else {
                        self.left_finished = true;
                        break;
                    };
                    crate::engine::cancel::check_cancelled()?;
                    left_rows.push(row);
                }
                (left_rows, None)
            };
            if left_rows.is_empty() {
                return Ok(None);
            }
            let rows = super::plan::execute_index_nested_loop_join_from_left_rows_with_shared_mrr(
                self.db,
                self.txn,
                self.left_plan,
                &left_rows,
                &self.right_table,
                &self.right_index,
                self.join_type,
                &self.lookup_plan,
                self.right_covering,
                self.right_projected_columns.as_deref().map(Vec::as_slice),
                &self.right_runtime_filters,
                self.shared_mrr.as_mut(),
            );
            drop(left_rows);
            let rows = rows?;
            self.current = rows.into_iter();
            self.current_charge = chunk_charge;
        }
    }
}

fn native_inlj_stream_chunk_rows(_unique_bounded_output: bool) -> usize {
    // Unique full-equality probes have bounded output and can retain an exact
    // capacity charge, but that does not make a larger input chunk free: the
    // lookup entries, sorted row ids and heap result coexist with it. Reuse
    // the common budget-derived policy so all native INLJ levels respond to
    // memory pressure consistently.
    super::plan::inlj_chunk_stream_rows()
}

impl BatchOperator for InljMrrBatchOperator<'_> {
    batch_wrapper_method!(schema);

    fn properties(&self) -> BatchProperties {
        BatchProperties::UNORDERED_SINGLE
    }

    fn install_runtime_filter(&mut self, filter: crate::planner::RuntimeFilter) -> bool {
        self.input.install_runtime_filter(filter)
    }

    fn next_batch(&mut self) -> Result<Option<Batch>, QueryError> {
        if self.finished {
            return Ok(None);
        }
        let target = scan_batch_rows();
        let mut accumulators: Vec<ColumnAccumulator> = self
            .schema
            .fields()
            .iter()
            .map(|field| ColumnAccumulator::for_field(field, target))
            .collect();
        let mut rows = 0usize;
        while rows < target {
            let Some(row) = self.input.next()? else {
                self.finished = true;
                break;
            };
            for (column, accumulator) in accumulators.iter_mut().enumerate() {
                accumulator.push(
                    row.fields
                        .get(column)
                        .map(|field| &field.value)
                        .unwrap_or(&Value::Null),
                );
            }
            rows += 1;
        }
        if rows == 0 {
            return Ok(None);
        }
        analyze_stats::add_pending_conversion_rows(rows as u64);
        Ok(Some(Batch::with_row_count(
            self.schema.clone(),
            accumulators
                .into_iter()
                .map(ColumnAccumulator::finish)
                .collect(),
            rows,
        )))
    }
}

pub(super) fn try_native_plan_row_stream<'a>(
    db: &'a Arc<Database>,
    txn: TxnView,
    plan: &'a PhysicalPlan,
) -> Result<Option<Box<dyn super::plan::PlanStream + 'a>>, QueryError> {
    let Some(VecBatchInput { stream, template }) = try_native_vec_input_stream(db, txn, plan)?
    else {
        return Ok(None);
    };
    Ok(Some(Box::new(BatchPlanRowStream {
        input: stream,
        template,
        current: None,
        visible: Vec::new(),
        position: 0,
        finished: false,
    })))
}

/// Keep wide, spilling ROLLUP input columnar through sorting. Only the final
/// merge crosses into the canonical ordered accumulator's row interface.
#[cfg(test)]
pub(crate) static COLUMNAR_ROLLUP_SORT_HITS: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);

pub(super) fn try_native_rollup_sorted_stream<'a>(
    db: &'a Arc<Database>,
    txn: TxnView,
    plan: &'a PhysicalPlan,
    group_by: &[Expr],
) -> Result<Option<Box<dyn super::plan::PlanStream + 'a>>, QueryError> {
    let template = plan_output_template(plan);
    let Some(columns) = group_by
        .iter()
        .map(|expr| expr_column_index_in_template(expr, &template))
        .collect::<Option<Vec<_>>>()
    else {
        return Ok(None);
    };
    let Some(budget) = mem_budget::current_budget() else {
        return Ok(None);
    };
    let bytes = crate::planner::estimated_rows(plan)
        * (128 + template.len() * std::mem::size_of::<Value>()) as f64;
    if group_by.is_empty() || bytes <= (budget.limit() / 4) as f64 {
        return Ok(None);
    }
    let Some(VecBatchInput {
        stream,
        template: runtime_template,
    }) = try_native_vec_input_stream(db, txn, plan)?
    else {
        return Ok(None);
    };
    let runtime_columns = group_by
        .iter()
        .map(|expr| expr_column_index_in_template(expr, &runtime_template))
        .collect::<Option<Vec<_>>>();
    if runtime_columns.as_ref() != Some(&columns) {
        return Ok(None);
    }
    let schema = stream.schema().clone();
    let fields = columns
        .iter()
        .map(|&column| crate::engine::executor::sort::SortField {
            ascending: true,
            nulls_first: true,
            collation_id: schema.fields()[column].source.collation_id,
        })
        .collect();
    let sorted = vec_sort::ExternalSortBatchOperator::new(
        None,
        stream,
        schema,
        BatchProperties::UNORDERED_SINGLE,
        columns
            .into_iter()
            .map(vec_sort::SortKeySource::Column)
            .collect(),
        fields,
        db.data_dir(),
    );
    #[cfg(test)]
    COLUMNAR_ROLLUP_SORT_HITS.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    Ok(Some(Box::new(BatchPlanRowStream {
        input: Box::new(sorted),
        template: runtime_template,
        current: None,
        visible: Vec::new(),
        position: 0,
        finished: false,
    })))
}

fn request_inlj_key_locality(
    lookup: &super::plan::IndexLookupPlan,
    stream: &mut dyn BatchOperator,
    template: &[PlanField],
) {
    if let Some(column) = lookup
        .scalar_equality_expr()
        .and_then(|expr| expr_column_index_in_template(expr, template))
    {
        stream.prefer_key_locality(&[column]);
    }
}

fn try_build_native_inlj_row_stream<'a>(
    db: &'a Arc<Database>,
    txn: TxnView,
    plan: &'a PhysicalPlan,
) -> Result<Option<Box<dyn super::plan::PlanStream + 'a>>, QueryError> {
    if !super::plan::inlj_chunk_stream_applicable(plan) {
        return Ok(None);
    }
    if super::plan::inlj_consecutive_semi_anti_fusion_applicable(plan) {
        let deepest_left = super::plan::inlj_consecutive_semi_anti_left(plan)
            .expect("fused SEMI/ANTI left input preflighted");
        let Some(VecBatchInput {
            mut stream,
            template,
        }) = try_native_vec_input_stream(db, txn, deepest_left)?
        else {
            return Ok(None);
        };
        let PhysicalPlan::IndexNestedLoopJoin {
            right_table,
            right_index,
            condition: Some(condition),
            ..
        } = plan
        else {
            unreachable!("fused INLJ preflighted");
        };
        let lookup = super::plan::compile_index_lookup_plan(condition, right_table, right_index)?;
        request_inlj_key_locality(&lookup, stream.as_mut(), &template);
        return Ok(Some(Box::new(FusedInljPlanRowStream {
            db,
            txn,
            plan,
            left: Box::new(BatchPlanRowStream {
                input: stream,
                template,
                current: None,
                visible: Vec::new(),
                position: 0,
                finished: false,
            }),
            current: Vec::new().into_iter(),
            current_charge: None,
            left_finished: false,
            chunk_rows: fused_inlj_stream_chunk_rows(),
        })));
    }
    let PhysicalPlan::IndexNestedLoopJoin {
        left,
        right_table,
        right_index,
        join_type,
        condition,
        right_covering,
        right_projected_columns,
        right_runtime_filters,
        ..
    } = plan
    else {
        unreachable!("INLJ shape checked above")
    };
    let lookup_plan = super::plan::compile_index_lookup_plan(
        condition
            .as_ref()
            .expect("chunk-stream-applicable INLJ has a condition"),
        right_table,
        right_index,
    )?;
    let left_stream = if let Some(stream) = try_build_native_inlj_row_stream(db, txn, left)? {
        stream
    } else {
        let Some(VecBatchInput {
            mut stream,
            template,
        }) = try_native_vec_input_stream(db, txn, left)?
        else {
            return Ok(None);
        };
        request_inlj_key_locality(&lookup_plan, stream.as_mut(), &template);
        Box::new(BatchPlanRowStream {
            input: stream,
            template,
            current: None,
            visible: Vec::new(),
            position: 0,
            finished: false,
        })
    };
    let unique_bounded_output = right_index.unique
        && lookup_plan.is_full_equality_for(right_index)
        && matches!(
            join_type,
            crate::parser::JoinType::Inner
                | crate::parser::JoinType::Left
                | crate::parser::JoinType::Semi
                | crate::parser::JoinType::Anti
        );
    Ok(Some(Box::new(NativeInljPlanRowStream {
        db,
        txn,
        left_plan: left,
        left: left_stream,
        right_table: right_table.clone(),
        right_index: right_index.clone(),
        join_type: *join_type,
        lookup_plan,
        right_covering: *right_covering,
        right_projected_columns: right_projected_columns.clone(),
        right_runtime_filters: right_runtime_filters.clone(),
        shared_mrr: super::plan::new_inlj_mrr_shared_caches(),
        current: Vec::new().into_iter(),
        current_charge: None,
        unique_bounded_output,
        left_finished: false,
        chunk_rows: native_inlj_stream_chunk_rows(unique_bounded_output),
    })))
}

fn try_build_inlj_batch_input<'a>(
    db: &'a Arc<Database>,
    txn: TxnView,
    plan: &'a PhysicalPlan,
) -> Result<Option<VecBatchInput<'a>>, QueryError> {
    if !super::plan::inlj_chunk_stream_applicable(plan) {
        return Ok(None);
    }
    if let Some(input) = fused_inlj::try_build_fused_neq_batch_input(db, txn, plan)? {
        return Ok(Some(input));
    }
    let template = plan_output_template(plan);
    if template.is_empty() {
        return Ok(None);
    }
    let schema = template_to_batch_schema(&template);
    if let Some(input) = try_build_native_inlj_row_stream(db, txn, plan)? {
        return Ok(Some(VecBatchInput {
            stream: Box::new(InljMrrBatchOperator {
                input,
                schema,
                finished: false,
            }),
            template,
        }));
    }
    let input = super::plan::open_inlj_chunk_stream(db, txn, plan.clone())?;
    Ok(Some(VecBatchInput {
        stream: Box::new(InljMrrBatchOperator {
            input,
            schema,
            finished: false,
        }),
        template,
    }))
}

// ---------------------------------------------------------------------
// Storage-direct BatchOperator (skips PlanRow)
// ---------------------------------------------------------------------

/// Native vec leaf for a `CteScan`: convert the materialised CTE's in-memory
/// rows directly into `Batch`es, skipping the `PlanRow` round-trip the row
/// pipeline pays (per-cell `Value` clone + interned `PlanField`, then a second
/// per-cell copy in `rows_to_batch`). A CTE→aggregate already routes through
/// vec via the fallback adapter; this removes that intermediate so the
/// aggregate consumes the CTE columns directly.
///
/// Scope: materialised references with a non-empty row set. In-memory CTEs
/// use their shared column cache; spilled CTEs are decoded incrementally into
/// typed Batch lanes and never re-materialised as one `Vec<Vec<Value>>`.
///
/// Increment 2 adds **filtered** references: a `CteScan.filter` is compiled via
/// the *same* `compile_cte_row_predicate` the row CteScan uses and applied
/// row-wise before the columns are built, so the surviving rows are
/// bit-identical to the row path's filtered output (predicate semantics are
/// the single source of truth, not re-derived for the vec path). A predicate
/// the compiler can't lower (`None`) takes the scalar row fallback. Empty
/// and fully filtered inputs end their batch stream normally. Projection
/// (`projected_columns`) is honoured; the filter still reads the full row, so a
/// predicate column pruned from the output projection is evaluated correctly.
fn try_build_cte_batch_scan<'a>(
    plan: &'a PhysicalPlan,
) -> Result<Option<VecBatchInput<'a>>, QueryError> {
    let inner = peel_projection(plan);
    let PhysicalPlan::CteScan {
        name,
        alias,
        columns,
        filter,
        projected_columns,
        ..
    } = inner
    else {
        return Ok(None);
    };
    let Some(cte) = crate::engine::executor::cte::lookup_cte(name) else {
        return Ok(None);
    };
    let src_indices: Vec<usize> = match projected_columns.as_deref() {
        Some(indices) => indices.to_vec(),
        None => (0..columns.len()).collect(),
    };
    if src_indices.is_empty() {
        return Ok(None);
    }
    // Compile the scan filter before opening either source. Both the in-memory
    // and spilled readers evaluate this same row-side predicate, keeping CTE
    // filter semantics identical across storage modes.
    let predicate = match filter {
        None => None,
        Some(expr) => match super::plan::compile_cte_row_predicate(expr, columns) {
            Some(p) => Some(p),
            // Predicate the row-side compiler can't lower (e.g. a function
            // call); the row fallback's `eval_plan_bool` handles it.
            None => return Ok(None),
        },
    };
    // Alias-qualified column metadata, identical to the CteScan row path's
    // template, so the downstream vec agg resolves group/agg columns the same.
    let alias_str = alias.clone().unwrap_or_else(|| name.clone());
    let template: Vec<PlanField> = src_indices
        .iter()
        .map(|&i| PlanField {
            meta: crate::engine::executor::select::plan::intern_col_meta(ColMeta {
                invisible: false,
                table: name.clone(),
                alias: Some(alias_str.clone()),
                column: columns[i].name.clone(),
                column_type: columns[i].column_type,
                source: columns[i].source.clone(),
            }),
            value: Value::Null,
        })
        .collect();
    let schema = template_to_batch_schema(&template);
    if let Some(spill) = &cte.spill {
        let stream = SpilledCteBatchStream::new(spill.clone(), schema, src_indices, predicate);
        CTE_BATCH_SCAN_HITS.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        return Ok(Some(VecBatchInput {
            stream: Box::new(stream),
            template,
        }));
    }
    // A shared mirror is optional. If it cannot be admitted, decode directly
    // from the retained rows; never materialize every reference as a second
    // complete relation before its parent work area can consume it.
    let cache = (predicate.is_none() && cte.consumer_count > 1)
        .then(|| cte.store.columnar_chunks())
        .flatten();
    CTE_BATCH_SCAN_HITS.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    Ok(Some(VecBatchInput {
        stream: Box::new(CteRowsBatchStream {
            cte,
            cache,
            schema,
            src_indices,
            predicate,
            position: 0,
            cache_position: 0,
            batch_charge: None,
        }),
        template,
    }))
}

struct CteRowsBatchStream {
    cte: Arc<crate::engine::api::CteResult>,
    cache: Option<Arc<Vec<Vec<ColumnVector>>>>,
    schema: Arc<BatchSchema>,
    src_indices: Vec<usize>,
    predicate: Option<super::plan::CteRowPredicate>,
    position: usize,
    cache_position: usize,
    batch_charge: Option<mem_budget::ScopedCharge>,
}

impl BatchOperator for CteRowsBatchStream {
    fn schema(&self) -> &Arc<BatchSchema> {
        &self.schema
    }

    fn next_batch(&mut self) -> Result<Option<Batch>, QueryError> {
        self.batch_charge = None;
        crate::engine::cancel::check_cancelled()?;
        let columns = if let Some(cache) = &self.cache {
            let Some(chunk) = cache.get(self.cache_position) else {
                return Ok(None);
            };
            let bytes = self
                .src_indices
                .iter()
                .map(|&source| chunk[source].memory_bytes())
                .sum();
            self.batch_charge = Some(mem_budget::force_charge_scoped(bytes)?);
            self.cache_position += 1;
            self.src_indices
                .iter()
                .map(|&source| chunk[source].clone())
                .collect()
        } else {
            let rows = self.cte.rows();
            let target = scan_decode_batch_rows(&self.schema);
            let mut selected =
                Vec::with_capacity(target.min(rows.len().saturating_sub(self.position)));
            while selected.len() < target && self.position < rows.len() {
                let row = &rows[self.position];
                self.position += 1;
                if self.predicate.as_ref().map_or(Ok(true), |predicate| {
                    super::plan::eval_cte_row_predicate(predicate, row)
                })? {
                    selected.push(row.as_slice());
                }
            }
            if selected.is_empty() {
                return Ok(None);
            }
            // Bound the conversion scratch as well as the completed lanes.
            // Preserve from_values inference for every numeric/temporal lane.
            let bytes = selected.iter().fold(0usize, |bytes, row| {
                self.src_indices.iter().fold(bytes, |bytes, &source| {
                    bytes.saturating_add(row.get(source).map_or(
                        std::mem::size_of::<Value>(),
                        mem_budget::estimate_value_bytes,
                    ))
                })
            });
            self.batch_charge = Some(mem_budget::force_charge_scoped(bytes.saturating_mul(2))?);
            self.src_indices
                .iter()
                .map(|&source| {
                    ColumnVector::from_values(
                        selected
                            .iter()
                            .map(|row| row.get(source).cloned().unwrap_or(Value::Null))
                            .collect(),
                    )
                })
                .collect()
        };
        Ok(Some(Batch::new(self.schema.clone(), columns)))
    }
}

struct SpilledCteBatchStream {
    schema: Arc<BatchSchema>,
    src_indices: Vec<usize>,
    projection_has_duplicates: bool,
    predicate: Option<super::plan::CteRowPredicate>,
    reader: Option<crate::engine::executor::select::grace::ValueRowReader>,
    exhausted: bool,
    spill: Arc<crate::engine::api::CteSpillFile>,
}

impl SpilledCteBatchStream {
    fn new(
        spill: Arc<crate::engine::api::CteSpillFile>,
        schema: Arc<BatchSchema>,
        src_indices: Vec<usize>,
        predicate: Option<super::plan::CteRowPredicate>,
    ) -> Self {
        let projection_has_duplicates = src_indices
            .iter()
            .enumerate()
            .any(|(index, src)| src_indices[index + 1..].contains(src));
        Self {
            schema,
            src_indices,
            projection_has_duplicates,
            predicate,
            reader: None,
            exhausted: false,
            spill,
        }
    }
}

impl BatchOperator for SpilledCteBatchStream {
    batch_wrapper_method!(schema);

    fn properties(&self) -> BatchProperties {
        BatchProperties::UNORDERED_SINGLE
    }

    fn next_batch(&mut self) -> Result<Option<Batch>, QueryError> {
        crate::engine::cancel::check_cancelled()?;
        if self.exhausted {
            return Ok(None);
        }
        if self.reader.is_none() {
            self.reader = crate::engine::executor::select::grace::ValueRowReader::open(
                &self.spill.path,
                self.spill._dir.charger(),
            )?;
            if self.reader.is_none() {
                self.exhausted = true;
                if self.spill.row_count > 0 {
                    return Err(crate::storage::error(
                        crate::error::catalog::ER_UNKNOWN_ERROR,
                        "materialized CTE spill file is missing or truncated",
                    ));
                }
                return Ok(None);
            }
        }
        let batch_rows = scan_decode_batch_rows(&self.schema);
        if self.predicate.is_none()
            && self.src_indices.len() == self.reader.as_ref().expect("reader opened").column_count()
            && self
                .src_indices
                .iter()
                .enumerate()
                .all(|(index, &source)| index == source)
        {
            let reader = self.reader.as_mut().expect("reader opened");
            let batch_rows = reader.reserve_decode_batch(batch_rows)?;
            let mut columns = self
                .schema
                .fields()
                .iter()
                .map(|_| Vec::with_capacity(batch_rows))
                .collect::<Vec<_>>();
            let rows = reader.read_columns(&mut columns, batch_rows)?;
            self.exhausted = reader.is_exhausted();
            let mut charge = reader.take_decode_charge();
            use crate::engine::executor::memory_footprint::AllocatedBytes;
            charge.add_force(columns.allocated_bytes())?;
            if rows == 0 {
                return Ok(None);
            }
            return Ok(Some(
                Batch::with_row_count(
                    self.schema.clone(),
                    columns.into_iter().map(ColumnVector::from_values).collect(),
                    rows,
                )
                .with_memory_charge(charge)?,
            ));
        }
        let reader = self.reader.as_mut().expect("reader opened");
        let batch_rows = reader.reserve_decode_batch(batch_rows)?;
        let decode_limit = reader.decode_limit();
        let mut charge = reader.take_decode_charge();
        let fixed_bytes = charge.charged();
        let mut values: Vec<Vec<Value>> = self
            .schema
            .fields()
            .iter()
            .map(|_| Vec::with_capacity(batch_rows))
            .collect();
        let mut rows = 0usize;
        while rows < batch_rows
            && (rows == 0 || charge.charged().saturating_sub(fixed_bytes) < decode_limit)
        {
            let row = self.reader.as_mut().expect("reader opened").next_owned()?;
            let Some((mut row, mut row_charge)) = row else {
                self.exhausted = true;
                break;
            };
            if let Some(predicate) = &self.predicate {
                if !super::plan::eval_cte_row_predicate(predicate, &row)? {
                    continue;
                }
            }
            let mut retained = 0usize;
            for (output_index, (column, &src)) in
                values.iter_mut().zip(&self.src_indices).enumerate()
            {
                // The spill reader owns this row. Move each projected value
                // directly into its typed column on the final occurrence;
                // duplicate projections clone only the earlier occurrences.
                let value = if self.projection_has_duplicates
                    && self.src_indices[output_index + 1..].contains(&src)
                {
                    if let Some(value) = row.get(src) {
                        charge.add_force(mem_budget::estimate_value_bytes(value))?;
                        mem_budget::try_clone_value(value, "CTE duplicate projection")?
                    } else {
                        Value::Null
                    }
                } else {
                    row.get_mut(src)
                        .map(|value| std::mem::replace(value, Value::Null))
                        .unwrap_or(Value::Null)
                };
                use crate::engine::executor::memory_footprint::AllocatedBytes;
                retained = retained.saturating_add(value.allocated_bytes());
                column.push(value);
            }
            row_charge.release_bytes(row_charge.charged().saturating_sub(retained));
            charge.absorb(&mut row_charge);
            rows += 1;
        }
        if rows == 0 {
            return Ok(None);
        }
        use crate::engine::executor::memory_footprint::AllocatedBytes;
        charge.add_force(values.allocated_bytes())?;
        let columns = values.into_iter().map(ColumnVector::from_values).collect();
        Ok(Some(
            Batch::with_row_count(self.schema.clone(), columns, rows).with_memory_charge(charge)?,
        ))
    }
}

/// Native batch leaf for every `generate_series(...)` family. Integer ranges
/// use a primitive i64 lane; decimal and timestamp ranges use the same typed
/// Value/interval semantics as the scalar oracle but flush directly into
/// column vectors without constructing PlanRows.
fn try_build_generate_series_batch<'a>(
    plan: &'a PhysicalPlan,
) -> Result<Option<VecBatchInput<'a>>, QueryError> {
    let inner = peel_projection(plan);
    let PhysicalPlan::GenerateSeries {
        series,
        relation,
        with_ordinality,
        columns,
        filter,
        ..
    } = inner
    else {
        return Ok(None);
    };
    let template: Vec<PlanField> = columns
        .iter()
        .map(|col| PlanField {
            meta: crate::engine::executor::select::plan::intern_col_meta(ColMeta {
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
    let schema = template_to_batch_schema(&template);
    let base: Box<dyn BatchOperator + 'a> = match series {
        crate::planner::SeriesKind::Int { start, stop, step } => {
            Box::new(GenerateSeriesIntBatchStream {
                schema,
                current: *start,
                stop: *stop,
                step: *step,
                ordinal: 0,
                with_ordinality: *with_ordinality,
                done: false,
            })
        }
        crate::planner::SeriesKind::Dec {
            start,
            stop,
            step,
            scale,
        } => Box::new(GenerateSeriesValueBatchStream {
            schema,
            state: GenerateSeriesValueState::Decimal {
                current: *start,
                stop: *stop,
                step: *step,
                scale: *scale,
            },
            ordinal: 0,
            with_ordinality: *with_ordinality,
            done: false,
        }),
        crate::planner::SeriesKind::Ts {
            start,
            stop,
            amount,
            unit,
            ascending,
        } => Box::new(GenerateSeriesValueBatchStream {
            schema,
            state: GenerateSeriesValueState::Timestamp {
                current: *start,
                stop: *stop,
                amount: amount.clone(),
                unit: unit.clone(),
                ascending: *ascending,
            },
            ordinal: 0,
            with_ordinality: *with_ordinality,
            done: false,
        }),
    };
    let stream: Box<dyn BatchOperator + 'a> = if let Some(predicate) = filter {
        let schema = base.schema().clone();
        let Some(program) = crate::engine::executor::vec::expr_batch::BatchExprProgram::compile(
            predicate, &schema,
        )?
        else {
            return Ok(None);
        };
        let properties = base.properties();
        Box::new(FilterBatchStream {
            upstream: base,
            program,
            schema,
            properties,
            analyze_plan: Some(plan),
            analyze_recorded: false,
        })
    } else {
        base
    };
    Ok(Some(VecBatchInput { stream, template }))
}

struct GenerateSeriesIntBatchStream {
    schema: Arc<BatchSchema>,
    current: i64,
    stop: i64,
    step: i64,
    ordinal: i64,
    with_ordinality: bool,
    done: bool,
}

impl BatchOperator for GenerateSeriesIntBatchStream {
    batch_wrapper_method!(schema);

    fn properties(&self) -> BatchProperties {
        BatchProperties {
            ordering: BatchOrdering::Ordered(vec![BatchSortKey {
                column_index: 0,
                ascending: self.step > 0,
                nulls_first: self.step > 0,
            }]),
            partitioning: BatchPartitioning::Single,
        }
    }

    fn next_batch(&mut self) -> Result<Option<Batch>, QueryError> {
        if self.done {
            return Ok(None);
        }
        crate::engine::cancel::check_cancelled()?;
        let mut data: Vec<i64> = Vec::with_capacity(DEFAULT_BATCH_ROWS);
        let mut ord: Vec<i64> = if self.with_ordinality {
            Vec::with_capacity(DEFAULT_BATCH_ROWS)
        } else {
            Vec::new()
        };
        while data.len() < DEFAULT_BATCH_ROWS {
            let in_range = if self.step > 0 {
                self.current <= self.stop
            } else {
                self.current >= self.stop
            };
            if !in_range {
                self.done = true;
                break;
            }
            data.push(self.current);
            if self.with_ordinality {
                self.ordinal += 1;
                ord.push(self.ordinal);
            }
            match self.current.checked_add(self.step) {
                Some(next) => self.current = next,
                None => {
                    self.done = true;
                    break;
                }
            }
        }
        if data.is_empty() {
            return Ok(None);
        }
        let mut columns = vec![ColumnVector::I64 { data, nulls: None }];
        if self.with_ordinality {
            columns.push(ColumnVector::I64 {
                data: ord,
                nulls: None,
            });
        }
        Ok(Some(Batch::new(self.schema.clone(), columns)))
    }
}

/// Peel non-DISTINCT Projection wrappers to reach the inner plan,
/// matching the behaviour of `execute_plan_stream`.
fn peel_projection(plan: &PhysicalPlan) -> &PhysicalPlan {
    match plan {
        PhysicalPlan::Projection {
            input,
            distinct: false,
            ..
        } => peel_projection(input),
        other => other,
    }
}

enum GenerateSeriesValueState {
    Decimal {
        current: i128,
        stop: i128,
        step: i128,
        scale: u8,
    },
    Timestamp {
        current: i64,
        stop: i64,
        amount: Value,
        unit: String,
        ascending: bool,
    },
}

struct GenerateSeriesValueBatchStream {
    schema: Arc<BatchSchema>,
    state: GenerateSeriesValueState,
    ordinal: i64,
    with_ordinality: bool,
    done: bool,
}

impl BatchOperator for GenerateSeriesValueBatchStream {
    batch_wrapper_method!(schema);

    fn properties(&self) -> BatchProperties {
        let ascending = match &self.state {
            GenerateSeriesValueState::Decimal { step, .. } => *step > 0,
            GenerateSeriesValueState::Timestamp { ascending, .. } => *ascending,
        };
        BatchProperties {
            ordering: BatchOrdering::Ordered(vec![BatchSortKey {
                column_index: 0,
                ascending,
                nulls_first: ascending,
            }]),
            partitioning: BatchPartitioning::Single,
        }
    }

    fn next_batch(&mut self) -> Result<Option<Batch>, QueryError> {
        if self.done {
            return Ok(None);
        }
        crate::engine::cancel::check_cancelled()?;
        let mut values = Vec::with_capacity(DEFAULT_BATCH_ROWS);
        let mut ordinals = Vec::with_capacity(DEFAULT_BATCH_ROWS);
        while values.len() < DEFAULT_BATCH_ROWS {
            let next = match &mut self.state {
                GenerateSeriesValueState::Decimal {
                    current,
                    stop,
                    step,
                    scale,
                } => {
                    let in_range = if *step > 0 {
                        *current <= *stop
                    } else {
                        *current >= *stop
                    };
                    if !in_range {
                        None
                    } else {
                        let value = match i64::try_from(*current) {
                            Ok(compact) => Value::DecimalCompact(compact, *scale),
                            Err(_) => Value::Decimal(crate::util::decimal::format_decimal_parts(
                                *current, *scale,
                            )),
                        };
                        match current.checked_add(*step) {
                            Some(next) => *current = next,
                            None => self.done = true,
                        }
                        Some(value)
                    }
                }
                GenerateSeriesValueState::Timestamp {
                    current,
                    stop,
                    amount,
                    unit,
                    ascending,
                } => {
                    let in_range = if *ascending {
                        *current <= *stop
                    } else {
                        *current >= *stop
                    };
                    if !in_range {
                        None
                    } else {
                        let value = Value::DateTime(*current);
                        let next = crate::engine::expr::add_interval_to_micros(
                            *current, amount, unit, false,
                        )?;
                        if next == *current {
                            self.done = true;
                        } else {
                            *current = next;
                        }
                        Some(value)
                    }
                }
            };
            let Some(value) = next else {
                self.done = true;
                break;
            };
            self.ordinal = self.ordinal.saturating_add(1);
            values.push(value);
            if self.with_ordinality {
                ordinals.push(self.ordinal);
            }
        }
        if values.is_empty() {
            return Ok(None);
        }
        let row_count = values.len();
        let mut columns = vec![ColumnVector::from_values(values)];
        if self.with_ordinality {
            columns.push(ColumnVector::I64 {
                data: ordinals,
                nulls: None,
            });
        }
        Ok(Some(Batch::with_row_count(
            self.schema.clone(),
            columns,
            row_count,
        )))
    }
}

// ---------------------------------------------------------------------
// Top-N (ORDER BY + LIMIT)
// ---------------------------------------------------------------------

fn computed_batch_field(program: &ExprProgram, table: &str, name: String) -> Option<BatchField> {
    let descriptor = program.result_type();
    if matches!(
        descriptor.kind,
        crate::engine::expr_type::ExprTypeKind::Unknown
            | crate::engine::expr_type::ExprTypeKind::String {
                kind: crate::engine::expr_type::StringKind::Enum
                    | crate::engine::expr_type::StringKind::Set
            }
    ) {
        return None;
    }
    let mut source = crate::types::ColumnSource::default();
    descriptor.apply_to_source(&mut source);
    Some(BatchField {
        table: table.to_string(),
        alias: None,
        name,
        column_type: descriptor.column_type(),
        source,
    })
}

fn batch_field_template(field: &BatchField) -> PlanField {
    PlanField {
        meta: super::plan::intern_col_meta(ColMeta {
            invisible: false,
            table: field.table.clone(),
            alias: field.alias.clone(),
            column: field.name.clone(),
            column_type: field.column_type,
            source: field.source.clone(),
        }),
        value: Value::Null,
    }
}

thread_local! {
    static LAST_AGG_METRICS: std::cell::Cell<Option<vec_hash_agg::AggMetrics>> =
        const { std::cell::Cell::new(None) };
}

fn record_pending_agg_metrics(m: vec_hash_agg::AggMetrics) {
    LAST_AGG_METRICS.with(|cell| cell.set(Some(m)));
}

/// Drain the pending HashAggregate metrics, if any, and forward them to
/// the active `EXPLAIN ANALYZE` collector keyed by `plan`. No-op when the
/// vectorized fast path wasn't taken or stats collection is inactive.
pub(super) fn forward_vec_hash_agg_metrics(plan: &PhysicalPlan) {
    let m = LAST_AGG_METRICS.with(|cell| cell.take());
    let Some(m) = m else { return };
    if m.peak_memory_bytes > 0 {
        analyze_stats::add_peak_memory(plan, m.peak_memory_bytes);
    }
    if m.spill_bytes > 0 {
        analyze_stats::add_spill_bytes(plan, m.spill_bytes);
        analyze_stats::add_spill_shape(
            plan,
            m.spill_runs,
            m.spill_passes,
            m.spill_partitions,
            "memory_limit",
        );
    }
    if m.is_streaming {
        analyze_stats::set_streaming(plan, true);
    }
    analyze_stats::add_vector_runtime(plan, m.batches, m.conversion_rows, m.workers);
    if let Some(mode) = m.expression_mode {
        analyze_stats::add_expression_runtime(
            plan,
            mode,
            m.native_expression_rows,
            m.fallback_expression_rows,
        );
    }
    if let Some(mode) = m.aggregate_mode {
        analyze_stats::add_aggregate_runtime(
            plan,
            mode,
            m.native_aggregate_rows,
            m.fallback_aggregate_rows,
            m.distinct_spill_bytes,
        );
    }
}

// ---------------------------------------------------------------------
// Distinct
// ---------------------------------------------------------------------

/// Try to execute `DISTINCT projection` via the vectorized streaming
/// dedup. Returns `Ok(None)` for any projection containing wildcards,
/// expressions that aren't plain column references, or expressions the
/// vectorized engine doesn't yet support.
pub(super) fn try_vec_distinct(
    db: &Arc<Database>,
    txn: TxnView,
    upstream: &PhysicalPlan,
    projection: &[SelectItem],
) -> Result<Option<Vec<PlanRow>>, QueryError> {
    if !vectorize_enabled_for_plan(upstream) {
        return Ok(None);
    }
    if projection.is_empty() {
        return Ok(None);
    }
    // *** Static pre-check, before any IO. *** Reject expressions and
    // qualified wildcards up-front so we don't start the upstream
    // stream just to bail out and let the legacy path repeat the scan.
    // Bare `Wildcard` is allowed — it expands to every input column.
    for item in projection {
        match item {
            SelectItem::Wildcard => {}
            SelectItem::Expr { expr, .. } => {
                if !is_simple_column_ref(expr) {
                    return Ok(None);
                }
            }
            SelectItem::QualifiedWildcard(_) => return Ok(None),
        }
    }

    // Native columnar inputs only. With the row-stream fallback the
    // "vectorized" DISTINCT degraded into a sandwich — run the whole
    // row pipeline, convert rows→batches, hash-dedup, convert back —
    // strictly more work than the legacy streaming dedup the caller
    // falls back to (StreamingDistinct's input is sorted by planner
    // invariant, so adjacent-compare dedup suffices there).
    let Some(VecBatchInput {
        stream: mut input_stream,
        template,
    }) = try_native_vec_input_stream(db, txn, upstream)?
    else {
        return Ok(None);
    };
    if template.is_empty() {
        return Ok(Some(Vec::new()));
    }

    let mut out_columns: Vec<usize> = Vec::with_capacity(projection.len());
    for item in projection {
        match item {
            SelectItem::Wildcard => {
                for i in 0..template.len() {
                    out_columns.push(i);
                }
            }
            SelectItem::Expr { expr, .. } => match expr_column_index_in_template(expr, &template) {
                Some(i) => out_columns.push(i),
                None => return Ok(None),
            },
            SelectItem::QualifiedWildcard(_) => return Ok(None),
        }
    }

    let rows = vec_distinct::run_distinct(input_stream.as_mut(), &out_columns, db.data_dir())?;

    let out_template: Vec<PlanField> = out_columns.iter().map(|&i| template[i].clone()).collect();

    Ok(Some(
        rows.into_iter()
            .map(|vs| values_to_plan_row(vs, &out_template))
            .collect(),
    ))
}

#[cfg(test)]
mod tests;

impl BatchOperator for FilterBatchStream<'_> {
    batch_wrapper_method!(prefer_key_locality);

    fn limit_column_prefixes(&mut self, limits: &[(usize, usize)]) {
        let needed = self.program.referenced_columns();
        let limits: Vec<_> = limits
            .iter()
            .copied()
            .filter(|(column, _)| !needed.contains(column))
            .collect();
        self.upstream.limit_column_prefixes(&limits);
    }
    batch_wrapper_method!(schema);

    batch_wrapper_method!(properties);

    batch_wrapper_method!(install_runtime_filter);

    batch_wrapper_method!(install_runtime_index_bound);

    fn next_batch(&mut self) -> Result<Option<Batch>, QueryError> {
        use crate::engine::executor::vec::batch::Selection;
        while let Some(batch) = self.upstream.next_batch()? {
            crate::engine::cancel::check_cancelled()?;
            let mask = self.program.eval_predicate_bitmap(&batch)?;
            // Compose with any selection the upstream already carries: the
            // visible set is the intersection (a row must already be visible
            // *and* pass the predicate). `eval_predicate_bitmap` already
            // honors the upstream selection; composing here keeps this stream
            // robust if another bitmap producer is introduced later.
            let composed = match batch.selection() {
                None => Selection::Mask(mask),
                Some(existing) => existing
                    .clone()
                    .intersect(Selection::Mask(mask), batch.num_rows()),
            };
            if composed.count() == 0 {
                let _ = self.upstream.grow_candidate_batch_rows();
                continue;
            }
            return Ok(Some(batch.with_selection(composed)));
        }
        self.record_expression_stats();
        Ok(None)
    }
}
