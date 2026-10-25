mod column_validation;
use column_validation::{validate_expr_columns, validate_window_frame_columns};
mod semi_anti;
pub(crate) use join_cost::adaptive_candidate_cost;
pub(crate) use join_cost::reverse_semi_anti_hash_join_eligible;
pub(crate) use semi_anti::expr_has_window_function;
pub(crate) use semi_anti::qualify_outer_idents_by_owner;
use semi_anti::*;
mod join_cost;
use join_cost::*;
pub(crate) use sip::propagate_sideways_dimension_filters;
mod sip;
pub(crate) use enum_set::{rewrite_enum_set_dml, rewrite_enum_set_numeric_semantics};
use sip::*;
mod enum_set;
mod prune;
use prune::*;
pub(crate) use prune::{
    prune_redundant_constant_group_keys, prune_redundant_functional_group_keys,
};
mod hints;
use hints::*;
mod explain_fmt;
use explain_fmt::*;
pub use explain_fmt::{explain_result_columns, explain_rows, explain_rows_to_values};
mod estimate;
use estimate::*;
pub(crate) use estimate::{
    estimate_equijoin_rows, estimate_filter_rows, estimate_group_by_rows_for_plan,
};
#[cfg(test)]
mod expr_visit_tests;
pub(crate) mod index_bounds;
use crate::parser::boolean::flip_cmp as mirror_cmp_op;
use crate::parser::boolean::flip_cmp;
use crate::parser::boolean::{
    collect_expr_conjuncts, collect_expr_conjuncts as flatten_and, collect_expr_disjuncts,
};
#[cfg(test)]
use crate::parser::Statement;
use crate::planner_decorrelate::sublink::expr_is_outer_column as expr_resolves_to_outer_column;
use index_bounds::{target_for_index_column, ColumnRange, ConstraintValue as PlanConstraintValue};
mod access;
pub(crate) mod boolean_range;
pub(crate) mod explain_access;
pub(crate) mod index_residual;
mod tuple_access;
mod visit;
pub(crate) use visit::{plan_children, plan_children_mut, PlanChildren, PlanChildrenMut};

use crate::engine::executor::select::bloom::{BloomFilter, ExactIntKeySet};
use crate::engine::expr_program::expr_semantically_eq;
use crate::parser::{
    BinaryOp, Expr, GroupByModifier, IndexHint, IndexHintScope, IndexHintType, JoinType,
    LimitValue, OrderBy, QuantifiedComparisonRight, QueryHint, Select, SelectItem, SetOp,
    SetOperation, SubQuery, TableRef, UnaryOp,
};
use crate::storage::{
    error, value_to_string, DataTypeKind, Database, IndexBound, IndexKey, IndexSchema, KeyPart,
    TableSchema,
};
use crate::types::{Column, ColumnType, QueryError, Value};
use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::{Arc, OnceLock};

const DEFAULT_PARALLEL_THRESHOLD_ROWS: u64 = 100_000;

#[derive(Debug, Clone)]
pub enum RuntimeFilter {
    BloomKey {
        keys: Vec<Expr>,
        null_safe: Vec<bool>,
        filter: Arc<BloomFilter>,
        /// Per-filter EXPLAIN ANALYZE rejection counter. Runtime filters are
        /// immutable once injected, so workers can update this shared
        /// sidecar without consulting global process counters.
        rejected: Option<Arc<std::sync::atomic::AtomicU64>>,
    },
    /// Exact membership for a single canonical integer join key. This avoids
    /// Bloom false positives on dense foreign-key domains while retaining the
    /// parent join as the semantic check for unsupported runtime values.
    ExactIntKey {
        keys: Vec<Expr>,
        null_safe: Vec<bool>,
        filter: Arc<ExactIntKeySet>,
        /// ANALYZE installs this lazily before the first probe batch. Normal
        /// execution therefore pays no atomic increment on rejected rows.
        rejected: Arc<OnceLock<Arc<std::sync::atomic::AtomicU64>>>,
    },
    KeySet {
        keys: Vec<Expr>,
        null_safe: Vec<bool>,
        values: Arc<Vec<Vec<Value>>>,
        /// Compact membership probe for candidate-first evaluation. False
        /// positives are harmless because the parent hash join remains the
        /// correctness check; keeping this beside `values` avoids rebuilding
        /// a hash set for every streamed INLJ batch.
        filter: Arc<BloomFilter>,
    },
}

impl RuntimeFilter {
    pub(crate) fn enable_rejection_counter(&mut self) {
        if let Self::ExactIntKey { rejected, .. } = self {
            let _ = rejected.set(Arc::new(std::sync::atomic::AtomicU64::new(0)));
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BuildSide {
    Left,
    Right,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AdaptiveJoinChoice {
    Hash,
    IndexNestedLoop,
    NestedLoop,
}

#[derive(Debug, Clone)]
pub enum AdaptiveJoinCandidate {
    Hash {
        left_keys: Vec<Expr>,
        right_keys: Vec<Expr>,
        null_safe: Vec<bool>,
        build_side: BuildSide,
        residual: Option<Expr>,
        estimated_rows: f64,
        cost: crate::engine::cbo::Cost,
    },
    IndexNestedLoop {
        right_table: PlanTable,
        right_index: IndexSchema,
        condition: Option<Expr>,
        eq_prefix_len: usize,
        estimated_rows: f64,
        cost: crate::engine::cbo::Cost,
        right_covering: bool,
        right_projected_columns: Option<Arc<Vec<usize>>>,
    },
    NestedLoop {
        condition: Option<Expr>,
        estimated_rows: f64,
        cost: crate::engine::cbo::Cost,
    },
}

/// Evaluated argument bounds for a `PhysicalPlan::GenerateSeries` leaf. The
/// integer family is implemented in M2; numeric (decimal) and timestamp
/// (interval) families are added in M4 as further variants.
#[derive(Debug, Clone)]
pub enum SeriesKind {
    /// Integer family: signed 64-bit `start`, `stop`, and non-zero `step`.
    Int { start: i64, stop: i64, step: i64 },
    /// Numeric (fixed-point) family: `i128` magnitudes at a common `scale`.
    Dec {
        start: i128,
        stop: i128,
        step: i128,
        scale: u8,
    },
    /// Timestamp family: `start`/`stop` as microseconds since the epoch with an
    /// interval step (`amount` + `unit`); `ascending` is the step direction.
    Ts {
        start: i64,
        stop: i64,
        amount: crate::types::Value,
        unit: String,
        ascending: bool,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ZoneMapPlanInfo {
    pub surviving_pages: u64,
    pub total_pages: u64,
    pub ranges: u64,
    pub zones_examined: u64,
    pub metadata_read_bytes: u64,
    pub metadata_cache_misses: u64,
    pub storage_objects: u64,
    pub estimate_exact: bool,
}

#[derive(Debug, Clone)]
pub struct FulltextMatchPlan {
    pub index: IndexSchema,
    pub mode: crate::parser::FtMode,
    pub query: String,
    pub query_param: Option<usize>,
    pub columns: Vec<String>,
    pub match_fingerprint: u64,
    pub restrict_to_matches: bool,
    pub needs_score: bool,
}

#[derive(Debug, Clone)]
pub struct FulltextLikePrefilterPlan {
    pub column: String,
    pub tokens: Vec<String>,
    pub literal_segments: usize,
    pub pattern_param: Option<usize>,
    pub escape_char: Option<char>,
}

/// A B-tree access leaf used only to produce physical row identities for an
/// Index Merge. Heap rows are fetched once, after the complete merge tree has
/// removed duplicates, and the original predicate remains the correctness
/// oracle.
#[derive(Debug, Clone)]
pub struct IndexMergeLeaf {
    pub(crate) index: IndexSchema,
    pub(crate) access: IndexAccessKind,
    /// True when the raw B-tree stream is already ordered by RowId. A full
    /// single-key lookup has a constant key prefix and therefore inherits the
    /// `(key, row_id)` leaf ordering; multi-key/prefix/range scans do not.
    pub(crate) row_id_ordered: bool,
}

/// Boolean row-id program for one table. Every node emits a sorted, unique
/// RowId stream; `sort_union` records whether an OR node must first sort one
/// or more non-row-ordered inputs.
#[derive(Debug, Clone)]
pub enum IndexMergeNode {
    Leaf(IndexMergeLeaf),
    Union {
        children: Vec<IndexMergeNode>,
        sort_union: bool,
    },
    Intersection {
        children: Vec<IndexMergeNode>,
    },
}

impl IndexMergeNode {
    pub(crate) fn leaf_count(&self) -> usize {
        match self {
            Self::Leaf(_) => 1,
            Self::Union { children, .. } | Self::Intersection { children } => {
                children.iter().map(Self::leaf_count).sum()
            }
        }
    }

    pub(crate) fn collect_index_names(&self, out: &mut Vec<String>) {
        match self {
            Self::Leaf(leaf) => out.push(leaf.index.name.clone()),
            Self::Union { children, .. } | Self::Intersection { children } => {
                for child in children {
                    child.collect_index_names(out);
                }
            }
        }
    }

    pub(crate) fn is_row_id_ordered(&self) -> bool {
        match self {
            Self::Leaf(leaf) => leaf.row_id_ordered,
            Self::Union { sort_union, .. } => !sort_union,
            Self::Intersection { .. } => true,
        }
    }

    pub(crate) fn first_leaf(&self) -> Option<&IndexMergeLeaf> {
        match self {
            Self::Leaf(leaf) => Some(leaf),
            Self::Union { children, .. } | Self::Intersection { children } => {
                children.iter().find_map(Self::first_leaf)
            }
        }
    }
}

/// Optional ordinary B-tree candidate source evaluated alongside MATCH.
/// MATCH score maps remain authoritative; this access path only narrows the
/// heap row-id superset for regular conjuncts and is intersected with every
/// restricting posting set at execution time.
#[derive(Debug, Clone)]
pub enum FulltextBaseAccess {
    Lookup {
        index: IndexSchema,
        key: IndexKey,
        exact: bool,
    },
    MultiLookup {
        index: IndexSchema,
        keys: Vec<IndexKey>,
        exact: bool,
    },
    Range {
        index: IndexSchema,
        low: Option<IndexBound>,
        high: Option<IndexBound>,
    },
    ZoneMap {
        filter: Expr,
    },
}

/// Deterministic preorder identity of a node inside a finalized physical plan.
/// IDs depend only on topology and child order, never on allocation address or
/// runtime memory pressure.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct PlanNodeId(pub(crate) u32);

#[derive(Debug, Clone)]
pub enum PhysicalPlan {
    /// Restore the nullable-side schema after an outer join was reduced to
    /// an existence test. The input is finalized in its own projection scope.
    NullExtend {
        input: Box<PhysicalPlan>,
        table: PlanTable,
        estimated_rows: f64,
        cost: crate::engine::cbo::Cost,
    },
    /// Constant relation produced by `VALUES ROW(...), ...`. Expressions are
    /// evaluated lazily in row order by the batch source, so prepared
    /// parameters, warnings and cancellation share the ordinary pipeline.
    ValuesScan {
        rows: Vec<Vec<Expr>>,
        columns: Vec<crate::types::Column>,
        filter: Option<Expr>,
        estimated_rows: f64,
        cost: crate::engine::cbo::Cost,
    },
    /// `JSON_TABLE` relation. The source expression and path definition stay
    /// in the finalized plan; DOM expansion starts only when the operator is
    /// opened, after the complete pipeline graph has been lowered.
    JsonTableScan {
        source: crate::parser::JsonTableSource,
        relation: String,
        columns: Vec<crate::types::Column>,
        filter: Option<Expr>,
        estimated_rows: f64,
        cost: crate::engine::cbo::Cost,
    },
    /// Lazy information/performance/mysql/sys relation. Catalog row
    /// production starts from the BatchOperator source only after the whole
    /// pipeline has been lowered; upper joins/aggregates/sorts are ordinary
    /// physical operators rather than a system-query dispatcher.
    SystemScan {
        name: String,
        relation: String,
        columns: Vec<crate::types::Column>,
        filter: Option<Expr>,
        estimated_rows: f64,
        cost: crate::engine::cbo::Cost,
        projected_columns: Option<Arc<Vec<usize>>>,
    },
    /// Hash-count / concatenate set operator inside the same physical graph.
    /// UNION ALL is non-blocking; DISTINCT/INTERSECT/EXCEPT are spillable
    /// blocking operators. ORDER BY and LIMIT remain ordinary parent nodes.
    SetOp {
        left: Box<PhysicalPlan>,
        right: Box<PhysicalPlan>,
        op: SetOp,
        all: bool,
        estimated_rows: f64,
        cost: crate::engine::cbo::Cost,
    },
    /// Explicit reusable/spillable pipeline boundary. The child still emits
    /// native batches; the enclosing query-level spool sink retains them for
    /// replay by CteScan/derived consumers. Keeping the boundary in the plan
    /// makes costing, EXPLAIN and ANALYZE agree with execution instead of
    /// hiding materialisation in dispatch.
    Materialize {
        input: Box<PhysicalPlan>,
        reuse_key: String,
        estimated_rows: f64,
        cost: crate::engine::cbo::Cost,
    },
    /// Iterative recursive-CTE operator. `anchor` is executed once;
    /// `recursive` is re-opened against the previous delta spool until it is
    /// empty or `max_depth` is reached. UNION DISTINCT uses the same spillable
    /// hash-count state as SetOp to subtract prior history.
    RecursiveUnion {
        anchor: Box<PhysicalPlan>,
        recursive: Box<PhysicalPlan>,
        name: String,
        union_all: bool,
        max_depth: u32,
        estimated_rows: f64,
        cost: crate::engine::cbo::Cost,
    },
    SeqScan {
        table: PlanTable,
        filter: Option<Expr>,
        estimated_rows: f64,
        access_rows: f64,
        cost: crate::engine::cbo::Cost,
        possible_keys: Vec<String>,
        runtime_filters: Vec<RuntimeFilter>,
        hint_note: Option<String>,
        /// Column indices to materialise when streaming rows out. `None`
        /// keeps every column (default for single-table queries and CTEs).
        /// Filled in by `apply_column_pruning` for multi-table joins so
        /// downstream hash join build/probe sides only carry columns that
        /// some upper plan node references — avoiding the spill that
        /// happens when wide unused columns inflate the build over the
        /// hash-join memory threshold.
        projected_columns: Option<Arc<Vec<usize>>>,
    },
    /// Sequential heap scan whose physical page ranges are derived from the
    /// table's Zone Map at execution time.  The cached plan deliberately keeps
    /// estimates only: prepared parameters and concurrent Zone Map generations
    /// must be resolved against current metadata for every execution.
    ZoneMapScan {
        table: PlanTable,
        filter: Option<Expr>,
        estimated_rows: f64,
        access_rows: f64,
        cost: crate::engine::cbo::Cost,
        possible_keys: Vec<String>,
        runtime_filters: Vec<RuntimeFilter>,
        hint_note: Option<String>,
        projected_columns: Option<Arc<Vec<usize>>>,
        zone_map: ZoneMapPlanInfo,
    },
    /// Multiple B-tree probes over one table, merged by physical RowId before
    /// a single MVCC heap fetch and residual-predicate pass.
    IndexMerge {
        table: PlanTable,
        root: IndexMergeNode,
        filter: Option<Expr>,
        estimated_rows: f64,
        access_rows: f64,
        cost: crate::engine::cbo::Cost,
        possible_keys: Vec<String>,
        hint_note: Option<String>,
        projected_columns: Option<Arc<Vec<usize>>>,
    },
    IndexLookup {
        table: PlanTable,
        index: IndexSchema,
        key: IndexKey,
        exact: bool,
        filter: Option<Expr>,
        estimated_rows: f64,
        access_rows: f64,
        cost: crate::engine::cbo::Cost,
        possible_keys: Vec<String>,
        covering: bool,
        hint_note: Option<String>,
        /// Same role as `SeqScan::projected_columns`: non-covering lookups
        /// can decode only the listed heap columns when an upper plan node
        /// references a strict subset of the table. `None` keeps full-row
        /// materialisation. Covering lookups ignore this field (they emit
        /// rows straight from index entries).
        projected_columns: Option<Arc<Vec<usize>>>,
    },
    IndexMultiLookup {
        table: PlanTable,
        index: IndexSchema,
        keys: Vec<IndexKey>,
        exact: bool,
        filter: Option<Expr>,
        estimated_rows: f64,
        access_rows: f64,
        cost: crate::engine::cbo::Cost,
        possible_keys: Vec<String>,
        covering: bool,
        hint_note: Option<String>,
        /// Mirror of [`IndexLookup::projected_columns`] for the IN-list /
        /// multi-key flavour. Used in the non-covering branch to limit
        /// per-row heap decode width.
        projected_columns: Option<Arc<Vec<usize>>>,
    },
    IndexRangeScan {
        table: PlanTable,
        index: IndexSchema,
        low: Option<IndexBound>,
        high: Option<IndexBound>,
        filter: Option<Expr>,
        estimated_rows: f64,
        access_rows: f64,
        cost: crate::engine::cbo::Cost,
        possible_keys: Vec<String>,
        covering: bool,
        reverse: bool,
        hint_note: Option<String>,
        /// Mirror of `SeqScan::projected_columns` for non-covering range
        /// scans. When `Some`, the executor reads only the listed heap
        /// columns instead of decoding the full row, and downstream
        /// PlanRows carry only those fields. `None` keeps full-row
        /// materialisation. Filled in by `apply_column_pruning` when an
        /// upper plan node references a strict subset of the table.
        projected_columns: Option<Arc<Vec<usize>>>,
    },
    /// Index-only ("fast") scan: walk the B-tree leaf chain (optionally
    /// bounded by `low`/`high`) and emit rows directly from index
    /// entries without touching the heap. Selected when every required
    /// column lives in the index (declared key columns or INCLUDE
    /// columns) so a regular range scan would do redundant heap I/O.
    /// Cost is computed via `operator_cost::index_fast_scan` and is
    /// strictly cheaper than the equivalent `IndexRangeScan` because
    /// the heap-correlation term is gone.
    IndexFastScan {
        table: PlanTable,
        index: IndexSchema,
        low: Option<IndexBound>,
        high: Option<IndexBound>,
        filter: Option<Expr>,
        estimated_rows: f64,
        access_rows: f64,
        cost: crate::engine::cbo::Cost,
        possible_keys: Vec<String>,
        reverse: bool,
        hint_note: Option<String>,
        projected_columns: Option<Arc<Vec<usize>>>,
    },
    /// MySQL `MATCH(cols) AGAINST('q')` full-text search. The inverted
    /// index is the FULLTEXT B-tree (one `(token, row_id)` entry per
    /// token). The executor tokenises `query` with the index's own
    /// parser, unions the per-token postings into a candidate set
    /// (natural-language OR semantics), scores each candidate with BM25,
    /// and injects the relevance as a hidden `__ft_score_<fingerprint>`
    /// pseudo-column so WHERE / SELECT / ORDER BY occurrences of the same
    /// MATCH can read it back by fingerprint. Selected ahead of regular
    /// index access in `choose_scan`; never produced by the CBO's
    /// Binary-only constraint collection.
    FulltextScan {
        table: PlanTable,
        index: IndexSchema,
        /// Search mode (natural language vs boolean). Both modes are fully
        /// executed by `execute_fulltext_scan`; `WITH QUERY EXPANSION` is
        /// rejected at parse time.
        mode: crate::parser::FtMode,
        /// Raw AGAINST query text, tokenised at execution time.
        query: String,
        /// Deferred AGAINST(?) parameter. `query` is empty when present.
        query_param: Option<usize>,
        /// Normalised indexed column names this MATCH targets — already
        /// validated to equal the index's column set.
        columns: Vec<String>,
        /// `expr_fingerprint` of the originating `__match_against`
        /// Function expr, used as the score pseudo-column key.
        match_fingerprint: u64,
        /// Residual WHERE after the bare MATCH predicate is removed.
        filter: Option<Expr>,
        estimated_rows: f64,
        access_rows: f64,
        cost: crate::engine::cbo::Cost,
        possible_keys: Vec<String>,
        hint_note: Option<String>,
        projected_columns: Option<Arc<Vec<usize>>>,
        /// True only when the originating MATCH is itself a positive
        /// top-level AND conjunct. Other boolean contexts must scan the
        /// complete base row set and use 0.0 for non-matching documents.
        restrict_to_matches: bool,
        /// Other distinct MATCH expressions in WHERE/projection/order.
        additional_matches: Vec<FulltextMatchPlan>,
        /// Safe posting intersection for an ordinary LIKE residual over an
        /// existing single-column ngram FULLTEXT index.
        like_prefilter: Option<FulltextLikePrefilterPlan>,
        /// Optional B-tree row-id source for regular conjuncts. This never
        /// replaces MATCH evaluation: score maps are still built and the
        /// original residual predicate is evaluated for every candidate.
        base_access: Option<FulltextBaseAccess>,
        /// Whether the enclosing plan ever reads back this MATCH's BM25
        /// score (via `MATCH() AGAINST()` in SELECT/ORDER BY/HAVING or via
        /// the residual `filter`). When `false`, execution skips the
        /// per-term `by_row` hashmaps, the `doc_len_of` map and BM25 math
        /// — a large win for `WHERE MATCH() … ORDER BY <non-score>` shapes.
        /// Populated post-plan by [`crate::planner::mark_fulltext_score_needs`]
        /// which walks the whole plan tree; defaults to `true` (safe).
        needs_score: bool,
    },
    /// MySQL spatial-index access. A spatial predicate (`MBRContains` /
    /// `ST_Contains` / `ST_Intersects` / …) with one constant geometry
    /// operand and the other a column covered by a `SPATIAL` index is driven
    /// by the R-tree (`Database::spatial_search`): the constant geometry's MBR
    /// is the search window, the R-tree returns every row whose stored MBR
    /// intersects it (a correct superset), and the residual `filter` (the full
    /// original WHERE, including the exact spatial predicate) re-checks each
    /// candidate row. Selected in `choose_scan` ahead of a seq scan.
    SpatialScan {
        table: PlanTable,
        index: IndexSchema,
        /// Search-window MBR `(minx, miny, maxx, maxy)` — the constant
        /// geometry operand's bounding box.
        query_probe: SpatialProbe,
        /// Residual WHERE re-evaluated per candidate row (includes the exact
        /// spatial predicate, so the superset is filtered down precisely).
        filter: Option<Expr>,
        estimated_rows: f64,
        access_rows: f64,
        cost: crate::engine::cbo::Cost,
        possible_keys: Vec<String>,
        hint_note: Option<String>,
        projected_columns: Option<Arc<Vec<usize>>>,
    },
    /// MySQL-compatible vector KNN access via an HNSW index. Recognised from an
    /// `ORDER BY DISTANCE(col, const[, metric]) [ASC] LIMIT k` shape where `col`
    /// carries a Public `HNSW` index whose metric matches the query. The HNSW
    /// graph (`Database::vector_index_search_for_partition`) is a candidate *generator*: it
    /// returns the nearest `row_id`s, the executor re-reads each under MVCC
    /// visibility, re-ranks by the *live* heap vector (so UPDATE/DELETE staleness
    /// only costs recall, never correctness), applies the residual `filter`, and
    /// truncates to `k`. Because the scan already returns rows in distance order,
    /// `plan_satisfies_order_by` suppresses the redundant Sort/TopK. Selected in
    /// `choose_scan` ahead of a seq scan + full sort.
    VectorIndexScan {
        table: PlanTable,
        index: IndexSchema,
        /// Query vector in internal little-endian `f32` bytes. Literal KNN
        /// queries store their final value here. Prepared / auto-param KNN
        /// queries also keep `query_vector_expr` so execution can resolve the
        /// current parameter instead of baking the first vector into the plan.
        query_vector: crate::vector::VectorValue,
        /// Parameter-bearing query-vector expression, e.g. `?` or
        /// `STRING_TO_VECTOR(?)`, evaluated against the active parameter
        /// context at execution time.
        query_vector_expr: Option<Expr>,
        /// Canonical distance-metric name (`"L2"` / `"COSINE"` / `"DOT"`) —
        /// equals the index's configured metric (validated at plan time; a
        /// mismatch falls back to a seq scan + sort). Stored as a string so the
        /// `pub` plan enum doesn't leak the crate-internal `Metric` type; the
        /// executor parses it back at the scan boundary.
        metric: String,
        /// Number of nearest neighbours the scan returns = `offset + limit`.
        /// Planning-time peek when the LIMIT was auto-parameterized —
        /// `binding` re-resolves at execution (with `ef_search` re-clamped
        /// to the runtime `k`).
        k: usize,
        /// Planning-time OFFSET paired with `k`. Kept separately so a cached
        /// plan whose LIMIT is rebound can reconstruct `runtime_k =
        /// runtime_offset + runtime_limit` instead of treating the whole baked
        /// `k` as the LIMIT.
        baked_offset: usize,
        binding: LimitBinding,
        /// HNSW dynamic candidate-list size at layer 0 (clamped `>= k`); larger
        /// = higher recall, more work. This is the *planning-time* resolution;
        /// the executor re-resolves it from the live session (see `ef_hint`) so
        /// a cached plan honours a later `SET mydb_hnsw_ef_search`.
        ef_search: usize,
        /// Per-query `/*+ HNSW_EF(n) */` override, if any. Baked so the executor
        /// can re-resolve `ef_search` against the *current* session config
        /// (`effective_ef_search`) without losing an explicit hint — the plan
        /// cache key doesn't include `mydb_hnsw_ef_search`, so re-resolving at
        /// execution is what makes the session knob actually take effect.
        ef_hint: Option<u64>,
        /// The `ORDER BY` this scan satisfies — used by
        /// `plan_satisfies_order_by` to drop the redundant Sort and by EXPLAIN.
        order_by: Vec<OrderBy>,
        /// Residual WHERE re-evaluated per candidate row.
        filter: Option<Expr>,
        estimated_rows: f64,
        access_rows: f64,
        cost: crate::engine::cbo::Cost,
        possible_keys: Vec<String>,
        hint_note: Option<String>,
        projected_columns: Option<Arc<Vec<usize>>>,
    },
    /// Brute-force **exact** KNN over a fixed-dim `VECTOR(n)` column with no
    /// usable HNSW index — the `ORDER BY DISTANCE(col, const) LIMIT k` shape
    /// when the index path is unavailable (no index, metric mismatch,
    /// `/*+ NO_VECTOR_INDEX */`, or a table below the brute threshold) and
    /// there is no residual WHERE. The executor scores every visible row's
    /// vector bytes in place off the heap payload (zero `Value::Vector`
    /// copies), keeps a bounded top-`k`, and re-materialises only the
    /// winners — fanning the scan across cores. Like `VectorIndexScan` it
    /// returns rows pre-sorted ascending by distance, so `provided_order_prefix_len`
    /// suppresses the redundant Sort/TopK.
    VectorBruteScan {
        table: PlanTable,
        /// Physical position of the `VECTOR(n)` column in the row value array
        /// (== `schema.column_index(col)`).
        column_index: usize,
        /// Query vector in internal little-endian `f32` bytes. See
        /// `VectorIndexScan::query_vector_expr` for parameterized KNN queries.
        query_vector: crate::vector::VectorValue,
        query_vector_expr: Option<Expr>,
        /// Canonical metric name (`"L2"` / `"COSINE"` / `"DOT"`); the executor
        /// parses it back to the crate-internal `Metric`.
        metric: String,
        /// `offset + limit`. Re-resolved from `binding` at execution when the
        /// LIMIT was auto-parameterized.
        k: usize,
        /// Planning-time OFFSET; see `VectorIndexScan::baked_offset`.
        baked_offset: usize,
        binding: LimitBinding,
        /// The `ORDER BY` this scan satisfies (drives Sort suppression + EXPLAIN).
        order_by: Vec<OrderBy>,
        estimated_rows: f64,
        access_rows: f64,
        cost: crate::engine::cbo::Cost,
        possible_keys: Vec<String>,
    },
    IndexInLookup {
        table: PlanTable,
        index: IndexSchema,
        key_prefix: IndexKey,
        in_column: String,
        subquery: Box<Select>,
        subquery_plan: Box<PhysicalPlan>,
        filter: Option<Expr>,
        estimated_rows: f64,
        access_rows: f64,
        cost: crate::engine::cbo::Cost,
        possible_keys: Vec<String>,
        covering: bool,
        hint_note: Option<String>,
        /// Mirror of [`IndexLookup::projected_columns`] for the
        /// `column IN (subquery)` access path. Non-covering branch
        /// honours this to prune per-row heap decode width — important
        /// because IN-subquery typically drives a large fan-out of
        /// row-id lookups against a wide fact table.
        projected_columns: Option<Arc<Vec<usize>>>,
    },
    NestedLoopJoin {
        left: Box<PhysicalPlan>,
        right: Box<PhysicalPlan>,
        join_type: JoinType,
        condition: Option<Expr>,
        estimated_rows: f64,
        cost: crate::engine::cbo::Cost,
    },
    IndexNestedLoopJoin {
        left: Box<PhysicalPlan>,
        right_table: PlanTable,
        right_index: IndexSchema,
        join_type: JoinType,
        condition: Option<Expr>,
        eq_prefix_len: usize,
        estimated_rows: f64,
        cost: crate::engine::cbo::Cost,
        right_covering: bool,
        /// Non-covering INLJ only needs the subset of right-table heap
        /// columns referenced above the join. `None` preserves full-row
        /// materialisation, for example under SELECT *.
        right_projected_columns: Option<Arc<Vec<usize>>>,
        /// Runtime filters pushed through a parent hash join onto the
        /// INLJ's right-table rows. Empty for ordinary planned INLJs;
        /// executor runtime-filter injection fills it on cloned plans.
        right_runtime_filters: Vec<RuntimeFilter>,
    },
    HashJoin {
        left: Box<PhysicalPlan>,
        right: Box<PhysicalPlan>,
        join_type: JoinType,
        left_keys: Vec<Expr>,
        right_keys: Vec<Expr>,
        null_safe: Vec<bool>,
        build_side: BuildSide,
        residual: Option<Expr>,
        /// S2 join-level projection pruning: the positions within this node's output (the left ++
        /// right concatenation order; Semi/Anti is left only) of columns still referenced by upper
        /// layers. `None` = keep everything. Computed top-down by `apply_join_output_pruning` in the
        /// finish_plan stage; all executor hash join paths (materialized + chained streaming) honor it
        /// uniformly, and `collect_plan_fields` reflects it in sync so upper-layer position compilation stays consistent.
        output_projection: Option<std::sync::Arc<Vec<usize>>>,
        estimated_rows: f64,
        cost: crate::engine::cbo::Cost,
    },
    AdaptiveJoin {
        left: Box<PhysicalPlan>,
        right: Box<PhysicalPlan>,
        join_type: JoinType,
        candidates: Vec<AdaptiveJoinCandidate>,
        preferred: AdaptiveJoinChoice,
        estimated_rows: f64,
        cost: crate::engine::cbo::Cost,
    },
    HashAggregate {
        input: Box<PhysicalPlan>,
        group_by: Vec<Expr>,
        having: Option<Expr>,
        /// Phase A: deduplicated aggregate function calls referenced by
        /// projection / HAVING / ORDER BY. Pre-computed at plan time so
        /// the executor can build per-group accumulators without
        /// retaining input rows.
        aggregates: Vec<Expr>,
        estimated_rows: f64,
        cost: crate::engine::cbo::Cost,
    },
    HashRollupAggregate {
        input: Box<PhysicalPlan>,
        group_by: Vec<Expr>,
        having: Option<Expr>,
        aggregates: Vec<Expr>,
        estimated_rows: f64,
        cost: crate::engine::cbo::Cost,
    },
    HashGroupingSetsAggregate {
        input: Box<PhysicalPlan>,
        group_by: Vec<Expr>,
        grouping_sets: Vec<Vec<usize>>,
        having: Option<Expr>,
        aggregates: Vec<Expr>,
        estimated_rows: f64,
        cost: crate::engine::cbo::Cost,
    },
    Sort {
        input: Box<PhysicalPlan>,
        order_by: Vec<OrderBy>,
        estimated_rows: f64,
        cost: crate::engine::cbo::Cost,
    },
    /// Sort only the suffix of ORDER BY when the input is already
    /// ordered by a non-empty prefix. This is the row-executor analogue
    /// of PostgreSQL incremental sort: contiguous groups sharing the
    /// prefix are sorted independently by the full key.
    IncrementalSort {
        input: Box<PhysicalPlan>,
        order_by: Vec<OrderBy>,
        presorted_prefix: usize,
        estimated_rows: f64,
        cost: crate::engine::cbo::Cost,
    },
    /// Phase B: bounded-memory ORDER BY + LIMIT. Equivalent to
    /// `Limit(offset, limit) -> Sort(order_by) -> input` but uses a
    /// fixed-capacity binary heap of size `k = offset + limit` so peak
    /// memory is O(k) regardless of input cardinality.
    TopK {
        input: Box<PhysicalPlan>,
        order_by: Vec<OrderBy>,
        /// Projection tuple used for bounded duplicate elimination. Present
        /// only for the safe `DISTINCT + ORDER BY(projected exprs) + LIMIT`
        /// rewrite.
        dedup_projection: Option<Vec<SelectItem>>,
        /// Heap capacity (`offset + limit`). The TopK operator emits up
        /// to `k` rows, sorted; downstream Limit then drops the first
        /// `offset` and keeps `limit`. Planning-time peek when the
        /// LIMIT was auto-parameterized — `binding` re-resolves at
        /// execution.
        k: usize,
        offset: usize,
        binding: LimitBinding,
        estimated_rows: f64,
        cost: crate::engine::cbo::Cost,
    },
    /// Deep-page ORDER BY + LIMIT/OFFSET. Unlike `TopK`, this node emits
    /// exactly the requested page window, so no downstream `Limit` should
    /// be added for the same SELECT.
    TopWindow {
        input: Box<PhysicalPlan>,
        order_by: Vec<OrderBy>,
        limit: usize,
        offset: usize,
        binding: LimitBinding,
        estimated_rows: f64,
        cost: crate::engine::cbo::Cost,
    },
    Limit {
        input: Box<PhysicalPlan>,
        limit: Option<usize>,
        offset: Option<usize>,
        binding: LimitBinding,
        estimated_rows: f64,
        cost: crate::engine::cbo::Cost,
    },
    Projection {
        input: Box<PhysicalPlan>,
        projection: Vec<SelectItem>,
        distinct: bool,
        estimated_rows: f64,
        cost: crate::engine::cbo::Cost,
    },
    /// Apply a boolean `predicate` to every input row, emitting only rows for
    /// which it evaluates to TRUE. Unlike a join `residual`/ON condition, this
    /// is a genuine post-join WHERE filter: it drops NULL-extended outer-join
    /// rows that fail the predicate (rather than preserving them). The planner
    /// emits it when a WHERE conjunct that references the null-supplied side of
    /// an OUTER join cannot be folded into a scan/inner-join (folding it into an
    /// outer join's residual would wrongly null-extend instead of filter —
    /// the long-standing TPC-DS Q78 `LEFT JOIN ... WHERE inner.col` bug).
    Filter {
        input: Box<PhysicalPlan>,
        predicate: Expr,
        estimated_rows: f64,
        cost: crate::engine::cbo::Cost,
    },
    /// Batched parameterized execution for a predicate that still contains a
    /// scalar/EXISTS/IN correlated subquery after decorrelation. `outer` is
    /// opened once and the predicate is evaluated over its current batch;
    /// correlation-key memoization lives in the shared ExecutionContext.
    /// This is a physical operator, not a signal to restart the row executor.
    Apply {
        outer: Box<PhysicalPlan>,
        predicate: Expr,
        estimated_rows: f64,
        cost: crate::engine::cbo::Cost,
    },
    /// Compute one or more window functions over the input. Each `windows`
    /// entry produces a virtual column whose name is `output_alias`. Multiple
    /// windows that share the same `(partition_by, order_by)` are evaluated
    /// after a single sort pass.
    WindowAgg {
        input: Box<PhysicalPlan>,
        windows: Vec<PhysicalWindowSpec>,
        estimated_rows: f64,
        cost: crate::engine::cbo::Cost,
    },
    /// Read rows from a materialised CTE result held in the active CTE
    /// registry. Used by the planner whenever a FROM/JOIN target resolves
    /// to a CTE name rather than a real table.
    CteScan {
        name: String,
        alias: Option<String>,
        columns: Vec<crate::types::Column>,
        filter: Option<Expr>,
        estimated_rows: f64,
        cost: crate::engine::cbo::Cost,
        /// When `Some`, the executor only constructs `PlanField`s for the
        /// listed CTE-column indices (against `columns`). Non-referenced
        /// columns are skipped, avoiding the per-row `Value::clone()` they
        /// would otherwise cost. `None` keeps the full-row reconstruction.
        /// Filled in by `apply_column_pruning` once the upper plan node's
        /// column requirements are known.
        projected_columns: Option<Arc<Vec<usize>>>,
    },
    /// PostgreSQL `generate_series(...)` table function leaf. Emits one
    /// `BIGINT` column (integer family; numeric / timestamp families land in
    /// M4) plus an optional 1-based ordinality column. Currently materialised
    /// by `execute_plan_rows_inner`; a streaming / vectorized leaf is M5. See
    /// `docs/generate-series-design.md`.
    GenerateSeries {
        series: SeriesKind,
        /// Relation name for column qualification (`generate_series` or the
        /// `AS t(...)` table alias).
        relation: String,
        /// Output column name (`generate_series` by default, else the first
        /// `AS t(name)` alias).
        output_name: String,
        with_ordinality: bool,
        /// Ordinality column name (`ordinality` by default); empty string when
        /// `with_ordinality` is false.
        ordinality_name: String,
        /// Result column metadata, in output order.
        columns: Vec<crate::types::Column>,
        /// Residual WHERE predicate, evaluated per generated row.
        filter: Option<Expr>,
        estimated_rows: f64,
        cost: crate::engine::cbo::Cost,
    },
    /// Phase B #B4: streaming aggregate. Selected by `finish_plan`
    /// when the input plan's row order already matches `group_by`.
    /// Walks rows in order and emits exactly one output per group
    /// transition, allocating no hash table.
    StreamingAggregate {
        input: Box<PhysicalPlan>,
        group_by: Vec<Expr>,
        having: Option<Expr>,
        /// Phase A: pre-collected aggregate calls (mirrors HashAggregate).
        aggregates: Vec<Expr>,
        estimated_rows: f64,
        cost: crate::engine::cbo::Cost,
    },
    StreamingRollupAggregate {
        input: Box<PhysicalPlan>,
        group_by: Vec<Expr>,
        having: Option<Expr>,
        aggregates: Vec<Expr>,
        estimated_rows: f64,
        cost: crate::engine::cbo::Cost,
    },
    StreamingGroupingSetsAggregate {
        input: Box<PhysicalPlan>,
        group_by: Vec<Expr>,
        grouping_sets: Vec<Vec<usize>>,
        having: Option<Expr>,
        aggregates: Vec<Expr>,
        estimated_rows: f64,
        cost: crate::engine::cbo::Cost,
    },
    /// Phase B #B4: streaming distinct. Selected when the input
    /// plan is sorted on the projected columns. Compares each
    /// incoming projection to the last-emitted one and skips
    /// duplicates without a HashSet.
    StreamingDistinct {
        input: Box<PhysicalPlan>,
        projection: Vec<SelectItem>,
        estimated_rows: f64,
        cost: crate::engine::cbo::Cost,
    },
    /// Aggregate-pushdown to a covering index. Replaces
    /// `HashAggregate(input=base scan)` for narrow single-table shapes:
    /// no GROUP BY / HAVING / DISTINCT, projection holds exactly one of
    /// `COUNT(*)`, `MIN(col)`, or `MAX(col)`. COUNT can carry optional
    /// index bounds when the WHERE clause is fully represented by the
    /// chosen index range.
    /// Yields a single row.
    IndexAggregate {
        table: PlanTable,
        index: IndexSchema,
        low: Option<IndexBound>,
        high: Option<IndexBound>,
        /// Residual predicate for filtered MIN/MAX edge walks. COUNT and
        /// unfiltered MIN/MAX keep this empty. Execution applies it to heap
        /// rows in index order and stops at the first qualifying non-NULL
        /// aggregate value.
        filter: Option<Expr>,
        op: IndexAggOp,
        /// `aggregate_key(expr)` of the source aggregate call so the
        /// projection layer's `agg_values` cache lookup hits.
        agg_key: u64,
        output_alias: String,
        output_type: crate::types::ColumnType,
        estimated_rows: f64,
        access_rows: f64,
        cost: crate::engine::cbo::Cost,
    },
    /// General covering-index aggregate. Workers/executors scan index
    /// leaf entries and update aggregate state directly; they never
    /// materialise one `PlanRow` per index entry.
    CoveringIndexAggregate {
        table: PlanTable,
        index: IndexSchema,
        low: Option<IndexBound>,
        high: Option<IndexBound>,
        group_by: Vec<Expr>,
        having: Option<Expr>,
        aggregates: Vec<Expr>,
        estimated_rows: f64,
        access_rows: f64,
        cost: crate::engine::cbo::Cost,
    },
    /// Parallel boundary. Spawns `degree` worker threads that each
    /// produce a slice of `input`'s output. Workers run independently
    /// (no inter-worker communication) and the Exchange node concatenates
    /// their results in the order specified by `output_order`.
    ///
    /// `Unordered` simply concats per-worker outputs (cheapest, used
    /// when downstream is ORDER-BY-agnostic). `Sorted` performs a
    /// k-way merge using `merge_keys` (used when an upstream
    /// `ParallelSort` has already locally sorted each partition).
    Exchange {
        input: Box<PhysicalPlan>,
        degree: u32,
        output_order: ExchangeOrder,
        merge_keys: Vec<crate::parser::OrderBy>,
        estimated_rows: f64,
        cost: crate::engine::cbo::Cost,
    },
    /// Parallel sequential scan. Each of `degree` workers scans a
    /// disjoint contiguous heap-page range `[lo, hi)` computed at
    /// runtime as `page_count / degree`. Filter, runtime filters,
    /// and column pruning apply per-worker exactly as in `SeqScan`.
    /// Must always sit underneath a `Exchange` (the planner enforces
    /// this — direct execution of a `ParallelSeqScan` without a
    /// surrounding Exchange is a planner bug).
    ParallelSeqScan {
        table: PlanTable,
        filter: Option<Expr>,
        estimated_rows: f64,
        access_rows: f64,
        cost: crate::engine::cbo::Cost,
        possible_keys: Vec<String>,
        runtime_filters: Vec<RuntimeFilter>,
        hint_note: Option<String>,
        projected_columns: Option<Arc<Vec<usize>>>,
        /// Subset of `projected_columns` whose values survive into the
        /// downstream `PlanRow`. Empty / `None` means "carry every
        /// projected column", matching the legacy behaviour. When the
        /// planner can prove that some columns are referenced *only*
        /// by the scan's pushed-down filter and nowhere else in the
        /// plan, those positions are excluded here so the per-row
        /// `PlanRow` build skips the `values[i].clone()` (typically a
        /// heap `Value::String` clone) entirely. Critical for queries
        /// like TPC-H Q13 where the filter column is a wide `VARCHAR`
        /// that never flows past the scan.
        ///
        /// The indices are *positions within `projected_columns`*, not
        /// schema column indices — so the streaming probe can look up
        /// `values[pos]` directly.
        output_columns_in_projection: Option<Arc<Vec<usize>>>,
        degree: u32,
    },
    /// Parallel counterpart of [`PhysicalPlan::ZoneMapScan`]. Candidate page
    /// ranges are rebuilt once by the coordinator and scheduled as dynamic
    /// morsels; workers never consult Zone Map metadata independently.
    ParallelZoneMapScan {
        table: PlanTable,
        filter: Option<Expr>,
        estimated_rows: f64,
        access_rows: f64,
        cost: crate::engine::cbo::Cost,
        possible_keys: Vec<String>,
        runtime_filters: Vec<RuntimeFilter>,
        hint_note: Option<String>,
        projected_columns: Option<Arc<Vec<usize>>>,
        output_columns_in_projection: Option<Arc<Vec<usize>>>,
        degree: u32,
        zone_map: ZoneMapPlanInfo,
    },
    /// Parallel index-only full/range scan. Each worker walks a
    /// disjoint slice of the B-tree leaf chain. It is unordered by
    /// design and must sit underneath a `Exchange`, so ordered index
    /// scans stay serial.
    ParallelIndexFastScan {
        table: PlanTable,
        index: IndexSchema,
        low: Option<IndexBound>,
        high: Option<IndexBound>,
        filter: Option<Expr>,
        estimated_rows: f64,
        access_rows: f64,
        cost: crate::engine::cbo::Cost,
        possible_keys: Vec<String>,
        hint_note: Option<String>,
        projected_columns: Option<Arc<Vec<usize>>>,
        degree: u32,
    },
    /// Parallel `IndexRangeScan` (heap-fetching variant). Each worker
    /// walks a disjoint slice of the B-tree leaf chain and does the
    /// per-row heap fetch + projection itself. Unordered by design and
    /// must sit underneath a `Exchange`. Selected by `parallelize_seq_scans`
    /// when the underlying range is large enough that the per-worker
    /// heap-fetch CPU dominates the leaf-traversal cost — see
    /// `cbo_wants_parallel_for_index_range_scan`.
    ParallelIndexRangeScan {
        table: PlanTable,
        index: IndexSchema,
        low: Option<IndexBound>,
        high: Option<IndexBound>,
        filter: Option<Expr>,
        estimated_rows: f64,
        access_rows: f64,
        cost: crate::engine::cbo::Cost,
        possible_keys: Vec<String>,
        hint_note: Option<String>,
        projected_columns: Option<Arc<Vec<usize>>>,
        degree: u32,
    },
    /// Parallel version of `CoveringIndexAggregate`. Each worker scans
    /// a disjoint leaf slice and returns partial accumulator state,
    /// which the coordinator merges by group key.
    ParallelIndexAggregate {
        table: PlanTable,
        index: IndexSchema,
        low: Option<IndexBound>,
        high: Option<IndexBound>,
        group_by: Vec<Expr>,
        having: Option<Expr>,
        aggregates: Vec<Expr>,
        estimated_rows: f64,
        access_rows: f64,
        cost: crate::engine::cbo::Cost,
        degree: u32,
    },
    /// Parallel hash aggregate. Phase 3 design: materialise the input
    /// once, hash-partition rows by `group_by` keys into `degree`
    /// buckets, run an independent serial `HashAggregate` inside
    /// each worker, then concatenate per-worker outputs. Hash
    /// partitioning ensures every group lives in exactly one bucket,
    /// so no per-group state merging is required.
    ///
    /// Falls back to serial when the input is already so small that
    /// the partitioning + spawn overhead exceeds the savings — the
    /// executor handles that gate at runtime.
    ParallelHashAggregate {
        input: Box<PhysicalPlan>,
        group_by: Vec<Expr>,
        having: Option<Expr>,
        aggregates: Vec<Expr>,
        degree: u32,
        estimated_rows: f64,
        cost: crate::engine::cbo::Cost,
    },
    ParallelRollupAggregate {
        input: Box<PhysicalPlan>,
        group_by: Vec<Expr>,
        having: Option<Expr>,
        aggregates: Vec<Expr>,
        degree: u32,
        estimated_rows: f64,
        cost: crate::engine::cbo::Cost,
    },
    ParallelGroupingSetsAggregate {
        input: Box<PhysicalPlan>,
        group_by: Vec<Expr>,
        grouping_sets: Vec<Vec<usize>>,
        having: Option<Expr>,
        aggregates: Vec<Expr>,
        degree: u32,
        estimated_rows: f64,
        cost: crate::engine::cbo::Cost,
    },
    /// Parallel sort. Phase 4 design: materialise input rows, split
    /// into `degree` equal-size chunks (no sample/pivot pass — chunks
    /// can have any content), each worker locally sorts its chunk,
    /// then a final k-way merge yields the globally sorted result.
    /// This avoids the data-dependent skew of sample-pivot routing
    /// and gives O(N log N / P + N log P) work overall.
    ParallelSort {
        input: Box<PhysicalPlan>,
        order_by: Vec<crate::parser::OrderBy>,
        degree: u32,
        estimated_rows: f64,
        cost: crate::engine::cbo::Cost,
    },
    /// Parallel TopK. Variant of `TopK` for inputs whose subtree is
    /// already parallel-eligible (e.g. wraps a `Exchange` /
    /// `ParallelHashAggregate`). Each worker maintains a local
    /// `BoundedTopK<k>` over its chunk, so per-worker CPU is
    /// `(N/P) log k` instead of `(N/P) log (N/P)` for a parallel sort
    /// — and per-worker memory is `O(k)` instead of `O(N/P)`. The
    /// final merge sees at most `P * k` rows and is itself bounded.
    ///
    /// The planner emits this in `parallelize_sort` (which also handles
    /// `TopK` upgrades). The executor mirrors `execute_parallel_sort`
    /// but per-worker uses the `BoundedTopK` heap. `k` carries
    /// `offset + limit` exactly like the serial `TopK`; downstream
    /// `Limit` strips the first `offset` rows.
    ParallelTopK {
        input: Box<PhysicalPlan>,
        order_by: Vec<crate::parser::OrderBy>,
        dedup_projection: Option<Vec<SelectItem>>,
        k: usize,
        offset: usize,
        binding: LimitBinding,
        degree: u32,
        estimated_rows: f64,
        cost: crate::engine::cbo::Cost,
    },
}

#[derive(Debug, Clone)]
pub enum SpatialProbe {
    Constant([f64; 4]),
    Parameter(Expr),
}

/// Runtime re-resolution indices for LIMIT / OFFSET counts that were
/// auto-parameterized (`crate::engine::auto_param`) or written as `?`
/// placeholders. The baked `k` / `limit` / `offset` fields on the plan
/// node hold the planning-time peek (used for plan-shape and cost
/// decisions); when a binding index is `Some`, executors re-read
/// parameter `idx` from the thread-local parameter context at execution
/// time so one cached plan serves every literal value sharing the shape.
/// `None` indices mean the baked count is authoritative (the literal
/// case — exactly today's behavior).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct LimitBinding {
    pub limit_param: Option<usize>,
    pub offset_param: Option<usize>,
}

impl LimitBinding {
    /// Re-resolve the LIMIT count against the active parameter context.
    /// Falls back to `baked` when the binding is absent or the context
    /// has no usable value (never happens for plans reached through an
    /// auto-param fingerprint key, which always binds its params first).
    pub fn resolve_limit(&self, baked: Option<usize>) -> Option<usize> {
        match self.limit_param {
            Some(idx) => crate::engine::params::current_param_usize(idx).or(baked),
            None => baked,
        }
    }

    pub fn resolve_offset(&self, baked: Option<usize>) -> Option<usize> {
        match self.offset_param {
            Some(idx) => crate::engine::params::current_param_usize(idx).or(baked),
            None => baked,
        }
    }

    /// Re-resolve a TopK-style heap bound. `baked_k = offset + limit`
    /// from planning time; the runtime value recombines the re-resolved
    /// components, falling back to the baked split when unbound.
    pub fn resolve_k_offset(&self, baked_k: usize, baked_offset: usize) -> (usize, usize) {
        let offset = self
            .resolve_offset(Some(baked_offset))
            .unwrap_or(baked_offset);
        let baked_limit = baked_k.saturating_sub(baked_offset);
        let limit = self.resolve_limit(Some(baked_limit)).unwrap_or(baked_limit);
        (offset.saturating_add(limit), offset)
    }

    pub fn resolve_limit_offset(&self, baked_limit: usize, baked_offset: usize) -> (usize, usize) {
        let limit = self.resolve_limit(Some(baked_limit)).unwrap_or(baked_limit);
        let offset = self
            .resolve_offset(Some(baked_offset))
            .unwrap_or(baked_offset);
        (limit, offset)
    }
}

/// Extract the parameter indices of placeholder LIMIT / OFFSET clauses on
/// a SELECT, for stamping into limit-consuming plan nodes.
fn limit_binding_of(select: &Select) -> LimitBinding {
    LimitBinding {
        limit_param: match select.limit {
            Some(LimitValue::Placeholder(idx) | LimitValue::OffsetCommaPlaceholder(idx)) => {
                Some(idx)
            }
            _ => None,
        },
        offset_param: match select.offset {
            Some(LimitValue::Placeholder(idx) | LimitValue::OffsetCommaPlaceholder(idx)) => {
                Some(idx)
            }
            _ => None,
        },
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExchangeOrder {
    /// Concatenate per-worker outputs without re-ordering. Cheapest.
    Unordered,
    /// k-way merge per-worker outputs assuming each is locally sorted
    /// by `Exchange.merge_keys`. Used together with `ParallelSort`.
    Sorted,
}

#[derive(Debug, Clone, PartialEq)]
pub enum IndexAggOp {
    CountStar,
    /// `COUNT(col)` over a column that is *covered* by the chosen index.
    /// Behaves like `CountStar` but skips entries whose key value for
    /// `col` is NULL. For NOT NULL columns the planner downgrades to
    /// `CountStar` (since no entry can be NULL); only nullable columns
    /// reach this variant.
    CountColumn(String),
    /// `COUNT(DISTINCT col)` when `col` is the complete leading column
    /// of the chosen B-tree. Equal SQL values are contiguous in index
    /// order, so execution needs only the previous visible key.
    CountDistinctColumn(String),
    Min(String),
    Max(String),
}

#[derive(Debug, Clone)]
pub struct PhysicalWindowSpec {
    pub name: String,
    pub args: Vec<Expr>,
    pub partition_by: Vec<Expr>,
    pub order_by: Vec<OrderBy>,
    pub frame: Option<crate::parser::WindowFrame>,
    pub output_alias: String,
    pub output_type: crate::types::ColumnType,
    /// Top-N pushdown: when `Some(k)`, the enclosing query filters this
    /// window's output column with `alias <= k` (or `< k+1`) and the window
    /// is `RANK`/`DENSE_RANK`/`ROW_NUMBER`, so the operator may keep only the
    /// rank-bounded prefix of each partition and drop the rest. `None`
    /// (the default) computes every row's window value as usual.
    pub top_k: Option<usize>,
}

#[derive(Debug, Clone)]
pub struct PlanTable {
    /// Table identifier captured at planning time. The plan-cache uses this
    /// (paired with `schema`'s schema_version) to detect when a cached plan
    /// references a table that has since been altered. The cached `schema`
    /// Arc is only authoritative during plan construction; executors should
    /// re-resolve through the live catalog by id when the plan is reused.
    pub table_id: u64,
    pub schema: Arc<TableSchema>,
    pub alias: Option<String>,
    pub system_time: Option<crate::parser::SystemTimeAsOf>,
    /// Lazily-populated shared ColMeta handle for every schema
    /// column (indexed by schema column index). Shared across `PlanTable`
    /// clones via the inner `Arc`, so the cache populates once per
    /// (table, alias) shape and every subsequent row materialiser
    /// (`stored_row_to_plan_row` / `projected_values_to_plan_row`) just
    /// indexes the slice — no per-cell hash + 3 × String::clone in
    /// `intern_col_meta`. Hot path: TPC-H Q5 worker threads spent ~38 %
    /// of CPU re-interning ColMeta per cell before this cache existed.
    pub(crate) column_metas: Arc<OnceLock<Vec<crate::engine::executor::select::plan::ColMetaRef>>>,
    /// Shared row schema for the common full logical-row materializer.
    pub(crate) row_schema: Arc<OnceLock<Arc<crate::engine::executor::select::plan::RowSchema>>>,
    /// Live sidecar "modified rows since last ANALYZE" delta, snapshot
    /// at planning time so EXPLAIN's `stats: stale` diagnostic stays
    /// accurate after the DML hot path moved its bookkeeping out of
    /// `schema.stats.modified_rows_since_analyze` into a lock-free
    /// atomic. Set by the executor entry-points that have a `Database`
    /// reference; constructors that don't pass `0`.
    pub(crate) stats_sidecar_delta: u64,
    /// Committed INSERT minus DELETE rows since ANALYZE. UPDATE contributes
    /// zero. This is separate from the unsigned touched-row staleness counter.
    pub(crate) live_row_delta: i64,
    pub partition_pruning: crate::storage::PartitionPruningPlan,
    /// Explicit PARTITION selection belongs to this table reference, including
    /// its alias. It must never constrain another scan or nested statement.
    pub explicit_partition_storage_ids: Option<Vec<u64>>,
}

impl PlanTable {
    /// Construct a `PlanTable` with a fresh empty meta cache.
    pub(crate) fn new(table_id: u64, schema: Arc<TableSchema>, alias: Option<String>) -> Self {
        Self {
            table_id,
            schema,
            alias,
            system_time: None,
            column_metas: Arc::new(OnceLock::new()),
            row_schema: Arc::new(OnceLock::new()),
            stats_sidecar_delta: 0,
            live_row_delta: 0,
            partition_pruning: crate::storage::PartitionPruningPlan::All,
            explicit_partition_storage_ids: None,
        }
    }

    pub fn resolved_partition_storage_ids(&self, filter: Option<&Expr>) -> Option<Vec<u64>> {
        let planned = match &self.partition_pruning {
            crate::storage::PartitionPruningPlan::All => None,
            crate::storage::PartitionPruningPlan::Static(set) => {
                let leaves = self.schema.storage_ids();
                Some(
                    set.ordinals()
                        .filter_map(|ordinal| leaves.get(ordinal).copied())
                        .collect(),
                )
            }
            crate::storage::PartitionPruningPlan::Program => {
                crate::engine::executor::partition_router::pruned_storage_ids_from_filter(
                    &self.schema,
                    filter,
                )
            }
        };
        match (&self.explicit_partition_storage_ids, planned) {
            (Some(explicit), Some(mut planned)) => {
                planned.retain(|id| explicit.contains(id));
                Some(planned)
            }
            (Some(explicit), None) => Some(explicit.clone()),
            (None, planned) => planned,
        }
    }

    pub(crate) fn in_scope_storage_ids(&self) -> Vec<u64> {
        let mut ids = crate::engine::executor::partition_router::in_scope_storage_ids(&self.schema);
        if let Some(planned) = self.resolved_partition_storage_ids(None) {
            ids.retain(|id| planned.contains(id));
        }
        ids
    }

    /// Same as [`PlanTable::new`] but folds the lock-free sidecar
    /// "modified rows since ANALYZE" delta into the plan so EXPLAIN's
    /// `stats: stale` diagnostic still trips on schema-state changes
    /// the DML hot path no longer records into `schema.stats` itself.
    pub(crate) fn with_sidecar(
        table_id: u64,
        schema: Arc<TableSchema>,
        alias: Option<String>,
        sidecar_delta: u64,
        live_row_delta: i64,
    ) -> Self {
        Self {
            table_id,
            schema,
            alias,
            system_time: None,
            column_metas: Arc::new(OnceLock::new()),
            row_schema: Arc::new(OnceLock::new()),
            stats_sidecar_delta: sidecar_delta,
            live_row_delta,
            partition_pruning: crate::storage::PartitionPruningPlan::All,
            explicit_partition_storage_ids: None,
        }
    }

    fn with_system_time(mut self, system_time: Option<crate::parser::SystemTimeAsOf>) -> Self {
        self.system_time = system_time;
        self
    }
}

fn plan_table_for_ref_if_resolved(db: &Database, table_ref: &TableRef) -> Option<PlanTable> {
    if let Some(PhysicalPlan::CteScan { columns, .. }) = try_plan_cte_scan(table_ref) {
        let schema = TableSchema::synthetic_for_planning(&table_ref.name, &columns);
        return Some(PlanTable::new(0, Arc::new(schema), table_ref.alias.clone()));
    }
    if let Ok(columns) = crate::engine::information_schema::columns_for_table_name(&table_ref.name)
    {
        let schema = TableSchema::synthetic_for_planning(&table_ref.name, &columns);
        return Some(PlanTable::new(0, Arc::new(schema), table_ref.alias.clone()));
    }
    db.table(&table_ref.name).ok().map(|schema| {
        let sidecar = db.table_modification_delta(schema.id);
        let live_delta = db.table_live_row_delta(schema.id);
        PlanTable::with_sidecar(
            schema.id,
            schema,
            table_ref.alias.clone(),
            sidecar,
            live_delta,
        )
    })
}

#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct PlannedSelect {
    pub physical: PhysicalPlan,
    /// Combined hint diagnostics: parser-level syntax issues plus
    /// planner-level conflicts, missing-object lookups, and CBO
    /// non-adoption reasons. Surfaced in `EXPLAIN` via the first row's
    /// `Extra` column.
    pub hint_warnings: Vec<String>,
}

#[derive(Debug, Clone)]
pub struct ExplainRow {
    pub id: usize,
    pub select_type: String,
    pub table: String,
    pub partitions: String,
    pub access_type: String,
    pub possible_keys: String,
    pub key: String,
    pub key_len: String,
    pub ref_: String,
    pub rows: usize,
    pub filtered: f64,
    pub extra: String,
    /// Deterministic preorder identity of the originating plan node. `None`
    /// for synthetic rows that don't correspond to a single plan node
    /// (CTE wrappers, window-step rows, subquery placeholders, etc.).
    /// Not serialised.
    pub plan_id: Option<PlanNodeId>,
}

/// Master switch for the [`crate::planner_decorrelate`] passes. Defaults
/// to ON. Set `MYDB_DECORRELATE=off` (or `0` / `false`) to fall back to
/// the legacy eval-time path — useful for differential testing,
/// regression triage, and benchmark baselines.
fn decorrelate_enabled() -> bool {
    decorrelate_enabled_external()
}

/// Public alias so executor / explain callers can gate the same way.
pub(crate) fn decorrelate_enabled_external() -> bool {
    let resolve = || {
        !matches!(
            crate::util::env::var("MYDB_DECORRELATE")
                .unwrap_or_default()
                .to_ascii_lowercase()
                .as_str(),
            "off" | "0" | "false" | "no"
        )
    };
    #[cfg(not(test))]
    {
        static ENABLED: OnceLock<bool> = OnceLock::new();
        *ENABLED.get_or_init(resolve)
    }
    #[cfg(test)]
    {
        resolve()
    }
}

pub fn plan_select(db: &Database, select: &Select) -> Result<PlannedSelect, QueryError> {
    crate::engine::params::validate_limit(select.limit.as_ref())?;
    crate::engine::params::validate_limit(select.offset.as_ref())?;
    let _optimizing = crate::engine::cancel::enter_process_stage(
        crate::engine::session_registry::ProcessState::Optimizing,
    );
    // Tier 0 #1: normalize predicates (OR-chain → IN, NOT pushdown,
    // BETWEEN low>high → false, bool literal short-circuit) before
    // logical/physical planning so downstream selectivity estimates
    // and constraint extraction see the canonical form.
    //
    // NOTE: scalar-subquery decorrelation does NOT run here; it runs
    // earlier (in `execute_select` for execution, and in the EXPLAIN
    // entrypoint for EXPLAIN) because the rewrite may add derived
    // joins that the executor's CTE-join routing handles but
    // `plan_select` itself doesn't natively plan. Running it twice
    // would also double-rewrite already-decorrelated input.
    let mut select_owned = select.clone();
    resolve_wildcard_order_ordinals(db, &mut select_owned)?;
    resolve_join_using_natural(db, &mut select_owned)?;
    resolve_group_by_refs(db, &mut select_owned);
    prune_redundant_constant_group_keys(&mut select_owned);
    prune_redundant_functional_group_keys(db, &mut select_owned);
    bind_rollup_expressions(&mut select_owned)?;
    validate_select_column_references(db, &select_owned)?;
    rewrite_enum_set_numeric_semantics(db, &mut select_owned);
    normalize_select_predicates(db, &mut select_owned);
    // Sideways predicate propagation: push small-dimension constant
    // filters across multi-hop inner-join equi-chains so the fact
    // table sees its effective cardinality drop before join-order
    // DP runs. Synthetic predicates are uncorrelated `IN (subq)` so
    // the existing IN-subquery evaluation path picks them up.
    propagate_sideways_dimension_filters(db, &mut select_owned);
    let (physical_res, plan_warnings) =
        with_hint_warning_collector(|| build_physical_plan(db, &select_owned));
    let mut physical = physical_res?;
    index_residual::prune(&mut physical);
    lower_remaining_subquery_filters_to_apply(&mut physical);
    mark_fulltext_score_needs(&mut physical);
    crate::engine::cbo::trace::finish(&physical);
    let mut hint_warnings = select.hint_warnings.clone();
    hint_warnings.extend(validate_query_hints(db, &select_owned));
    hint_warnings.extend(plan_warnings);
    dedup_preserving_order(&mut hint_warnings);
    Ok(PlannedSelect {
        physical,
        hint_warnings,
    })
}

/// Lower a query expression (SELECT or nested set operation) into one
/// physical graph. WITH/recursive setup is handled by its enclosing
/// `Materialize`/`RecursiveUnion` planner and intentionally returns `None`
/// here until that registry has been installed.
pub(crate) fn plan_query_physical<'q>(
    db: &Database,
    statement: impl Into<crate::parser::QueryRef<'q>>,
) -> Result<Option<PhysicalPlan>, QueryError> {
    // Set-operation chains are left-deep: recursion is one large planner
    // frame per branch.
    let statement = statement.into();
    crate::util::stack::with_stack(
        crate::engine::executor::mem_budget::ControlledMemoryCategory::Executor,
        || plan_query_physical_checked(db, statement),
    )?
}

fn plan_query_physical_checked(
    db: &Database,
    statement: crate::parser::QueryRef<'_>,
) -> Result<Option<PhysicalPlan>, QueryError> {
    let plan = match statement {
        crate::parser::QueryRef::Select(select) => {
            // Native plans currently carry no row-lock operator. Preserve the
            // locking executor for every leaf of a set operation.
            if select.for_update || select.lock_clause.is_some() {
                return Ok(None);
            }
            if has_window_in_projection(&select.projection)
                || select
                    .order_by
                    .iter()
                    .any(|o| expr_has_window_function(&o.expr))
            {
                return Ok(None);
            }
            plan_select(db, select)?.physical
        }
        crate::parser::QueryRef::SetOperation(set) => {
            crate::engine::params::validate_limit(set.limit.as_ref())?;
            crate::engine::params::validate_limit(set.offset.as_ref())?;
            let Some(mut left) = plan_query_physical(db, &set.left)? else {
                return Ok(None);
            };
            let Some(mut right) = plan_query_physical(db, &set.right)? else {
                return Ok(None);
            };
            if !set.all {
                reduce_distinct_integer_set_branch(db, &mut left);
                reduce_distinct_integer_set_branch(db, &mut right);
            }
            let left_rows = estimated_rows(&left);
            let right_rows = estimated_rows(&right);
            let set_rows = match (set.op, set.all) {
                (SetOp::Union, true) => left_rows + right_rows,
                (SetOp::Union, false) => (left_rows + right_rows) * 0.75,
                (SetOp::Intersect, _) => left_rows.min(right_rows) * 0.5,
                (SetOp::Except, _) => left_rows * 0.5,
            };
            let input_cost = plan_cost(&left).add(&plan_cost(&right));
            let work = left_rows + right_rows;
            let blocking = !(set.op == SetOp::Union && set.all);
            let mut cost = input_cost.with_added_total(work.max(1.0) * 0.01);
            cost.cpu_units += work.max(1.0);
            cost.mem_bytes = cost
                .mem_bytes
                .max(if blocking { work * 32.0 } else { 64.0 * 1024.0 });
            if blocking {
                cost.startup = cost.total;
            }
            let mut plan = PhysicalPlan::SetOp {
                left: Box::new(left),
                right: Box::new(right),
                op: set.op,
                all: set.all,
                estimated_rows: set_rows,
                cost,
            };
            if !set.order_by.is_empty() {
                let rows = estimated_rows(&plan);
                let mut sort_cost =
                    plan_cost(&plan).with_added_total(rows.max(2.0) * rows.max(2.0).log2() * 0.01);
                sort_cost.startup = sort_cost.total;
                plan = PhysicalPlan::Sort {
                    input: Box::new(plan),
                    order_by: set.order_by.clone(),
                    estimated_rows: rows,
                    cost: sort_cost,
                };
            }
            if set.limit.is_some() || set.offset.is_some() {
                let input_rows = estimated_rows(&plan);
                let offset = limit_usize(set.offset.as_ref()).unwrap_or(0);
                let limit = limit_usize(set.limit.as_ref());
                let output_rows = limit
                    .map(|limit| limit.min((input_rows - offset as f64).max(0.0) as usize) as f64)
                    .unwrap_or_else(|| (input_rows - offset as f64).max(0.0));
                let cost = plan_cost(&plan).with_added_total(output_rows.max(1.0) * 0.001);
                plan = PhysicalPlan::Limit {
                    input: Box::new(plan),
                    limit,
                    offset: Some(offset),
                    binding: LimitBinding {
                        limit_param: match set.limit.as_ref() {
                            Some(
                                LimitValue::Placeholder(index)
                                | LimitValue::OffsetCommaPlaceholder(index),
                            ) => Some(*index),
                            _ => None,
                        },
                        offset_param: match set.offset.as_ref() {
                            Some(
                                LimitValue::Placeholder(index)
                                | LimitValue::OffsetCommaPlaceholder(index),
                            ) => Some(*index),
                            _ => None,
                        },
                    },
                    estimated_rows: output_rows,
                    cost,
                };
            }
            synchronize_and_validate_plan_costs(&mut plan)?;
            plan
        }
        _ => return Ok(None),
    };
    Ok(Some(plan))
}

/// Add the explicit spillable/reusable boundary used by CTE and derived-table
/// owners. The estimate is derived solely from the finalized child plan, so
/// runtime memory pressure can choose in-memory versus spill without changing
/// topology.
pub(crate) fn materialize_plan(input: PhysicalPlan, reuse_key: impl Into<String>) -> PhysicalPlan {
    let estimated_rows = estimated_rows(&input);
    let estimated_bytes = estimated_rows.max(1.0) * plan_output_width(&input).max(1.0);
    let mut cost = plan_cost(&input);
    let serialization = estimated_rows.max(1.0) * 0.01;
    cost.total += serialization;
    cost.startup = cost.total;
    cost.cpu_units += estimated_rows.max(1.0);
    cost.mem_bytes = cost.mem_bytes.max(estimated_bytes);
    cost.spill_bytes = cost.spill_bytes.max(estimated_bytes);
    PhysicalPlan::Materialize {
        input: Box::new(input),
        reuse_key: reuse_key.into(),
        estimated_rows,
        cost,
    }
}

pub(crate) fn recursive_union_plan(
    anchor: PhysicalPlan,
    recursive: PhysicalPlan,
    name: impl Into<String>,
    union_all: bool,
    max_depth: u32,
) -> PhysicalPlan {
    let anchor_rows = estimated_rows(&anchor).max(1.0);
    let recursive_rows = estimated_rows(&recursive).max(0.0);
    // Statistics cannot know the convergence depth. Use a bounded geometric
    // estimate for sizing only; execution always stops on an empty delta.
    let growth = if recursive_rows <= anchor_rows {
        4.0
    } else {
        8.0
    };
    let estimated_rows = anchor_rows + recursive_rows * growth;
    let estimated_bytes = estimated_rows * plan_output_width(&anchor).max(1.0);
    let mut cost = plan_cost(&anchor).add(&plan_cost(&recursive));
    let iterative_cpu = estimated_rows.max(1.0) * if union_all { 0.02 } else { 0.04 };
    cost.total += iterative_cpu;
    cost.startup = cost.total;
    cost.cpu_units += estimated_rows.max(1.0);
    cost.mem_bytes = cost.mem_bytes.max(estimated_bytes);
    cost.spill_bytes = cost.spill_bytes.max(estimated_bytes);
    PhysicalPlan::RecursiveUnion {
        anchor: Box::new(anchor),
        recursive: Box::new(recursive),
        name: name.into(),
        union_all,
        max_depth,
        estimated_rows,
        cost,
    }
}

/// Build only the executable physical artifact for a SELECT that has already
/// passed `prepare_select_for_planning` in the executor.  The public
/// [`plan_select`] entry remains the standalone planner/EXPLAIN oracle and
/// still performs every normalization, validation and logical-plan build.
///
/// Repeating those passes in the execution path deep-cloned the AST and built
/// a logical tree that no executor consumer reads. Single-table statements
/// need no sideways join propagation, so they can borrow the prepared AST all
/// the way through physical planning. Join statements retain a private clone
/// for the one planner-only propagation pass.
pub(crate) fn plan_prepared_physical_select(
    db: &Database,
    select: &Select,
) -> Result<PhysicalPlan, QueryError> {
    fn build(db: &Database, select: &Select) -> Result<PhysicalPlan, QueryError> {
        let (physical, _warnings) = with_hint_warning_collector(|| build_physical_plan(db, select));
        let mut physical = physical?;
        index_residual::prune(&mut physical);
        lower_remaining_subquery_filters_to_apply(&mut physical);
        mark_fulltext_score_needs(&mut physical);
        crate::engine::cbo::trace::finish(&physical);
        Ok(physical)
    }

    if select.joins.is_empty() && select.from.len() <= 1 {
        build(db, select)
    } else {
        let mut planner_owned = select.clone();
        propagate_sideways_dimension_filters(db, &mut planner_owned);
        build(db, &planner_owned)
    }
}

/// Walk `plan` collecting every FULLTEXT `match_fingerprint` referenced
/// upstream of a `FulltextScan` (i.e. in any expression that could ever
/// read back the `__ft_score_<fingerprint>` pseudo-column), then flip the
/// scan's `needs_score` off when its fingerprint isn't in that set. Lets
/// `execute_fulltext_scan` skip the per-term `by_row` hashmaps, the
/// `doc_len_of` map, and BM25 arithmetic for the common
/// `WHERE MATCH() … ORDER BY <non-score>` shape.
///
/// A safe pass: any expression we can't fully analyse (subquery plans,
/// nested plan trees we don't recurse into here) leaves the referenced
/// scan at the safe `needs_score = true` default.
pub(crate) fn mark_fulltext_score_needs(plan: &mut PhysicalPlan) {
    let mut referenced: std::collections::HashSet<u64> = std::collections::HashSet::new();
    collect_fulltext_match_fingerprints_in_plan(plan, &mut referenced);
    apply_fulltext_needs_score_flags(plan, &referenced);
}

fn reduce_distinct_integer_set_branch(db: &Database, plan: &mut PhysicalPlan) {
    use crate::storage::DataTypeKind;
    let PhysicalPlan::Projection {
        input,
        projection,
        distinct: false,
        estimated_rows,
        cost,
    } = plan
    else {
        return;
    };
    let mut rows = self::estimated_rows(input);
    if rows < 50_000.0 {
        return;
    }
    let tables = collect_physical_plan_tables(input);
    let mut keys = Vec::new();
    let mut source = None;
    let mut source_rows = 0.0_f64;
    for item in projection.iter() {
        let SelectItem::Expr { expr, .. } = item else {
            return;
        };
        let Some(column_name) = expr_column_name(expr) else {
            return;
        };
        let mut matches = tables.iter().filter_map(|table| {
            let index = table.schema.column_index(column_name).ok()?;
            expr_matches_table_column(expr, table, column_name)
                .then_some((table, &table.schema.columns[index]))
        });
        let Some((table, column)) = matches.next() else {
            return;
        };
        if matches.next().is_some()
            || column.unsigned
            || !matches!(
                column.data_type,
                DataTypeKind::TinyInt
                    | DataTypeKind::SmallInt
                    | DataTypeKind::MediumInt
                    | DataTypeKind::Int
                    | DataTypeKind::BigInt
            )
        {
            return;
        }
        let identity = (
            table.schema.id,
            table
                .alias
                .as_deref()
                .unwrap_or(&table.schema.name)
                .to_ascii_lowercase(),
        );
        if source.as_ref().is_some_and(|source| source != &identity)
            || table.schema.stats.row_count == 0
        {
            return;
        }
        source = Some(identity);
        source_rows = table.schema.stats.row_count as f64;
        keys.push(expr.clone());
    }
    if keys.is_empty() {
        return;
    }
    // A tuple projected entirely from one relation has no more distinct
    // values than that relation's rows, even when joins replicate it heavily.
    let groups = estimate_group_by_rows_for_plan(input, &keys, rows).min(source_rows);
    if groups * 8.0 > rows {
        return;
    }
    if let Some(source) = source.as_ref() {
        reduce_distinct_join_to_existence(input, source);
        rows = self::estimated_rows(input);
    }
    let ctx = crate::engine::cbo::CboContext::from_db(db);
    let aggregate_cost = crate::engine::cbo::operator_cost::hash_aggregate(
        &ctx,
        &plan_cost(input),
        rows,
        groups,
        keys.len() as f64 * 8.0,
    );
    if aggregate_cost.spill_bytes > plan_cost(input).spill_bytes {
        return;
    }
    let mut degree = 1;
    input.walk(|node| match node {
        PhysicalPlan::ParallelSeqScan {
            degree: workers, ..
        }
        | PhysicalPlan::ParallelZoneMapScan {
            degree: workers, ..
        }
        | PhysicalPlan::Exchange {
            degree: workers, ..
        }
        | PhysicalPlan::ParallelHashAggregate {
            degree: workers, ..
        } => degree = degree.max(*workers),
        _ => {}
    });
    let placeholder = PhysicalPlan::ValuesScan {
        rows: Vec::new(),
        columns: Vec::new(),
        filter: None,
        estimated_rows: 0.0,
        cost: crate::engine::cbo::Cost::ZERO,
    };
    let original = std::mem::replace(input, Box::new(placeholder));
    let mut aggregate = PhysicalPlan::HashAggregate {
        input: original,
        group_by: keys,
        having: None,
        aggregates: Vec::new(),
        estimated_rows: groups,
        cost: aggregate_cost,
    };
    if degree > 1 {
        parallelize_hash_aggregate(&mut aggregate, degree, ParallelMode::CostBased)
    }
    *cost = plan_cost(&aggregate);
    *estimated_rows = groups;
    *input = Box::new(aggregate);
}

/// A DISTINCT tuple read entirely from one side of an equijoin only needs
/// existence on the other side. Preserve the projected relation as the SEMI
/// probe, avoiding one joined output row for every matching duplicate.
fn reduce_distinct_join_to_existence(plan: &mut PhysicalPlan, source: &(u64, String)) {
    fn safe_predicate(expr: &Expr) -> bool {
        match expr {
            Expr::Identifier(_)
            | Expr::CompoundIdentifier(_)
            | Expr::Literal(_)
            | Expr::BoundValue(_)
            | Expr::Placeholder(_) => true,
            Expr::Binary { left, right, op } => {
                matches!(
                    op,
                    BinaryOp::And
                        | BinaryOp::Or
                        | BinaryOp::Eq
                        | BinaryOp::NotEq
                        | BinaryOp::Lt
                        | BinaryOp::LtEq
                        | BinaryOp::Gt
                        | BinaryOp::GtEq
                ) && safe_predicate(left)
                    && safe_predicate(right)
            }
            Expr::IsNull(expr) | Expr::IsNotNull(expr) => safe_predicate(expr),
            Expr::Between {
                expr, low, high, ..
            } => safe_predicate(expr) && safe_predicate(low) && safe_predicate(high),
            Expr::InList { expr, list, .. } => {
                safe_predicate(expr) && list.iter().all(safe_predicate)
            }
            _ => false,
        }
    }
    let mut safe = true;
    plan.walk(|node| {
        safe &= match node {
            PhysicalPlan::SeqScan { filter, .. }
            | PhysicalPlan::ZoneMapScan { filter, .. }
            | PhysicalPlan::ParallelSeqScan { filter, .. }
            | PhysicalPlan::ParallelZoneMapScan { filter, .. }
            | PhysicalPlan::IndexFastScan { filter, .. }
            | PhysicalPlan::IndexRangeScan { filter, .. }
            | PhysicalPlan::IndexLookup { filter, .. } => {
                filter.as_ref().is_none_or(safe_predicate)
            }
            PhysicalPlan::Exchange { .. } => true,
            PhysicalPlan::HashJoin {
                join_type: JoinType::Inner,
                residual: None,
                left_keys,
                right_keys,
                ..
            } => left_keys
                .iter()
                .chain(right_keys)
                .all(|expr| expr_column_name(expr).is_some()),
            _ => false,
        };
    });
    if !safe {
        return;
    }
    let PhysicalPlan::HashJoin {
        left,
        right,
        join_type: join_type @ JoinType::Inner,
        left_keys,
        right_keys,
        build_side,
        residual: None,
        output_projection,
        estimated_rows,
        ..
    } = plan
    else {
        return;
    };
    let contains_source = |input: &PhysicalPlan| {
        collect_physical_plan_tables(input).iter().any(|table| {
            table.schema.id == source.0
                && table
                    .alias
                    .as_deref()
                    .unwrap_or(&table.schema.name)
                    .eq_ignore_ascii_case(&source.1)
        })
    };
    let left_source = contains_source(left);
    let right_source = contains_source(right);
    if left_source == right_source {
        return;
    }
    if right_source {
        std::mem::swap(left, right);
        std::mem::swap(left_keys, right_keys);
    }
    *join_type = JoinType::Semi;
    *build_side = BuildSide::Right;
    *output_projection = None;
    *estimated_rows = self::estimated_rows(left).min(*estimated_rows);
}

fn visit_fulltext_matches(expr: &Expr, mut inspect: impl FnMut(&Expr)) {
    use crate::parser::visit::{visit_expr, AstVisitor};
    struct Matches<F>(F);
    impl<F: FnMut(&Expr)> AstVisitor for Matches<F> {
        fn enter_expr(&mut self, expr: &Expr) {
            if let Expr::Function { name, .. } = expr {
                if name.eq_ignore_ascii_case(crate::parser::FT_MATCH_FUNCTION) {
                    (self.0)(expr);
                }
            }
        }
        fn walk_expr_children(&self, expr: &Expr) -> bool {
            !matches!(
                expr,
                Expr::Subquery(_)
                    | Expr::Exists { .. }
                    | Expr::InSubquery { .. }
                    | Expr::QuantifiedComparison { .. }
                    | Expr::Grouping(_)
            )
        }
    }
    visit_expr(&mut Matches(&mut inspect), expr);
}

fn collect_match_fingerprints_in_expr(expr: &Expr, out: &mut std::collections::HashSet<u64>) {
    visit_fulltext_matches(expr, |expr| {
        out.insert(crate::engine::expr_program::expr_fingerprint(expr));
    });
}

fn collect_fulltext_matches_in_expr(
    expr: &Expr,
    seen: &mut std::collections::HashSet<u64>,
    out: &mut Vec<Expr>,
) {
    visit_fulltext_matches(expr, |expr| {
        if seen.insert(crate::engine::expr_program::expr_fingerprint(expr)) {
            out.push(expr.clone());
        }
    });
}

fn collect_fulltext_matches_in_select(select: &Select) -> Vec<Expr> {
    let mut seen = std::collections::HashSet::new();
    let mut out = Vec::new();
    if let Some(selection) = &select.selection {
        collect_fulltext_matches_in_expr(selection, &mut seen, &mut out);
    }
    for item in &select.projection {
        if let SelectItem::Expr { expr, .. } = item {
            collect_fulltext_matches_in_expr(expr, &mut seen, &mut out);
        }
    }
    for expr in &select.group_by {
        collect_fulltext_matches_in_expr(expr, &mut seen, &mut out);
    }
    if let Some(having) = &select.having {
        collect_fulltext_matches_in_expr(having, &mut seen, &mut out);
    }
    for order in &select.order_by {
        collect_fulltext_matches_in_expr(&order.expr, &mut seen, &mut out);
    }
    out
}

fn collect_fulltext_match_fingerprints_in_plan(
    plan: &PhysicalPlan,
    out: &mut std::collections::HashSet<u64>,
) {
    // Depth-first walk over the plan tree. For every expression node
    // reachable from this plan we collect `__match_against` fingerprints
    // referenced anywhere. A `FulltextScan`'s own residual `filter` is a
    // special case: a MATCH there refers to that same scan's score, so it
    // still counts as "score referenced". We don't need to elide it —
    // even the safe over-count preserves correctness (only recovers
    // slightly less perf on the residual-MATCH shape).
    match plan {
        PhysicalPlan::FulltextScan { filter, .. } => {
            if let Some(f) = filter {
                collect_match_fingerprints_in_expr(f, out);
            }
        }
        PhysicalPlan::SeqScan { filter, .. }
        | PhysicalPlan::ZoneMapScan { filter, .. }
        | PhysicalPlan::IndexRangeScan { filter, .. }
        | PhysicalPlan::IndexLookup { filter, .. }
        | PhysicalPlan::IndexMultiLookup { filter, .. }
        | PhysicalPlan::IndexFastScan { filter, .. }
        | PhysicalPlan::ParallelIndexFastScan { filter, .. }
        | PhysicalPlan::ParallelIndexRangeScan { filter, .. }
        | PhysicalPlan::SpatialScan { filter, .. }
        | PhysicalPlan::VectorIndexScan { filter, .. } => {
            if let Some(f) = filter {
                collect_match_fingerprints_in_expr(f, out);
            }
        }
        _ => {}
    }
    // Recurse into any expression list this node type carries
    // (projection lists, ORDER BY keys, GROUP BY, HAVING, join predicates, …).
    visit_plan_top_level_exprs(plan, |expr| {
        collect_match_fingerprints_in_expr(expr, out);
    });
    // Recurse into child plans.
    for child in plan_children(plan) {
        collect_fulltext_match_fingerprints_in_plan(child, out);
    }
}

/// Best-effort enumeration of the top-level scalar expressions a plan node
/// carries. Missing an obscure node type is safe: the corresponding
/// `FulltextScan` just stays at `needs_score = true` (the current default).
fn visit_plan_top_level_exprs(plan: &PhysicalPlan, mut inspect: impl FnMut(&Expr)) {
    match plan {
        PhysicalPlan::Projection { projection, .. } => {
            for item in projection {
                if let SelectItem::Expr { expr, .. } = item {
                    inspect(expr);
                }
            }
        }
        PhysicalPlan::Sort { order_by, .. }
        | PhysicalPlan::IncrementalSort { order_by, .. }
        | PhysicalPlan::ParallelSort { order_by, .. }
        | PhysicalPlan::TopK { order_by, .. }
        | PhysicalPlan::ParallelTopK { order_by, .. }
        | PhysicalPlan::TopWindow { order_by, .. } => {
            for k in order_by {
                inspect(&k.expr);
            }
        }
        PhysicalPlan::Filter { predicate, .. } | PhysicalPlan::Apply { predicate, .. } => {
            inspect(predicate);
        }
        PhysicalPlan::WindowAgg { windows, .. } => {
            for window in windows {
                window.args.iter().for_each(&mut inspect);
                window.partition_by.iter().for_each(&mut inspect);
                window
                    .order_by
                    .iter()
                    .map(|o| &o.expr)
                    .for_each(&mut inspect);
            }
        }
        _ => {}
    }
}

/// Make undecorrelated subquery execution explicit in the physical graph.
/// The logical Filter and Apply have identical row semantics; the distinction
/// is operational: Apply promises that the outer subtree is opened once and
/// that the executor evaluates/memoizes the parameterized predicate inside
/// the current batch instead of treating it as an unsupported filter shape.
fn lower_remaining_subquery_filters_to_apply(plan: &mut PhysicalPlan) {
    for child in plan_children_mut(plan) {
        lower_remaining_subquery_filters_to_apply(child);
    }
    let scan_predicate = match plan {
        PhysicalPlan::ValuesScan { filter, .. }
        | PhysicalPlan::JsonTableScan { filter, .. }
        | PhysicalPlan::SystemScan { filter, .. }
        | PhysicalPlan::SeqScan { filter, .. }
        | PhysicalPlan::ZoneMapScan { filter, .. }
        | PhysicalPlan::IndexLookup { filter, .. }
        | PhysicalPlan::IndexMultiLookup { filter, .. }
        | PhysicalPlan::IndexRangeScan { filter, .. }
        | PhysicalPlan::IndexFastScan { filter, .. }
        | PhysicalPlan::FulltextScan { filter, .. }
        | PhysicalPlan::SpatialScan { filter, .. }
        | PhysicalPlan::VectorIndexScan { filter, .. }
        | PhysicalPlan::IndexInLookup { filter, .. }
        | PhysicalPlan::CteScan { filter, .. }
        | PhysicalPlan::GenerateSeries { filter, .. }
        | PhysicalPlan::ParallelSeqScan { filter, .. }
        | PhysicalPlan::ParallelZoneMapScan { filter, .. }
        | PhysicalPlan::ParallelIndexFastScan { filter, .. }
        | PhysicalPlan::ParallelIndexRangeScan { filter, .. } => take_apply_conjuncts(filter),
        _ => None,
    };
    if let Some(predicate) = scan_predicate {
        let estimated_rows = estimated_rows(plan);
        let cost = plan_cost(plan);
        let placeholder = PhysicalPlan::ValuesScan {
            rows: Vec::new(),
            columns: Vec::new(),
            filter: None,
            estimated_rows: 0.0,
            cost: crate::engine::cbo::Cost::ZERO,
        };
        let outer = Box::new(std::mem::replace(plan, placeholder));
        *plan = PhysicalPlan::Apply {
            outer,
            predicate,
            estimated_rows,
            cost,
        };
        return;
    }
    let PhysicalPlan::Filter {
        input,
        predicate,
        estimated_rows,
        cost,
    } = plan
    else {
        return;
    };
    if !sip_expr_has_subquery(predicate) {
        return;
    }
    let original = std::mem::replace(predicate, Expr::BoundValue(crate::types::Value::Null));
    let (ordinary, apply) = partition_apply_conjuncts(original);
    let Some(apply_predicate) = combine_conjuncts(apply) else {
        *predicate =
            combine_conjuncts(ordinary).unwrap_or(Expr::BoundValue(crate::types::Value::Int(1)));
        return;
    };
    let placeholder = Box::new(PhysicalPlan::ValuesScan {
        rows: Vec::new(),
        columns: Vec::new(),
        filter: None,
        estimated_rows: 0.0,
        cost: crate::engine::cbo::Cost::ZERO,
    });
    let input = std::mem::replace(input, placeholder);
    let estimated_rows = *estimated_rows;
    let cost = *cost;
    let outer = if let Some(ordinary_predicate) = combine_conjuncts(ordinary) {
        Box::new(PhysicalPlan::Filter {
            input,
            predicate: ordinary_predicate,
            estimated_rows,
            cost,
        })
    } else {
        input
    };
    *plan = PhysicalPlan::Apply {
        outer,
        predicate: apply_predicate,
        estimated_rows,
        cost,
    };
}

/// Split only top-level AND conjuncts at the scan/Apply boundary.  Boolean
/// contexts such as OR/NOT remain intact because pulling a subquery out of
/// them would change SQL three-valued semantics.  Ordinary conjuncts stay on
/// the scan so access-path pruning and the batch predicate lane remain usable.
fn partition_apply_conjuncts(predicate: Expr) -> (Vec<Expr>, Vec<Expr>) {
    let mut conjuncts = Vec::new();
    split_conjuncts(&predicate, &mut conjuncts);
    conjuncts
        .into_iter()
        .partition(|conjunct| !sip_expr_has_subquery(conjunct))
}

fn take_apply_conjuncts(filter: &mut Option<Expr>) -> Option<Expr> {
    let predicate = filter.take()?;
    if !sip_expr_has_subquery(&predicate) {
        *filter = Some(predicate);
        return None;
    }
    let (ordinary, apply) = partition_apply_conjuncts(predicate);
    *filter = combine_conjuncts(ordinary);
    combine_conjuncts(apply)
}

/// Depth-first predicate over a physical plan. Executor-side plan
/// inspection routes through this function so operator child relationships
/// have one source of truth.
pub(crate) fn plan_any(
    plan: &PhysicalPlan,
    predicate: &mut impl FnMut(&PhysicalPlan) -> bool,
) -> bool {
    predicate(plan)
        || plan
            .children()
            .into_iter()
            .any(|child| plan_any(child, predicate))
}

pub(crate) fn walk_plan(plan: &PhysicalPlan, visitor: &mut impl FnMut(&PhysicalPlan)) {
    visitor(plan);
    for child in plan.children() {
        walk_plan(child, visitor);
    }
}

fn walk_plan_with_ids(
    plan: &PhysicalPlan,
    next: &mut u32,
    visitor: &mut impl FnMut(PlanNodeId, &PhysicalPlan),
) {
    let id = PlanNodeId(*next);
    *next = next
        .checked_add(1)
        .expect("physical plan contains more than u32::MAX nodes");
    visitor(id, plan);
    for child in plan.children() {
        walk_plan_with_ids(child, next, visitor);
    }
}

pub(crate) fn walk_plan_mut(plan: &mut PhysicalPlan, visitor: &mut impl FnMut(&mut PhysicalPlan)) {
    visitor(plan);
    for child in plan.children_mut() {
        walk_plan_mut(child, visitor);
    }
}

fn apply_fulltext_needs_score_flags(
    plan: &mut PhysicalPlan,
    referenced: &std::collections::HashSet<u64>,
) {
    plan.walk_mut(|node| {
        if let PhysicalPlan::FulltextScan {
            match_fingerprint,
            needs_score,
            additional_matches,
            ..
        } = node
        {
            if !referenced.contains(match_fingerprint) {
                *needs_score = false;
            }
            for spec in additional_matches {
                spec.needs_score = referenced.contains(&spec.match_fingerprint);
            }
        }
    });
}

/// Substitute select-list alias references in GROUP BY with the aliased
/// expression, honouring MySQL's resolution order for GROUP BY/HAVING:
/// the FROM clause is searched *before* the select list (the opposite of
/// ORDER BY), so an alias that shadows a real column must lose. The
/// column check needs schema knowledge, hence the `db` parameter; when
/// the scope cannot be built (e.g. CTE sources without a catalog
/// identity) the alias substitution proceeds unconditionally — the
/// pre-existing behavior — rather than failing the query.
///
/// Ordinals from SQL text are already gone by the time this runs (the
/// parser substitutes them, see `parser::resolve_group_by_ordinals`);
/// the `resolve_group_ref_expr` fallback still handles them for
/// internally-built selects.
pub(crate) fn resolve_group_by_refs(db: &Database, select: &mut Select) {
    let aliases = projection_alias_exprs(select);
    let has_ordinal = select.group_by.iter().any(|expr| {
        matches!(expr, Expr::Literal(crate::parser::Literal::Int(_)))
            || matches!(expr, Expr::Literal(crate::parser::Literal::UInt(_)))
    });
    if aliases.is_empty() && !has_ordinal {
        return;
    }
    let projection_exprs: Vec<Expr> = select
        .projection
        .iter()
        .filter_map(|item| match item {
            SelectItem::Expr { expr, .. } => Some(expr.clone()),
            _ => None,
        })
        .collect();
    // Build the column scope only when some GROUP BY identifier actually
    // collides with an alias name — the only case where the FROM-first
    // precedence question arises. Keeps the common path free of catalog
    // lookups (validation builds its own scope right after anyway).
    let needs_scope = select.group_by.iter().any(|expr| {
        matches!(expr, Expr::Identifier(name) if aliases.contains_key(&name.to_ascii_lowercase()))
    });
    let scope = if needs_scope {
        build_column_scope(db, select, None).ok()
    } else {
        None
    };
    for expr in &mut select.group_by {
        if let Expr::Identifier(name) = expr {
            if let Some(scope) = &scope {
                if scope.column_matches(None, name, true) > 0 {
                    // A visible FROM column wins over a same-named alias.
                    if aliases.contains_key(&name.to_ascii_lowercase()) {
                        // Resolution is intentionally invoked on the
                        // executable SELECT and on planner-owned clones.
                        // Keep the diagnostic statement-scoped and
                        // idempotent across those passes.
                        crate::engine::expr::with_expression_warning_dedup(
                            0x4752_4f55_5041_4c49,
                            || {
                                crate::engine::api::push_statement_warning(
                                    crate::error::ConditionLevel::Warning,
                                    crate::error::catalog::ER_NON_UNIQ_ERROR,
                                    format!("Column '{name}' in group statement is ambiguous"),
                                );
                            },
                        );
                    }
                    continue;
                }
            }
        }
        *expr = resolve_group_ref_expr(expr.clone(), &projection_exprs, &aliases);
    }
}

fn integer_affine_group_offset(expr: &Expr, base: &str) -> Option<i128> {
    if expr_column_name(expr).is_some_and(|name| name.eq_ignore_ascii_case(base)) {
        return Some(0);
    }
    let Expr::Binary { left, op, right } = expr else {
        return None;
    };
    let literal = |expr: &Expr| match expr {
        Expr::Literal(crate::parser::Literal::Int(value)) => Some(i128::from(*value)),
        Expr::Literal(crate::parser::Literal::UInt(value)) => Some(i128::from(*value)),
        _ => None,
    };
    match op {
        BinaryOp::Add => integer_affine_group_offset(left, base)
            .and_then(|offset| offset.checked_add(literal(right)?))
            .or_else(|| {
                integer_affine_group_offset(right, base)
                    .and_then(|offset| offset.checked_add(literal(left)?))
            }),
        BinaryOp::Sub => integer_affine_group_offset(left, base)
            .and_then(|offset| offset.checked_sub(literal(right)?)),
        _ => None,
    }
}

/// Resolve an unqualified ORDER BY identifier that names a SELECT output column
/// into that projection's expression — MySQL resolves an `ORDER BY name` against
/// the select list's output column names (explicit alias *or* the implicit name
/// of a bare/qualified column) before the FROM tables. e.g. TPC-DS q72's
/// `ORDER BY ... d_week_seq` over `SELECT d1.d_week_seq ... FROM date_dim d1,
/// date_dim d2, date_dim d3` must bind to the output column `d1.d_week_seq`, not
/// be reported `1052 ambiguous` against the three `date_dim` copies; q58's
/// `ORDER BY item_id` likewise binds to `ss_items.item_id`, not the three CTEs.
///
/// Conservative precedence (matching `resolve_group_by_refs`): a uniquely
/// resolvable FROM column wins, so this only rewrites identifiers that are
/// otherwise ambiguous or absent in the FROM scope — currently-unambiguous
/// queries are untouched. Only rewrites when exactly one output column carries
/// the name; a duplicated output name is left for normal (ambiguous) resolution.
/// Wildcard widths are only known after binding. Expand the original list
/// once so resolved constant expressions cannot be mistaken for ordinals on
/// a second planning pass.
pub(crate) fn resolve_wildcard_order_ordinals(
    db: &Database,
    select: &mut Select,
) -> Result<(), QueryError> {
    let ordinal = |expr: &Expr| match expr {
        Expr::Literal(crate::parser::Literal::Int(n)) if *n >= 0 => Some(*n as u64),
        Expr::Literal(crate::parser::Literal::UInt(n)) => Some(*n),
        _ => None,
    };
    if !select.projection.iter().any(|item| {
        matches!(
            item,
            SelectItem::Wildcard | SelectItem::QualifiedWildcard(_)
        )
    }) || !select
        .order_by
        .iter()
        .any(|item| ordinal(&item.expr).is_some())
        && !select.group_by.iter().any(|expr| ordinal(expr).is_some())
    {
        return Ok(());
    }
    resolve_join_using_natural(db, select)?;
    let scope = build_column_scope(db, select, None)?;
    let mut projection = Vec::new();
    for item in &select.projection {
        match item {
            SelectItem::Wildcard | SelectItem::QualifiedWildcard(_) => {
                for table in &scope.tables {
                    if let SelectItem::QualifiedWildcard(qualifier) = item {
                        if !table
                            .alias
                            .as_deref()
                            .unwrap_or(&table.name)
                            .eq_ignore_ascii_case(qualifier)
                        {
                            continue;
                        }
                    }
                    for name in &table.wildcard_columns {
                        projection.push(SelectItem::Expr {
                            expr: Expr::CompoundIdentifier(vec![
                                table.alias.as_ref().unwrap_or(&table.name).clone(),
                                name.clone(),
                            ]),
                            alias: Some(name.clone()),
                        });
                    }
                }
            }
            other => projection.push(other.clone()),
        }
    }
    for order in &mut select.order_by {
        if let Some(pos) = ordinal(&order.expr) {
            let item = pos
                .checked_sub(1)
                .and_then(|idx| usize::try_from(idx).ok())
                .and_then(|idx| projection.get(idx));
            let Some(SelectItem::Expr { expr, .. }) = item else {
                return Err(error(
                    crate::error::catalog::ER_BAD_FIELD_ERROR,
                    format!("Unknown column '{pos}' in 'order clause'"),
                ));
            };
            order.expr = expr.clone();
        }
    }
    for key in &mut select.group_by {
        if let Some(pos) = ordinal(key) {
            let item = pos
                .checked_sub(1)
                .and_then(|idx| usize::try_from(idx).ok())
                .and_then(|idx| projection.get(idx));
            let Some(SelectItem::Expr { expr, .. }) = item else {
                return Err(error(
                    crate::error::catalog::ER_BAD_FIELD_ERROR,
                    format!("Unknown column '{pos}' in 'group statement'"),
                ));
            };
            if expr_has_aggregate(expr) {
                return Err(error(
                    crate::error::catalog::ER_WRONG_GROUP_FIELD,
                    "cannot group on an aggregate",
                ));
            }
            *key = expr.clone();
        }
    }
    select.projection = projection;
    Ok(())
}

pub(crate) fn resolve_order_by_output_refs(db: &Database, select: &mut Select) {
    if select.order_by.is_empty() {
        return;
    }
    // Output-column name → its projection expression; `None` marks a name
    // carried by more than one output column (ambiguous, not rewritten).
    let mut output: std::collections::HashMap<String, Option<Expr>> =
        std::collections::HashMap::new();
    for item in &select.projection {
        if let SelectItem::Expr { expr, alias } = item {
            let name = alias
                .clone()
                .unwrap_or_else(|| crate::engine::expr::expr_name(expr))
                .to_ascii_lowercase();
            output
                .entry(name)
                .and_modify(|slot| *slot = None)
                .or_insert_with(|| Some(expr.clone()));
        }
    }
    let needs = select.order_by.iter().any(|ob| {
        matches!(&ob.expr, Expr::Identifier(n)
            if matches!(output.get(&n.to_ascii_lowercase()), Some(Some(_))))
    });
    if !needs {
        return;
    }
    let scope = build_column_scope(db, select, None).ok();
    for ob in &mut select.order_by {
        if let Expr::Identifier(name) = &ob.expr {
            let key = name.to_ascii_lowercase();
            let Some(Some(target)) = output.get(&key) else {
                continue;
            };
            // A uniquely-resolvable FROM column keeps its meaning.
            if let Some(scope) = &scope {
                if scope.column_matches(None, name, true) == 1 {
                    continue;
                }
            }
            ob.expr = target.clone();
        }
    }
}

/// Resolve SELECT aliases referenced anywhere inside HAVING. Unlike GROUP BY,
/// MySQL gives an unqualified projection alias precedence in HAVING, including
/// implicit single-group aggregates such as `SELECT MAX(d) AS m HAVING m > x`.
///
/// Subquery bodies establish their own name-resolution scope and are left
/// untouched; their normal planning pass resolves their aliases separately.
pub(crate) fn resolve_having_aliases(select: &mut Select) {
    let mut aliases = projection_alias_exprs(select);
    for (index, group) in select.group_by.iter().enumerate() {
        if let Some(name) = expr_column_name(group) {
            let key = name.to_ascii_lowercase();
            if aliases.contains_key(&key) {
                aliases.insert(
                    key,
                    Expr::GroupKeyRef {
                        index,
                        expr: Box::new(group.clone()),
                    },
                );
            }
        }
    }
    if aliases.is_empty() {
        return;
    }
    if let Some(having) = select.having.take() {
        select.having = Some(resolve_having_alias_expr(having, &aliases));
    }
}

pub(crate) fn resolve_having_alias_expr(mut expr: Expr, aliases: &HashMap<String, Expr>) -> Expr {
    crate::parser::visit::rewrite_value_expr(&mut expr, |node| {
        if let Expr::Identifier(name) = node {
            if let Some(alias) = aliases.get(&name.to_ascii_lowercase()) {
                *node = alias.clone();
            }
            return false;
        }
        true
    });
    expr
}

pub(crate) fn bind_rollup_select_for_execution(
    db: &Database,
    select: &mut Select,
) -> Result<(), QueryError> {
    resolve_group_by_refs(db, select);
    bind_rollup_expressions(select)
}

fn projection_alias_exprs(select: &Select) -> HashMap<String, Expr> {
    let mut aliases = HashMap::new();
    for item in &select.projection {
        if let SelectItem::Expr {
            expr,
            alias: Some(alias),
        } = item
        {
            aliases.insert(alias.to_ascii_lowercase(), expr.clone());
        }
    }
    aliases
}

fn resolve_group_ref_expr(
    expr: Expr,
    projection_exprs: &[Expr],
    aliases: &HashMap<String, Expr>,
) -> Expr {
    // A GROUP BY entry is the key itself. When resolution runs again on an
    // already bound SELECT (`GROUP BY 1` over a constant projection item that
    // is now `GroupKeyRef(0)`), use the referenced key's source expression
    // instead of a self-reference no input row can supply.
    let mut resolved = resolve_group_ref_target(expr, projection_exprs, aliases);
    while let Expr::GroupKeyRef { expr, .. } = resolved {
        resolved = *expr;
    }
    resolved
}

fn resolve_group_ref_target(
    expr: Expr,
    projection_exprs: &[Expr],
    aliases: &HashMap<String, Expr>,
) -> Expr {
    match expr {
        Expr::Literal(crate::parser::Literal::Int(pos)) if pos > 0 => projection_exprs
            .get(pos as usize - 1)
            .cloned()
            .unwrap_or(Expr::Literal(crate::parser::Literal::Int(pos))),
        Expr::Literal(crate::parser::Literal::UInt(pos)) if pos > 0 => projection_exprs
            .get(pos as usize - 1)
            .cloned()
            .unwrap_or(Expr::Literal(crate::parser::Literal::UInt(pos))),
        Expr::Identifier(name) => aliases
            .get(&name.to_ascii_lowercase())
            .cloned()
            .unwrap_or(Expr::Identifier(name)),
        other => other,
    }
}

/// Resolve a SELECT-list alias inside a projection/HAVING/ORDER expression.
///
/// Integer ordinals are deliberately not handled here. `GROUP BY 1` is
/// resolved by `resolve_group_by_refs` (and normally by the parser), while a
/// top-level `ORDER BY 1` is resolved before entering `bind_rollup_expr`.
/// Recursively interpreting every integer literal as an ordinal corrupts
/// ordinary expressions such as `AVG(1) = 1` into `AVG(1) = group_key`.
fn resolve_rollup_alias_ref(expr: Expr, aliases: &HashMap<String, Expr>) -> Expr {
    match expr {
        Expr::Identifier(name) => aliases
            .get(&name.to_ascii_lowercase())
            .cloned()
            .unwrap_or(Expr::Identifier(name)),
        other => other,
    }
}

fn bind_rollup_expressions(select: &mut Select) -> Result<(), QueryError> {
    if select.group_by.is_empty() && select.grouping_sets.is_empty() {
        if select.group_by_modifier == GroupByModifier::None {
            return Ok(());
        }
        return Err(error(
            crate::error::catalog::ER_PARSE_ERROR,
            "GROUP BY modifier requires at least one GROUP BY expression",
        ));
    }
    let aliases = projection_alias_exprs(select);
    let projection_exprs: Vec<Expr> = select
        .projection
        .iter()
        .filter_map(|item| match item {
            SelectItem::Expr { expr, .. } => Some(expr.clone()),
            _ => None,
        })
        .collect();
    let group_by = select.group_by.clone();
    for item in &mut select.projection {
        if let SelectItem::Expr { expr, .. } = item {
            *expr = bind_rollup_expr(expr.clone(), &group_by, &projection_exprs, &aliases, false)?;
        }
    }
    if let Some(having) = select.having.take() {
        select.having = Some(bind_rollup_expr(
            having,
            &group_by,
            &projection_exprs,
            &aliases,
            true,
        )?);
    }
    for order_by in &mut select.order_by {
        // ORDER BY accepts a positive top-level ordinal. Resolve it exactly
        // once here; literals nested inside arithmetic/functions/tuples are
        // values, not projection references.
        let order_expr = resolve_group_ref_expr(order_by.expr.clone(), &projection_exprs, &aliases);
        order_by.expr = bind_rollup_expr(order_expr, &group_by, &projection_exprs, &aliases, true)?;
    }
    Ok(())
}

fn bind_rollup_expr(
    expr: Expr,
    group_by: &[Expr],
    projection_exprs: &[Expr],
    aliases: &HashMap<String, Expr>,
    allow_alias: bool,
) -> Result<Expr, QueryError> {
    if let Expr::Function { name, args, .. } = &expr {
        if is_aggregate_function(name) {
            return Ok(expr);
        }
        if name.eq_ignore_ascii_case("GROUPING") {
            if args.is_empty() {
                return Err(error(
                    crate::error::catalog::ER_WRONG_PARAMCOUNT_TO_NATIVE_FCT,
                    "Incorrect parameter count in the call to native function 'GROUPING'",
                ));
            }
            let mut indices = Vec::with_capacity(args.len());
            for arg in args {
                // GROUPING() arguments use the same top-level name/ordinal
                // resolution as GROUP BY itself. This is deliberately local
                // to each argument: integer literals nested inside an
                // arithmetic/function expression remain values.
                let resolved = resolve_group_ref_expr(arg.clone(), projection_exprs, aliases);
                let Some(idx) = group_by
                    .iter()
                    .position(|group_expr| expr_semantically_eq(group_expr, &resolved))
                else {
                    return Err(error(
                        crate::error::catalog::ER_WINDOW_CIRCULARITY_IN_WINDOW_GRAPH,
                        format!(
                            "argument '{}' of GROUPING function is not in GROUP BY",
                            crate::parser::expr_to_sql(arg)
                        ),
                    ));
                };
                indices.push(idx);
            }
            return Ok(Expr::Grouping(indices));
        }
    }
    let resolved_group_ref = if allow_alias {
        resolve_rollup_alias_ref(expr.clone(), aliases)
    } else {
        expr.clone()
    };
    if resolved_group_ref != expr {
        if let Some(idx) = group_by
            .iter()
            .position(|group_expr| expr_semantically_eq(group_expr, &resolved_group_ref))
        {
            return Ok(Expr::GroupKeyRef {
                index: idx,
                expr: Box::new(resolved_group_ref),
            });
        }
    }
    if let Some(idx) = group_by
        .iter()
        .position(|group_expr| expr_semantically_eq(group_expr, &expr))
    {
        return Ok(Expr::GroupKeyRef {
            index: idx,
            expr: Box::new(expr),
        });
    }
    // Issue #65: a bare identifier that resolves to a select-list alias which
    // is not itself a GROUP BY key must be replaced by the aliased expression,
    // so HAVING / ORDER BY can reference it (e.g. `HAVING tot > 15` /
    // `ORDER BY tot` over `SUM(v) AS tot`). Bind the substitution with the
    // current alias removed to guard against a self-referential alias looping.
    if resolved_group_ref != expr {
        if let Expr::Identifier(name) = &expr {
            let mut reduced = aliases.clone();
            reduced.remove(&name.to_ascii_lowercase());
            return bind_rollup_expr(
                resolved_group_ref,
                group_by,
                projection_exprs,
                &reduced,
                allow_alias,
            );
        }
    }
    Ok(match expr {
        Expr::Unary { op, expr } => Expr::Unary {
            op,
            expr: Box::new(bind_rollup_expr(
                *expr,
                group_by,
                projection_exprs,
                aliases,
                allow_alias,
            )?),
        },
        Expr::Binary { left, op, right } => Expr::Binary {
            left: Box::new(bind_rollup_expr(
                *left,
                group_by,
                projection_exprs,
                aliases,
                allow_alias,
            )?),
            op,
            right: Box::new(bind_rollup_expr(
                *right,
                group_by,
                projection_exprs,
                aliases,
                allow_alias,
            )?),
        },
        Expr::IsNull(expr) => Expr::IsNull(Box::new(bind_rollup_expr(
            *expr,
            group_by,
            projection_exprs,
            aliases,
            allow_alias,
        )?)),
        Expr::IsNotNull(expr) => Expr::IsNotNull(Box::new(bind_rollup_expr(
            *expr,
            group_by,
            projection_exprs,
            aliases,
            allow_alias,
        )?)),
        Expr::InList {
            expr,
            list,
            negated,
        } => Expr::InList {
            expr: Box::new(bind_rollup_expr(
                *expr,
                group_by,
                projection_exprs,
                aliases,
                allow_alias,
            )?),
            list: list
                .into_iter()
                .map(|item| {
                    bind_rollup_expr(item, group_by, projection_exprs, aliases, allow_alias)
                })
                .collect::<Result<Vec<_>, _>>()?,
            negated,
        },
        Expr::InSubquery {
            expr,
            subquery,
            negated,
        } => Expr::InSubquery {
            expr: Box::new(bind_rollup_expr(
                *expr,
                group_by,
                projection_exprs,
                aliases,
                allow_alias,
            )?),
            subquery,
            negated,
        },
        Expr::QuantifiedComparison {
            left,
            op,
            quantifier,
            right,
        } => Expr::QuantifiedComparison {
            left: Box::new(bind_rollup_expr(
                *left,
                group_by,
                projection_exprs,
                aliases,
                allow_alias,
            )?),
            op,
            quantifier,
            right: match right {
                QuantifiedComparisonRight::Subquery(subquery) => {
                    QuantifiedComparisonRight::Subquery(subquery)
                }
                QuantifiedComparisonRight::List(list) => QuantifiedComparisonRight::List(
                    list.into_iter()
                        .map(|item| {
                            bind_rollup_expr(item, group_by, projection_exprs, aliases, allow_alias)
                        })
                        .collect::<Result<Vec<_>, _>>()?,
                ),
            },
        },
        Expr::Between {
            expr,
            low,
            high,
            negated,
        } => Expr::Between {
            expr: Box::new(bind_rollup_expr(
                *expr,
                group_by,
                projection_exprs,
                aliases,
                allow_alias,
            )?),
            low: Box::new(bind_rollup_expr(
                *low,
                group_by,
                projection_exprs,
                aliases,
                allow_alias,
            )?),
            high: Box::new(bind_rollup_expr(
                *high,
                group_by,
                projection_exprs,
                aliases,
                allow_alias,
            )?),
            negated,
        },
        Expr::Like {
            expr,
            pattern,
            negated,
            escape_char,
        } => Expr::Like {
            expr: Box::new(bind_rollup_expr(
                *expr,
                group_by,
                projection_exprs,
                aliases,
                allow_alias,
            )?),
            pattern: Box::new(bind_rollup_expr(
                *pattern,
                group_by,
                projection_exprs,
                aliases,
                allow_alias,
            )?),
            negated,
            escape_char,
        },
        Expr::Case {
            operand,
            conditions,
            results,
            else_result,
        } => Expr::Case {
            operand: operand
                .map(|expr| {
                    bind_rollup_expr(*expr, group_by, projection_exprs, aliases, allow_alias)
                        .map(Box::new)
                })
                .transpose()?,
            conditions: conditions
                .into_vec()
                .into_iter()
                .map(|expr| {
                    bind_rollup_expr(expr, group_by, projection_exprs, aliases, allow_alias)
                })
                .collect::<Result<Vec<_>, _>>()?
                .into_boxed_slice(),
            results: results
                .into_iter()
                .map(|expr| {
                    bind_rollup_expr(expr, group_by, projection_exprs, aliases, allow_alias)
                })
                .collect::<Result<Vec<_>, _>>()?,
            else_result: else_result
                .map(|expr| {
                    bind_rollup_expr(*expr, group_by, projection_exprs, aliases, allow_alias)
                        .map(Box::new)
                })
                .transpose()?,
        },
        Expr::Function {
            name,
            args,
            distinct,
        } => Expr::Function {
            name,
            args: args
                .into_iter()
                .map(|arg| bind_rollup_expr(arg, group_by, projection_exprs, aliases, allow_alias))
                .collect::<Result<Vec<_>, _>>()?,
            distinct,
        },
        Expr::WindowFunction { name, args, spec } => {
            let crate::parser::WindowSpec {
                partition_by,
                order_by,
                frame,
                inherits,
            } = *spec;
            Expr::WindowFunction {
                name,
                args: args
                    .into_iter()
                    .map(|arg| {
                        bind_rollup_expr(arg, group_by, projection_exprs, aliases, allow_alias)
                    })
                    .collect::<Result<Vec<_>, _>>()?,
                spec: Box::new(crate::parser::WindowSpec {
                    partition_by: partition_by
                        .into_iter()
                        .map(|expr| {
                            bind_rollup_expr(expr, group_by, projection_exprs, aliases, allow_alias)
                        })
                        .collect::<Result<Vec<_>, _>>()?,
                    order_by: order_by
                        .into_iter()
                        .map(|mut item| {
                            item.expr = bind_rollup_expr(
                                item.expr,
                                group_by,
                                projection_exprs,
                                aliases,
                                allow_alias,
                            )?;
                            Ok(item)
                        })
                        .collect::<Result<Vec<_>, QueryError>>()?,
                    frame: frame
                        .map(|frame| {
                            bind_rollup_window_frame(
                                frame,
                                group_by,
                                projection_exprs,
                                aliases,
                                allow_alias,
                            )
                        })
                        .transpose()?,
                    inherits,
                }),
            }
        }
        Expr::Tuple(items) => Expr::Tuple(
            items
                .into_iter()
                .map(|item| {
                    bind_rollup_expr(item, group_by, projection_exprs, aliases, allow_alias)
                })
                .collect::<Result<Vec<_>, _>>()?,
        ),
        other => other,
    })
}

fn bind_rollup_window_frame(
    frame: crate::parser::WindowFrame,
    group_by: &[Expr],
    projection_exprs: &[Expr],
    aliases: &HashMap<String, Expr>,
    allow_alias: bool,
) -> Result<crate::parser::WindowFrame, QueryError> {
    Ok(crate::parser::WindowFrame {
        units: frame.units,
        start: bind_rollup_window_frame_bound(
            frame.start,
            group_by,
            projection_exprs,
            aliases,
            allow_alias,
        )?,
        end: bind_rollup_window_frame_bound(
            frame.end,
            group_by,
            projection_exprs,
            aliases,
            allow_alias,
        )?,
        exclusion: frame.exclusion,
    })
}

fn bind_rollup_window_frame_bound(
    bound: crate::parser::WindowFrameBound,
    group_by: &[Expr],
    projection_exprs: &[Expr],
    aliases: &HashMap<String, Expr>,
    allow_alias: bool,
) -> Result<crate::parser::WindowFrameBound, QueryError> {
    Ok(match bound {
        crate::parser::WindowFrameBound::Preceding(expr) => {
            crate::parser::WindowFrameBound::Preceding(Box::new(bind_rollup_expr(
                *expr,
                group_by,
                projection_exprs,
                aliases,
                allow_alias,
            )?))
        }
        crate::parser::WindowFrameBound::Following(expr) => {
            crate::parser::WindowFrameBound::Following(Box::new(bind_rollup_expr(
                *expr,
                group_by,
                projection_exprs,
                aliases,
                allow_alias,
            )?))
        }
        crate::parser::WindowFrameBound::UnboundedPreceding
        | crate::parser::WindowFrameBound::UnboundedFollowing
        | crate::parser::WindowFrameBound::CurrentRow => bound,
    })
}

thread_local! {
    /// Active collector for planner-emitted hint diagnostics. `None`
    /// means "not collecting"; `Some(v)` accumulates push calls from
    /// `apply_parallel_hints` and friends. Set by
    /// `with_hint_warning_collector` around the top-level
    /// `build_physical_plan`; reset to `None` inside nested CBO
    /// cost-estimate calls so duplicate warnings aren't emitted while
    /// the same subquery is re-planned for row counting.
    static HINT_WARNINGS: std::cell::RefCell<Option<Vec<String>>> =
        const { std::cell::RefCell::new(None) };
}

fn push_hint_warning(msg: impl Into<String>) {
    HINT_WARNINGS.with(|cell| {
        if let Some(v) = cell.borrow_mut().as_mut() {
            v.push(msg.into());
        }
    });
}

fn with_hint_warning_collector<F, R>(f: F) -> (R, Vec<String>)
where
    F: FnOnce() -> R,
{
    let saved = HINT_WARNINGS.with(|cell| cell.replace(Some(Vec::new())));
    let result = f();
    let collected = HINT_WARNINGS
        .with(|cell| cell.replace(saved))
        .unwrap_or_default();
    (result, collected)
}

fn with_hint_warnings_paused<F, R>(f: F) -> R
where
    F: FnOnce() -> R,
{
    let saved = HINT_WARNINGS.with(|cell| cell.replace(None));
    let result = f();
    HINT_WARNINGS.with(|cell| *cell.borrow_mut() = saved);
    result
}

fn dedup_preserving_order(v: &mut Vec<String>) {
    if v.len() <= 1 {
        return;
    }
    let mut seen: std::collections::HashSet<String> = std::collections::HashSet::new();
    v.retain(|s| seen.insert(s.clone()));
}

/// Walk `select`'s `query_hints` and emit diagnostics for logical issues
/// the parser can't catch on its own: PARALLEL/NO_PARALLEL conflicts,
/// HASH_JOIN vs NO_HASH_JOIN over the same table set, and references to
/// tables/indexes that don't exist in the query's FROM or schema.
fn validate_query_hints(db: &Database, select: &Select) -> Vec<String> {
    let mut warnings: Vec<String> = Vec::new();
    let hints = &select.query_hints;
    if hints.is_empty() {
        return warnings;
    }
    let has_parallel = hints
        .iter()
        .any(|h| matches!(h, crate::parser::QueryHint::Parallel { .. }));
    let has_no_parallel = hints
        .iter()
        .any(|h| matches!(h, crate::parser::QueryHint::NoParallel));
    if has_parallel && has_no_parallel {
        warnings.push("PARALLEL ignored: NO_PARALLEL takes precedence".to_string());
    }

    fn normalize_group(tables: &[String]) -> Vec<String> {
        let mut t: Vec<String> = tables.iter().map(|s| s.to_ascii_lowercase()).collect();
        t.sort();
        t
    }
    let mut hash_join_groups: Vec<Vec<String>> = Vec::new();
    let mut no_hash_join_groups: Vec<Vec<String>> = Vec::new();
    for h in hints {
        match h {
            crate::parser::QueryHint::HashJoin { tables } => {
                hash_join_groups.push(normalize_group(tables));
            }
            crate::parser::QueryHint::NoHashJoin { tables } => {
                no_hash_join_groups.push(normalize_group(tables));
            }
            _ => {}
        }
    }
    for hj in &hash_join_groups {
        if no_hash_join_groups.iter().any(|n| n == hj) {
            warnings.push(format!(
                "HASH_JOIN({}) and NO_HASH_JOIN({}) conflict",
                hj.join(","),
                hj.join(",")
            ));
        }
    }

    // Collect FROM/JOIN table names + aliases in lowercase for membership
    // checks. Qualification has already rewritten table names like `t` into
    // `db.t`, so register both the qualified form *and* the unqualified
    // suffix so user-written hints (which rarely include the schema) still
    // match.
    let mut from_names: std::collections::HashSet<String> = std::collections::HashSet::new();
    let mut name_for_table: std::collections::HashMap<String, String> =
        std::collections::HashMap::new();
    let register = |raw: &str,
                    real: &str,
                    names: &mut std::collections::HashSet<String>,
                    map: &mut std::collections::HashMap<String, String>| {
        let lower = raw.to_ascii_lowercase();
        names.insert(lower.clone());
        map.entry(lower.clone()).or_insert_with(|| real.to_string());
        if let Some(suffix) = lower.rsplit('.').next() {
            if suffix != lower {
                names.insert(suffix.to_string());
                map.entry(suffix.to_string())
                    .or_insert_with(|| real.to_string());
            }
        }
    };
    for t in &select.from {
        register(&t.name, &t.name, &mut from_names, &mut name_for_table);
        if let Some(alias) = &t.alias {
            register(alias, &t.name, &mut from_names, &mut name_for_table);
        }
    }
    for j in &select.joins {
        register(
            &j.table.name,
            &j.table.name,
            &mut from_names,
            &mut name_for_table,
        );
        if let Some(alias) = &j.table.alias {
            register(alias, &j.table.name, &mut from_names, &mut name_for_table);
        }
    }

    let check_table = |hint_name: &str, table: &str, out: &mut Vec<String>| -> Option<String> {
        let lower = table.to_ascii_lowercase();
        if !from_names.contains(&lower) {
            out.push(format!(
                "{hint_name}: table \"{table}\" not referenced in FROM"
            ));
            return None;
        }
        Some(
            name_for_table
                .get(&lower)
                .cloned()
                .unwrap_or_else(|| table.to_string()),
        )
    };

    let check_index = |hint_name: &str,
                       table_real: &str,
                       table_display: &str,
                       index: &str,
                       out: &mut Vec<String>| {
        let Ok(schema) = db.table(table_real) else {
            return;
        };
        let has = schema
            .iter_queryable_indexes()
            .any(|i| i.name.eq_ignore_ascii_case(index));
        if !has {
            out.push(format!(
                "{hint_name}: index \"{index}\" not found on \"{table_display}\""
            ));
        }
    };

    for h in hints {
        match h {
            crate::parser::QueryHint::UseIndex { table, indexes } => {
                if let Some(real) = check_table("USE_INDEX", table, &mut warnings) {
                    for index in indexes {
                        check_index("USE_INDEX", &real, table, index, &mut warnings);
                    }
                }
            }
            crate::parser::QueryHint::NoIndex { table, index } => {
                if let Some(real) = check_table("NO_INDEX", table, &mut warnings) {
                    check_index("NO_INDEX", &real, table, index, &mut warnings);
                }
            }
            crate::parser::QueryHint::IndexMerge { table, indexes }
            | crate::parser::QueryHint::NoIndexMerge { table, indexes } => {
                let hint_name = if matches!(h, crate::parser::QueryHint::IndexMerge { .. }) {
                    "INDEX_MERGE"
                } else {
                    "NO_INDEX_MERGE"
                };
                if let Some(real) = check_table(hint_name, table, &mut warnings) {
                    for index in indexes {
                        check_index(hint_name, &real, table, index, &mut warnings);
                    }
                }
            }
            crate::parser::QueryHint::FullScan { table } => {
                check_table("FULL_SCAN", table, &mut warnings);
            }
            crate::parser::QueryHint::ZoneMap { table } => {
                if let Some(real) = check_table("ZONE_MAP", table, &mut warnings) {
                    if let Ok(schema) = db.table(&real) {
                        if !db.zone_map_enabled() || !schema.zone_map_enabled() {
                            warnings.push(format!("ZONE_MAP: zone map is disabled on \"{table}\""));
                        } else if select.selection.as_ref().is_some_and(|filter| {
                            db.zone_map_cost_estimate_forced(&schema, filter).is_none()
                        }) {
                            warnings.push(format!(
                                "ZONE_MAP: predicate on \"{table}\" cannot be safely pruned"
                            ));
                        }
                    }
                }
            }
            crate::parser::QueryHint::NoZoneMap { table } => {
                check_table("NO_ZONE_MAP", table, &mut warnings);
            }
            crate::parser::QueryHint::IndexFfs { table, index } => {
                if let Some(real) = check_table("INDEX_FFS", table, &mut warnings) {
                    if let Some(idx) = index {
                        check_index("INDEX_FFS", &real, table, idx, &mut warnings);
                    }
                }
            }
            crate::parser::QueryHint::HashJoin { tables }
            | crate::parser::QueryHint::NoHashJoin { tables }
            | crate::parser::QueryHint::NlJoin { tables }
            | crate::parser::QueryHint::IndexNlj { tables } => {
                let hint_name = match h {
                    crate::parser::QueryHint::HashJoin { .. } => "HASH_JOIN",
                    crate::parser::QueryHint::NoHashJoin { .. } => "NO_HASH_JOIN",
                    crate::parser::QueryHint::NlJoin { .. } => "NL_JOIN",
                    crate::parser::QueryHint::IndexNlj { .. } => "INDEX_NLJ",
                    _ => unreachable!(),
                };
                for t in tables {
                    check_table(hint_name, t, &mut warnings);
                }
            }
            _ => {}
        }
    }
    warnings
}

/// Cheap structural check: does the plan tree contain any of the
/// parallel-execution node variants the planner wraps scans/aggregates
/// in? Used by `apply_parallel_hints` to detect when a forced
/// `PARALLEL(n)` hint didn't actually transform the tree (typically
/// because no eligible SeqScan / HashAggregate / Sort survived after
/// other rewrites).
fn plan_has_parallel_node(plan: &PhysicalPlan) -> bool {
    match plan {
        PhysicalPlan::Exchange { .. }
        | PhysicalPlan::ParallelSeqScan { .. }
        | PhysicalPlan::ParallelZoneMapScan { .. }
        | PhysicalPlan::ParallelIndexFastScan { .. }
        | PhysicalPlan::ParallelIndexRangeScan { .. }
        | PhysicalPlan::ParallelIndexAggregate { .. }
        | PhysicalPlan::ParallelHashAggregate { .. }
        | PhysicalPlan::ParallelRollupAggregate { .. }
        | PhysicalPlan::ParallelSort { .. } => true,
        PhysicalPlan::Limit { input, .. }
        | PhysicalPlan::Sort { input, .. }
        | PhysicalPlan::TopK { input, .. }
        | PhysicalPlan::TopWindow { input, .. }
        | PhysicalPlan::ParallelTopK { input, .. }
        | PhysicalPlan::Projection { input, .. }
        | PhysicalPlan::HashAggregate { input, .. }
        | PhysicalPlan::HashRollupAggregate { input, .. }
        | PhysicalPlan::HashGroupingSetsAggregate { input, .. }
        | PhysicalPlan::StreamingAggregate { input, .. }
        | PhysicalPlan::StreamingRollupAggregate { input, .. }
        | PhysicalPlan::StreamingGroupingSetsAggregate { input, .. }
        | PhysicalPlan::StreamingDistinct { input, .. }
        | PhysicalPlan::WindowAgg { input, .. } => plan_has_parallel_node(input),
        PhysicalPlan::NestedLoopJoin { left, right, .. }
        | PhysicalPlan::HashJoin { left, right, .. }
        | PhysicalPlan::AdaptiveJoin { left, right, .. } => {
            plan_has_parallel_node(left) || plan_has_parallel_node(right)
        }
        PhysicalPlan::IndexNestedLoopJoin { left, .. } => plan_has_parallel_node(left),
        _ => false,
    }
}

#[derive(Debug, Clone)]
struct ScopeTable {
    name: String,
    alias: Option<String>,
    /// All addressable column names (name resolution / ambiguity checks).
    columns: Vec<String>,
    /// Names `SELECT *` expands to — `columns` minus INVISIBLE ones.
    wildcard_columns: Vec<String>,
    visible: bool,
}

#[derive(Debug)]
struct ColumnScope<'a> {
    tables: Vec<ScopeTable>,
    outer: Option<&'a ColumnScope<'a>>,
}

pub(crate) fn validate_select_column_references(
    db: &Database,
    select: &Select,
) -> Result<(), QueryError> {
    validate_select_column_references_inner(db, select, None)
}

#[derive(Debug, Clone)]
struct JoinOutputColumn {
    name: String,
    expr: Expr,
    coalesced: bool,
    qualifiers: Vec<String>,
}

#[derive(Debug, Clone, Default)]
struct CoalescedRewrite {
    unqualified: HashMap<String, Expr>,
    qualified: HashMap<(String, String), Expr>,
}

/// Resolve MySQL `JOIN ... USING (...)` and `NATURAL ... JOIN` once table
/// schemas are known. The parser cannot do this because `NATURAL` needs
/// the left/right column sets, and `USING` changes both the join predicate
/// and the logical `SELECT *` shape.
pub fn resolve_join_using_natural(db: &Database, select: &mut Select) -> Result<(), QueryError> {
    // Set-operation branches resolve their own NATURAL/USING joins when
    // executed via the derived-table bridge (each runs through
    // `prepare_select_for_planning`), so only descend into a plain SELECT here.
    if let Some(derived) = select.derived.as_mut().and_then(SubQuery::as_select_mut) {
        resolve_join_using_natural(db, derived)?;
    }
    for join in &mut select.joins {
        if let Some(derived) = join.derived.as_mut().and_then(SubQuery::as_select_mut) {
            resolve_join_using_natural(db, derived)?;
        }
    }
    if !select
        .joins
        .iter()
        .any(|join| join.natural || !join.using_columns.is_empty())
    {
        return Ok(());
    }

    let Some(base) = select.from.first() else {
        return Ok(());
    };
    let mut preceding_scope = ColumnScope {
        tables: vec![scope_table_for_ref(
            db,
            base,
            select.derived.as_ref(),
            None,
            true,
        )?],
        outer: None,
    };
    let mut output = join_source_output_columns(db, base, select.derived.as_ref(), None)?;
    let mut has_coalescing_join = false;

    for join in &mut select.joins {
        let outer = join.lateral.then_some(&preceding_scope);
        let right = join_source_output_columns(db, &join.table, join.derived.as_ref(), outer)?;
        let right_scope = scope_table_for_ref(db, &join.table, join.derived.as_ref(), outer, true)?;
        preceding_scope.tables.push(right_scope);
        let using_columns = if join.natural {
            natural_join_columns(&output, &right)
        } else {
            ordered_using_columns(&output, &join.using_columns)
        };

        let left_rewrite = coalesced_rewrite_map(&output);
        if let Some(condition) = join.condition.as_mut() {
            rewrite_coalesced_identifiers(condition, &left_rewrite);
        }

        if using_columns.is_empty() {
            join.natural = false;
            output.extend(right);
            continue;
        }

        let condition = build_using_join_condition(&output, &right, &using_columns)?;
        if join.condition.is_none() {
            join.condition = Some(condition);
        }
        join.using_columns = using_columns.clone();
        join.natural = false;
        has_coalescing_join = true;
        output = if join.join_type == JoinType::Right {
            merge_join_output_columns(&right, &output, &using_columns)?
        } else {
            merge_join_output_columns(&output, &right, &using_columns)?
        };
    }

    let rewrite = coalesced_rewrite_map(&output);
    rewrite_select_level_coalesced_identifiers(select, &rewrite);
    if has_coalescing_join {
        expand_bare_wildcards(select, &output);
    }
    Ok(())
}

fn join_source_output_columns(
    db: &Database,
    table: &TableRef,
    derived: Option<&SubQuery>,
    outer: Option<&ColumnScope<'_>>,
) -> Result<Vec<JoinOutputColumn>, QueryError> {
    let scope = scope_table_for_ref(db, table, derived, outer, true)?;
    let qualifier = scope.alias.clone().unwrap_or_else(|| {
        scope
            .name
            .rsplit('.')
            .next()
            .unwrap_or(&scope.name)
            .to_string()
    });
    let qualifiers = scope_qualifiers(&scope);
    Ok(scope
        .wildcard_columns
        .into_iter()
        .map(|name| JoinOutputColumn {
            expr: Expr::CompoundIdentifier(vec![qualifier.clone(), name.clone()]),
            name,
            coalesced: false,
            qualifiers: qualifiers.clone(),
        })
        .collect())
}

fn scope_qualifiers(scope: &ScopeTable) -> Vec<String> {
    let mut out = Vec::new();
    let mut push = |name: &str| {
        let key = name.to_ascii_lowercase();
        if !out.contains(&key) {
            out.push(key);
        }
        if let Some(suffix) = name.rsplit('.').next() {
            let key = suffix.to_ascii_lowercase();
            if !out.contains(&key) {
                out.push(key);
            }
        }
    };
    push(&scope.name);
    if let Some(alias) = &scope.alias {
        push(alias);
    }
    out
}

fn natural_join_columns(left: &[JoinOutputColumn], right: &[JoinOutputColumn]) -> Vec<String> {
    let right_names = right
        .iter()
        .map(|col| col.name.to_ascii_lowercase())
        .collect::<HashSet<_>>();
    let mut seen = HashSet::new();
    let mut out = Vec::new();
    for col in left {
        let key = col.name.to_ascii_lowercase();
        if right_names.contains(&key) && seen.insert(key) {
            out.push(col.name.clone());
        }
    }
    out
}

fn ordered_using_columns(left: &[JoinOutputColumn], requested: &[String]) -> Vec<String> {
    if requested.is_empty() {
        return Vec::new();
    }
    let requested_set = requested
        .iter()
        .map(|name| name.to_ascii_lowercase())
        .collect::<HashSet<_>>();
    let mut seen = HashSet::new();
    let mut out = Vec::with_capacity(requested.len());
    for col in left {
        let key = col.name.to_ascii_lowercase();
        if requested_set.contains(&key) && seen.insert(key) {
            out.push(col.name.clone());
        }
    }
    for col in requested {
        let key = col.to_ascii_lowercase();
        if seen.insert(key) {
            out.push(col.clone());
        }
    }
    out
}

fn columns_matching<'a>(columns: &'a [JoinOutputColumn], name: &str) -> Vec<&'a JoinOutputColumn> {
    columns
        .iter()
        .filter(|col| col.name.eq_ignore_ascii_case(name))
        .collect()
}

fn build_using_join_condition(
    left: &[JoinOutputColumn],
    right: &[JoinOutputColumn],
    using_columns: &[String],
) -> Result<Expr, QueryError> {
    let mut conjuncts = Vec::with_capacity(using_columns.len());
    for column in using_columns {
        let left_matches = columns_matching(left, column);
        let right_matches = columns_matching(right, column);
        if left_matches.is_empty() || right_matches.is_empty() {
            return Err(error(
                crate::error::catalog::ER_BAD_FIELD_ERROR,
                format!("unknown column '{column}' in 'from clause'"),
            ));
        }
        if left_matches.len() != 1 || right_matches.len() != 1 {
            return Err(error(
                crate::error::catalog::ER_NON_UNIQ_ERROR,
                format!("Column '{column}' in from clause is ambiguous"),
            ));
        }
        conjuncts.push(Expr::Binary {
            left: Box::new(coalesce_join_columns(&left_matches)),
            op: BinaryOp::Eq,
            right: Box::new(coalesce_join_columns(&right_matches)),
        });
    }
    combine_conjuncts(conjuncts).ok_or_else(|| {
        error(
            crate::error::catalog::ER_NOT_SUPPORTED_YET,
            "JOIN USING column list",
        )
    })
}

fn merge_join_output_columns(
    left: &[JoinOutputColumn],
    right: &[JoinOutputColumn],
    using_columns: &[String],
) -> Result<Vec<JoinOutputColumn>, QueryError> {
    let mut common = Vec::with_capacity(using_columns.len());
    let mut using_set = HashSet::new();
    for column in using_columns {
        let left_matches = columns_matching(left, column);
        let right_matches = columns_matching(right, column);
        if left_matches.is_empty() || right_matches.is_empty() {
            return Err(error(
                crate::error::catalog::ER_BAD_FIELD_ERROR,
                format!("unknown column '{column}' in 'from clause'"),
            ));
        }
        let name = left_matches
            .first()
            .map(|col| col.name.clone())
            .unwrap_or_else(|| column.clone());
        let mut all = left_matches;
        all.extend(right_matches);
        common.push(JoinOutputColumn {
            name,
            expr: coalesce_join_columns(&all),
            coalesced: true,
            qualifiers: merged_qualifiers(&all),
        });
        using_set.insert(column.to_ascii_lowercase());
    }

    let mut out = common;
    out.extend(
        left.iter()
            .filter(|col| !using_set.contains(&col.name.to_ascii_lowercase()))
            .cloned(),
    );
    out.extend(
        right
            .iter()
            .filter(|col| !using_set.contains(&col.name.to_ascii_lowercase()))
            .cloned(),
    );
    Ok(out)
}

fn coalesce_join_columns(columns: &[&JoinOutputColumn]) -> Expr {
    if columns.len() == 1 {
        return columns[0].expr.clone();
    }
    Expr::Function {
        name: "COALESCE".to_string(),
        args: columns.iter().map(|col| col.expr.clone()).collect(),
        distinct: false,
    }
}

fn merged_qualifiers(columns: &[&JoinOutputColumn]) -> Vec<String> {
    let mut out = Vec::new();
    for col in columns {
        for qualifier in &col.qualifiers {
            if !out.contains(qualifier) {
                out.push(qualifier.clone());
            }
        }
    }
    out
}

fn coalesced_rewrite_map(columns: &[JoinOutputColumn]) -> CoalescedRewrite {
    let mut counts: HashMap<String, usize> = HashMap::new();
    for col in columns {
        *counts.entry(col.name.to_ascii_lowercase()).or_default() += 1;
    }
    let mut rewrite = CoalescedRewrite::default();
    for col in columns.iter().filter(|col| col.coalesced) {
        let column_key = col.name.to_ascii_lowercase();
        for qualifier in &col.qualifiers {
            rewrite
                .qualified
                .insert((qualifier.clone(), column_key.clone()), col.expr.clone());
        }
        if counts.get(&column_key).copied() == Some(1) {
            rewrite.unqualified.insert(column_key, col.expr.clone());
        }
    }
    rewrite
}

fn rewrite_select_level_coalesced_identifiers(select: &mut Select, rewrite: &CoalescedRewrite) {
    for item in &mut select.projection {
        if let SelectItem::Expr { expr, alias } = item {
            let implicit_name = if alias.is_none() {
                if let Expr::Identifier(name) = expr {
                    rewrite
                        .unqualified
                        .contains_key(&name.to_ascii_lowercase())
                        .then(|| name.clone())
                } else if let Expr::CompoundIdentifier(parts) = expr {
                    let column = parts.last().cloned();
                    let qualifier = (parts.len() >= 2).then(|| parts[parts.len() - 2].clone());
                    qualifier.and_then(|qualifier| {
                        column.and_then(|column| {
                            rewrite
                                .qualified
                                .contains_key(&(
                                    qualifier.to_ascii_lowercase(),
                                    column.to_ascii_lowercase(),
                                ))
                                .then_some(column)
                        })
                    })
                } else {
                    None
                }
            } else {
                None
            };
            rewrite_coalesced_identifiers(expr, rewrite);
            if let Some(name) = implicit_name {
                *alias = Some(name);
            }
        }
    }
    if let Some(selection) = select.selection.as_mut() {
        rewrite_coalesced_identifiers(selection, rewrite);
    }
    for expr in &mut select.group_by {
        rewrite_coalesced_identifiers(expr, rewrite);
    }
    if let Some(having) = select.having.as_mut() {
        rewrite_coalesced_identifiers(having, rewrite);
    }
    for order_by in &mut select.order_by {
        rewrite_coalesced_identifiers(&mut order_by.expr, rewrite);
    }
    for window in &mut select.named_windows {
        for expr in &mut window.partition_by {
            rewrite_coalesced_identifiers(expr, rewrite);
        }
        for order_by in &mut window.order_by {
            rewrite_coalesced_identifiers(&mut order_by.expr, rewrite);
        }
        if let Some(frame) = window.frame.as_mut() {
            rewrite_window_frame_coalesced_identifiers(frame, rewrite);
        }
    }
}

fn expand_bare_wildcards(select: &mut Select, columns: &[JoinOutputColumn]) {
    let mut expanded = Vec::with_capacity(select.projection.len() + columns.len());
    for item in std::mem::take(&mut select.projection) {
        match item {
            SelectItem::Wildcard => {
                expanded.extend(columns.iter().map(|col| SelectItem::Expr {
                    expr: col.expr.clone(),
                    alias: Some(col.name.clone()),
                }));
            }
            other => expanded.push(other),
        }
    }
    select.projection = expanded;
}

fn rewrite_coalesced_identifiers(expr: &mut Expr, rewrite: &CoalescedRewrite) {
    struct Rewrite<'a> {
        map: &'a CoalescedRewrite,
        descend: bool,
    }
    impl crate::parser::visit::AstVisitorMut for Rewrite<'_> {
        fn enter_expr(&mut self, expr: &mut Expr) {
            let replacement = match expr {
                Expr::Identifier(name) => self.map.unqualified.get(&name.to_ascii_lowercase()),
                Expr::CompoundIdentifier(parts) if parts.len() >= 2 => self.map.qualified.get(&(
                    parts[parts.len() - 2].to_ascii_lowercase(),
                    parts.last().unwrap().to_ascii_lowercase(),
                )),
                _ => None,
            };
            self.descend = replacement.is_none();
            if let Some(replacement) = replacement {
                *expr = replacement.clone();
            }
        }
        fn walk_expr_children(&self, _: &Expr) -> bool {
            self.descend
        }
        fn walk_select_children(&self, _: &Select) -> bool {
            false
        }
    }
    crate::parser::visit::visit_expr_mut(
        &mut Rewrite {
            map: rewrite,
            descend: true,
        },
        expr,
    );
}

fn rewrite_window_frame_coalesced_identifiers(
    frame: &mut crate::parser::WindowFrame,
    rewrite: &CoalescedRewrite,
) {
    rewrite_window_frame_bound_coalesced_identifiers(&mut frame.start, rewrite);
    rewrite_window_frame_bound_coalesced_identifiers(&mut frame.end, rewrite);
}

fn rewrite_window_frame_bound_coalesced_identifiers(
    bound: &mut crate::parser::WindowFrameBound,
    rewrite: &CoalescedRewrite,
) {
    match bound {
        crate::parser::WindowFrameBound::Preceding(expr)
        | crate::parser::WindowFrameBound::Following(expr) => {
            rewrite_coalesced_identifiers(expr, rewrite);
        }
        crate::parser::WindowFrameBound::UnboundedPreceding
        | crate::parser::WindowFrameBound::UnboundedFollowing
        | crate::parser::WindowFrameBound::CurrentRow => {}
    }
}

fn validate_select_column_references_inner(
    db: &Database,
    select: &Select,
    outer: Option<&ColumnScope<'_>>,
) -> Result<(), QueryError> {
    let scope = build_column_scope(db, select, outer)?;
    validate_select_in_column_scope(db, select, &scope)
}

/// DISTINCT removes the source row identity. An ORDER BY expression must be
/// evaluable entirely from selected expressions (or constants).
fn distinct_order_is_selected(
    db: &Database,
    expr: &Expr,
    select: &Select,
    scope: &ColumnScope<'_>,
) -> bool {
    fn column<'a>(expr: &'a Expr, scope: &ColumnScope<'_>) -> Option<(usize, &'a str)> {
        let (qualifier, name) = match expr {
            Expr::Identifier(name) => (None, name.as_str()),
            Expr::CompoundIdentifier(parts) => (
                parts.iter().rev().nth(1).map(String::as_str),
                parts.last()?.as_str(),
            ),
            _ => return None,
        };
        let mut tables = scope
            .tables
            .iter()
            .enumerate()
            .filter(|(_, t)| qualifier.is_none_or(|q| t.matches_name(q)) && t.has_column(name));
        let (idx, _) = tables.next()?;
        tables.next().is_none().then_some((idx, name))
    }
    fn canonical(expr: &Expr, scope: &ColumnScope<'_>) -> Expr {
        struct Binder<'a, 'b> {
            scope: &'a ColumnScope<'b>,
            nested: usize,
        }
        impl crate::parser::visit::AstVisitorMut for Binder<'_, '_> {
            fn enter_select(&mut self, _: &mut Select) {
                self.nested += 1;
            }
            fn exit_select(&mut self, _: &mut Select) {
                self.nested -= 1;
            }
            fn enter_expr(&mut self, expr: &mut Expr) {
                if self.nested == 0 {
                    if let Some((idx, name)) = column(expr, self.scope) {
                        *expr = Expr::CompoundIdentifier(vec![
                            format!("__scope{idx}"),
                            name.to_ascii_lowercase(),
                        ]);
                    }
                }
            }
        }
        let mut normalized = expr.clone();
        crate::parser::visit::visit_expr_mut(&mut Binder { scope, nested: 0 }, &mut normalized);
        normalized
    }
    let identity = column(expr, scope);
    if select.projection.iter().any(|item| match item {
        SelectItem::Wildcard => identity.is_some(),
        SelectItem::QualifiedWildcard(q) => identity.is_some_and(|(idx, _)| scope.tables[idx].matches_name(q)),
        SelectItem::Expr { expr: selected, alias } => expr_semantically_eq(selected, expr)
            || expr_semantically_eq(&canonical(selected, scope), &canonical(expr, scope))
            || matches!(expr, Expr::Identifier(n) if alias.as_ref().is_some_and(|a| a.eq_ignore_ascii_case(n)))
            || identity.zip(column(selected, scope)).is_some_and(|((a,n),(b,m))| a==b && n.eq_ignore_ascii_case(m)),
    }) { return true; }
    let allowed = |e: &Expr| distinct_order_is_selected(db, e, select, scope);
    let subquery_allowed = || {
        let mut references = Vec::new();
        collect_correlated_group_columns(db, expr, scope, &mut HashSet::new(), &mut references)
            .is_ok()
            && references.iter().all(allowed)
    };
    match expr {
        Expr::Identifier(_) | Expr::CompoundIdentifier(_) => false,
        Expr::Unary { expr, .. }
        | Expr::IsNull(expr)
        | Expr::IsNotNull(expr)
        | Expr::GroupKeyRef { expr, .. } => allowed(expr),
        Expr::Binary { left, right, .. } => allowed(left) && allowed(right),
        Expr::Between {
            expr, low, high, ..
        } => allowed(expr) && allowed(low) && allowed(high),
        Expr::Like { expr, pattern, .. } => allowed(expr) && allowed(pattern),
        Expr::InList { expr, list, .. } => allowed(expr) && list.iter().all(allowed),
        Expr::Function { name, args, .. } => {
            !is_aggregate_function(name) && args.iter().all(allowed)
        }
        Expr::Tuple(args) => args.iter().all(allowed),
        Expr::Case {
            operand,
            conditions,
            results,
            else_result,
        } => {
            operand.as_deref().is_none_or(allowed)
                && conditions.iter().all(allowed)
                && results.iter().all(allowed)
                && else_result.as_deref().is_none_or(allowed)
        }
        Expr::Subquery(_) | Expr::Exists { .. } => subquery_allowed(),
        Expr::InSubquery { expr, .. } => allowed(expr) && subquery_allowed(),
        Expr::QuantifiedComparison { left, right, .. } => {
            allowed(left)
                && match right {
                    QuantifiedComparisonRight::Subquery(_) => subquery_allowed(),
                    QuantifiedComparisonRight::List(items) => items.iter().all(allowed),
                }
        }
        Expr::WindowFunction { .. } => false,
        _ => true,
    }
}

fn validate_select_in_column_scope(
    db: &Database,
    select: &Select,
    scope: &ColumnScope<'_>,
) -> Result<(), QueryError> {
    let aliases = projection_aliases(select);
    for item in &select.projection {
        match item {
            SelectItem::Wildcard => {}
            SelectItem::QualifiedWildcard(qualifier) => scope.resolve_qualifier(qualifier)?,
            SelectItem::Expr { expr, .. } => {
                validate_expr_columns(db, expr, &scope, None, true)?;
            }
        }
    }
    if let Some(selection) = &select.selection {
        validate_expr_columns(db, selection, &scope, None, false)?;
    }
    for join in &select.joins {
        if let Some(condition) = &join.condition {
            validate_expr_columns(db, condition, &scope, None, false)?;
        }
    }
    for expr in &select.group_by {
        // Nested/derived query blocks can be validated while their parent
        // scope is being built, before the child reaches
        // `prepare_select_for_planning` and has `GROUP BY <select-alias>`
        // rewritten to the projection expression. Accept projection aliases
        // at this early validation boundary; the executable/planner-owned
        // SELECT still runs `resolve_group_by_refs`, which enforces MySQL's
        // FROM-column-before-alias precedence and emits ambiguity warnings.
        validate_expr_columns(db, expr, &scope, Some(&aliases), true)?;
    }
    if let Some(having) = &select.having {
        validate_expr_columns(db, having, &scope, Some(&aliases), true)?;
    }
    for (index, order_by) in select.order_by.iter().enumerate() {
        validate_expr_columns(db, &order_by.expr, &scope, Some(&aliases), true)?;
        if select.distinct && !distinct_order_is_selected(db, &order_by.expr, select, scope) {
            return Err(error(crate::error::catalog::ER_FIELD_IN_ORDER_NOT_SELECT,
                format!("Expression #{} of ORDER BY clause is not in SELECT list; this is incompatible with DISTINCT", index + 1)));
        }
    }
    for window in &select.named_windows {
        for expr in &window.partition_by {
            validate_expr_columns(db, expr, &scope, None, true)?;
        }
        for order_by in &window.order_by {
            validate_expr_columns(db, &order_by.expr, &scope, None, true)?;
        }
        if let Some(frame) = &window.frame {
            validate_window_frame_columns(db, frame, &scope)?;
        }
    }
    Ok(())
}

fn build_column_scope<'a>(
    db: &Database,
    select: &Select,
    outer: Option<&'a ColumnScope<'a>>,
) -> Result<ColumnScope<'a>, QueryError> {
    let mut tables = Vec::new();
    if let Some(base) = select.from.first() {
        if base.name == "$nested_join_preserve" {
            let inner = select
                .derived
                .as_ref()
                .and_then(SubQuery::leftmost_select)
                .ok_or_else(|| {
                    error(
                        crate::error::catalog::ER_NOT_SUPPORTED_YET,
                        "invalid nested join group",
                    )
                })?;
            let inner_scope = build_column_scope(db, inner, outer)?;
            validate_select_in_column_scope(db, inner, &inner_scope)?;
            tables.extend(inner_scope.tables);
        } else {
            tables.push(scope_table_for_ref(
                db,
                base,
                select.derived.as_ref(),
                outer,
                true,
            )?);
        }
    }
    for join in &select.joins {
        let visible = !matches!(join.join_type, JoinType::Semi | JoinType::Anti);
        if join.table.name == "$nested_join_preserve" {
            let inner = join
                .derived
                .as_ref()
                .and_then(SubQuery::leftmost_select)
                .ok_or_else(|| {
                    error(
                        crate::error::catalog::ER_NOT_SUPPORTED_YET,
                        "nested join group must contain a SELECT query block",
                    )
                })?;
            let mut inner_scope = build_column_scope(db, inner, outer)?;
            validate_select_in_column_scope(db, inner, &inner_scope)?;
            for table in &mut inner_scope.tables {
                table.visible &= visible;
            }
            tables.extend(inner_scope.tables);
            continue;
        }
        if join.lateral {
            let lateral_outer = ColumnScope {
                tables: tables.clone(),
                outer,
            };
            tables.push(scope_table_for_ref(
                db,
                &join.table,
                join.derived.as_ref(),
                Some(&lateral_outer),
                visible,
            )?);
            continue;
        }
        tables.push(scope_table_for_ref(
            db,
            &join.table,
            join.derived.as_ref(),
            outer,
            visible,
        )?);
    }
    Ok(ColumnScope { tables, outer })
}

/// Bind derived output names only after wildcard/set-operation width is known.
pub(crate) fn apply_derived_column_aliases<'a>(
    table: &TableRef,
    columns: impl ExactSizeIterator<Item = &'a mut String>,
) -> Result<(), QueryError> {
    if table.column_aliases.is_empty() {
        return Ok(());
    }
    if columns.len() != table.column_aliases.len() {
        return Err(error(
            crate::error::catalog::ER_VIEW_WRONG_LIST,
            "derived table column count does not match its column aliases",
        ));
    }
    let mut seen = std::collections::HashSet::new();
    for (name, alias) in columns.zip(&table.column_aliases) {
        if !seen.insert(alias.to_ascii_lowercase()) {
            return Err(error(
                crate::error::catalog::ER_DUP_FIELDNAME,
                format!("Duplicate column name '{alias}'"),
            ));
        }
        name.clone_from(alias);
    }
    Ok(())
}

fn scope_table_for_ref(
    db: &Database,
    table: &TableRef,
    derived: Option<&SubQuery>,
    outer: Option<&ColumnScope<'_>>,
    visible: bool,
) -> Result<ScopeTable, QueryError> {
    if let Some(derived @ SubQuery::WithCte(with)) = derived {
        let stubs = crate::engine::executor::cte::describe_definitions(&with.ctes, |query| {
            crate::engine::executor::select::describe_subquery_columns(db, query)
        })?;
        let mut columns = crate::engine::executor::cte::with_cte_registry_extended(stubs, || {
            if let crate::parser::QueryExpr::Select(body) = &with.body {
                let scope = build_column_scope(db, body, outer)?;
                validate_select_in_column_scope(db, body, &scope)?;
                select_output_column_names(body, &scope)
            } else {
                crate::engine::executor::select::describe_subquery_columns(db, derived)
                    .map(|columns| columns.into_iter().map(|column| column.name).collect())
            }
        })?;
        apply_derived_column_aliases(table, columns.iter_mut())?;
        return Ok(ScopeTable {
            name: table.name.clone(),
            alias: table.alias.clone(),
            wildcard_columns: columns.clone(),
            columns,
            visible,
        });
    }
    if let Some(derived) = derived {
        // A set-operation derived table exposes the output columns of its
        // leftmost SELECT (SQL semantics); each branch validates itself when
        // executed through the derived-table bridge (`execute_subquery`).
        let inner = derived.leftmost_select().ok_or_else(|| {
            error(
                crate::error::catalog::ER_NOT_SUPPORTED_YET,
                "derived set operation branch must be a SELECT",
            )
        })?;
        // Validate and describe using the same scope. Rebuilding it here
        // traversed each nested derived/view subtree twice per level (2^N).
        let inner_scope = build_column_scope(db, inner, outer)?;
        validate_select_in_column_scope(db, inner, &inner_scope)?;
        let mut columns = select_output_column_names(inner, &inner_scope)?;
        apply_derived_column_aliases(table, columns.iter_mut())?;
        return Ok(ScopeTable {
            name: table.name.clone(),
            alias: table.alias.clone(),
            wildcard_columns: columns.clone(),
            columns,
            visible,
        });
    }
    if let Some(spec) = table.generate_series.as_deref() {
        let columns = generate_series_column_names(spec);
        return Ok(ScopeTable {
            name: table.name.clone(),
            alias: table.alias.clone(),
            wildcard_columns: columns.clone(),
            columns,
            visible,
        });
    }
    if let Some(PhysicalPlan::CteScan { columns, .. }) = try_plan_cte_scan(table) {
        let columns: Vec<String> = columns.into_iter().map(|column| column.name).collect();
        return Ok(ScopeTable {
            name: table.name.clone(),
            alias: table.alias.clone(),
            wildcard_columns: columns.clone(),
            columns,
            visible,
        });
    }
    if let Ok(columns) = crate::engine::information_schema::columns_for_table_name(&table.name) {
        let columns: Vec<String> = columns.into_iter().map(|column| column.name).collect();
        return Ok(ScopeTable {
            name: crate::storage::split_table_name(crate::storage::DEFAULT_DATABASE, &table.name).1,
            alias: table.alias.clone(),
            wildcard_columns: columns.clone(),
            columns,
            visible,
        });
    }
    if let Some(error) = crate::engine::information_schema::hidden_system_table_error(&table.name) {
        return Err(error);
    }
    let schema = db.table(&table.name)?;
    Ok(ScopeTable {
        name: schema.name.clone(),
        alias: table.alias.clone(),
        columns: schema
            .iter_visible_columns()
            .map(|(_, column)| column.name.clone())
            .collect(),
        wildcard_columns: schema
            .iter_select_columns()
            .map(|(_, column)| column.name.clone())
            .collect(),
        visible,
    })
}

/// Freeze wildcard projection slots from the same scopes used for column binding.
/// Source restrictions (partition, snapshot and hints) remain on their relations.
pub(crate) fn freeze_select_wildcards(
    db: &Database,
    select: &mut Select,
) -> Result<(), QueryError> {
    if select.values.is_some() {
        return Ok(());
    }
    let scope = build_column_scope(db, select, None)?;
    let mut projection = Vec::new();
    for item in std::mem::take(&mut select.projection) {
        let requested_qualifier = match &item {
            SelectItem::Wildcard => None,
            SelectItem::QualifiedWildcard(name) => Some(name.as_str()),
            _ => {
                projection.push(item);
                continue;
            }
        };
        for table in scope.tables.iter().filter(|table| {
            table.visible && requested_qualifier.is_none_or(|name| table.matches_name(name))
        }) {
            let qualifier = table
                .alias
                .as_deref()
                .unwrap_or(&table.name)
                .rsplit('.')
                .next()
                .unwrap_or(&table.name);
            for name in &table.wildcard_columns {
                projection.push(SelectItem::Expr {
                    expr: Expr::CompoundIdentifier(vec![qualifier.to_owned(), name.clone()]),
                    alias: Some(name.clone()),
                });
            }
        }
    }
    select.projection = projection;
    Ok(())
}

pub(crate) fn query_output_column_names(
    db: &Database,
    select: &Select,
) -> Result<Vec<String>, QueryError> {
    let scope = build_column_scope(db, select, None)?;
    select_output_column_names(select, &scope)
}

fn select_output_column_names(
    select: &Select,
    scope: &ColumnScope<'_>,
) -> Result<Vec<String>, QueryError> {
    if let Some(values) = &select.values {
        if select
            .projection
            .iter()
            .any(|item| matches!(item, SelectItem::Wildcard))
        {
            return Ok(values.columns.clone());
        }
    }
    if let Some(json_table) = &select.json_table {
        if select
            .projection
            .iter()
            .any(|item| matches!(item, SelectItem::Wildcard))
        {
            let mut columns = Vec::new();
            json_table_column_names(&json_table.columns, &mut columns);
            return Ok(columns);
        }
    }
    let mut columns = Vec::new();
    for item in &select.projection {
        match item {
            SelectItem::Wildcard => {
                for table in &scope.tables {
                    columns.extend(table.wildcard_columns.iter().cloned());
                }
            }
            SelectItem::QualifiedWildcard(qualifier) => {
                for table in scope.matching_tables(qualifier) {
                    columns.extend(table.wildcard_columns.iter().cloned());
                }
            }
            SelectItem::Expr { expr, alias } => {
                columns.push(
                    alias
                        .clone()
                        .unwrap_or_else(|| crate::engine::expr::expr_name(expr)),
                );
            }
        }
    }
    Ok(columns)
}

fn json_table_column_names(columns: &[crate::parser::JsonTableColumnDef], out: &mut Vec<String>) {
    for column in columns {
        match column {
            crate::parser::JsonTableColumnDef::Ordinality { name }
            | crate::parser::JsonTableColumnDef::Named { name, .. } => out.push(name.clone()),
            crate::parser::JsonTableColumnDef::Nested { columns, .. } => {
                json_table_column_names(columns, out);
            }
        }
    }
}

fn projection_aliases(select: &Select) -> HashSet<String> {
    select
        .projection
        .iter()
        .filter_map(|item| match item {
            SelectItem::Expr {
                alias: Some(alias), ..
            } => Some(alias.to_ascii_lowercase()),
            _ => None,
        })
        .collect()
}

impl ColumnScope<'_> {
    fn resolve_qualifier(&self, qualifier: &str) -> Result<(), QueryError> {
        if self.matching_tables(qualifier).next().is_some() {
            return Ok(());
        }
        if let Some(outer) = self.outer {
            return outer.resolve_qualifier(qualifier);
        }
        Err(crate::error::messages::bad_field(qualifier, "field list"))
    }

    fn resolve_column(
        &self,
        qualifier: Option<&str>,
        column: &str,
        output_only: bool,
    ) -> Result<(), QueryError> {
        let matches = self.column_matches(qualifier, column, output_only);
        if matches == 1 {
            return Ok(());
        }
        if matches > 1 {
            return Err(error(
                crate::error::catalog::ER_NON_UNIQ_ERROR,
                format!("column '{column}' is ambiguous"),
            ));
        }
        if let Some(outer) = self.outer {
            return outer.resolve_column(qualifier, column, output_only);
        }
        Err(crate::error::messages::bad_field(column, "field list"))
    }

    fn column_matches(&self, qualifier: Option<&str>, column: &str, output_only: bool) -> usize {
        self.tables
            .iter()
            .filter(|table| !output_only || table.visible)
            .filter(|table| qualifier.is_none_or(|q| table.matches_name(q)))
            .filter(|table| table.has_column(column))
            .count()
    }

    fn matching_tables<'a>(&'a self, qualifier: &'a str) -> impl Iterator<Item = &'a ScopeTable> {
        self.tables
            .iter()
            .filter(move |table| table.matches_name(qualifier))
    }
}

impl ScopeTable {
    fn matches_name(&self, name: &str) -> bool {
        // SQL scoping: once a source is aliased, the alias is the *only* valid
        // qualifier — the base table/CTE name is hidden. This is what keeps a
        // CTE referenced several times, some copies aliased, unambiguous:
        // `FROM v1, v1 v1_lag, v1 v1_lead` makes `v1.x` match only the
        // unaliased `v1`, not the aliased copies (TPC-DS q47 / q57). Matching
        // the base name through an alias produced a spurious `1052 ambiguous`.
        match &self.alias {
            Some(alias) => alias.eq_ignore_ascii_case(name),
            None => self.name.eq_ignore_ascii_case(name),
        }
    }

    fn has_column(&self, column: &str) -> bool {
        self.columns
            .iter()
            .any(|candidate| candidate.eq_ignore_ascii_case(column))
    }
}

/// Approximate output tuple width (bytes) for a physical plan node.
/// Used by Phase 3 cost formulas to size join build-tables and sort
/// memory. Falls back to a 64-byte conservative default when the
/// width can't be inferred from a base table.
pub(crate) fn plan_output_width(plan: &PhysicalPlan) -> f64 {
    match plan {
        PhysicalPlan::NullExtend { input, table, .. } => {
            plan_output_width(input) + projected_row_width(table, None)
        }

        PhysicalPlan::SeqScan {
            table,
            projected_columns,
            ..
        }
        | PhysicalPlan::ZoneMapScan {
            table,
            projected_columns,
            ..
        }
        | PhysicalPlan::IndexMerge {
            table,
            projected_columns,
            ..
        }
        | PhysicalPlan::ParallelSeqScan {
            table,
            projected_columns,
            ..
        }
        | PhysicalPlan::ParallelZoneMapScan {
            table,
            projected_columns,
            ..
        }
        | PhysicalPlan::IndexFastScan {
            table,
            projected_columns,
            ..
        }
        | PhysicalPlan::ParallelIndexFastScan {
            table,
            projected_columns,
            ..
        }
        | PhysicalPlan::ParallelIndexRangeScan {
            table,
            projected_columns,
            ..
        }
        | PhysicalPlan::IndexRangeScan {
            table,
            projected_columns,
            ..
        }
        | PhysicalPlan::FulltextScan {
            table,
            projected_columns,
            ..
        }
        | PhysicalPlan::SpatialScan {
            table,
            projected_columns,
            ..
        }
        | PhysicalPlan::VectorIndexScan {
            table,
            projected_columns,
            ..
        } => projected_row_width(table, projected_columns.as_deref().map(|v| v.as_slice())),
        // VectorBruteScan re-materialises full winner rows (no projection
        // pushdown), so its row width is the table's full projected width.
        PhysicalPlan::VectorBruteScan { table, .. } => projected_row_width(table, None),
        PhysicalPlan::IndexLookup {
            table,
            projected_columns,
            ..
        }
        | PhysicalPlan::IndexMultiLookup {
            table,
            projected_columns,
            ..
        }
        | PhysicalPlan::IndexInLookup {
            table,
            projected_columns,
            ..
        } => projected_row_width(table, projected_columns.as_deref().map(|v| v.as_slice())),
        PhysicalPlan::IndexNestedLoopJoin {
            left,
            right_table,
            right_projected_columns,
            ..
        } => {
            plan_output_width(left)
                + projected_row_width(
                    right_table,
                    right_projected_columns.as_deref().map(|v| v.as_slice()),
                )
        }
        PhysicalPlan::NestedLoopJoin { left, right, .. }
        | PhysicalPlan::HashJoin { left, right, .. }
        | PhysicalPlan::AdaptiveJoin { left, right, .. } => {
            plan_output_width(left) + plan_output_width(right)
        }
        PhysicalPlan::SetOp { left, right, .. } => {
            plan_output_width(left).max(plan_output_width(right))
        }
        PhysicalPlan::RecursiveUnion { anchor, .. } => plan_output_width(anchor),
        PhysicalPlan::HashAggregate { .. }
        | PhysicalPlan::HashRollupAggregate { .. }
        | PhysicalPlan::HashGroupingSetsAggregate { .. }
        | PhysicalPlan::StreamingAggregate { .. }
        | PhysicalPlan::StreamingRollupAggregate { .. }
        | PhysicalPlan::StreamingGroupingSetsAggregate { .. }
        | PhysicalPlan::ParallelHashAggregate { .. }
        | PhysicalPlan::ParallelRollupAggregate { .. }
        | PhysicalPlan::ParallelGroupingSetsAggregate { .. }
        | PhysicalPlan::CoveringIndexAggregate { .. }
        | PhysicalPlan::ParallelIndexAggregate { .. } => 64.0,
        PhysicalPlan::IndexAggregate { .. } => 16.0,
        PhysicalPlan::Sort { input, .. }
        | PhysicalPlan::IncrementalSort { input, .. }
        | PhysicalPlan::TopK { input, .. }
        | PhysicalPlan::TopWindow { input, .. }
        | PhysicalPlan::ParallelTopK { input, .. }
        | PhysicalPlan::Limit { input, .. }
        | PhysicalPlan::WindowAgg { input, .. }
        | PhysicalPlan::Projection { input, .. }
        | PhysicalPlan::Filter { input, .. }
        | PhysicalPlan::Apply { outer: input, .. }
        | PhysicalPlan::Materialize { input, .. }
        | PhysicalPlan::StreamingDistinct { input, .. }
        | PhysicalPlan::Exchange { input, .. }
        | PhysicalPlan::ParallelSort { input, .. } => plan_output_width(input),
        PhysicalPlan::SystemScan {
            columns,
            projected_columns,
            ..
        } => {
            16.0 + (projected_columns
                .as_deref()
                .map_or(columns.len(), |projection| projection.len()) as f64)
                * 32.0
        }
        PhysicalPlan::CteScan { columns, .. }
        | PhysicalPlan::GenerateSeries { columns, .. }
        | PhysicalPlan::ValuesScan { columns, .. }
        | PhysicalPlan::JsonTableScan { columns, .. } => 16.0 + (columns.len() as f64) * 32.0,
    }
}

fn projected_row_width(table: &PlanTable, projection: Option<&[usize]>) -> f64 {
    use crate::engine::cbo::operator_cost::estimated_row_width;
    let full = estimated_row_width(&table.schema);
    let Some(columns) = projection else {
        return full;
    };
    if columns.is_empty() || table.schema.columns.is_empty() {
        return full;
    }
    let body = columns
        .iter()
        .filter_map(|index| table.schema.columns.get(*index))
        .map(|column| {
            table
                .schema
                .stats
                .columns
                .get(&column.name.to_ascii_lowercase())
                .and_then(|stats| stats.average_width_bytes)
                .unwrap_or(column.data_type.key_byte_length() as f64)
        })
        .sum::<f64>();
    (16.0 + body).max(8.0)
}

impl PhysicalPlan {
    pub(crate) fn output_width(&self) -> f64 {
        plan_output_width(self)
    }

    pub(crate) fn children(&self) -> PlanChildren<'_> {
        plan_children(self)
    }

    pub(crate) fn children_mut(&mut self) -> PlanChildrenMut<'_> {
        plan_children_mut(self)
    }

    pub(crate) fn any(&self, mut predicate: impl FnMut(&PhysicalPlan) -> bool) -> bool {
        plan_any(self, &mut predicate)
    }

    pub(crate) fn walk(&self, mut visitor: impl FnMut(&PhysicalPlan)) {
        walk_plan(self, &mut visitor);
    }

    pub(crate) fn walk_with_ids(&self, mut visitor: impl FnMut(PlanNodeId, &PhysicalPlan)) {
        walk_plan_with_ids(self, &mut 0, &mut visitor);
    }

    pub(crate) fn walk_mut(&mut self, mut visitor: impl FnMut(&mut PhysicalPlan)) {
        walk_plan_mut(self, &mut visitor);
    }

    pub(crate) fn node_count(&self) -> usize {
        let mut count = 0usize;
        self.walk_with_ids(|_, _| count += 1);
        count
    }
}

// Cost and cardinality are present on every physical node. Declare that
// exhaustive layout once for read and write access; new variants cannot be
// added without updating all three accessors together.
macro_rules! define_plan_metadata {
    ($($variant:ident),+ $(,)?) => {
        pub fn estimated_rows(plan: &PhysicalPlan) -> f64 {
            match plan { $(PhysicalPlan::$variant { estimated_rows, .. })|+ => *estimated_rows }
        }
        pub(crate) fn plan_cost(plan: &PhysicalPlan) -> crate::engine::cbo::Cost {
            match plan { $(PhysicalPlan::$variant { cost, .. })|+ => *cost }
        }
        fn plan_cost_mut(plan: &mut PhysicalPlan) -> &mut crate::engine::cbo::Cost {
            match plan { $(PhysicalPlan::$variant { cost, .. })|+ => cost }
        }
    };
}

define_plan_metadata! {
    NullExtend,
    ValuesScan,
    JsonTableScan,
    SystemScan,
    SetOp,
    Materialize,
    RecursiveUnion,
    SeqScan,
    ZoneMapScan,
    IndexMerge,
    IndexLookup,
    IndexMultiLookup,
    IndexRangeScan,
    IndexFastScan,
    FulltextScan,
    SpatialScan,
    VectorIndexScan,
    VectorBruteScan,
    IndexInLookup,
    NestedLoopJoin,
    IndexNestedLoopJoin,
    HashJoin,
    AdaptiveJoin,
    HashAggregate,
    HashRollupAggregate,
    HashGroupingSetsAggregate,
    Sort,
    IncrementalSort,
    TopK,
    TopWindow,
    Limit,
    Projection,
    WindowAgg,
    CteScan,
    GenerateSeries,
    StreamingAggregate,
    StreamingRollupAggregate,
    StreamingGroupingSetsAggregate,
    StreamingDistinct,
    IndexAggregate,
    CoveringIndexAggregate,
    Exchange,
    ParallelSeqScan,
    ParallelZoneMapScan,
    ParallelIndexFastScan,
    ParallelIndexRangeScan,
    ParallelIndexAggregate,
    ParallelHashAggregate,
    ParallelRollupAggregate,
    ParallelGroupingSetsAggregate,
    ParallelTopK,
    Filter,
    Apply,
    ParallelSort,
}

fn build_physical_plan(db: &Database, select: &Select) -> Result<PhysicalPlan, QueryError> {
    let cbo_ctx = crate::engine::cbo::CboContext::from_db(db);
    if select.table.is_none()
        && select.from.is_empty()
        && select.values.is_none()
        && select.json_table.is_none()
    {
        // A source-free SELECT is a one-row, zero-column relation followed by
        // the ordinary projection/filter/aggregate pipeline. This makes
        // constants and prepared expressions first-class ValuesScan leaves in
        // set operations, derived tables and CTAS instead of routing them to a
        // separate evaluator.
        let scan = PhysicalPlan::ValuesScan {
            rows: vec![Vec::new()],
            columns: Vec::new(),
            filter: select.selection.clone(),
            estimated_rows: 1.0,
            cost: crate::engine::cbo::Cost::scalar(0.01),
        };
        return finish_plan(db, &cbo_ctx, select, scan);
    }
    if let Some(values) = &select.values {
        let width = values.columns.len();
        if values.rows.iter().any(|row| row.len() != width) {
            return Err(error(
                crate::error::catalog::ER_WRONG_VALUE_COUNT_ON_ROW,
                "Column count doesn't match value count at row 1",
            ));
        }
        let resolve_column = |_expr: &Expr| None;
        let resolve_placeholder = |_index: usize| None;
        let type_context = crate::engine::expr_type::TypeContext::context_free(
            &resolve_column,
            &resolve_placeholder,
        );
        let columns = values
            .columns
            .iter()
            .enumerate()
            .map(|(index, name)| crate::types::Column {
                name: name.clone(),
                column_type: values
                    .rows
                    .iter()
                    .filter_map(|row| row.get(index))
                    .map(|expr| crate::engine::expr_type::infer_expr_type(expr, &type_context))
                    .reduce(crate::engine::expr_type::aggregate_branch_types)
                    .map(|ty| ty.column_type())
                    .unwrap_or_default(),
                source: Default::default(),
            })
            .collect();
        let estimated_rows = values.rows.len() as f64;
        let scan = PhysicalPlan::ValuesScan {
            rows: values.rows.clone(),
            columns,
            filter: select.selection.clone(),
            estimated_rows,
            cost: crate::engine::cbo::Cost::scalar(estimated_rows * 0.01),
        };
        return finish_plan(db, &cbo_ctx, select, scan);
    }
    if let Some(source) = &select.json_table {
        let columns = crate::engine::executor::select::json_table_columns(source);
        let mut seen = std::collections::HashSet::new();
        if let Some(duplicate) = columns
            .iter()
            .find(|column| !seen.insert(column.name.to_ascii_lowercase()))
        {
            return Err(error(
                crate::error::catalog::ER_DUP_FIELDNAME,
                format!("Duplicate column name '{}'", duplicate.name),
            ));
        }
        let scan = PhysicalPlan::JsonTableScan {
            source: source.clone(),
            relation: select
                .from
                .first()
                .and_then(|table| table.alias.clone())
                .unwrap_or_else(|| "json_table".to_string()),
            columns,
            filter: select.selection.clone(),
            estimated_rows: 100.0,
            cost: crate::engine::cbo::Cost::scalar(1.0),
        };
        return finish_plan(db, &cbo_ctx, select, scan);
    }
    let Some(base) = select.from.first() else {
        return Err(error(
            crate::error::catalog::ER_NOT_SUPPORTED_YET,
            "constant SELECT does not use CBO planner",
        ));
    };
    // CTE short-circuit: if FROM[0] is a CTE and there are no joins, build a
    // CteScan-rooted plan with optional filter/sort/limit/projection on top.
    // CTE-with-JOIN currently goes through the dedicated in-memory join path
    // outside the planner.
    if select.joins.is_empty() {
        if let Some(spec) = base.generate_series.as_deref() {
            let mut scan = plan_generate_series(base, spec)?;
            if let Some(predicate) = &select.selection {
                if let PhysicalPlan::GenerateSeries { filter, .. } = &mut scan {
                    *filter = Some(predicate.clone());
                }
            }
            return finish_plan(db, &cbo_ctx, select, scan);
        }
        if let Some(mut scan) = try_plan_cte_scan(base) {
            if let Some(predicate) = &select.selection {
                if let PhysicalPlan::CteScan {
                    filter,
                    estimated_rows,
                    cost,
                    columns,
                    name,
                    ..
                } = &mut scan
                {
                    *filter = Some(predicate.clone());
                    // P1-4: refine the post-filter row estimate from the
                    // materialised CTE's per-column NDV so downstream
                    // operator sizing/parallelism sees a realistic
                    // cardinality. Plan-only; never affects results.
                    if let Some(cte) = crate::engine::executor::cte::lookup_cte(name) {
                        if let Some(stats) = &cte.stats {
                            let sel = cte_scan_selectivity(stats, columns, predicate);
                            if sel < 1.0 {
                                let refined = (*estimated_rows * sel).max(1.0);
                                *estimated_rows = refined;
                                *cost = crate::engine::cbo::Cost::scalar(refined);
                            }
                        }
                    }
                }
            }
            return finish_plan(db, &cbo_ctx, select, scan);
        }
    }
    let mut tables = Vec::new();
    let mut source_refs = Vec::new();
    source_refs.push(base.clone());
    tables.push(plan_table(db, base)?);
    for join in &select.joins {
        source_refs.push(join.table.clone());
        tables.push(plan_table(db, &join.table)?);
    }
    if let Some(rewritten) = rewrite_unused_left_join_as_anti(select, &tables) {
        return build_physical_plan(db, &rewritten);
    }
    if let Some(plan) = try_plan_left_join_is_null(db, &cbo_ctx, select, &tables)? {
        return Ok(plan);
    }
    let mut base_predicates = Vec::new();
    let mut residual_predicates = Vec::new();
    let mut join_conditions = select
        .joins
        .iter()
        .map(|join| join.condition.clone())
        .collect::<Vec<_>>();
    let flexible_inner_joins = select
        .joins
        .iter()
        .all(|join| matches!(join.join_type, JoinType::Inner | JoinType::Cross));
    let mut where_predicates = Vec::new();
    if let Some(predicate) = &select.selection {
        split_conjuncts(predicate, &mut where_predicates);
    }
    if select.joins.is_empty() || flexible_inner_joins {
        let mut pushdown_pool = where_predicates.clone();
        for condition in join_conditions.iter().flatten() {
            split_conjuncts(condition, &mut pushdown_pool);
        }
        let or_derived = derive_or_pushdown_predicates(db, &pushdown_pool, &tables);
        pushdown_pool.extend(or_derived);
        for table in &tables {
            base_predicates.push(extract_table_predicate(db, &mut pushdown_pool, table));
        }
        if let Some(where_expr) = combine_conjuncts(where_predicates) {
            if let Some(remaining) = subtract_base_predicates(where_expr, &base_predicates) {
                split_conjuncts(&remaining, &mut residual_predicates);
            }
        }
        for condition in &mut join_conditions {
            *condition = condition
                .take()
                .and_then(|condition| subtract_base_predicates(condition, &base_predicates));
        }
    } else {
        // Outer joins are order- and null-extension-sensitive, so ON
        // predicate pushdown has to be join-type-aware. Still, ordinary
        // WHERE filters on tables that cannot be null-supplied by the
        // left-deep join tree are safe to apply at the base scan. This
        // matters for EXISTS/NOT EXISTS rewrites: a Semi/Anti join filters
        // the left input, and outer WHERE predicates such as date ranges
        // must not be stranded above the join.
        base_predicates.resize(tables.len(), None);
        let where_safe = where_pushdown_safe_tables(select, tables.len());
        let mut pushdown_pool = where_predicates.clone();
        let or_derived = derive_or_pushdown_predicates(db, &pushdown_pool, &tables);
        pushdown_pool.extend(or_derived);
        for (idx, table) in tables.iter().enumerate() {
            if !where_safe[idx] {
                continue;
            }
            if let Some(predicate) = extract_table_predicate(db, &mut pushdown_pool, table) {
                add_base_predicate(&mut base_predicates, idx, predicate);
            }
        }
        if let Some(where_expr) = combine_conjuncts(where_predicates) {
            if let Some(remaining) = subtract_base_predicates(where_expr, &base_predicates) {
                split_conjuncts(&remaining, &mut residual_predicates);
            }
        }
        push_join_on_predicates(
            db,
            select,
            &tables,
            &where_safe,
            &mut join_conditions,
            &mut base_predicates,
        );
    }

    // Order-preserving + LIMIT bonus only applies when there's a single
    // base table; multi-table joins (HashJoin / NLJ) generally do not
    // preserve a child scan's index order at the top of the plan tree,
    // so a per-scan order-aware bonus would be misleading.
    let base_index_hints = select
        .from
        .first()
        .map(|t| t.index_hints.as_slice())
        .unwrap_or(&[]);
    let base_table_name = select
        .from
        .first()
        .map(|t| t.alias.as_deref().unwrap_or(t.name.as_str()))
        .unwrap_or("");
    let fulltext_matches = collect_fulltext_matches_in_select(select);
    let aggregate_state_bytes_per_group = {
        let specs: Vec<(String, bool)> = collect_aggregate_calls(select)
            .iter()
            .filter_map(|expr| match expr {
                Expr::Function { name, distinct, .. } if is_aggregate_function(name) => {
                    Some((name.clone(), *distinct))
                }
                _ => None,
            })
            .collect();
        (!specs.is_empty())
            .then(|| crate::engine::cbo::operator_cost::aggregate_state_bytes_from_specs(&specs))
    };
    let single_table_hints = if tables.len() == 1 {
        // Text-protocol SELECTs auto-parameterize LIMIT/OFFSET before
        // planning. Read the currently bound values just like the later
        // TopK/TopWindow decision does; using `LimitValue::as_usize` here
        // made placeholders look unbounded and suppressed an otherwise
        // order-providing index scan. The same plan then fell through to a
        // full-table TopK/TopWindow (and deep pages could reserve O(table)
        // sort-key memory) even for `ORDER BY PRIMARY KEY LIMIT ...`.
        let limit_with_offset = ordered_limit_row_goal(select);
        let row_goal = plain_limit_row_goal(select);
        let required = collect_required_columns(select, &tables[0]);
        let has_wildcard = select.projection.iter().any(|item| {
            matches!(
                item,
                SelectItem::Wildcard | SelectItem::QualifiedWildcard(_)
            )
        });
        ScanHints {
            order_by: &select.order_by,
            group_by: &select.group_by,
            aggregate_state_bytes_per_group,
            limit_with_offset,
            row_goal,
            required_columns: if has_wildcard { None } else { Some(required) },
            index_hints: base_index_hints,
            query_hints: &select.query_hints,
            table_name: base_table_name,
            fulltext_matches: &fulltext_matches,
            join_predicates: &[],
        }
    } else {
        ScanHints::none()
    };
    let joined_required_columns = if tables.len() > 1 && !select_has_wildcard_projection(select) {
        collect_required_columns_by_table(select, &tables, &base_predicates)
    } else {
        Vec::new()
    };
    // Multi-table KNN: when the ORDER BY belongs to the leftmost table, keep
    // its vector scan as the join driver. The ordered NLJ path below preserves
    // that driver's order; hash/reordered joins are deliberately suppressed.
    let join_knn = if tables.len() > 1
        && !select
            .query_hints
            .iter()
            .any(|h| matches!(h, QueryHint::HashJoin { .. }))
    {
        try_knn_vector_scan(
            db,
            &tables[0],
            select,
            base_predicates[0].clone(),
            tables[0]
                .schema
                .indexes
                .iter()
                .map(|index| index.name.clone())
                .collect(),
        )
        .filter(|plan| matches!(plan, PhysicalPlan::VectorIndexScan { .. }))
    } else {
        None
    };
    let knn_join_driver = join_knn.is_some();
    let mut scans = if tables.len() == 1 {
        // KNN fast path: `ORDER BY DISTANCE(col, const) LIMIT k` over an HNSW
        // index drives a VectorIndexScan (ordered candidates from the graph,
        // re-ranked under MVCC), suppressing the downstream Sort. Falls through
        // to ordinary scan selection when the shape doesn't match.
        let knn = try_knn_vector_scan(
            db,
            &tables[0],
            select,
            base_predicates[0].clone(),
            tables[0]
                .schema
                .indexes
                .iter()
                .map(|index| index.name.clone())
                .collect(),
        );
        let scan = match knn {
            Some(plan) => plan,
            None => choose_scan_for_source(
                db,
                &source_refs[0],
                &tables[0],
                base_predicates[0].clone(),
                &single_table_hints,
            )?,
        };
        vec![scan]
    } else {
        tables
            .iter()
            .zip(source_refs.iter())
            .zip(base_predicates.iter())
            .enumerate()
            .map(|(idx, ((table, source_ref), predicate))| {
                if idx == 0 {
                    if let Some(plan) = join_knn.clone() {
                        return Ok(plan);
                    }
                }
                let (table_index_hints, tname) = if idx == 0 {
                    (base_index_hints, base_table_name)
                } else {
                    let hints = select
                        .joins
                        .get(idx - 1)
                        .map(|j| j.table.index_hints.as_slice())
                        .unwrap_or(&[]);
                    let name = select
                        .joins
                        .get(idx - 1)
                        .map(|j| j.table.alias.as_deref().unwrap_or(j.table.name.as_str()))
                        .unwrap_or("");
                    (hints, name)
                };
                let possible_join_predicates: Vec<Expr> = join_conditions
                    .iter()
                    .flatten()
                    .chain(residual_predicates.iter())
                    .cloned()
                    .collect();
                let per_hints = ScanHints {
                    order_by: &[],
                    group_by: &[],
                    aggregate_state_bytes_per_group: None,
                    limit_with_offset: None,
                    row_goal: None,
                    required_columns: joined_required_columns.get(idx).cloned().flatten(),
                    index_hints: table_index_hints,
                    query_hints: &select.query_hints,
                    table_name: tname,
                    fulltext_matches: &[],
                    join_predicates: &possible_join_predicates,
                };
                choose_scan_for_source(db, source_ref, table, predicate.clone(), &per_hints)
            })
            .collect::<Result<Vec<_>, _>>()?
    };

    if scans.len() == 1 {
        let mut plan = scans.remove(0);
        if let Some(predicate) = combine_conjuncts(residual_predicates) {
            plan = attach_filter(plan, predicate);
        }
        return finish_plan(db, &cbo_ctx, select, plan);
    }

    let has_ordered = knn_join_driver
        || select.straight_join
        || select
            .query_hints
            .iter()
            .any(|h| matches!(h, QueryHint::Ordered));
    let mut join_hints = select.query_hints.clone();
    if knn_join_driver {
        // Empty table list applies to this join edge; put it first so an
        // order-destroying hash join cannot override the KNN driver's contract.
        join_hints.insert(0, QueryHint::NlJoin { tables: Vec::new() });
    }

    let all_inner = select
        .joins
        .iter()
        .all(|join| matches!(join.join_type, JoinType::Inner | JoinType::Cross));
    if all_inner && select.straight_join && !knn_join_driver {
        // MySQL evaluates a full-key unique lookup as a const table before
        // the remaining join order, even under SELECT STRAIGHT_JOIN.
        scans.sort_by_key(|scan| !is_mysql_const_scan(scan));
    }
    let has_straight_edge = select.joins.iter().any(|join| join.straight);
    let mut plan = if all_inner && has_straight_edge && !has_ordered {
        let mut cross_preds = Vec::new();
        for cond in join_conditions.drain(..).flatten() {
            split_conjuncts(&cond, &mut cross_preds);
        }
        cross_preds.extend(drain_join_predicates(&mut residual_predicates, &tables));
        let (plan, leftover) =
            plan_straight_inner_joins(&cbo_ctx, scans, cross_preds, select, &join_hints);
        residual_predicates.extend(leftover);
        plan
    } else if all_inner && !has_ordered {
        let mut cross_preds: Vec<Expr> = Vec::new();
        for cond in join_conditions.drain(..) {
            if let Some(c) = cond {
                split_conjuncts(&c, &mut cross_preds);
            }
        }
        cross_preds.extend(drain_join_predicates(&mut residual_predicates, &tables));
        let graph =
            crate::engine::cbo::join_order::JoinGraph::new(scans.clone(), cross_preds.clone());
        if let Some((dp_plan, leftover)) = crate::engine::cbo::join_order::enumerate_inner_joins(
            &cbo_ctx,
            &graph,
            &select.query_hints,
        ) {
            for pred in leftover {
                residual_predicates.push(pred);
            }
            dp_plan
        } else {
            reorder_inner_join_scans(&mut scans, select);
            let mut head = scans.remove(0);
            let mut plan_tables = collect_physical_plan_tables(&head);
            let mut remaining_preds = cross_preds;
            // Connectivity-aware greedy (DP fell back here because n >
            // MAX_DP_TABLES). At each step join a *connected* scan, preferring
            // an equi-join edge over a cartesian product. With cost ordering on
            // (default) we pick the connected candidate whose join yields the
            // fewest rows, so a selective join (TPC-DS Q64's `item` filter onto
            // the 2.9M store_sales stream) lands early instead of after 14
            // neutral dimension joins. With it off we fall back to the first
            // connected scan; `scans` is size-sorted, so that is the smallest
            // connected one, and a disconnected component takes the next
            // remaining scan (an unavoidable cross product). A fixed size-sorted
            // order would instead let a table whose only join key is supplied by
            // a not-yet-joined relation (Q64: date_dim d2/d3 keyed on `customer`
            // columns) form a cross product, materialising |left|×|right| rows.
            let cost_order = greedy_cost_order_enabled();
            while !scans.is_empty() {
                let pick = greedy_pick_next_join(
                    &cbo_ctx,
                    &head,
                    &plan_tables,
                    &scans,
                    &remaining_preds,
                    cost_order,
                );
                let right = scans.remove(pick);
                let right_tables = collect_physical_plan_tables(&right);
                let mut join_parts = Vec::new();
                let mut still = Vec::new();
                for pred in remaining_preds {
                    if expr_can_resolve_in_table_set(&pred, &plan_tables, &right_tables)
                        && expr_columns_resolved_across(&pred, &plan_tables, &right_tables)
                    {
                        join_parts.push(pred);
                    } else {
                        still.push(pred);
                    }
                }
                remaining_preds = still;
                plan_tables.extend(right_tables);
                let cond = combine_conjuncts(join_parts);
                head = choose_join_with_hints(
                    &cbo_ctx,
                    head,
                    right,
                    JoinType::Inner,
                    cond,
                    &join_hints,
                );
            }
            for pred in remaining_preds {
                residual_predicates.push(pred);
            }
            head
        }
    } else if all_inner && has_ordered {
        let mut cross_preds: Vec<Expr> = Vec::new();
        for cond in join_conditions.drain(..) {
            if let Some(c) = cond {
                split_conjuncts(&c, &mut cross_preds);
            }
        }
        cross_preds.extend(drain_join_predicates(&mut residual_predicates, &tables));
        let mut head = scans.remove(0);
        let mut plan_tables = collect_physical_plan_tables(&head);
        let mut remaining_preds = cross_preds;
        for right in scans.into_iter() {
            let right_tables = collect_physical_plan_tables(&right);
            let mut join_parts = Vec::new();
            let mut still = Vec::new();
            for pred in remaining_preds {
                if expr_can_resolve_in_table_set(&pred, &plan_tables, &right_tables)
                    && expr_columns_resolved_across(&pred, &plan_tables, &right_tables)
                {
                    join_parts.push(pred);
                } else {
                    still.push(pred);
                }
            }
            remaining_preds = still;
            plan_tables.extend(right_tables);
            let cond = combine_conjuncts(join_parts);
            head =
                choose_join_with_hints(&cbo_ctx, head, right, JoinType::Inner, cond, &join_hints);
        }
        for pred in remaining_preds {
            residual_predicates.push(pred);
        }
        head
    } else if !has_ordered && !has_straight_edge {
        if let Some(plan) = try_plan_leading_inner_join_island(
            &cbo_ctx,
            select,
            &tables,
            scans.clone(),
            &mut join_conditions,
            &mut residual_predicates,
        ) {
            plan
        } else {
            reorder_inner_join_scans(&mut scans, select);
            let mut head = scans.remove(0);
            for (idx, right) in scans.into_iter().enumerate() {
                let join = &select.joins[idx];
                let right_tables = collect_physical_plan_tables(&right);
                let plan_tables = collect_physical_plan_tables(&head);
                let residual_condition = extract_residual_predicates_for_inner_join_edge(
                    join.join_type,
                    &mut residual_predicates,
                    &plan_tables,
                    &right_tables,
                );
                let condition =
                    combine_optional_exprs(join_conditions[idx].take(), residual_condition);
                let join_type = inner_join_type_for_extracted_condition(join.join_type, &condition);
                head = choose_join_with_hints(
                    &cbo_ctx,
                    head,
                    right,
                    join_type,
                    condition,
                    &join_hints,
                );
            }
            head
        }
    } else {
        let mut head = scans.remove(0);
        for (idx, right) in scans.into_iter().enumerate() {
            let join = &select.joins[idx];
            let right_tables = collect_physical_plan_tables(&right);
            let plan_tables = collect_physical_plan_tables(&head);
            let residual_condition = extract_residual_predicates_for_inner_join_edge(
                join.join_type,
                &mut residual_predicates,
                &plan_tables,
                &right_tables,
            );
            let condition = combine_optional_exprs(join_conditions[idx].take(), residual_condition);
            let join_type = inner_join_type_for_extracted_condition(join.join_type, &condition);
            head = choose_join_with_hints(&cbo_ctx, head, right, join_type, condition, &join_hints);
        }
        head
    };
    if let Some(predicate) = combine_conjuncts(residual_predicates) {
        plan = attach_filter(plan, predicate);
    }
    finish_plan(db, &cbo_ctx, select, plan)
}

/// Remove unused right payload from one LEFT/IS NULL edge surrounded by
/// INNER/CROSS edges. Upper right references retain the NULL-extension path.
fn rewrite_unused_left_join_as_anti(select: &Select, tables: &[PlanTable]) -> Option<Select> {
    if select.joins.len() < 2
        || select.for_update
        || select.lock_clause.is_some()
        || tables.len() != select.joins.len() + 1
        || tables
            .iter()
            .any(|table| table.table_id == 0 || table.system_time.is_some())
    {
        return None;
    }
    let edge = select
        .joins
        .iter()
        .position(|join| join.join_type == JoinType::Left)?;
    if select.joins[edge]
        .condition
        .as_ref()
        .is_some_and(|expr| sip_expr_has_subquery(expr) || expr_has_volatile_evaluation(expr))
    {
        return None;
    }
    // A later RIGHT/FULL edge could null-supply earlier matched rows anew.
    // Restrict this proof to one LEFT edge among INNER/CROSS joins.
    if select.joins.iter().enumerate().any(|(index, join)| {
        index != edge && !matches!(join.join_type, JoinType::Inner | JoinType::Cross)
    }) {
        return None;
    }
    let right = &tables[edge + 1];
    let right_column = |expr: &Expr| -> Option<usize> {
        let name = expr_column_name(expr)?;
        if !expr_refs_only_table(expr, right)
            || tables
                .iter()
                .enumerate()
                .any(|(index, table)| index != edge + 1 && expr_refs_only_table(expr, table))
        {
            return None;
        }
        right.schema.column_index(&normalize_expr_column(name)).ok()
    };
    let mut predicates = select
        .selection
        .as_ref()
        .map(expr_conjuncts)
        .unwrap_or_default()
        .into_iter()
        .cloned()
        .collect::<Vec<_>>();
    let witness = predicates.iter().position(|predicate| {
        let Expr::IsNull(expr) = predicate else {
            return false;
        };
        let Some(column) = right_column(expr) else {
            return false;
        };
        !right.schema.columns[column].nullable
            || select.joins[edge]
                .condition
                .as_ref()
                .is_some_and(|condition| {
                    expr_conjuncts(condition).iter().any(|part| {
                        matches!(part,
                Expr::Binary { left, op: BinaryOp::Eq, right: rhs }
                if right_column(left) == Some(column) || right_column(rhs) == Some(column))
                    })
                })
    })?;
    let mut upper = Vec::new();
    for item in &select.projection {
        match item {
            SelectItem::Wildcard => return None,
            SelectItem::QualifiedWildcard(qualifier)
                if matches_table(qualifier, &right.schema.name, right.alias.as_deref()) =>
            {
                return None
            }
            SelectItem::Expr { expr, .. } => upper.push(expr),
            _ => {}
        }
    }
    upper.extend(&select.group_by);
    upper.extend(select.having.iter());
    upper.extend(select.order_by.iter().map(|order| &order.expr));
    upper.extend(
        predicates
            .iter()
            .enumerate()
            .filter_map(|(index, expr)| (index != witness).then_some(expr)),
    );
    upper.extend(
        select.joins.iter().enumerate().filter_map(|(index, join)| {
            (index != edge).then_some(join.condition.as_ref()).flatten()
        }),
    );
    if upper.into_iter().any(|expr| {
        sip_expr_has_subquery(expr)
            || expr_has_volatile_evaluation(expr)
            || expr_touches_table_set(expr, std::slice::from_ref(right))
    }) {
        return None;
    }
    predicates.remove(witness);
    let mut rewritten = select.clone();
    rewritten.joins[edge].join_type = JoinType::Anti;
    rewritten.selection = combine_conjuncts(predicates);
    Some(rewritten)
}

/// A NULL test on a column that cannot be NULL in a matched right row
/// turns LEFT JOIN into an anti join. Keep NULL extension as a separate,
/// streaming operator so SELECT *, expressions and aggregates retain the
/// original schema without fetching the right-side payload.
fn try_plan_left_join_is_null(
    db: &Database,
    ctx: &crate::engine::cbo::CboContext,
    select: &Select,
    tables: &[PlanTable],
) -> Result<Option<PhysicalPlan>, QueryError> {
    // Restrict the proof to a single outer edge. Later outer joins can
    // null-supply an earlier edge again and require a different proof.
    if tables.len() != 2
        || select.joins.len() != 1
        || select.joins[0].join_type != JoinType::Left
        || select.for_update
        || select.lock_clause.is_some()
        || select.derived.is_some()
        // Synthetic/temporal sources need an output-schema nullability proof,
        // rather than the current catalog-column proof used here.
        || tables
            .iter()
            .any(|table| table.table_id == 0 || table.system_time.is_some())
    {
        return Ok(None);
    }
    let right = &tables[1];
    let mut predicates = select
        .selection
        .as_ref()
        .map(expr_conjuncts)
        .unwrap_or_default()
        .into_iter()
        .cloned()
        .collect::<Vec<_>>();
    let right_column = |expr: &Expr| -> Option<usize> {
        let name = expr_column_name(expr)?;
        if !expr_refs_only_table(expr, right) || expr_refs_only_table(expr, &tables[0]) {
            return None;
        }
        right.schema.column_index(&normalize_expr_column(name)).ok()
    };
    let witness = predicates.iter().position(|predicate| {
        let Expr::IsNull(expr) = predicate else {
            return false;
        };
        let Some(index) = right_column(expr) else {
            return false;
        };
        if !right.schema.columns[index].nullable {
            return true;
        }
        // Ordinary equality rejects NULL even when the catalog column is
        // nullable. Null-safe equality (<=>) must NOT supply this proof.
        select.joins[0].condition.as_ref().is_some_and(|condition| {
            expr_conjuncts(condition).iter().any(|conjunct| {
                matches!(conjunct, Expr::Binary { left, op: BinaryOp::Eq, right: rhs }
                    if right_column(left) == Some(index) || right_column(rhs) == Some(index))
            })
        })
    });
    let Some(witness) = witness else {
        return Ok(None);
    };
    predicates.remove(witness);
    let mut inner = select.clone();
    inner.joins[0].join_type = JoinType::Anti;
    // Finalize this scope independently: only ON/scan predicates may demand
    // right-side data. Upper references to that table are synthesized NULLs.
    let required = collect_required_columns_by_table(select, tables, &[]);
    let left_wildcard = select.projection.iter().any(|item| match item {
        SelectItem::Wildcard => true,
        SelectItem::QualifiedWildcard(qualifier) => matches_table(
            qualifier,
            &tables[0].schema.name,
            tables[0].alias.as_deref(),
        ),
        _ => false,
    });
    inner.projection = tables[0]
        .schema
        .iter_visible_columns()
        .filter(|(_, column)| {
            left_wildcard
                || required[0]
                    .as_ref()
                    .is_some_and(|needed| needed.contains(&normalize_expr_column(&column.name)))
        })
        .map(|(_, column)| SelectItem::Expr {
            expr: Expr::CompoundIdentifier(vec![
                tables[0]
                    .alias
                    .clone()
                    .unwrap_or_else(|| tables[0].schema.name.clone()),
                column.name.clone(),
            ]),
            alias: None,
        })
        .collect();
    inner.selection = extract_table_predicate(db, &mut predicates, &tables[0]);
    inner.group_by.clear();
    inner.grouping_sets.clear();
    inner.group_by_modifier = GroupByModifier::None;
    inner.having = None;
    inner.order_by.clear();
    inner.limit = None;
    inner.offset = None;
    inner.distinct = false;
    inner.sql_calc_found_rows = false;
    inner.named_windows.clear();
    let input = build_physical_plan(db, &inner)?;
    let mut plan = PhysicalPlan::NullExtend {
        estimated_rows: estimated_rows(&input),
        cost: plan_cost(&input),
        input: Box::new(input),
        table: right.clone(),
    };
    if let Some(predicate) = combine_conjuncts(predicates) {
        plan = PhysicalPlan::Filter {
            estimated_rows: (estimated_rows(&plan) * 0.25).max(1.0),
            cost: plan_cost(&plan),
            input: Box::new(plan),
            predicate,
        };
    }
    Ok(Some(finish_plan(db, ctx, select, plan)?))
}

fn try_plan_leading_inner_join_island(
    ctx: &crate::engine::cbo::CboContext,
    select: &Select,
    tables: &[PlanTable],
    scans: Vec<PhysicalPlan>,
    join_conditions: &mut [Option<Expr>],
    residual_predicates: &mut Vec<Expr>,
) -> Option<PhysicalPlan> {
    let leading_inner_joins = select
        .joins
        .iter()
        .take_while(|join| matches!(join.join_type, JoinType::Inner | JoinType::Cross))
        .count();
    let island_len = leading_inner_joins + 1;
    if island_len < 2 || island_len >= scans.len() {
        return None;
    }

    let island_tables = &tables[..island_len];
    let island_scans = scans[..island_len].to_vec();
    let mut cross_preds = Vec::new();
    for condition in join_conditions.iter_mut().take(leading_inner_joins) {
        if let Some(condition) = condition.take() {
            split_conjuncts(&condition, &mut cross_preds);
        }
    }
    cross_preds.extend(drain_join_predicates_for_tables(
        residual_predicates,
        island_tables,
    ));

    let mut plan = plan_inner_join_island(ctx, island_scans, cross_preds, select);
    for (idx, right) in scans.into_iter().enumerate().skip(island_len) {
        let join_idx = idx - 1;
        let join = &select.joins[join_idx];
        let right_tables = collect_physical_plan_tables(&right);
        let plan_tables = collect_physical_plan_tables(&plan);
        let residual_condition = extract_residual_predicates_for_inner_join_edge(
            join.join_type,
            residual_predicates,
            &plan_tables,
            &right_tables,
        );
        let condition =
            combine_optional_exprs(join_conditions[join_idx].take(), residual_condition);
        let join_type = inner_join_type_for_extracted_condition(join.join_type, &condition);
        plan = choose_join_with_hints(ctx, plan, right, join_type, condition, &select.query_hints);
    }
    Some(plan)
}

fn extract_residual_predicates_for_inner_join_edge(
    join_type: JoinType,
    residual_predicates: &mut Vec<Expr>,
    left_tables: &[PlanTable],
    right_tables: &[PlanTable],
) -> Option<Expr> {
    if !matches!(join_type, JoinType::Inner | JoinType::Cross) || residual_predicates.is_empty() {
        return None;
    }

    let mut join_parts = Vec::new();
    let mut remaining = Vec::new();
    for pred in residual_predicates.drain(..) {
        if expr_can_resolve_in_table_set(&pred, left_tables, right_tables)
            && expr_columns_resolved_across(&pred, left_tables, right_tables)
            && !expr_contains_or(&pred)
        {
            join_parts.push(pred);
        } else {
            remaining.push(pred);
        }
    }
    *residual_predicates = remaining;
    combine_conjuncts(join_parts)
}

fn expr_contains_or(expr: &Expr) -> bool {
    crate::parser::visit::any_expr_matching(
        expr,
        |node| {
            matches!(
                node,
                Expr::Binary {
                    op: BinaryOp::Or,
                    ..
                }
            )
        },
        crate::parser::visit::scalar_expr_children,
    )
}

fn inner_join_type_for_extracted_condition(
    join_type: JoinType,
    condition: &Option<Expr>,
) -> JoinType {
    if join_type == JoinType::Cross && condition.is_some() {
        JoinType::Inner
    } else {
        join_type
    }
}

fn plan_inner_join_island(
    ctx: &crate::engine::cbo::CboContext,
    mut scans: Vec<PhysicalPlan>,
    cross_preds: Vec<Expr>,
    select: &Select,
) -> PhysicalPlan {
    let graph = crate::engine::cbo::join_order::JoinGraph::new(scans.clone(), cross_preds.clone());
    if let Some((mut plan, leftover)) =
        crate::engine::cbo::join_order::enumerate_inner_joins(ctx, &graph, &select.query_hints)
    {
        if let Some(predicate) = combine_conjuncts(leftover) {
            plan = attach_filter(plan, predicate);
        }
        return plan;
    }

    let mut head = scans.remove(0);
    let mut plan_tables = collect_physical_plan_tables(&head);
    let mut remaining_preds = cross_preds;
    for right in scans {
        let right_tables = collect_physical_plan_tables(&right);
        let mut join_parts = Vec::new();
        let mut still = Vec::new();
        for pred in remaining_preds {
            if expr_can_resolve_in_table_set(&pred, &plan_tables, &right_tables)
                && expr_columns_resolved_across(&pred, &plan_tables, &right_tables)
            {
                join_parts.push(pred);
            } else {
                still.push(pred);
            }
        }
        remaining_preds = still;
        plan_tables.extend(right_tables);
        let cond = combine_conjuncts(join_parts);
        head = choose_join_with_hints(ctx, head, right, JoinType::Inner, cond, &select.query_hints);
    }
    if let Some(predicate) = combine_conjuncts(remaining_preds) {
        attach_filter(head, predicate)
    } else {
        head
    }
}

fn drain_join_predicates_for_tables(predicates: &mut Vec<Expr>, tables: &[PlanTable]) -> Vec<Expr> {
    let mut joinable = Vec::new();
    let mut remaining = Vec::new();
    for predicate in predicates.drain(..) {
        if predicate_can_join_any_pair(&predicate, tables)
            && expr_columns_resolved_across(&predicate, tables, &[])
        {
            joinable.push(predicate);
        } else {
            remaining.push(predicate);
        }
    }
    *predicates = remaining;
    joinable
}

fn subtract_base_predicates(condition: Expr, base_predicates: &[Option<Expr>]) -> Option<Expr> {
    let mut conjuncts = Vec::new();
    split_conjuncts(&condition, &mut conjuncts);
    let base = base_predicates
        .iter()
        .filter_map(|predicate| predicate.as_ref())
        .flat_map(expr_conjuncts)
        .collect::<Vec<_>>();
    if conjuncts.len().saturating_mul(base.len()) <= 256 {
        conjuncts.retain(|conjunct| !base.iter().any(|predicate| *conjunct == **predicate));
    } else {
        // Wide conjunctions would otherwise compare every pair. Equal
        // expressions have equal fingerprints, so buckets keep exactness.
        let fingerprint = crate::engine::expr_program::expr_fingerprint;
        let mut index: HashMap<u64, Vec<&Expr>> = HashMap::with_capacity(base.len());
        for predicate in &base {
            index
                .entry(fingerprint(predicate))
                .or_default()
                .push(predicate);
        }
        conjuncts.retain(|conjunct| {
            !index.get(&fingerprint(conjunct)).is_some_and(|candidates| {
                candidates.iter().any(|predicate| *conjunct == **predicate)
            })
        });
    }
    combine_conjuncts(conjuncts)
}

fn add_base_predicate(base_predicates: &mut [Option<Expr>], idx: usize, predicate: Expr) {
    base_predicates[idx] = combine_optional(base_predicates[idx].take(), predicate);
}

fn where_pushdown_safe_tables(select: &Select, table_count: usize) -> Vec<bool> {
    let mut safe = vec![false; table_count];
    if table_count == 0 {
        return safe;
    }
    safe[0] = true;
    for (join_idx, join) in select.joins.iter().enumerate() {
        let right_idx = join_idx + 1;
        if right_idx >= table_count {
            break;
        }
        match join.join_type {
            JoinType::Inner | JoinType::Cross => {
                safe[right_idx] = true;
            }
            JoinType::Left => {
                safe[right_idx] = false;
            }
            JoinType::Right => {
                for item in safe.iter_mut().take(right_idx) {
                    *item = false;
                }
                safe[right_idx] = true;
            }
            JoinType::Full => {
                // FULL preserves both sides: no predicate is safe to push
                // below the join to either side. (FULL is routed to the
                // in-memory join path, so this is defensive.)
                for item in safe.iter_mut().take(right_idx) {
                    *item = false;
                }
                safe[right_idx] = false;
            }
            JoinType::Semi | JoinType::Anti => {
                safe[right_idx] = false;
            }
        }
    }
    safe
}

fn push_join_on_predicates(
    db: &Database,
    select: &Select,
    tables: &[PlanTable],
    where_safe: &[bool],
    join_conditions: &mut [Option<Expr>],
    base_predicates: &mut [Option<Expr>],
) {
    for (join_idx, join) in select.joins.iter().enumerate() {
        let Some(condition) = join_conditions.get_mut(join_idx).and_then(Option::take) else {
            continue;
        };
        let mut conjuncts = Vec::new();
        split_conjuncts(&condition, &mut conjuncts);
        let safe_tables =
            join_on_pushdown_safe_tables(join.join_type, join_idx, tables.len(), where_safe);
        for (table_idx, table) in tables.iter().enumerate() {
            if !safe_tables[table_idx] {
                continue;
            }
            if let Some(predicate) = extract_table_predicate(db, &mut conjuncts, table) {
                add_base_predicate(base_predicates, table_idx, predicate);
            }
        }
        join_conditions[join_idx] = combine_conjuncts(conjuncts);
    }
}

fn join_on_pushdown_safe_tables(
    join_type: JoinType,
    join_idx: usize,
    table_count: usize,
    where_safe: &[bool],
) -> Vec<bool> {
    let mut safe = vec![false; table_count];
    let right_idx = join_idx + 1;
    if right_idx >= table_count {
        return safe;
    }
    match join_type {
        JoinType::Inner | JoinType::Cross => {
            for idx in 0..right_idx {
                safe[idx] = where_safe.get(idx).copied().unwrap_or(false);
            }
            safe[right_idx] = true;
        }
        JoinType::Left => {
            safe[right_idx] = true;
        }
        JoinType::Right => {
            for idx in 0..right_idx {
                safe[idx] = where_safe.get(idx).copied().unwrap_or(false);
            }
        }
        JoinType::Full => {
            // FULL preserves both sides — no ON predicate is safe to push
            // below the join (leave `safe` all-false). Defensive: FULL is
            // routed to the in-memory join path.
        }
        JoinType::Semi | JoinType::Anti => {
            // Semi/Anti joins are filters over the left input. Pushing
            // right-only ON predicates into the subquery table is safe
            // and keeps EXISTS rewrites selective; left-side ON predicates
            // stay attached to preserve Anti and prior outer-join cases.
            safe[right_idx] = true;
        }
    }
    safe
}

fn drain_join_predicates(predicates: &mut Vec<Expr>, tables: &[PlanTable]) -> Vec<Expr> {
    let mut joinable = Vec::new();
    let mut remaining = Vec::new();
    for predicate in predicates.drain(..) {
        if predicate_can_join_any_pair(&predicate, tables) {
            joinable.push(predicate);
        } else {
            remaining.push(predicate);
        }
    }
    *predicates = remaining;
    joinable
}

fn predicate_can_join_any_pair(predicate: &Expr, tables: &[PlanTable]) -> bool {
    for i in 0..tables.len() {
        for j in (i + 1)..tables.len() {
            if expr_can_resolve_in_table_set(
                predicate,
                std::slice::from_ref(&tables[i]),
                std::slice::from_ref(&tables[j]),
            ) {
                return true;
            }
        }
    }
    false
}

fn finish_plan(
    db: &Database,
    ctx: &crate::engine::cbo::CboContext,
    select: &Select,
    mut plan: PhysicalPlan,
) -> Result<PhysicalPlan, QueryError> {
    use crate::engine::cbo::operator_cost;
    // Walk the assembled plan tree once and flip `covering = true` on
    // every index-access node whose schema columns are a superset of
    // what the query needs. Done here (not inside choose_scan/choose_join)
    // so multi-table plans and INLJ inner sides are also considered.
    mark_covering_index_recursive(ctx, &mut plan, select);
    // Compute per-SeqScan column-pruning lists so wide unused columns
    // don't inflate hash-join build/probe materialisations and trigger
    // a Grace partition spill.
    apply_column_pruning(&mut plan, select);
    // S2: join-level output projection pruning — intermediates of multi-level joins carry only the
    // columns still referenced above, so leaf columns along a K-level chain are no longer cloned K times.
    apply_join_output_pruning(&mut plan, select);
    if select.group_by_modifier == GroupByModifier::None {
        if let Some(pushed) = try_covering_index_aggregate_pushdown(ctx, select, &plan) {
            plan = pushed;
            plan = apply_parallel_hints(plan, select);
            apply_output_column_pruning_pass(&mut plan, select);
            synchronize_and_validate_plan_costs(&mut plan)?;
            return Ok(plan);
        }
        if let Some(pushed) = try_filtered_min_max_index_aggregate_pushdown(ctx, select, &plan) {
            plan = pushed;
            synchronize_and_validate_plan_costs(&mut plan)?;
            return Ok(plan);
        }
        if let Some(pushed) = try_index_aggregate_pushdown(ctx, select, &plan) {
            plan = pushed;
            synchronize_and_validate_plan_costs(&mut plan)?;
            return Ok(plan);
        }
    }
    let having_has_aggregate = select.having.as_ref().is_some_and(has_aggregate_expr);
    let query_block_is_aggregate = !select.group_by.is_empty()
        || !select.grouping_sets.is_empty()
        || has_aggregate_select_items(&select.projection)
        || having_has_aggregate;
    if query_block_is_aggregate {
        let input_rows = estimated_rows(&plan);
        let base_group_count = if select.group_by.is_empty() {
            1.0
        } else {
            estimate_group_by_rows_for_plan(&plan, &select.group_by, input_rows)
        };
        let has_grouping_sets = !select.grouping_sets.is_empty();
        let rollup_all = has_grouping_sets
            && select.group_by_modifier == GroupByModifier::Rollup
            && grouping_sets_match_rollup_all(select.group_by.len(), &select.grouping_sets);
        let grouping_sets_prefix_chain =
            has_grouping_sets && grouping_sets_are_prefix_chain(&select.grouping_sets);
        // ENUM ordinal zero and a legitimately declared empty member render
        // as the same string but are distinct GROUP BY keys. A lexical sort
        // cannot make those key classes contiguous (zero/member/zero may
        // interleave), so Sort+StreamingAggregate is not a valid physical
        // implementation for that schema. Hash aggregation uses the stored
        // identity and remains correct.
        let enum_empty_group_aliasing = group_by_has_enum_empty_member(&plan, &select.group_by);
        let group_count = if has_grouping_sets {
            estimate_grouping_sets_rows_for_plan(
                &plan,
                &select.group_by,
                &select.grouping_sets,
                input_rows,
            )
        } else {
            base_group_count
        };
        // HashAggregate must size and cost its state from every distinct
        // group, but downstream joins consume only rows that survive HAVING.
        // Keeping both cardinalities equal made any grouped subquery with a
        // selective HAVING look as large as its full group domain and could
        // turn a tiny indexed join into a second fact-table scan. Use a stable
        // SQL-shape estimate here; it affects only downstream cardinality,
        // never the aggregate's memory/spill estimate or physical topology.
        let output_group_count = estimate_having_output_rows(select.having.as_ref(), group_count);
        let width = plan_output_width(&plan);
        let aggregates = collect_aggregate_calls_scoped(db, select);
        let duplicate_aware_hash = duplicate_aware_count_topk_candidate(
            &plan,
            select,
            &aggregates,
            input_rows,
            group_count,
        );
        let streaming = !select.group_by.is_empty()
            && !enum_empty_group_aliasing
            && input_satisfies_grouping_order(&plan, &select.group_by)
            && (!has_grouping_sets || grouping_sets_prefix_chain);
        // P7: refine HashAggregate's per-group state estimate by walking
        // the aggregate slot list. The pre-P7 formula `output_width * 1.5
        // + 64` priced a `COUNT(*)` plan the same as a
        // `GROUP_CONCAT(name SEPARATOR ', ')` plan, which made the CBO
        // happy to spill enormous DISTINCT / concat tables that would
        // crush the budget at runtime.
        let state_per_group = compact_key_only_group_state_bytes(
            &plan,
            &select.group_by,
            &aggregates,
        )
        .or_else(|| {
            aggregate_state_bytes_per_group_for_plan(&plan, &aggregates, input_rows, group_count)
        });
        // Streaming when the input already provides the grouping order.
        // Otherwise: cost Hash vs Sort+Streaming and pick the cheaper.
        // The CBO already accounts for spill in `hash_aggregate_with_state`,
        // so a spill-bound plan can lose to the sort-merge alternative.
        let in_cost = plan_cost(&plan);
        let cost_streaming =
            operator_cost::streaming_aggregate(&in_cost, input_rows, group_count, width);
        let cost_hash = operator_cost::hash_aggregate_with_state(
            ctx,
            &in_cost,
            input_rows,
            group_count,
            width,
            state_per_group,
        );
        let sort_pre_cost = operator_cost::sort(ctx, &in_cost, input_rows, width);
        let cost_sort_stream =
            operator_cost::streaming_aggregate(&sort_pre_cost, input_rows, group_count, width);
        #[derive(Copy, Clone)]
        enum AggForce {
            None,
            Hash,
            SortStream,
            Streaming,
        }
        // This is an executor/testing escape hatch, so read it per planning
        // operation. A process-wide OnceLock made a transient setting leak
        // into every later statement and prevented differential tests from
        // forcing both physical implementations.
        let force = match crate::util::env::var("MYDB_AGG_STRATEGY").ok().as_deref() {
            Some("hash") => AggForce::Hash,
            Some("sort_stream") => AggForce::SortStream,
            Some("streaming") => AggForce::Streaming,
            _ => AggForce::None,
        };
        let force_hash = matches!(force, AggForce::Hash);
        let force_sort_stream = matches!(force, AggForce::SortStream);
        let force_streaming = matches!(force, AggForce::Streaming);
        let aggregate_has_unbounded_state = aggregates.iter().any(|aggregate| match aggregate {
            Expr::Function { name, distinct, .. } => {
                *distinct
                    || matches!(
                        name.to_ascii_uppercase().as_str(),
                        "GROUP_CONCAT" | "JSON_ARRAYAGG" | "JSON_OBJECTAGG"
                    )
            }
            _ => false,
        });
        // Hash and Sort+Streaming are costed for every ordinary input,
        // including joins. Join→Aggregate match-index delivery is represented
        // by the physical pipeline cost rather than by suppressing a CBO
        // candidate based on query shape.
        let sort_stream_cost_comparable = !has_grouping_sets
            && !enum_empty_group_aliasing
            // Sorting only removes the hash table for GROUP BY keys. A
            // DISTINCT/collection accumulator still retains unbounded state
            // for the current group, so pricing Sort+StreamingAggregate as
            // O(1) memory can select a full-table external sort without
            // eliminating the pressure it was meant to avoid.
            && !aggregate_has_unbounded_state
            // Sort+StreamingAggregate otherwise evaluates a complex grouping
            // expression once to build sort keys and again to detect group
            // transitions. Apart from wasted work, warning-producing and
            // stored-function expressions would observably execute twice.
            // Direct columns are reusable without expression side effects;
            // complex keys use the hash aggregate's single-evaluation path.
            && select.group_by.iter().all(|expr| {
                expr_column_name(match expr {
                    Expr::GroupKeyRef { expr, .. } => expr,
                    other => other,
                })
                .is_some()
            });
        let use_sort_stream = if streaming || select.group_by.is_empty() {
            false
        } else if force_hash {
            false
        } else if force_sort_stream {
            true
        } else if duplicate_aware_hash {
            // The native Top-K bridge can retain compact fingerprints and only
            // aggregate duplicate candidates for a near-unique integer key.
            // Preserve HashAggregate here so the later parallel pass also
            // keeps the scan/worker topology; a costed Sort+Streaming choice
            // would be peeled by that bridge anyway, but would strand the
            // accelerator on a serial scan.
            false
        } else if !sort_stream_cost_comparable {
            false
        } else {
            cost_sort_stream.cmp_total(&cost_hash).is_lt()
        };
        crate::engine::cbo::trace::event(
            "aggregate_candidates",
            serde_json::json!({
                "hash_total": cost_hash.total,
                "streaming_total": cost_streaming.total,
                "sort_stream_total": cost_sort_stream.total,
                "input_already_ordered": streaming,
                "sort_stream_cost_comparable": sort_stream_cost_comparable,
                "aggregate_has_unbounded_state": aggregate_has_unbounded_state,
                "forced": match force {
                    AggForce::None => "none",
                    AggForce::Hash => "hash",
                    AggForce::SortStream => "sort_stream",
                    AggForce::Streaming => "streaming",
                },
                "selected": if streaming || force_streaming || use_sort_stream {
                    "streaming"
                } else {
                    "hash"
                },
            }),
        );
        let fixed_scale_sum_oracle = scan_plan_table(&plan)
            .is_some_and(|table| aggregate_has_fixed_scale_approx_sum(&aggregates, &table.schema));
        let chosen_streaming =
            !fixed_scale_sum_oracle && (streaming || force_streaming || use_sort_stream);
        let (cost, agg_input_plan) = if streaming {
            (cost_streaming, plan)
        } else if chosen_streaming && use_sort_stream {
            // Wrap input in a Sort over GROUP BY columns; the
            // StreamingAggregate that follows reads grouped rows
            // sequentially with O(1) memory.
            let grouping_order: Vec<OrderBy> = select
                .group_by
                .iter()
                .map(|expr| OrderBy {
                    expr: expr.clone(),
                    asc: true,
                    nulls_first: None,
                })
                .collect();
            let sorted = PhysicalPlan::Sort {
                input: Box::new(plan),
                order_by: grouping_order,
                estimated_rows: input_rows,
                cost: sort_pre_cost,
            };
            (cost_sort_stream, sorted)
        } else if chosen_streaming {
            // force_streaming with no need-to-sort: trust the user. Use the
            // streaming cost; runtime may still error if input order is
            // wrong, but `MYDB_AGG_STRATEGY=streaming` is an opt-in escape.
            (cost_streaming, plan)
        } else {
            (cost_hash, plan)
        };
        plan = if has_grouping_sets && !rollup_all && chosen_streaming {
            PhysicalPlan::StreamingGroupingSetsAggregate {
                input: Box::new(agg_input_plan),
                group_by: select.group_by.clone(),
                grouping_sets: select.grouping_sets.clone(),
                having: select.having.clone(),
                aggregates: aggregates.clone(),
                estimated_rows: output_group_count,
                cost,
            }
        } else if has_grouping_sets && !rollup_all {
            PhysicalPlan::HashGroupingSetsAggregate {
                input: Box::new(agg_input_plan),
                group_by: select.group_by.clone(),
                grouping_sets: select.grouping_sets.clone(),
                having: select.having.clone(),
                aggregates,
                estimated_rows: output_group_count,
                cost,
            }
        } else if chosen_streaming && rollup_all {
            PhysicalPlan::StreamingRollupAggregate {
                input: Box::new(agg_input_plan),
                group_by: select.group_by.clone(),
                having: select.having.clone(),
                aggregates: aggregates.clone(),
                estimated_rows: output_group_count,
                cost,
            }
        } else if chosen_streaming {
            PhysicalPlan::StreamingAggregate {
                input: Box::new(agg_input_plan),
                group_by: select.group_by.clone(),
                having: select.having.clone(),
                aggregates: aggregates.clone(),
                estimated_rows: output_group_count,
                cost,
            }
        } else if rollup_all {
            PhysicalPlan::HashRollupAggregate {
                input: Box::new(agg_input_plan),
                group_by: select.group_by.clone(),
                having: select.having.clone(),
                aggregates,
                estimated_rows: output_group_count,
                cost,
            }
        } else {
            PhysicalPlan::HashAggregate {
                input: Box::new(agg_input_plan),
                group_by: select.group_by.clone(),
                having: select.having.clone(),
                aggregates,
                estimated_rows: output_group_count,
                cost,
            }
        };
    }
    if !query_block_is_aggregate {
        if let Some(having) = &select.having {
            // Alias resolution has already rewritten projection aliases to
            // their defining expressions, so evaluating this against the
            // joined input preserves MySQL's row-wise HAVING semantics.
            plan = attach_filter(plan, having.clone());
        }
    }
    let mut limit_consumed_by_order_window = false;
    // An order-providing scan normally needs no Sort/TopK wrapper. DISTINCT
    // is different: the bounded operator is also the deduplicator. When every
    // ORDER BY expression is projected, an ordered input may stop after the
    // first K distinct tuples because no later tuple can sort ahead of the
    // retained boundary. Keep this explicit node so execution can apply that
    // proof without teaching scans about SELECT semantics.
    if select.distinct
        && distinct_topk_is_safe(select)
        && !select.order_by.is_empty()
        && plan_satisfies_order_by(&plan, &select.order_by)
    {
        if let Some(limit_count) = limit_usize(select.limit.as_ref()) {
            let offset = limit_usize(select.offset.as_ref()).unwrap_or(0);
            let k = offset.saturating_add(limit_count);
            if k <= topk_max_heap_from_env() {
                let rows = estimated_rows(&plan);
                let width = plan_output_width(&plan);
                let cost = operator_cost::top_k(&plan_cost(&plan), rows, k as f64, width);
                plan = PhysicalPlan::TopK {
                    input: Box::new(plan),
                    order_by: select.order_by.clone(),
                    dedup_projection: Some(select.projection.clone()),
                    k,
                    offset,
                    binding: limit_binding_of(select),
                    estimated_rows: (k as f64).min(rows),
                    cost,
                };
            }
        }
    }
    if !select.order_by.is_empty() && !plan_satisfies_order_by(&plan, &select.order_by) {
        let rows = estimated_rows(&plan);
        let width = plan_output_width(&plan);
        // Phase B: when ORDER BY is paired with a concrete LIMIT and
        // there's no DISTINCT in the way, choose between a small TopK
        // heap and a deep-page TopWindow quickselect. TopWindow emits
        // exactly the requested page, so the later Limit node is skipped.
        let limit_val = limit_usize(select.limit.as_ref());
        let offset_val = limit_usize(select.offset.as_ref()).unwrap_or(0);
        let topk_max = topk_max_heap_from_env();
        let mut ordered_limit_planned = false;
        let bounded_distinct = select.distinct && distinct_topk_is_safe(select);
        if let Some(limit_count) = limit_val.filter(|_| !select.distinct || bounded_distinct) {
            let k = offset_val.saturating_add(limit_count);
            let input_cost = plan_cost(&plan);
            let topk_cost = operator_cost::top_k(&input_cost, rows, k as f64, width);
            let window_cost = operator_cost::top_window(
                &input_cost,
                rows,
                limit_count as f64,
                offset_val as f64,
                width,
                select.order_by.len(),
            );
            let topk_allowed = k <= topk_max;
            let want_top_window = !bounded_distinct
                && offset_val > 0
                && (!topk_allowed || window_cost.weighted_total() < topk_cost.weighted_total());
            if want_top_window {
                let window_rows = if offset_val as f64 >= rows {
                    0.0
                } else {
                    (limit_count as f64).min(rows - offset_val as f64)
                };
                plan = PhysicalPlan::TopWindow {
                    input: Box::new(plan),
                    order_by: select.order_by.clone(),
                    limit: limit_count,
                    offset: offset_val,
                    binding: limit_binding_of(select),
                    estimated_rows: window_rows,
                    cost: window_cost,
                };
                limit_consumed_by_order_window = true;
                ordered_limit_planned = true;
            } else if topk_allowed {
                let topk_rows = (k as f64).min(rows);
                plan = PhysicalPlan::TopK {
                    input: Box::new(plan),
                    order_by: select.order_by.clone(),
                    dedup_projection: bounded_distinct.then(|| select.projection.clone()),
                    k,
                    offset: offset_val,
                    binding: limit_binding_of(select),
                    estimated_rows: topk_rows,
                    cost: topk_cost,
                };
                ordered_limit_planned = true;
            }
        }
        if !ordered_limit_planned {
            let presorted_prefix = provided_order_prefix_len(&plan, &select.order_by);
            if presorted_prefix > 0 && presorted_prefix < select.order_by.len() {
                let cost = operator_cost::incremental_sort(
                    ctx,
                    &plan_cost(&plan),
                    rows,
                    width,
                    presorted_prefix,
                    select.order_by.len(),
                );
                plan = PhysicalPlan::IncrementalSort {
                    input: Box::new(plan),
                    order_by: select.order_by.clone(),
                    presorted_prefix,
                    estimated_rows: rows,
                    cost,
                };
            } else {
                let sort_cost = operator_cost::sort(ctx, &plan_cost(&plan), rows, width);
                plan = PhysicalPlan::Sort {
                    input: Box::new(plan),
                    order_by: select.order_by.clone(),
                    estimated_rows: rows,
                    cost: sort_cost,
                };
            }
        }
    }
    let estimated = estimated_rows(&plan);
    let offset_rows = limit_usize(select.offset.as_ref()).unwrap_or(0) as f64;
    let rows = if limit_consumed_by_order_window {
        estimated
    } else {
        match limit_usize(select.limit.as_ref()) {
            Some(limit) => {
                let end = (offset_rows + limit as f64).min(estimated);
                (end - offset_rows.min(estimated)).max(0.0)
            }
            None => (estimated - offset_rows.min(estimated)).max(0.0),
        }
    };
    let proj_width = plan_output_width(&plan);
    let input_rows = estimated_rows(&plan);
    let distinct_consumed_by_topk = matches!(
        &plan,
        PhysicalPlan::TopK {
            dedup_projection: Some(_),
            ..
        }
    );
    let proj_cost = if select.distinct && !distinct_consumed_by_topk {
        if input_satisfies_distinct_order(&plan, &select.projection) {
            operator_cost::streaming_distinct(&plan_cost(&plan), input_rows, rows, proj_width)
        } else {
            operator_cost::hash_distinct(ctx, &plan_cost(&plan), input_rows, rows, proj_width)
        }
    } else {
        operator_cost::projection(&plan_cost(&plan), rows, proj_width)
    };
    plan = if select.distinct
        && !distinct_consumed_by_topk
        && input_satisfies_distinct_order(&plan, &select.projection)
    {
        PhysicalPlan::StreamingDistinct {
            input: Box::new(plan),
            projection: select.projection.clone(),
            estimated_rows: rows,
            cost: proj_cost,
        }
    } else {
        PhysicalPlan::Projection {
            input: Box::new(plan),
            projection: select.projection.clone(),
            distinct: select.distinct && !distinct_consumed_by_topk,
            estimated_rows: rows,
            cost: proj_cost,
        }
    };
    if (select.limit.is_some() || select.offset.is_some()) && !limit_consumed_by_order_window {
        let limit_rows = limit_usize(select.limit.as_ref()).unwrap_or(0) as f64;
        let offset_rows = limit_usize(select.offset.as_ref()).unwrap_or(0) as f64;
        let cost = operator_cost::limit(&plan_cost(&plan), rows, limit_rows.max(rows), offset_rows);
        plan = PhysicalPlan::Limit {
            input: Box::new(plan),
            limit: limit_usize(select.limit.as_ref()),
            offset: limit_usize(select.offset.as_ref()),
            binding: limit_binding_of(select),
            estimated_rows: rows,
            cost,
        };
    }
    plan = apply_parallel_hints(plan, select);
    // Run output-column pruning AFTER the parallel rewrite: the rewrite
    // converts `SeqScan` into `Exchange { ParallelSeqScan }`, and the
    // streaming hash-join probe (the consumer that benefits) only
    // recognises the parallel flavour. Doing this here also means the
    // pass sees the FINAL plan shape (with any post-finalize wrappers
    // like Sort/Limit/Projection) so we collect references from
    // everything the plan actually does.
    apply_output_column_pruning_pass(&mut plan, select);
    synchronize_and_validate_plan_costs(&mut plan)?;
    Ok(plan)
}

/// Stable planning-time eligibility for the exact duplicate-aware COUNT Top-K
/// executor. This is deliberately the same conservative envelope as its
/// runtime preflight: direct fixed-width integer keys, high analyzed NDV,
/// one descending COUNT(*) ordering key and a bounded literal/parameter K.
fn duplicate_aware_count_topk_candidate(
    input: &PhysicalPlan,
    select: &Select,
    aggregates: &[Expr],
    input_rows: f64,
    group_rows: f64,
) -> bool {
    const MIN_INPUT_ROWS: f64 = 65_536.0;
    const MIN_DISTINCT_RATIO: f64 = 0.875;
    const MAX_K: usize = 4_096;
    if input_rows < MIN_INPUT_ROWS
        || !input_rows.is_finite()
        || !group_rows.is_finite()
        || group_rows / input_rows < MIN_DISTINCT_RATIO
        || select.having.is_some()
        || select.order_by.len() != 1
        || select.order_by[0].asc
        || limit_usize(select.limit.as_ref()).is_none_or(|limit| limit == 0 || limit > MAX_K)
    {
        return false;
    }
    let order_key =
        crate::engine::executor::select::aggregate::aggregate_key(&select.order_by[0].expr);
    if !aggregates.iter().any(|aggregate| {
        crate::engine::executor::select::aggregate::aggregate_key(aggregate) == order_key
            && matches!(
                aggregate,
                Expr::Function { name, args, distinct: false }
                    if name.eq_ignore_ascii_case("COUNT")
                        && matches!(args.as_slice(), [Expr::Wildcard])
            )
    }) {
        return false;
    }
    let Some(table) = scan_plan_table(input) else {
        return false;
    };
    !select.group_by.is_empty()
        && select.group_by.iter().all(|expr| {
            let Some(column) = expr_column_name(expr) else {
                return false;
            };
            table
                .schema
                .column_index(column)
                .ok()
                .and_then(|index| table.schema.columns.get(index))
                .is_some_and(|column| {
                    matches!(
                        column.data_type.column_type(),
                        crate::types::ColumnType::Tiny
                            | crate::types::ColumnType::Short
                            | crate::types::ColumnType::MediumInt
                            | crate::types::ColumnType::Long
                            | crate::types::ColumnType::LongLong
                    )
                })
        })
}

fn aggregate_literal_comparison(expr: &Expr) -> bool {
    let Expr::Binary { left, op, right } = expr else {
        return false;
    };
    if !matches!(
        op,
        BinaryOp::Eq
            | BinaryOp::NotEq
            | BinaryOp::Gt
            | BinaryOp::GtEq
            | BinaryOp::Lt
            | BinaryOp::LtEq
    ) {
        return false;
    }
    (has_aggregate_expr(left) && expr_literal_value(right).is_some())
        || (has_aggregate_expr(right) && expr_literal_value(left).is_some())
}

fn count_star_predicate_keeps_every_group(expr: &Expr) -> bool {
    let Expr::Binary { left, op, right } = expr else {
        return false;
    };
    let (op, bound) = if is_count_star_expr(left) {
        (*op, right.as_ref())
    } else if is_count_star_expr(right) {
        (flip_cmp(*op), left.as_ref())
    } else {
        return false;
    };
    let Some(bound) = integer_literal_i128(bound) else {
        return false;
    };
    match op {
        BinaryOp::Gt => bound < 1,
        BinaryOp::GtEq => bound <= 1,
        BinaryOp::NotEq => bound == 0,
        _ => false,
    }
}

fn is_count_star_expr(expr: &Expr) -> bool {
    matches!(
        expr,
        Expr::Function {
            name,
            args,
            distinct: false,
        } if name.eq_ignore_ascii_case("COUNT") && matches!(args.as_slice(), [Expr::Wildcard])
    )
}

fn integer_literal_i128(expr: &Expr) -> Option<i128> {
    match expr {
        Expr::Literal(crate::parser::Literal::Int(value)) => Some(i128::from(*value)),
        Expr::Literal(crate::parser::Literal::UInt(value)) => Some(i128::from(*value)),
        _ => None,
    }
}

fn group_by_has_enum_empty_member(plan: &PhysicalPlan, group_by: &[Expr]) -> bool {
    let tables = collect_physical_plan_tables(plan);
    tables.iter().any(|table| {
        table.schema.columns.iter().any(|column| {
            matches!(
                &column.data_type,
                DataTypeKind::Enum { values } if values.iter().any(String::is_empty)
            ) && group_by
                .iter()
                .any(|expr| expr_matches_grouping_table_column(expr, table, &column.name, &tables))
        })
    })
}

/// Phase 1+2: wire `/*+ PARALLEL(n) */` / `/*+ NO_PARALLEL */` hints
/// AND CBO auto-parallelism into the physical plan.
///
/// Decision order:
///   1. `NO_PARALLEL` hint or `MYDB_PARALLEL_DISABLE=1` env → strictly serial.
///   2. `PARALLEL(n)` hint → wrap reachable scans in `Exchange { Parallel... }`
///      unless an ancestor relies on the scan's natural index order.
///   3. Otherwise: CBO auto-decides. For each reachable `SeqScan`, compare
///      the serial cost against the parallel cost (defined as half of the
///      serial cost). Wrap if the parallel cost is cheaper by at least
///      10 % AND the row count exceeds `MYDB_PARALLEL_THRESHOLD_ROWS`
///      AND the table has at least `MYDB_PARALLEL_MIN_PAGES` heap pages.
///
/// CTE scans and already-parallel nodes are not considered here — Phase 5
/// (joins) handles its own parallel plan node.
fn apply_parallel_hints(plan: PhysicalPlan, select: &Select) -> PhysicalPlan {
    let mut requested_degree: Option<u32> = None;
    let mut suppressed = false;
    for hint in &select.query_hints {
        match hint {
            crate::parser::QueryHint::NoParallel => suppressed = true,
            crate::parser::QueryHint::Parallel { degree } => {
                requested_degree = Some(*degree);
            }
            _ => {}
        }
    }
    if suppressed {
        return plan;
    }
    if parallel_planner_config().disabled {
        if requested_degree.is_some() {
            push_hint_warning("PARALLEL ignored: MYDB_PARALLEL_DISABLE is set");
        }
        return plan;
    }
    let mut plan = plan;
    let output_order_sensitive = !select.order_by.is_empty();
    if let Some(degree_hint) = requested_degree {
        let degree = if degree_hint == 0 {
            default_parallel_degree()
        } else {
            degree_hint
        };
        if degree > 1 {
            parallelize_seq_scans(
                &mut plan,
                degree,
                ParallelMode::Forced,
                output_order_sensitive,
            );
            parallelize_hash_aggregate(&mut plan, degree, ParallelMode::Forced);
            parallelize_sort(&mut plan, degree, ParallelMode::Forced);
            if !plan_has_parallel_node(&plan) {
                push_hint_warning("PARALLEL hint did not apply: no eligible scans or aggregates");
            }
        } else {
            push_hint_warning("PARALLEL hint did not apply: effective degree is 1");
        }
    } else {
        if plain_limit_row_goal(select).is_some() {
            return plan;
        }
        // Auto-parallel compares generic resource-model candidates at the
        // configured degree. Query-shape worker floors are intentionally not
        // used: setup, gather, bandwidth and memory terms decide instead.
        let degree = default_parallel_degree();
        if degree > 1 {
            parallelize_seq_scans(
                &mut plan,
                degree,
                ParallelMode::CostBased,
                output_order_sensitive,
            );
            parallelize_hash_aggregate(&mut plan, degree, ParallelMode::CostBased);
            parallelize_sort(&mut plan, degree, ParallelMode::CostBased);
        }
    }
    plan
}

/// Phase 4: replace `Sort { input }` with `ParallelSort { input,
/// degree }` when the input is already parallel-eligible AND
/// MYDB_PARALLEL_SORT_DISABLE is unset.
fn parallelize_sort(plan: &mut PhysicalPlan, degree: u32, mode: ParallelMode) {
    if parallel_planner_config().sort_disabled {
        return;
    }
    parallelize_sort_inner(plan, degree, mode);
}

fn parallelize_sort_inner(plan: &mut PhysicalPlan, degree: u32, mode: ParallelMode) {
    match plan {
        PhysicalPlan::Sort {
            input,
            order_by,
            estimated_rows,
            cost,
        } => {
            // CBO mode: only flip when input is already parallel.
            if mode == ParallelMode::CostBased && !plan_contains_parallel(input) {
                return;
            }
            let new_cost = parallel_cost_from_serial(*cost, degree);
            let new_rows = *estimated_rows;
            let placeholder = PhysicalPlan::CteScan {
                name: String::new(),
                alias: None,
                columns: Vec::new(),
                filter: None,
                estimated_rows: 0.0,
                cost: crate::engine::cbo::Cost::ZERO,
                projected_columns: None,
            };
            let real_input = std::mem::replace(input.as_mut(), placeholder);
            let order_by_owned = std::mem::take(order_by);
            *plan = PhysicalPlan::ParallelSort {
                input: Box::new(real_input),
                order_by: order_by_owned,
                degree,
                estimated_rows: new_rows,
                cost: new_cost,
            };
        }
        // TopK → ParallelTopK: same gating as Sort → ParallelSort.
        // Each worker now does O((N/P) log k) per chunk instead of
        // O((N/P) log (N/P)) for a parallel sort, and per-worker memory
        // is bounded by k. The leader merges P × k rows.
        PhysicalPlan::TopK {
            input,
            order_by,
            dedup_projection,
            k,
            offset,
            binding,
            estimated_rows,
            cost,
        } => {
            if mode == ParallelMode::CostBased && !plan_contains_parallel(input) {
                return;
            }
            let new_cost = parallel_cost_from_serial(*cost, degree);
            let new_rows = *estimated_rows;
            let k_owned = *k;
            let offset_owned = *offset;
            let binding_owned = *binding;
            let placeholder = PhysicalPlan::CteScan {
                name: String::new(),
                alias: None,
                columns: Vec::new(),
                filter: None,
                estimated_rows: 0.0,
                cost: crate::engine::cbo::Cost::ZERO,
                projected_columns: None,
            };
            let real_input = std::mem::replace(input.as_mut(), placeholder);
            let order_by_owned = std::mem::take(order_by);
            let dedup_projection_owned = std::mem::take(dedup_projection);
            *plan = PhysicalPlan::ParallelTopK {
                input: Box::new(real_input),
                order_by: order_by_owned,
                dedup_projection: dedup_projection_owned,
                k: k_owned,
                offset: offset_owned,
                binding: binding_owned,
                degree,
                estimated_rows: new_rows,
                cost: new_cost,
            };
        }
        // Recurse through wrappers to find a deeper Sort/TopK.
        PhysicalPlan::Limit { input, .. }
        | PhysicalPlan::Projection { input, .. }
        | PhysicalPlan::WindowAgg { input, .. }
        | PhysicalPlan::StreamingDistinct { input, .. }
        | PhysicalPlan::HashAggregate { input, .. }
        | PhysicalPlan::ParallelHashAggregate { input, .. } => {
            parallelize_sort_inner(input, degree, mode);
        }
        _ => {}
    }
}

/// Phase 3: replace `HashAggregate { input }` with
/// `ParallelHashAggregate { input, degree }` when the input is itself
/// already parallel-eligible (typically a `Exchange { ParallelSeqScan }`
/// produced by the previous pass) AND at least one MYDB_PARALLEL_AGG
/// gate accepts. Disable knob: `MYDB_PARALLEL_AGGREGATE_DISABLE`.
///
/// We only consider the top-level `HashAggregate` (the planner places
/// it just below the final Projection / Limit). Aggregates nested
/// inside subqueries / joins are left to Phase 5.
fn parallelize_hash_aggregate(plan: &mut PhysicalPlan, degree: u32, mode: ParallelMode) {
    if parallel_planner_config().aggregate_disabled {
        return;
    }
    parallelize_hash_aggregate_inner(plan, degree, mode);
}

/// Apply the normal aggregate admission policy to fused physical plans.
pub(crate) fn parallelize_fused_aggregate(plan: &mut PhysicalPlan) {
    if !parallel_planner_config().disabled {
        let degree = default_parallel_degree();
        if degree > 1 {
            parallelize_hash_aggregate(plan, degree, ParallelMode::CostBased);
        }
    }
}

fn parallelize_hash_aggregate_inner(plan: &mut PhysicalPlan, degree: u32, mode: ParallelMode) {
    match plan {
        PhysicalPlan::HashAggregate {
            input,
            group_by,
            having,
            aggregates,
            estimated_rows,
            cost,
        } => {
            // An implicit single group is still decomposable: workers build
            // local accumulator states and the coordinator merges the one
            // bucket. This is especially important for deterministic
            // computed inputs such as collation keys, whose CPU cost would
            // otherwise serialize above an already-parallel scan.
            // A serial producer does not require serial reduction. Keep its
            // cost undivided and compare only the aggregate-local work against
            // worker setup before admitting a decomposable reducer.
            let serial_producer = !plan_contains_parallel(input);
            let mut candidate_cost = parallel_cost_from_serial(*cost, degree);
            if serial_producer {
                let producer_cost = plan_cost(input);
                let local_work = (cost.total - producer_cost.total).max(0.0);
                let setup =
                    degree as f64 * crate::engine::cbo::consts::consts().parallel_setup_cost();
                candidate_cost.total = producer_cost.total + local_work / degree as f64 + setup;
                candidate_cost.startup = (candidate_cost.total
                    - (cost.total - cost.startup).max(0.0) / degree as f64)
                    .max(0.0);
            }
            if mode == ParallelMode::CostBased && serial_producer {
                // The producer can remain serial while deterministic computed
                // group/aggregate inputs run inside the hash-aggregate worker
                // loop.  Admit that CPU-parallel shape once its stable row
                // estimate clears the same floor used by scan parallelism;
                // Plain reducers also qualify when their local CPU/spill work
                // amortizes worker setup without discounting the producer.
                let has_computed_worker_input = group_by
                    .iter()
                    .any(|expr| !matches!(expr, Expr::Identifier(_) | Expr::CompoundIdentifier(_)))
                    || aggregates.iter().any(|aggregate| match aggregate {
                        Expr::Function { args, .. } => args.iter().any(|arg| {
                            !matches!(
                                arg,
                                Expr::Identifier(_) | Expr::CompoundIdentifier(_) | Expr::Wildcard
                            )
                        }),
                        _ => false,
                    });
                let mergeable = !aggregates.is_empty() && aggregates.iter().all(|expr| {
                    matches!(expr, Expr::Function { name, .. } if matches!(name.to_ascii_uppercase().as_str(),
                        "SUM" | "AVG" | "COUNT" | "MIN" | "MAX" | "STDDEV_POP" | "STDDEV_SAMP"
                        | "STDDEV" | "STD" | "VAR_POP" | "VAR_SAMP" | "VARIANCE" | "BIT_AND" | "BIT_OR" | "BIT_XOR"))
                });
                if (!has_computed_worker_input
                    && (!mergeable || candidate_cost.total >= cost.total * 0.9))
                    || self::estimated_rows(input) < parallel_planner_config().threshold_rows as f64
                {
                    return;
                }
            }
            // Build the parallel node, replacing in place.
            let new_cost = candidate_cost;
            let new_rows = *estimated_rows;
            // Move out of the borrow scope by taking ownership via a
            // placeholder swap.
            let placeholder = PhysicalPlan::CteScan {
                name: String::new(),
                alias: None,
                columns: Vec::new(),
                filter: None,
                estimated_rows: 0.0,
                cost: crate::engine::cbo::Cost::ZERO,
                projected_columns: None,
            };
            let real_input = std::mem::replace(input.as_mut(), placeholder);
            let group_by_owned = std::mem::take(group_by);
            let having_owned = having.take();
            let aggregates_owned = std::mem::take(aggregates);
            *plan = PhysicalPlan::ParallelHashAggregate {
                input: Box::new(real_input),
                group_by: group_by_owned,
                having: having_owned,
                aggregates: aggregates_owned,
                degree,
                estimated_rows: new_rows,
                cost: new_cost,
            };
        }
        PhysicalPlan::HashRollupAggregate {
            input,
            group_by,
            having,
            aggregates,
            estimated_rows,
            cost,
        } => {
            if group_by.is_empty() {
                return;
            }
            if mode == ParallelMode::CostBased && !plan_contains_parallel(input) {
                return;
            }
            let new_cost = parallel_cost_from_serial(*cost, degree);
            let new_rows = *estimated_rows;
            let placeholder = PhysicalPlan::CteScan {
                name: String::new(),
                alias: None,
                columns: Vec::new(),
                filter: None,
                estimated_rows: 0.0,
                cost: crate::engine::cbo::Cost::ZERO,
                projected_columns: None,
            };
            let real_input = std::mem::replace(input.as_mut(), placeholder);
            let group_by_owned = std::mem::take(group_by);
            let having_owned = having.take();
            let aggregates_owned = std::mem::take(aggregates);
            *plan = PhysicalPlan::ParallelRollupAggregate {
                input: Box::new(real_input),
                group_by: group_by_owned,
                having: having_owned,
                aggregates: aggregates_owned,
                degree,
                estimated_rows: new_rows,
                cost: new_cost,
            };
        }
        PhysicalPlan::HashGroupingSetsAggregate {
            input,
            group_by,
            grouping_sets,
            having,
            aggregates,
            estimated_rows,
            cost,
        } => {
            if group_by.is_empty() {
                return;
            }
            if mode == ParallelMode::CostBased && !plan_contains_parallel(input) {
                return;
            }
            let new_cost = parallel_cost_from_serial(*cost, degree);
            let new_rows = *estimated_rows;
            let placeholder = PhysicalPlan::CteScan {
                name: String::new(),
                alias: None,
                columns: Vec::new(),
                filter: None,
                estimated_rows: 0.0,
                cost: crate::engine::cbo::Cost::ZERO,
                projected_columns: None,
            };
            let real_input = std::mem::replace(input.as_mut(), placeholder);
            let group_by_owned = std::mem::take(group_by);
            let grouping_sets_owned = std::mem::take(grouping_sets);
            let having_owned = having.take();
            let aggregates_owned = std::mem::take(aggregates);
            *plan = PhysicalPlan::ParallelGroupingSetsAggregate {
                input: Box::new(real_input),
                group_by: group_by_owned,
                grouping_sets: grouping_sets_owned,
                having: having_owned,
                aggregates: aggregates_owned,
                degree,
                estimated_rows: new_rows,
                cost: new_cost,
            };
        }
        PhysicalPlan::Sort { input, .. }
        | PhysicalPlan::TopK { input, .. }
        | PhysicalPlan::TopWindow { input, .. }
        | PhysicalPlan::ParallelTopK { input, .. }
        | PhysicalPlan::Limit { input, .. }
        | PhysicalPlan::Projection { input, .. }
        | PhysicalPlan::WindowAgg { input, .. }
        | PhysicalPlan::StreamingDistinct { input, .. } => {
            parallelize_hash_aggregate_inner(input, degree, mode);
        }
        _ => {}
    }
}

pub(crate) fn plan_contains_parallel(plan: &PhysicalPlan) -> bool {
    match plan {
        PhysicalPlan::Exchange { .. }
        | PhysicalPlan::ParallelSeqScan { .. }
        | PhysicalPlan::ParallelIndexFastScan { .. }
        | PhysicalPlan::ParallelIndexRangeScan { .. }
        | PhysicalPlan::ParallelIndexAggregate { .. }
        | PhysicalPlan::ParallelRollupAggregate { .. }
        | PhysicalPlan::ParallelGroupingSetsAggregate { .. } => true,
        // Large non-covering IN-subquery lookups are opened as a native
        // page-partitioned heap Batch source.  Treat that deterministic
        // physical capability like the explicit parallel scan variants so
        // an adjacent aggregate/sort can choose its parallel node before any
        // rows are read.  Residual predicates and partitioned heaps retain
        // the semantic-complete serial lane for now.
        PhysicalPlan::IndexInLookup {
            table,
            filter,
            estimated_rows,
            covering,
            ..
        } => {
            !*covering
                && filter.is_none()
                && !table.schema.is_partitioned()
                && *estimated_rows >= 4096.0
                && default_parallel_degree() > 1
        }
        PhysicalPlan::Sort { input, .. }
        | PhysicalPlan::TopK { input, .. }
        | PhysicalPlan::TopWindow { input, .. }
        | PhysicalPlan::ParallelTopK { input, .. }
        | PhysicalPlan::Limit { input, .. }
        | PhysicalPlan::Projection { input, .. }
        | PhysicalPlan::WindowAgg { input, .. }
        | PhysicalPlan::StreamingDistinct { input, .. }
        | PhysicalPlan::StreamingAggregate { input, .. }
        | PhysicalPlan::StreamingRollupAggregate { input, .. }
        | PhysicalPlan::StreamingGroupingSetsAggregate { input, .. }
        | PhysicalPlan::HashAggregate { input, .. }
        | PhysicalPlan::HashRollupAggregate { input, .. }
        | PhysicalPlan::HashGroupingSetsAggregate { input, .. }
        | PhysicalPlan::ParallelHashAggregate { input, .. } => plan_contains_parallel(input),
        PhysicalPlan::NullExtend { input, .. } | PhysicalPlan::Filter { input, .. } => {
            plan_contains_parallel(input)
        }
        PhysicalPlan::Apply { outer, .. } => plan_contains_parallel(outer),
        PhysicalPlan::HashJoin { left, right, .. }
        | PhysicalPlan::NestedLoopJoin { left, right, .. }
        | PhysicalPlan::AdaptiveJoin { left, right, .. }
        | PhysicalPlan::SetOp { left, right, .. } => {
            plan_contains_parallel(left) || plan_contains_parallel(right)
        }
        PhysicalPlan::IndexNestedLoopJoin { left, .. } => plan_contains_parallel(left),
        _ => false,
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ParallelMode {
    /// User-supplied PARALLEL(n) hint — always wrap (subject to a tiny
    /// minimum-page floor so single-page tables stay serial).
    Forced,
    /// No hint; only wrap when CBO believes parallel is cheaper.
    CostBased,
}

#[derive(Debug, Clone, Copy)]
struct ParallelPlannerConfig {
    disabled: bool,
    degree: u32,
    use_all_cores: bool,
    cpu_bound_workers: Option<u32>,
    sort_disabled: bool,
    aggregate_disabled: bool,
    threshold_rows: u64,
    min_pages: u64,
}

fn planner_env_flag(name: &str) -> bool {
    crate::util::env::flag(name, false)
}

fn load_parallel_planner_config() -> ParallelPlannerConfig {
    ParallelPlannerConfig {
        disabled: planner_env_flag("MYDB_PARALLEL_DISABLE"),
        degree: crate::util::env::var("MYDB_PARALLEL_DEGREE")
            .ok()
            .and_then(|value| value.parse::<u32>().ok())
            .filter(|degree| *degree >= 1)
            // Four workers is the balanced default for the pipelined scan +
            // blocking-operator topology: on common 8-10 core hosts it leaves
            // capacity for both stages without the oversubscription observed
            // at five or more workers. Small plans still stay serial through
            // the cost/row/page admission gates below.
            .unwrap_or(4),
        use_all_cores: planner_env_flag("MYDB_PARALLEL_USE_ALL_CORES"),
        cpu_bound_workers: crate::util::env::var("MYDB_PARALLEL_CPU_BOUND_WORKERS")
            .ok()
            .and_then(|value| value.parse::<u32>().ok())
            .filter(|workers| *workers >= 1),
        sort_disabled: planner_env_flag("MYDB_PARALLEL_SORT_DISABLE"),
        aggregate_disabled: planner_env_flag("MYDB_PARALLEL_AGGREGATE_DISABLE"),
        threshold_rows: crate::util::env::var("MYDB_PARALLEL_THRESHOLD_ROWS")
            .ok()
            .and_then(|value| value.parse::<u64>().ok())
            .unwrap_or(DEFAULT_PARALLEL_THRESHOLD_ROWS),
        min_pages: crate::util::env::var("MYDB_PARALLEL_MIN_PAGES")
            .ok()
            .and_then(|value| value.parse::<u64>().ok())
            .unwrap_or(64),
    }
}

/// These knobs are immutable startup configuration in a server process.
/// Cache them so concurrent cache-disabled planners do not serialize on the
/// C-library environment lock. Tests retain dynamic reads for scoped config
/// overrides that exercise multiple settings inside one process.
fn parallel_planner_config() -> ParallelPlannerConfig {
    #[cfg(not(test))]
    {
        static CONFIG: OnceLock<ParallelPlannerConfig> = OnceLock::new();
        *CONFIG.get_or_init(load_parallel_planner_config)
    }
    #[cfg(test)]
    {
        load_parallel_planner_config()
    }
}

thread_local! {
    /// Non-zero while the current statement must run serially (e.g. it calls
    /// `nextval`/`setval`, which mutate per-session + durable state and can
    /// only be evaluated on the connection thread). Nesting-safe.
    static FORCE_SERIAL_DEPTH: std::cell::Cell<u32> = const { std::cell::Cell::new(0) };
}

/// RAII scope that forces serial execution while alive.
pub(crate) struct ForceSerialGuard;

impl Drop for ForceSerialGuard {
    fn drop(&mut self) {
        FORCE_SERIAL_DEPTH.with(|c| c.set(c.get().saturating_sub(1)));
    }
}

/// Force serial execution for the lifetime of the returned guard.
pub(crate) fn force_serial_scope() -> ForceSerialGuard {
    FORCE_SERIAL_DEPTH.with(|c| c.set(c.get() + 1));
    ForceSerialGuard
}

pub(crate) fn connection_thread_execution_required() -> bool {
    FORCE_SERIAL_DEPTH.with(|c| c.get() > 0)
}

fn parallelism_force_disabled() -> bool {
    connection_thread_execution_required()
}

/// Cheap check for whether a SQL statement references a sequence function, so
/// the executor can force it serial (sequence functions need the connection
/// thread's evaluation context). Over-triggering (e.g. a column literally
/// named `nextval`) only disables parallelism for that query — never wrong.
pub(crate) fn sql_mentions_sequence_fn(sql: &str) -> bool {
    // Every sequence function name ends in "val": scan for that suffix and
    // check the preceding keyword in place. This runs for every executed
    // statement, so it must not lowercase a copy of the SQL text.
    let bytes = sql.as_bytes();
    let mut from = 0;
    while let Some(pos) = ascii_find_ignore_case(&bytes[from..], b"val") {
        let at = from + pos;
        let head = &bytes[..at];
        if [b"next".as_slice(), b"set", b"curr", b"last"]
            .iter()
            .any(|prefix| ascii_ends_with_ignore_case(head, prefix))
        {
            return true;
        }
        from = at + 1;
    }
    false
}

/// LOAD_FILE performs server-side I/O and consults the live session security
/// context. Conservative text detection is sufficient: a false positive only
/// disables parallelism for one statement.
pub(crate) fn sql_mentions_load_file_fn(sql: &str) -> bool {
    ascii_find_ignore_case(sql.as_bytes(), b"load_file").is_some()
}

/// Position of the first ASCII case-insensitive occurrence of a lowercase
/// `needle` in `haystack`, without allocating a lowered copy.
fn ascii_find_ignore_case(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    let (&first, rest) = needle.split_first()?;
    let upper = first.to_ascii_uppercase();
    let last_start = haystack.len().checked_sub(needle.len())?;
    let mut start = 0;
    while start <= last_start {
        let pos = start + memchr::memchr2(first, upper, &haystack[start..=last_start])?;
        if haystack[pos + 1..pos + needle.len()].eq_ignore_ascii_case(rest) {
            return Some(pos);
        }
        start = pos + 1;
    }
    None
}

fn ascii_ends_with_ignore_case(haystack: &[u8], suffix: &[u8]) -> bool {
    haystack.len() >= suffix.len()
        && haystack[haystack.len() - suffix.len()..].eq_ignore_ascii_case(suffix)
}

/// True when `sql` contains a `:=` user-variable assignment. Such statements
/// write session state as a side effect of expression evaluation and must run
/// on the serial connection thread (the write reaches the session through the
/// stored-function context pointer, which parallel workers never install).
/// Cheap conservative substring test — a `:=` inside a string literal only
/// causes a harmless forced-serial run (issue #86).
pub(crate) fn sql_mentions_user_var_assign(sql: &str) -> bool {
    sql.contains(":=")
}

/// Single source of truth for query-execution parallelism. Every
/// query-side parallel operator (Exchange, parallel hash-agg / sort,
/// INLJ outer / batch parallelism, hash-join grace parallelism, the
/// vectorized scheduler's worker pool) takes its default from this
/// function so changing `MYDB_PARALLEL_DEGREE` predictably scales
/// the whole query path together — and so the planner's cost-model
/// parallel discount matches what the executor actually runs.
///
/// We intentionally do NOT fall back to `available_parallelism()`:
/// using all cores by default makes wall time depend on the host's
/// core count and easily over-subscribes shared machines / CI
/// runners. IO-side subsystems (buffer-pool shards, recovery
/// workers, startup IO threads) still scale with the host because
/// they're not part of the cost-modelled query plan.
///
/// Returns 1 while a [`force_serial_scope`] guard is active (sequence
/// functions must evaluate on the connection thread).
pub(crate) fn default_parallel_degree() -> u32 {
    if parallelism_force_disabled() {
        return 1;
    }
    parallel_planner_config().degree
}

#[cfg(test)]
mod sequence_force_serial_tests {
    use super::*;

    #[test]
    fn force_serial_scope_pins_degree_to_one() {
        assert!(!parallelism_force_disabled());
        {
            let _g = force_serial_scope();
            assert!(parallelism_force_disabled());
            assert_eq!(default_parallel_degree(), 1);
            {
                // Nesting-safe.
                let _g2 = force_serial_scope();
                assert_eq!(default_parallel_degree(), 1);
            }
            assert!(parallelism_force_disabled());
        }
        assert!(!parallelism_force_disabled());
    }

    #[test]
    fn detects_sequence_functions_in_sql() {
        assert!(sql_mentions_sequence_fn("SELECT nextval('s')"));
        assert!(sql_mentions_sequence_fn("select SETVAL('s',1)"));
        assert!(sql_mentions_sequence_fn("SELECT CurrVal('s')"));
        assert!(sql_mentions_sequence_fn("SELECT lastval()"));
        assert!(!sql_mentions_sequence_fn("SELECT a, b FROM t WHERE c = 1"));
        assert!(!sql_mentions_sequence_fn("SELECT interval_val, val FROM t"));
        assert!(sql_mentions_sequence_fn("NEXTVAL"));
        assert!(!sql_mentions_sequence_fn("VAL"));
        assert!(!sql_mentions_sequence_fn(""));
    }

    #[test]
    fn detects_load_file_in_sql_case_insensitively() {
        assert!(sql_mentions_load_file_fn("SELECT LOAD_FILE('/x')"));
        assert!(sql_mentions_load_file_fn("select load_File('/x')"));
        assert!(!sql_mentions_load_file_fn("SELECT load_fil, file FROM t"));
        assert!(!sql_mentions_load_file_fn("load_fil"));
    }

    #[test]
    fn ascii_function_search_matches_case_insensitive_byte_oracle() {
        for needle in [b"val".as_slice(), b"load_file"] {
            for mask in 0..(1 << needle.len()) {
                let spelling: Vec<_> = needle
                    .iter()
                    .enumerate()
                    .map(|(i, byte)| {
                        if mask & (1 << i) == 0 {
                            *byte
                        } else {
                            byte.to_ascii_uppercase()
                        }
                    })
                    .collect();
                for offset in [0, 1, 15, 16, 31, 32, 63, 64, 127] {
                    let mut bytes = vec![b'x'; offset];
                    bytes.extend_from_slice(&spelling);
                    bytes.extend_from_slice(" 字🙂 ".as_bytes());
                    for end in offset..=bytes.len() {
                        let haystack = &bytes[..end];
                        let expected = haystack
                            .windows(needle.len())
                            .position(|candidate| candidate.eq_ignore_ascii_case(needle));
                        assert_eq!(ascii_find_ignore_case(haystack, needle), expected);
                    }
                }
            }
        }
        assert_eq!(ascii_find_ignore_case(b"vVvVaL", b"val"), Some(3));
        assert_eq!(ascii_find_ignore_case(b"", b"val"), None);
    }
}

fn cpu_bound_parallel_degree(degree: u32) -> u32 {
    let config = parallel_planner_config();
    if config.use_all_cores {
        return degree.max(1);
    }
    let cap = config.cpu_bound_workers.or_else(performance_core_count);
    cap.map(|cap| degree.min(cap).max(1))
        .unwrap_or_else(|| degree.max(1))
}

fn performance_core_count() -> Option<u32> {
    static CORES: OnceLock<Option<u32>> = OnceLock::new();
    *CORES.get_or_init(detect_performance_core_count)
}

#[cfg(target_os = "macos")]
fn detect_performance_core_count() -> Option<u32> {
    let name = std::ffi::CString::new("hw.perflevel0.physicalcpu").ok()?;
    let mut value: libc::c_int = 0;
    let mut len = std::mem::size_of::<libc::c_int>();
    let rc = unsafe {
        libc::sysctlbyname(
            name.as_ptr(),
            &mut value as *mut _ as *mut libc::c_void,
            &mut len,
            std::ptr::null_mut(),
            0,
        )
    };
    if rc == 0 && value > 0 {
        Some(value as u32)
    } else {
        None
    }
}

#[cfg(not(target_os = "macos"))]
fn detect_performance_core_count() -> Option<u32> {
    None
}

fn parallelize_seq_scans(
    plan: &mut PhysicalPlan,
    degree: u32,
    mode: ParallelMode,
    order_sensitive: bool,
) {
    let fast_scan = matches!(plan, PhysicalPlan::IndexFastScan { .. });
    match plan {
        PhysicalPlan::SeqScan {
            table,
            filter,
            estimated_rows,
            access_rows,
            cost,
            possible_keys,
            runtime_filters,
            hint_note,
            projected_columns,
        } => {
            if order_sensitive {
                return;
            }
            // Cost-based mode: do nothing unless we project a real win.
            if mode == ParallelMode::CostBased
                && !cbo_wants_parallel_for_seqscan(
                    *estimated_rows,
                    *access_rows,
                    &table.schema,
                    *cost,
                    degree,
                )
            {
                return;
            }
            // Forced mode: skip if the table is provably empty per
            // catalog stats. Workers would all spin up to find no
            // pages, which is mostly wasted overhead.
            if mode == ParallelMode::Forced && table.schema.stats.row_count == 0 {
                // We still allow it through — INSERTed rows may not
                // yet be reflected in stats. The runtime
                // `effective_degree_for_pages` cap is the real safety
                // net.
            }
            // Keep parallel costing deliberately simple: any parallel
            // version of a serial operator is estimated as half of the
            // equivalent non-parallel cost, independent of worker count.
            let degree = cpu_bound_parallel_degree(degree);
            if degree <= 1 {
                return;
            }
            let parallel_cost = parallel_cost_from_serial(*cost, degree);
            let parallel = PhysicalPlan::ParallelSeqScan {
                table: table.clone(),
                filter: filter.clone(),
                estimated_rows: *estimated_rows,
                access_rows: *access_rows,
                cost: parallel_cost,
                possible_keys: possible_keys.clone(),
                runtime_filters: runtime_filters.clone(),
                hint_note: hint_note.clone(),
                projected_columns: projected_columns.clone(),
                // Populated by a later column-pruning post-pass once
                // the planner can see which columns are referenced
                // outside the scan's filter.
                output_columns_in_projection: None,
                degree,
            };
            let est = *estimated_rows;
            let exchange_cost = parallel_cost;
            *plan = PhysicalPlan::Exchange {
                input: Box::new(parallel),
                degree,
                output_order: ExchangeOrder::Unordered,
                merge_keys: Vec::new(),
                estimated_rows: est,
                cost: exchange_cost,
            };
        }
        PhysicalPlan::ZoneMapScan {
            table,
            filter,
            estimated_rows,
            access_rows,
            cost,
            possible_keys,
            runtime_filters,
            hint_note,
            projected_columns,
            zone_map,
        } => {
            if order_sensitive {
                return;
            }
            if mode == ParallelMode::CostBased
                && !cbo_wants_parallel_for_seqscan(
                    *estimated_rows,
                    *access_rows,
                    &table.schema,
                    *cost,
                    degree,
                )
            {
                return;
            }
            let degree = cpu_bound_parallel_degree(degree);
            if degree <= 1 {
                return;
            }
            let parallel_cost = parallel_cost_from_serial(*cost, degree);
            let parallel = PhysicalPlan::ParallelZoneMapScan {
                table: table.clone(),
                filter: filter.clone(),
                estimated_rows: *estimated_rows,
                access_rows: *access_rows,
                cost: parallel_cost,
                possible_keys: possible_keys.clone(),
                runtime_filters: runtime_filters.clone(),
                hint_note: hint_note.clone(),
                projected_columns: projected_columns.clone(),
                output_columns_in_projection: None,
                degree,
                zone_map: *zone_map,
            };
            let est = *estimated_rows;
            *plan = PhysicalPlan::Exchange {
                input: Box::new(parallel),
                degree,
                output_order: ExchangeOrder::Unordered,
                merge_keys: Vec::new(),
                estimated_rows: est,
                cost: parallel_cost,
            };
        }
        PhysicalPlan::IndexFastScan {
            table,
            index,
            low,
            high,
            filter,
            estimated_rows,
            access_rows,
            cost,
            possible_keys,
            reverse,
            hint_note,
            projected_columns,
        }
        | PhysicalPlan::IndexRangeScan {
            table,
            index,
            low,
            high,
            filter,
            estimated_rows,
            access_rows,
            cost,
            possible_keys,
            covering: _,
            reverse,
            hint_note,
            projected_columns,
        } => {
            if order_sensitive {
                return;
            }
            if *reverse {
                return;
            }
            // Worker threads do not inherit partition pruning; generic parameter
            // bounds have unreliable cardinality and can make tiny probes parallel.
            if table.schema.is_partitioned() {
                return;
            }
            let bound_has_param = low
                .as_ref()
                .map(|b| b.key.contains_param())
                .unwrap_or(false)
                || high
                    .as_ref()
                    .map(|b| b.key.contains_param())
                    .unwrap_or(false);
            if bound_has_param {
                return;
            }
            let degree = cpu_bound_parallel_degree(degree);
            if degree <= 1 {
                return;
            }
            let wants_parallel = match mode {
                ParallelMode::CostBased if fast_scan => cbo_wants_parallel_for_index_fast_scan(
                    *estimated_rows,
                    *access_rows,
                    *cost,
                    degree,
                ),
                ParallelMode::CostBased => cbo_wants_parallel_for_index_range_scan(
                    *estimated_rows,
                    *access_rows,
                    *cost,
                    degree,
                ),
                _ => true,
            };
            if !wants_parallel {
                return;
            }
            let parallel_cost = parallel_cost_from_serial(*cost, degree);
            let parallel = if fast_scan {
                PhysicalPlan::ParallelIndexFastScan {
                    table: table.clone(),
                    index: index.clone(),
                    low: low.clone(),
                    high: high.clone(),
                    filter: filter.clone(),
                    estimated_rows: *estimated_rows,
                    access_rows: *access_rows,
                    cost: parallel_cost,
                    possible_keys: possible_keys.clone(),
                    hint_note: hint_note.clone(),
                    projected_columns: projected_columns.clone(),
                    degree,
                }
            } else {
                PhysicalPlan::ParallelIndexRangeScan {
                    table: table.clone(),
                    index: index.clone(),
                    low: low.clone(),
                    high: high.clone(),
                    filter: filter.clone(),
                    estimated_rows: *estimated_rows,
                    access_rows: *access_rows,
                    cost: parallel_cost,
                    possible_keys: possible_keys.clone(),
                    hint_note: hint_note.clone(),
                    projected_columns: projected_columns.clone(),
                    degree,
                }
            };
            let est = *estimated_rows;
            *plan = PhysicalPlan::Exchange {
                input: Box::new(parallel),
                degree,
                output_order: ExchangeOrder::Unordered,
                merge_keys: Vec::new(),
                estimated_rows: est,
                cost: parallel_cost,
            };
        }
        PhysicalPlan::CoveringIndexAggregate {
            table,
            index,
            low,
            high,
            group_by,
            having,
            aggregates,
            estimated_rows,
            access_rows,
            cost,
        } => {
            if order_sensitive {
                return;
            }
            let degree = cpu_bound_parallel_degree(degree);
            if degree <= 1 {
                return;
            }
            if mode == ParallelMode::CostBased
                && !cbo_wants_parallel_for_index_fast_scan(
                    *estimated_rows,
                    *access_rows,
                    *cost,
                    degree,
                )
            {
                return;
            }
            let parallel_cost = parallel_cost_from_serial(*cost, degree);
            *plan = PhysicalPlan::ParallelIndexAggregate {
                table: table.clone(),
                index: index.clone(),
                low: low.clone(),
                high: high.clone(),
                group_by: group_by.clone(),
                having: having.clone(),
                aggregates: aggregates.clone(),
                estimated_rows: *estimated_rows,
                access_rows: *access_rows,
                cost: parallel_cost,
                degree,
            };
        }
        PhysicalPlan::Sort { input, .. }
        | PhysicalPlan::TopK { input, .. }
        | PhysicalPlan::ParallelTopK { input, .. } => {
            parallelize_seq_scans(input, degree, mode, false);
        }
        PhysicalPlan::HashAggregate { input, .. }
        | PhysicalPlan::HashRollupAggregate { input, .. }
        | PhysicalPlan::HashGroupingSetsAggregate { input, .. }
        | PhysicalPlan::ParallelRollupAggregate { input, .. } => {
            parallelize_seq_scans(input, degree, mode, false);
        }
        PhysicalPlan::StreamingAggregate { input, .. }
        | PhysicalPlan::StreamingRollupAggregate { input, .. }
        | PhysicalPlan::StreamingGroupingSetsAggregate { input, .. }
        | PhysicalPlan::StreamingDistinct { input, .. } => {
            parallelize_seq_scans(input, degree, mode, true);
        }
        PhysicalPlan::Limit { input, .. }
        | PhysicalPlan::Projection { input, .. }
        | PhysicalPlan::WindowAgg { input, .. } => {
            parallelize_seq_scans(input, degree, mode, order_sensitive);
        }
        // Push parallelism through joins: build and probe of a hash
        // join consume their children once and materialise them, so a
        // `Exchange { ParallelSeqScan }` underneath is just a parallel
        // producer feeding the join's hash table / probe loop. Plain
        // nested-loop and index nested-loop joins iterate the outer
        // side once; parallelising the outer (left) saves work, while
        // the inner is either per-row index probes or a cached inner
        // block where parallelism wouldn't help. Same reasoning for
        // IndexNestedLoopJoin (only one child, on the left).
        PhysicalPlan::HashJoin { left, right, .. } => {
            parallelize_seq_scans(left, degree, mode, order_sensitive);
            parallelize_seq_scans(right, degree, mode, order_sensitive);
        }
        PhysicalPlan::AdaptiveJoin { left, right, .. } => {
            parallelize_seq_scans(left, degree, mode, order_sensitive);
            parallelize_seq_scans(right, degree, mode, order_sensitive);
        }
        PhysicalPlan::NestedLoopJoin { left, .. }
        | PhysicalPlan::IndexNestedLoopJoin { left, .. } => {
            parallelize_seq_scans(left, degree, mode, order_sensitive);
        }
        // Other index scans, CTE scans, and already-parallel nodes stay as-is.
        _ => {}
    }
}

/// Phase 2 CBO heuristic: should this `SeqScan` go parallel?
///
/// Three independent gates that must ALL succeed (de Morgan-friendly,
/// so callers can flip via env):
///   - estimated rows ≥ MYDB_PARALLEL_THRESHOLD_ROWS (default 100_000)
///   - table row_count ≥ MYDB_PARALLEL_MIN_PAGES * ROWS_PER_PAGE
///     (rough, since we don't ask the heap for page_count at plan time)
///   - parallel cost beats serial cost by at least 10 %
fn cbo_wants_parallel_for_seqscan(
    _estimated_rows: f64,
    access_rows: f64,
    schema: &crate::storage::TableSchema,
    serial_cost: crate::engine::cbo::Cost,
    degree: u32,
) -> bool {
    // Threshold is gated on `access_rows` (rows the worker actually
    // touches before WHERE filters them out), not the post-filter
    // output. A `WHERE foo = 7` over a 1M-row table still does 1M
    // rows of work; that's what determines whether parallelism pays
    // off, not the much smaller filtered cardinality.
    let config = parallel_planner_config();
    let threshold = config.threshold_rows as f64;
    if access_rows < threshold {
        return false;
    }
    let min_pages = config.min_pages;
    // Use stats.row_count when available (post-ANALYZE), otherwise
    // fall back to the scanned-row estimate.
    let row_count = if schema.stats.row_count > 0 {
        schema.stats.row_count as f64
    } else {
        access_rows
    };
    let rows_per_page = 100.0;
    let approx_pages = (row_count / rows_per_page).ceil();
    if approx_pages < (min_pages as f64) {
        return false;
    }
    let parallel = parallel_cost_from_serial(serial_cost, degree);
    let serial_total = serial_cost.weighted_total();
    let parallel_total = parallel.weighted_total();
    // 10 % hysteresis margin to avoid plan flap near the break-even point.
    parallel_total * 1.10 < serial_total
}

fn cbo_wants_parallel_for_index_fast_scan(
    estimated_rows: f64,
    access_rows: f64,
    serial_cost: crate::engine::cbo::Cost,
    degree: u32,
) -> bool {
    let threshold = parallel_planner_config().threshold_rows as f64;
    if estimated_rows.max(access_rows) < threshold {
        return false;
    }
    let serial_total = serial_cost.weighted_total();
    let parallel_total = parallel_cost_from_serial(serial_cost, degree).weighted_total();
    parallel_total * 1.10 < serial_total
}

/// Phase 2 CBO heuristic for a heap-fetching `IndexRangeScan`.
///
/// Unlike an index-only walk, work here is governed by heap page probes and
/// row decoding rather than output cardinality.  A fixed row-count floor can
/// therefore reject a scattered, expensive range even when the calibrated
/// cost (including random-page work) is far above parallel setup. Literal
/// ranges already have stable estimates, parametric ranges are rejected by
/// the caller, and `parallel_cost_from_serial` charges worker setup, so the
/// cost comparison itself is the general break-even guard.
fn cbo_wants_parallel_for_index_range_scan(
    _estimated_rows: f64,
    _access_rows: f64,
    serial_cost: crate::engine::cbo::Cost,
    degree: u32,
) -> bool {
    let serial_total = serial_cost.weighted_total();
    let parallel_total = parallel_cost_from_serial(serial_cost, degree).weighted_total();
    parallel_total * 1.10 < serial_total
}

/// Approximate the cost of running `serial` across `degree` workers.
/// Mirrors [`crate::engine::cbo::operator_cost::parallel_seq_scan`]:
/// parallelizable work scales by `1/degree`, plus a per-worker thread-spawn
/// / merge tax of `degree * cbo_parallel_setup_cost` on both startup
/// and total. Serial startup work (for example Zone Map range discovery)
/// is preserved rather than divided across workers.
///
/// Previously this scaled every field by 1/2 regardless of degree and
/// charged no setup cost, so the CBO break-even check was independent
/// of `MYDB_PARALLEL_DEGREE` and ignored thread overhead — small scans
/// at high degree got parallelised even when net cost went up.
fn parallel_cost_from_serial(
    serial: crate::engine::cbo::Cost,
    degree: u32,
) -> crate::engine::cbo::Cost {
    let p = degree.max(1) as f64;
    let setup = p * crate::engine::cbo::consts::consts().parallel_setup_cost();
    let serial_startup = serial.startup.min(serial.total);
    let parallelizable_total = (serial.total - serial_startup).max(0.0);
    crate::engine::cbo::Cost {
        startup: serial.startup + setup,
        total: serial_startup + parallelizable_total / p + setup,
        // Keep diagnostics as cumulative work. Only latency is divided by
        // parallelism; the same pages/tuples are still processed overall.
        io_seq_pages: serial.io_seq_pages,
        io_rnd_pages: serial.io_rnd_pages,
        cpu_units: serial.cpu_units + setup,
        mem_bytes: serial.mem_bytes,
        spill_bytes: serial.spill_bytes,
    }
}

/// Maximum K for which the planner emits a TopK heap operator. Beyond
/// this threshold, fall back to a full Sort (which Phase C will switch
/// to external sort with spill). Tunable via `MYDB_TOPK_MAX_HEAP`.
fn topk_max_heap_from_env() -> usize {
    crate::util::env::var("MYDB_TOPK_MAX_HEAP")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .unwrap_or(1_000_000)
}

/// Planning-time view of a LIMIT / OFFSET count. Literal rows resolve
/// directly; a `?` placeholder is *peeked* from the active thread-local
/// parameter context (auto-param and prepared-execute both bind params
/// before planning), so plan-shape decisions (TopK heap vs full sort,
/// row goals, vector-KNN k) are made with the real first-execution
/// value. Cached plans re-resolve at execution via [`LimitBinding`];
/// the auto-param fingerprint folds a magnitude bucket of the literal
/// in, so a reused plan only ever sees values within 2× of the peek.
fn limit_usize(value: Option<&LimitValue>) -> Option<usize> {
    match value {
        Some(LimitValue::Rows(value)) => Some(*value),
        Some(LimitValue::Placeholder(idx) | LimitValue::OffsetCommaPlaceholder(idx)) => {
            crate::engine::params::current_param_usize(*idx)
        }
        None => None,
    }
}

fn plain_limit_row_goal(select: &Select) -> Option<usize> {
    let limit = limit_usize(select.limit.as_ref())?;
    if !select.order_by.is_empty()
        || !select.group_by.is_empty()
        || select.having.is_some()
        || select.distinct
        || has_aggregate_select_items(&select.projection)
        || !select.named_windows.is_empty()
    {
        return None;
    }
    Some(limit.saturating_add(limit_usize(select.offset.as_ref()).unwrap_or(0)))
}

fn ordered_limit_row_goal(select: &Select) -> Option<usize> {
    let limit = limit_usize(select.limit.as_ref())?;
    if select.order_by.is_empty()
        || select
            .order_by
            .iter()
            .any(|o| o.nulls_first_effective() != o.asc)
        || !select.group_by.is_empty()
        || !select.grouping_sets.is_empty()
        || select.having.is_some()
        || (select.distinct && !distinct_topk_is_safe(select))
        || has_aggregate_select_items(&select.projection)
        || has_window_in_projection(&select.projection)
        || !select.named_windows.is_empty()
    {
        return None;
    }
    Some(limit.saturating_add(limit_usize(select.offset.as_ref()).unwrap_or(0)))
}

fn order_by_index_direction(order_by: &[OrderBy]) -> Option<bool> {
    if order_by.is_empty() {
        return None;
    }
    let all_asc = order_by.iter().all(|item| item.asc);
    let all_desc = order_by.iter().all(|item| !item.asc);
    if all_asc {
        Some(false)
    } else if all_desc {
        Some(true)
    } else {
        None
    }
}

pub(crate) fn plan_satisfies_order_by(plan: &PhysicalPlan, order_by: &[OrderBy]) -> bool {
    !order_by.is_empty() && provided_order_prefix_len(plan, order_by) >= order_by.len()
}

fn provided_order_prefix_len(plan: &PhysicalPlan, order_by: &[OrderBy]) -> usize {
    if order_by.is_empty() {
        return 0;
    }
    match plan {
        PhysicalPlan::Sort {
            order_by: existing, ..
        }
        | PhysicalPlan::IncrementalSort {
            order_by: existing, ..
        }
        | PhysicalPlan::TopK {
            order_by: existing, ..
        }
        | PhysicalPlan::TopWindow {
            order_by: existing, ..
        }
        | PhysicalPlan::ParallelSort {
            order_by: existing, ..
        }
        // A VectorIndexScan returns rows already sorted ascending by the KNN
        // distance it was built for, so it satisfies that exact ORDER BY —
        // suppressing the redundant Sort/TopK in `finish_plan`.
        | PhysicalPlan::VectorIndexScan {
            order_by: existing, ..
        }
        // A VectorBruteScan likewise returns rows pre-sorted ascending by the
        // exact KNN distance, so it satisfies that ORDER BY directly.
        | PhysicalPlan::VectorBruteScan {
            order_by: existing, ..
        } => common_order_prefix(existing, order_by),
        PhysicalPlan::Limit { input, .. }
        | PhysicalPlan::Projection {
            input,
            distinct: false,
            ..
        }
        // A predicate can remove rows but cannot reorder the survivors.
        // Propagating this property through both ordinary filters and Apply
        // lets an ordered access path satisfy ORDER BY ... LIMIT directly.
        | PhysicalPlan::Filter { input, .. }
        | PhysicalPlan::Apply { outer: input, .. } => {
            provided_order_prefix_len(input, order_by)
        }
        // Only these joins preserve the outer driver's order. RIGHT/FULL
        // append unmatched right rows with NULL left keys after all matches.
        PhysicalPlan::NestedLoopJoin { left, join_type, .. }
        | PhysicalPlan::IndexNestedLoopJoin { left, join_type, .. }
            if matches!(
                join_type,
                JoinType::Inner | JoinType::Left | JoinType::Cross | JoinType::Semi | JoinType::Anti
            ) =>
        {
            provided_order_prefix_len(left, order_by)
        }
        // Measure aggregates can be executed by the vector hash core. Only
        // adjacent-key DISTINCT has an ordered implementation in every route.
        PhysicalPlan::StreamingAggregate { input, aggregates, .. } if aggregates.is_empty() => {
            provided_order_prefix_len(input, order_by)
        }
        PhysicalPlan::StreamingAggregate { .. } => 0,
        // ROLLUP appends subtotal/grand-total NULL keys at transitions. That
        // output need not preserve the input's global NULL ordering.
        PhysicalPlan::StreamingRollupAggregate { .. } => 0,
        // A partitioned index scan is executed one partition at a time
        // and the results are concatenated (see the executor's
        // per-partition fan-out), so per-partition index order is not
        // the global order the query asked for. It therefore provides no
        // usable ordering — keep the Sort.
        PhysicalPlan::IndexLookup {
            table,
            index,
            filter,
            ..
        } if !table.schema.is_partitioned() => {
            index_order_prefix_len(table, index, filter.as_ref(), order_by, false)
        }
        PhysicalPlan::IndexRangeScan {
            table,
            index,
            filter,
            reverse,
            ..
        }
        | PhysicalPlan::IndexFastScan {
            table,
            index,
            filter,
            reverse,
            ..
        } if !table.schema.is_partitioned() => {
            index_order_prefix_len(table, index, filter.as_ref(), order_by, *reverse)
        }
        _ => 0,
    }
}

fn common_order_prefix(left: &[OrderBy], right: &[OrderBy]) -> usize {
    left.iter()
        .zip(right.iter())
        .take_while(|(l, r)| {
            l.asc == r.asc
                && l.nulls_first_effective() == r.nulls_first_effective()
                && expr_order_key_eq(&l.expr, &r.expr)
        })
        .count()
}

fn index_order_prefix_len(
    plan_table: &PlanTable,
    index: &IndexSchema,
    filter: Option<&Expr>,
    order_by: &[OrderBy],
    is_reverse: bool,
) -> usize {
    let table = &plan_table.schema;
    let mut eq_columns = HashMap::new();
    let mut ranges = HashMap::new();
    if let Some(filter) = filter {
        collect_index_constraints(table, filter, &mut eq_columns, &mut ranges);
    }
    let mut index_pos = 0usize;
    while index_pos < index.columns.len()
        && eq_columns.contains_key(&normalize_expr_column(&index.columns[index_pos]))
    {
        index_pos += 1;
    }
    let expected_asc = !is_reverse;
    let mut prefix = 0usize;
    for item in order_by {
        if item.asc != expected_asc {
            break;
        }
        // B-tree keys rank NULL lowest (`KeyPart::Null` is the first
        // variant), so index order delivers NULLs in default position
        // only; explicit NULLS FIRST/LAST that deviates needs a sort.
        if item.has_custom_nulls() {
            break;
        }
        if let Expr::CompoundIdentifier(parts) = &item.expr {
            if parts.len() >= 2
                && !matches_table(
                    &parts[parts.len() - 2],
                    &table.name,
                    plan_table.alias.as_deref(),
                )
            {
                break;
            }
        }
        let Some(column) = expr_column_name(&item.expr) else {
            break;
        };
        let column = normalize_expr_column(column);
        let mut matched = false;
        while index_pos < index.columns.len() {
            let index_column = normalize_expr_column(&index.columns[index_pos]);
            if index_column == column {
                if index.column_prefix_len(index_pos).is_some() {
                    break;
                }
                index_pos += 1;
                matched = true;
                break;
            }
            if eq_columns.contains_key(&index_column) {
                index_pos += 1;
                continue;
            }
            break;
        }
        if !matched
            || table
                .columns
                .iter()
                .all(|schema_col| !schema_col.name.eq_ignore_ascii_case(&column))
        {
            break;
        }
        prefix += 1;
    }
    prefix
}

fn expr_order_key_eq(left: &Expr, right: &Expr) -> bool {
    match (left, right) {
        (Expr::Identifier(a), Expr::Identifier(b)) => {
            normalize_expr_column(a) == normalize_expr_column(b)
        }
        (Expr::CompoundIdentifier(a), Expr::CompoundIdentifier(b)) => {
            a.len() == b.len()
                && a.iter()
                    .zip(b)
                    .all(|(a, b)| normalize_expr_column(a) == normalize_expr_column(b))
        }
        _ => expr_semantically_eq(left, right),
    }
}

/// Phase B #B4: the input plan's natural order already groups rows
/// by `group_by` so `StreamingAggregate` can emit one row per
/// group transition. Reuses `plan_satisfies_order_by` by treating
/// each grouping expression as an ASC OrderBy item.
fn input_satisfies_grouping_order(plan: &PhysicalPlan, group_by: &[Expr]) -> bool {
    if group_by.is_empty() {
        return false;
    }
    if input_groups_are_contiguous_by_left_row(plan, group_by) {
        return true;
    }
    if let PhysicalPlan::AdaptiveJoin {
        left,
        join_type,
        preferred,
        ..
    } = plan
    {
        if *preferred == AdaptiveJoinChoice::IndexNestedLoop
            && input_groups_are_contiguous_by_left_row_like(left, *join_type, group_by)
        {
            return true;
        }
    }
    let order_by: Vec<OrderBy> = group_by
        .iter()
        .map(|expr| OrderBy {
            expr: expr.clone(),
            asc: true,
            nulls_first: None,
        })
        .collect();
    plan_satisfies_order_by(plan, &order_by)
}

fn plan_contains_join_operator(plan: &PhysicalPlan) -> bool {
    plan_any(plan, &mut |node| {
        matches!(
            node,
            PhysicalPlan::NestedLoopJoin { .. }
                | PhysicalPlan::IndexNestedLoopJoin { .. }
                | PhysicalPlan::HashJoin { .. }
                | PhysicalPlan::AdaptiveJoin { .. }
        )
    })
}

/// Index nested-loop joins emit every matching right-side row for a
/// left row before advancing to the next left row. If GROUP BY includes
/// a unique key that is still key-preserved by the left input, each
/// group appears in at most one left row, so matches are contiguous
/// even without global ordering. A unique key from a table that has
/// already been duplicated by an upstream one-to-many join is not
/// enough: equal groups may reappear later in the input.
fn input_groups_are_contiguous_by_left_row(plan: &PhysicalPlan, group_by: &[Expr]) -> bool {
    let PhysicalPlan::IndexNestedLoopJoin {
        left, join_type, ..
    } = plan
    else {
        return false;
    };
    input_groups_are_contiguous_by_left_row_like(left, *join_type, group_by)
}

fn input_groups_are_contiguous_by_left_row_like(
    left: &PhysicalPlan,
    join_type: JoinType,
    group_by: &[Expr],
) -> bool {
    if !matches!(join_type, JoinType::Inner | JoinType::Left) {
        return false;
    }
    group_by
        .iter()
        .all(|expr| expr_can_resolve_in_plan(expr, left))
        && group_by_contains_key_preserved_unique_key(left, group_by)
}

fn group_by_contains_key_preserved_unique_key(plan: &PhysicalPlan, group_by: &[Expr]) -> bool {
    match plan {
        PhysicalPlan::SeqScan { .. }
        | PhysicalPlan::IndexLookup { .. }
        | PhysicalPlan::IndexMultiLookup { .. }
        | PhysicalPlan::IndexRangeScan { .. }
        | PhysicalPlan::IndexFastScan { .. }
        | PhysicalPlan::IndexInLookup { .. } => {
            group_by_contains_unique_key_from_plan(plan, group_by)
        }
        PhysicalPlan::Sort { input, .. }
        | PhysicalPlan::TopK { input, .. }
        | PhysicalPlan::ParallelTopK { input, .. }
        | PhysicalPlan::Limit { input, .. }
        | PhysicalPlan::Projection { input, .. } => {
            group_by_contains_key_preserved_unique_key(input, group_by)
        }
        PhysicalPlan::IndexNestedLoopJoin {
            left,
            right_index,
            join_type,
            eq_prefix_len,
            ..
        } if join_preserves_left_row_uniqueness(*join_type, right_index, *eq_prefix_len) => {
            group_by_contains_key_preserved_unique_key(left, group_by)
        }
        PhysicalPlan::HashJoin {
            left, join_type, ..
        }
        | PhysicalPlan::NestedLoopJoin {
            left, join_type, ..
        } if matches!(join_type, JoinType::Semi | JoinType::Anti) => {
            group_by_contains_key_preserved_unique_key(left, group_by)
        }
        _ => false,
    }
}

fn join_preserves_left_row_uniqueness(
    join_type: JoinType,
    right_index: &IndexSchema,
    eq_prefix_len: usize,
) -> bool {
    matches!(join_type, JoinType::Semi | JoinType::Anti)
        || (matches!(join_type, JoinType::Inner | JoinType::Left)
            && right_index.unique
            && eq_prefix_len >= right_index.columns.len())
}

fn group_by_contains_unique_key_from_plan(plan: &PhysicalPlan, group_by: &[Expr]) -> bool {
    let tables = collect_physical_plan_tables(plan);
    tables.iter().any(|table| {
        table
            .schema
            .iter_queryable_indexes()
            .filter(|index| index.unique && !index.columns.is_empty())
            .any(|index| {
                index.columns.iter().all(|column| {
                    group_by.iter().any(|expr| {
                        expr_matches_grouping_table_column(expr, table, column, &tables)
                    })
                })
            })
    })
}

fn expr_matches_grouping_table_column(
    expr: &Expr,
    table: &PlanTable,
    column: &str,
    tables: &[PlanTable],
) -> bool {
    let column = normalize_expr_column(column);
    match expr {
        Expr::Identifier(name) => {
            normalize_expr_column(name) == column
                && table
                    .schema
                    .columns
                    .iter()
                    .any(|schema_col| normalize_expr_column(&schema_col.name) == column)
                && tables
                    .iter()
                    .filter(|candidate| {
                        candidate
                            .schema
                            .columns
                            .iter()
                            .any(|schema_col| normalize_expr_column(&schema_col.name) == column)
                    })
                    .count()
                    == 1
        }
        Expr::CompoundIdentifier(parts) => {
            let Some(expr_column) = parts.last() else {
                return false;
            };
            if normalize_expr_column(expr_column) != column {
                return false;
            }
            let qualifier = if parts.len() >= 2 {
                Some(parts[parts.len() - 2].as_str())
            } else {
                None
            };
            qualifier.is_some_and(|qualifier| {
                matches_table(qualifier, &table.schema.name, table.alias.as_deref())
            })
        }
        Expr::Default(_) | Expr::DefaultQualified(_) => false,
        _ => false,
    }
}

/// Phase B #B4: the input plan's natural order is enough that two
/// equal projection rows are always adjacent — letting
/// `StreamingDistinct` dedup by comparing each row's projection key
/// to the previous one. We accept the cheap-but-correct heuristic:
/// every `SelectItem::Expr` that is a column reference must appear
/// in the satisfied prefix; wildcards and complex expressions force
/// the fallback.
fn input_satisfies_distinct_order(plan: &PhysicalPlan, projection: &[SelectItem]) -> bool {
    let mut keys: Vec<OrderBy> = Vec::with_capacity(projection.len());
    for item in projection {
        match item {
            SelectItem::Expr { expr, .. } => keys.push(OrderBy {
                expr: expr.clone(),
                asc: true,
                nulls_first: None,
            }),
            // Wildcards or qualified-wildcards drag in unknown columns
            // — the cheapest safe answer is "we don't know, skip".
            SelectItem::Wildcard | SelectItem::QualifiedWildcard(_) => return false,
        }
    }
    if keys.is_empty() {
        return false;
    }
    plan_satisfies_order_by(plan, &keys)
}

/// A bounded DISTINCT TopK is valid when every ORDER BY expression is one of
/// the projected expressions. Equal DISTINCT tuples then have equal sort
/// keys, so retaining one representative in the heap is equivalent to the
/// legacy `Sort -> Distinct -> Limit` pipeline.
///
/// Keep the physical rewrite independently conservative even though MySQL
/// normally validates this relationship: wildcard and unresolved shapes stay
/// on the full-sort correctness-oracle path.
fn distinct_topk_is_safe(select: &Select) -> bool {
    if select.order_by.is_empty()
        || select.projection.iter().any(|item| {
            matches!(
                item,
                SelectItem::Wildcard | SelectItem::QualifiedWildcard(_)
            )
        })
    {
        return false;
    }
    select.order_by.iter().all(|order| {
        select.projection.iter().any(|item| match item {
            SelectItem::Expr { expr, .. } => expr_order_key_eq(expr, &order.expr),
            SelectItem::Wildcard | SelectItem::QualifiedWildcard(_) => false,
        })
    })
}

fn index_order_covers_order_by(
    table: &TableSchema,
    index: &IndexSchema,
    filter: Option<&Expr>,
    order_by: &[OrderBy],
) -> bool {
    let mut eq_columns = HashMap::new();
    let mut ranges = HashMap::new();
    if let Some(filter) = filter {
        collect_index_constraints(table, filter, &mut eq_columns, &mut ranges);
    }
    let mut index_pos = 0usize;
    while index_pos < index.columns.len()
        && eq_columns.contains_key(&normalize_expr_column(&index.columns[index_pos]))
    {
        index_pos += 1;
    }
    for item in order_by {
        let Some(column) = expr_column_name(&item.expr) else {
            return false;
        };
        let column = normalize_expr_column(column);
        let mut matched = false;
        while index_pos < index.columns.len() {
            let index_column = normalize_expr_column(&index.columns[index_pos]);
            if index_column == column {
                if index.column_prefix_len(index_pos).is_some() {
                    return false;
                }
                index_pos += 1;
                matched = true;
                break;
            }
            if eq_columns.contains_key(&index_column) {
                index_pos += 1;
                continue;
            }
            return false;
        }
        if !matched
            || table
                .columns
                .iter()
                .all(|schema_col| !schema_col.name.eq_ignore_ascii_case(&column))
        {
            return false;
        }
    }
    true
}

fn plan_table(db: &Database, table: &TableRef) -> Result<PlanTable, QueryError> {
    if let Some(spec) = table.generate_series.as_deref() {
        if table.system_time.is_some() {
            return Err(error(
                crate::error::catalog::ER_NOT_SUPPORTED_YET,
                "FOR SYSTEM_TIME AS OF cannot qualify a table function",
            ));
        }
        let PhysicalPlan::GenerateSeries {
            columns,
            estimated_rows,
            ..
        } = plan_generate_series(table, spec)?
        else {
            unreachable!("plan_generate_series must return a GenerateSeries leaf");
        };
        let mut schema = TableSchema::synthetic_for_planning(&table.name, &columns);
        schema.stats.row_count = estimated_rows as u64;
        schema.stats.analyzed_at_unix_ms = Some(0);
        return Ok(PlanTable::new(0, Arc::new(schema), table.alias.clone()));
    }
    if table.system_time.is_some()
        && (try_plan_cte_scan(table).is_some()
            || crate::engine::information_schema::columns_for_table_name(&table.name).is_ok())
    {
        return Err(error(
            crate::error::catalog::ER_NOT_SUPPORTED_YET,
            "FOR SYSTEM_TIME AS OF can qualify only persistent user base tables",
        ));
    }
    if let Some(plan_table) = plan_table_for_ref_if_resolved(db, table) {
        let mut plan_table = plan_table.with_system_time(table.system_time.clone());
        plan_table.explicit_partition_storage_ids =
            crate::engine::executor::partition_clause::resolve_partition_clause(
                db,
                &table.name,
                &table.partitions,
            )?;
        return Ok(plan_table);
    }
    if let Some(error) = crate::engine::information_schema::hidden_system_table_error(&table.name) {
        return Err(error);
    }
    let schema = db.table(&table.name)?;
    let sidecar = db.table_modification_delta(schema.id);
    let live_delta = db.table_live_row_delta(schema.id);
    if table.system_time.is_some() && schema.system {
        return Err(error(
            crate::error::catalog::ER_NOT_SUPPORTED_YET,
            "FOR SYSTEM_TIME AS OF cannot qualify a system table",
        ));
    }
    Ok(
        PlanTable::with_sidecar(schema.id, schema, table.alias.clone(), sidecar, live_delta)
            .with_system_time(table.system_time.clone()),
    )
}

/// Return `Some(CteScan)` if `table` resolves to a CTE in the active CTE
/// registry, otherwise `None`. Used by the planner's leaf-table builder to
/// emit a `CteScan` plan node instead of attempting catalog lookup.
pub(crate) fn try_plan_cte_scan(table: &TableRef) -> Option<PhysicalPlan> {
    let cte = crate::engine::executor::cte::lookup_cte(&table.name)?;
    // For a materialised CTE this is exactly `rows.len()`; during EXPLAIN (no
    // materialisation) it carries the CTE body plan's estimated row count so
    // the CteScan does not collapse to rows=0.
    let estimated_rows = cte.estimated_rows;
    Some(PhysicalPlan::CteScan {
        name: table.name.clone(),
        alias: table.alias.clone(),
        columns: cte.columns.clone(),
        filter: None,
        estimated_rows,
        cost: crate::engine::cbo::Cost::scalar(estimated_rows),
        projected_columns: None,
    })
}

fn try_plan_system_scan(table: &TableRef, filter: Option<Expr>) -> Option<PhysicalPlan> {
    let columns = crate::engine::information_schema::columns_for_table_name(&table.name).ok()?;
    let estimated_rows = 1_000.0;
    Some(PhysicalPlan::SystemScan {
        name: table.name.clone(),
        relation: table.alias.clone().unwrap_or_else(|| {
            table
                .name
                .rsplit_once('.')
                .map(|(_, name)| name.to_string())
                .unwrap_or_else(|| table.name.clone())
        }),
        columns,
        filter,
        estimated_rows,
        cost: crate::engine::cbo::Cost::scalar(estimated_rows),
        projected_columns: None,
    })
}

/// P1-4: estimate the combined selectivity of `predicate` over a materialised
/// CTE using its per-column NDV. Handles `col = const`, `col IN (list)`, and
/// conjunctions (assuming predicate independence); any other shape contributes
/// no reduction (1.0). Returns a factor in `(0, 1]`. Plan-only — a wrong
/// estimate only changes operator sizing, never results.
fn cte_scan_selectivity(
    stats: &crate::engine::api::CteStats,
    columns: &[Column],
    predicate: &Expr,
) -> f64 {
    match predicate {
        Expr::Binary {
            op: BinaryOp::And,
            left,
            right,
        } => {
            cte_scan_selectivity(stats, columns, left) * cte_scan_selectivity(stats, columns, right)
        }
        Expr::Binary {
            op: BinaryOp::Eq,
            left,
            right,
        } => {
            // col = const (column on either side, constant on the other).
            let idx = match (cte_col_index(left, columns), cte_col_index(right, columns)) {
                (Some(i), None) if !expr_is_column(right) => Some(i),
                (None, Some(i)) if !expr_is_column(left) => Some(i),
                _ => None,
            };
            idx.map(|i| cte_col_eq_selectivity(stats, i)).unwrap_or(1.0)
        }
        Expr::InList {
            expr,
            list,
            negated: false,
        } => {
            if let Some(i) = cte_col_index(expr, columns) {
                let k = list.len().max(1) as f64;
                (k * cte_col_eq_selectivity(stats, i)).min(1.0)
            } else {
                1.0
            }
        }
        _ => 1.0,
    }
}

/// Equality selectivity for CTE column `idx`: `1/NDV` when the NDV is known
/// (≤ cap), else 1.0 (high-cardinality / unknown — stay conservative and do
/// not reduce).
fn cte_col_eq_selectivity(stats: &crate::engine::api::CteStats, idx: usize) -> f64 {
    match stats.columns.get(idx).and_then(|c| c.ndv) {
        Some(ndv) if ndv > 0 => 1.0 / ndv as f64,
        _ => 1.0,
    }
}

fn expr_is_column(expr: &Expr) -> bool {
    matches!(expr, Expr::Identifier(_) | Expr::CompoundIdentifier(_))
}

/// Index of the CTE output column referenced by `expr` (bare or qualified
/// identifier), matched by name (case-insensitive). `None` for non-columns.
fn cte_col_index(expr: &Expr, columns: &[Column]) -> Option<usize> {
    let bare = match expr {
        Expr::Identifier(name) => name.as_str(),
        Expr::CompoundIdentifier(parts) => parts.last()?.as_str(),
        _ => return None,
    };
    columns
        .iter()
        .position(|c| c.name.eq_ignore_ascii_case(bare))
}

/// Output column names for a `generate_series(...)` source, in order: the
/// value column (default `generate_series`) plus an `ordinality` column when
/// `WITH ORDINALITY` is present. `AS t(...)` aliases override the defaults.
fn generate_series_column_names(spec: &crate::parser::GenerateSeriesSpec) -> Vec<String> {
    let mut names = vec![spec
        .column_aliases
        .first()
        .cloned()
        .unwrap_or_else(|| "generate_series".to_string())];
    if spec.with_ordinality {
        names.push(
            spec.column_aliases
                .get(1)
                .cloned()
                .unwrap_or_else(|| "ordinality".to_string()),
        );
    }
    names
}

/// Plan a `generate_series(...)` FROM item into a `GenerateSeries` leaf.
/// Arguments are evaluated as constants now (M2: integer family only), which
/// yields an exact row-count estimate and rejects correlated arguments.
fn plan_generate_series(
    table: &TableRef,
    spec: &crate::parser::GenerateSeriesSpec,
) -> Result<PhysicalPlan, QueryError> {
    // Output / ordinality column names from `AS t(value [, ordinality])`.
    let output_name = spec
        .column_aliases
        .first()
        .cloned()
        .unwrap_or_else(|| "generate_series".to_string());
    let ordinality_name = if spec.with_ordinality {
        spec.column_aliases
            .get(1)
            .cloned()
            .unwrap_or_else(|| "ordinality".to_string())
    } else {
        String::new()
    };
    let expected_cols = if spec.with_ordinality { 2 } else { 1 };
    if !spec.column_aliases.is_empty() && spec.column_aliases.len() != expected_cols {
        return Err(error(
            crate::error::catalog::ER_WRONG_ARGUMENTS,
            format!(
                "generate_series produces {expected_cols} column(s) but {} alias(es) were given",
                spec.column_aliases.len()
            ),
        ));
    }
    // Evaluate bounds as constants. A NULL in any argument yields an empty
    // series (PostgreSQL semantics); a non-integer or correlated argument is
    // rejected (the numeric / timestamp families arrive in M4).
    let (series, value_type) = if spec.step.as_ref().map(expr_is_interval).unwrap_or(false) {
        build_timestamp_series(spec)?
    } else {
        let start = eval_generate_series_arg("start", &spec.start)?;
        let stop = eval_generate_series_arg("stop", &spec.stop)?;
        let step = match &spec.step {
            Some(expr) => eval_generate_series_arg("step", expr)?,
            None => SeriesArg::Num { mag: 1, scale: 0 },
        };
        build_series_kind(start, stop, step)?
    };
    let row_count = series_row_count(&series);
    let mut columns = vec![crate::types::Column {
        name: output_name.clone(),
        column_type: value_type,
        source: Default::default(),
    }];
    if spec.with_ordinality {
        columns.push(crate::types::Column {
            name: ordinality_name.clone(),
            column_type: crate::types::ColumnType::LongLong,
            source: Default::default(),
        });
    }
    Ok(PhysicalPlan::GenerateSeries {
        series,
        // Use the user-supplied alias (`AS t`) for column qualification, not
        // `table.name` — the planner qualifies the latter with the database
        // (`test.t`), which would never match the `t.n` the user wrote.
        relation: table
            .alias
            .clone()
            .unwrap_or_else(|| "generate_series".to_string()),
        output_name,
        with_ordinality: spec.with_ordinality,
        ordinality_name,
        columns,
        filter: None,
        estimated_rows: row_count,
        // Pure CPU, no I/O: charge a small per-row unit so the optimizer treats
        // a series as a cheap, exactly-sized source.
        cost: crate::engine::cbo::Cost::scalar(row_count * 0.01),
    })
}

/// A `generate_series` bound after constant evaluation, normalised to a
/// fixed-point magnitude + scale (integers have scale 0). `Null` collapses the
/// whole series to empty (PostgreSQL semantics).
enum SeriesArg {
    Null,
    Num { mag: i128, scale: u8 },
}

/// Evaluate a `generate_series` bound as a constant integer or exact decimal.
/// Correlated, float, or otherwise non-numeric arguments are rejected.
fn eval_generate_series_arg(label: &str, expr: &Expr) -> Result<SeriesArg, QueryError> {
    let value = crate::engine::expr::eval_const(expr).map_err(|_| {
        error(
            crate::error::catalog::ER_NOT_SUPPORTED_YET,
            format!(
                "generate_series {label} argument must be a constant expression \
                 (correlated arguments are not supported)"
            ),
        )
    })?;
    match value {
        crate::types::Value::Null => Ok(SeriesArg::Null),
        crate::types::Value::Int(n) => Ok(SeriesArg::Num {
            mag: n as i128,
            scale: 0,
        }),
        crate::types::Value::UInt(n) => Ok(SeriesArg::Num {
            mag: n as i128,
            scale: 0,
        }),
        crate::types::Value::DecimalCompact(mag, scale) => Ok(SeriesArg::Num {
            mag: mag as i128,
            scale,
        }),
        crate::types::Value::Decimal(text) => crate::util::decimal::parse_exact_decimal_str(&text)
            .map(|(mag, scale)| SeriesArg::Num { mag, scale })
            .ok_or_else(|| {
                error(
                    crate::error::catalog::ER_NOT_SUPPORTED_YET,
                    format!("generate_series {label} argument is not an exact numeric"),
                )
            }),
        _ => Err(error(
            crate::error::catalog::ER_NOT_SUPPORTED_YET,
            "generate_series supports only integer and exact-numeric arguments \
             (timestamp + interval lands in a later milestone)"
                .to_string(),
        )),
    }
}

/// Combine three evaluated bounds into a `SeriesKind` and its output column
/// type. A NULL bound yields an empty integer series. Integer-valued bounds at
/// scale 0 that fit `i64` use the integer family; everything else is promoted
/// to the numeric (fixed-point) family at the common scale.
fn build_series_kind(
    start: SeriesArg,
    stop: SeriesArg,
    step: SeriesArg,
) -> Result<(SeriesKind, crate::types::ColumnType), QueryError> {
    let decimal_scale = [&start, &stop, &step]
        .iter()
        .filter_map(|arg| match arg {
            SeriesArg::Num { mag, scale } if *scale > 0 || !fits_i64(*mag) => Some(*scale),
            _ => None,
        })
        .max();
    let (a, b, c) = match (start, stop, step) {
        (
            SeriesArg::Num { mag: a, scale: sa },
            SeriesArg::Num { mag: b, scale: sb },
            SeriesArg::Num { mag: c, scale: sc },
        ) => ((a, sa), (b, sb), (c, sc)),
        // Any NULL → empty series, encoded as an empty integer range.
        _ => {
            return Ok((
                SeriesKind::Int {
                    start: 0,
                    stop: -1,
                    step: 1,
                },
                decimal_scale
                    .map(|scale| crate::types::ColumnType::Decimal {
                        precision: 38,
                        scale,
                    })
                    .unwrap_or(crate::types::ColumnType::LongLong),
            ))
        }
    };
    let scale = a.1.max(b.1).max(c.1);
    let rescale = |mag: i128, from: u8| {
        crate::util::decimal::rescale_round_i128(mag, from, scale).ok_or_else(|| {
            error(
                crate::error::catalog::ER_WRONG_ARGUMENTS,
                "generate_series argument exceeds numeric range".to_string(),
            )
        })
    };
    let start = rescale(a.0, a.1)?;
    let stop = rescale(b.0, b.1)?;
    let step = rescale(c.0, c.1)?;
    if step == 0 {
        return Err(error(
            crate::error::catalog::ER_WRONG_ARGUMENTS,
            "generate_series step size cannot be zero".to_string(),
        ));
    }
    if scale == 0 && fits_i64(start) && fits_i64(stop) && fits_i64(step) {
        Ok((
            SeriesKind::Int {
                start: start as i64,
                stop: stop as i64,
                step: step as i64,
            },
            crate::types::ColumnType::LongLong,
        ))
    } else {
        let precision = decimal_precision_for(start, stop, scale);
        Ok((
            SeriesKind::Dec {
                start,
                stop,
                step,
                scale,
            },
            crate::types::ColumnType::Decimal { precision, scale },
        ))
    }
}

fn fits_i64(mag: i128) -> bool {
    i64::try_from(mag).is_ok()
}

/// Pick a DECIMAL precision wide enough for every generated value.
fn decimal_precision_for(start: i128, stop: i128, scale: u8) -> u8 {
    let max_abs = start.unsigned_abs().max(stop.unsigned_abs());
    let mag_digits = if max_abs == 0 {
        1
    } else {
        (max_abs as f64).log10().floor() as u32 + 1
    };
    mag_digits.max(scale as u32 + 1).min(65) as u8
}

/// True when `expr` is a desugared `INTERVAL ...` literal (`__INTERVAL(..)`).
fn expr_is_interval(expr: &Expr) -> bool {
    matches!(expr, Expr::Function { name, .. } if name.eq_ignore_ascii_case("__INTERVAL"))
}

/// Build a timestamp-family series: `start`/`stop` are datetimes and `step` is
/// an INTERVAL. Bounds and interval are evaluated as constants now.
fn build_timestamp_series(
    spec: &crate::parser::GenerateSeriesSpec,
) -> Result<(SeriesKind, crate::types::ColumnType), QueryError> {
    let eval = |label: &str, expr: &Expr| -> Result<crate::types::Value, QueryError> {
        crate::engine::expr::eval_const(expr).map_err(|_| {
            error(
                crate::error::catalog::ER_NOT_SUPPORTED_YET,
                format!(
                    "generate_series {label} argument must be a constant expression \
                     (correlated arguments are not supported)"
                ),
            )
        })
    };
    let start_v = eval("start", &spec.start)?;
    let stop_v = eval("stop", &spec.stop)?;
    // A NULL bound → empty series, encoded as an empty integer range.
    if matches!(start_v, crate::types::Value::Null) || matches!(stop_v, crate::types::Value::Null) {
        return Ok((
            SeriesKind::Int {
                start: 0,
                stop: -1,
                step: 1,
            },
            crate::types::ColumnType::DateTime,
        ));
    }
    let start = crate::engine::expr::value_to_datetime_micros(&start_v)?;
    let stop = crate::engine::expr::value_to_datetime_micros(&stop_v)?;
    let step_expr = spec
        .step
        .as_ref()
        .expect("timestamp family requires an interval step");
    let (amount, unit) = crate::engine::expr::extract_interval_arg(step_expr, &mut |e| {
        crate::engine::expr::eval_const(e)
    })?;
    let next = crate::engine::expr::add_interval_to_micros(start, &amount, &unit, false)?;
    if next == start {
        return Err(error(
            crate::error::catalog::ER_WRONG_ARGUMENTS,
            "generate_series interval step is zero".to_string(),
        ));
    }
    let ascending = next > start;
    Ok((
        SeriesKind::Ts {
            start,
            stop,
            amount,
            unit,
            ascending,
        },
        crate::types::ColumnType::DateTime,
    ))
}

/// Exact number of rows a numeric series emits; an approximate count for the
/// timestamp family (month-based intervals vary in length, so the estimate
/// uses the first step's span — execution is still exact).
fn series_row_count(series: &SeriesKind) -> f64 {
    let (start, stop, step) = match series {
        SeriesKind::Int { start, stop, step } => (*start as i128, *stop as i128, *step as i128),
        SeriesKind::Dec {
            start, stop, step, ..
        } => (*start, *stop, *step),
        SeriesKind::Ts {
            start,
            stop,
            amount,
            unit,
            ..
        } => {
            let next = crate::engine::expr::add_interval_to_micros(*start, amount, unit, false)
                .unwrap_or(*start);
            (*start as i128, *stop as i128, next as i128 - *start as i128)
        }
    };
    if step == 0 || (step > 0 && start > stop) || (step < 0 && start < stop) {
        0.0
    } else {
        (stop.abs_diff(start) / step.unsigned_abs()) as f64 + 1.0
    }
}

/// First `__match_against` Function node anywhere in `expr` (bare
/// `MATCH(...)` predicate or nested, e.g. `MATCH(...) > 0.5`). `None` if
/// the predicate has no full-text search.
fn find_fulltext_match(expr: &Expr) -> Option<&Expr> {
    if let Expr::Function { name, .. } = expr {
        if name == crate::parser::FT_MATCH_FUNCTION {
            return Some(expr);
        }
    }
    match expr {
        Expr::Unary { expr, .. } | Expr::IsNull(expr) | Expr::IsNotNull(expr) => {
            find_fulltext_match(expr)
        }
        Expr::Binary { left, right, .. } => {
            find_fulltext_match(left).or_else(|| find_fulltext_match(right))
        }
        Expr::Like { expr, pattern, .. } => {
            find_fulltext_match(expr).or_else(|| find_fulltext_match(pattern))
        }
        Expr::Between {
            expr, low, high, ..
        } => find_fulltext_match(expr)
            .or_else(|| find_fulltext_match(low))
            .or_else(|| find_fulltext_match(high)),
        Expr::InList { expr, list, .. } => {
            find_fulltext_match(expr).or_else(|| list.iter().find_map(find_fulltext_match))
        }
        Expr::Function { args, .. } => args.iter().find_map(find_fulltext_match),
        Expr::Case {
            operand,
            conditions,
            results,
            else_result,
        } => operand
            .as_deref()
            .and_then(find_fulltext_match)
            .or_else(|| conditions.iter().find_map(find_fulltext_match))
            .or_else(|| results.iter().find_map(find_fulltext_match))
            .or_else(|| else_result.as_deref().and_then(find_fulltext_match)),
        _ => None,
    }
}

/// Decode the parser's `__match_against(mode, query, col, ...)` argument
/// layout into `(mode, query, columns)`. `None` if the node is malformed.
fn fulltext_match_parts(
    match_expr: &Expr,
) -> Option<(crate::parser::FtMode, String, Option<usize>, Vec<String>)> {
    let Expr::Function { name, args, .. } = match_expr else {
        return None;
    };
    if name != crate::parser::FT_MATCH_FUNCTION || args.len() < 3 {
        return None;
    }
    let mode = match &args[0] {
        Expr::Literal(crate::parser::Literal::String(s)) => crate::parser::FtMode::parse(s)?,
        _ => return None,
    };
    let (query, query_param) = match &args[1] {
        Expr::Literal(crate::parser::Literal::String(s)) => (s.clone(), None),
        Expr::BoundValue(Value::String(s)) => (s.clone(), None),
        Expr::BoundValue(Value::Bytes(bytes)) => {
            (String::from_utf8_lossy(bytes).into_owned(), None)
        }
        Expr::BoundValue(value @ Value::SmallString(_)) => (value.to_display_string(), None),
        Expr::Placeholder(idx) => (String::new(), Some(*idx)),
        _ => return None,
    };
    let mut columns = Vec::with_capacity(args.len() - 2);
    for arg in &args[2..] {
        match arg {
            Expr::Identifier(name) => columns.push(name.clone()),
            Expr::CompoundIdentifier(parts) => columns.push(parts.last()?.clone()),
            _ => return None,
        }
    }
    if columns.is_empty() {
        return None;
    }
    Some((mode, query, query_param, columns))
}

/// True for a top-level bare `MATCH(...)` boolean predicate (not a MATCH
/// nested inside a comparison). The FulltextScan already returns only
/// matching rows, so the bare predicate is redundant and is stripped from
/// the residual filter; a nested `MATCH(...) <op> x` is kept and reads the
/// score pseudo-column at eval time.
fn is_bare_fulltext_match(expr: &Expr) -> bool {
    matches!(expr, Expr::Function { name, .. } if name == crate::parser::FT_MATCH_FUNCTION)
}

/// Remove bare `MATCH(...)` conjuncts from a WHERE filter, recombining the
/// remainder. Returns the residual filter (`None` when nothing is left).
fn strip_bare_fulltext_predicate(filter: Option<Expr>, match_fingerprint: u64) -> Option<Expr> {
    let expr = filter?;
    let mut conjuncts = Vec::new();
    split_conjuncts(&expr, &mut conjuncts);
    let kept: Vec<Expr> = conjuncts
        .into_iter()
        .filter(|c| {
            !is_bare_fulltext_match(&c)
                || crate::engine::expr_program::expr_fingerprint(&c) != match_fingerprint
        })
        .collect();
    combine_conjuncts(kept)
}

fn is_positive_fulltext_conjunct(filter: &Expr, match_fingerprint: u64) -> bool {
    let mut conjuncts = Vec::new();
    split_conjuncts(filter, &mut conjuncts);
    conjuncts.into_iter().any(|expr| {
        is_bare_fulltext_match(&expr)
            && crate::engine::expr_program::expr_fingerprint(&expr) == match_fingerprint
    })
}

/// Find a queryable FULLTEXT index whose column set exactly equals the
/// MATCH column list (order-independent). MySQL requires an exact match —
/// never a prefix — so we compare sorted, lower-cased name sets.
fn find_matching_fulltext_index<'a>(
    table: &'a TableSchema,
    match_columns: &[String],
) -> Option<&'a IndexSchema> {
    let mut want: Vec<String> = match_columns
        .iter()
        .map(|c| c.to_ascii_lowercase())
        .collect();
    want.sort();
    table.iter_queryable_indexes().find(|index| {
        if !index.is_fulltext() {
            return false;
        }
        let mut have: Vec<String> = index
            .columns
            .iter()
            .map(|c| c.to_ascii_lowercase())
            .collect();
        have.sort();
        have == want
    })
}

fn cost_fulltext_base_access(
    table: &TableSchema,
    choice: IndexAccess,
    total_rows: f64,
) -> (FulltextBaseAccess, f64) {
    let access_rows = match &choice.kind {
        IndexAccessKind::Lookup { key, exact } => {
            estimate_constant_index_lookup_rows(table, &choice.index, key, *exact, total_rows)
        }
        IndexAccessKind::MultiLookup { keys, exact } => estimate_constant_index_multi_lookup_rows(
            table,
            &choice.index,
            keys,
            *exact,
            total_rows,
        ),
        IndexAccessKind::Range { low, high } => {
            estimate_index_range_access_rows(table, &choice.index, low, high, total_rows)
        }
    };
    let access = match choice.kind {
        IndexAccessKind::Lookup { key, exact } => FulltextBaseAccess::Lookup {
            index: choice.index,
            key,
            exact,
        },
        IndexAccessKind::MultiLookup { keys, exact } => FulltextBaseAccess::MultiLookup {
            index: choice.index,
            keys,
            exact,
        },
        IndexAccessKind::Range { low, high } => FulltextBaseAccess::Range {
            index: choice.index,
            low,
            high,
        },
    };
    (access, access_rows)
}

pub(crate) fn ngram_token_is_like_safe(column: &crate::storage::ColumnSchema, token: &str) -> bool {
    let Some(collation) = column.collation.as_deref() else {
        return true;
    };
    let Ok(info) = crate::charset::resolve_collation(collation) else {
        return false;
    };
    if info.binary || info.accent_sensitive {
        return true;
    }
    if token.is_empty() || crate::charset::collation_key(collation, token) != token.as_bytes() {
        return false;
    }
    token.chars().all(|ch| {
        if ch.is_ascii() {
            return ch.is_ascii_digit();
        }
        !unicode_normalization::char::is_combining_mark(ch)
            && ch.to_lowercase().collect::<String>() == ch.to_string()
    })
}

fn find_like_ngram_prefilter(
    table: &TableSchema,
    expr: &Expr,
    hints: &ScanHints<'_>,
) -> Option<(IndexSchema, FulltextLikePrefilterPlan)> {
    if !crate::storage::schema::like_ngram_prefilter_enabled() {
        return None;
    }
    let mut conjuncts = Vec::new();
    split_conjuncts(expr, &mut conjuncts);
    for conjunct in conjuncts {
        let Expr::Like {
            expr: source,
            pattern,
            negated: false,
            escape_char,
        } = conjunct
        else {
            continue;
        };
        let Some(column_name) = expr_column_name(&source) else {
            continue;
        };
        let (pattern, pattern_param) = match pattern.as_ref() {
            Expr::Placeholder(index) => (None, Some(*index)),
            _ => (expr_literal_string(&pattern), None),
        };
        if pattern.is_none() && pattern_param.is_none() {
            continue;
        }
        // Anchored literals/prefixes belong to B-tree/zone paths. This path
        // exists specifically for patterns whose first token is unanchored.
        if pattern.as_ref().is_some_and(|pattern| {
            crate::engine::expr::analyze_like_sargable_pattern(pattern, escape_char).is_some()
        }) {
            continue;
        }
        let column = table
            .columns
            .iter()
            .find(|column| column.name.eq_ignore_ascii_case(column_name))?;
        let index = table.iter_queryable_indexes().find(|index| {
            index.is_fulltext()
                && index.columns.len() == 1
                && index.columns[0].eq_ignore_ascii_case(column_name)
                && index
                    .fulltext
                    .as_ref()
                    .is_some_and(|meta| meta.parser.eq_ignore_ascii_case("ngram"))
                && index_allowed_by_hints(&index.name, hints.index_hints, IndexHintScope::Join)
        })?;
        let meta = index.fulltext.as_ref()?;
        // When a B-tree exists, its reusable LIKE parameter bounds provide
        // the adaptive literal/prefix path. Leave placeholders to that path;
        // this deferred ngram template is for otherwise sequential scans.
        if pattern_param.is_some()
            && table.iter_queryable_indexes().any(|candidate| {
                !candidate.is_fulltext()
                    && !candidate.is_spatial()
                    && !candidate.is_vector()
                    && candidate
                        .columns
                        .first()
                        .is_some_and(|name| name.eq_ignore_ascii_case(column_name))
                    && index_allowed_by_hints(
                        &candidate.name,
                        hints.index_hints,
                        IndexHintScope::Join,
                    )
            })
        {
            continue;
        }
        let tokenizer = crate::storage::fulltext::build_tokenizer_from_meta(meta);
        let segments = pattern
            .as_ref()
            .and_then(|pattern| crate::engine::expr::like_literal_segments(pattern, escape_char))
            .unwrap_or_default();
        let mut tokens = Vec::new();
        let mut seen = std::collections::HashSet::new();
        for segment in &segments {
            for token in tokenizer.tokenize(segment) {
                if ngram_token_is_like_safe(column, &token) && seen.insert(token.clone()) {
                    tokens.push(token);
                    if tokens.len() >= 64 {
                        break;
                    }
                }
            }
            if tokens.len() >= 64 {
                break;
            }
        }
        if tokens.is_empty() && pattern_param.is_none() {
            crate::engine::status_counters::record_like_ngram_no_safe_token();
            continue;
        }
        return Some((
            index.clone(),
            FulltextLikePrefilterPlan {
                column: column.name.clone(),
                tokens,
                literal_segments: segments.len(),
                pattern_param,
                escape_char,
            },
        ));
    }
    None
}

fn expr_has_positive_like(expr: &Expr) -> bool {
    match expr {
        Expr::Like { negated: false, .. } => true,
        Expr::Unary { expr, .. } => expr_has_positive_like(expr),
        Expr::Binary { left, right, .. } => {
            expr_has_positive_like(left) || expr_has_positive_like(right)
        }
        Expr::Between {
            expr, low, high, ..
        } => {
            expr_has_positive_like(expr)
                || expr_has_positive_like(low)
                || expr_has_positive_like(high)
        }
        Expr::InList { expr, list, .. } => {
            expr_has_positive_like(expr) || list.iter().any(expr_has_positive_like)
        }
        Expr::Function { args, .. } | Expr::Tuple(args) => args.iter().any(expr_has_positive_like),
        _ => false,
    }
}

/// Spatial predicates whose qualifying rows all have an MBR intersecting the
/// query geometry's MBR — i.e. the R-tree search window is a correct
/// *superset*. `Disjoint` is excluded: its qualifying rows are the complement
/// of the window, which an R-tree cannot prune.
fn is_spatial_index_predicate(upper: &str) -> bool {
    matches!(
        upper,
        "MBRCONTAINS"
            | "MBRWITHIN"
            | "MBRINTERSECTS"
            | "MBRCOVERS"
            | "MBRCOVEREDBY"
            | "MBREQUALS"
            | "MBRTOUCHES"
            | "MBROVERLAPS"
            | "ST_CONTAINS"
            | "ST_WITHIN"
            | "ST_INTERSECTS"
            | "ST_CROSSES"
            | "ST_OVERLAPS"
            | "ST_EQUALS"
            | "ST_TOUCHES"
    )
}

/// True when `expr` references any table column. Used to confirm a spatial
/// predicate's "constant geometry" operand can be folded against an empty row
/// without touching `values` (the legacy evaluator indexes `values` for a
/// column ref, which would panic on `&[]`). Unrecognised shapes are treated
/// conservatively as column-referencing so the spatial index is simply not
/// used rather than risking an evaluation against a missing row.
fn expr_has_column_ref(expr: &Expr) -> bool {
    match expr {
        Expr::Identifier(_) | Expr::CompoundIdentifier(_) => true,
        Expr::Literal(_)
        | Expr::BoundValue(_)
        | Expr::UserVariable(_)
        | Expr::SystemVariable { .. }
        | Expr::Placeholder(_) => false,
        Expr::Unary { expr, .. } => expr_has_column_ref(expr),
        Expr::Binary { left, right, .. } => expr_has_column_ref(left) || expr_has_column_ref(right),
        Expr::IsNull(e) | Expr::IsNotNull(e) => expr_has_column_ref(e),
        Expr::Function { args, .. } => args.iter().any(expr_has_column_ref),
        _ => true,
    }
}

fn expr_has_placeholder(expr: &Expr) -> bool {
    match expr {
        Expr::Placeholder(_) => true,
        Expr::Unary { expr, .. } | Expr::IsNull(expr) | Expr::IsNotNull(expr) => {
            expr_has_placeholder(expr)
        }
        Expr::Binary { left, right, .. } => {
            expr_has_placeholder(left) || expr_has_placeholder(right)
        }
        Expr::Function { args, .. } => args.iter().any(expr_has_placeholder),
        _ => false,
    }
}

/// Find a Public SPATIAL index over exactly `column`.
fn find_spatial_index_for_column<'a>(
    table: &'a TableSchema,
    column: &str,
    hints: &ScanHints<'_>,
) -> Option<&'a IndexSchema> {
    let srid_restricted = table
        .iter_visible_columns()
        .map(|(_, column)| column)
        .find(|candidate| candidate.name.eq_ignore_ascii_case(column))
        .is_some_and(|candidate| {
            matches!(
                candidate.data_type,
                crate::storage::DataTypeKind::Geometry { srid: Some(_), .. }
            )
        });
    if !srid_restricted {
        // MySQL maintains an index on an unrestricted geometry column but
        // never considers it for spatial range access.
        return None;
    }
    table.iter_queryable_indexes().find(|index| {
        index.is_spatial()
            && special_index_allowed(index, table, hints)
            && index.columns.len() == 1
            && index.columns[0].eq_ignore_ascii_case(column)
    })
}

/// Detect a spatial-index-driving predicate in `expr`'s top-level conjuncts:
/// `pred(col, const)` / `pred(const, col)` where `col` carries a SPATIAL index
/// and the other operand is a constant geometry. Returns the index and the
/// constant geometry's MBR (the R-tree search window).
fn find_spatial_index_scan<'a>(
    table: &'a TableSchema,
    expr: &Expr,
    hints: &ScanHints<'_>,
) -> Option<(&'a IndexSchema, SpatialProbe)> {
    // The spatial scan reads the index under the table's own storage id; on a
    // partitioned table the MBR entries live under per-partition storage ids,
    // so fall back to a normal scan to avoid missing rows.
    if table.is_partitioned() {
        return None;
    }
    for conj in expr_conjuncts(expr) {
        let Expr::Function { name, args, .. } = conj else {
            continue;
        };
        if args.len() != 2 || !is_spatial_index_predicate(&name.to_ascii_uppercase()) {
            continue;
        }
        for (col_i, const_i) in [(0usize, 1usize), (1usize, 0usize)] {
            let Some(col) = expr_column_name(&args[col_i]) else {
                continue;
            };
            let Some(index) = find_spatial_index_for_column(table, col, hints) else {
                continue;
            };
            if expr_has_column_ref(&args[const_i]) {
                continue;
            }
            // A prepared parameter must remain an execution-time probe even
            // when the first execution already has a bound value. Baking that
            // first value into the cached physical plan makes later COM_STMT
            // executions probe the wrong MBR and also lets parameter values
            // change physical-plan state.
            if expr_has_placeholder(&args[const_i]) || !expr_is_deterministic(&args[const_i]) {
                return Some((index, SpatialProbe::Parameter(args[const_i].clone())));
            }
            // Fold the constant geometry operand and take its MBR as the
            // search window. `&[]` is safe — we just confirmed no column ref.
            let Ok(crate::types::Value::Geometry(bytes)) =
                crate::engine::expr::eval_expr_legacy(&args[const_i], table, &[])
            else {
                continue;
            };
            if let Some(m) = crate::engine::geo::mbr_of(&bytes) {
                return Some((
                    index,
                    SpatialProbe::Constant([m.minx, m.miny, m.maxx, m.maxy]),
                ));
            }
        }
    }
    None
}

/// Enumerate every public vector index over `column` whose persisted distance
/// metric matches the query. A column can legitimately have HNSW and IVF (or
/// several tuned variants) at once; callers must cost them instead of relying
/// on catalog order.
fn vector_indexes_for_column<'a>(
    table: &'a TableSchema,
    column: &str,
    metric: crate::engine::vector::Metric,
) -> impl Iterator<Item = &'a IndexSchema> + 'a {
    let column = column.to_string();
    table.iter_queryable_indexes().filter(move |index| {
        index.is_vector()
            && index.columns.len() == 1
            && index.columns[0].eq_ignore_ascii_case(&column)
            && index.vector_meta_for_table(table).metric_enum() == metric
    })
}

/// If `expr` is a bare identifier naming a projection alias, return the
/// aliased expression (so `ORDER BY d` resolves to its `DISTANCE(...) AS d`
/// definition); otherwise return `expr` itself. Only single-name identifiers
/// are resolved — a qualified `t.d` is left as-is.
fn resolve_order_alias<'a>(expr: &'a Expr, projection: &'a [SelectItem]) -> &'a Expr {
    let Expr::Identifier(name) = expr else {
        return expr;
    };
    for item in projection {
        if let SelectItem::Expr {
            expr: pexpr,
            alias: Some(alias),
        } = item
        {
            if alias.eq_ignore_ascii_case(name) {
                return pexpr;
            }
        }
    }
    expr
}

/// Recognise a vector-distance ranking call `pred(col, const)` /
/// `pred(const, col)` and return `(column, &const_operand, metric)`:
///   * `L2_DISTANCE(a, b)` / `COSINE_DISTANCE(a, b)` — fixed metric, 2 args.
///   * `DISTANCE(a, b)` / `VEC_DISTANCE(a, b)` — default L2.
///   * `DISTANCE(a, b, 'METRIC')` — metric from a constant string 3rd arg.
/// One operand must be a plain column and the other free of column refs (a
/// constant query vector). Returns `None` for any other shape.
fn match_distance_call(expr: &Expr) -> Option<(&str, &Expr, crate::engine::vector::Metric)> {
    use crate::engine::vector::Metric;
    // pgvector's `<#>` parser rewrite is `(0 - DOT_PRODUCT(col, query))`.
    // Accept that shape, the equivalent unary-minus form, and the explicit
    // MAX_INNER_PRODUCT_DISTANCE function as the canonical DOT ordering
    // distance. A bare DOT_PRODUCT is deliberately not accepted: ascending
    // DOT_PRODUCT orders the *least* similar row first.
    let dot_wrapper = match expr {
        Expr::Binary {
            left,
            op: BinaryOp::Sub,
            right,
        } if matches!(
            left.as_ref(),
            Expr::Literal(crate::parser::Literal::Int(0) | crate::parser::Literal::UInt(0))
        ) =>
        {
            Some(right.as_ref())
        }
        Expr::Unary {
            op: crate::parser::UnaryOp::Neg,
            expr,
        } => Some(expr.as_ref()),
        _ => None,
    };
    let function = dot_wrapper.unwrap_or(expr);
    let Expr::Function {
        name,
        args,
        distinct,
    } = function
    else {
        return None;
    };
    if *distinct || args.len() < 2 {
        return None;
    }
    let upper = name.to_ascii_uppercase();
    let metric = if dot_wrapper.is_some() {
        match upper.as_str() {
            "DOT_PRODUCT" | "INNER_PRODUCT" if args.len() == 2 => Metric::Dot,
            _ => return None,
        }
    } else {
        match upper.as_str() {
            "L2_DISTANCE" if args.len() == 2 => Metric::L2,
            "HAMMING_DISTANCE" if args.len() == 2 => Metric::Hamming,
            "JACCARD_DISTANCE" if args.len() == 2 => Metric::Jaccard,
            "COSINE_DISTANCE" if args.len() == 2 => Metric::Cosine,
            "MAX_INNER_PRODUCT_DISTANCE" if args.len() == 2 => Metric::Dot,
            "DISTANCE" | "VEC_DISTANCE" => match args.len() {
                2 => Metric::L2,
                3 => {
                    let Expr::Literal(crate::parser::Literal::String(s)) = &args[2] else {
                        return None;
                    };
                    Metric::parse(s)?
                }
                _ => return None,
            },
            _ => return None,
        }
    };
    for (col_i, const_i) in [(0usize, 1usize), (1usize, 0usize)] {
        let Some(col) = expr_column_name(&args[col_i]) else {
            continue;
        };
        if expr_has_column_ref(&args[const_i]) {
            continue;
        }
        return Some((col, &args[const_i], metric));
    }
    None
}

/// Recognise the similarity form accepted for descending DOT KNN:
/// `DOT_PRODUCT(col, const) DESC` / `INNER_PRODUCT(col, const) DESC`.
/// A descending similarity is equivalent to ascending negative inner product.
fn match_dot_similarity_call(expr: &Expr) -> Option<(&str, &Expr, crate::engine::vector::Metric)> {
    let Expr::Function {
        name,
        args,
        distinct,
    } = expr
    else {
        return None;
    };
    if *distinct
        || args.len() != 2
        || !matches!(
            name.to_ascii_uppercase().as_str(),
            "DOT_PRODUCT" | "INNER_PRODUCT"
        )
    {
        return None;
    }
    for (col_i, const_i) in [(0usize, 1usize), (1usize, 0usize)] {
        let Some(col) = expr_column_name(&args[col_i]) else {
            continue;
        };
        if !expr_has_column_ref(&args[const_i]) {
            return Some((col, &args[const_i], crate::engine::vector::Metric::Dot));
        }
    }
    None
}

/// Coerce a folded constant `Value` into internal vector bytes (little-endian
/// `f32`). Accepts an existing `Value::Vector` or a `[a,b,c]` text / raw-byte
/// form; returns `None` for anything else (the KNN path then falls back).
pub(crate) fn value_to_vector_bytes(value: &Value) -> Option<crate::vector::VectorValue> {
    match value {
        Value::Vector(b) => crate::engine::vector::normalize_internal(b).ok(),
        Value::String(s) | Value::Decimal(s) => {
            crate::engine::vector::coerce_bytes_to_internal(s.as_bytes()).ok()
        }
        Value::Bytes(b) => crate::engine::vector::codec::coerce_value(
            Value::Bytes(b.clone()),
            crate::vector::VectorDataType::Float32,
            None,
        )
        .ok(),
        Value::SmallString(_) => {
            crate::engine::vector::coerce_bytes_to_internal(value.to_display_string().as_bytes())
                .ok()
        }
        _ => None,
    }
}

fn query_vector_binding(
    table: &PlanTable,
    expr: &Expr,
) -> Option<(crate::vector::VectorValue, Option<Expr>)> {
    let has_placeholder = expr_has_placeholder(expr) || !expr_is_deterministic(expr);
    let baked = crate::engine::expr::eval_expr_legacy(expr, &table.schema, &[])
        .ok()
        .and_then(|value| value_to_vector_bytes(&value))
        .unwrap_or_default();
    if baked.is_empty() && !has_placeholder {
        return None;
    }
    Some((
        baked,
        if has_placeholder {
            Some(expr.clone())
        } else {
            None
        },
    ))
}

/// Recognise an `ORDER BY DISTANCE(col, const[, metric]) [ASC] LIMIT k` KNN
/// query over an HNSW-indexed column and build a [`PhysicalPlan::VectorIndexScan`].
/// Returns `None` — falling back to a seq scan + full sort — when any
/// precondition fails: no concrete LIMIT, a descending or multi-key ORDER BY,
/// no matching HNSW index, a metric mismatch, or an unfoldable query vector.
/// M5.3 also returns `None` when the table is small enough that brute-force
/// is cheaper, when `/*+ NO_VECTOR_INDEX */` is set, or when an estimated
/// filter selectivity narrows the search to fewer rows than the brute-row
/// threshold. M5.5 routes a partitioned table through scatter-gather.
/// `filter` is the table's residual WHERE, re-checked per candidate row by
/// the executor.
fn try_knn_vector_scan(
    db: &Database,
    table: &PlanTable,
    select: &Select,
    filter: Option<Expr>,
    possible_keys: Vec<String>,
) -> Option<PhysicalPlan> {
    // A scan-level TopK cannot cross operators that change row multiplicity
    // or need the complete input. Keep their final relational TopK intact.
    if select.from.len() > 1
        || !select.joins.is_empty()
        || select.distinct
        || !select.group_by.is_empty()
        || !select.grouping_sets.is_empty()
        || has_aggregate_select_items(&select.projection)
        || select.having.is_some()
        || has_window_in_projection(&select.projection)
        || select
            .order_by
            .iter()
            .any(|order| expr_has_window_function(&order.expr))
    {
        return None;
    }
    // M5.5 — partitioned tables run scatter-gather over per-partition HNSW
    // graphs at execution time. The planner builds the same VectorIndexScan
    // node; the executor (see `execute_vector_index_scan`) peeks
    // `table.is_partitioned()` and fans out across `table.storage_ids()`.
    // KNN needs a concrete `k`; without LIMIT, "nearest" is the whole table and
    // an ordinary full sort is the correct plan. A placeholder LIMIT peeks the
    // bound parameter (auto-param / prepared execute); the stamped `binding`
    // re-resolves `k` at execution so a cached plan stays correct across
    // different bound values.
    let limit = limit_usize(select.limit.as_ref())?;
    let offset = limit_usize(select.offset.as_ref()).unwrap_or(0);
    let k = offset.checked_add(limit).filter(|k| *k > 0)?;
    // Exactly one sort key; a tie-breaker would impose an order the index cannot
    // honour. Nearest-first distance is ASC. DOT_PRODUCT is a similarity, so
    // its nearest-neighbour form is DESC.
    if select.order_by.len() != 1 {
        return None;
    }
    let order_expr = resolve_order_alias(&select.order_by[0].expr, &select.projection);
    let matched = if select.order_by[0].asc {
        match_distance_call(order_expr)
    } else if let Some(dot) = match_dot_similarity_call(order_expr) {
        Some(dot)
    } else {
        if match_distance_call(order_expr).is_some() {
            crate::engine::api::push_statement_warning(
                crate::error::ConditionLevel::Warning,
                 crate::error::catalog::ER_WARN_DEPRECATED_SYNTAX,
                "descending vector distance requests farthest-first order and cannot use a nearest-neighbour vector index; falling back to a full sort",
            );
        }
        None
    };
    let (col, query_const, metric) = matched?;
    // Fold the constant operand to internal bytes. `&[]` is safe — the matcher
    // confirmed it carries no column reference. If it contains a placeholder,
    // keep the expression for execution-time resolution so prepared KNN plans
    // stay reusable across different query vectors.
    let (mut query_vector, query_vector_expr) = query_vector_binding(table, query_const)?;
    let binary_column = table
        .schema
        .column_index(col)
        .ok()
        .and_then(|i| table.schema.columns.get(i))
        .is_some_and(|c| {
            matches!(
                c.data_type,
                crate::storage::schema::DataTypeKind::Vector {
                    datatype: crate::vector::VectorDataType::Binary,
                    ..
                }
            )
        });
    let metric = if binary_column
        && matches!(order_expr, Expr::Function {name,args,..} if matches!(name.to_ascii_uppercase().as_str(),"DISTANCE"|"VEC_DISTANCE") && args.len()==2)
    {
        crate::engine::vector::Metric::Hamming
    } else {
        metric
    };
    if binary_column && !query_vector.is_empty() {
        if let Ok(value) = crate::engine::expr::eval_expr_legacy(query_const, &table.schema, &[]) {
            if !matches!(value, Value::Vector(_)) {
                query_vector = crate::engine::vector::codec::coerce_value(
                    value,
                    crate::vector::VectorDataType::Binary,
                    None,
                )
                .ok()?;
            }
        }
    }

    // M5.1/M5.3 — session knobs (defaults when no session).
    let hnsw_cfg = crate::engine::vector::session::current_config();
    let mut total_rows = table_row_estimate(db, table).ok()?;
    // A restart clears the in-memory modification sidecar. When catalog
    // statistics are missing, treating that zero as an actually empty/small
    // table makes the first KNN silently choose the brute-force heap scan and
    // never open its durable paged HNSW index. Recover a conservative O(1)
    // estimate from the heap file's physical page count instead. This reads
    // only file metadata (not heap rows), keeps genuinely small tables on the
    // exact path, and lets large cold indexes reach their persistent `.hvec`
    // graph without an O(n) first-query scan.
    if total_rows <= 0.0 {
        let heap_pages = if table.schema.is_partitioned() {
            table
                .schema
                .storage_ids()
                .into_iter()
                .filter_map(|storage_id| db.heap_page_count_for_storage(storage_id).ok())
                .sum::<u64>()
        } else {
            let qualified = db.storage_key_for_table(&table.schema);
            db.heap_page_count_by_name(&qualified).unwrap_or(0)
        };
        if heap_pages > 0 {
            let width =
                crate::engine::cbo::operator_cost::estimated_row_width(&table.schema).max(8.0);
            total_rows = ((heap_pages as f64 * crate::storage::PAGE_SIZE as f64 * 0.85)
                / (width * 1.25))
                .floor()
                .max(1.0);
        }
    }
    // ANN graphs are current-state accelerators. Historical KNN must use the
    // exact heap/undo oracle; retaining graph topology across row versions
    // would otherwise introduce false negatives.
    let no_vector_index_hint = table.system_time.is_some()
        || select
            .query_hints
            .iter()
            .any(|h| matches!(h, crate::parser::QueryHint::NoVectorIndex));

    // Is the vector index path usable for this query? It needs a single-column
    // vector index (HNSW or IVF) on `col` whose build-time metric matches,
    // no explicit `/*+ NO_VECTOR_INDEX */`, and a table/filter not small enough
    // that brute-force wins. Any "no" routes to the brute-force exact path below
    // (or, if that's unavailable, to the generic seq-scan + sort).
    // A column may have several vector indexes (different metrics and/or
    // algorithms).  Selecting the first index before checking its metric made
    // a later compatible index invisible to the optimizer.
    let hnsw_hint = select.query_hints.iter().find_map(|h| match h {
        crate::parser::QueryHint::HnswEf { ef } if *ef > 0 => Some(*ef),
        _ => None,
    });
    let ivf_hint = select.query_hints.iter().find_map(|h| match h {
        crate::parser::QueryHint::IvfNprobe { nprobe } if *nprobe > 0 => Some(*nprobe),
        _ => None,
    });
    if hnsw_hint.is_some() && ivf_hint.is_some() {
        crate::engine::api::push_statement_warning(
            crate::error::ConditionLevel::Warning,
             crate::error::catalog::ER_WARN_DEPRECATED_SYNTAX,
            "HNSW_EF and IVF_NPROBE cannot pin the same KNN scan simultaneously; choosing the lower-cost compatible index",
        );
    }
    let dim = table
        .schema
        .column_index(col)
        .ok()
        .and_then(|i| table.schema.columns.get(i))
        .and_then(|c| match &c.data_type {
            crate::storage::schema::DataTypeKind::Vector { dim, .. } => *dim,
            _ => None,
        })
        .map(f64::from)
        .unwrap_or_else(|| (query_vector.dimension as usize).max(1) as f64);
    let lane_cost = (dim / 8.0).max(1.0);
    let index_cost = |index: &&IndexSchema| match index.vector_algo() {
        crate::storage::schema::VectorAlgo::Hnsw => {
            let ef = crate::engine::vector::session::effective_ef_search(
                &hnsw_cfg,
                hnsw_hint,
                index.vector_meta().default_ef_search.map(u64::from),
                k,
            ) as f64;
            let fanout = index.vector_meta().m as f64 + total_rows.max(2.0).ln();
            ef * fanout * lane_cost
        }
        crate::storage::schema::VectorAlgo::Ivf => {
            let nlist = index
                .vector_meta()
                .nlist
                .map(f64::from)
                .unwrap_or_else(|| total_rows.max(1.0).sqrt())
                .max(1.0);
            let nprobe = crate::engine::vector::session::effective_nprobe(
                &hnsw_cfg,
                ivf_hint,
                index.vector_meta().default_nprobe.map(u64::from),
            ) as f64;
            let centroid_cost = nlist * lane_cost;
            let list_scan_cost = total_rows * (nprobe / nlist).min(1.0) * (dim / 16.0).max(1.0);
            let rerank_cost = (k.saturating_mul(4).max(k) as f64) * 4.0;
            centroid_cost + list_scan_cost + rerank_cost
        }
    };
    let index = vector_indexes_for_column(&table.schema, col, metric)
        .filter(|index| {
            let index_hints = select
                .from
                .first()
                .map(|t| t.index_hints.as_slice())
                .unwrap_or_default();
            let use_indexes = query_hint_use_index(
                &select.query_hints,
                table.alias.as_deref().unwrap_or(&table.schema.name),
                &table.schema.name,
            );
            index_allowed_by_hints(&index.name, index_hints, IndexHintScope::Join)
                && index_allowed_by_hints(&index.name, index_hints, IndexHintScope::OrderBy)
                && (use_indexes.is_empty()
                    || use_indexes
                        .iter()
                        .any(|name| index.name.eq_ignore_ascii_case(name)))
                && !query_hint_no_index(
                    &select.query_hints,
                    table.alias.as_deref().unwrap_or(&table.schema.name),
                    &table.schema.name,
                )
                .iter()
                .any(|n| index.name.eq_ignore_ascii_case(n))
        })
        .filter(|index| {
            if ivf_hint.is_some() && hnsw_hint.is_none() {
                index.vector_algo() != crate::storage::schema::VectorAlgo::Hnsw
            } else if hnsw_hint.is_some() && ivf_hint.is_none() {
                index.vector_algo() == crate::storage::schema::VectorAlgo::Hnsw
            } else {
                true
            }
        })
        .min_by(|a, b| index_cost(a).total_cmp(&index_cost(b)));

    // Resolve the per-query search-effort hint appropriate to the index's
    // algorithm: `/*+ HNSW_EF(n) */` for an HNSW index, `/*+ IVF_NPROBE(n) */`
    // for an IVF index. Either one pins the index (skips the brute gates)
    // and is baked into the plan node's `ef_hint` field.
    let hint_ef = match index.map(|i| i.vector_algo()) {
        Some(crate::storage::schema::VectorAlgo::Ivf) => ivf_hint,
        _ => hnsw_hint,
    };
    if index.is_none() && (hnsw_hint.is_some() || ivf_hint.is_some()) {
        crate::engine::api::push_statement_warning(
            crate::error::ConditionLevel::Warning,
             crate::error::catalog::ER_WARN_DEPRECATED_SYNTAX,
            "the requested algorithm-specific vector hint has no compatible index; falling back to an exact scan",
        );
    }
    // Surface (1287) a metric mismatch: a vector index (HNSW or IVF) exists
    // on this column but its build-time metric differs from the query's, so we
    // silently fall back to a non-indexed scan — a performance cliff worth
    // telling the user about.
    if index.is_none() {
        if let Some(idx) = table.schema.iter_queryable_indexes().find(|idx| {
            idx.is_vector() && idx.columns.len() == 1 && idx.columns[0].eq_ignore_ascii_case(col)
        }) {
            let idx_metric = idx.vector_meta_for_table(&table.schema).metric_enum();
            if !no_vector_index_hint && idx_metric != metric {
                crate::engine::api::push_statement_warning(
                    crate::error::ConditionLevel::Warning,
                    crate::error::catalog::ER_WARN_DEPRECATED_SYNTAX,
                    format!(
                        "vector index `{}` metric={} does not match query metric={}; \
                     falling back to a non-indexed scan",
                        idx.name,
                        idx_metric.as_str(),
                        metric.as_str()
                    ),
                );
            }
        }
    }
    let index_usable = index.is_some_and(|index| {
        if no_vector_index_hint
            || index.vector_meta_for_table(&table.schema).metric_enum() != metric
        {
            return false;
        }
        // Skip the small-table / selective-filter brute gates when the user
        // pinned a HNSW_EF — that's an explicit ask for the index.
        if hint_ef.is_none()
            && hnsw_cfg.brute_threshold > 0
            && (total_rows as u64) < hnsw_cfg.brute_threshold
        {
            return false;
        }
        if hint_ef.is_none() && hnsw_cfg.brute_filter_rows > 0 {
            if let Some(predicate) = filter.as_ref() {
                let est = estimate_filter_rows(&table.schema, predicate, total_rows);
                if (est as u64) < hnsw_cfg.brute_filter_rows {
                    return false;
                }
            }
        }
        // Cost crossover beyond the fixed row thresholds. Both costs are in
        // approximate SIMD distance-lane units; this deliberately favors the
        // exact scan for very large k / broad IVF probing, where ANN would do
        // more distance work plus random graph/page accesses.
        if hint_ef.is_none() && hnsw_cfg.brute_threshold > 0 {
            let dim = table
                .schema
                .column_index(col)
                .ok()
                .and_then(|i| table.schema.columns.get(i))
                .and_then(|c| match &c.data_type {
                    crate::storage::schema::DataTypeKind::Vector { dim, .. } => *dim,
                    _ => None,
                })
                .map(|d| d as f64)
                .unwrap_or_else(|| (query_vector.dimension as usize).max(1) as f64);
            let scan_rows = filter
                .as_ref()
                .map(|p| estimate_filter_rows(&table.schema, p, total_rows))
                .unwrap_or(total_rows)
                .max(1.0);
            let brute_cost = scan_rows * (dim / 8.0).max(1.0);
            let index_cost = match index.vector_algo() {
                crate::storage::schema::VectorAlgo::Hnsw => {
                    let ef = crate::engine::vector::session::effective_ef_search(
                        &hnsw_cfg,
                        None,
                        index.vector_meta().default_ef_search.map(u64::from),
                        k,
                    ) as f64;
                    let fanout = index.vector_meta().m as f64 + total_rows.max(2.0).ln();
                    ef * fanout * (dim / 8.0).max(1.0)
                }
                crate::storage::schema::VectorAlgo::Ivf => {
                    let nprobe = crate::engine::vector::session::effective_nprobe(
                        &hnsw_cfg,
                        None,
                        index.vector_meta().default_nprobe.map(u64::from),
                    ) as f64;
                    let nlist = index
                        .vector_meta()
                        .nlist
                        .map(f64::from)
                        .unwrap_or_else(|| total_rows.max(1.0).sqrt())
                        .max(1.0);
                    total_rows * (nprobe / nlist).min(1.0) * (dim / 8.0).max(1.0)
                }
            };
            if brute_cost < index_cost {
                return false;
            }
        }
        true
    });

    if index_usable {
        let index = index.expect("index_usable implies Some");
        // Bake the algorithm-appropriate search effort into `ef_search`: an ef
        // candidate-pool size for HNSW, an nprobe cell count for IVF. The
        // executor re-resolves this from the live session at run time (the plan
        // cache key excludes the knob), so the baked value is only a fallback
        // for session-less internal callers.
        let ef_search = match index.vector_algo() {
            crate::storage::schema::VectorAlgo::Ivf => {
                let index_default = index.vector_meta().default_nprobe.map(|v| v as u64);
                crate::engine::vector::session::effective_nprobe(&hnsw_cfg, hint_ef, index_default)
            }
            crate::storage::schema::VectorAlgo::Hnsw => {
                let index_default_ef = index.vector_meta().default_ef_search.map(|v| v as u64);
                crate::engine::vector::session::effective_ef_search(
                    &hnsw_cfg,
                    hint_ef,
                    index_default_ef,
                    k,
                )
            }
        };
        let estimated_rows = (k as f64).min(total_rows.max(1.0));
        // `access_rows` seeds the plan `cost`, which downstream CBO/parallelism
        // heuristics compare — so it stays the conservative candidate-pool
        // estimate for *both* algorithms to keep plan selection stable.
        let access_rows = (ef_search as f64).min(total_rows.max(1.0));
        // Separately, the `rows_examined_per_scan` figure EXPLAIN *displays*
        // for IVF should reflect that `ef_search` is `nprobe` (a *cell* count),
        // so the rows actually scanned are the probed fraction of the table:
        // `nprobe/nlist · N` (`nlist` from the index's `NLIST=` or the auto √N
        // default). Reporting the bare nprobe (≈8) understates it badly (P4).
        // This display-only value never feeds the cost, so it can't perturb
        // planning.
        let display_rows = match index.vector_algo() {
            crate::storage::schema::VectorAlgo::Ivf => {
                let nlist = index
                    .vector_meta()
                    .nlist
                    .map(|v| v as f64)
                    .unwrap_or_else(|| total_rows.max(1.0).sqrt())
                    .max(1.0);
                let nprobe = (ef_search as f64).min(nlist);
                ((nprobe / nlist) * total_rows).clamp(estimated_rows, total_rows.max(1.0))
            }
            crate::storage::schema::VectorAlgo::Hnsw => access_rows,
        };
        // M5.5 — partitioned tables are handled by the executor by scattering
        // the search across the routed partition storage ids and merging the
        // per-partition candidate lists. The plan node itself stays the same;
        // dispatch peeks `table.schema.is_partitioned()` to fan out.
        return Some(PhysicalPlan::VectorIndexScan {
            table: table.clone(),
            index: index.clone(),
            query_vector,
            query_vector_expr: query_vector_expr.clone(),
            metric: metric.as_str().to_string(),
            k,
            baked_offset: offset,
            binding: limit_binding_of(select),
            ef_search,
            ef_hint: hint_ef,
            order_by: select.order_by.clone(),
            filter,
            estimated_rows,
            access_rows: display_rows,
            cost: crate::engine::cbo::Cost::scalar(access_rows),
            possible_keys,
            hint_note: None,
            projected_columns: None,
        });
    }

    try_brute_vector_scan(
        table,
        select,
        filter,
        possible_keys,
        col,
        query_vector,
        query_vector_expr,
        metric,
        k,
        total_rows,
    )
}

/// Build a [`PhysicalPlan::VectorBruteScan`] for the no-index `ORDER BY
/// DISTANCE(col, const) LIMIT k` shape, or `None` to fall through to the
/// generic seq-scan + sort. Emitted only when:
///   * brute-force scanning isn't disabled (`MYDB_VECTOR_BRUTE_SCAN`),
///   * there's **no** residual WHERE — the filtered case stays on the row
///     pipeline (it must decode filter columns anyway, so the zero-copy win
///     is moot and the generic path stays the single source of truth), and
///   * `col` is a fixed-dim `VECTOR(n)` whose width matches the query vector.
///
/// The executor's [`Database::vector_brute_knn`] scores vectors in place off
/// the heap and only materialises the K winners, so this beats the row
/// pipeline (which heap-copies every row's vector into a `Value::Vector`) by
/// a wide margin while staying exact.
#[allow(clippy::too_many_arguments)]
fn try_brute_vector_scan(
    table: &PlanTable,
    select: &Select,
    filter: Option<Expr>,
    possible_keys: Vec<String>,
    col: &str,
    query_vector: crate::vector::VectorValue,
    query_vector_expr: Option<Expr>,
    metric: crate::engine::vector::Metric,
    k: usize,
    total_rows: f64,
) -> Option<PhysicalPlan> {
    if !vector_brute_scan_enabled() || filter.is_some() {
        return None;
    }
    // Partitioned tables are supported: the storage-layer scan fans out across
    // partitions and the bounded top-k now carries each winner's `storage_id`
    // (RowIds are only unique within a partition), which the executor uses to
    // route the row read to the right partition.
    let col_idx = table.schema.column_index(col).ok()?;
    let dim = match table.schema.columns.get(col_idx).map(|c| &c.data_type) {
        Some(crate::storage::schema::DataTypeKind::Vector { dim: Some(d), .. }) => *d as usize,
        // Dimensionless `VECTOR` (variable-length LOB) and non-vector columns
        // have no fixed in-row byte slice to score zero-copy — let the generic
        // path handle (or correctly error) those.
        _ => return None,
    };
    if dim == 0 || (query_vector_expr.is_none() && dim != query_vector.dimension as usize) {
        return None;
    }
    let estimated_rows = (k as f64).min(total_rows.max(1.0));
    let access_rows = total_rows.max(1.0);
    Some(PhysicalPlan::VectorBruteScan {
        table: table.clone(),
        column_index: col_idx,
        query_vector,
        query_vector_expr,
        metric: metric.as_str().to_string(),
        k,
        baked_offset: limit_usize(select.offset.as_ref()).unwrap_or(0),
        binding: limit_binding_of(select),
        order_by: select.order_by.clone(),
        estimated_rows,
        access_rows,
        cost: crate::engine::cbo::Cost::scalar(access_rows),
        possible_keys,
    })
}

/// Whether the brute-force `VectorBruteScan` operator is enabled. Reads
/// `MYDB_VECTOR_BRUTE_SCAN` (default on); set to `0`/`off`/`false` to force
/// the legacy generic seq-scan + sort path (the differential oracle).
fn vector_brute_scan_enabled() -> bool {
    let resolve = || {
        crate::util::env::var("MYDB_VECTOR_BRUTE_SCAN")
            .map(|v| !matches!(v.as_str(), "0" | "off" | "OFF" | "false" | "FALSE"))
            .unwrap_or(true)
    };
    #[cfg(not(test))]
    {
        static ENABLED: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
        *ENABLED.get_or_init(resolve)
    }
    #[cfg(test)]
    resolve()
}

fn choose_scan_for_source(
    db: &Database,
    source: &TableRef,
    table: &PlanTable,
    filter: Option<Expr>,
    hints: &ScanHints<'_>,
) -> Result<PhysicalPlan, QueryError> {
    if let Some(spec) = source.generate_series.as_deref() {
        let mut scan = plan_generate_series(source, spec)?;
        if let PhysicalPlan::GenerateSeries {
            filter: scan_filter,
            estimated_rows,
            cost,
            ..
        } = &mut scan
        {
            if let Some(predicate) = filter {
                *scan_filter = Some(predicate);
                *estimated_rows = (*estimated_rows * 0.25).max(1.0);
                *cost = crate::engine::cbo::Cost::scalar(*estimated_rows);
            }
        }
        return Ok(scan);
    }
    if let Some(scan) = try_plan_system_scan(source, filter.clone()) {
        return Ok(scan);
    }
    if let Some(mut scan) = try_plan_cte_scan(source) {
        if let PhysicalPlan::CteScan {
            filter: scan_filter,
            estimated_rows,
            cost,
            ..
        } = &mut scan
        {
            if let Some(predicate) = filter {
                let total_rows = (*estimated_rows).max(1.0);
                let output_rows = estimate_filter_rows(&table.schema, &predicate, total_rows);
                *scan_filter = Some(predicate);
                *estimated_rows = output_rows;
                *cost = crate::engine::cbo::Cost::scalar(total_rows);
            }
        }
        return Ok(scan);
    }
    choose_scan(db, table, filter, hints)
}

fn choose_scan(
    db: &Database,
    table: &PlanTable,
    filter: Option<Expr>,
    hints: &ScanHints<'_>,
) -> Result<PhysicalPlan, QueryError> {
    use crate::engine::cbo::operator_cost;
    let partition_pruning = build_partition_pruning_plan(&table.schema, filter.as_ref());
    // Non-partitioned tables (and predicates that retain the existing plan)
    // can borrow the caller's PlanTable. Clone only when pruning metadata
    // actually changes; the winning physical operator will take the one
    // ownership clone it needs later.
    let planned_table = (partition_pruning != table.partition_pruning).then(|| {
        let mut planned = table.clone();
        planned.partition_pruning = partition_pruning;
        planned
    });
    let table = planned_table.as_ref().unwrap_or(table);
    let ctx = crate::engine::cbo::CboContext::from_db(db);
    let possible_keys = table
        .schema
        .iter_queryable_indexes()
        .filter(|index| {
            let Some(first) = index.columns.first() else {
                return false;
            };
            let mut relevant = false;
            for predicate in filter
                .iter()
                .chain(hints.fulltext_matches.iter())
                .chain(hints.join_predicates.iter())
            {
                walk_expr_for_idents(predicate, &mut |node| {
                    relevant |= expr_matches_table_column(node, table, first);
                    if let Some(ast) = index
                        .key_expressions
                        .first()
                        .and_then(Option::as_ref)
                        .and_then(|e| e.ast.as_ref())
                    {
                        relevant |= expr_semantically_eq(node, ast);
                    }
                });
            }
            relevant
        })
        .map(|index| index.name.clone())
        .collect::<Vec<_>>();
    let table_total_rows = table_row_estimate(db, table)?;
    let pruned_population = filter
        .as_ref()
        .and_then(|filter| static_partition_population(&table.schema, filter));
    let total_rows = pruned_population
        .map(|(rows, _)| rows)
        .unwrap_or(table_total_rows);
    let seq_rows = filter
        .as_ref()
        .map(|expr| estimate_filter_rows_with_subqueries(db, &table.schema, expr, total_rows))
        .unwrap_or(total_rows);
    let seq_access_rows = hints
        .row_goal
        .map(|goal| expected_rows_to_reach_goal(total_rows, seq_rows, goal))
        .unwrap_or(total_rows);
    let seq_output_rows = hints
        .row_goal
        .map(|goal| seq_rows.min(goal as f64))
        .unwrap_or(seq_rows);
    let heap_row_width = operator_cost::estimated_row_width(&table.schema);
    let row_width = hints
        .required_columns
        .as_ref()
        .and_then(|cols| projected_indices_for_referenced(table, cols))
        .map(|indices| projected_row_width(table, Some(indices.as_slice())))
        .unwrap_or(heap_row_width);
    // Heap I/O is paid at full-page granularity, but tuple decode CPU follows
    // the columns actually required by this query. Prefer ANALYZE's physical
    // page count; a projected width must never alter heap row density.
    let table_pages = if let Some((_, pages)) = pruned_population {
        pages
    } else if table.schema.stats.heap_page_count > 0 {
        table.schema.stats.heap_page_count as f64
    } else {
        operator_cost::estimated_heap_pages(total_rows, heap_row_width)
    };
    let seq_pages = if total_rows > 0.0 {
        table_pages * (seq_access_rows / total_rows).clamp(0.0, 1.0)
    } else {
        table_pages
    };
    let qual_cpu = filter.as_ref().map(qual_cost_per_row).unwrap_or(0.0);
    // Keep the incumbent as scalar planning data until it actually wins.
    // Materialising a SeqScan here deep-cloned the table schema and predicate
    // even when a cheap B-tree path was selected a few lines later.
    let mut seq_cost = operator_cost::seq_scan(
        &ctx,
        seq_pages,
        seq_access_rows,
        seq_output_rows,
        row_width,
        qual_cpu,
    );
    if table.system_time.is_some() && db.temporal_history_index_degraded() {
        return Ok(materialize_seq_scan(
            table,
            filter,
            seq_output_rows,
            seq_access_rows,
            seq_cost,
            possible_keys,
            None,
        ));
    }
    // FULL_SCAN fixes the access path before candidate enumeration. In
    // particular, a winning Zone Map must never replace the full scan's
    // cost or access-row population while only its node annotation is dropped.
    // Keep partition pruning and LIMIT row goals in the base scan estimate.
    if query_hint_full_scan(hints.query_hints, hints.table_name, &table.schema.name) {
        let mut plan = materialize_seq_scan(
            table,
            filter,
            seq_output_rows,
            seq_access_rows,
            seq_cost,
            possible_keys,
            None,
        );
        if let PhysicalPlan::SeqScan { hint_note, .. } = &mut plan {
            *hint_note = Some("FULL_SCAN".to_string());
        }
        return Ok(plan);
    }
    // Enumerate cheap, statistics-only candidates first. Their best complete
    // cost becomes an upper bound for expensive access methods below. This is
    // intentionally access-shape agnostic: non-unique equality, IN/multi-key,
    // range and covering B-tree candidates can all make a Zone Map expansion
    // unnecessary.
    let index_expr = filter.as_ref().or_else(|| hints.fulltext_matches.first());
    let index_choice = index_expr.and_then(|expr| {
        best_index_access(
            &ctx,
            &table.schema,
            expr,
            hints,
            total_rows,
            table_pages,
            row_width,
        )
    });
    let forced_index_merge =
        query_hint_index_merge(hints.query_hints, hints.table_name, &table.schema.name).is_some();
    let no_index_merge =
        query_hint_no_index_merge(hints.query_hints, hints.table_name, &table.schema.name);
    let (mut merge_union_allowed, mut merge_sort_union_allowed, mut merge_intersection_allowed) =
        index_merge_switches();
    if forced_index_merge {
        merge_union_allowed = true;
        merge_sort_union_allowed = true;
        merge_intersection_allowed = true;
    }
    if no_index_merge.as_ref().is_some_and(Vec::is_empty) {
        merge_union_allowed = false;
        merge_sort_union_allowed = false;
        merge_intersection_allowed = false;
    }
    let index_merge_choice = filter.as_ref().and_then(|expr| {
        build_index_merge_node(
            &ctx,
            &table.schema,
            expr,
            hints,
            total_rows,
            table_pages,
            row_width,
            merge_union_allowed,
            merge_sort_union_allowed,
            merge_intersection_allowed,
        )
        .filter(|candidate| candidate.node.leaf_count() >= 2)
    });
    let exact_unique_lookup = index_choice.as_ref().is_some_and(|(choice, _)| {
        choice.index.unique && matches!(choice.kind, IndexAccessKind::Lookup { exact: true, .. })
    });
    let mut selected_seq_access_rows = seq_access_rows;
    let mut selected_zone_map: Option<ZoneMapPlanInfo> = None;
    let zone_hint = query_hint_zone_map(hints.query_hints, hints.table_name, &table.schema.name);
    // Zone-map metadata can only price predicates the scan itself will own.
    // Subquery conjuncts are lowered to Apply after physical planning; using
    // them here used to advertise a small candidate range and then fall back
    // to a full heap scan at execution. Keep only independent top-level AND
    // conjuncts, exactly like the later scan/Apply split.
    let zone_predicate = filter.clone().and_then(|predicate| {
        let (ordinary, _) = partition_apply_conjuncts(predicate);
        combine_conjuncts(ordinary).map(|mut predicate| {
            bind_zone_map_cost_parameters(&mut predicate);
            predicate
        })
    });
    // ZoneMapScan is represented by the sequential scan leaf plus an access
    // annotation so every executor bridge continues to share the established
    // MVCC/row-predicate oracle.  The actual ranges are regenerated at
    // execution (important for prepared parameters and concurrent merges).
    if db.zone_map_enabled() && table.system_time.is_none() && zone_hint != Some(false) {
        if let Some(expr) = zone_predicate.as_ref() {
            let forced = zone_hint == Some(true);
            let mut frontier = AccessPathFrontier::new(seq_cost.weighted_total());
            if let Some((_, index_cost)) = index_choice.as_ref() {
                frontier.consider(*index_cost);
            }
            let estimate = if forced {
                db.zone_map_cost_details_forced(&table.schema, expr)
            } else if zone_map_sampling_cpu_floor_from_stats(&table.schema)
                .is_some_and(|floor| !frontier.should_expand(floor, false))
            {
                None
            } else if let Some(lower_bound) = db.zone_map_cost_lower_bound(&table.schema, expr) {
                let metadata_cpu = lower_bound.zones_examined as f64
                    * crate::engine::cbo::cbo_consts().cpu_operator_cost();
                frontier
                    .should_expand(metadata_cpu, false)
                    .then(|| db.zone_map_cost_details(&table.schema, expr))
                    .flatten()
            } else {
                None
            };
            if let Some(estimate) = estimate {
                let physical_surviving_pages = estimate.surviving_pages as f64;
                let physical_total_pages = estimate.total_pages as f64;
                let surviving_fraction = if physical_total_pages > 0.0 {
                    (physical_surviving_pages / physical_total_pages).clamp(0.0, 1.0)
                } else {
                    0.0
                };
                // The base scan uses statistics-derived heap pages while the
                // Zone Map walks real heap metadata.  Apply the physical
                // survival ratio to the base estimate so both alternatives
                // remain on the same scale.
                let costed_surviving_pages = seq_pages * surviving_fraction;
                if surviving_fraction < 1.0 || zone_hint == Some(true) {
                    let zone_consts = crate::engine::cbo::cbo_consts();
                    let range_jumps = estimate.ranges as f64 * zone_consts.rnd_page_cost();
                    // Borrowed summary evaluation touches logical records;
                    // a warm page is not another disk read per sampled zone.
                    let metadata_cpu = estimate.zones_examined as f64
                        * zone_consts.cpu_operator_cost()
                        + estimate.metadata_read_bytes as f64 * zone_consts.cpu_byte_decode_cost();
                    let metadata_io =
                        estimate.metadata_cache_misses as f64 * zone_consts.rnd_page_cost();
                    let startup_overhead = metadata_cpu + metadata_io + range_jumps;
                    let mut zone_cost = operator_cost::seq_scan(
                        &ctx,
                        costed_surviving_pages,
                        seq_access_rows * surviving_fraction,
                        seq_output_rows,
                        row_width,
                        qual_cpu,
                    );
                    // Candidate-range discovery and the first seek are paid
                    // before rows can flow, but they are also part of the
                    // total work.  Cost::weighted_total() and TREE EXPLAIN
                    // use `total`, so updating startup alone made a fully
                    // pruned scan incorrectly appear free.
                    zone_cost.startup += startup_overhead;
                    zone_cost.total += startup_overhead;
                    zone_cost.cpu_units += metadata_cpu;
                    zone_cost.io_rnd_pages +=
                        estimate.ranges as f64 + estimate.metadata_cache_misses as f64;
                    if zone_hint == Some(true)
                        || zone_cost.weighted_total() < seq_cost.weighted_total()
                    {
                        seq_cost = zone_cost;
                        selected_seq_access_rows *= surviving_fraction;
                        selected_zone_map = Some(ZoneMapPlanInfo {
                            surviving_pages: estimate.surviving_pages,
                            total_pages: estimate.total_pages,
                            ranges: estimate.ranges as u64,
                            zones_examined: estimate.zones_examined,
                            metadata_read_bytes: estimate.metadata_read_bytes,
                            metadata_cache_misses: estimate.metadata_cache_misses,
                            storage_objects: estimate.storage_objects,
                            estimate_exact: estimate.exact,
                        });
                    }
                }
            } else if zone_hint == Some(true) {
                push_hint_warning(format!(
                    "ZONE_MAP({}): predicate or metadata cannot safely produce zone ranges",
                    hints.table_name
                ));
            }
        } else if zone_hint == Some(true) {
            push_hint_warning(format!(
                "ZONE_MAP({}): requires a supported filter predicate",
                hints.table_name
            ));
        }
    }
    if selected_zone_map.is_some() {
        if filter.as_ref().is_some_and(expr_has_positive_like) {
            crate::engine::status_counters::record_like_zone_selected();
        }
    }
    if zone_hint == Some(true) && selected_zone_map.is_some() {
        return Ok(materialize_seq_scan(
            table,
            filter.clone(),
            seq_output_rows,
            selected_seq_access_rows,
            seq_cost,
            possible_keys.clone(),
            selected_zone_map,
        ));
    }
    let has_query_use_index =
        !query_hint_use_index(hints.query_hints, hints.table_name, &table.schema.name).is_empty();
    // INDEX_FFS hint: short-circuit to a fast full scan over the named
    // (or narrowest covering) index when the hint applies to this
    // table. Falls through silently if no index can cover the projected
    // / filter columns, mirroring USE INDEX semantics.
    if table.system_time.is_none() {
        if let Some(index_arg) =
            query_hint_index_ffs(hints.query_hints, hints.table_name, &table.schema.name)
        {
            if let Some(plan) = build_index_ffs_plan(
                &ctx,
                table,
                &hints,
                total_rows,
                &possible_keys,
                filter.clone(),
                index_arg,
            ) {
                return Ok(plan);
            }
        }
    }
    if filter.is_none() && hints.fulltext_matches.is_empty() {
        if table.system_time.is_some() {
            return Ok(set_scan_query_hint_note(
                materialize_seq_scan(
                    table,
                    filter.clone(),
                    seq_output_rows,
                    selected_seq_access_rows,
                    seq_cost,
                    possible_keys.clone(),
                    None,
                ),
                hints,
            ));
        }
        if let Some(plan) = choose_orderby_index_scan(
            &ctx,
            table,
            &hints,
            total_rows,
            total_rows,
            None,
            &possible_keys,
        ) {
            return Ok(set_scan_query_hint_note(plan, hints));
        }
        if let Some(plan) =
            choose_groupby_index_scan(&ctx, table, &hints, total_rows, &possible_keys)
        {
            return Ok(set_scan_query_hint_note(plan, hints));
        }
        if let Some(plan) =
            choose_covering_index_scan(&ctx, table, &hints, total_rows, &possible_keys)
        {
            let seq_compare_cost = projected_seq_scan_cost(
                &ctx,
                table,
                hints.required_columns.as_ref(),
                total_rows,
                seq_rows,
                table_pages,
                qual_cpu,
            );
            if has_query_use_index
                || total_rows <= 1_000.0
                || plan_cost(&plan).weighted_total() < seq_compare_cost.weighted_total()
            {
                return Ok(set_scan_query_hint_note(plan, hints));
            }
        }
        return Ok(set_scan_query_hint_note(
            materialize_seq_scan(
                table,
                filter.clone(),
                seq_output_rows,
                selected_seq_access_rows,
                seq_cost,
                possible_keys.clone(),
                selected_zone_map,
            ),
            hints,
        ));
    }
    let expr = filter
        .as_ref()
        .or_else(|| hints.fulltext_matches.first())
        .expect("filter or fulltext scalar context");
    // Keep diagnostics independent from the borrowed expression because the
    // selected physical-plan branch below consumes `filter`.
    let has_positive_like = expr_has_positive_like(expr);
    // FULLTEXT scalar context. Every distinct MATCH is planned independently;
    // only positive top-level AND conjuncts constrain the base candidate set.
    // Projection/ORDER BY-only MATCH expressions are included through
    // ScanHints and therefore score every visible row (non-hits read 0.0).
    let mut match_exprs: Vec<&Expr> = hints.fulltext_matches.iter().collect();
    if match_exprs.is_empty() {
        if let Some(found) = find_fulltext_match(expr) {
            match_exprs.push(found);
        }
    }
    if !match_exprs.is_empty() {
        let mut seen = std::collections::HashSet::new();
        let mut specs = Vec::new();
        for match_expr in match_exprs {
            let match_fingerprint = crate::engine::expr_program::expr_fingerprint(match_expr);
            if !seen.insert(match_fingerprint) {
                continue;
            }
            let Some((mode, query, query_param, match_columns)) = fulltext_match_parts(match_expr)
            else {
                return Err(error(
                    crate::error::catalog::ER_NOT_SUPPORTED_YET,
                    "unsupported MATCH/AGAINST expression shape",
                ));
            };
            let Some(ft_index) = find_matching_fulltext_index(&table.schema, &match_columns)
                .filter(|index| special_index_allowed(index, &table.schema, hints))
            else {
                return Err(error(
                    crate::error::catalog::ER_FT_MATCHING_KEY_NOT_FOUND,
                    "Can't find FULLTEXT index matching the column list",
                ));
            };
            specs.push(FulltextMatchPlan {
                index: ft_index.clone(),
                mode,
                query,
                query_param,
                columns: ft_index.columns.clone(),
                match_fingerprint,
                restrict_to_matches: filter.as_ref().is_some_and(|predicate| {
                    is_positive_fulltext_conjunct(predicate, match_fingerprint)
                }),
                needs_score: true,
            });
        }
        if !specs.is_empty() {
            // Drive from a restricting MATCH when one exists; this makes the
            // primary posting set selective while additional restricting
            // MATCH sets are intersected by the executor.
            if let Some(pos) = specs.iter().position(|spec| spec.restrict_to_matches) {
                specs.swap(0, pos);
            }
            let mut residual = filter.clone();
            for spec in specs.iter().filter(|spec| spec.restrict_to_matches) {
                residual = strip_bare_fulltext_predicate(residual, spec.match_fingerprint);
            }
            let fulltext_access_rows =
                estimate_fulltext_access_rows(&table.schema, &specs, total_rows);
            // MATCH does not preclude an ordinary conjunct from providing a
            // cheaper base row-id set. Keep the MATCH posting/score maps, but
            // intersect them with a sufficiently selective B-tree access.
            let mut base_candidates = Vec::new();
            if let Some((access, rows)) = index_choice
                .clone()
                .map(|(choice, _)| cost_fulltext_base_access(&table.schema, choice, total_rows))
                .filter(|(_, rows)| *rows < total_rows && *rows <= fulltext_access_rows.max(1.0))
            {
                base_candidates.push((access, rows, "FULLTEXT + B-tree candidates".to_string()));
            }
            if !table.schema.is_partitioned() && selected_zone_map.is_some() {
                if let Some(zone_filter) = filter.as_ref() {
                    if selected_seq_access_rows < total_rows
                        && selected_seq_access_rows <= fulltext_access_rows.max(1.0)
                    {
                        base_candidates.push((
                            FulltextBaseAccess::ZoneMap {
                                filter: zone_filter.clone(),
                            },
                            selected_seq_access_rows,
                            "FULLTEXT + Zone Map candidates".to_string(),
                        ));
                    }
                }
            }
            let base_access = base_candidates
                .into_iter()
                .min_by(|left, right| left.1.total_cmp(&right.1));
            let access_rows = base_access
                .as_ref()
                .map(|(_, rows, _)| rows.min(fulltext_access_rows))
                .unwrap_or(fulltext_access_rows);
            let estimated_rows = residual
                .as_ref()
                .map(|e| estimate_filter_rows(&table.schema, e, access_rows))
                .unwrap_or(access_rows);
            let (base_access, base_note) = match base_access {
                Some((access, _, note)) => (Some(access), Some(note)),
                None => (None, None),
            };
            let primary = specs.remove(0);
            return Ok(PhysicalPlan::FulltextScan {
                table: table.clone(),
                index: primary.index,
                mode: primary.mode,
                query: primary.query,
                query_param: primary.query_param,
                columns: primary.columns,
                match_fingerprint: primary.match_fingerprint,
                filter: residual,
                estimated_rows,
                access_rows,
                cost: crate::engine::cbo::Cost::scalar(access_rows),
                possible_keys,
                hint_note: base_note,
                projected_columns: None,
                restrict_to_matches: primary.restrict_to_matches,
                additional_matches: specs,
                like_prefilter: None,
                base_access,
                needs_score: true,
            });
        }
    }
    // SPATIAL search: a spatial predicate over a SPATIAL-indexed geometry
    // column with a constant query geometry is driven by the R-tree. The
    // residual `filter` is the full WHERE — re-checked per candidate row so
    // the R-tree's MBR superset is filtered down to the exact answer.
    if let Some((sp_index, query_probe)) = find_spatial_index_scan(&table.schema, expr, hints) {
        let access_rows = (total_rows * 0.3).clamp(1.0, total_rows.max(1.0));
        let estimated_rows = estimate_filter_rows(&table.schema, expr, access_rows);
        return Ok(PhysicalPlan::SpatialScan {
            table: table.clone(),
            index: sp_index.clone(),
            query_probe,
            filter: filter.clone(),
            estimated_rows,
            access_rows,
            cost: crate::engine::cbo::Cost::scalar(access_rows),
            possible_keys: possible_keys.clone(),
            hint_note: None,
            projected_columns: None,
        });
    }
    if let Some((ngram_index, like_prefilter)) =
        find_like_ngram_prefilter(&table.schema, expr, hints)
    {
        crate::engine::status_counters::record_like_ngram_selected();
        let access_rows = estimate_ngram_prefilter_rows(
            &table.schema,
            &ngram_index,
            &like_prefilter.tokens,
            total_rows,
        );
        let estimated_rows = estimate_filter_rows(&table.schema, expr, access_rows);
        return Ok(PhysicalPlan::FulltextScan {
            table: table.clone(),
            index: ngram_index.clone(),
            mode: crate::parser::FtMode::NaturalLanguage,
            query: String::new(),
            query_param: None,
            columns: ngram_index.columns.clone(),
            match_fingerprint: 0,
            filter: filter.clone(),
            estimated_rows,
            access_rows,
            cost: crate::engine::cbo::Cost::scalar(access_rows),
            possible_keys,
            hint_note: Some("LIKE ngram prefilter".to_string()),
            projected_columns: None,
            restrict_to_matches: true,
            additional_matches: Vec::new(),
            like_prefilter: Some(like_prefilter),
            base_access: None,
            needs_score: false,
        });
    }
    if !exact_unique_lookup {
        if let Some(choice) = best_in_subquery_index_access(
            db,
            &ctx,
            table,
            expr,
            filter.clone(),
            hints,
            total_rows,
            table_pages,
            row_width,
        ) {
            let access = choice.access;
            // A unique-key IN-subquery often shrinks the rows that
            // flow into later joins by far more than the single-table
            // access-path comparison can see. Keep that keyset-driven
            // lookup when it is still materially selective; otherwise
            // fall back to the cheaper full scan.
            let preserves_selectivity =
                access.index.unique && choice.access_rows <= total_rows * 0.25;
            // Compare every substantial heap access against the same planned
            // parallel scan alternative used by ordinary ranges. A fixed
            // selectivity gate can keep an expensive 29% lookup while
            // rejecting an otherwise identical 30% lookup. Tiny-table lookup
            // startup and explicit index hints retain their existing policy.
            let keyset_exceeds_table = choice.lookup_count > total_rows.max(1.0);
            let forced_index = hints.index_hints.iter().any(|hint| {
                matches!(hint.hint_type, IndexHintType::Force)
                    && matches!(hint.scope, IndexHintScope::All | IndexHintScope::Join)
            });
            let scan_cost = anticipated_seq_scan_cost_parts(
                table,
                seq_output_rows,
                selected_seq_access_rows,
                seq_cost,
                hints,
            );
            if !has_query_use_index
                && !forced_index
                && (keyset_exceeds_table || total_rows >= 50_000.0)
                && choice.cost.weighted_total() >= scan_cost.weighted_total()
                && !preserves_selectivity
            {
                return Ok(set_scan_query_hint_note(
                    materialize_seq_scan(
                        table,
                        filter.clone(),
                        seq_output_rows,
                        selected_seq_access_rows,
                        seq_cost,
                        possible_keys.clone(),
                        selected_zone_map,
                    ),
                    hints,
                ));
            }
            // The IndexInLookup operator already enforces the IN predicate
            // by driving index seeks from the subquery's rows. Strip the
            // matching `Expr::InSubquery` from the residual filter so it
            // is not re-evaluated for every visited row.
            let residual_filter = remove_in_subquery_predicate_for_column(
                filter,
                table,
                &access.in_column,
                &access.subquery,
            );
            let in_lookup_covering =
                table.system_time.is_none() && index_would_cover(&access.index, hints);
            let mut subquery = access.subquery;
            normalize_select_predicates(db, &mut subquery);
            let subquery_plan = build_physical_plan(db, &subquery)?;
            let plan = PhysicalPlan::IndexInLookup {
                table: table.clone(),
                index: access.index,
                key_prefix: access.key_prefix,
                in_column: access.in_column,
                subquery: Box::new(subquery),
                subquery_plan: Box::new(subquery_plan),
                filter: residual_filter,
                estimated_rows: choice.estimated_rows,
                access_rows: choice.access_rows,
                cost: choice.cost,
                possible_keys,
                covering: in_lookup_covering,
                hint_note: None,
                projected_columns: None,
            };
            return Ok(set_scan_query_hint_note(plan, hints));
        }
    }

    if let Some(merge) = index_merge_choice {
        let single_cost = index_choice
            .as_ref()
            .map(|(_, cost)| *cost)
            .unwrap_or(f64::INFINITY);
        let single_access_rows = index_choice
            .as_ref()
            .map(|(access, _)| estimate_index_access_rows(&table.schema, access, total_rows))
            .unwrap_or(total_rows);
        let intersection_reduces_heap = matches!(&merge.node, IndexMergeNode::Intersection { .. })
            && merge.rows * 4.0 < single_access_rows;
        let scan_cost = anticipated_seq_scan_cost_parts(
            table,
            seq_output_rows,
            selected_seq_access_rows,
            seq_cost,
            hints,
        )
        .weighted_total();
        if forced_index_merge
            || ((merge.cost < single_cost || intersection_reduces_heap) && merge.cost < scan_cost)
        {
            let access_rows = merge.rows.max(1.0);
            let estimated_rows =
                estimate_filter_rows(&table.schema, expr, total_rows).clamp(0.0, access_rows);
            let mut cost = crate::engine::cbo::Cost::scalar(merge.cost);
            cost.mem_bytes = access_rows * std::mem::size_of::<crate::storage::RowId>() as f64;
            let mut merge_indexes = Vec::new();
            merge.node.collect_index_names(&mut merge_indexes);
            crate::engine::cbo::trace::event(
                "index_merge_candidate",
                serde_json::json!({
                    "table": table.schema.name,
                    "indexes": merge_indexes,
                    "leaf_count": merge.node.leaf_count(),
                    "access_rows": access_rows,
                    "output_rows": estimated_rows,
                    "merge_cost": merge.cost,
                    "single_index_cost": single_cost,
                    "scan_cost": scan_cost,
                    "forced": forced_index_merge,
                }),
            );
            return Ok(set_scan_query_hint_note(
                PhysicalPlan::IndexMerge {
                    table: table.clone(),
                    root: merge.node,
                    filter,
                    estimated_rows,
                    access_rows,
                    cost,
                    possible_keys,
                    hint_note: None,
                    projected_columns: None,
                },
                hints,
            ));
        }
    }

    let Some((choice, _choice_cost)) = index_choice else {
        // A non-sargable residual predicate must not suppress an index whose
        // only job is to provide ORDER BY ... LIMIT order.  The old early
        // return made volatile/scalar predicates fall straight to a blocking
        // ZoneMap/SeqScan + TopK even when the primary key matched ORDER BY.
        if let Some(ordered) = choose_orderby_index_scan(
            &ctx,
            table,
            hints,
            total_rows,
            seq_rows,
            filter.clone(),
            &possible_keys,
        ) {
            let ordered_limit = hints.limit_with_offset.unwrap_or(usize::MAX);
            let scan_cost = anticipated_seq_scan_cost_parts(
                table,
                seq_output_rows,
                selected_seq_access_rows,
                seq_cost,
                hints,
            );
            let unordered_cost = anticipated_ordered_limit_cost(
                &ctx,
                scan_cost,
                seq_output_rows,
                row_width,
                ordered_limit,
            )
            .weighted_total();
            let force_order = hints.index_hints.iter().any(|hint| {
                matches!(hint.hint_type, IndexHintType::Force)
                    && matches!(hint.scope, IndexHintScope::All | IndexHintScope::OrderBy)
            });
            // This candidate exists only to exploit an ordered row goal; it
            // must not silently defeat an explicit parallel request whose
            // established contract is ParallelScan + ParallelTopK for a
            // residual-filtered input. FORCE INDEX FOR ORDER BY remains the
            // user's stronger, access-path-specific instruction.
            let explicit_parallel = hints
                .query_hints
                .iter()
                .any(|hint| matches!(hint, QueryHint::Parallel { degree } if *degree != 1));
            let bounded_analytical_goal = ordered_limit >= 64
                && match &ordered {
                    PhysicalPlan::IndexRangeScan { access_rows, .. }
                    | PhysicalPlan::IndexFastScan { access_rows, .. } => {
                        *access_rows <= total_rows * 0.01
                    }
                    _ => false,
                };
            if force_order
                || (!explicit_parallel
                    && (bounded_analytical_goal
                        || plan_cost(&ordered).weighted_total() < unordered_cost))
            {
                return Ok(set_scan_query_hint_note(ordered, hints));
            }
        }
        return Ok(set_scan_query_hint_note(
            materialize_seq_scan(
                table,
                filter.clone(),
                seq_output_rows,
                selected_seq_access_rows,
                seq_cost,
                possible_keys.clone(),
                selected_zone_map,
            ),
            hints,
        ));
    };
    let height = index_cost_height(&table.schema, &choice.index, total_rows);
    // Compute the order-preserving-LIMIT signal before `filter` and
    // `expr` are consumed by the IndexAccessKind branches below. Used
    // by the seq-vs-index safety check at the end of this function.
    let order_direction = order_by_index_direction(hints.order_by);
    let order_preserving = order_direction.is_some()
        && index_order_covers_order_by(&table.schema, &choice.index, Some(expr), hints.order_by);
    let order_preserving_with_limit = hints.limit_with_offset.is_some() && order_preserving;
    let ordered_limit = order_preserving
        .then_some(hints.limit_with_offset)
        .flatten();
    let scan_row_goal = hints.row_goal.or(ordered_limit);
    // Same predicate `mark_covering_index_recursive` will use later, but
    // we evaluate it up front so the cost model can credit covering scans
    // for skipping heap fetches. Without this, two candidate indexes —
    // one covering, one not — get identical cost estimates and the CBO
    // can't prefer the covering one.
    let will_cover = table.system_time.is_none() && index_would_cover(&choice.index, hints);
    // Only a range path can be rejected by the final seq-vs-index safety
    // comparison. Preserve fallback inputs for that shape alone; lookup and
    // multi-lookup plans avoid these otherwise wasted deep clones.
    let seq_fallback_inputs = matches!(&choice.kind, IndexAccessKind::Range { .. })
        .then(|| (filter.clone(), possible_keys.clone()));
    let has_force = hints.index_hints.iter().any(|h| {
        matches!(h.hint_type, IndexHintType::Force)
            && matches!(h.scope, IndexHintScope::All | IndexHintScope::Join)
    });
    let ordered_alternative_inputs =
        (!has_force && !has_query_use_index && !order_preserving_with_limit)
            .then(|| (filter.clone(), possible_keys.clone()));
    let index_plan = match choice.kind {
        IndexAccessKind::Lookup { key, exact } => {
            let access_rows = estimate_constant_index_lookup_rows(
                &table.schema,
                &choice.index,
                &key,
                exact,
                total_rows,
            );
            let estimated_rows =
                estimate_index_output_rows(&table.schema, filter.as_ref(), access_rows, total_rows);
            let cost_access_rows =
                access_rows_for_row_goal(access_rows, estimated_rows, scan_row_goal);
            let cost_output_rows = scan_row_goal
                .map(|goal| estimated_rows.min(goal as f64))
                .unwrap_or(estimated_rows);
            // index_lookup() already factors heap fetches in via
            // table_pages; for a covering lookup, pass 0 to remove that
            // contribution.
            let effective_table_pages = if will_cover { 0.0 } else { table_pages };
            let mut cost = operator_cost::index_lookup(
                &ctx,
                height,
                cost_access_rows,
                effective_table_pages,
                row_width,
            );
            let qual_cpu = qual_cpu * cost_access_rows;
            cost.total += qual_cpu;
            cost.cpu_units += qual_cpu;
            PhysicalPlan::IndexLookup {
                table: table.clone(),
                index: choice.index,
                key,
                exact,
                filter,
                estimated_rows: cost_output_rows,
                access_rows: cost_access_rows,
                cost,
                possible_keys,
                covering: will_cover,
                hint_note: None,
                projected_columns: None,
            }
        }
        IndexAccessKind::MultiLookup { keys, exact } => {
            let access_rows = estimate_constant_index_multi_lookup_rows(
                &table.schema,
                &choice.index,
                &keys,
                exact,
                total_rows,
            );
            let estimated_rows =
                estimate_index_output_rows(&table.schema, filter.as_ref(), access_rows, total_rows);
            let cost_access_rows =
                access_rows_for_row_goal(access_rows, estimated_rows, scan_row_goal);
            let cost_output_rows = scan_row_goal
                .map(|goal| estimated_rows.min(goal as f64))
                .unwrap_or(estimated_rows);
            let effective_table_pages = if will_cover {
                0.0
            } else {
                table_pages_for_batched_in_lookup(
                    &table.schema,
                    &choice.index,
                    table_pages,
                    keys.len() as f64,
                )
            };
            let mut cost = operator_cost::index_lookup(
                &ctx,
                height,
                cost_access_rows,
                effective_table_pages,
                row_width,
            );
            let qual_cpu = qual_cpu * cost_access_rows;
            let probe_cpu = extra_multi_lookup_cpu(height, keys.len());
            cost.total += qual_cpu + probe_cpu;
            cost.cpu_units += qual_cpu + probe_cpu;
            PhysicalPlan::IndexMultiLookup {
                table: table.clone(),
                index: choice.index,
                keys,
                exact,
                filter,
                estimated_rows: cost_output_rows,
                access_rows: cost_access_rows,
                cost,
                possible_keys,
                covering: will_cover,
                hint_note: None,
                projected_columns: None,
            }
        }
        IndexAccessKind::Range { low, high } => {
            let access_rows = estimate_index_range_access_rows(
                &table.schema,
                &choice.index,
                &low,
                &high,
                total_rows,
            );
            let estimated_rows =
                estimate_index_output_rows(&table.schema, filter.as_ref(), access_rows, total_rows);
            let cost_access_rows =
                access_rows_for_row_goal(access_rows, estimated_rows, scan_row_goal);
            let cost_output_rows = scan_row_goal
                .map(|goal| estimated_rows.min(goal as f64))
                .unwrap_or(estimated_rows);
            // The chosen index covers `rows` entries; estimate leaf-page
            // count from the actual key width rather than the legacy
            // fixed "100 entries per leaf" assumption.
            let leaf_pages = operator_cost::estimated_index_leaf_pages(
                &table.schema,
                &choice.index,
                cost_access_rows,
            );
            if will_cover {
                // Covering range: cost is index-only — no heap term at all.
                // Emit the dedicated IndexFastScan operator so EXPLAIN and
                // downstream consumers see this as a distinct access path.
                let cost = operator_cost::index_fast_scan(
                    &ctx,
                    height,
                    leaf_pages,
                    cost_access_rows,
                    row_width,
                );
                let mut cost = cost;
                let qual_cpu = qual_cpu * cost_access_rows;
                cost.total += qual_cpu;
                cost.cpu_units += qual_cpu;
                PhysicalPlan::IndexFastScan {
                    table: table.clone(),
                    index: choice.index,
                    low,
                    high,
                    filter,
                    estimated_rows: cost_output_rows,
                    access_rows: cost_access_rows,
                    cost,
                    possible_keys,
                    reverse: order_direction.unwrap_or(false) && order_preserving,
                    hint_note: None,
                    projected_columns: None,
                }
            } else {
                let correlation = index_cost_correlation(&table.schema, &choice.index);
                let estimate = operator_cost::estimate_index_range_scan(
                    &ctx,
                    operator_cost::IndexRangeScanInput {
                        index_height: height,
                        leaf_pages,
                        matched_rows: cost_access_rows,
                        table_rows: total_rows,
                        table_pages,
                        output_width: row_width,
                        correlation,
                        mrr_chunk_rows: range_scan_mrr_chunk_rows(),
                    },
                );
                let residual_qual_cpu = qual_cpu * cost_access_rows;
                let seq_candidate_cost = anticipated_seq_scan_cost_parts(
                    table,
                    seq_output_rows,
                    selected_seq_access_rows,
                    seq_cost,
                    hints,
                );
                crate::engine::cbo::trace::event(
                    "scan_candidates",
                    serde_json::json!({
                        "table": table.schema.name,
                        "projection_width": row_width,
                        "heap_rows": total_rows,
                        "heap_pages": table_pages,
                        "seq": {
                            "access_rows": seq_access_rows,
                            "output_rows": seq_output_rows,
                            "serial_cost": seq_cost.weighted_total(),
                            "final_cost": seq_candidate_cost.weighted_total(),
                            "parallel": seq_candidate_cost.weighted_total()
                                < seq_cost.weighted_total(),
                        },
                        "index": {
                            "name": choice.index.name,
                            "candidate_rows": cost_access_rows,
                            "output_rows": cost_output_rows,
                            "correlation": correlation,
                            "logical_heap_probes": estimate.logical_heap_probes,
                            "distinct_heap_pages": estimate.distinct_heap_pages,
                            "miss_heap_pages": estimate.miss_heap_pages,
                            "hit_heap_probes": estimate.hit_heap_probes,
                            "rows_per_heap_page": estimate.rows_per_heap_page,
                            "mrr_chunk_rows": range_scan_mrr_chunk_rows(),
                            "cost": estimate.cost.total + residual_qual_cpu,
                        }
                    }),
                );
                let mut cost = estimate.cost;
                cost.total += residual_qual_cpu;
                cost.cpu_units += residual_qual_cpu;
                PhysicalPlan::IndexRangeScan {
                    table: table.clone(),
                    index: choice.index,
                    low,
                    high,
                    filter,
                    estimated_rows: cost_output_rows,
                    access_rows: cost_access_rows,
                    cost,
                    possible_keys,
                    covering: false,
                    reverse: order_direction.unwrap_or(false) && order_preserving,
                    hint_note: None,
                    projected_columns: None,
                }
            }
        }
    };
    // FORCE INDEX and optimizer-level USE_INDEX: when a usable named
    // index access path exists, keep it and skip the CBO alternatives below.
    // Table-level USE INDEX still only limits the candidate set.
    if has_force || has_query_use_index {
        if has_positive_like {
            crate::engine::status_counters::record_like_btree_selected();
        }
        return Ok(set_scan_query_hint_note(index_plan, hints));
    }

    // `best_index_access` ranks indexes that implement WHERE constraints.
    // An index whose only purpose is to provide ORDER BY ... LIMIT order is
    // a different complete-plan alternative: it may read a few extra rows,
    // but it can stop at the row goal and avoid a blocking TopK/Sort.  Keep
    // enumerating that alternative even when a usable predicate index was
    // found.  Previously this happened only in the `index_choice == None`
    // branch above, so a low-cardinality secondary predicate (for example
    // `status = 'accept'`) suppressed an ordered primary-key scan and forced
    // every matching secondary entry through Apply and TopK.
    if let Some((ordered_filter, ordered_possible_keys)) = ordered_alternative_inputs {
        if let (Some(row_goal), Some(ordered)) = (
            hints.limit_with_offset,
            choose_orderby_index_scan(
                &ctx,
                table,
                hints,
                total_rows,
                seq_rows,
                ordered_filter,
                &ordered_possible_keys,
            ),
        ) {
            let unordered_cost = anticipated_ordered_limit_cost(
                &ctx,
                plan_cost(&index_plan).clone(),
                estimated_rows(&index_plan),
                row_width,
                row_goal,
            )
            .weighted_total();
            let ordered_cost = plan_cost(&ordered).weighted_total();
            let force_order = hints.index_hints.iter().any(|hint| {
                matches!(hint.hint_type, IndexHintType::Force)
                    && matches!(hint.scope, IndexHintScope::All | IndexHintScope::OrderBy)
            });
            let explicit_parallel = hints
                .query_hints
                .iter()
                .any(|hint| matches!(hint, QueryHint::Parallel { degree } if *degree != 1));
            crate::engine::cbo::trace::event(
                "ordered_limit_index_candidates",
                serde_json::json!({
                    "table": table.schema.name,
                    "limit_with_offset": row_goal,
                    "unordered_index_with_order_cost": unordered_cost,
                    "ordered_index_cost": ordered_cost,
                }),
            );
            if force_order || (!explicit_parallel && ordered_cost < unordered_cost) {
                return Ok(set_scan_query_hint_note(ordered, hints));
            }
        }
    }
    // CBO cost comparison: prefer a sequential scan whenever it is
    // cheaper than the chosen non-clustered IndexRangeScan.
    //
    // Both costs were already computed above
    // (operator_cost::seq_scan / index_range_scan) so the comparison
    // is essentially free. Trusting the cost model directly catches
    // cases the old 50%-selectivity gate missed — e.g. a 5%-selective
    // predicate where ~70k random heap fetches still cost more than a
    // single sequential pass over the heap. The downstream
    // `apply_parallel_hints()` pass may then wrap the chosen SeqScan in
    // `Exchange { ParallelSeqScan }` when that is cheaper still.
    if let PhysicalPlan::IndexRangeScan {
        index,
        cost: index_cost,
        access_rows: matched,
        ..
    } = &index_plan
    {
        // Skip the comparison when:
        //   * the index is the primary key (clustered: no extra random
        //     I/O, cost model already accurate);
        //   * an order-preserving LIMIT whose blocking alternative cannot be
        //     costed at this scan. Simple single-table shapes compare the
        //     ordered index against Zone Map/SeqScan plus a conservative
        //     TopK/Sort ordering cost;
        //   * matched rows ≤ 1000 — on tiny tables the absolute cost
        //     difference is negligible and existing regression tests pin
        //     the index choice.
        let ordered_cost_comparable = !order_preserving_with_limit || ordered_limit.is_some();
        if !index.primary && ordered_cost_comparable && total_rows > 0.0 && *matched > 1000.0 {
            let scan_cost = anticipated_seq_scan_cost_parts(
                table,
                seq_output_rows,
                selected_seq_access_rows,
                seq_cost,
                hints,
            );
            let complete_seq_cost = ordered_limit
                .map(|limit| {
                    anticipated_ordered_limit_cost(
                        &ctx,
                        scan_cost,
                        seq_output_rows,
                        row_width,
                        limit,
                    )
                })
                .unwrap_or(scan_cost)
                .weighted_total();
            if let Some(row_goal) = ordered_limit {
                crate::engine::cbo::trace::event(
                    "ordered_limit_scan_candidates",
                    serde_json::json!({
                        "table": table.schema.name,
                        "limit_with_offset": row_goal,
                        "ordered_index_cost": index_cost.weighted_total(),
                        "unordered_scan_with_order_cost": complete_seq_cost,
                    }),
                );
            }
            if complete_seq_cost <= index_cost.weighted_total() {
                let (seq_filter, seq_possible_keys) =
                    seq_fallback_inputs.expect("only index range scans reach sequential fallback");
                return Ok(set_scan_query_hint_note(
                    materialize_seq_scan(
                        table,
                        seq_filter,
                        seq_output_rows,
                        selected_seq_access_rows,
                        seq_cost,
                        seq_possible_keys,
                        selected_zone_map,
                    ),
                    hints,
                ));
            }
        }
    }
    if has_positive_like {
        crate::engine::status_counters::record_like_btree_selected();
    }
    Ok(set_scan_query_hint_note(index_plan, hints))
}

#[allow(clippy::too_many_arguments)]
fn materialize_seq_scan(
    table: &PlanTable,
    filter: Option<Expr>,
    estimated_rows: f64,
    access_rows: f64,
    cost: crate::engine::cbo::Cost,
    possible_keys: Vec<String>,
    zone_map: Option<ZoneMapPlanInfo>,
) -> PhysicalPlan {
    match zone_map {
        Some(zone_map) => PhysicalPlan::ZoneMapScan {
            table: table.clone(),
            filter,
            estimated_rows,
            access_rows,
            cost,
            possible_keys,
            runtime_filters: Vec::new(),
            hint_note: None,
            projected_columns: None,
            zone_map,
        },
        None => PhysicalPlan::SeqScan {
            table: table.clone(),
            filter,
            estimated_rows,
            access_rows,
            cost,
            possible_keys,
            runtime_filters: Vec::new(),
            hint_note: None,
            projected_columns: None,
        },
    }
}

fn build_partition_pruning_plan(
    table: &TableSchema,
    filter: Option<&Expr>,
) -> crate::storage::PartitionPruningPlan {
    let Some(partitioning) = table.partitioning.as_ref() else {
        return crate::storage::PartitionPruningPlan::All;
    };
    let Some(filter) = filter else {
        return crate::storage::PartitionPruningPlan::All;
    };
    if expr_has_placeholder(filter) {
        return crate::storage::PartitionPruningPlan::Program;
    }
    let Some(selected) = crate::engine::executor::partition_router::pruned_storage_ids_from_filter(
        table,
        Some(filter),
    ) else {
        return crate::storage::PartitionPruningPlan::All;
    };
    let leaves = partitioning.storage_ids();
    let selected = selected
        .into_iter()
        .collect::<std::collections::HashSet<_>>();
    let ordinals = leaves
        .iter()
        .enumerate()
        .filter_map(|(ordinal, storage_id)| selected.contains(storage_id).then_some(ordinal));
    crate::storage::PartitionPruningPlan::Static(crate::storage::PartitionSet::from_ordinals(
        leaves.len(),
        ordinals,
    ))
}

/// Return row/page population for a statically-pruned physical leaf set.
/// Prepared parameters are intentionally excluded: their execution-time
/// bitmap may shrink scans, but it must not alter cached access paths, join
/// order or parallel topology.
fn static_partition_population(table: &TableSchema, filter: &Expr) -> Option<(f64, f64)> {
    if expr_has_placeholder(filter) {
        return None;
    }
    let storage_ids = crate::engine::executor::partition_router::pruned_storage_ids_from_filter(
        table,
        Some(filter),
    )?;
    let mut rows = 0u64;
    let mut pages = 0u64;
    for storage_id in storage_ids {
        let stats = table.stats.partition_stats.get(&storage_id)?;
        rows = rows.saturating_add(stats.row_count);
        pages = pages.saturating_add(stats.heap_page_count);
    }
    Some((rows as f64, pages as f64))
}

fn range_scan_mrr_chunk_rows() -> Option<f64> {
    // Planning uses a versioned canonical MRR window. The executor's actual
    // work-area grant and MYDB_MRR_SORT diagnostic switch are deliberately
    // excluded so memory/resource changes cannot alter the physical plan.
    Some(crate::engine::cbo::PLANNING_MRR_WINDOW_ROWS)
}

fn anticipated_seq_scan_cost_parts(
    table: &PlanTable,
    estimated_rows: f64,
    access_rows: f64,
    serial: crate::engine::cbo::Cost,
    hints: &ScanHints<'_>,
) -> crate::engine::cbo::Cost {
    let parallel_disabled = hints
        .query_hints
        .iter()
        .any(|hint| matches!(hint, crate::parser::QueryHint::NoParallel))
        || parallel_planner_config().disabled;
    if parallel_disabled || hints.row_goal.is_some() || !hints.order_by.is_empty() {
        return serial;
    }
    let degree = cpu_bound_parallel_degree(default_parallel_degree());
    if degree > 1
        && cbo_wants_parallel_for_seqscan(
            estimated_rows,
            access_rows,
            &table.schema,
            serial,
            degree,
        )
    {
        parallel_cost_from_serial(serial, degree)
    } else {
        serial
    }
}

fn anticipated_ordered_limit_cost(
    ctx: &crate::engine::cbo::CboContext,
    input: crate::engine::cbo::Cost,
    input_rows: f64,
    row_width: f64,
    row_goal: usize,
) -> crate::engine::cbo::Cost {
    use crate::engine::cbo::operator_cost;

    if row_goal <= topk_max_heap_from_env() {
        operator_cost::top_k(&input, input_rows, row_goal as f64, row_width)
    } else {
        operator_cost::sort(ctx, &input, input_rows, row_width)
    }
}

fn build_hint_note(hints: &[IndexHint]) -> Option<String> {
    if hints.is_empty() {
        return None;
    }
    let parts: Vec<String> = hints
        .iter()
        .map(|h| {
            let kind = match h.hint_type {
                IndexHintType::Use => "USE INDEX",
                IndexHintType::Force => "FORCE INDEX",
                IndexHintType::Ignore => "IGNORE INDEX",
            };
            let scope = match h.scope {
                IndexHintScope::All => "",
                IndexHintScope::Join => " FOR JOIN",
                IndexHintScope::OrderBy => " FOR ORDER BY",
                IndexHintScope::GroupBy => " FOR GROUP BY",
            };
            let names = h.index_names.join(", ");
            format!("{kind}{scope} ({names})")
        })
        .collect();
    Some(parts.join("; "))
}

fn projected_seq_scan_cost(
    ctx: &crate::engine::cbo::CboContext,
    table: &PlanTable,
    required: Option<&HashSet<String>>,
    total_rows: f64,
    output_rows: f64,
    table_pages: f64,
    qual_cpu: f64,
) -> crate::engine::cbo::Cost {
    use crate::engine::cbo::operator_cost;
    let row_width = required
        .and_then(|cols| projected_indices_for_referenced(table, cols))
        .map(|indices| projected_row_width(table, Some(indices.as_slice())))
        .unwrap_or_else(|| operator_cost::estimated_row_width(&table.schema));
    operator_cost::seq_scan(
        ctx,
        table_pages,
        total_rows,
        output_rows,
        row_width,
        qual_cpu,
    )
}

fn expected_rows_to_reach_goal(total_rows: f64, output_rows: f64, goal: usize) -> f64 {
    if goal == 0 {
        return 0.0;
    }
    if total_rows <= 0.0 || output_rows <= 0.0 {
        return total_rows.max(0.0);
    }
    let selectivity = (output_rows / total_rows).clamp(1.0 / total_rows.max(1.0), 1.0);
    ((goal as f64) / selectivity)
        .ceil()
        .clamp(total_rows.min(1.0), total_rows)
}

fn access_rows_for_row_goal(access_rows: f64, output_rows: f64, goal: Option<usize>) -> f64 {
    let Some(goal) = goal else {
        return access_rows;
    };
    expected_rows_to_reach_goal(access_rows, output_rows.min(access_rows), goal)
}

fn set_scan_query_hint_note(mut plan: PhysicalPlan, scan_hints: &ScanHints<'_>) -> PhysicalPlan {
    // Multi-table planning already knows every base-table column referenced
    // anywhere in the query block. Materialise that projection on the scan
    // before join enumeration (rather than waiting for finish_plan), so the
    // width of a newly built join is a faithful input to the next join's
    // memory/spill cost.
    match &mut plan {
        PhysicalPlan::SeqScan {
            table,
            projected_columns,
            ..
        }
        | PhysicalPlan::ZoneMapScan {
            table,
            projected_columns,
            ..
        }
        | PhysicalPlan::IndexLookup {
            table,
            projected_columns,
            ..
        }
        | PhysicalPlan::IndexMultiLookup {
            table,
            projected_columns,
            ..
        }
        | PhysicalPlan::IndexRangeScan {
            table,
            projected_columns,
            ..
        }
        | PhysicalPlan::IndexFastScan {
            table,
            projected_columns,
            ..
        }
        | PhysicalPlan::IndexInLookup {
            table,
            projected_columns,
            ..
        } => {
            *projected_columns = scan_hints.required_columns.as_ref().and_then(|required| {
                let indices = projected_indices_for_referenced(table, required)?;
                (indices.len() < table.schema.columns.len()).then(|| Arc::new(indices))
            });
        }
        _ => {}
    }

    let mut parts = Vec::new();
    if let Some(idx_note) = build_hint_note(scan_hints.index_hints) {
        parts.push(idx_note);
    }
    let use_idx = query_hint_use_index(scan_hints.query_hints, scan_hints.table_name, "");
    if !use_idx.is_empty() {
        parts.push(format!("USE_INDEX({})", use_idx.join(", ")));
    }
    let no_idx = query_hint_no_index(scan_hints.query_hints, scan_hints.table_name, "");
    for idx_name in &no_idx {
        parts.push(format!("NO_INDEX({idx_name})"));
    }
    if parts.is_empty() {
        return plan;
    }
    let note = Some(parts.join("; "));
    match &mut plan {
        PhysicalPlan::SeqScan { hint_note, .. }
        | PhysicalPlan::ZoneMapScan { hint_note, .. }
        | PhysicalPlan::IndexLookup { hint_note, .. }
        | PhysicalPlan::IndexRangeScan { hint_note, .. }
        | PhysicalPlan::IndexFastScan { hint_note, .. }
        | PhysicalPlan::IndexInLookup { hint_note, .. } => {
            *hint_note = note;
        }
        _ => {}
    }
    plan
}

fn choose_orderby_index_scan(
    ctx: &crate::engine::cbo::CboContext,
    table: &PlanTable,
    hints: &ScanHints<'_>,
    total_rows: f64,
    qualifying_rows: f64,
    filter: Option<Expr>,
    possible_keys: &[String],
) -> Option<PhysicalPlan> {
    use crate::engine::cbo::operator_cost;
    let limit = hints.limit_with_offset?;
    if hints.order_by.is_empty()
        || hints
            .order_by
            .iter()
            .any(|o| o.nulls_first_effective() != o.asc)
    {
        return None;
    }
    // A partitioned table keeps one index per partition; the executor
    // fans an index scan out across partitions and concatenates the
    // results, so per-partition index order is not the global order the
    // ORDER BY demands. Don't offer an order-providing index scan (which
    // would elide the Sort) — let a real Sort run over the fan-out.
    if table.schema.is_partitioned() {
        return None;
    }
    let all_asc = hints.order_by.iter().all(|item| item.asc);
    let all_desc = hints.order_by.iter().all(|item| !item.asc);
    if !all_asc && !all_desc {
        return None;
    }
    let index_hints = ScanIndexHints::new(hints, &table.schema.name);
    let mut best: Option<PhysicalPlan> = None;
    for index in table.schema.iter_queryable_indexes().filter(|i| {
        !i.is_fulltext() && !i.is_spatial() && !i.is_vector() && !i.has_multi_valued_parts()
    }) {
        if !index_hints.allows(&index.name, hints.index_hints, IndexHintScope::OrderBy) {
            continue;
        }
        if index.columns.len() < hints.order_by.len() {
            continue;
        }
        if index.has_prefix_columns() {
            continue;
        }
        let mut matched = true;
        for (i, item) in hints.order_by.iter().enumerate() {
            match index.key_part(i) {
                Some(crate::storage::IndexKeyPart::Column(name)) => {
                    let Some(column) = expr_column_name(&item.expr) else {
                        matched = false;
                        break;
                    };
                    if !name.eq_ignore_ascii_case(column) {
                        matched = false;
                        break;
                    }
                }
                Some(crate::storage::IndexKeyPart::Expression(expr_part)) => {
                    let Some(ast) = expr_part.ast.as_ref() else {
                        matched = false;
                        break;
                    };
                    if crate::engine::expr_program::expr_fingerprint(ast)
                        != crate::engine::expr_program::expr_fingerprint(&item.expr)
                    {
                        matched = false;
                        break;
                    }
                }
                None => {
                    matched = false;
                    break;
                }
            }
        }
        if !matched {
            continue;
        }
        let range = filter
            .as_ref()
            .and_then(|expr| boolean_range::candidate(&table.schema, index, expr));
        let range_rows = range
            .as_ref()
            .map(|range| estimate_index_access_rows(&table.schema, range, total_rows))
            .unwrap_or(total_rows);
        let mut access_rows =
            expected_rows_to_reach_goal(range_rows, qualifying_rows.min(range_rows), limit);
        // An ORDER-providing index and an unrelated heap residual can be
        // strongly correlated in physical order (queue/status columns are a
        // common example). Marginal selectivity alone may predict five probes
        // while the first qualifying row is hundreds of thousands of keys
        // away. Cost a stable minimum candidate window on non-trivial tables;
        // the ordinary seq+TopK alternative still has to win the full cost
        // comparison, and small OLTP tables retain exact early-stop costing.
        let has_uncovered_residual = filter
            .as_ref()
            .is_some_and(|expr| !index_aggregate_filter_covered_by_bounds(expr, index));
        if has_uncovered_residual && total_rows >= 10_000.0 {
            // A single-row goal is maximally exposed to an unlucky deep
            // correlation between index order and a heap residual. Larger
            // goals average many successes: scale the conservative tail floor
            // by sqrt(K), preserving the established LIMIT 1 protection while
            // allowing LIMIT 100+ analytical Top-K scans to exploit order.
            let goal_dilution = (limit.max(1) as f64).sqrt();
            let correlation_uncertainty = (total_rows.sqrt() * 16.0).max(16_384.0) / goal_dilution;
            access_rows = access_rows.max(correlation_uncertainty.min(range_rows));
        }
        let output_rows = qualifying_rows.min(limit as f64).max(1.0);
        let row_width = hints
            .required_columns
            .as_ref()
            .and_then(|columns| projected_indices_for_referenced(table, columns))
            .map(|indices| projected_row_width(table, Some(indices.as_slice())))
            .unwrap_or_else(|| operator_cost::estimated_row_width(&table.schema));
        // Heap occupancy comes from ANALYZE (or the full physical row as a
        // fallback), never from the much narrower aggregate projection.
        let table_pages = table_heap_pages_for_cost(&table.schema);
        let height = index_cost_height(&table.schema, index, total_rows);
        let leaf_pages = index_cost_leaf_pages(&table.schema, index, access_rows, total_rows);
        let correlation = index_cost_correlation(&table.schema, index);
        let cost = operator_cost::index_range_scan(
            ctx,
            height,
            leaf_pages,
            access_rows,
            total_rows,
            table_pages,
            row_width,
            correlation,
        );
        let (low, high) = match range {
            Some(IndexAccess {
                kind: IndexAccessKind::Range { low, high },
                ..
            }) => (low, high),
            _ => (None, None),
        };
        let candidate = PhysicalPlan::IndexRangeScan {
            table: table.clone(),
            index: index.clone(),
            low,
            high,
            filter: filter.clone(),
            estimated_rows: output_rows,
            access_rows,
            cost,
            possible_keys: possible_keys.to_vec(),
            covering: false,
            reverse: all_desc,
            hint_note: None,
            projected_columns: None,
        };
        if best.as_ref().is_none_or(|previous| {
            plan_cost(&candidate).weighted_total() < plan_cost(previous).weighted_total()
        }) {
            best = Some(candidate);
        }
    }
    best
}

fn choose_groupby_index_scan(
    ctx: &crate::engine::cbo::CboContext,
    table: &PlanTable,
    hints: &ScanHints<'_>,
    total_rows: f64,
    possible_keys: &[String],
) -> Option<PhysicalPlan> {
    use crate::engine::cbo::operator_cost;
    if hints.group_by.is_empty() {
        return None;
    }
    // Streaming group-by relies on the index delivering rows in group
    // order. On a partitioned table the scan fans out per partition and
    // concatenates, so a group's rows are split across partition
    // boundaries — the streaming aggregate would emit duplicate partial
    // groups. Fall back to a hash aggregate over a sequential scan.
    if table.schema.is_partitioned() {
        return None;
    }
    // Each group_by item may be either a plain column ref (NDV / prefix
    // matching uses its name) or an arbitrary expression that we'll
    // match against a functional index part by fingerprint.
    let column_group_count = hints
        .group_by
        .iter()
        .try_fold(1.0_f64, |acc, item| {
            let column = expr_column_name(item)?;
            ndv_for_column(&table.schema, column).map(|ndv| acc * ndv as f64)
        })
        .unwrap_or_else(|| default_group_by_rows(total_rows))
        .clamp(1.0, total_rows.max(1.0));
    let force_by_hint = hints.index_hints.iter().any(|hint| {
        matches!(hint.hint_type, IndexHintType::Force)
            && matches!(hint.scope, IndexHintScope::All | IndexHintScope::GroupBy)
    });

    let index_hints = ScanIndexHints::new(hints, &table.schema.name);
    let mut best: Option<(f64, PhysicalPlan)> = None;
    for index in table.schema.iter_queryable_indexes().filter(|i| {
        !i.is_fulltext() && !i.is_spatial() && !i.is_vector() && !i.has_multi_valued_parts()
    }) {
        if !index_hints.allows(&index.name, hints.index_hints, IndexHintScope::GroupBy) {
            continue;
        }
        if index.columns.len() < hints.group_by.len() {
            continue;
        }
        let matches_group_prefix =
            hints
                .group_by
                .iter()
                .enumerate()
                .all(|(i, group_expr)| match index.key_part(i) {
                    Some(crate::storage::IndexKeyPart::Column(name)) => {
                        expr_column_name(group_expr)
                            .map(|col| normalize_expr_column(name) == normalize_expr_column(col))
                            .unwrap_or(false)
                    }
                    Some(crate::storage::IndexKeyPart::Expression(expr_part)) => {
                        expr_part.ast.as_ref().is_some_and(|ast| {
                            crate::engine::expr_program::expr_fingerprint(ast)
                                == crate::engine::expr_program::expr_fingerprint(group_expr)
                        })
                    }
                    None => false,
                });
        if !matches_group_prefix {
            continue;
        }
        let index_group_count = table
            .schema
            .stats
            .indexes
            .get(&normalize_expr_column(&index.name))
            .and_then(|stats| stats.prefix_distinct_counts.get(hints.group_by.len() - 1))
            .copied()
            .filter(|count| *count > 0)
            .map(|count| count as f64)
            .unwrap_or(column_group_count)
            .clamp(1.0, total_rows.max(1.0));
        let row_width = hints
            .required_columns
            .as_ref()
            .and_then(|columns| projected_indices_for_referenced(table, columns))
            .map(|indices| projected_row_width(table, Some(indices.as_slice())))
            .unwrap_or_else(|| operator_cost::estimated_row_width(&table.schema));
        // Use the physical heap occupancy recorded by ANALYZE. Deriving heap
        // pages from the aggregate's projection width mixes two unrelated
        // quantities and can make a narrow seq scan look larger than a
        // non-covering full index walk (TPC-H Q18 is the canonical shape).
        let table_pages = table_heap_pages_for_cost(&table.schema);
        let height = index_cost_height(&table.schema, index, total_rows);
        let leaf_pages = index_cost_leaf_pages(&table.schema, index, total_rows, total_rows);
        let correlation = index_cost_correlation(&table.schema, index);
        let index_covers_required = hints.required_columns.as_ref().is_some_and(|required| {
            !index.has_prefix_columns() && {
                let available: HashSet<String> = index
                    .effective_columns()
                    .iter()
                    .map(|column| normalize_expr_column(column))
                    .collect();
                required.iter().all(|column| available.contains(column))
            }
        });
        let cost = operator_cost::index_range_scan(
            ctx,
            height,
            leaf_pages,
            total_rows,
            total_rows,
            table_pages,
            row_width,
            correlation,
        );
        // Compare complete physical alternatives. Comparing only the scans
        // discarded the ordered candidate before the parent aggregate could
        // price hundreds of MiB of HashAgg spill.
        if !force_by_hint {
            let serial_seq =
                operator_cost::seq_scan(ctx, table_pages, total_rows, total_rows, row_width, 0.0);
            let degree = default_parallel_degree();
            let parallel_off = parallel_planner_config().disabled;
            let seq_parallel = !parallel_off
                && cbo_wants_parallel_for_seqscan(
                    total_rows,
                    total_rows,
                    &table.schema,
                    serial_seq,
                    degree,
                );
            let seq_input = if seq_parallel {
                parallel_cost_from_serial(serial_seq, degree)
            } else {
                serial_seq
            };
            // A full non-covering ordered index walk performs one heap lookup
            // for every base row while also forcing the aggregate into a
            // single-threaded streaming pipeline. Even with a warm buffer pool
            // those per-row probes/latches/decodes dominate large analytical
            // GROUP BY shapes; keep the parallel seq-scan + hash-aggregate
            // alternative available unless the user explicitly forced this
            // GROUP BY index.
            if seq_parallel && !index_covers_required {
                crate::engine::cbo::trace::event(
                    "group_order_index_rejected_noncovering_full_scan",
                    serde_json::json!({
                        "table": table.schema.name,
                        "index": index.name,
                        "effective_cache_pages": ctx.effective_cache_pages,
                        "heap_pages": table_pages,
                        "leaf_pages": leaf_pages,
                    }),
                );
                continue;
            }
            let state_bytes_per_group =
                hints.aggregate_state_bytes_per_group.unwrap_or_else(|| {
                    // Internally-built plans may lack a SELECT aggregate list;
                    // retain a width-based conservative fallback for them.
                    (row_width * 1.5 + 64.0).clamp(64.0, 512.0)
                });
            let hash_complete = operator_cost::hash_aggregate_with_state(
                ctx,
                &seq_input,
                total_rows,
                index_group_count,
                row_width,
                Some(state_bytes_per_group),
            );
            // The physical rewrite parallelizes both the scan and the local
            // hash aggregation when the scan passes the parallel admission
            // gates. Price that complete operator, not a parallel scan feeding
            // a fictitiously serial HashAgg.
            let hash_complete = if seq_parallel {
                parallel_cost_from_serial(hash_complete, degree)
            } else {
                hash_complete
            };
            let stream_complete =
                operator_cost::streaming_aggregate(&cost, total_rows, index_group_count, row_width);
            crate::engine::cbo::trace::event(
                "group_order_index_candidate",
                serde_json::json!({
                    "table": table.schema.name,
                    "index": index.name,
                    "groups": index_group_count,
                    "hash_total": hash_complete.total,
                    "hash_spill_bytes": hash_complete.spill_bytes,
                    "hash_state_bytes_per_group": state_bytes_per_group,
                    "hash_parallel_degree": if seq_parallel { degree } else { 1 },
                    "stream_total": stream_complete.total,
                }),
            );
            if stream_complete.cmp_total(&hash_complete).is_ge() {
                continue;
            }
        }
        let plan = PhysicalPlan::IndexRangeScan {
            table: table.clone(),
            index: index.clone(),
            low: None,
            high: None,
            filter: None,
            estimated_rows: total_rows,
            access_rows: total_rows,
            cost,
            possible_keys: possible_keys.to_vec(),
            covering: false,
            reverse: false,
            hint_note: None,
            projected_columns: None,
        };
        let complete_total =
            operator_cost::streaming_aggregate(&cost, total_rows, index_group_count, row_width)
                .weighted_total();
        if force_by_hint {
            return Some(plan);
        }
        if best
            .as_ref()
            .is_none_or(|(best_total, _)| complete_total < *best_total)
        {
            best = Some((complete_total, plan));
        }
    }
    best.map(|(_, plan)| plan)
}

fn choose_covering_index_scan(
    ctx: &crate::engine::cbo::CboContext,
    table: &PlanTable,
    hints: &ScanHints<'_>,
    total_rows: f64,
    possible_keys: &[String],
) -> Option<PhysicalPlan> {
    use crate::engine::cbo::operator_cost;
    let required = hints.required_columns.as_ref()?;
    if required.is_empty() {
        return None;
    }
    let index_hints = ScanIndexHints::new(hints, &table.schema.name);
    let mut best: Option<&IndexSchema> = None;
    for index in table.schema.iter_queryable_indexes().filter(|i| {
        !i.is_fulltext() && !i.is_spatial() && !i.is_vector() && !i.has_multi_valued_parts()
    }) {
        if index.has_prefix_columns() {
            continue;
        }
        if !index_hints.allows(&index.name, hints.index_hints, IndexHintScope::Join) {
            continue;
        }
        let index_columns: HashSet<String> = index
            .effective_columns()
            .iter()
            .map(|c| normalize_expr_column(c))
            .collect();
        if required.iter().all(|col| index_columns.contains(col)) {
            // Prefer the narrowest scan: tie-break on key-column count
            // first (shorter user-declared keys land fewer entries per
            // leaf), then on total effective width.
            let candidate_key = (index.columns.len(), index.effective_columns().len());
            let beats_best = best.as_ref().is_none_or(|b| {
                let best_key = (b.columns.len(), b.effective_columns().len());
                candidate_key < best_key
            });
            if beats_best {
                best = Some(index);
            }
        }
    }
    let index = best?;
    // Covering index: full leaf walk, no heap fetch. Use the dedicated
    // index-only cost formula so the cost reflects "read just the index"
    // rather than the residual heap term that `index_range_scan` keeps
    // even at correlation=1.0. The output width is the projected
    // (required-column) width — *not* the full table row — so the per-row
    // decode cost matches the projected seq-scan it is compared against in
    // `choose_scan` (both emit the same columns) and the page-count
    // difference is what decides.
    let row_width = projected_indices_for_referenced(table, required)
        .map(|indices| projected_row_width(table, Some(indices.as_slice())))
        .unwrap_or_else(|| operator_cost::estimated_row_width(&table.schema));
    let height = index_cost_height(&table.schema, index, total_rows);
    let leaf_pages = index_cost_leaf_pages(&table.schema, index, total_rows, total_rows);
    let cost = operator_cost::index_fast_scan(ctx, height, leaf_pages, total_rows, row_width);
    Some(PhysicalPlan::IndexFastScan {
        table: table.clone(),
        index: index.clone(),
        low: None,
        high: None,
        filter: None,
        estimated_rows: total_rows,
        access_rows: total_rows,
        cost,
        possible_keys: possible_keys.to_vec(),
        reverse: false,
        hint_note: None,
        projected_columns: None,
    })
}

/// Build the physical plan for an `INDEX_FFS(table [idx])` hint. Returns
/// `None` when no index on `table` can cover the projected/filter
/// columns (or when the user-named index does not exist or does not
/// cover) — caller falls back to ordinary cost-based selection in that
/// case so a misapplied hint behaves like Oracle / MySQL: silently
/// ignored rather than producing a wrong plan.
fn build_index_ffs_plan(
    ctx: &crate::engine::cbo::CboContext,
    table: &PlanTable,
    hints: &ScanHints<'_>,
    total_rows: f64,
    possible_keys: &[String],
    filter: Option<Expr>,
    index_arg: Option<&str>,
) -> Option<PhysicalPlan> {
    use crate::engine::cbo::operator_cost;
    let required = hints.required_columns.as_ref()?;
    if required.is_empty() {
        return None;
    }
    let covers = |index: &IndexSchema| -> bool {
        if index.has_prefix_columns() {
            return false;
        }
        let cols: HashSet<String> = index
            .effective_columns()
            .iter()
            .map(|c| normalize_expr_column(c))
            .collect();
        required.iter().all(|col| cols.contains(col))
    };
    let chosen: &IndexSchema = if let Some(name) = index_arg {
        // Specific index requested: use it only if it actually covers.
        let exact = table
            .schema
            .iter_queryable_indexes()
            .find(|i| i.name.eq_ignore_ascii_case(name))?;
        if !covers(exact) {
            return None;
        }
        exact
    } else {
        // No index named: pick the narrowest covering one — same
        // tie-break as `choose_covering_index_scan`.
        let mut best: Option<&IndexSchema> = None;
        for index in table.schema.iter_queryable_indexes().filter(|i| {
            !i.is_fulltext() && !i.is_spatial() && !i.is_vector() && !i.has_multi_valued_parts()
        }) {
            if !covers(index) {
                continue;
            }
            let candidate_key = (index.columns.len(), index.effective_columns().len());
            let beats_best = best.as_ref().is_none_or(|b| {
                let best_key = (b.columns.len(), b.effective_columns().len());
                candidate_key < best_key
            });
            if beats_best {
                best = Some(index);
            }
        }
        best?
    };
    let row_width = operator_cost::estimated_row_width(&table.schema);
    let height = index_cost_height(&table.schema, chosen, total_rows);
    let leaf_pages = index_cost_leaf_pages(&table.schema, chosen, total_rows, total_rows);
    let cost = operator_cost::index_fast_scan(ctx, height, leaf_pages, total_rows, row_width);
    let label = match index_arg {
        Some(name) => format!("INDEX_FFS({name})"),
        None => "INDEX_FFS".to_string(),
    };
    Some(PhysicalPlan::IndexFastScan {
        table: table.clone(),
        index: chosen.clone(),
        low: None,
        high: None,
        filter,
        estimated_rows: total_rows,
        access_rows: total_rows,
        cost,
        possible_keys: possible_keys.to_vec(),
        reverse: false,
        hint_note: Some(label),
        projected_columns: None,
    })
}

pub(crate) fn table_row_estimate(_db: &Database, table: &PlanTable) -> Result<f64, QueryError> {
    // Planning must stay non-blocking. When stats are stale, use the
    // catalog row count plus the modification counter instead of scanning
    // the heap for an exact count; execution paths that need exact rows
    // still do their own MVCC reads.
    //
    // `modified_rows_since_analyze` and the lock-free sidecar count rows
    // *touched*, not a signed live-row delta. Adding them to an analyzed
    // row_count makes UPDATE and DELETE grow a table forever. Keep the last
    // analyzed population as the estimate until auto-ANALYZE refreshes it.
    // For a never-analyzed/previously-empty table, the touched-row counter is
    // still the only non-blocking population signal and is used as a
    // low-confidence bootstrap estimate.
    let stats = &table.schema.stats;
    let modified = stats
        .modified_rows_since_analyze
        .saturating_add(table.stats_sidecar_delta);
    let rows = if stats.analyzed_at_unix_ms.is_some() {
        (stats.row_count as i128 + table.live_row_delta as i128).max(0) as f64
    } else if table.live_row_delta != 0 {
        table.live_row_delta.max(0) as f64
    } else {
        modified as f64
    };
    Ok(rows)
}

fn non_null_rows(table: &TableSchema, column: &str, total_rows: f64) -> f64 {
    table
        .stats
        .columns
        .get(&normalize_expr_column(column))
        .map(|stats| {
            table.stats.row_count.saturating_sub(stats.null_count) as f64
                * stats_scale(table, total_rows)
        })
        .unwrap_or(total_rows)
        .clamp(0.0, total_rows)
}

/// Classify the **weakest** statistics tier the row-count estimate for
/// `expr` relies on, for EXPLAIN FORMAT=JSON's `rows_estimation_source`
/// field (Phase 2b). Mirrors `estimate_filter_rows`' branch structure but
/// only reports provenance — it never recomputes the estimate, so it
/// cannot perturb the battle-tested cost numbers. When a predicate mixes
/// tiers (one leaf hits a histogram, another falls back to a default
/// constant) the worst tier wins: the field is meant to flag "did the
/// planner actually have statistics where it mattered, or guess".
pub(crate) fn filter_rows_source(
    table: &TableSchema,
    expr: &Expr,
) -> crate::engine::cbo::SelectivitySource {
    use crate::engine::cbo::SelectivitySource as S;
    match expr {
        Expr::Binary { left, op, right } if *op == BinaryOp::And || *op == BinaryOp::Or => {
            worst_selectivity_source(
                filter_rows_source(table, left),
                filter_rows_source(table, right),
            )
        }
        Expr::Binary { left, op, right } => {
            if let (Some(column), Some(value)) = (expr_column_name(left), expr_literal_value(right))
            {
                classify_column_source(table, column, *op, &value)
            } else if let (Some(value), Some(column)) =
                (expr_literal_value(left), expr_column_name(right))
            {
                classify_column_source(table, column, flip_cmp(*op), &value)
            } else {
                S::Default
            }
        }
        Expr::Between { expr, .. } => match expr_column_name(expr) {
            // Range provenance keys off the column's histogram / min-max,
            // independent of the exact bounds.
            Some(column) => classify_column_source(table, column, BinaryOp::Gt, &Value::Null),
            None => S::Default,
        },
        Expr::IsNull(e) | Expr::IsNotNull(e) => match expr_column_name(e)
            .and_then(|c| table.stats.columns.get(&normalize_expr_column(c)))
        {
            // Null counts come straight from ColumnStats when present.
            Some(_) => S::Ndv,
            None => S::Default,
        },
        Expr::InList { expr, list, .. } => {
            let Some(column) = expr_column_name(expr) else {
                return S::Default;
            };
            list.iter()
                .filter_map(expr_literal_value)
                .map(|v| classify_column_source(table, column, BinaryOp::Eq, &v))
                .reduce(worst_selectivity_source)
                .unwrap_or(S::Default)
        }
        Expr::Like { expr, .. } => match expr_column_name(expr)
            .and_then(|c| table.stats.columns.get(&normalize_expr_column(c)))
        {
            Some(stats) if !stats.histogram.is_empty() => S::Histogram,
            Some(_) => S::Ndv,
            None => S::Default,
        },
        Expr::Unary { expr, .. } => filter_rows_source(table, expr),
        _ => S::Default,
    }
}

/// Combine two provenance tiers, keeping the least-confident one
/// (`Default` < `Ndv` < `Histogram`).
fn worst_selectivity_source(
    a: crate::engine::cbo::SelectivitySource,
    b: crate::engine::cbo::SelectivitySource,
) -> crate::engine::cbo::SelectivitySource {
    use crate::engine::cbo::SelectivitySource as S;
    fn rank(s: S) -> u8 {
        match s {
            S::Default => 0,
            S::Ndv => 1,
            S::Histogram => 2,
        }
    }
    if rank(a) <= rank(b) {
        a
    } else {
        b
    }
}

/// Provenance tier for a single `column <op> value` leaf, matching the
/// decision points inside `estimate_eq_rows` / `estimate_range_rows_for_column`.
fn classify_column_source(
    table: &TableSchema,
    column: &str,
    op: BinaryOp,
    value: &Value,
) -> crate::engine::cbo::SelectivitySource {
    use crate::engine::cbo::SelectivitySource as S;
    let Some(stats) = table.stats.columns.get(&normalize_expr_column(column)) else {
        return S::Default;
    };
    match op {
        BinaryOp::Eq | BinaryOp::NotEq => {
            if !matches!(value, Value::Null)
                && stats
                    .top_values
                    .iter()
                    .any(|f| stats_values_equal(&f.value, value))
            {
                S::Histogram
            } else if stats.distinct_count > 0 {
                S::Ndv
            } else {
                S::Default
            }
        }
        BinaryOp::Gt | BinaryOp::GtEq | BinaryOp::Lt | BinaryOp::LtEq => {
            if !stats.histogram.is_empty() {
                S::Histogram
            } else if stats.min.is_some() && stats.max.is_some() {
                S::Ndv
            } else {
                S::Default
            }
        }
        _ => S::Default,
    }
}

/// Pull `col = const` out of a conjunct, returning the normalized column
/// name and the literal value (either operand order).
fn equality_column_value(expr: &Expr) -> Option<(String, Value)> {
    if let Expr::Binary { left, op, right } = expr {
        if *op == BinaryOp::Eq {
            if let (Some(col), Some(val)) = (expr_column_name(left), expr_literal_value(right)) {
                return Some((normalize_expr_column(col), val));
            }
            if let (Some(val), Some(col)) = (expr_literal_value(left), expr_column_name(right)) {
                return Some((normalize_expr_column(col), val));
            }
        }
    }
    None
}

/// Pull a one-sided ordered bound out of a conjunct. The operator is
/// normalized to `column OP literal`, regardless of the operand order in the
/// SQL text.
fn range_column_value(expr: &Expr) -> Option<(String, BinaryOp, Value)> {
    let Expr::Binary { left, op, right } = expr else {
        return None;
    };
    if !matches!(
        op,
        BinaryOp::Gt | BinaryOp::GtEq | BinaryOp::Lt | BinaryOp::LtEq
    ) {
        return None;
    }
    if let (Some(column), Some(value)) = (expr_column_name(left), expr_literal_value(right)) {
        return Some((normalize_expr_column(column), *op, value));
    }
    if let (Some(value), Some(column)) = (expr_literal_value(left), expr_column_name(right)) {
        return Some((normalize_expr_column(column), flip_cmp(*op), value));
    }
    None
}

#[derive(Default)]
struct ConjunctRangeBounds {
    lower: Option<(BinaryOp, Value)>,
    upper: Option<(BinaryOp, Value)>,
    conjuncts: Vec<usize>,
    comparable: bool,
}

fn tighter_range_bound(
    candidate_op: BinaryOp,
    candidate: &Value,
    current_op: BinaryOp,
    current: &Value,
    lower: bool,
    collation: Option<&str>,
) -> Option<bool> {
    use std::cmp::Ordering;
    let ordering =
        crate::storage::partitioning::compare_value_with_collation(candidate, current, collation);
    Some(if lower {
        ordering == Ordering::Greater
            || (ordering == Ordering::Equal
                && candidate_op == BinaryOp::Gt
                && current_op == BinaryOp::GtEq)
    } else {
        ordering == Ordering::Less
            || (ordering == Ordering::Equal
                && candidate_op == BinaryOp::Lt
                && current_op == BinaryOp::LtEq)
    })
}

/// A bounded, fully constant domain for cardinality estimation. An unresolved
/// expression is not silently dropped from IN: its selectivity stays on the
/// existing fallback path. NULL never survives an equality/positive-IN WHERE.
fn estimation_finite_domain(table: &TableSchema, expr: &Expr) -> Option<(String, Vec<Value>)> {
    let (column, values) = if let Some((column, value)) = equality_column_value(expr) {
        (column, vec![value])
    } else if let Expr::InList {
        expr,
        list,
        negated: false,
    } = expr
    {
        if list.len() > 256 {
            return None;
        }
        (
            normalize_expr_column(expr_column_name(expr)?),
            list.iter()
                .map(expr_literal_value)
                .collect::<Option<Vec<_>>>()?,
        )
    } else {
        return None;
    };
    let schema = table
        .columns
        .iter()
        .find(|c| c.name.eq_ignore_ascii_case(&column));
    let mut unique = Vec::new();
    for value in values {
        if matches!(value, Value::Null) {
            continue;
        }
        let value = schema
            .and_then(|c| {
                crate::engine::expr::coerce_comparison_constant_for_type(&value, &c.data_type)
            })
            .unwrap_or(value);
        if !unique
            .iter()
            .any(|seen| estimation_domain_values_equal(table, &column, seen, &value))
        {
            unique.push(value);
        }
    }
    Some((column, unique))
}

fn estimation_domain_values_equal(
    table: &TableSchema,
    column: &str,
    left: &Value,
    right: &Value,
) -> bool {
    let collation = table
        .columns
        .iter()
        .find(|c| c.name.eq_ignore_ascii_case(column))
        .and_then(|c| c.collation.as_deref())
        .map(crate::charset::collation_id);
    crate::engine::expr::with_compare_collation(collation, || stats_values_equal(left, right))
}

fn expr_literal_string(expr: &Expr) -> Option<String> {
    match expr {
        Expr::Literal(crate::parser::Literal::String(s)) => Some(s.clone()),
        _ => None,
    }
}

fn unanchored_like_literal_tail(literal_chars: usize) -> usize {
    const DEFAULT_LIKE_LITERAL_CHARS: usize = 8;
    literal_chars.saturating_sub(DEFAULT_LIKE_LITERAL_CHARS)
}

/// B8: shrink `base` by `(1 - selectivity)^literal_chars`, clamped to
/// [1, total]. PG-style: each literal char multiplies the surviving
/// row fraction. Floor of 1 prevents "zero rows" plans that flip to
/// index-lookup-and-abort shapes on truly non-selective LIKEs.
fn apply_like_residual_discount(base: f64, literal_chars: usize) -> f64 {
    if base <= 0.0 {
        return 0.0;
    }
    if literal_chars == 0 {
        return base.max(0.0);
    }
    let sel = crate::engine::cbo::cbo_consts()
        .like_char_selectivity()
        .clamp(0.0, 1.0);
    // 0 or 1 selectivity would collapse to 0 rows / no discount; keep
    // the multiplier strictly inside (0, 1) so the geometric decay
    // stays finite.
    let survive = (1.0 - sel).clamp(0.001, 0.999);
    let factor = survive.powi(literal_chars as i32);
    (base * factor).max(1.0)
}

/// Cardinality tiers for parameterized predicates. Deferred keys preserve
/// result correctness across bindings, but a selective lookup plan must not
/// be shared with a hot value that should use a broad scan/hash join. Powers
/// of two bound the number of variants without keying the cache by values.
/// Unique point lookups keep their existing cross-value sharing.
pub(crate) fn parameter_selectivity_signature(
    db: &Database,
    select: &Select,
    params: &[Value],
) -> String {
    use std::fmt::Write as _;
    if params.is_empty() {
        return String::new();
    }
    let mut tables = smallvec::SmallVec::<[(&str, Option<&str>, Arc<TableSchema>); 4]>::new();
    for source in select
        .from
        .iter()
        .chain(select.joins.iter().map(|j| &j.table))
    {
        if let Ok(schema) = db.table(&source.name) {
            tables.push((&source.name, source.alias.as_deref(), schema));
        }
    }
    if tables.is_empty() {
        if let Some(name) = select.table.as_deref() {
            if let Ok(schema) = db.table(name) {
                tables.push((name, None, schema));
            }
        }
    }
    let mut signature = String::new();
    let mut append = |column_expr: &Expr,
                      parameter: &Expr,
                      op: BinaryOp,
                      second: Option<(&Expr, BinaryOp)>| {
        let Expr::Placeholder(index) = parameter else {
            return;
        };
        let Some(value) = params.get(*index) else {
            return;
        };
        let Some(column) = expr_column_name(column_expr) else {
            return;
        };
        let column = normalize_expr_column(column);
        let qualifier = match column_expr {
            Expr::Identifier(_) => None,
            Expr::CompoundIdentifier(parts) if parts.len() >= 2 => {
                Some(parts[..parts.len() - 1].join("."))
            }
            _ => return,
        };
        let mut matches = tables.iter().filter(|(name, alias, schema)| {
            schema.find_column_index(&column).is_some()
                && qualifier
                    .as_deref()
                    .is_none_or(|q| matches_table(q, name, *alias))
        });
        let Some((_, _, schema)) = matches.next() else {
            return;
        };
        if matches.next().is_some() {
            return;
        }
        let Some(stats) = schema.stats.columns.get(&column) else {
            return;
        };
        if stats.top_values.is_empty() {
            return;
        }
        if matches!(op, BinaryOp::Eq | BinaryOp::NullSafeEq)
            && schema.iter_queryable_indexes().any(|i| {
                i.unique && i.columns.len() == 1 && normalize_expr_column(&i.columns[0]) == column
            })
        {
            return;
        }
        // A conjunctive interval has one cardinality. Bucketing its two
        // half-lines independently needlessly invalidates a prepared memo as
        // a narrow window moves through the domain, even when its cost stays
        // in the same tier. Use the planner's joint range oracle instead.
        let second = second.and_then(|(expr, op)| match expr {
            Expr::Placeholder(index) => params.get(*index).map(|value| (*index, op, value)),
            _ => None,
        });
        let rows = if let Some((_, second_op, second_value)) = second {
            estimate_range_rows_for_column(
                schema,
                &column,
                Some((op, value)),
                Some((second_op, second_value)),
                schema.stats.row_count as f64,
            )
        } else {
            estimate_column_predicate_rows(
                schema,
                &column,
                op,
                value,
                schema.stats.row_count as f64,
            )
        };
        let rows = rows.ceil().max(0.0) as u64;
        let bucket = if rows == 0 {
            0
        } else {
            1 + (u64::BITS - rows.saturating_sub(1).leading_zeros())
        };
        if let Some((second_index, _, _)) = second {
            let _ = write!(signature, ":r{index},{second_index}={bucket}");
        } else {
            let _ = write!(signature, ":s{index}={bucket}");
        }
    };
    fn walk(
        expr: &Expr,
        append: &mut impl FnMut(&Expr, &Expr, BinaryOp, Option<(&Expr, BinaryOp)>),
        allow_joint: bool,
    ) {
        fn bound(expr: &Expr) -> Option<(&Expr, &Expr, BinaryOp)> {
            let Expr::Binary { left, op, right } = expr else {
                return None;
            };
            if !matches!(
                op,
                BinaryOp::Gt | BinaryOp::GtEq | BinaryOp::Lt | BinaryOp::LtEq
            ) {
                return None;
            }
            if matches!(right.as_ref(), Expr::Placeholder(_)) && expr_column_name(left).is_some() {
                Some((left, right, *op))
            } else if matches!(left.as_ref(), Expr::Placeholder(_))
                && expr_column_name(right).is_some()
            {
                Some((right, left, flip_cmp(*op)))
            } else {
                None
            }
        }
        fn lower(op: BinaryOp) -> bool {
            matches!(op, BinaryOp::Gt | BinaryOp::GtEq)
        }
        fn conjuncts<'a>(expr: &'a Expr, terms: &mut smallvec::SmallVec<[&'a Expr; 8]>) -> bool {
            if let Expr::Binary {
                left,
                op: BinaryOp::And,
                right,
            } = expr
            {
                conjuncts(left, terms) && conjuncts(right, terms)
            } else {
                // Bound matching stays within the inline scratch capacity;
                // large conjunctions retain the existing linear signature walk.
                if terms.len() == terms.inline_size() {
                    return false;
                }
                terms.push(expr);
                true
            }
        }
        match expr {
            Expr::Binary {
                op: BinaryOp::And, ..
            } if allow_joint => {
                let mut terms = smallvec::SmallVec::<[&Expr; 8]>::new();
                if !conjuncts(expr, &mut terms) {
                    walk(expr, append, false);
                    return;
                }
                let mut paired = smallvec::SmallVec::<[usize; 8]>::new();
                for (index, term) in terms.iter().enumerate() {
                    if paired.contains(&index) {
                        continue;
                    }
                    if let Some((column, parameter, op)) = bound(term) {
                        let peers = terms
                            .iter()
                            .enumerate()
                            .filter_map(|(other, term)| {
                                let (other_column, other_parameter, other_op) = bound(term)?;
                                (other_column == column).then_some((
                                    other,
                                    other_parameter,
                                    other_op,
                                ))
                            })
                            .collect::<smallvec::SmallVec<[(usize, &Expr, BinaryOp); 4]>>();
                        // More than one bound on either side retains the old
                        // conservative signature; OR/NOT never enter this group.
                        if peers.len() == 2 {
                            if let Some(&(other, other_parameter, other_op)) =
                                peers.iter().find(|(other, _, other_op)| {
                                    *other != index && lower(*other_op) != lower(op)
                                })
                            {
                                append(column, parameter, op, Some((other_parameter, other_op)));
                                paired.push(other);
                                continue;
                            }
                        }
                    }
                    walk(term, append, allow_joint);
                }
            }
            Expr::Binary { left, op, right } => {
                if matches!(
                    op,
                    BinaryOp::Eq
                        | BinaryOp::NullSafeEq
                        | BinaryOp::NotEq
                        | BinaryOp::Lt
                        | BinaryOp::LtEq
                        | BinaryOp::Gt
                        | BinaryOp::GtEq
                ) {
                    append(left, right, *op, None);
                    append(right, left, flip_cmp(*op), None);
                }
                walk(left, append, allow_joint);
                walk(right, append, allow_joint);
            }
            Expr::InList { expr, list, .. } => {
                for value in list {
                    append(expr, value, BinaryOp::Eq, None);
                }
                walk(expr, append, allow_joint);
            }
            Expr::Between {
                expr,
                low,
                high,
                negated,
            } => {
                if allow_joint
                    && !*negated
                    && matches!(low.as_ref(), Expr::Placeholder(_))
                    && matches!(high.as_ref(), Expr::Placeholder(_))
                {
                    append(expr, low, BinaryOp::GtEq, Some((high, BinaryOp::LtEq)));
                } else {
                    append(expr, low, BinaryOp::GtEq, None);
                    append(expr, high, BinaryOp::LtEq, None);
                }
            }
            Expr::Unary {
                op: UnaryOp::Not,
                expr,
            } => walk(expr, append, false),
            Expr::Unary { expr, .. } | Expr::IsNull(expr) | Expr::IsNotNull(expr) => {
                walk(expr, append, allow_joint)
            }
            Expr::Function { args, .. } | Expr::Tuple(args) => {
                for arg in args {
                    walk(arg, append, allow_joint);
                }
            }
            _ => {}
        }
    }
    for predicate in select
        .selection
        .iter()
        .chain(select.having.iter())
        .chain(select.joins.iter().filter_map(|j| j.condition.as_ref()))
    {
        walk(predicate, &mut append, true);
    }
    signature
}

/// Statistics retain the physical value lane decoded from the column, while
/// literals retain the parser's lane. SQL-equal numerics therefore need not
/// be enum-equal (`DECIMAL(5,2) -5.00` versus integer literal `-5`). Using
/// executor comparison semantics here keeps TOP-K matching aligned with the
/// predicate that will actually run and prevents an existing heavy value from
/// being mistaken for an absent residual value.
fn stats_values_equal(left: &Value, right: &Value) -> bool {
    crate::engine::expr::compare_values(left, right)
        .is_ok_and(|ordering| ordering == std::cmp::Ordering::Equal)
}

fn single_column_lookup_cardinality(
    table: &TableSchema,
    column: &str,
    index_cardinality: u64,
) -> u64 {
    let mut cardinality = index_cardinality;
    if let Some(column_ndv) = table
        .stats
        .columns
        .get(&normalize_expr_column(column))
        .map(|stats| stats.distinct_count)
        .filter(|ndv| *ndv > 0)
    {
        cardinality = cardinality.min(column_ndv);
    }
    if let Some(cap) = numeric_domain_cardinality_cap(table, column) {
        cardinality = cardinality.min(cap);
    }
    cardinality.max(1)
}

fn integer_stat_value(value: &Value) -> Option<i128> {
    match value {
        Value::Int(v) => Some(i128::from(*v)),
        Value::UInt(v) => Some(i128::from(*v)),
        _ => None,
    }
}

fn integer_column_bounds(table: &TableSchema, column: &str) -> Option<(i128, i128)> {
    let column = normalize_expr_column(column);
    if !table
        .columns
        .get(table.column_index(&column).ok()?)?
        .data_type
        .is_integer()
    {
        return None;
    }
    let stats = table.stats.columns.get(&column)?;
    let min = integer_stat_value(stats.min.as_ref()?)?;
    let max = integer_stat_value(stats.max.as_ref()?)?;
    (max >= min).then_some((min, max))
}

fn numeric_domain_cardinality_cap(table: &TableSchema, column: &str) -> Option<u64> {
    // Integer subtraction before conversion preserves adjacent BIGINT keys
    // above 2^53. Strings, fractional numerics and temporal values are not
    // bounded by max-min+1 in this estimator's coordinate system.
    let (min, max) = integer_column_bounds(table, column)?;
    Some((max - min + 1).min(i128::from(u64::MAX)) as u64)
}

fn dense_integer_value(value: &Value) -> Option<f64> {
    let integer = integer_stat_value(value)?;
    (integer.abs() <= 9_007_199_254_740_991).then_some(integer as f64)
}

fn dense_integer_column_bounds(table: &TableSchema, column: &str) -> Option<(f64, f64)> {
    let (min, max) = integer_column_bounds(table, column)?;
    if min.abs().max(max.abs()) > 9_007_199_254_740_991 {
        return None;
    }
    Some((min as f64, max as f64))
}

fn stats_scale(table: &TableSchema, total_rows: f64) -> f64 {
    let analyzed = table.stats.row_count as f64;
    if analyzed > 0.0 {
        (total_rows / analyzed).max(0.0)
    } else {
        1.0
    }
}

fn default_eq_rows(total_rows: f64) -> f64 {
    let sel = crate::engine::cbo::cbo_consts().default_eq_selectivity();
    (total_rows * sel).clamp(0.0, total_rows.max(0.0))
}

fn default_range_rows(total_rows: f64) -> f64 {
    let sel = crate::engine::cbo::cbo_consts().default_range_selectivity();
    (total_rows * sel).clamp(0.0, total_rows.max(0.0))
}

fn default_like_rows(total_rows: f64) -> f64 {
    let sel = crate::engine::cbo::cbo_consts().default_like_selectivity();
    (total_rows * sel).clamp(0.0, total_rows.max(0.0))
}

fn default_filter_rows(total_rows: f64) -> f64 {
    default_range_rows(total_rows)
}

fn default_group_by_rows(input_rows: f64) -> f64 {
    if input_rows <= 0.0 {
        return 0.0;
    }
    (input_rows * 0.01).min(100.0).clamp(0.0, input_rows)
}

/// The vector key-only aggregate uses a contiguous fixed-width set for up to
/// eight signed integer/temporal columns, regardless of its memory grant.
/// Cost that physical state rather than generic Value/accumulator buckets;
/// otherwise high-NDV deduplication is sent to an unnecessary external sort.
fn compact_key_only_group_state_bytes(
    plan: &PhysicalPlan,
    group_by: &[Expr],
    aggregates: &[Expr],
) -> Option<f64> {
    use crate::engine::expr_type::ExprTypeKind;
    if !aggregates.is_empty() || group_by.is_empty() || group_by.len() > 8 {
        return None;
    }
    for expr in group_by {
        let expr = match expr {
            Expr::GroupKeyRef { expr, .. } => expr,
            other => other,
        };
        if expr_column_name(expr).is_none()
            || !matches!(
                crate::engine::executor::select::plan::physical_expr_type(plan, expr).kind,
                ExprTypeKind::Integer {
                    unsigned: false,
                    ..
                } | ExprTypeKind::Temporal { .. }
            )
        {
            return None;
        }
    }
    // Keys + hash + null mask + partition row id, with average geometric
    // Vec capacity slack, plus a u32 probe slot at the arena's 70% load.
    Some((group_by.len() as f64 * 8.0 + 8.0 + 1.0 + 4.0) * 1.5 + 4.0 / 0.7)
}

/// Estimate the complete resident accumulator state per output group. Plain
/// accumulators are fixed-size; DISTINCT additionally owns one hash entry per
/// distinct argument value. Keeping that variable component separate fixes
/// the implicit-group case where `group_count == 1` previously reported only
/// the 104-byte empty-set header for a multi-million-entry set.
fn aggregate_state_bytes_per_group_for_plan(
    plan: &PhysicalPlan,
    aggregates: &[Expr],
    input_rows: f64,
    group_count: f64,
) -> Option<f64> {
    let specs: Vec<(String, bool)> = aggregates
        .iter()
        .filter_map(|expr| match expr {
            Expr::Function { name, distinct, .. } if is_aggregate_function(name) => {
                Some((name.clone(), *distinct))
            }
            _ => None,
        })
        .collect();
    if specs.is_empty() {
        return None;
    }
    let mut bytes = crate::engine::cbo::operator_cost::aggregate_state_bytes_from_specs(&specs);
    let groups = group_count.max(1.0);
    for aggregate in aggregates {
        let Expr::Function {
            name,
            args,
            distinct: true,
        } = aggregate
        else {
            continue;
        };
        let direct_column = args.first().and_then(expr_column_name);
        let argument_ndv = direct_column
            .and_then(|column| ndv_in_plan(plan, column))
            .map(|ndv| ndv as f64)
            // Unknown expression NDV: reserve for a meaningful fraction of
            // input rather than pretending the set has only one member. The
            // runtime governor remains the hard bound and can spill sooner.
            .unwrap_or_else(|| (input_rows * 0.10).max(1.0));
        let total_entries = input_rows.max(0.0).min(argument_ndv.max(1.0) * groups);
        let entries_per_group = total_entries / groups;
        let width = direct_column
            .and_then(|column| average_column_width_in_plan(plan, column))
            .unwrap_or(32.0)
            .clamp(1.0, 4096.0);
        // hashbrown control bytes/alignment + the concrete key/value headers.
        // COUNT DISTINCT's compact state stores only its canonical key;
        // SUM/AVG DISTINCT retain both key and representative value.
        let entry_bytes = if name.eq_ignore_ascii_case("COUNT") {
            96.0 + width
        } else {
            144.0 + width * 2.0
        };
        bytes += entries_per_group * entry_bytes;
    }
    Some(bytes)
}

fn grouping_sets_match_rollup_all(group_by_len: usize, grouping_sets: &[Vec<usize>]) -> bool {
    if grouping_sets.len() != group_by_len + 1 {
        return false;
    }
    grouping_sets.iter().enumerate().all(|(pos, set)| {
        let prefix_len = group_by_len - pos;
        set.len() == prefix_len && set.iter().copied().eq(0..prefix_len)
    })
}

fn grouping_sets_are_prefix_chain(grouping_sets: &[Vec<usize>]) -> bool {
    grouping_sets.iter().all(|set| {
        set.iter()
            .enumerate()
            .all(|(expected, actual)| expected == *actual)
    })
}

fn histogram_estimate_in_range(
    histogram: &[crate::storage::HistogramBucket],
    column_min: f64,
    low: f64,
    high: f64,
    low_inclusive: bool,
    high_inclusive: bool,
) -> f64 {
    if histogram.is_empty() {
        return 0.0;
    }
    let mut prev_upper = column_min;
    let mut prev_cumulative: u64 = 0;
    let mut accum: f64 = 0.0;
    for bucket in histogram {
        let upper = numeric_stat_value(&bucket.upper);
        let bucket_count = bucket.cumulative_count.saturating_sub(prev_cumulative) as f64;
        let span = (upper - prev_upper).max(f64::EPSILON);
        let overlap_low = low.max(prev_upper);
        let overlap_high = high.min(upper);
        if upper == prev_upper {
            if (upper > low || (low_inclusive && upper == low))
                && (upper < high || (high_inclusive && upper == high))
            {
                accum += bucket_count;
            }
        } else if overlap_high >= overlap_low {
            let frac = ((overlap_high - overlap_low) / span).clamp(0.0, 1.0);
            accum += bucket_count * frac;
        }
        prev_upper = upper;
        prev_cumulative = bucket.cumulative_count;
    }
    accum
}

fn numeric_stat_value_opt(value: &Value) -> Option<f64> {
    match value {
        Value::Int(v) => Some(*v as f64),
        Value::UInt(v) => Some(*v as f64),
        Value::Float(v) => Some(*v as f64),
        Value::Double(v) => Some(*v),
        Value::Decimal(v) => v.parse().ok(),
        Value::DecimalCompact(mag, scale) => Some(*mag as f64 / 10f64.powi(*scale as i32)),
        Value::Decimal128(mag, scale) => Some(*mag as f64 / 10f64.powi(*scale as i32)),
        Value::DecimalWide(value) => {
            crate::util::decimal::format_big_decimal_parts(&value.magnitude, value.scale)
                .parse()
                .ok()
        }
        Value::String(s) => parse_temporal_or_numeric_opt(s),
        Value::SmallString(b) => {
            parse_temporal_or_numeric_opt(unsafe { std::str::from_utf8_unchecked(b) })
        }
        Value::Bytes(b) => std::str::from_utf8(b)
            .ok()
            .and_then(parse_temporal_or_numeric_opt),
        Value::Date(days) => Some(*days as f64),
        Value::DateTime(micros) | Value::Timestamp(micros) | Value::Time(micros) => {
            Some(*micros as f64 / 1_000_000.0)
        }
        Value::Year(b) => Some(1900.0 + *b as f64),
        Value::Json(_) | Value::Geometry(_) | Value::Vector(_) | Value::Null => None,
    }
}

fn numeric_stat_value(value: &Value) -> f64 {
    numeric_stat_value_opt(value).unwrap_or_else(|| match value {
        Value::Int(v) => *v as f64,
        Value::UInt(v) => *v as f64,
        Value::Float(v) => *v as f64,
        Value::Double(v) => *v,
        Value::Decimal(v) => v.parse().unwrap_or(0.0),
        Value::DecimalCompact(mag, scale) => *mag as f64 / 10f64.powi(*scale as i32),
        Value::Decimal128(mag, scale) => *mag as f64 / 10f64.powi(*scale as i32),
        Value::DecimalWide(value) => {
            crate::util::decimal::format_big_decimal_parts(&value.magnitude, value.scale)
                .parse()
                .unwrap_or(0.0)
        }
        Value::String(s) => parse_temporal_or_numeric(s),
        Value::SmallString(b) => {
            parse_temporal_or_numeric(unsafe { std::str::from_utf8_unchecked(b) })
        }
        Value::Bytes(b) => std::str::from_utf8(b)
            .map(parse_temporal_or_numeric)
            .unwrap_or(0.0),
        // Binary temporal variants (introduced alongside the row codec
        // v3 zero-round-trip work). These must map to the SAME numeric
        // scale that `parse_temporal_or_numeric` produces for the
        // string form, otherwise stats (min/max/histogram) and user
        // bounds (which still arrive as `Value::String("YYYY-MM-DD…")`
        // from SQL literals) won't be comparable, and range estimation
        // collapses to `min==max==0.0` → rows=1.
        Value::Date(days) => *days as f64,
        Value::DateTime(micros) => *micros as f64 / 1_000_000.0,
        Value::Timestamp(micros) => *micros as f64 / 1_000_000.0,
        Value::Time(micros) => *micros as f64 / 1_000_000.0,
        Value::Year(b) => 1900.0 + *b as f64,
        Value::Json(b) => crate::engine::json::coerce_f64(b),
        Value::Geometry(_) => 0.0,
        Value::Vector(_) => 0.0,
        Value::Null => 0.0,
    })
}

/// Best-effort numeric coercion for stats / cardinality estimation.
/// TPC-H stores DATE/TIMESTAMP columns as strings (e.g. "1995-01-01"),
/// and `estimate_range_rows_for_column` collapses to a constant 1.0
/// when min/max can't be coerced — which catastrophically pessimises
/// `BETWEEN date_a AND date_b` plans on lineitem etc. Parse the
/// common temporal forms first, then fall back to plain f64.
fn parse_temporal_or_numeric_opt(s: &str) -> Option<f64> {
    let trimmed = s.trim();
    if let Ok(days) = chrono::NaiveDate::parse_from_str(trimmed, "%Y-%m-%d") {
        // Use ordinal day count from CE for a stable monotonic mapping.
        return Some(
            days.signed_duration_since(
                chrono::NaiveDate::from_ymd_opt(1970, 1, 1).expect("epoch valid"),
            )
            .num_days() as f64,
        );
    }
    if let Ok(dt) = chrono::NaiveDateTime::parse_from_str(trimmed, "%Y-%m-%d %H:%M:%S") {
        return Some(dt.and_utc().timestamp() as f64);
    }
    if let Ok(dt) = chrono::NaiveDateTime::parse_from_str(trimmed, "%Y-%m-%dT%H:%M:%S") {
        return Some(dt.and_utc().timestamp() as f64);
    }
    trimmed.parse::<f64>().ok()
}

fn parse_temporal_or_numeric(s: &str) -> f64 {
    parse_temporal_or_numeric_opt(s).unwrap_or(0.0)
}

/// Extra context passed to access-path selection so an order-preserving index
/// and an unordered scan plus its blocking order operator are costed as
/// complete alternatives. Row goals model rows actually fetched rather than
/// rows that would be scanned without an early stop.
#[derive(Debug, Clone)]
struct ScanHints<'a> {
    order_by: &'a [OrderBy],
    group_by: &'a [Expr],
    /// Accumulator-state estimate collected from the actual aggregate calls.
    /// Scan enumeration uses it to compare the complete HashAgg alternative
    /// with an order-preserving streaming aggregate.
    aggregate_state_bytes_per_group: Option<f64>,
    /// LIMIT + OFFSET for a non-blocking single-table ORDER BY shape. An index
    /// that provides the requested order can stop after this goal; an
    /// unordered scan must include TopK/Sort before its cost is comparable.
    limit_with_offset: Option<usize>,
    /// Unordered row goal for LIMIT/OFFSET over a non-blocking
    /// single-table plan. This lets scan costing model early stop
    /// without promising any stable row order.
    row_goal: Option<usize>,
    required_columns: Option<HashSet<String>>,
    index_hints: &'a [IndexHint],
    query_hints: &'a [QueryHint],
    table_name: &'a str,
    fulltext_matches: &'a [Expr],
    join_predicates: &'a [Expr],
}

/// Branch-and-bound frontier for access-path enumeration. Cheap candidates
/// publish the cost of a complete executable plan; an expensive candidate is
/// expanded only when its optimistic lower bound can still improve that
/// incumbent. The abstraction intentionally knows nothing about uniqueness or
/// access shape, so lookup, multi-lookup, range and sequential paths compete
/// under the same rule.
#[derive(Debug, Clone, Copy)]
struct AccessPathFrontier {
    upper_bound: f64,
}

impl AccessPathFrontier {
    fn new(initial: f64) -> Self {
        Self {
            upper_bound: if initial.is_finite() {
                initial.max(0.0)
            } else {
                f64::INFINITY
            },
        }
    }

    fn consider(&mut self, complete_plan_cost: f64) {
        if complete_plan_cost.is_finite() && complete_plan_cost >= 0.0 {
            self.upper_bound = self.upper_bound.min(complete_plan_cost);
        }
    }

    fn should_expand(&self, optimistic_lower_bound: f64, forced: bool) -> bool {
        forced || !optimistic_lower_bound.is_finite() || optimistic_lower_bound < self.upper_bound
    }
}

/// Constant-time first tier for Zone Map branch-and-bound. ANALYZE's physical
/// heap-page count is already the common population used to cost the incumbent
/// sequential and index paths. Derive the optimizer's bounded sampling work
/// from that same snapshot before touching live Zone Map metadata. Ambiguous
/// cases fall through to `Database::zone_map_cost_lower_bound`, which uses the
/// actual storage objects and trust state but still does not inspect zones.
fn zone_map_sampling_cpu_floor_from_stats(table: &TableSchema) -> Option<f64> {
    if table.is_partitioned() || table.stats.heap_page_count == 0 {
        return None;
    }
    let total_zones = table
        .stats
        .heap_page_count
        .div_ceil(u64::from(table.zone_pages()).max(1));
    if total_zones == 0 {
        return Some(0.0);
    }
    let sample_cap = if total_zones < 64 {
        total_zones
    } else {
        4096.min(total_zones)
    };
    let step = total_zones.div_ceil(sample_cap).max(1);
    let samples = total_zones.div_ceil(step);
    Some(samples as f64 * crate::engine::cbo::cbo_consts().cpu_operator_cost())
}

impl<'a> ScanHints<'a> {
    fn none() -> ScanHints<'static> {
        ScanHints {
            order_by: &[],
            group_by: &[],
            aggregate_state_bytes_per_group: None,
            limit_with_offset: None,
            row_goal: None,
            required_columns: None,
            index_hints: &[],
            query_hints: &[],
            table_name: "",
            fulltext_matches: &[],
            join_predicates: &[],
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) struct IndexAccess {
    pub(crate) index: IndexSchema,
    pub(crate) kind: IndexAccessKind,
}

#[derive(Debug, Clone)]
struct InSubqueryIndexAccess {
    index: IndexSchema,
    key_prefix: IndexKey,
    in_column: String,
    subquery: Select,
}

#[derive(Debug, Clone)]
struct CostedInSubqueryIndexAccess {
    access: InSubqueryIndexAccess,
    lookup_count: f64,
    access_rows: f64,
    estimated_rows: f64,
    cost: crate::engine::cbo::Cost,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum IndexAccessKind {
    Lookup {
        key: IndexKey,
        exact: bool,
    },
    MultiLookup {
        keys: Vec<IndexKey>,
        exact: bool,
    },
    Range {
        low: Option<IndexBound>,
        high: Option<IndexBound>,
    },
}

#[inline]
fn constraint_key_part_for_index(
    value: &PlanConstraintValue,
    target: crate::storage::ParamTarget,
    index: &IndexSchema,
    pos: usize,
) -> Option<KeyPart> {
    // Plan-time fast path: indexes without prefix metadata skip the
    // per-call `column_prefix_len` chain and the truncation pass.
    if index.column_prefix_lengths.is_empty() {
        return Some(value.clone().into_key_part(target));
    }
    let part = value.clone().into_key_part(target);
    // Concrete literal: truncate immediately so the plan-cached
    // IndexKey is already at the column's prefix width. Deferred
    // `KeyPart::Param` parts can't be truncated until the param is
    // bound, so they pass through unchanged — `resolve_index_key` /
    // `resolve_index_bound` at the storage boundary do the truncation
    // after `KeyPart::resolve_param` materialises the value.
    if matches!(value, PlanConstraintValue::Param(_)) {
        return Some(part);
    }
    Some(index.apply_prefix_to_key_part(pos, part))
}

/// True for a constraint that is a NULL literal. `col = NULL` (and any other
/// non-null-safe comparison against a NULL literal) is UNKNOWN for every row,
/// so it must never drive an index probe — left to the residual filter it
/// correctly returns no rows, matching MySQL. `col IS NULL` is collected
/// separately (it intentionally probes the concrete NULL key) and so is
/// unaffected by this guard.
fn plan_constraint_is_null_literal(value: &PlanConstraintValue) -> bool {
    matches!(value, PlanConstraintValue::Concrete(Value::Null))
}

fn expr_to_plan_constraint(expr: &Expr) -> Option<PlanConstraintValue> {
    match expr {
        Expr::Literal(_) | Expr::BoundValue(_) => {
            expr_literal_value(expr).map(PlanConstraintValue::Concrete)
        }
        Expr::Placeholder(idx) => Some(PlanConstraintValue::Param(*idx)),
        Expr::Unary {
            op: UnaryOp::Neg,
            expr: inner,
        } => match inner.as_ref() {
            Expr::Literal(crate::parser::Literal::Int(v)) => {
                Some(PlanConstraintValue::Concrete(Value::Int(-*v)))
            }
            _ => None,
        },
        // Fall back to constant-folding for everything else. Currently
        // covers `DATE 'x' ± INTERVAL n unit` (parsed as `DATE_ADD`/
        // `DATE_SUB(date_literal, __INTERVAL(...))`), which is the shape
        // used by TPC-H queries like `l_shipdate <= DATE '1998-12-01' -
        // INTERVAL '90' DAY`. Without this, the index constraint is
        // dropped and the planner falls back to a full table scan even
        // though the predicate could drive a covering index-only scan
        // on `idx_lineitem_shipdate`.
        _ => expr_literal_value(expr).map(PlanConstraintValue::Concrete),
    }
}

fn special_index_allowed(index: &IndexSchema, schema: &TableSchema, hints: &ScanHints<'_>) -> bool {
    index_allowed_by_hints(&index.name, hints.index_hints, IndexHintScope::Join)
        && {
            let names = query_hint_use_index(hints.query_hints, hints.table_name, &schema.name);
            names.is_empty() || names.iter().any(|n| index.name.eq_ignore_ascii_case(n))
        }
        && !query_hint_no_index(hints.query_hints, hints.table_name, &schema.name)
            .iter()
            .any(|n| index.name.eq_ignore_ascii_case(n))
}

/// Precompute optimizer hint names once per candidate enumeration; SQL
/// index-hint scope is supplied by each order/group/join access path.
struct ScanIndexHints<'a> {
    use_indexes: Vec<&'a str>,
    ignored_indexes: Vec<&'a str>,
}

impl<'a> ScanIndexHints<'a> {
    fn new(hints: &ScanHints<'a>, schema_name: &str) -> Self {
        Self {
            use_indexes: query_hint_use_index(hints.query_hints, hints.table_name, schema_name),
            ignored_indexes: query_hint_no_index(hints.query_hints, hints.table_name, schema_name),
        }
    }

    #[inline]
    fn allows(&self, name: &str, sql_hints: &[IndexHint], scope: IndexHintScope) -> bool {
        index_allowed_by_hints(name, sql_hints, scope)
            && (self.use_indexes.is_empty()
                || self
                    .use_indexes
                    .iter()
                    .any(|candidate| name.eq_ignore_ascii_case(candidate)))
            && !self
                .ignored_indexes
                .iter()
                .any(|candidate| name.eq_ignore_ascii_case(candidate))
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
enum JoinHintPref {
    None,
    ForceHash,
    NoHash,
    ForceNl,
    ForceIndexNlj,
}

fn resolve_join_hint(
    hints: &[QueryHint],
    left_tables: &[PlanTable],
    right_tables: &[PlanTable],
) -> JoinHintPref {
    for hint in hints {
        let (pref, tables) = match hint {
            QueryHint::HashJoin { tables } => (JoinHintPref::ForceHash, tables),
            QueryHint::NoHashJoin { tables } => (JoinHintPref::NoHash, tables),
            QueryHint::NlJoin { tables } => (JoinHintPref::ForceNl, tables),
            QueryHint::IndexNlj { tables } => (JoinHintPref::ForceIndexNlj, tables),
            _ => continue,
        };
        if !hint_tables_are_in_join(tables, left_tables, right_tables) {
            continue;
        }
        if matches!(pref, JoinHintPref::ForceIndexNlj)
            && !hint_mentions_right_side(tables, right_tables)
        {
            continue;
        }
        return pref;
    }
    JoinHintPref::None
}

fn hint_tables_are_in_join(
    hint_tables: &[String],
    left_tables: &[PlanTable],
    right_tables: &[PlanTable],
) -> bool {
    hint_tables.iter().all(|name| {
        left_tables
            .iter()
            .chain(right_tables.iter())
            .any(|table| hint_table_matches(name, table))
    })
}

fn hint_mentions_right_side(hint_tables: &[String], right_tables: &[PlanTable]) -> bool {
    hint_tables.iter().any(|name| {
        right_tables
            .iter()
            .any(|table| hint_table_matches(name, table))
    })
}

fn hint_table_matches(name: &str, table: &PlanTable) -> bool {
    matches_table(name, &table.schema.name, table.alias.as_deref())
}

fn best_index_access(
    ctx: &crate::engine::cbo::CboContext,
    table: &TableSchema,
    expr: &Expr,
    hints: &ScanHints<'_>,
    total_rows: f64,
    table_pages: f64,
    row_width: f64,
) -> Option<(IndexAccess, f64)> {
    let mut eq_values = HashMap::new();
    let mut ranges = HashMap::new();
    let mut in_values = HashMap::new();
    let mut eq_values_by_expr = HashMap::new();
    let mut ranges_by_expr = HashMap::new();
    let mut in_values_by_expr = HashMap::new();
    let mut multi_valued_values_by_expr = HashMap::new();
    collect_index_constraints(table, expr, &mut eq_values, &mut ranges);
    // OR-domain implications are access-path metadata only. Adding them to
    // the table filter itself would make the cardinality estimator multiply
    // an implication by its source predicate and can distort later join
    // ordering. Feed them solely to index enumeration, while the unchanged
    // original expression remains the residual and selectivity source.
    collect_implied_or_in_constraints(table, expr, &mut in_values);
    collect_index_in_constraints(table, expr, &mut in_values);
    collect_index_expression_constraints(table, expr, &mut eq_values_by_expr, &mut ranges_by_expr);
    collect_index_expression_in_constraints(expr, &mut in_values_by_expr);
    collect_multi_valued_index_constraints(expr, &mut multi_valued_values_by_expr);
    let mut candidates = tuple_access::candidates(table, expr, &eq_values);
    if boolean_range::has_range_or(expr) {
        candidates.extend(
            table
                .iter_queryable_indexes()
                .filter(|i| !i.is_fulltext() && !i.is_spatial() && !i.is_vector())
                .filter_map(|index| boolean_range::candidate(table, index, expr)),
        );
    }
    for index in table
        .iter_queryable_indexes()
        .filter(|i| !i.is_fulltext() && !i.is_spatial() && !i.is_vector())
    {
        if let Some(candidate) = multi_valued_index_access(
            table,
            index,
            &eq_values,
            &eq_values_by_expr,
            &multi_valued_values_by_expr,
        ) {
            candidates.push(candidate);
            continue;
        }
        let mut parts = Vec::new();
        for pos in 0..index.columns.len() {
            let value = match index.key_part(pos) {
                Some(crate::storage::IndexKeyPart::Column(name)) => {
                    let column_key = normalize_expr_column(name);
                    let Some(v) = eq_values.get(&column_key) else {
                        break;
                    };
                    v.clone()
                }
                Some(crate::storage::IndexKeyPart::Expression(expr_part)) => {
                    let Some(ast) = expr_part.ast.as_ref() else {
                        break;
                    };
                    let fp = crate::engine::expr_program::expr_fingerprint(ast);
                    let Some(v) = eq_values_by_expr.get(&fp) else {
                        break;
                    };
                    v.clone()
                }
                None => break,
            };
            let target = target_for_index_column(table, index, pos);
            // A character index cannot implement MySQL's numeric comparison
            // domain. For example, `varchar_col = 1` also matches `'01'`,
            // while an exact character-key probe for `"1"` does not. Keep
            // such predicates as residual filters over a scan. Prepared
            // parameters are likewise unsafe unless the prepared-plan caller
            // has explicitly published their comparison domains.
            if !index_in_lookup_preserves_comparison(target, std::slice::from_ref(&value)) {
                break;
            }
            let Some(part) = constraint_key_part_for_index(&value, target, index, pos) else {
                break;
            };
            parts.push(part);
        }
        if parts.len() == index.columns.len() {
            if !parts.is_empty() {
                // INCLUDE suffixes and prefix key parts both make a
                // declared-column lookup non-exact: the former adds
                // trailing key parts, the latter intentionally groups
                // several full values under one stored key prefix.
                let exact = index.key_parts_are_exact(parts.len());
                candidates.push(IndexAccess {
                    index: index.clone(),
                    kind: IndexAccessKind::Lookup {
                        key: IndexKey::from(parts),
                        exact,
                    },
                });
            }
            continue;
        }
        let range_pos = parts.len();
        let range_part = index.key_part(range_pos);
        let range_part_is_expression = matches!(
            range_part,
            Some(crate::storage::IndexKeyPart::Expression(_))
        );
        let range_column = if range_part_is_expression {
            None
        } else {
            index
                .columns
                .get(range_pos)
                .map(|column| normalize_expr_column(column))
        };
        // Resolve the range slot. Column parts honour `column_prefix_len`
        // (prefix indexes can't satisfy a precise range); functional
        // parts look up by AST fingerprint and never have a prefix.
        let expr_range =
            if let Some(crate::storage::IndexKeyPart::Expression(expr_part)) = range_part {
                expr_part.ast.as_ref().and_then(|ast| {
                    ranges_by_expr.get(&crate::engine::expr_program::expr_fingerprint(ast))
                })
            } else {
                None
            };
        let column_range = if !range_part_is_expression {
            range_column.as_ref().and_then(|column| {
                ranges.get(column).filter(|range| {
                    index.column_prefix_len(range_pos).is_none()
                        || range.like_prefix.is_some()
                        || range.like_param.is_some()
                })
            })
        } else {
            None
        };
        let range = expr_range.or(column_range);
        if let Some(range) = range {
            let range_target = target_for_index_column(table, index, range_pos);
            let range_values = range
                .low
                .iter()
                .map(|(value, _)| value)
                .chain(range.high.iter().map(|(value, _)| value))
                .cloned()
                .collect::<Vec<_>>();
            // Numeric bounds on a character column are not ordered like its
            // B-tree keys (`BETWEEN 9 AND 20` is non-empty numerically but
            // reversed lexically). The residual predicate must see every row
            // so it can apply MySQL's numeric coercion and diagnostics.
            if !index_in_lookup_preserves_comparison(range_target, &range_values) {
                continue;
            }
            let Some((low, high)) = index_bounds::build_range_bounds(
                index,
                &parts,
                range_pos,
                range_target,
                range,
                |value, target| Some(value.into_key_part(target)),
            ) else {
                continue;
            };
            candidates.push(IndexAccess {
                index: index.clone(),
                kind: IndexAccessKind::Range { low, high },
            });
        } else if let Some(values) = {
            // Expression IN-list takes precedence when the slot is a
            // functional part (`((LOWER(c)))`); otherwise fall back to
            // the column-keyed IN map. Both produce `Vec<...>` so the
            // downstream code is shape-compatible.
            let expr_values =
                if let Some(crate::storage::IndexKeyPart::Expression(expr_part)) = range_part {
                    expr_part.ast.as_ref().and_then(|ast| {
                        in_values_by_expr.get(&crate::engine::expr_program::expr_fingerprint(ast))
                    })
                } else {
                    None
                };
            expr_values.or_else(|| {
                range_column
                    .as_ref()
                    .and_then(|column| in_values.get(column))
            })
        } {
            let target = target_for_index_column(table, index, range_pos);
            // A character column compared with a numeric IN operand uses
            // MySQL's numeric comparison domain.  Converting the constants
            // to character index keys is not equivalent: non-numeric stored
            // strings may compare as zero and must also publish conversion
            // warnings.  Keep the predicate as a scan/residual in this case.
            // Prepared parameters are likewise unsafe unless the
            // prepared-plan caller has explicitly published their comparison
            // domains.
            if !index_in_lookup_preserves_comparison(target, values) {
                continue;
            }
            publish_collapsed_numeric_in_diagnostics(target, values);
            let mut keys = values
                .iter()
                .filter_map(|value| {
                    let mut key_parts = parts.clone();
                    let part = constraint_key_part_for_index(value, target, index, range_pos)?;
                    key_parts.push(part);
                    Some(IndexKey::from(key_parts))
                })
                .collect::<Vec<_>>();
            keys.sort_unstable();
            keys.dedup();
            if !keys.is_empty() {
                let key_len = keys[0].0.len();
                let exact = key_len == index.columns.len() && index.key_parts_are_exact(key_len);
                candidates.push(IndexAccess {
                    index: index.clone(),
                    kind: IndexAccessKind::MultiLookup { keys, exact },
                });
            }
        } else if !parts.is_empty() {
            candidates.push(IndexAccess {
                index: index.clone(),
                kind: IndexAccessKind::Lookup {
                    key: IndexKey::from(parts),
                    exact: false,
                },
            });
        }
    }
    filter_index_candidates(&mut candidates, hints.index_hints, IndexHintScope::Join);
    let use_idx = query_hint_use_index(hints.query_hints, hints.table_name, &table.name);
    if !use_idx.is_empty() {
        candidates.retain(|c| use_idx.iter().any(|n| c.index.name.eq_ignore_ascii_case(n)));
    }
    let no_idx = query_hint_no_index(hints.query_hints, hints.table_name, &table.name);
    if !no_idx.is_empty() {
        candidates.retain(|c| !no_idx.iter().any(|n| c.index.name.eq_ignore_ascii_case(n)));
    }
    candidates
        .into_iter()
        .map(|access| {
            let cost = effective_index_access_cost(
                ctx,
                table,
                &access,
                expr,
                hints,
                total_rows,
                table_pages,
                row_width,
            );
            (access, cost)
        })
        .min_by(|left, right| left.1.total_cmp(&right.1))
}

#[derive(Debug, Clone)]
struct CostedIndexMergeNode {
    node: IndexMergeNode,
    rows: f64,
    cost: f64,
}

fn optimizer_switch_flag_enabled(name: &str, default: bool) -> bool {
    crate::engine::expr::with_active_session(|session| {
        let Some(session) = session else {
            return default;
        };
        let on = format!("{name}=on");
        let off = format!("{name}=off");
        if session
            .sysvars
            .optimizer_switch
            .split(',')
            .any(|part| part.trim() == off)
        {
            false
        } else if session
            .sysvars
            .optimizer_switch
            .split(',')
            .any(|part| part.trim() == on)
        {
            true
        } else {
            default
        }
    })
}

thread_local! {
    static INDEX_MERGE_SWITCH_SCOPE: std::cell::Cell<(bool, bool, bool)> =
        const { std::cell::Cell::new((true, true, true)) };
}

pub(crate) struct IndexMergeOptimizerScope {
    previous: (bool, bool, bool),
}

impl IndexMergeOptimizerScope {
    pub(crate) fn install(union: bool, sort_union: bool, intersection: bool) -> Self {
        let previous =
            INDEX_MERGE_SWITCH_SCOPE.with(|slot| slot.replace((union, sort_union, intersection)));
        Self { previous }
    }
}

impl Drop for IndexMergeOptimizerScope {
    fn drop(&mut self) {
        INDEX_MERGE_SWITCH_SCOPE.with(|slot| slot.set(self.previous));
    }
}

pub(crate) fn index_merge_switches() -> (bool, bool, bool) {
    crate::engine::expr::with_active_session(|session| {
        if session.is_none() {
            return INDEX_MERGE_SWITCH_SCOPE.with(std::cell::Cell::get);
        }
        let master = optimizer_switch_flag_enabled("index_merge", true);
        (
            master && optimizer_switch_flag_enabled("index_merge_union", true),
            master && optimizer_switch_flag_enabled("index_merge_sort_union", true),
            master && optimizer_switch_flag_enabled("index_merge_intersection", true),
        )
    })
}

fn index_merge_leaf_from_expr(
    ctx: &crate::engine::cbo::CboContext,
    table: &TableSchema,
    expr: &Expr,
    hints: &ScanHints<'_>,
    total_rows: f64,
    table_pages: f64,
    row_width: f64,
) -> Option<CostedIndexMergeNode> {
    let (access, cost) =
        best_index_access(ctx, table, expr, hints, total_rows, table_pages, row_width)?;
    if let Some(only) = query_hint_index_merge(hints.query_hints, hints.table_name, &table.name) {
        if !only.is_empty()
            && !only
                .iter()
                .any(|name| access.index.name.eq_ignore_ascii_case(name))
        {
            return None;
        }
    }
    if let Some(blocked) =
        query_hint_no_index_merge(hints.query_hints, hints.table_name, &table.name)
    {
        if blocked.is_empty()
            || blocked
                .iter()
                .any(|name| access.index.name.eq_ignore_ascii_case(name))
        {
            return None;
        }
    }
    let rows = estimate_index_access_rows(table, &access, total_rows);
    let row_id_ordered = matches!(access.kind, IndexAccessKind::Lookup { exact: true, .. });
    Some(CostedIndexMergeNode {
        node: IndexMergeNode::Leaf(IndexMergeLeaf {
            index: access.index,
            access: access.kind,
            row_id_ordered,
        }),
        rows,
        cost,
    })
}

/// Build a safe RowId superset for a boolean subtree. OR requires both sides;
/// AND may use either side and intersects when both sides are indexable. The
/// unchanged original filter is always evaluated after the heap read.
#[allow(clippy::too_many_arguments)]
fn build_index_merge_node(
    ctx: &crate::engine::cbo::CboContext,
    table: &TableSchema,
    expr: &Expr,
    hints: &ScanHints<'_>,
    total_rows: f64,
    table_pages: f64,
    row_width: f64,
    union_allowed: bool,
    sort_union_allowed: bool,
    intersection_allowed: bool,
) -> Option<CostedIndexMergeNode> {
    match expr {
        Expr::Binary {
            left,
            op: BinaryOp::Or,
            right,
        } => {
            // Preserve the existing, cheaper same-index OR-domain collapse.
            let forced_indexes =
                query_hint_index_merge(hints.query_hints, hints.table_name, &table.name);
            if forced_indexes
                .as_ref()
                .is_none_or(|indexes| indexes.len() <= 1)
            {
                if let Some(single) = index_merge_leaf_from_expr(
                    ctx,
                    table,
                    expr,
                    hints,
                    total_rows,
                    table_pages,
                    row_width,
                ) {
                    return Some(single);
                }
            }
            let left = build_index_merge_node(
                ctx,
                table,
                left,
                hints,
                total_rows,
                table_pages,
                row_width,
                union_allowed,
                sort_union_allowed,
                intersection_allowed,
            )?;
            let right = build_index_merge_node(
                ctx,
                table,
                right,
                hints,
                total_rows,
                table_pages,
                row_width,
                union_allowed,
                sort_union_allowed,
                intersection_allowed,
            )?;
            let needs_sort = !left.node.is_row_id_ordered() || !right.node.is_row_id_ordered();
            if (needs_sort && !sort_union_allowed) || (!needs_sort && !union_allowed) {
                return None;
            }
            let overlap = if total_rows > 0.0 {
                left.rows * right.rows / total_rows
            } else {
                0.0
            };
            let rows = (left.rows + right.rows - overlap).clamp(0.0, total_rows.max(1.0));
            Some(CostedIndexMergeNode {
                node: IndexMergeNode::Union {
                    children: vec![left.node, right.node],
                    sort_union: needs_sort,
                },
                rows,
                cost: left.cost + right.cost + rows.max(1.0),
            })
        }
        Expr::Binary {
            left,
            op: BinaryOp::And,
            right,
        } => {
            if left == right {
                return build_index_merge_node(
                    ctx,
                    table,
                    left,
                    hints,
                    total_rows,
                    table_pages,
                    row_width,
                    union_allowed,
                    sort_union_allowed,
                    intersection_allowed,
                );
            }
            let left = build_index_merge_node(
                ctx,
                table,
                left,
                hints,
                total_rows,
                table_pages,
                row_width,
                union_allowed,
                sort_union_allowed,
                intersection_allowed,
            );
            let right = build_index_merge_node(
                ctx,
                table,
                right,
                hints,
                total_rows,
                table_pages,
                row_width,
                union_allowed,
                sort_union_allowed,
                intersection_allowed,
            );
            match (left, right) {
                (Some(left), Some(right))
                    if matches!(
                        (&left.node, &right.node),
                        (IndexMergeNode::Leaf(a), IndexMergeNode::Leaf(b))
                            if a.index.name == b.index.name && a.access == b.access
                    ) =>
                {
                    // Syntactically different predicates can resolve to the
                    // same sorted/coerced index key domain. Reading it twice
                    // cannot reduce the RowId set, nor square its selectivity.
                    Some(if left.cost <= right.cost { left } else { right })
                }
                (Some(left), Some(right)) if intersection_allowed => {
                    let rows = if total_rows > 0.0 {
                        (left.rows * right.rows / total_rows).clamp(0.0, left.rows.min(right.rows))
                    } else {
                        0.0
                    };
                    Some(CostedIndexMergeNode {
                        node: IndexMergeNode::Intersection {
                            children: vec![left.node, right.node],
                        },
                        rows,
                        // Merge leaves carry compact RowIds; the heap is read
                        // only for the final intersection.
                        cost: left.cost + right.cost + rows.max(1.0),
                    })
                }
                (Some(left), Some(right)) => {
                    if left.cost <= right.cost {
                        Some(left)
                    } else {
                        Some(right)
                    }
                }
                (Some(node), None) | (None, Some(node)) => Some(node),
                (None, None) => None,
            }
        }
        _ => {
            index_merge_leaf_from_expr(ctx, table, expr, hints, total_rows, table_pages, row_width)
        }
    }
}

thread_local! {
    /// Bindings available to access-path costing and comparison-domain guards.
    /// Keep these separate from execution params: general normalization must
    /// not fold one execution's values into a reusable physical plan.
    static PLANNING_PARAMS: std::cell::RefCell<Option<Vec<Value>>> =
        const { std::cell::RefCell::new(None) };
}

struct PlanningParamsGuard(Option<Vec<Value>>);

impl Drop for PlanningParamsGuard {
    fn drop(&mut self) {
        PLANNING_PARAMS.with(|slot| {
            slot.replace(self.0.take());
        });
    }
}

pub(crate) fn with_planning_params<R>(params: &[Value], f: impl FnOnce() -> R) -> R {
    let _exact_domains = index_residual::enter_param_domains(params);
    let previous = PLANNING_PARAMS.with(|slot| slot.replace(Some(params.to_vec())));
    let _guard = PlanningParamsGuard(previous);
    f()
}

/// Bind only the disposable Zone Map costing predicate. The scan's own
/// filter retains placeholders and rebuilds candidates with each execution's
/// bindings, even when a cached plan was costed with zero surviving pages.
fn bind_zone_map_cost_parameters(predicate: &mut Expr) {
    PLANNING_PARAMS.with(|slot| {
        let params = slot.borrow();
        let Some(params) = params.as_ref() else {
            return;
        };
        crate::parser::visit::rewrite_value_expr(predicate, |expr| {
            if let Expr::Placeholder(index) = expr {
                if let Some(value) = params.get(*index) {
                    *expr = Expr::BoundValue(value.clone());
                }
            }
            true
        });
    });
}

fn index_comparison_param_is_string_domain(index: usize) -> bool {
    PLANNING_PARAMS.with(|slot| {
        slot.borrow()
            .as_ref()
            .and_then(|params| params.get(index))
            .map(|value| {
                matches!(
                    value,
                    Value::String(_) | Value::SmallString(_) | Value::Bytes(_) | Value::Null
                )
            })
            .unwrap_or(false)
    })
}

fn index_comparison_param_is_integer_domain(index: usize) -> bool {
    let domain = PLANNING_PARAMS.with(|slot| {
        slot.borrow()
            .as_ref()
            .and_then(|params| params.get(index))
            .map(|value| matches!(value, Value::Int(_) | Value::UInt(_) | Value::Null))
    });
    // Direct planner callers may install params without the cache-domain
    // guard. Consult their type only; never bake a bound parameter value.
    domain.unwrap_or_else(|| {
        crate::engine::params::current_param(index)
            .is_none_or(|value| matches!(value, Value::Int(_) | Value::UInt(_) | Value::Null))
    })
}

fn index_in_lookup_preserves_comparison(
    target: crate::storage::ParamTarget,
    values: &[PlanConstraintValue],
) -> bool {
    if matches!(
        target,
        crate::storage::ParamTarget::Date
            | crate::storage::ParamTarget::Time
            | crate::storage::ParamTarget::DateTime
            | crate::storage::ParamTarget::Timestamp
    ) {
        return values.iter().all(|v| match v {
            PlanConstraintValue::Concrete(v) => KeyPart::for_target(v.clone(), target).is_ok(),
            PlanConstraintValue::Param(i) => {
                crate::engine::expr::eval_const(&Expr::Placeholder(*i))
                    .is_ok_and(|v| KeyPart::for_target(v, target).is_ok())
            }
        });
    }
    let character_target = matches!(
        target,
        crate::storage::ParamTarget::CharString
            | crate::storage::ParamTarget::CollatedCharString { .. }
            | crate::storage::ParamTarget::String
            | crate::storage::ParamTarget::CollatedString { .. }
    );
    if !character_target {
        if matches!(
            target,
            crate::storage::ParamTarget::Float32
                | crate::storage::ParamTarget::Float64
                | crate::storage::ParamTarget::Decimal { .. }
                | crate::storage::ParamTarget::Year
        ) {
            // Text comparisons use the evaluator's numeric domain, which can
            // differ from a rounded FLOAT/DECIMAL/YEAR storage bound.
            return values.iter().all(|value| match value {
                PlanConstraintValue::Concrete(value) => !matches!(
                    value,
                    Value::String(_) | Value::SmallString(_) | Value::Bytes(_)
                ),
                PlanConstraintValue::Param(index) => {
                    !index_comparison_param_is_string_domain(*index)
                }
            });
        }
        if matches!(
            target,
            crate::storage::ParamTarget::Int | crate::storage::ParamTarget::UInt
        ) {
            return values.iter().all(|v| match v {
                PlanConstraintValue::Param(i) => index_comparison_param_is_integer_domain(*i),
                PlanConstraintValue::Concrete(v) => {
                    // Integer ranges cannot truncate fractional comparison bounds.
                    if !matches!(
                        v,
                        Value::String(_) | Value::SmallString(_) | Value::Bytes(_) | Value::Null
                    ) {
                        let Ok(key) = KeyPart::for_target(v.clone(), target) else {
                            return false;
                        };
                        let restored = key.to_value(&crate::storage::DataTypeKind::BigInt);
                        if crate::engine::expr::compare_numeric_for_sort(v, &restored)
                            != Some(std::cmp::Ordering::Equal)
                        {
                            return false;
                        }
                        if matches!(v, Value::Float(n) if n.abs() >= (1u64 << 53) as f32)
                            || matches!(v, Value::Double(n) if n.abs() >= (1u64 << 53) as f64)
                        {
                            return false;
                        }
                    }
                    let text = match v {
                        Value::String(t) => Some(t.clone()),
                        Value::SmallString(_) | Value::Bytes(_) => Some(v.to_display_string()),
                        _ => None,
                    };
                    text.is_none_or(|t| {
                        let n = crate::util::decimal::parse_lenient_f64(&t).value;
                        n.is_finite()
                            && n.fract() == 0.0
                            && n.abs() < (1u64 << 53) as f64
                            && (!matches!(target, crate::storage::ParamTarget::UInt) || n >= 0.0)
                    })
                }
            });
        }
        return true;
    }
    values.iter().all(|value| match value {
        PlanConstraintValue::Concrete(value) => matches!(
            value,
            Value::String(_) | Value::SmallString(_) | Value::Bytes(_) | Value::Null
        ),
        PlanConstraintValue::Param(index) => index_comparison_param_is_string_domain(*index),
    })
}

/// Numeric index keys intentionally collapse different invalid string
/// constants to zero. The first such operand is diagnosed by the ordinary
/// range/key coercion path; operands after it used to disappear during key
/// dedup (`IN ('a','b')` became one zero probe), losing their 1292 warnings.
/// Validate only those additional lossy operands before deduplication.
fn publish_collapsed_numeric_in_diagnostics(
    target: crate::storage::ParamTarget,
    values: &[PlanConstraintValue],
) {
    if !matches!(
        target,
        crate::storage::ParamTarget::Int
            | crate::storage::ParamTarget::UInt
            | crate::storage::ParamTarget::Float32
            | crate::storage::ParamTarget::Float64
            | crate::storage::ParamTarget::Decimal { .. }
            | crate::storage::ParamTarget::Year
    ) {
        return;
    }
    let mut saw_lossy = false;
    for value in values {
        let runtime = match value {
            PlanConstraintValue::Concrete(value) => Some(value.clone()),
            PlanConstraintValue::Param(index) => crate::engine::params::current_param(*index),
        };
        let Some(text) = runtime.as_ref().and_then(|value| match value {
            Value::String(text) | Value::Decimal(text) => Some(text.clone()),
            Value::SmallString(text) => Some(
                // SAFETY: SmallString maintains its UTF-8 invariant.
                unsafe { std::str::from_utf8_unchecked(text) }.to_string(),
            ),
            Value::Bytes(bytes) => Some(String::from_utf8_lossy(bytes).into_owned()),
            _ => None,
        }) else {
            continue;
        };
        if crate::util::decimal::parse_lenient_f64(&text).clean {
            continue;
        }
        if !saw_lossy {
            saw_lossy = true;
            continue;
        }
        let context = 0x494e_4b45_595f_434fu64;
        crate::engine::expr::with_expression_warning_dedup(context, || {
            crate::engine::api::push_statement_warning(
                crate::error::ConditionLevel::Warning,
                crate::error::catalog::ER_TRUNCATED_WRONG_VALUE,
                format!("Truncated incorrect DOUBLE value: '{text}'"),
            );
        });
    }
}

fn multi_valued_index_access(
    table: &TableSchema,
    index: &IndexSchema,
    eq_values: &HashMap<String, PlanConstraintValue>,
    eq_values_by_expr: &HashMap<u64, PlanConstraintValue>,
    values_by_expr: &HashMap<u64, Vec<PlanConstraintValue>>,
) -> Option<IndexAccess> {
    if !index.has_multi_valued_parts() {
        return None;
    }
    let (multi_pos, expr_part) =
        index
            .key_expressions
            .iter()
            .enumerate()
            .find_map(|(pos, expr)| {
                expr.as_ref()
                    .filter(|expr| expr.multi_valued)
                    .map(|expr| (pos, expr))
            })?;
    let ast = expr_part.ast.as_ref()?;
    let source = multi_valued_index_source_expr(ast).unwrap_or(ast);
    let fp = crate::engine::expr_program::expr_fingerprint(source);
    let values = values_by_expr.get(&fp)?;

    let mut prefix = Vec::with_capacity(multi_pos);
    for pos in 0..multi_pos {
        let constraint = match index.key_part(pos)? {
            crate::storage::IndexKeyPart::Column(name) => {
                eq_values.get(&normalize_expr_column(name))?
            }
            crate::storage::IndexKeyPart::Expression(expr) => {
                let ast = expr.ast.as_ref()?;
                eq_values_by_expr.get(&crate::engine::expr_program::expr_fingerprint(ast))?
            }
        };
        let target = target_for_index_column(table, index, pos);
        prefix.push(constraint_key_part_for_index(
            constraint, target, index, pos,
        )?);
    }
    if prefix.len() != multi_pos {
        return None;
    }
    let mut keys = Vec::with_capacity(values.len());
    for value in values {
        let part = match value {
            PlanConstraintValue::Concrete(value) => table
                .multi_valued_index_lookup_part(index, multi_pos, value.clone())
                .ok()?,
            PlanConstraintValue::Param(idx) => {
                let target = table.multi_valued_index_param_target(index)?;
                KeyPart::Param { idx: *idx, target }
            }
        };
        let mut parts = prefix.clone();
        parts.push(part);
        keys.push(IndexKey::from(parts));
    }
    keys.sort_unstable();
    keys.dedup();
    let exact = multi_pos + 1 == index.columns.len();
    match keys.len() {
        0 => None,
        1 => Some(IndexAccess {
            index: index.clone(),
            kind: IndexAccessKind::Lookup {
                key: keys.pop().unwrap(),
                exact,
            },
        }),
        _ => Some(IndexAccess {
            index: index.clone(),
            kind: IndexAccessKind::MultiLookup { keys, exact },
        }),
    }
}

fn extra_multi_lookup_cpu(height: f64, keys: usize) -> f64 {
    // index_lookup accounts for one descent. Sorted batch probes reuse pages,
    // but every additional key still needs comparisons along the tree.
    keys.saturating_sub(1) as f64 * height * crate::engine::cbo::cbo_consts().cpu_index_tuple_cost()
}

fn effective_index_access_cost(
    ctx: &crate::engine::cbo::CboContext,
    table: &TableSchema,
    access: &IndexAccess,
    expr: &Expr,
    hints: &ScanHints<'_>,
    total_rows: f64,
    table_pages: f64,
    row_width: f64,
) -> f64 {
    use crate::engine::cbo::operator_cost;
    let height = index_cost_height(table, &access.index, total_rows);
    let will_cover = index_would_cover(&access.index, hints);
    let mut rows = estimate_index_access_rows(table, access, total_rows);
    let output_rows = estimate_index_output_rows(table, Some(expr), rows, total_rows);
    let ordered_row_goal = hints.limit_with_offset.and_then(|row_goal| {
        (order_by_index_direction(hints.order_by).is_some()
            && index_order_covers_order_by(table, &access.index, Some(expr), hints.order_by))
        .then_some(row_goal)
    });
    rows = access_rows_for_row_goal(rows, output_rows, hints.row_goal.or(ordered_row_goal));
    let cost = match &access.kind {
        IndexAccessKind::Lookup { .. } => {
            let effective_table_pages = if will_cover { 0.0 } else { table_pages };
            let mut cost =
                operator_cost::index_lookup(ctx, height, rows, effective_table_pages, row_width);
            let qual_cpu = qual_cost_per_row(expr) * rows;
            cost.total += qual_cpu;
            cost.cpu_units += qual_cpu;
            cost
        }
        IndexAccessKind::MultiLookup { keys, .. } => {
            let effective_table_pages = if will_cover {
                0.0
            } else {
                table_pages_for_batched_in_lookup(
                    table,
                    &access.index,
                    table_pages,
                    keys.len() as f64,
                )
            };
            let mut cost =
                operator_cost::index_lookup(ctx, height, rows, effective_table_pages, row_width);
            let qual_cpu = qual_cost_per_row(expr) * rows;
            let probe_cpu = extra_multi_lookup_cpu(height, keys.len());
            cost.total += qual_cpu + probe_cpu;
            cost.cpu_units += qual_cpu + probe_cpu;
            cost
        }
        IndexAccessKind::Range { .. } if will_cover => {
            let leaf_pages = operator_cost::estimated_index_leaf_pages(table, &access.index, rows);
            let mut cost = operator_cost::index_fast_scan(ctx, height, leaf_pages, rows, row_width);
            let qual_cpu = qual_cost_per_row(expr) * rows;
            cost.total += qual_cpu;
            cost.cpu_units += qual_cpu;
            cost
        }
        IndexAccessKind::Range { .. } => {
            let leaf_pages = operator_cost::estimated_index_leaf_pages(table, &access.index, rows);
            let correlation = index_cost_correlation(table, &access.index);
            let mut cost = operator_cost::index_range_scan(
                ctx,
                height,
                leaf_pages,
                rows,
                total_rows,
                table_pages,
                row_width,
                correlation,
            );
            let qual_cpu = qual_cost_per_row(expr) * rows;
            cost.total += qual_cpu;
            cost.cpu_units += qual_cpu;
            cost
        }
    };
    cost.weighted_total()
}

fn concrete_key_part_value(
    part: &KeyPart,
    data_type: &crate::storage::DataTypeKind,
) -> Option<Value> {
    match part {
        KeyPart::Param { .. } => part
            .resolve_param()
            .ok()
            .and_then(|resolved| concrete_key_part_value(&resolved, data_type)),
        _ => Some(part.to_value(data_type)),
    }
}

fn best_in_subquery_index_access(
    db: &Database,
    ctx: &crate::engine::cbo::CboContext,
    table: &PlanTable,
    expr: &Expr,
    filter: Option<Expr>,
    hints: &ScanHints<'_>,
    total_rows: f64,
    table_pages: f64,
    row_width: f64,
) -> Option<CostedInSubqueryIndexAccess> {
    let mut eq_values = HashMap::new();
    let mut ranges = HashMap::new();
    collect_index_constraints(&table.schema, expr, &mut eq_values, &mut ranges);
    let mut in_subqueries = Vec::new();
    collect_in_subquery_constraints(expr, &mut in_subqueries);
    let use_idx = query_hint_use_index(hints.query_hints, hints.table_name, &table.schema.name);
    let no_idx = query_hint_no_index(hints.query_hints, hints.table_name, &table.schema.name);

    table
        .schema
        .iter_queryable_indexes()
        .filter(|i| {
            !i.is_fulltext() && !i.is_spatial() && !i.is_vector() && !i.has_multi_valued_parts()
        })
        .filter_map(|index| {
            if !index_allowed_by_hints(&index.name, hints.index_hints, IndexHintScope::Join) {
                return None;
            }
            if !use_idx.is_empty() && !use_idx.iter().any(|n| index.name.eq_ignore_ascii_case(n)) {
                return None;
            }
            if no_idx.iter().any(|n| index.name.eq_ignore_ascii_case(n)) {
                return None;
            }
            let mut parts = Vec::new();
            for (pos, column) in index.columns.iter().enumerate() {
                let column_key = normalize_expr_column(column);
                let Some(value) = eq_values.get(&column_key) else {
                    break;
                };
                let target = target_for_index_column(&table.schema, index, pos);
                let Some(part) = constraint_key_part_for_index(value, target, index, pos) else {
                    break;
                };
                parts.push(part);
            }
            let in_column = index.columns.get(parts.len())?;
            if index.column_prefix_len(parts.len()).is_some() {
                return None;
            }
            let in_subquery = in_subqueries
                .iter()
                .find(|candidate| expr_matches_table_column(candidate.expr, table, in_column))?;
            if !single_value_subquery_not_referencing_table(db, in_subquery.subquery, table) {
                return None;
            }
            // A single encoded probe is exact only within one comparison
            // domain. A lossy cast can both miss matches and create false ones.
            let SelectItem::Expr {
                expr: projected, ..
            } = in_subquery.subquery.projection.first()?
            else {
                return None;
            };
            let source_tables = subquery_plan_tables(db, in_subquery.subquery)?;
            let mut source_columns = source_tables.iter().filter_map(|source| {
                source
                    .schema
                    .iter_visible_columns()
                    .find(|(_, col)| expr_matches_table_column(projected, source, &col.name))
                    .map(|(_, col)| col)
            });
            let source = source_columns.next()?;
            let target = &table.schema.columns[table.schema.column_index(in_column).ok()?];
            let domain = |column: &crate::storage::schema::ColumnSchema| {
                crate::storage::ParamTarget::for_column_with_collation(
                    &column.data_type,
                    column.unsigned,
                    column.collation.as_deref(),
                )
            };
            if source_columns.next().is_some() || domain(source) != domain(target) {
                return None;
            }

            let access = InSubqueryIndexAccess {
                index: index.clone(),
                key_prefix: IndexKey::from(parts),
                in_column: in_column.clone(),
                subquery: (*in_subquery.subquery).clone(),
            };
            let costed = cost_in_subquery_index_access(
                db,
                ctx,
                table,
                &access,
                hints,
                filter.clone(),
                total_rows,
                table_pages,
                row_width,
            );
            Some(costed)
        })
        .min_by(|left, right| {
            left.cost
                .weighted_total()
                .partial_cmp(&right.cost.weighted_total())
                .unwrap_or(std::cmp::Ordering::Equal)
                .then_with(|| {
                    left.access_rows
                        .partial_cmp(&right.access_rows)
                        .unwrap_or(std::cmp::Ordering::Equal)
                })
                .then_with(|| {
                    left.lookup_count
                        .partial_cmp(&right.lookup_count)
                        .unwrap_or(std::cmp::Ordering::Equal)
                })
                .then_with(|| {
                    right
                        .access
                        .key_prefix
                        .0
                        .len()
                        .cmp(&left.access.key_prefix.0.len())
                })
                .then_with(|| {
                    let left_width = (
                        left.access.index.columns.len(),
                        left.access.index.effective_columns().len(),
                    );
                    let right_width = (
                        right.access.index.columns.len(),
                        right.access.index.effective_columns().len(),
                    );
                    left_width.cmp(&right_width)
                })
                .then_with(|| left.access.index.name.cmp(&right.access.index.name))
        })
}

fn cost_in_subquery_index_access(
    db: &Database,
    ctx: &crate::engine::cbo::CboContext,
    table: &PlanTable,
    access: &InSubqueryIndexAccess,
    hints: &ScanHints<'_>,
    filter: Option<Expr>,
    total_rows: f64,
    table_pages: f64,
    row_width: f64,
) -> CostedInSubqueryIndexAccess {
    let key_len = access.key_prefix.0.len() + 1;
    let per_lookup_rows =
        estimate_index_lookup_rows(&table.schema, &access.index, key_len, false, total_rows);
    let subquery_rows = estimate_in_lookup_subquery_result_rows(db, &access.subquery)
        .or_else(|| estimate_subquery_result_rows(db, &access.subquery))
        .unwrap_or(100.0);
    let lookup_count = subquery_rows.max(1.0);
    let access_rows = (per_lookup_rows * lookup_count).clamp(1.0, total_rows.max(1.0));
    let residual_filter =
        remove_in_subquery_predicate_for_column(filter, table, &access.in_column, &access.subquery);
    let estimated_rows = residual_filter
        .as_ref()
        .map(|residual| estimate_filter_rows(&table.schema, residual, access_rows))
        .unwrap_or(access_rows)
        .clamp(1.0, access_rows.max(1.0));
    let height = index_cost_height(&table.schema, &access.index, total_rows);
    // A small key set bounds B-tree descents, not heap fanout. A non-unique
    // key can visit millions of heap rows even with a single probe. Cost the
    // complete heap population rather than pretending it fits in key-count
    // pages, which made broad IN-subquery accesses appear cache-resident.
    let effective_table_pages =
        if table.system_time.is_none() && index_would_cover(&access.index, hints) {
            0.0
        } else {
            table_pages
        };
    let per_lookup_cost = crate::engine::cbo::operator_cost::index_lookup(
        ctx,
        height,
        per_lookup_rows,
        effective_table_pages,
        row_width,
    );
    let cost = crate::engine::cbo::Cost {
        startup: per_lookup_cost.startup,
        total: per_lookup_cost.total * lookup_count,
        io_seq_pages: per_lookup_cost.io_seq_pages * lookup_count,
        io_rnd_pages: per_lookup_cost.io_rnd_pages * lookup_count,
        cpu_units: per_lookup_cost.cpu_units * lookup_count,
        mem_bytes: per_lookup_cost.mem_bytes,
        spill_bytes: per_lookup_cost.spill_bytes,
    };
    CostedInSubqueryIndexAccess {
        access: access.clone(),
        lookup_count,
        access_rows,
        estimated_rows,
        cost,
    }
}

struct InSubqueryConstraint<'a> {
    expr: &'a Expr,
    subquery: &'a Select,
}

fn collect_in_subquery_constraints<'a>(expr: &'a Expr, out: &mut Vec<InSubqueryConstraint<'a>>) {
    match expr {
        Expr::Binary { left, op, right } if *op == BinaryOp::And => {
            collect_in_subquery_constraints(left, out);
            collect_in_subquery_constraints(right, out);
        }
        Expr::InSubquery {
            expr,
            subquery,
            negated: false,
        } => {
            out.push(InSubqueryConstraint { expr, subquery });
        }
        _ => {}
    }
}

fn predicate_has_missing_eq_stats(table: &TableSchema, expr: &Expr) -> bool {
    match expr {
        Expr::Binary { left, op, right } if *op == BinaryOp::And || *op == BinaryOp::Or => {
            predicate_has_missing_eq_stats(table, left)
                || predicate_has_missing_eq_stats(table, right)
        }
        Expr::Binary { left, op, right } if *op == BinaryOp::Eq => {
            let column = expr_column_name(left).or_else(|| expr_column_name(right));
            let literal = expr_literal_value(left).or_else(|| expr_literal_value(right));
            column.filter(|_| literal.is_some()).is_some_and(|name| {
                !table
                    .stats
                    .columns
                    .contains_key(&normalize_expr_column(name))
            })
        }
        _ => false,
    }
}

/// Remove the AND-conjuncted `Expr::InSubquery` node whose left expression
/// references `(table, column)` and whose `negated` flag is false. Used
/// when an `IndexInLookup` plan already enforces the IN predicate through
/// index seeks, so the residual filter can drop it instead of re-running
/// the subquery on every visited row.
fn remove_in_subquery_predicate_for_column(
    filter: Option<Expr>,
    table: &PlanTable,
    column: &str,
    subquery: &Select,
) -> Option<Expr> {
    fn walk(expr: Expr, table: &PlanTable, column: &str, selected: &Select) -> Option<Expr> {
        match expr {
            Expr::Binary { left, op, right } if op == BinaryOp::And => {
                let new_left = walk(*left, table, column, selected);
                let new_right = walk(*right, table, column, selected);
                match (new_left, new_right) {
                    (Some(l), Some(r)) => Some(Expr::Binary {
                        left: Box::new(l),
                        op,
                        right: Box::new(r),
                    }),
                    (Some(only), None) | (None, Some(only)) => Some(only),
                    (None, None) => None,
                }
            }
            Expr::InSubquery {
                ref expr,
                negated: false,
                ref subquery,
            } if expr_matches_table_column(expr, table, column)
                && subquery.as_ref() == selected =>
            {
                None
            }
            other => Some(other),
        }
    }
    filter.and_then(|f| walk(f, table, column, subquery))
}

fn single_value_subquery_not_referencing_table(
    db: &Database,
    subquery: &Select,
    outer_table: &PlanTable,
) -> bool {
    let Some(subquery_tables) = subquery_plan_tables(db, subquery) else {
        return false;
    };
    subquery.projection.len() == 1
        && !subquery.distinct
        && !subquery_tables_conflict_with_outer_table(&subquery_tables, outer_table)
        && subquery
            .projection
            .iter()
            .all(|item| !select_item_references_table(item, outer_table, &subquery_tables))
        && subquery
            .selection
            .as_ref()
            .is_none_or(|expr| !expr_references_table(expr, outer_table, &subquery_tables))
        && subquery.joins.iter().all(|join| {
            join.condition
                .as_ref()
                .is_none_or(|expr| !expr_references_table(expr, outer_table, &subquery_tables))
        })
        && subquery
            .order_by
            .iter()
            .all(|order_by| !expr_references_table(&order_by.expr, outer_table, &subquery_tables))
        && subquery
            .group_by
            .iter()
            .all(|expr| !expr_references_table(expr, outer_table, &subquery_tables))
        && subquery
            .having
            .as_ref()
            .is_none_or(|expr| !expr_references_table(expr, outer_table, &subquery_tables))
}

fn subquery_plan_tables(db: &Database, subquery: &Select) -> Option<Vec<PlanTable>> {
    subquery
        .from
        .iter()
        .chain(subquery.joins.iter().map(|join| &join.table))
        .map(|table_ref| plan_table_for_ref_if_resolved(db, table_ref))
        .collect()
}

thread_local! {
    static SUBQUERY_ROW_ESTIMATE_DEPTH: std::cell::Cell<usize> = std::cell::Cell::new(0);
}

fn table_pages_for_batched_in_lookup(
    schema: &TableSchema,
    index: &IndexSchema,
    table_pages: f64,
    lookup_count: f64,
) -> f64 {
    table_pages_for_index_nlj_lookup(schema, index, table_pages, lookup_count)
}

fn subquery_tables_conflict_with_outer_table(
    subquery_tables: &[PlanTable],
    outer_table: &PlanTable,
) -> bool {
    subquery_tables.iter().any(|table| {
        table
            .schema
            .qualified_name()
            .eq_ignore_ascii_case(&outer_table.schema.qualified_name())
            || table.alias.as_deref().is_some_and(|alias| {
                matches_table(
                    alias,
                    &outer_table.schema.name,
                    outer_table.alias.as_deref(),
                )
            })
    })
}

fn select_item_references_table(
    item: &SelectItem,
    outer_table: &PlanTable,
    subquery_tables: &[PlanTable],
) -> bool {
    matches!(item, SelectItem::Expr { expr, .. } if expr_references_table(expr, outer_table, subquery_tables))
}

fn expr_references_table(
    expr: &Expr,
    outer_table: &PlanTable,
    subquery_tables: &[PlanTable],
) -> bool {
    crate::parser::visit::try_inspect_value_expr(expr, |node| {
        let references = match node {
            Expr::Identifier(name) => {
                !subquery_has_unqualified_column(subquery_tables, name)
                    && outer_table
                        .schema
                        .columns
                        .iter()
                        .any(|column| column.name.eq_ignore_ascii_case(name))
            }
            Expr::CompoundIdentifier(parts) => {
                let Some(column) = parts.last() else {
                    return Ok(false);
                };
                let qualifier = if parts.len() >= 2 {
                    Some(parts[parts.len() - 2].as_str())
                } else {
                    None
                };
                qualifier
                    .map(|qualifier| {
                        !subquery_has_qualified_column(subquery_tables, qualifier, column)
                            && matches_table(
                                qualifier,
                                &outer_table.schema.name,
                                outer_table.alias.as_deref(),
                            )
                            && outer_table
                                .schema
                                .columns
                                .iter()
                                .any(|schema_col| schema_col.name.eq_ignore_ascii_case(column))
                    })
                    .unwrap_or(false)
            }
            _ => false,
        };
        if references {
            Err(())
        } else {
            Ok(!matches!(node, Expr::Grouping(_)))
        }
    })
    .is_err()
}

fn subquery_has_unqualified_column(subquery_tables: &[PlanTable], column: &str) -> bool {
    subquery_tables.iter().any(|table| {
        table
            .schema
            .columns
            .iter()
            .any(|col| col.name.eq_ignore_ascii_case(column))
    })
}

fn subquery_has_qualified_column(
    subquery_tables: &[PlanTable],
    qualifier: &str,
    column: &str,
) -> bool {
    subquery_tables.iter().any(|table| {
        matches_table(qualifier, &table.schema.name, table.alias.as_deref())
            && table
                .schema
                .columns
                .iter()
                .any(|col| col.name.eq_ignore_ascii_case(column))
    })
}

/// Collect equality and range predicates whose LHS/RHS is a non-trivial
/// expression (anything other than a bare column reference), keyed by
/// `expr_fingerprint`. Used to match functional index key parts
/// (`((LOWER(c)))`) against WHERE clauses like `WHERE LOWER(c) = 'foo'`
/// or `WHERE YEAR(d) BETWEEN 2024 AND 2025`. Plain column predicates
/// are still handled by `collect_index_constraints`.
fn expr_string_collation<'a>(table: &'a TableSchema, expr: &Expr) -> Option<&'a str> {
    match expr {
        Expr::Identifier(name) => table
            .columns
            .iter()
            .find(|column| column.name.eq_ignore_ascii_case(name))
            .and_then(|column| column.collation.as_deref()),
        Expr::CompoundIdentifier(parts) => parts.last().and_then(|name| {
            table
                .columns
                .iter()
                .find(|column| column.name.eq_ignore_ascii_case(name))
                .and_then(|column| column.collation.as_deref())
        }),
        // String-preserving functional indexes currently supported by the
        // LIKE range path (LOWER/LCASE/UPPER/UCASE) inherit their argument's
        // collation.  For any future expression shape, returning None keeps
        // the old open/conservative behavior instead of inventing bounds.
        Expr::Function { args, .. } => args
            .first()
            .and_then(|arg| expr_string_collation(table, arg)),
        _ => None,
    }
}

fn collect_index_expression_constraints(
    table: &TableSchema,
    expr: &Expr,
    eq_values: &mut HashMap<u64, PlanConstraintValue>,
    ranges: &mut HashMap<u64, ColumnRange>,
) {
    match expr {
        Expr::Binary { left, op, right } if *op == BinaryOp::And => {
            collect_index_expression_constraints(table, left, eq_values, ranges);
            collect_index_expression_constraints(table, right, eq_values, ranges);
        }
        Expr::Binary { left, op, right } if *op == BinaryOp::Eq => {
            let mut try_pair = |target: &Expr, value: &Expr| -> Option<()> {
                if expr_column_name(target).is_some() {
                    return None;
                }
                let value = expr_to_plan_constraint(value)?;
                // `LOWER(c) = NULL` is UNKNOWN for every row — leave it to the
                // residual filter, never a functional-index probe.
                if plan_constraint_is_null_literal(&value) {
                    return None;
                }
                let fp = crate::engine::expr_program::expr_fingerprint(target);
                eq_values.insert(fp, value);
                Some(())
            };
            try_pair(left, right);
            try_pair(right, left);
        }
        Expr::Binary { left, op, right }
            if matches!(
                op,
                BinaryOp::Gt | BinaryOp::GtEq | BinaryOp::Lt | BinaryOp::LtEq
            ) =>
        {
            // Same shape as the column path: bind the non-trivial side
            // as the "target" expression, the literal side as the bound.
            if expr_column_name(left).is_none() {
                if let Some(value) = expr_to_plan_constraint(right) {
                    let fp = crate::engine::expr_program::expr_fingerprint(left);
                    record_expression_range(ranges, fp, *op, value);
                }
            } else if expr_column_name(right).is_none() {
                if let Some(value) = expr_to_plan_constraint(left) {
                    let fp = crate::engine::expr_program::expr_fingerprint(right);
                    record_expression_range(ranges, fp, flip_cmp(*op), value);
                }
            }
        }
        Expr::Between {
            expr,
            negated: false,
            low,
            high,
        } => {
            if expr_column_name(expr).is_none() {
                if let (Some(low), Some(high)) =
                    (expr_to_plan_constraint(low), expr_to_plan_constraint(high))
                {
                    let fp = crate::engine::expr_program::expr_fingerprint(expr);
                    record_expression_range(ranges, fp, BinaryOp::GtEq, low);
                    record_expression_range(ranges, fp, BinaryOp::LtEq, high);
                }
            }
        }
        Expr::Like {
            expr,
            pattern,
            negated: false,
            escape_char,
        } => {
            // `LOWER(c) LIKE 'a%'` → `[a, b)` half-open range. Only
            // useful when the underlying expression returns a string;
            // for numeric expressions the bound encoding will simply
            // not match any leaf, so this is harmless.
            if expr_column_name(expr).is_some() {
                return;
            }
            let Some(pattern_str) = expr_literal_string(pattern) else {
                return;
            };
            // B10: if the wrapping expression is `LOWER(...)` / `UPPER(...)`,
            // pre-fold the LIKE pattern to the same case so the extracted
            // prefix aligns with what the functional index actually stores.
            // Without this, `LOWER(c) LIKE 'FOO%'` extracts `'FOO'`, but
            // every leaf in `idx_lower(c)` holds lowercase — the range
            // `[FOO, FOP)` matches nothing and the query silently full-scans.
            // Restricted to ASCII: `char::to_lowercase` on some code points
            // (e.g. 'İ' → 'i' + '\u{307}') expands to multiple chars, which
            // would misalign single-char pattern positions against the
            // index's per-char storage. For non-ASCII we conservatively skip
            // the range pushdown and let the residual filter own it.
            let folded_pattern = match wrapping_case_fold(expr) {
                Some(CaseFold::Lower) if pattern_str.is_ascii() => {
                    std::borrow::Cow::Owned(pattern_str.to_ascii_lowercase())
                }
                Some(CaseFold::Upper) if pattern_str.is_ascii() => {
                    std::borrow::Cow::Owned(pattern_str.to_ascii_uppercase())
                }
                Some(_) => return, // LOWER/UPPER but non-ASCII → skip
                None => std::borrow::Cow::Borrowed(pattern_str.as_str()),
            };
            // B12: hand the escape to the extractor so LIKE-under-ESCAPE
            // still yields a prefix; the extractor drops the special
            // char in-place and pushes the following literal.
            let fp = crate::engine::expr_program::expr_fingerprint(expr);
            let Some(shape) =
                crate::engine::expr::analyze_like_sargable_pattern(&folded_pattern, *escape_char)
            else {
                return;
            };
            if let crate::engine::expr::LikeSargablePattern::Literal(literal) = shape {
                eq_values.insert(fp, PlanConstraintValue::Concrete(Value::String(literal)));
                return;
            }
            let crate::engine::expr::LikeSargablePattern::Prefix(prefix) = shape else {
                unreachable!()
            };
            record_expression_range(
                ranges,
                fp,
                BinaryOp::GtEq,
                PlanConstraintValue::Concrete(Value::String(prefix.clone())),
            );
            let collation = expr_string_collation(table, expr);
            if let Some(high) =
                crate::engine::expr::like_prefix_successor_for_collation(&prefix, collation)
            {
                record_expression_range(
                    ranges,
                    fp,
                    BinaryOp::Lt,
                    PlanConstraintValue::Concrete(high),
                );
            }
            if let Some(range) = ranges.get_mut(&fp) {
                range.like_prefix = Some(prefix);
            }
        }
        _ => {}
    }
}

/// B10 — case-fold direction the wrapping function performs on its arg.
/// Used to pre-fold a LIKE pattern so `LOWER(c) LIKE 'FOO%'` and
/// `UPPER(c) LIKE 'foo%'` both drive the underlying functional index.
#[derive(Debug, Clone, Copy)]
enum CaseFold {
    Lower,
    Upper,
}

fn wrapping_case_fold(expr: &Expr) -> Option<CaseFold> {
    let Expr::Function { name, args, .. } = expr else {
        return None;
    };
    if args.len() != 1 {
        return None;
    }
    match name.to_ascii_uppercase().as_str() {
        "LOWER" | "LCASE" => Some(CaseFold::Lower),
        "UPPER" | "UCASE" => Some(CaseFold::Upper),
        _ => None,
    }
}

fn record_expression_range(
    ranges: &mut HashMap<u64, ColumnRange>,
    fingerprint: u64,
    op: BinaryOp,
    value: PlanConstraintValue,
) {
    let range = ranges.entry(fingerprint).or_insert(ColumnRange {
        low: None,
        high: None,
        like_prefix: None,
        like_param: None,
    });
    match op {
        BinaryOp::Gt => range.low = Some((value, false)),
        BinaryOp::GtEq => range.low = Some((value, true)),
        BinaryOp::Lt => range.high = Some((value, false)),
        BinaryOp::LtEq => range.high = Some((value, true)),
        _ => {}
    }
}

fn collect_index_constraints(
    table: &TableSchema,
    expr: &Expr,
    eq_values: &mut HashMap<String, PlanConstraintValue>,
    ranges: &mut HashMap<String, ColumnRange>,
) {
    crate::planner::index_bounds::collect_index_constraints(
        table,
        expr,
        eq_values,
        ranges,
        &expr_to_plan_constraint,
    );
}

/// A point domain must be a superset of SQL equality, not just a value
/// convertible to the stored type. In particular an integer column compared
/// in f64 space can match several adjacent integers above 2^53. Likewise a
/// binary string compared numerically cannot be found with one byte key.
/// Retain the scan oracle for these lossy cross-domain comparisons.
fn write_in_lookup_preserves_comparison(
    target: crate::storage::ParamTarget,
    values: &[PlanConstraintValue],
) -> bool {
    use crate::storage::ParamTarget;
    if !index_in_lookup_preserves_comparison(target, values) {
        return false;
    }
    values.iter().all(|value| {
        let runtime;
        let value = match value {
            PlanConstraintValue::Concrete(value) => value,
            PlanConstraintValue::Param(index) => {
                runtime = crate::engine::params::current_param(*index);
                let Some(value) = runtime.as_ref() else {
                    return false;
                };
                value
            }
        };
        if matches!(value, Value::Null) {
            return true;
        }
        match target {
            ParamTarget::Int | ParamTarget::UInt => matches!(value, Value::Int(_) | Value::UInt(_)),
            ParamTarget::Decimal { .. } => matches!(
                value,
                Value::Int(_)
                    | Value::UInt(_)
                    | Value::Decimal(_)
                    | Value::DecimalCompact(..)
                    | Value::Decimal128(..)
                    | Value::DecimalWide(..)
            ),
            ParamTarget::Float32 | ParamTarget::Float64 => {
                // The persisted float order distinguishes -0 and +0, while
                // SQL equality does not. A single zero probe is incomplete.
                match KeyPart::for_target(value.clone(), target) {
                    Ok(KeyPart::Float32(bits)) => {
                        let value = crate::storage::f32_from_order_u32(bits);
                        value.is_finite() && value != 0.0
                    }
                    Ok(KeyPart::Float64(bits)) => {
                        let value = crate::storage::f64_from_order_u64(bits);
                        value.is_finite() && value != 0.0
                    }
                    _ => false,
                }
            }
            ParamTarget::Bytes => matches!(
                value,
                Value::String(_) | Value::SmallString(_) | Value::Bytes(_)
            ),
            _ => true,
        }
    })
}

/// Enumerate a finite IN domain for a write without changing its predicate.
/// Reuse SELECT's constraint folding, collation guard and index-key shaping;
/// callers must materialize/deduplicate RowIds before changing any indexed key,
/// and recheck the complete predicate against the locked current row.
/// Expand one IN slot only: multiple lists never form an unbounded Cartesian
/// product. Other conjuncts remain residuals, including later index columns.
pub(crate) fn write_in_index_access(table: &TableSchema, expr: &Expr) -> Option<IndexAccess> {
    let mut in_values = HashMap::new();
    collect_index_in_constraints(table, expr, &mut in_values);
    let mut expression_in_values = HashMap::new();
    collect_index_expression_in_constraints(expr, &mut expression_in_values);
    if in_values.is_empty() && expression_in_values.is_empty() {
        return None;
    }
    let mut eq_values = HashMap::new();
    let mut ranges = HashMap::new();
    collect_index_constraints(table, expr, &mut eq_values, &mut ranges);
    let mut expression_eq_values = HashMap::new();
    let mut expression_ranges = HashMap::new();
    collect_index_expression_constraints(
        table,
        expr,
        &mut expression_eq_values,
        &mut expression_ranges,
    );
    let mut best: Option<(usize, usize, IndexAccess)> = None;
    for index in table
        .iter_queryable_indexes()
        .filter(|i| !i.is_fulltext() && !i.is_spatial() && !i.is_vector())
    {
        if index.has_multi_valued_parts() {
            continue;
        }
        let mut prefix = Vec::new();
        for pos in 0..index.columns.len() {
            let (equality, values) = match index.key_part(pos) {
                Some(crate::storage::IndexKeyPart::Column(name)) => {
                    let name = normalize_expr_column(name);
                    (eq_values.get(&name), in_values.get(&name))
                }
                Some(crate::storage::IndexKeyPart::Expression(part)) => {
                    let Some(ast) = part.ast.as_ref() else { break };
                    let fingerprint = crate::engine::expr_program::expr_fingerprint(ast);
                    (
                        expression_eq_values.get(&fingerprint),
                        expression_in_values.get(&fingerprint),
                    )
                }
                None => break,
            };
            let target = target_for_index_column(table, index, pos);
            if let Some(value) = equality {
                if !write_in_lookup_preserves_comparison(target, std::slice::from_ref(value)) {
                    break;
                }
                let Some(part) = constraint_key_part_for_index(value, target, index, pos) else {
                    break;
                };
                // Resolve parameters before sorting: parameter ordinal order is
                // unrelated to key order and distinct parameters may be equal.
                let Ok(part) = part.resolve_param() else {
                    break;
                };
                // Nullable unique keys may have multiple physical entries.
                // Do not use the unique batch decoder for an IS NULL prefix.
                if matches!(part, KeyPart::Null) {
                    break;
                }
                prefix.push(index.apply_prefix_to_key_part(pos, part));
                continue;
            }
            let Some(values) = values else { break };
            if !write_in_lookup_preserves_comparison(target, values) {
                break;
            }
            let keys = values
                .iter()
                .map(|value| {
                    let part = constraint_key_part_for_index(value, target, index, pos)?;
                    let mut parts = prefix.clone();
                    parts.push(index.apply_prefix_to_key_part(pos, part.resolve_param().ok()?));
                    Some(IndexKey::from(parts))
                })
                .collect::<Option<Vec<_>>>();
            let Some(mut keys) = keys else { break };
            keys.sort_unstable();
            keys.dedup();
            let exact = index.key_parts_are_exact(pos + 1) && pos + 1 == index.columns.len();
            // Prefer full unique probes, then longer constrained prefixes,
            // then fewer probes. No table-size/workload-specific thresholds.
            let rank = usize::from(exact && index.unique) * (table.columns.len() + 1) + pos + 1;
            if best.as_ref().is_none_or(|(old_rank, old_len, _)| {
                rank > *old_rank || (rank == *old_rank && keys.len() < *old_len)
            }) {
                best = Some((
                    rank,
                    keys.len(),
                    IndexAccess {
                        index: index.clone(),
                        kind: IndexAccessKind::MultiLookup { keys, exact },
                    },
                ));
            }
            break;
        }
    }
    best.map(|(_, _, access)| access)
}

fn collect_index_in_constraints(
    table: &TableSchema,
    expr: &Expr,
    in_values: &mut HashMap<String, Vec<PlanConstraintValue>>,
) {
    match expr {
        Expr::Binary { left, op, right } if *op == BinaryOp::And => {
            collect_index_in_constraints(table, left, in_values);
            collect_index_in_constraints(table, right, in_values);
        }
        Expr::InList {
            expr,
            list,
            negated: false,
        } => {
            let Some(column) = expr_column_name(expr) else {
                return;
            };
            let Some(values) = list
                .iter()
                .map(|item| {
                    let value = expr_to_plan_constraint(item)?;
                    Some(match value {
                        PlanConstraintValue::Concrete(value) => PlanConstraintValue::Concrete(
                            crate::engine::expr::contextual_numeric_literal(
                                value,
                                item,
                                &table.columns[table.column_index(column).ok()?].data_type,
                            ),
                        ),
                        value => value,
                    })
                })
                .collect::<Option<Vec<_>>>()
            else {
                return;
            };
            if !values.is_empty() {
                in_values.insert(normalize_expr_column(column), values);
            }
        }
        _ => {}
    }
}

/// Surface equality/IN domains that are implied by every branch of a
/// top-level OR, exclusively for index enumeration. For example,
/// `(state IN ('CA','OR') AND x) OR (state='NY' AND y)` safely permits a
/// three-key state-index lookup while the full OR remains the row filter.
fn collect_implied_or_in_constraints(
    table: &TableSchema,
    expr: &Expr,
    in_values: &mut HashMap<String, Vec<PlanConstraintValue>>,
) {
    let mut conjuncts = Vec::new();
    split_conjuncts(expr, &mut conjuncts);
    for conjunct in conjuncts {
        let disjuncts = split_disjuncts_top(&conjunct);
        if disjuncts.len() < 2 {
            continue;
        }
        let branches: Vec<Vec<Expr>> = disjuncts
            .into_iter()
            .map(|branch| {
                let mut branch_conjuncts = Vec::new();
                split_conjuncts(&branch, &mut branch_conjuncts);
                branch_conjuncts
            })
            .collect();
        for implied in derive_or_column_domain_unions(&branches) {
            collect_index_in_constraints(table, &implied, in_values);
        }
    }
}

/// IN-list predicate collector for functional index targets. Mirrors
/// `collect_index_in_constraints` but keys by AST fingerprint so
/// `WHERE LOWER(c) IN ('a','b')` can drive a MultiLookup over an
/// `INDEX ((LOWER(c)))`.
fn collect_index_expression_in_constraints(
    expr: &Expr,
    in_values: &mut HashMap<u64, Vec<PlanConstraintValue>>,
) {
    match expr {
        Expr::Binary { left, op, right } if *op == BinaryOp::And => {
            collect_index_expression_in_constraints(left, in_values);
            collect_index_expression_in_constraints(right, in_values);
        }
        Expr::InList {
            expr,
            list,
            negated: false,
        } => {
            // Bare-column shapes are handled by the column-keyed
            // collector; only record when LHS is a non-trivial
            // expression.
            if expr_column_name(expr).is_some() {
                return;
            }
            let Some(values) = list
                .iter()
                .map(expr_to_plan_constraint)
                .collect::<Option<Vec<_>>>()
            else {
                return;
            };
            if !values.is_empty() {
                let fp = crate::engine::expr_program::expr_fingerprint(expr);
                in_values.insert(fp, values);
            }
        }
        _ => {}
    }
}

fn collect_multi_valued_index_constraints(
    expr: &Expr,
    values_by_expr: &mut HashMap<u64, Vec<PlanConstraintValue>>,
) {
    match expr {
        Expr::Binary { left, op, right } if *op == BinaryOp::And => {
            collect_multi_valued_index_constraints(left, values_by_expr);
            collect_multi_valued_index_constraints(right, values_by_expr);
        }
        Expr::Function { name, args, .. }
            if name.eq_ignore_ascii_case("__MEMBER_OF") && args.len() == 2 =>
        {
            let Some(value) = expr_to_plan_constraint(&args[0]) else {
                return;
            };
            let fp = crate::engine::expr_program::expr_fingerprint(&args[1]);
            values_by_expr.entry(fp).or_default().push(value);
        }
        Expr::Function { name, args, .. }
            if name.eq_ignore_ascii_case("JSON_CONTAINS") && args.len() >= 2 =>
        {
            let (source, value) = if args.len() == 2 {
                (&args[0], &args[1])
            } else {
                let Some(source) = json_extract_expr(&args[0], &args[2]) else {
                    return;
                };
                let Some(value) = expr_to_single_json_array_candidate(&args[1]) else {
                    return;
                };
                let fp = crate::engine::expr_program::expr_fingerprint(&source);
                values_by_expr.entry(fp).or_default().push(value);
                return;
            };
            let Some(value) = expr_to_single_json_array_candidate(value) else {
                return;
            };
            let fp = crate::engine::expr_program::expr_fingerprint(source);
            values_by_expr.entry(fp).or_default().push(value);
        }
        Expr::Function { name, args, .. }
            if name.eq_ignore_ascii_case("JSON_OVERLAPS") && args.len() == 2 =>
        {
            let (source, values) = if let Some(values) = expr_to_json_array_candidates(&args[1]) {
                (&args[0], values)
            } else if let Some(values) = expr_to_json_array_candidates(&args[0]) {
                (&args[1], values)
            } else {
                return;
            };
            let fp = crate::engine::expr_program::expr_fingerprint(source);
            values_by_expr.entry(fp).or_default().extend(values);
        }
        _ => {}
    }
}

fn expr_to_single_json_array_candidate(expr: &Expr) -> Option<PlanConstraintValue> {
    expr_to_json_array_candidates(expr).and_then(|mut values| {
        if values.len() == 1 {
            Some(values.remove(0))
        } else {
            None
        }
    })
}

fn expr_to_json_array_candidates(expr: &Expr) -> Option<Vec<PlanConstraintValue>> {
    match expr_literal_value(expr)? {
        Value::Json(bytes) => json_array_values_to_plan_constraints(&bytes),
        Value::String(text) | Value::Decimal(text) => crate::engine::json::parse_text_opt(&text)
            .and_then(|bytes| json_array_values_to_plan_constraints(&bytes)),
        Value::Bytes(bytes) => {
            let text = String::from_utf8_lossy(&bytes);
            crate::engine::json::parse_text_opt(&text)
                .and_then(|bytes| json_array_values_to_plan_constraints(&bytes))
        }
        other => Some(vec![PlanConstraintValue::Concrete(other)]),
    }
}

fn json_array_values_to_plan_constraints(bytes: &[u8]) -> Option<Vec<PlanConstraintValue>> {
    let dom = crate::engine::json::to_serde(bytes).ok()?;
    let serde_json::Value::Array(items) = dom else {
        // Scalar candidate: single-element candidate list (treat like an
        // array with one element per MySQL semantics for JSON_CONTAINS).
        // Non-scalar (array/object) as the whole candidate is rejected —
        // MVI keys are scalars only.
        return match dom {
            serde_json::Value::Object(_) | serde_json::Value::Array(_) => None,
            _ => Some(vec![PlanConstraintValue::Concrete(Value::Json(
                bytes.to_vec(),
            ))]),
        };
    };
    let mut values = Vec::with_capacity(items.len());
    for item in items {
        if item.is_null() {
            continue;
        }
        // M2/C4 (fixes P2-9): if ANY element in the candidate array is
        // itself non-scalar (nested array or object), the MVI fast path
        // is unsound — an MVI stores CAST(... AS <scalar type> ARRAY)
        // entries, and cast_multi_valued_index_element rejects non-scalar
        // elements with 3903. Without this bail-out the planner would
        // silently coerce nested items to 0 and produce false positives.
        // Falling back to the general row-eval path is always correct.
        if item.is_object() || item.is_array() {
            return None;
        }
        values.push(PlanConstraintValue::Concrete(Value::Json(
            crate::engine::json::from_serde(&item),
        )));
    }
    Some(values)
}

fn json_extract_expr(doc: &Expr, path: &Expr) -> Option<Expr> {
    let path_value = expr_literal_value(path)?;
    let path = match path_value {
        Value::String(s) | Value::Decimal(s) => s,
        Value::Bytes(b) => String::from_utf8_lossy(&b).into_owned(),
        other => other.to_display_string(),
    };
    if path.is_empty() {
        return None;
    }
    Some(Expr::Binary {
        left: Box::new(doc.clone()),
        op: BinaryOp::Arrow,
        right: Box::new(Expr::Literal(crate::parser::Literal::String(path))),
    })
}

fn multi_valued_index_source_expr(expr: &Expr) -> Option<&Expr> {
    let Expr::Function { name, args, .. } = expr else {
        return None;
    };
    if name.eq_ignore_ascii_case("CAST") && args.len() >= 2 {
        args.first()
    } else {
        None
    }
}

const SECONDARY_COVERAGE_PENALTY_START: f64 = 0.20;
const SECONDARY_COVERAGE_PENALTY_END: f64 = 0.50;

fn inlj_coverage_floor_weight(
    coverage: f64,
    _distinct_coverage: f64,
    _matched_rows: f64,
    _table_rows: f64,
    _working_set_pages: f64,
    _effective_cache_pages: f64,
) -> f64 {
    if coverage <= SECONDARY_COVERAGE_PENALTY_START {
        return 0.0;
    }
    let coverage_weight = ((coverage - SECONDARY_COVERAGE_PENALTY_START)
        / (SECONDARY_COVERAGE_PENALTY_END - SECONDARY_COVERAGE_PENALTY_START))
        .clamp(0.0, 1.0);
    // Cache residency removes storage misses, not B-tree routing, RID
    // collection/sort, buffer lookup and heap decoding. Once a non-covering
    // probe spans a broad fraction of heap pages, row selectivity must not
    // suppress that work: sparse keys scattered over half the heap still pay
    // a half-heap access pattern. The smooth page-coverage ramp above is the
    // stable signal; it is derived only from persisted page geometry.
    coverage_weight
}

fn index_covers_projected_columns(
    table: &PlanTable,
    index: &IndexSchema,
    projection: Option<&[usize]>,
) -> bool {
    if index.has_prefix_columns() || index.has_expression_parts() {
        return false;
    }
    let available: HashSet<String> = index
        .effective_columns()
        .iter()
        .map(|column| normalize_expr_column(column))
        .collect();
    let columns: Box<dyn Iterator<Item = usize> + '_> = match projection {
        Some(projection) => Box::new(projection.iter().copied()),
        None => Box::new(0..table.schema.columns.len()),
    };
    columns
        .filter_map(|position| table.schema.columns.get(position))
        .all(|column| available.contains(&normalize_expr_column(&column.name)))
}

fn apply_large_secondary_index_nlj_penalty(
    per_lookup: &mut crate::engine::cbo::Cost,
    schema: &TableSchema,
    index: &IndexSchema,
    left_rows: f64,
    rows_per_lookup: f64,
) {
    if acts_like_primary_for_cost(schema, index) {
        return;
    }
    let matched_rows = left_rows * rows_per_lookup;
    if matched_rows <= 32_768.0 {
        return;
    }
    let factor = (matched_rows / 32_768.0).sqrt().clamp(1.0, 4.0);
    per_lookup.total *= factor;
    per_lookup.io_rnd_pages *= factor;
    per_lookup.cpu_units *= factor;
}

fn table_pages_for_index_nlj_lookup(
    schema: &TableSchema,
    index: &IndexSchema,
    table_pages: f64,
    lookup_count: f64,
) -> f64 {
    if table_pages <= 1.0 || lookup_count <= 1.0 {
        return table_pages;
    }
    if acts_like_primary_for_cost(schema, index) || lookup_count <= 1_024.0 {
        // Repeated probes revisit upper B-tree levels and small keysets tend
        // to touch a bounded heap working set. Bound only the cache model's
        // working set; do not amortize the lookup itself. Large non-primary
        // NLJs still cost against the full table so fact-table fanout does
        // not look artificially cheap.
        return lookup_count.min(table_pages).max(1.0);
    }
    table_pages
}

/// Whether a hash join's probe side has the shape the executor's
/// streaming chain (`join_stream::chain_applicable`) can consume: a
/// (parallel) seq scan tail or another hash-join level, with no
/// residual and a chain-sized build. Used as a *tie-break signal* when
/// choosing between a hash join and an INLJ for the same join — NOT as
/// a cost adjustment, so join-order enumeration is unaffected.
/// Whether a plan's root is a bare scan (or a Exchange over one) — the
/// shapes whose rows the batched-INLJ outer-parallel workers can chunk
/// without fighting an upstream join pipeline.
fn plan_root_is_scan_or_exchange(plan: &PhysicalPlan) -> bool {
    match plan {
        PhysicalPlan::SeqScan { .. }
        | PhysicalPlan::ZoneMapScan { .. }
        | PhysicalPlan::ParallelSeqScan { .. }
        | PhysicalPlan::ParallelZoneMapScan { .. }
        | PhysicalPlan::IndexRangeScan { .. }
        | PhysicalPlan::ParallelIndexRangeScan { .. } => true,
        PhysicalPlan::Exchange { input, .. } => plan_root_is_scan_or_exchange(input),
        _ => false,
    }
}

pub(crate) fn adaptive_candidate_choice(candidate: &AdaptiveJoinCandidate) -> AdaptiveJoinChoice {
    match candidate {
        AdaptiveJoinCandidate::Hash { .. } => AdaptiveJoinChoice::Hash,
        AdaptiveJoinCandidate::IndexNestedLoop { .. } => AdaptiveJoinChoice::IndexNestedLoop,
        AdaptiveJoinCandidate::NestedLoop { .. } => AdaptiveJoinChoice::NestedLoop,
    }
}

const SEVERE_GRACE_BUILD_OVERFLOW: f64 = 6.0;

fn severe_grace_build_overflow(
    cost: crate::engine::cbo::Cost,
    planning_mem_budget_bytes: u64,
) -> bool {
    cost.spill_bytes >= planning_mem_budget_bytes as f64 * SEVERE_GRACE_BUILD_OVERFLOW
}

fn grace_hash_unique_lookup_risk_enabled() -> bool {
    !matches!(
        crate::util::env::var("MYDB_CBO_GRACE_UNIQUE_LOOKUP_RISK")
            .unwrap_or_default()
            .to_ascii_lowercase()
            .as_str(),
        "off" | "0" | "false" | "no" | "disable" | "disabled"
    )
}

pub(crate) fn adaptive_candidate_rows(candidate: &AdaptiveJoinCandidate) -> f64 {
    match candidate {
        AdaptiveJoinCandidate::Hash { estimated_rows, .. }
        | AdaptiveJoinCandidate::IndexNestedLoop { estimated_rows, .. }
        | AdaptiveJoinCandidate::NestedLoop { estimated_rows, .. } => *estimated_rows,
    }
}

pub(crate) fn adaptive_choice_name(choice: AdaptiveJoinChoice) -> &'static str {
    match choice {
        AdaptiveJoinChoice::Hash => "hash",
        AdaptiveJoinChoice::IndexNestedLoop => "index_nested_loop",
        AdaptiveJoinChoice::NestedLoop => "nested_loop",
    }
}

fn adaptive_candidate_reference_expr(candidates: &[AdaptiveJoinCandidate]) -> Option<Expr> {
    let mut exprs = Vec::new();
    for candidate in candidates {
        match candidate {
            AdaptiveJoinCandidate::Hash {
                left_keys,
                right_keys,
                residual,
                ..
            } => {
                exprs.extend(left_keys.iter().cloned());
                exprs.extend(right_keys.iter().cloned());
                if let Some(residual) = residual {
                    exprs.push(residual.clone());
                }
            }
            AdaptiveJoinCandidate::IndexNestedLoop { condition, .. }
            | AdaptiveJoinCandidate::NestedLoop { condition, .. } => {
                if let Some(condition) = condition {
                    exprs.push(condition.clone());
                }
            }
        }
    }
    combine_conjuncts(exprs)
}

/// Estimate an OR of exact literal assignments over the same qualified
/// column set after its per-table filters have been pushed down.
///
/// For `(a.region='E' AND b.region='W') OR
/// (a.region='W' AND b.region='E')`, the scans already retain E/W on each
/// side. Two of the four remaining assignments survive, so the residual
/// join selectivity is 2/4 rather than the generic theta fallback.
fn literal_assignment_dnf_selectivity(expr: &Expr) -> Option<f64> {
    fn qualified_column(expr: &Expr) -> Option<String> {
        let Expr::CompoundIdentifier(parts) = expr else {
            return None;
        };
        if parts.len() < 2 {
            return None;
        }
        Some(format!(
            "{}.{}",
            parts[parts.len() - 2].to_ascii_lowercase(),
            parts.last()?.to_ascii_lowercase()
        ))
    }
    fn constant_fingerprint(expr: &Expr) -> Option<u64> {
        matches!(expr, Expr::Literal(_) | Expr::BoundValue(_))
            .then(|| crate::engine::expr_program::expr_fingerprint(expr))
    }

    let mut disjuncts = Vec::new();
    collect_expr_disjuncts(expr, &mut disjuncts);
    if disjuncts.len() < 2 {
        return None;
    }
    let mut branches: Vec<std::collections::BTreeMap<String, u64>> =
        Vec::with_capacity(disjuncts.len());
    for disjunct in disjuncts {
        let mut conjuncts = Vec::new();
        collect_expr_conjuncts(disjunct, &mut conjuncts);
        let mut branch = std::collections::BTreeMap::new();
        for conjunct in conjuncts {
            let Expr::Binary {
                left,
                op: BinaryOp::Eq,
                right,
            } = conjunct
            else {
                return None;
            };
            let (column, literal) = qualified_column(left)
                .zip(constant_fingerprint(right))
                .or_else(|| qualified_column(right).zip(constant_fingerprint(left)))?;
            if branch.insert(column, literal).is_some() {
                return None;
            }
        }
        branches.push(branch);
    }
    let columns: Vec<String> = branches.first()?.keys().cloned().collect();
    if columns.len() < 2
        || branches
            .iter()
            .any(|branch| branch.keys().ne(columns.iter()))
    {
        return None;
    }
    let mut domains: Vec<HashSet<u64>> = (0..columns.len()).map(|_| HashSet::new()).collect();
    let mut assignments: HashSet<Vec<u64>> = HashSet::new();
    for branch in branches {
        let assignment: Vec<u64> = columns
            .iter()
            .enumerate()
            .map(|(idx, column)| {
                let value = *branch.get(column).expect("column set checked above");
                domains[idx].insert(value);
                value
            })
            .collect();
        assignments.insert(assignment);
    }
    let universe = domains
        .iter()
        .try_fold(1usize, |product, domain| product.checked_mul(domain.len()))?
        .max(1);
    Some((assignments.len() as f64 / universe as f64).clamp(0.0, 1.0))
}

/// A theta comparison can retain a large fraction of the Cartesian input.
/// Do not price it with the equality-like join fallback: that makes a broad
/// dimension-to-dimension range join look tiny and cheap to build a hash
/// table on. Use the same range fallback as scans/dependent range lookups,
/// including comparisons whose operands are expressions or different types.
fn theta_join_selectivity(condition: &Expr, model: &crate::engine::cbo::CostModelSnapshot) -> f64 {
    if let Some(selectivity) = literal_assignment_dnf_selectivity(condition) {
        return selectivity;
    }
    match condition {
        Expr::Binary {
            op: BinaryOp::Lt | BinaryOp::LtEq | BinaryOp::Gt | BinaryOp::GtEq,
            ..
        } => model.default_range_selectivity,
        Expr::Binary {
            op: BinaryOp::NotEq,
            ..
        } => 1.0 - model.default_join_selectivity,
        // Keep the existing fallback for equality and unsupported predicate
        // shapes; compound predicates need their own correlation estimates.
        _ => model.default_join_selectivity,
    }
    .clamp(0.0, 1.0)
}

pub(crate) fn choose_join_with_hints(
    ctx: &crate::engine::cbo::CboContext,
    left: PhysicalPlan,
    right: PhysicalPlan,
    join_type: JoinType,
    condition: Option<Expr>,
    query_hints: &[QueryHint],
) -> PhysicalPlan {
    let left_tables = collect_physical_plan_tables(&left);
    let right_tables = collect_physical_plan_tables(&right);
    let join_hint = resolve_join_hint(query_hints, &left_tables, &right_tables);

    let left_rows = estimated_rows(&left);
    let right_rows = estimated_rows(&right);
    // Converting a base access path into an INLJ embeds only its table/index
    // metadata and otherwise drops the right plan. Preserve the scan's
    // required-column projection here: upper hash-join costing happens before
    // the final pruning pass, so losing it makes a narrow INLJ subtree look as
    // wide as the complete inner table and can invent a large spill penalty.
    let inlj_right_projected_columns = base_scan_projected_columns(&right);

    let inlj_candidate = if matches!(join_hint, JoinHintPref::ForceHash | JoinHintPref::ForceNl) {
        None
    } else if matches!(
        join_type,
        JoinType::Inner | JoinType::Left | JoinType::Semi | JoinType::Anti
    ) {
        dependent_range_join_index(&left, &right, condition.as_ref(), query_hints)
    } else {
        None
    };
    let inlj_data = inlj_candidate
        .as_ref()
        .map(|(rt, ri, eql, has_range, enforced_filter)| {
            let candidate_condition = match (condition.clone(), enforced_filter.clone()) {
                (Some(left), Some(right)) => Some(Expr::Binary {
                    left: Box::new(left),
                    op: BinaryOp::And,
                    right: Box::new(right),
                }),
                (left, right) => left.or(right),
            };
            let mut est = estimate_index_nlj_rows(
                &left,
                rt,
                ri,
                *eql,
                left_rows,
                join_type,
                candidate_condition.as_ref(),
            );
            // Right-only residual: conjuncts (from the explicit condition or
            // a filter attached to the right scan) that survive the index
            // probe and apply per-row to inner output. Used for both
            // selectivity scaling AND per-row CPU cost in the cost model.
            let residual =
                collect_inlj_right_only_residual(candidate_condition.as_ref(), &right, rt);
            // Apply right-only residual selectivity: conjuncts that survive
            // the index probe and filter the per-probe output. Without this,
            // INLJ's output estimate
            // doesn't reflect the residual filter while the hash join's
            // does (its scan-side filter is applied below the join), so
            // the cost model systematically over-prices INLJ vs hash.
            est = apply_inlj_right_only_residual_selectivity(
                est,
                candidate_condition.as_ref(),
                &left,
                &right,
                rt,
                join_type,
                left_rows,
            );
            let batchable = !*has_range;
            let right_covering = index_covers_projected_columns(
                rt,
                ri,
                inlj_right_projected_columns.as_deref().map(Vec::as_slice),
            );
            let cost = estimate_index_nlj_operator_cost(
                ctx,
                &left,
                rt,
                ri,
                *eql,
                batchable,
                join_type,
                left_rows,
                est,
                candidate_condition.as_ref(),
                residual.as_ref(),
                right_covering,
            );
            let right_filter = extract_scan_filter(&right);
            let full_condition = match (candidate_condition, right_filter) {
                (Some(c), Some(f)) => Some(Expr::Binary {
                    left: Box::new(c),
                    op: BinaryOp::And,
                    right: Box::new(f),
                }),
                (c, f) => c.or(f),
            };
            (
                rt.clone(),
                ri.clone(),
                *eql,
                est,
                cost,
                full_condition,
                inlj_right_projected_columns.clone(),
                right_covering,
            )
        });

    let hj_candidate = if matches!(
        join_hint,
        JoinHintPref::NoHash | JoinHintPref::ForceNl | JoinHintPref::ForceIndexNlj
    ) {
        None
    } else if matches!(
        join_type,
        JoinType::Inner
            | JoinType::Left
            | JoinType::Right
            | JoinType::Semi
            | JoinType::Anti
            // FULL OUTER JOIN with equi keys: the executor's hash join already
            // emits unmatched rows on BOTH sides (`build_unmatched_must_output`
            // / `probe_unmatched_must_output` return true for Full, driving the
            // `build_matched` bitmap + probe-unmatched emit). Without this the
            // join falls to an O(N×M) nested loop (e.g. TPC-DS Q97's CTE FULL
            // join estimated 1.1B rows). No equi key ⇒ `extract_join_keys`
            // returns None ⇒ NLJ fallback, unchanged.
            | JoinType::Full
    ) {
        condition
            .as_ref()
            .and_then(|expr| extract_join_keys_for_plans(&left, &right, expr))
    } else {
        None
    };
    let mut hj_data = hj_candidate
        .as_ref()
        .map(|(left_keys, right_keys, null_safe, residual)| {
            let (cost, build_side, est) = estimate_hash_join_operator_cost(
                ctx,
                &left,
                &right,
                join_type,
                left_keys,
                right_keys,
                residual.as_ref(),
                left_rows,
                right_rows,
            );
            (
                left_keys.clone(),
                right_keys.clone(),
                null_safe.clone(),
                residual.clone(),
                cost,
                build_side,
                est,
            )
        });

    // Scope the Grace codec risk to a concrete alternative at this join
    // boundary.  Recording it in the hash candidate itself (rather than only
    // in the local min comparison) is important: dynamic-programming join
    // enumeration compares the cumulative cost of the returned subtree and
    // would otherwise prune the unique-lookup subtree using the unadjusted
    // spilled-hash number.
    let direct_safe_unique_lookup =
        inlj_data
            .as_ref()
            .is_some_and(|(_, right_index, eq_prefix_len, _, cost, _, _, _)| {
                right_index.unique
                    && *eq_prefix_len >= right_index.columns.len()
                    && !right_index.has_prefix_columns()
                    && !right_index.has_expression_parts()
                    && cost.spill_bytes <= 0.0
            });
    // INNER hash joins are commutative. Complete-DP evaluates both
    // orientations and may keep the hash node whose unique-index table is on
    // the left, even though the equivalent lookup node is emitted only when
    // that table is on the right. Detect the reverse exact lookup as well so
    // swapping children cannot erase the same physical spill risk.
    let reverse_safe_unique_lookup = join_type == JoinType::Inner
        && dependent_range_join_index(&right, &left, condition.as_ref(), query_hints).is_some_and(
            |(_, index, eq_prefix_len, has_range, _)| {
                !has_range
                    && index.unique
                    && eq_prefix_len >= index.columns.len()
                    && !index.has_prefix_columns()
                    && !index.has_expression_parts()
            },
        );
    // Small, one-pass spills can still beat a fanout lookup (fresh A/Bs on
    // broad joins confirm that). Reserve the risk envelope for a severe build
    // overflow where encoded state is many work areas larger than the grant;
    // below that boundary the ordinary byte/page model remains authoritative.
    let severe_grace_spill = hj_data.as_ref().is_some_and(|(_, _, _, _, cost, _, _)| {
        severe_grace_build_overflow(*cost, ctx.planning_mem_budget_bytes)
    });
    let has_safe_unique_lookup = grace_hash_unique_lookup_risk_enabled()
        && join_type == JoinType::Inner
        && severe_grace_spill
        && (direct_safe_unique_lookup || reverse_safe_unique_lookup);
    if has_safe_unique_lookup {
        if let Some((_, _, _, _, cost, _, _)) = hj_data.as_mut() {
            let surcharge = grace_hash_unique_lookup_risk_surcharge(*cost);
            if surcharge > 0.0 {
                let original_total = cost.total;
                cost.startup += surcharge;
                cost.total += surcharge;
                crate::engine::cbo::trace::event(
                    "grace_hash_unique_lookup_risk",
                    serde_json::json!({
                        "hash_cost": original_total,
                        "adjusted_hash_cost": cost.total,
                        "spill_bytes": cost.spill_bytes,
                        "risk_surcharge": surcharge,
                    }),
                );
            }
        }
    }

    use crate::engine::cbo::operator_cost;
    // Theta-join estimates use predicate-appropriate public coefficients and
    // is adjusted for join semantics. Outer joins emit every preserved-side row at
    // least once (NULL-extended when unmatched), and semi/anti emit a subset
    // of the left side. Without this, a LEFT/RIGHT/FULL NLJ could estimate
    // fewer rows than its preserved side, and a semi/anti NLJ could exceed
    // the left side — the same outer-join floor the hash-join path applies.
    let inner_est = if join_type == JoinType::Cross {
        left_rows * right_rows
    } else {
        let selectivity = condition
            .as_ref()
            .map(|condition| theta_join_selectivity(condition, &ctx.model))
            .unwrap_or(ctx.model.default_join_selectivity);
        left_rows * right_rows * selectivity
    };
    let est_rows = match join_type {
        // Keep a non-empty inner join from collapsing below one expected
        // row solely because several independent fallback selectivities were
        // multiplied together. Sub-unit estimates are useful for filters but
        // toxic as an outer cardinality: every downstream point/fanout probe
        // is then under-priced by the same factor. Preserve a true zero when
        // either input is known empty.
        JoinType::Inner if left_rows > 0.0 && right_rows > 0.0 => inner_est.max(1.0),
        JoinType::Inner | JoinType::Cross => inner_est,
        JoinType::Left => inner_est.max(left_rows),
        JoinType::Right => inner_est.max(right_rows),
        JoinType::Full => inner_est.max(left_rows.max(right_rows)),
        // Semi: at most one output row per left row.
        JoinType::Semi => inner_est.min(left_rows).max(0.0),
        // Anti: the left rows with no match ≈ left_rows minus the (bounded)
        // matched left rows; sparse matches ⇒ most left rows survive.
        JoinType::Anti => (left_rows - inner_est.min(left_rows)).max(0.0),
    };
    let qual_cpu = condition.as_ref().map(qual_cost_per_row).unwrap_or(0.0);
    let mut nlj = operator_cost::nested_loop_join(
        &plan_cost(&left),
        &plan_cost(&right),
        left_rows,
        right_rows,
        est_rows,
        left.output_width() + right.output_width(),
        qual_cpu,
    );
    // WS-N7a: the executor parallelizes by outer chunk once |L|×|R| exceeds
    // `MYDB_NLJ_OUTER_PARALLEL_THRESHOLD`. The CBO discounts this node's incremental CPU/total cost
    // accordingly, so NLJ and the parallel executor use the same basis when comparing adaptive join candidates.
    // The discount applies only to the increment above this node (`cost - outer_cost`), ensuring the
    // join can never be cheaper than the already-built outer (the cumulative-cost invariance constraint).
    if !cbo_nlj_outer_parallel_discount_disabled() {
        let degree = nlj_outer_parallelism_for_cost(left_rows, right_rows);
        if degree > 1 {
            let outer = plan_cost(&left);
            let d = degree as f64;
            let inc_total = (nlj.total - outer.total).max(0.0);
            let inc_cpu = (nlj.cpu_units - outer.cpu_units).max(0.0);
            nlj.total = outer.total + inc_total / d;
            nlj.cpu_units = outer.cpu_units + inc_cpu / d;
        }
    }

    if matches!(join_hint, JoinHintPref::ForceIndexNlj) {
        if let Some((
            right_table,
            right_index,
            eq_prefix_len,
            est,
            cost,
            full_condition,
            right_projected_columns,
            right_covering,
        )) = inlj_data
        {
            return PhysicalPlan::IndexNestedLoopJoin {
                left: Box::new(left),
                right_table,
                right_index,
                join_type,
                condition: full_condition,
                eq_prefix_len,
                estimated_rows: est,
                cost,
                right_covering,
                right_projected_columns,
                right_runtime_filters: Vec::new(),
            };
        }
    }
    if matches!(join_hint, JoinHintPref::ForceHash) {
        if let Some((left_keys, right_keys, null_safe, residual, cost, build_side, est)) = hj_data {
            return PhysicalPlan::HashJoin {
                left: Box::new(left),
                right: Box::new(right),
                join_type,
                left_keys,
                right_keys,
                null_safe,
                build_side,
                residual,
                output_projection: None,
                estimated_rows: est,
                cost,
            };
        }
    }
    if matches!(join_hint, JoinHintPref::ForceNl) {
        return PhysicalPlan::NestedLoopJoin {
            left: Box::new(left),
            right: Box::new(right),
            join_type,
            condition,
            estimated_rows: est_rows,
            cost: nlj,
        };
    }

    let mut candidates = Vec::new();
    if let Some((
        right_table,
        right_index,
        eq_prefix_len,
        est,
        cost,
        full_condition,
        right_projected_columns,
        right_covering,
    )) = inlj_data.clone()
    {
        candidates.push(AdaptiveJoinCandidate::IndexNestedLoop {
            right_table,
            right_index,
            condition: full_condition,
            eq_prefix_len,
            estimated_rows: est,
            cost,
            right_covering,
            right_projected_columns,
        });
    }
    if let Some((left_keys, right_keys, null_safe, residual, cost, build_side, est)) =
        hj_data.clone()
    {
        candidates.push(AdaptiveJoinCandidate::Hash {
            left_keys,
            right_keys,
            null_safe,
            build_side,
            residual,
            estimated_rows: est,
            cost,
        });
    }
    candidates.push(AdaptiveJoinCandidate::NestedLoop {
        condition: condition.clone(),
        estimated_rows: est_rows,
        cost: nlj,
    });

    let candidate_choice_weight = |candidate: &AdaptiveJoinCandidate| -> f64 {
        adaptive_candidate_cost(candidate).weighted_total()
    };

    let adaptive_enabled = crate::util::env::var("MYDB_ADAPTIVE_JOIN")
        .map(|v| !matches!(v.as_str(), "0" | "off" | "OFF" | "false" | "FALSE"))
        .unwrap_or(false);
    let cost_preferred = candidates
        .iter()
        .min_by(|a, b| {
            candidate_choice_weight(a)
                .partial_cmp(&candidate_choice_weight(b))
                .unwrap_or(std::cmp::Ordering::Equal)
        })
        .map(adaptive_candidate_choice)
        .unwrap_or(AdaptiveJoinChoice::NestedLoop);
    let preferred = if adaptive_enabled {
        cost_preferred
    } else if let Some(non_nlj_preferred) = candidates
        .iter()
        .filter(|candidate| adaptive_candidate_choice(candidate) != AdaptiveJoinChoice::NestedLoop)
        .min_by(|a, b| {
            candidate_choice_weight(a)
                .partial_cmp(&candidate_choice_weight(b))
                .unwrap_or(std::cmp::Ordering::Equal)
        })
        .map(adaptive_candidate_choice)
    {
        non_nlj_preferred
    } else {
        AdaptiveJoinChoice::NestedLoop
    };
    if adaptive_enabled && candidates.len() > 1 {
        let (cost, estimated_rows) = candidates
            .iter()
            .find(|candidate| adaptive_candidate_choice(candidate) == preferred)
            .map(|candidate| {
                (
                    adaptive_candidate_cost(candidate),
                    adaptive_candidate_rows(candidate),
                )
            })
            .unwrap_or((nlj, est_rows));
        return PhysicalPlan::AdaptiveJoin {
            left: Box::new(left),
            right: Box::new(right),
            join_type,
            candidates,
            preferred,
            estimated_rows,
            cost,
        };
    }

    if preferred == AdaptiveJoinChoice::IndexNestedLoop {
        if let Some((
            right_table,
            right_index,
            eq_prefix_len,
            est,
            cost,
            full_condition,
            right_projected_columns,
            right_covering,
        )) = inlj_data
        {
            return PhysicalPlan::IndexNestedLoopJoin {
                left: Box::new(left),
                right_table,
                right_index,
                join_type,
                condition: full_condition,
                eq_prefix_len,
                estimated_rows: est,
                cost,
                right_covering,
                right_projected_columns,
                right_runtime_filters: Vec::new(),
            };
        }
    }
    if preferred == AdaptiveJoinChoice::Hash {
        if let Some((left_keys, right_keys, null_safe, residual, cost, build_side, est)) = hj_data {
            return PhysicalPlan::HashJoin {
                left: Box::new(left),
                right: Box::new(right),
                join_type,
                left_keys,
                right_keys,
                null_safe,
                build_side,
                residual,
                output_projection: None,
                estimated_rows: est,
                cost,
            };
        }
    }
    PhysicalPlan::NestedLoopJoin {
        left: Box::new(left),
        right: Box::new(right),
        join_type,
        condition,
        estimated_rows: est_rows,
        cost: nlj,
    }
}

fn plan_table_stats(plan: &PhysicalPlan) -> Option<&Arc<TableSchema>> {
    match plan {
        PhysicalPlan::SeqScan { table, .. }
        | PhysicalPlan::ZoneMapScan { table, .. }
        | PhysicalPlan::IndexLookup { table, .. }
        | PhysicalPlan::IndexRangeScan { table, .. }
        | PhysicalPlan::IndexFastScan { table, .. }
        | PhysicalPlan::ParallelSeqScan { table, .. }
        | PhysicalPlan::ParallelIndexRangeScan { table, .. }
        | PhysicalPlan::ParallelIndexFastScan { table, .. }
        | PhysicalPlan::IndexInLookup { table, .. } => Some(&table.schema),
        PhysicalPlan::IndexNestedLoopJoin { right_table, .. } => Some(&right_table.schema),
        _ => None,
    }
}

fn ndv_for_column(schema: &TableSchema, col: &str) -> Option<u64> {
    let column_ndv = schema
        .stats
        .columns
        .get(&normalize_expr_column(col))
        .map(|s| s.distinct_count)
        .filter(|&ndv| ndv > 0);
    // Cross-check against the leading-prefix NDV of any index on this
    // column. Both come from the same ANALYZE pass, but join-cardinality
    // estimation divides by NDV — so an *underestimate* on either side
    // blows the join estimate up by the same factor. Take the larger of
    // the two: it keeps the hash-join cardinality consistent with what
    // the INLJ path derives from index fanout on the identical join.
    let index_ndv = schema
        .indexes
        .iter()
        .filter(|index| {
            index
                .columns
                .first()
                .is_some_and(|c| normalize_expr_column(c) == normalize_expr_column(col))
        })
        .filter_map(|index| {
            schema
                .stats
                .indexes
                .get(&normalize_expr_column(&index.name))
                .and_then(|s| s.prefix_distinct_counts.first().copied())
                .filter(|&n| n > 0)
        })
        .max();
    match (column_ndv, index_ndv) {
        (Some(c), Some(i)) => Some(c.max(i)),
        (a, b) => a.or(b),
    }
}

fn exprs_equivalent_in_plan(plan: &PhysicalPlan, left: &Expr, right: &Expr) -> bool {
    let (Some(left), Some(right)) = (equivalence_column_key(left), equivalence_column_key(right))
    else {
        return left == right;
    };
    if left == right {
        return true;
    }

    fn add_edge(graph: &mut HashMap<EqColumnKey, Vec<EqColumnKey>>, left: &Expr, right: &Expr) {
        let (Some(left), Some(right)) =
            (equivalence_column_key(left), equivalence_column_key(right))
        else {
            return;
        };
        graph.entry(left.clone()).or_default().push(right.clone());
        graph.entry(right).or_default().push(left);
    }

    fn add_expr_edges(graph: &mut HashMap<EqColumnKey, Vec<EqColumnKey>>, expr: &Expr) {
        let mut conjuncts = Vec::new();
        split_conjuncts(expr, &mut conjuncts);
        for conjunct in conjuncts {
            if let Expr::Binary {
                left,
                op: BinaryOp::Eq | BinaryOp::NullSafeEq,
                right,
            } = conjunct
            {
                add_edge(graph, &left, &right);
            }
        }
    }

    fn collect(plan: &PhysicalPlan, graph: &mut HashMap<EqColumnKey, Vec<EqColumnKey>>) {
        match plan {
            PhysicalPlan::HashJoin {
                join_type: JoinType::Inner,
                left_keys,
                right_keys,
                residual,
                ..
            } => {
                for (left, right) in left_keys.iter().zip(right_keys.iter()) {
                    add_edge(graph, left, right);
                }
                if let Some(residual) = residual {
                    add_expr_edges(graph, residual);
                }
            }
            PhysicalPlan::NestedLoopJoin {
                join_type: JoinType::Inner,
                condition: Some(condition),
                ..
            }
            | PhysicalPlan::IndexNestedLoopJoin {
                join_type: JoinType::Inner,
                condition: Some(condition),
                ..
            } => add_expr_edges(graph, condition),
            PhysicalPlan::Filter { predicate, .. } | PhysicalPlan::Apply { predicate, .. } => {
                add_expr_edges(graph, predicate)
            }
            _ => {}
        }
        for child in plan.children() {
            collect(child, graph);
        }
    }

    let mut graph = HashMap::new();
    collect(plan, &mut graph);
    let mut pending = vec![left.clone()];
    let mut seen = HashSet::new();
    while let Some(current) = pending.pop() {
        if !seen.insert(current.clone()) {
            continue;
        }
        if current == right {
            return true;
        }
        if let Some(neighbors) = graph.get(&current) {
            pending.extend(neighbors.iter().cloned());
        }
    }
    false
}

/// Prove that `prefiltered.key = unique.key` cannot remove rows from the
/// prefiltered side because its keyset came from the same unique-key relation.
///
/// SIP produces exactly this shape:
///
///   customer.nation_key IN (SELECT nation.key FROM ... filtered nation ...)
///   customer.nation_key = nation.key
///
/// Charging the nation-domain selectivity again makes the propagated filter a
/// liability in the cost model. The proof is deliberately narrow: the
/// subquery must project the joined column from the unique relation, and any
/// filter retained on that relation must be present in (or be a one-hop SIP
/// restatement of) the keyset subquery.
fn keyset_prefilter_guarantees_unique_join(
    prefiltered_plan: &PhysicalPlan,
    unique_plan: &PhysicalPlan,
    prefiltered_key: &Expr,
    unique_key: &Expr,
) -> bool {
    let Some(subquery) = keyset_subquery_for_plan_key(prefiltered_plan, prefiltered_key) else {
        return false;
    };
    let Some((unique_table, unique_filter)) = unique_scan_for_key(unique_plan, unique_key) else {
        return false;
    };
    let Some(unique_column) = expr_column_name(unique_key) else {
        return false;
    };
    if !unique_table.schema.iter_queryable_indexes().any(|index| {
        index.unique
            && index.columns.len() == 1
            && normalize_expr_column(&index.columns[0]) == normalize_expr_column(unique_column)
    }) {
        return false;
    }
    let projects_unique_key = matches!(
        subquery.projection.as_slice(),
        [SelectItem::Expr { expr, .. }]
            if expr_matches_table_column(expr, unique_table, unique_column)
    );
    let mentions_table = subquery_mentions_table(subquery, unique_table);
    let filter_covered =
        unique_filter.is_none_or(|filter| keyset_subquery_covers_filter(subquery, filter));
    projects_unique_key && mentions_table && filter_covered
}

fn keyset_subquery_for_plan_key<'a>(plan: &'a PhysicalPlan, key: &Expr) -> Option<&'a Select> {
    fn in_filter<'a>(filter: &'a Expr, table: &PlanTable, key: &Expr) -> Option<&'a Select> {
        match filter {
            Expr::Binary {
                left,
                op: BinaryOp::And,
                right,
            } => in_filter(left, table, key).or_else(|| in_filter(right, table, key)),
            Expr::InSubquery {
                expr,
                subquery,
                negated: false,
            } if expr_column_name(key).is_some_and(|column| {
                expr_matches_table_column(expr, table, column)
                    && expr_matches_table_column(key, table, column)
            }) =>
            {
                Some(subquery)
            }
            _ => None,
        }
    }

    match plan {
        PhysicalPlan::IndexInLookup {
            table,
            in_column,
            subquery,
            ..
        } if expr_matches_table_column(key, table, in_column) => Some(subquery),
        PhysicalPlan::SeqScan {
            table,
            filter: Some(filter),
            ..
        }
        | PhysicalPlan::ZoneMapScan {
            table,
            filter: Some(filter),
            ..
        }
        | PhysicalPlan::IndexLookup {
            table,
            filter: Some(filter),
            ..
        }
        | PhysicalPlan::IndexMultiLookup {
            table,
            filter: Some(filter),
            ..
        }
        | PhysicalPlan::IndexRangeScan {
            table,
            filter: Some(filter),
            ..
        }
        | PhysicalPlan::IndexFastScan {
            table,
            filter: Some(filter),
            ..
        }
        | PhysicalPlan::ParallelSeqScan {
            table,
            filter: Some(filter),
            ..
        }
        | PhysicalPlan::ParallelZoneMapScan {
            table,
            filter: Some(filter),
            ..
        }
        | PhysicalPlan::ParallelIndexFastScan {
            table,
            filter: Some(filter),
            ..
        }
        | PhysicalPlan::ParallelIndexRangeScan {
            table,
            filter: Some(filter),
            ..
        } => in_filter(filter, table, key),
        PhysicalPlan::HashJoin { left, right, .. }
        | PhysicalPlan::NestedLoopJoin { left, right, .. }
        | PhysicalPlan::AdaptiveJoin { left, right, .. } => keyset_subquery_for_plan_key(left, key)
            .or_else(|| keyset_subquery_for_plan_key(right, key)),
        PhysicalPlan::IndexNestedLoopJoin { left, .. } => keyset_subquery_for_plan_key(left, key),
        PhysicalPlan::HashAggregate { input, .. }
        | PhysicalPlan::HashRollupAggregate { input, .. }
        | PhysicalPlan::HashGroupingSetsAggregate { input, .. }
        | PhysicalPlan::StreamingAggregate { input, .. }
        | PhysicalPlan::StreamingRollupAggregate { input, .. }
        | PhysicalPlan::StreamingGroupingSetsAggregate { input, .. }
        | PhysicalPlan::Sort { input, .. }
        | PhysicalPlan::IncrementalSort { input, .. }
        | PhysicalPlan::TopK { input, .. }
        | PhysicalPlan::TopWindow { input, .. }
        | PhysicalPlan::ParallelTopK { input, .. }
        | PhysicalPlan::Limit { input, .. }
        | PhysicalPlan::Projection { input, .. }
        | PhysicalPlan::Filter { input, .. }
        | PhysicalPlan::Apply { outer: input, .. }
        | PhysicalPlan::Materialize { input, .. }
        | PhysicalPlan::WindowAgg { input, .. }
        | PhysicalPlan::StreamingDistinct { input, .. }
        | PhysicalPlan::Exchange { input, .. } => keyset_subquery_for_plan_key(input, key),
        _ => None,
    }
}

fn unique_scan_for_key<'a>(
    plan: &'a PhysicalPlan,
    key: &Expr,
) -> Option<(&'a PlanTable, Option<&'a Expr>)> {
    fn scan_key_is_unique(table: &PlanTable, key: &Expr) -> bool {
        let Some(column) = expr_column_name(key) else {
            return false;
        };
        let normalized = normalize_expr_column(column);
        table.schema.column_index(&normalized).is_ok()
            && expr_matches_table_column(key, table, column)
            && table.schema.iter_queryable_indexes().any(|index| {
                index.unique
                    && index.columns.len() == 1
                    && normalize_expr_column(&index.columns[0]) == normalized
            })
    }

    match plan {
        PhysicalPlan::SeqScan { table, filter, .. }
        | PhysicalPlan::ZoneMapScan { table, filter, .. }
        | PhysicalPlan::IndexLookup { table, filter, .. }
        | PhysicalPlan::IndexMultiLookup { table, filter, .. }
        | PhysicalPlan::IndexRangeScan { table, filter, .. }
        | PhysicalPlan::IndexFastScan { table, filter, .. }
        | PhysicalPlan::IndexInLookup { table, filter, .. }
        | PhysicalPlan::ParallelSeqScan { table, filter, .. }
        | PhysicalPlan::ParallelZoneMapScan { table, filter, .. }
        | PhysicalPlan::ParallelIndexFastScan { table, filter, .. }
        | PhysicalPlan::ParallelIndexRangeScan { table, filter, .. }
            if scan_key_is_unique(table, key) =>
        {
            Some((table, filter.as_ref()))
        }
        PhysicalPlan::HashJoin {
            left,
            right,
            join_type: JoinType::Inner,
            left_keys,
            right_keys,
            ..
        } => {
            if let Some(source) = unique_scan_for_key(left, key) {
                let right_is_key_preserving =
                    right_keys.len() == 1 && unique_scan_for_key(right, &right_keys[0]).is_some();
                if right_is_key_preserving {
                    return Some(source);
                }
            }
            if let Some(source) = unique_scan_for_key(right, key) {
                let left_is_key_preserving =
                    left_keys.len() == 1 && unique_scan_for_key(left, &left_keys[0]).is_some();
                if left_is_key_preserving {
                    return Some(source);
                }
            }
            None
        }
        PhysicalPlan::Projection { input, .. }
        | PhysicalPlan::Sort { input, .. }
        | PhysicalPlan::IncrementalSort { input, .. }
        | PhysicalPlan::TopK { input, .. }
        | PhysicalPlan::TopWindow { input, .. }
        | PhysicalPlan::ParallelTopK { input, .. }
        | PhysicalPlan::Limit { input, .. }
        | PhysicalPlan::Exchange { input, .. } => unique_scan_for_key(input, key),
        _ => None,
    }
}

fn subquery_mentions_table(subquery: &Select, table: &PlanTable) -> bool {
    subquery
        .from
        .iter()
        .chain(subquery.joins.iter().map(|join| &join.table))
        .any(|table_ref| {
            matches_table(
                table_ref.alias.as_deref().unwrap_or_else(|| {
                    table_ref.name.rsplit('.').next().unwrap_or(&table_ref.name)
                }),
                &table.schema.name,
                table.alias.as_deref(),
            )
        })
}

fn keyset_subquery_covers_filter(subquery: &Select, filter: &Expr) -> bool {
    fn selection_contains(selection: Option<&Expr>, predicate: &Expr) -> bool {
        let Some(selection) = selection else {
            return false;
        };
        let target = crate::engine::expr_program::expr_fingerprint(predicate);
        let mut conjuncts = Vec::new();
        split_conjuncts(selection, &mut conjuncts);
        conjuncts
            .iter()
            .any(|candidate| crate::engine::expr_program::expr_fingerprint(candidate) == target)
    }

    let mut conjuncts = Vec::new();
    split_conjuncts(filter, &mut conjuncts);
    conjuncts.into_iter().all(|conjunct| match conjunct {
        Expr::InSubquery {
            subquery: nested,
            negated: false,
            ..
        } => {
            let mut nested_conjuncts = Vec::new();
            if let Some(selection) = nested.selection.as_ref() {
                split_conjuncts(selection, &mut nested_conjuncts);
            }
            !nested_conjuncts.is_empty()
                && nested_conjuncts
                    .iter()
                    .all(|predicate| selection_contains(subquery.selection.as_ref(), predicate))
        }
        other => selection_contains(subquery.selection.as_ref(), &other),
    })
}

#[derive(Clone, Copy, Debug)]
struct FilteredKeyDomainEstimate {
    key_distinct: f64,
    min: f64,
    max: f64,
    density: f64,
}

fn filtered_key_domain_in_plan(
    plan: &PhysicalPlan,
    key_expr: &Expr,
) -> Option<FilteredKeyDomainEstimate> {
    let key_col = normalize_expr_column(expr_column_name(key_expr)?);
    fn walk(plan: &PhysicalPlan, key_col: &str) -> Option<FilteredKeyDomainEstimate> {
        if let Some((schema, filter, estimated_rows)) = base_scan_schema_and_filter(plan) {
            if schema.column_index(key_col).is_ok() {
                if let Some(filter) = filter.as_ref() {
                    if let Some(domain) =
                        filtered_key_domain_for_schema(schema, filter, key_col, estimated_rows)
                    {
                        return Some(domain);
                    }
                }
            }
        }
        match plan {
            PhysicalPlan::HashJoin { left, right, .. }
            | PhysicalPlan::NestedLoopJoin { left, right, .. }
            | PhysicalPlan::AdaptiveJoin { left, right, .. } => {
                walk(left, key_col).or_else(|| walk(right, key_col))
            }
            PhysicalPlan::IndexNestedLoopJoin { left, .. } => walk(left, key_col),
            PhysicalPlan::HashAggregate { input, .. }
            | PhysicalPlan::HashRollupAggregate { input, .. }
            | PhysicalPlan::HashGroupingSetsAggregate { input, .. }
            | PhysicalPlan::StreamingAggregate { input, .. }
            | PhysicalPlan::StreamingRollupAggregate { input, .. }
            | PhysicalPlan::StreamingGroupingSetsAggregate { input, .. }
            | PhysicalPlan::Sort { input, .. }
            | PhysicalPlan::TopK { input, .. }
            | PhysicalPlan::ParallelTopK { input, .. }
            | PhysicalPlan::Limit { input, .. }
            | PhysicalPlan::Projection { input, .. }
            | PhysicalPlan::WindowAgg { input, .. }
            | PhysicalPlan::StreamingDistinct { input, .. }
            | PhysicalPlan::Exchange { input, .. } => walk(input, key_col),
            _ => None,
        }
    }
    walk(plan, &key_col)
}

fn filtered_key_domain_for_schema(
    schema: &TableSchema,
    filter: &Expr,
    key_col: &str,
    estimated_rows: f64,
) -> Option<FilteredKeyDomainEstimate> {
    // A lexical or fractional domain must never be treated as a dense
    // sequence of integers just because its endpoints coerce to a number.
    if !schema
        .columns
        .get(schema.column_index(key_col).ok()?)?
        .data_type
        .is_integer()
    {
        return None;
    }
    let unique_key = schema.iter_queryable_indexes().any(|index| {
        index.unique
            && index.columns.len() == 1
            && normalize_expr_column(&index.columns[0]) == key_col
    });
    let mut best: Option<(usize, FilteredKeyDomainEstimate)> = None;
    for domain in &schema.stats.conditional_key_domains {
        if domain.key_columns.len() != 1 || normalize_expr_column(&domain.key_columns[0]) != key_col
        {
            continue;
        }
        let mut min = f64::INFINITY;
        let mut max = f64::NEG_INFINITY;
        let mut key_distinct = 0.0;
        let mut matched = 0usize;
        for entry in &domain.entries {
            if entry.filter_values.len() != domain.filter_columns.len()
                || !domain
                    .filter_columns
                    .iter()
                    .zip(&entry.filter_values)
                    .all(|(column, value)| {
                        let collation = schema
                            .columns
                            .iter()
                            .find(|c| c.name.eq_ignore_ascii_case(column))
                            .and_then(|c| c.collation.as_deref())
                            .map(crate::charset::collation_id);
                        crate::engine::expr::with_compare_collation(collation, || {
                            constant_domain_predicate_matches(
                                filter,
                                &normalize_expr_column(column),
                                value,
                            )
                        }) == Some(true)
                    })
            {
                continue;
            }
            let (Some(entry_min), Some(entry_max)) =
                (entry.key_min.as_ref(), entry.key_max.as_ref())
            else {
                continue;
            };
            let (Some(entry_min), Some(entry_max)) = (
                dense_integer_value(entry_min),
                dense_integer_value(entry_max),
            ) else {
                continue;
            };
            if !entry_min.is_finite() || !entry_max.is_finite() || entry_max < entry_min {
                continue;
            }
            let sampled = entry.key_distinct_count.max(1) as f64;
            let integer_span = (entry_max.floor() - entry_min.ceil() + 1.0).floor();
            let distinct = if integer_span.is_finite() && integer_span >= 1.0 {
                sampled.max(integer_span.min(entry.row_count.max(1) as f64))
            } else {
                sampled
            };
            min = min.min(entry_min);
            max = max.max(entry_max);
            key_distinct += distinct;
            matched += 1;
        }
        // Conditional entries have disjoint filter values, but only a unique
        // key proves their key sets disjoint. Do not add overlapping NDVs.
        if matched == 0 || (matched > 1 && !unique_key) {
            continue;
        }
        if unique_key {
            key_distinct = key_distinct.max(estimated_rows.max(1.0));
        }
        let span = (max - min + 1.0).max(1.0);
        key_distinct = key_distinct.min(span);
        let estimate = FilteredKeyDomainEstimate {
            key_distinct,
            min,
            max,
            density: (key_distinct / span).clamp(0.0, 1.0),
        };
        let prefix_len = domain.filter_columns.len();
        if best
            .as_ref()
            .is_none_or(|(best_prefix_len, _)| prefix_len > *best_prefix_len)
        {
            best = Some((prefix_len, estimate));
        }
    }
    best.map(|(_, estimate)| estimate)
}

/// Match a statistics entry against constant constraints on one column.
/// None means no usable restriction, so incomplete OR branches and dynamic
/// expressions cannot manufacture a selective domain. AND may retain an
/// independently known restriction, which is a conservative superset.
fn constant_domain_predicate_matches(expr: &Expr, column: &str, value: &Value) -> Option<bool> {
    let is_column = |expr: &Expr| {
        expr_column_name(expr).is_some_and(|name| normalize_expr_column(name) == column)
    };
    let compare = |op: BinaryOp, bound: &Expr| -> Option<bool> {
        let bound = expr_literal_value(bound)?;
        if matches!(value, Value::Null) || matches!(bound, Value::Null) {
            return Some(false);
        }
        let order = crate::engine::expr::compare_values(value, &bound).ok()?;
        Some(match op {
            BinaryOp::Eq => order == std::cmp::Ordering::Equal,
            BinaryOp::Lt => order == std::cmp::Ordering::Less,
            BinaryOp::LtEq => order != std::cmp::Ordering::Greater,
            BinaryOp::Gt => order == std::cmp::Ordering::Greater,
            BinaryOp::GtEq => order != std::cmp::Ordering::Less,
            _ => return None,
        })
    };
    match expr {
        Expr::Binary {
            left,
            op: BinaryOp::And,
            right,
        } => {
            match (
                constant_domain_predicate_matches(left, column, value),
                constant_domain_predicate_matches(right, column, value),
            ) {
                (Some(a), Some(b)) => Some(a && b),
                (a, b) => a.or(b),
            }
        }
        Expr::Binary {
            left,
            op: BinaryOp::Or,
            right,
        } => Some(
            constant_domain_predicate_matches(left, column, value)?
                | constant_domain_predicate_matches(right, column, value)?,
        ),
        Expr::Binary { left, op, right } if is_column(left) => compare(*op, right),
        Expr::Binary { left, op, right } if is_column(right) => compare(flip_cmp(*op), left),
        Expr::InList {
            expr,
            list,
            negated: false,
        } if is_column(expr) => {
            let mut found = false;
            for bound in list {
                found |= compare(BinaryOp::Eq, bound)?;
            }
            Some(found)
        }
        Expr::Between {
            expr,
            low,
            high,
            negated: false,
        } if is_column(expr) => {
            Some(compare(BinaryOp::GtEq, low)? && compare(BinaryOp::LtEq, high)?)
        }
        _ => None,
    }
}

fn base_scan_schema_and_filter(plan: &PhysicalPlan) -> Option<(&TableSchema, Option<&Expr>, f64)> {
    match plan {
        PhysicalPlan::SeqScan {
            table,
            filter,
            estimated_rows,
            ..
        }
        | PhysicalPlan::ZoneMapScan {
            table,
            filter,
            estimated_rows,
            ..
        }
        | PhysicalPlan::IndexLookup {
            table,
            filter,
            estimated_rows,
            ..
        }
        | PhysicalPlan::IndexMultiLookup {
            table,
            filter,
            estimated_rows,
            ..
        }
        | PhysicalPlan::IndexMerge {
            table,
            filter,
            estimated_rows,
            ..
        }
        | PhysicalPlan::IndexRangeScan {
            table,
            filter,
            estimated_rows,
            ..
        }
        | PhysicalPlan::IndexFastScan {
            table,
            filter,
            estimated_rows,
            ..
        }
        | PhysicalPlan::IndexInLookup {
            table,
            filter,
            estimated_rows,
            ..
        }
        | PhysicalPlan::ParallelSeqScan {
            table,
            filter,
            estimated_rows,
            ..
        }
        | PhysicalPlan::ParallelZoneMapScan {
            table,
            filter,
            estimated_rows,
            ..
        }
        | PhysicalPlan::ParallelIndexFastScan {
            table,
            filter,
            estimated_rows,
            ..
        }
        | PhysicalPlan::ParallelIndexRangeScan {
            table,
            filter,
            estimated_rows,
            ..
        } => Some((table.schema.as_ref(), filter.as_ref(), *estimated_rows)),
        _ => None,
    }
}

fn key_numeric_min_max_in_plan(plan: &PhysicalPlan, col: &str) -> Option<(f64, f64)> {
    fn walk(plan: &PhysicalPlan, col: &str) -> Option<(f64, f64)> {
        if let Some(schema) = plan_table_stats(plan) {
            if let Some(bounds) = dense_integer_column_bounds(schema, col) {
                return Some(bounds);
            }
        }
        match plan {
            PhysicalPlan::HashJoin { left, right, .. }
            | PhysicalPlan::NestedLoopJoin { left, right, .. }
            | PhysicalPlan::AdaptiveJoin { left, right, .. } => {
                walk(left, col).or_else(|| walk(right, col))
            }
            PhysicalPlan::IndexNestedLoopJoin { left, .. } => walk(left, col),
            PhysicalPlan::HashAggregate { input, .. }
            | PhysicalPlan::HashRollupAggregate { input, .. }
            | PhysicalPlan::HashGroupingSetsAggregate { input, .. }
            | PhysicalPlan::StreamingAggregate { input, .. }
            | PhysicalPlan::StreamingRollupAggregate { input, .. }
            | PhysicalPlan::StreamingGroupingSetsAggregate { input, .. }
            | PhysicalPlan::Sort { input, .. }
            | PhysicalPlan::TopK { input, .. }
            | PhysicalPlan::ParallelTopK { input, .. }
            | PhysicalPlan::Limit { input, .. }
            | PhysicalPlan::Projection { input, .. }
            | PhysicalPlan::WindowAgg { input, .. }
            | PhysicalPlan::StreamingDistinct { input, .. }
            | PhysicalPlan::Exchange { input, .. } => walk(input, col),
            _ => None,
        }
    }
    walk(plan, col)
}

/// Return the joint NDV (`prefix_distinct_counts[len-1]`) of any
/// composite index on a base scan inside `plan` whose leading columns
/// match `keys` in order. Falls back to `None` when no such index
/// exists.
fn composite_ndv_in_plan(plan: &PhysicalPlan, keys: &[Expr]) -> Option<u64> {
    if keys.iter().any(|key| expr_column_name(key).is_none()) {
        return None;
    }
    let tables = collect_physical_plan_tables(plan);
    for table in &tables {
        for index in table.schema.iter_queryable_indexes().filter(|i| {
            !i.is_fulltext() && !i.is_spatial() && !i.is_vector() && !i.has_multi_valued_parts()
        }) {
            if index.columns.len() < keys.len()
                || !keys.iter().zip(index.columns.iter()).all(|(key, column)| {
                    plan_key_equivalent_to_table_column(plan, &tables, key, table, column)
                })
            {
                continue;
            }
            if let Some(joint) = table
                .schema
                .stats
                .indexes
                .get(&normalize_expr_column(&index.name))
                .and_then(|stats| stats.prefix_distinct_counts.get(keys.len() - 1).copied())
                .filter(|&ndv| ndv > 0)
            {
                return Some(joint);
            }
        }
    }
    None
}

/// Like [`composite_ndv_in_plan`], but consults the order-independent
/// `multi_column_stats` collected at ANALYZE time (composite-index column
/// *sets* + foreign-key tuples). Fires when the join keys form a collected
/// group regardless of column order, or for an FK group lacking a covering
/// composite index — letting the joint-NDV join path replace the
/// sqrt-damping independence guess in those cases.
fn multi_column_ndv_in_plan(plan: &PhysicalPlan, keys: &[Expr]) -> Option<u64> {
    if keys.len() < 2 || keys.iter().any(|key| expr_column_name(key).is_none()) {
        return None;
    }
    let tables = collect_physical_plan_tables(plan);
    for table in &tables {
        for group in table.schema.stats.multi_column_stats.values() {
            if group.columns.len() != keys.len() || group.distinct_count == 0 {
                continue;
            }
            let mut used = vec![false; group.columns.len()];
            let matched = keys.iter().all(|key| {
                group.columns.iter().enumerate().any(|(idx, column)| {
                    if used[idx]
                        || !plan_key_equivalent_to_table_column(plan, &tables, key, table, column)
                    {
                        return false;
                    }
                    used[idx] = true;
                    true
                })
            });
            if matched {
                return Some(group.distinct_count);
            }
        }
    }
    None
}

/// Estimate an uncollected tuple NDV only when all keys resolve to one
/// base relation. Independence can never produce more distinct tuples than
/// that relation has rows. Use base rows, not filtered/output rows: an
/// unrelated filter does not prove that the join-key universe shrank.
fn uncollected_joint_ndv_in_plan(plan: &PhysicalPlan, keys: &[Expr]) -> Option<u64> {
    let tables = collect_physical_plan_tables(plan);
    tables.iter().find_map(|table| {
        let rows = table.schema.stats.row_count;
        if rows == 0 {
            return None;
        }
        let mut product = 1_u64;
        for key in keys {
            let column = table.schema.columns.iter().find(|column| {
                plan_key_equivalent_to_table_column(plan, &tables, key, table, &column.name)
            })?;
            let ndv = ndv_for_column(&table.schema, &column.name)?;
            product = product.saturating_mul(ndv).min(rows);
        }
        Some(product)
    })
}

/// Match a join key to a concrete base-table column through equalities that
/// are already enforced inside `plan`. This is needed for joint statistics on
/// an embedded INLJ right side: after `part.p_partkey = partsupp.ps_partkey`,
/// the output key pair `(p_partkey, ps_suppkey)` is the same statistical
/// domain as the persisted `(ps_partkey, ps_suppkey)` group.
///
/// The unqualified fallback is allowed only when the column name belongs to a
/// single table in the subtree. That keeps equivalence inference conservative
/// for joins containing same-named columns without aliases.
fn plan_key_equivalent_to_table_column(
    plan: &PhysicalPlan,
    tables: &[PlanTable],
    key: &Expr,
    table: &PlanTable,
    column: &str,
) -> bool {
    if expr_matches_table_column(key, table, column) {
        return true;
    }
    let mut candidates = Vec::with_capacity(3);
    if let Some(alias) = table.alias.as_ref() {
        candidates.push(Expr::CompoundIdentifier(vec![
            alias.clone(),
            column.to_string(),
        ]));
    }
    let qualifier = table
        .schema
        .name
        .rsplit('.')
        .next()
        .unwrap_or(&table.schema.name);
    candidates.push(Expr::CompoundIdentifier(vec![
        qualifier.to_string(),
        column.to_string(),
    ]));
    if tables
        .iter()
        .filter(|candidate| table_has_column(candidate, column))
        .count()
        == 1
    {
        candidates.push(Expr::Identifier(column.to_string()));
    }
    candidates
        .iter()
        .any(|candidate| exprs_equivalent_in_plan(plan, key, candidate))
}

/// Recursively search the physical plan tree for the schema of a base
/// scan that exposes column `col`. This lets `estimate_equijoin_rows`
/// find NDV statistics even when the join's left/right is itself a
/// nested join (in which case `plan_table_stats` returns None).
fn ndv_in_plan(plan: &PhysicalPlan, col: &str) -> Option<u64> {
    fn walk(plan: &PhysicalPlan, col: &str) -> Option<u64> {
        if let Some(schema) = plan_table_stats(plan) {
            if let Some(ndv) = ndv_for_column(schema, col) {
                return Some(ndv);
            }
        }
        plan.children().iter().find_map(|child| walk(child, col))
    }
    walk(plan, col)
}

fn average_column_width_in_plan(plan: &PhysicalPlan, col: &str) -> Option<f64> {
    fn walk(plan: &PhysicalPlan, col: &str) -> Option<f64> {
        if let Some(schema) = plan_table_stats(plan) {
            let normalized = normalize_expr_column(col);
            if let Some(column) = schema
                .columns
                .iter()
                .find(|column| column.name.eq_ignore_ascii_case(&normalized))
            {
                let fallback_width = match column.data_type {
                    crate::storage::DataTypeKind::Char { .. }
                    | crate::storage::DataTypeKind::VarChar { .. }
                    | crate::storage::DataTypeKind::Text { .. }
                    | crate::storage::DataTypeKind::Binary { .. }
                    | crate::storage::DataTypeKind::VarBinary { .. }
                    | crate::storage::DataTypeKind::Blob { .. } => 64.0,
                    _ => column.data_type.key_byte_length() as f64,
                };
                return Some(
                    schema
                        .stats
                        .columns
                        .get(&normalized)
                        .and_then(|stats| stats.average_width_bytes)
                        .unwrap_or(fallback_width),
                );
            }
        }
        plan.children().iter().find_map(|child| walk(child, col))
    }
    walk(plan, col)
}

/// Effective NDV of one equijoin key after scan-side filtering.
///
/// `ndv_in_plan` intentionally returns the base-column universe because
/// semi/anti existence costing needs that denominator. Equijoin cardinality
/// needs the opposite: a filtered input cannot expose more distinct values
/// than rows, and an `IndexInLookup` exposes only the fraction of its key
/// domain visited by the subquery. Keeping the two estimators separate avoids
/// changing semi/anti semantics while preventing SIP filters from being
/// charged a second time by a later explicit dimension join.
fn equijoin_ndv_in_plan(plan: &PhysicalPlan, key: &Expr) -> Option<f64> {
    fn predicate_only_restricts_key(expr: &Expr, table: &PlanTable, column: &str) -> bool {
        match expr {
            Expr::Binary {
                left,
                op: BinaryOp::And,
                right,
            } => {
                predicate_only_restricts_key(left, table, column)
                    && predicate_only_restricts_key(right, table, column)
            }
            Expr::Binary { left, right, .. } => {
                (expr_matches_table_column(left, table, column)
                    && expr_literal_value(right).is_some())
                    || (expr_matches_table_column(right, table, column)
                        && expr_literal_value(left).is_some())
            }
            Expr::InSubquery {
                expr,
                negated: false,
                ..
            }
            | Expr::InList {
                expr,
                negated: false,
                ..
            }
            | Expr::Between {
                expr,
                negated: false,
                ..
            } => expr_matches_table_column(expr, table, column),
            Expr::IsNull(expr) | Expr::IsNotNull(expr) => {
                expr_matches_table_column(expr, table, column)
            }
            _ => false,
        }
    }

    fn schema_domain_cap(schema: &TableSchema, column: &str, raw: f64) -> f64 {
        numeric_domain_cardinality_cap(schema, column).map_or(raw, |cap| raw.min(cap as f64))
    }

    fn leaf_ndv(
        table: &PlanTable,
        key: &Expr,
        estimated_rows: f64,
        filter: Option<&Expr>,
        in_lookup_access_rows: Option<(&str, f64)>,
    ) -> Option<f64> {
        let column = expr_column_name(key)?;
        if !expr_matches_table_column(key, table, column) {
            return None;
        }
        let raw = ndv_for_column(&table.schema, column)? as f64;
        let mut effective = schema_domain_cap(&table.schema, column, raw);
        if filter.is_some_and(|predicate| predicate_only_restricts_key(predicate, table, column)) {
            let table_rows = table.schema.stats.row_count.max(1) as f64;
            effective = effective.min(raw * (estimated_rows / table_rows).clamp(0.0, 1.0));
        }
        if let Some((in_column, access_rows)) = in_lookup_access_rows {
            if normalize_expr_column(column) == normalize_expr_column(in_column) {
                let table_rows = table.schema.stats.row_count.max(1) as f64;
                effective = effective.min(raw * (access_rows / table_rows).clamp(0.0, 1.0));
            }
        }
        Some(effective.max(1.0))
    }

    fn walk(plan: &PhysicalPlan, key: &Expr) -> Option<f64> {
        match plan {
            PhysicalPlan::IndexInLookup {
                table,
                in_column,
                estimated_rows,
                access_rows,
                ..
            } => leaf_ndv(
                table,
                key,
                *estimated_rows,
                None,
                Some((in_column, *access_rows)),
            ),
            PhysicalPlan::SeqScan {
                table,
                filter,
                estimated_rows,
                ..
            }
            | PhysicalPlan::ZoneMapScan {
                table,
                filter,
                estimated_rows,
                ..
            }
            | PhysicalPlan::IndexLookup {
                table,
                filter,
                estimated_rows,
                ..
            }
            | PhysicalPlan::IndexMultiLookup {
                table,
                filter,
                estimated_rows,
                ..
            }
            | PhysicalPlan::IndexRangeScan {
                table,
                filter,
                estimated_rows,
                ..
            }
            | PhysicalPlan::IndexFastScan {
                table,
                filter,
                estimated_rows,
                ..
            }
            | PhysicalPlan::ParallelSeqScan {
                table,
                filter,
                estimated_rows,
                ..
            }
            | PhysicalPlan::ParallelZoneMapScan {
                table,
                filter,
                estimated_rows,
                ..
            }
            | PhysicalPlan::ParallelIndexFastScan {
                table,
                filter,
                estimated_rows,
                ..
            }
            | PhysicalPlan::ParallelIndexRangeScan {
                table,
                filter,
                estimated_rows,
                ..
            } => leaf_ndv(table, key, *estimated_rows, filter.as_ref(), None),
            PhysicalPlan::HashJoin { left, right, .. }
            | PhysicalPlan::NestedLoopJoin { left, right, .. }
            | PhysicalPlan::AdaptiveJoin { left, right, .. } => {
                walk(left, key).or_else(|| walk(right, key))
            }
            PhysicalPlan::IndexNestedLoopJoin {
                left,
                right_table,
                estimated_rows,
                ..
            } => {
                walk(left, key).or_else(|| leaf_ndv(right_table, key, *estimated_rows, None, None))
            }
            PhysicalPlan::HashAggregate { input, .. }
            | PhysicalPlan::HashRollupAggregate { input, .. }
            | PhysicalPlan::HashGroupingSetsAggregate { input, .. }
            | PhysicalPlan::StreamingAggregate { input, .. }
            | PhysicalPlan::StreamingRollupAggregate { input, .. }
            | PhysicalPlan::StreamingGroupingSetsAggregate { input, .. }
            | PhysicalPlan::Sort { input, .. }
            | PhysicalPlan::IncrementalSort { input, .. }
            | PhysicalPlan::TopK { input, .. }
            | PhysicalPlan::TopWindow { input, .. }
            | PhysicalPlan::ParallelTopK { input, .. }
            | PhysicalPlan::Limit { input, .. }
            | PhysicalPlan::Projection { input, .. }
            | PhysicalPlan::Filter { input, .. }
            | PhysicalPlan::Apply { outer: input, .. }
            | PhysicalPlan::Materialize { input, .. }
            | PhysicalPlan::WindowAgg { input, .. }
            | PhysicalPlan::StreamingDistinct { input, .. }
            | PhysicalPlan::Exchange { input, .. } => walk(input, key),
            _ => None,
        }
    }

    walk(plan, key)
}

fn cbo_nlj_outer_parallel_discount_disabled() -> bool {
    matches!(
        crate::util::env::var("MYDB_CBO_NLJ_OUTER_PARALLEL_DISCOUNT")
            .unwrap_or_default()
            .to_ascii_lowercase()
            .as_str(),
        "off" | "0" | "false" | "no" | "disable" | "disabled"
    )
}

/// Return true iff every column reference inside `expr` resolves to a
/// column of `table` (i.e. the expression is right-table-only). Function
/// calls, subqueries and other unsupported nodes return `false`
/// conservatively. Used by INLJ row-estimate residual handling.
fn expr_refs_only_table(expr: &Expr, table: &PlanTable) -> bool {
    crate::parser::visit::try_inspect_value_expr(expr, |expr| {
        let valid = match expr {
            Expr::Identifier(name) => table.schema.column_index(name).is_ok(),
            Expr::CompoundIdentifier(parts) => parts.last().is_some_and(|column| {
                (parts.len() < 2
                    || matches_table(
                        &parts[parts.len() - 2],
                        &table.schema.name,
                        table.alias.as_deref(),
                    ))
                    && table.schema.column_index(column).is_ok()
            }),
            // These nodes can reach another scope or depend on session state.
            Expr::Subquery(_)
            | Expr::Exists { .. }
            | Expr::InSubquery { .. }
            | Expr::QuantifiedComparison { .. }
            | Expr::WindowFunction { .. }
            | Expr::Wildcard
            | Expr::UserVariable(_)
            | Expr::SystemVariable { .. } => false,
            _ => true,
        };
        if valid {
            Ok(true)
        } else {
            Err(())
        }
    })
    .is_ok()
}

/// Scale the INLJ output-row estimate by the selectivity of its
/// right-only residual conjuncts (post-probe filter). Mirrors what the
/// cost model already gets for free on the hash-join side (the scan
/// filter sits *below* the join and shrinks the input rows the join
/// sees). Without this, INLJ output is `left_rows * per_probe_match`
/// regardless of the post-fetch filter, which systematically overstates
/// INLJ output and inflates downstream agg/sort costs.
///
/// Considers both right-only conjuncts inside `condition` (the explicit
/// join condition) AND any filter already attached to the right-side
/// scan (e.g. an `l_shipdate > ...` pushed down to `SeqScan { filter }`):
/// when the planner forces INLJ that filter does not become a separate
/// SeqScan at runtime — it becomes the INLJ's residual.
fn apply_inlj_right_only_residual_selectivity(
    base_est: f64,
    condition: Option<&Expr>,
    left_plan: &PhysicalPlan,
    right_plan: &PhysicalPlan,
    right_table: &PlanTable,
    join_type: JoinType,
    left_rows: f64,
) -> f64 {
    let Some(combined) = collect_inlj_right_only_residual(condition, right_plan, right_table)
    else {
        return base_est;
    };
    // A SIP keyset may already have restricted the outer rows to keys selected
    // from this same unique inner relation.  Reapplying that relation's
    // right-only predicate after a unique lookup is a correctness recheck, not
    // another cardinality reduction.  Hash-join costing has the equivalent
    // proof in `keyset_prefilter_guarantees_unique_join`; mirror it for INLJ
    // so the physical algorithm does not change the logical row estimate.
    if matches!(join_type, JoinType::Inner | JoinType::Left)
        && keyset_prefilter_guarantees_unique_inlj(left_plan, right_table, condition, &combined)
    {
        return base_est;
    }
    let total = (right_table.schema.stats.row_count.max(1) as f64).max(1.0);
    use crate::engine::cbo::selectivity::Selectivity;
    let kept = crate::engine::cbo::selectivity::default_selectivity().predicate_rows(
        &right_table.schema,
        &combined,
        total,
    );
    let selectivity = (kept / total).clamp(0.0, 1.0);
    match join_type {
        // Anti: more inner rows failing the residual ⇒ fewer keys match
        // ⇒ more outer rows survive the anti. base_est already encodes
        // matches_no_residual = left_rows - base_est; scale that count
        // by residual selectivity and re-derive anti rows.
        JoinType::Anti => {
            let matches_no_residual = (left_rows - base_est).max(0.0);
            (left_rows - matches_no_residual * selectivity).max(1.0)
        }
        // Semi: base_est here = matches; multiply by residual selectivity.
        // This is the linear approximation (true probability that no
        // inner row passes is `(1-sel)^N`); good enough vs the bigger
        // gains from Fix 4's NDV-based base estimate.
        JoinType::Semi => (base_est * selectivity).max(1.0),
        // LEFT: the right-only residual filters *matches*, but every outer
        // row that loses its match is still emitted NULL-extended, so the
        // residual can never pull the output below the preserved side.
        // Re-apply the `left_rows` floor that `estimate_index_nlj_rows`
        // established, since multiplying by selectivity (<1) would breach it.
        JoinType::Left => (base_est * selectivity).max(left_rows),
        // INLJ is only built for Inner/Left/Semi/Anti (Right/Full never
        // reach here); Inner keeps the plain residual-scaled estimate.
        _ => (base_est * selectivity).max(1.0),
    }
}

fn keyset_prefilter_guarantees_unique_inlj(
    prefiltered_plan: &PhysicalPlan,
    unique_table: &PlanTable,
    condition: Option<&Expr>,
    right_filter: &Expr,
) -> bool {
    let Some(condition) = condition else {
        return false;
    };
    unique_table.schema.iter_queryable_indexes().any(|index| {
        if !index.unique || index.columns.len() != 1 {
            return false;
        }
        let unique_column = &index.columns[0];
        let outer_key = expr_conjuncts(condition).into_iter().find_map(|conjunct| {
            equality_between_table_column_and_left_plan(
                conjunct,
                unique_table,
                unique_column,
                prefiltered_plan,
            )
        });
        let Some(outer_key) = outer_key else {
            return false;
        };
        let Some(subquery) = keyset_subquery_for_plan_key(prefiltered_plan, &outer_key) else {
            return false;
        };
        let projects_unique_key = matches!(
            subquery.projection.as_slice(),
            [SelectItem::Expr { expr, .. }]
                if expr_matches_table_column(expr, unique_table, unique_column)
        );
        projects_unique_key
            && subquery_mentions_table(subquery, unique_table)
            && keyset_subquery_covers_filter(subquery, right_filter)
    })
}

/// Combine the right-only conjuncts from the explicit join `condition`
/// and any filter attached to the right-side scan into a single Expr.
/// Returns None if no residual exists. Shared between selectivity
/// estimation and per-row CPU cost computation for batched INL.
fn collect_inlj_right_only_residual(
    condition: Option<&Expr>,
    right_plan: &PhysicalPlan,
    right_table: &PlanTable,
) -> Option<Expr> {
    let mut residuals: Vec<Expr> = Vec::new();
    if let Some(cond) = condition {
        residuals.extend(
            expr_conjuncts(cond)
                .into_iter()
                .filter(|conj| expr_refs_only_table(conj, right_table))
                .cloned(),
        );
    }
    if let Some(scan_filter) = extract_scan_filter(right_plan) {
        residuals.extend(
            expr_conjuncts(&scan_filter)
                .into_iter()
                .filter(|conj| expr_refs_only_table(conj, right_table))
                .cloned(),
        );
    }
    combine_conjuncts(residuals)
}

/// Price the trailing outer-dependent range the executor actually probes.
/// Equality-prefix cardinality alone charges the full suffix even when the
/// lookup has lower/upper bounds. Constant-only ranges are deliberately left
/// to residual costing: the executor can promote them to batched equality
/// lookups, which still visit the complete prefix.
fn index_nlj_dependent_range_fraction(
    left: &PhysicalPlan,
    table: &PlanTable,
    index: &IndexSchema,
    eq_prefix_len: usize,
    condition: Option<&Expr>,
) -> f64 {
    let Some(condition) = condition else {
        return 1.0;
    };
    if index.key_part(eq_prefix_len).is_none()
        || !expr_conjuncts(condition).into_iter().any(|expr| {
            matches!(
                expr,
                Expr::Binary {
                    op: BinaryOp::Gt | BinaryOp::GtEq | BinaryOp::Lt | BinaryOp::LtEq,
                    ..
                }
            )
        })
    {
        return 1.0;
    }
    // Do not discount a prefix that requires cross-domain key coercion.
    // Such comparisons can need a broad residual fallback at execution time.
    let outer_tables = collect_physical_plan_tables(left);
    let outer_target = |expr: &Expr| {
        let mut targets = outer_tables.iter().flat_map(|t| {
            t.schema.columns.iter().filter_map(move |c| {
                expr_matches_table_column(expr, t, &c.name).then(|| {
                    crate::storage::ParamTarget::for_column_with_collation(
                        &c.data_type,
                        c.unsigned,
                        c.collation.as_deref(),
                    )
                })
            })
        });
        let first = targets.next()?;
        targets.next().is_none().then_some(first)
    };
    let index_target = |pos| match index.key_part(pos) {
        Some(crate::storage::IndexKeyPart::Column(name)) => {
            table.schema.column_index(name).ok().map(|i| {
                let c = &table.schema.columns[i];
                crate::storage::ParamTarget::for_column_with_collation(
                    &c.data_type,
                    c.unsigned,
                    c.collation.as_deref(),
                )
            })
        }
        Some(crate::storage::IndexKeyPart::Expression(expr)) => Some(
            crate::storage::ParamTarget::for_column(&expr.data_type, false),
        ),
        None => None,
    };
    for pos in 0..eq_prefix_len {
        let value = expr_conjuncts(condition).into_iter().find_map(|expr| {
            equality_between_index_part_and_left_plan(expr, table, index, pos, left)
        });
        if value
            .as_ref()
            .and_then(outer_target)
            .zip(index_target(pos))
            .is_none_or(|(a, b)| a != b)
        {
            return 1.0;
        }
    }
    let has_dependent_range = expr_conjuncts(condition).into_iter().any(|expr| {
        let bound = match index.key_part(eq_prefix_len) {
            Some(crate::storage::IndexKeyPart::Column(column)) => {
                range_on_table_column_with_left_expr(expr, table, column, left)
            }
            Some(crate::storage::IndexKeyPart::Expression(_)) => {
                range_on_index_part_with_left_expr(expr, table, index, eq_prefix_len, left)
            }
            None => None,
        };
        bound.is_some_and(|(_, value)| {
            !crate::engine::expr::expr_is_constant(&value)
                && outer_target(&value)
                    .zip(index_target(eq_prefix_len))
                    .is_some_and(|(a, b)| a == b)
        })
    });
    if has_dependent_range {
        crate::engine::cbo::cbo_consts()
            .default_range_selectivity()
            .clamp(0.001, 1.0)
    } else {
        1.0
    }
}

fn dense_filtered_domain_rows_for_inlj(
    left: &PhysicalPlan,
    right_table: &PlanTable,
    right_index: &IndexSchema,
    condition: Option<&Expr>,
    left_rows: f64,
) -> Option<f64> {
    const MIN_DENSE_DOMAIN_DENSITY: f64 = 0.50;
    if left_rows <= 0.0 {
        return None;
    }
    let inner_col = right_index.columns.first()?;
    let outer_expr = condition.and_then(|cond| {
        expr_conjuncts(cond).into_iter().find_map(|conj| {
            equality_between_table_column_and_left_plan(conj, right_table, inner_col, left)
        })
    })?;
    let domain = filtered_key_domain_in_plan(left, &outer_expr)?;
    if domain.density < MIN_DENSE_DOMAIN_DENSITY {
        return None;
    }
    let raw_other_ndv = ndv_for_column(&right_table.schema, inner_col)? as f64;
    if raw_other_ndv <= 0.0 {
        return None;
    }
    let (other_min, other_max) = dense_integer_column_bounds(&right_table.schema, inner_col)?;
    let other_value_span = (other_max.floor() - other_min.ceil() + 1.0).floor();
    let other_ndv = if other_value_span.is_finite() && other_value_span >= 1.0 {
        raw_other_ndv.min(other_value_span)
    } else {
        raw_other_ndv
    };
    let domain_span = (domain.max - domain.min + 1.0).max(1.0);
    let overlap_low = domain.min.max(other_min);
    let overlap_high = domain.max.min(other_max);
    if overlap_high < overlap_low {
        return Some(1.0);
    }
    let overlap_span = (overlap_high - overlap_low + 1.0).clamp(1.0, domain_span);
    let selected_keys =
        (domain.key_distinct * (overlap_span / domain_span)).clamp(1.0, other_ndv.max(1.0));
    let right_rows = right_table.schema.stats.row_count.max(1) as f64;
    Some((right_rows * (selected_keys / other_ndv)).clamp(1.0, right_rows.max(1.0)))
}

/// Clamp an inner-index per-probe match estimate with the *outer* join
/// key's NDV. The textbook equi-join estimator divides by
/// `max(ndv_outer, ndv_inner)`; `estimate_index_lookup_rows` only sees
/// the inner index's sampled prefix cardinality, which page-based
/// ANALYZE systematically under-estimates for uniform FK columns (the
/// KMV sketch runs over page-correlated samples). The outer side often
/// carries an exact NDV (e.g. a PRIMARY KEY), so taking the max keeps
/// the INLJ cardinality consistent with `estimate_equijoin_rows` — the
/// same logical join must not change cardinality with the physical
/// operator. (TPC-H part⋈lineitem sf=1: sampled `l_partkey` NDV 84.5k
/// vs true 200k inflated the INLJ matched-rows estimate 2.4× over the
/// hash-join estimate of the identical join, which cascaded through
/// heap I/O, the large-secondary penalty factor and the row-id sort
/// CPU term.)
///
/// Only the leading index column's outer counterpart is consulted: for
/// a multi-column prefix the outer joint NDV is ≥ the leading column's
/// NDV, so the clamp stays conservative (never under-estimates the
/// per-probe row count).
fn clamp_index_lookup_rows_with_outer_ndv(
    per_match: f64,
    left: &PhysicalPlan,
    right_table: &PlanTable,
    right_index: &IndexSchema,
    condition: Option<&Expr>,
    right_total_rows: f64,
) -> f64 {
    if per_match <= 1.0 {
        return per_match;
    }
    let Some(inner_col) = right_index.columns.first() else {
        return per_match;
    };
    let outer_col = condition.and_then(|cond| {
        expr_conjuncts(cond).into_iter().find_map(|conj| {
            equality_between_table_column_and_left_plan(conj, right_table, inner_col, left)
                .as_ref()
                .and_then(expr_column_name)
                .map(str::to_string)
        })
    });
    let Some(outer_col) = outer_col else {
        return per_match;
    };
    match ndv_in_plan(left, outer_col.as_str()) {
        Some(outer_ndv) if outer_ndv > 0 => {
            per_match.min((right_total_rows / outer_ndv as f64).max(1.0))
        }
        _ => per_match,
    }
}

fn base_scan_projected_columns(plan: &PhysicalPlan) -> Option<Arc<Vec<usize>>> {
    match plan {
        PhysicalPlan::SeqScan {
            projected_columns, ..
        }
        | PhysicalPlan::ZoneMapScan {
            projected_columns, ..
        }
        | PhysicalPlan::IndexLookup {
            projected_columns, ..
        }
        | PhysicalPlan::IndexMultiLookup {
            projected_columns, ..
        }
        | PhysicalPlan::IndexRangeScan {
            projected_columns, ..
        }
        | PhysicalPlan::IndexFastScan {
            projected_columns, ..
        }
        | PhysicalPlan::IndexInLookup {
            projected_columns, ..
        } => projected_columns.clone(),
        _ => None,
    }
}

fn dependent_range_join_index(
    left: &PhysicalPlan,
    right: &PhysicalPlan,
    condition: Option<&Expr>,
    query_hints: &[QueryHint],
) -> Option<(PlanTable, IndexSchema, usize, bool, Option<Expr>)> {
    let condition = condition?;
    // `IndexInLookup` is one access path, not a reason to discard every
    // outer-driven index alternative. For an unprefixed IN lookup we can
    // preserve its exact predicate as an INLJ residual and let the ordinary
    // cost comparison decide whether a different index is cheaper. Prefixed
    // composite lookups stay on their existing path until all prefix
    // equalities can be reconstructed here without loss.
    let (table, enforced_filter) = match right {
        PhysicalPlan::SeqScan { table, .. }
        | PhysicalPlan::ZoneMapScan { table, .. }
        | PhysicalPlan::IndexLookup { table, .. }
        | PhysicalPlan::IndexRangeScan { table, .. }
        | PhysicalPlan::IndexFastScan { table, .. } => (table, None),
        // Multi-lookups retain the complete scan predicate. It becomes the
        // INLJ residual when a different outer-driven index is chosen.
        PhysicalPlan::IndexMultiLookup {
            table,
            filter: Some(_),
            ..
        } => (table, None),
        PhysicalPlan::IndexInLookup {
            table,
            key_prefix,
            in_column,
            subquery,
            ..
        } if key_prefix.0.is_empty() => {
            let qualifier = table
                .alias
                .as_ref()
                .unwrap_or(&table.schema.name)
                .to_string();
            (
                table,
                Some(Expr::InSubquery {
                    expr: Box::new(Expr::CompoundIdentifier(vec![qualifier, in_column.clone()])),
                    subquery: subquery.clone(),
                    negated: false,
                }),
            )
        }
        _ => return None,
    };
    // A partitioned inner table keeps one index/heap per partition and a
    // RowId is only unique within a partition; the INLJ probe issues a
    // per-outer-row index lookup against the base `table.id` index, which
    // is empty. Don't offer an index NLJ over a partitioned inner — hash
    // join / plain NLJ scan it partition-safely instead.
    if table.schema.is_partitioned() {
        return None;
    }
    let conjuncts = expr_conjuncts(condition);
    let total_rows = table.schema.stats.row_count.max(1) as f64;
    // INLJ inner-side hint filtering: the inner table here is the join
    // probe, so a `USE_INDEX`/`NO_INDEX` hint targeting it must steer
    // the index choice exactly the same way it would on a standalone
    // scan. Without this, a hint like `/*+ USE_INDEX(lineitem,
    // idx_lineitem_orderkey) */` was silently ignored and PRIMARY won
    // the tie (it sorts earlier in `table.schema.indexes`).
    let hint_table_name = table.alias.as_deref().unwrap_or(table.schema.name.as_str());
    let use_index_hints = query_hint_use_index(query_hints, hint_table_name, &table.schema.name);
    let no_index_hints = query_hint_no_index(query_hints, hint_table_name, &table.schema.name);
    table
        .schema
        .iter_queryable_indexes()
        .filter(|index| {
            !index.has_prefix_columns()
                && (use_index_hints.is_empty()
                    || use_index_hints
                        .iter()
                        .any(|name| index.name.eq_ignore_ascii_case(name)))
                && !no_index_hints
                    .iter()
                    .any(|name| index.name.eq_ignore_ascii_case(name))
        })
        .filter_map(|index| {
            let eq_prefix_len = (0..index.columns.len())
                .take_while(|pos| {
                    conjuncts.iter().any(|expr| {
                        equality_between_index_part_and_left_plan(expr, table, index, *pos, left)
                            .is_some()
                    })
                })
                .count();
            if eq_prefix_len == 0 {
                return None;
            }
            let has_range = match index.key_part(eq_prefix_len) {
                Some(crate::storage::IndexKeyPart::Column(column)) => {
                    conjuncts.iter().any(|expr| {
                        range_on_table_column_with_left_expr(expr, table, column, left).is_some()
                    })
                }
                Some(crate::storage::IndexKeyPart::Expression(_)) => conjuncts.iter().any(|expr| {
                    range_on_index_part_with_left_expr(expr, table, index, eq_prefix_len, left)
                        .is_some()
                }),
                None => false,
            };
            let score = eq_prefix_len * 2 + if has_range { 1 } else { 0 };
            let unique_exact = index.unique && eq_prefix_len >= index.columns.len();
            let mut rows = estimate_index_lookup_rows(
                &table.schema,
                index,
                eq_prefix_len,
                unique_exact,
                total_rows,
            );
            if has_range {
                rows *= crate::engine::cbo::cbo_consts()
                    .default_range_selectivity()
                    .clamp(0.001, 1.0);
            }
            Some((
                table.clone(),
                index.clone(),
                score,
                eq_prefix_len,
                rows.max(1.0),
            ))
        })
        .min_by(|left, right| {
            left.4
                .partial_cmp(&right.4)
                .unwrap_or(std::cmp::Ordering::Equal)
                .then_with(|| right.2.cmp(&left.2))
                // Final tiebreak: a narrower index beats a composite
                // one. Both have the same per-key cardinality on the
                // matched prefix, but the narrower leaves carry fewer
                // bytes per entry → denser pages → fewer page reads on
                // probe. PRIMARY on TPC-H lineitem is `(l_orderkey,
                // l_linenumber)`, so without this rule it beat
                // `idx_lineitem_orderkey` purely on iteration order.
                .then_with(|| left.1.columns.len().cmp(&right.1.columns.len()))
        })
        .map(|(table, index, score, eq_prefix_len, _)| {
            let has_range = score % 2 == 1;
            (table, index, eq_prefix_len, has_range, enforced_filter)
        })
}

/// Returns `true` when `column` is the leading column of some
/// PRIMARY/UNIQUE composite index on `schema`. Such columns are the
/// targets of `estimate_index_lookup_rows`' `row_count / 16` NDV
/// floor; secondary indexes whose leading column matches deserve the
/// same floor because they describe the same column distribution.
fn shared_leading_pk_or_unique_composite(schema: &TableSchema, column: &str) -> bool {
    schema.iter_queryable_indexes().any(|other| {
        (other.primary || other.unique)
            && other.columns.len() > 1
            && other
                .columns
                .first()
                .is_some_and(|c| c.eq_ignore_ascii_case(column))
    })
}

fn expr_conjuncts(expr: &Expr) -> Vec<&Expr> {
    let mut out = Vec::new();
    collect_expr_conjuncts(expr, &mut out);
    out
}

fn equality_between_table_column_and_left_plan(
    expr: &Expr,
    table: &PlanTable,
    column: &str,
    left: &PhysicalPlan,
) -> Option<Expr> {
    let Expr::Binary {
        left: a,
        op: BinaryOp::Eq,
        right: b,
    } = expr
    else {
        return None;
    };
    if expr_matches_table_column(a, table, column) && expr_can_resolve_in_plan(b, left) {
        Some((**b).clone())
    } else if expr_matches_table_column(b, table, column) && expr_can_resolve_in_plan(a, left) {
        Some((**a).clone())
    } else {
        None
    }
}

/// IndexNestedLoop variant that matches either a column key part by
/// name or a functional key part by AST fingerprint — `LOWER(t.c) =
/// drivers.x` against an `INDEX ((LOWER(c)))` becomes a probeable join.
/// Compares the user-side expression with the stored index AST after
/// stripping any table qualifier matching `table`, because catalog
/// expressions are parsed at CREATE TABLE time without aliases
/// (`LOWER(email)`) while ON conditions usually include them
/// (`LOWER(t.email)`).
fn equality_between_index_part_and_left_plan(
    expr: &Expr,
    table: &PlanTable,
    index: &IndexSchema,
    pos: usize,
    left: &PhysicalPlan,
) -> Option<Expr> {
    let Expr::Binary {
        left: a,
        op: BinaryOp::Eq,
        right: b,
    } = expr
    else {
        return None;
    };
    let matches_part = |side: &Expr| -> bool {
        match index.key_part(pos) {
            Some(crate::storage::IndexKeyPart::Column(name)) => {
                expr_matches_table_column(side, table, name)
            }
            Some(crate::storage::IndexKeyPart::Expression(expr_part)) => {
                match expr_part.ast.as_ref() {
                    Some(ast) => {
                        let stripped = strip_qualifier_for_table(side, table);
                        crate::engine::expr_program::expr_fingerprint(&stripped)
                            == crate::engine::expr_program::expr_fingerprint(ast)
                    }
                    None => false,
                }
            }
            None => false,
        }
    };
    let outer = if matches_part(a) && expr_can_resolve_in_plan(b, left) {
        b.as_ref()
    } else if matches_part(b) && expr_can_resolve_in_plan(a, left) {
        a.as_ref()
    } else {
        return None;
    };
    use crate::engine::expr_type::ExprTypeKind;
    let outer_type = crate::engine::executor::select::plan::physical_expr_type(left, outer).kind;
    let target = index_bounds::target_for_index_column(&table.schema, index, pos);
    // A numeric comparison can match multiple string encodings, and f64
    // equality can match several adjacent wide exact numbers. One index key
    // cannot cover either domain; keep those joins on a comparison operator.
    if matches!(
        target,
        crate::storage::ParamTarget::String
            | crate::storage::ParamTarget::CharString
            | crate::storage::ParamTarget::CollatedString { .. }
            | crate::storage::ParamTarget::CollatedCharString { .. }
            | crate::storage::ParamTarget::Bytes
    ) && !matches!(outer_type, ExprTypeKind::String { .. } | ExprTypeKind::Null)
    {
        return None;
    }
    let wide_exact = match index.key_part(pos) {
        Some(crate::storage::IndexKeyPart::Column(name)) => {
            table.schema.column_index(name).ok().is_some_and(|i| {
                matches!(
                    table.schema.columns[i].data_type,
                    DataTypeKind::BigInt
                        | DataTypeKind::Decimal {
                            precision: Some(16..),
                            ..
                        }
                )
            })
        }
        Some(crate::storage::IndexKeyPart::Expression(expr)) => matches!(
            expr.data_type,
            DataTypeKind::BigInt
                | DataTypeKind::Decimal {
                    precision: Some(16..),
                    ..
                }
        ),
        _ => false,
    };
    if wide_exact
        && !matches!(
            outer_type,
            ExprTypeKind::Integer { .. }
                | ExprTypeKind::Decimal { .. }
                | ExprTypeKind::Bit { .. }
                | ExprTypeKind::Null
        )
    {
        return None;
    }
    Some(outer.clone())
}

/// Public re-export for the executor's IndexNestedLoop key compiler.
pub(crate) fn strip_qualifier_for_table_pub(expr: &Expr, table: &PlanTable) -> Expr {
    strip_qualifier_for_table(expr, table)
}

/// Return a copy of `expr` with every `CompoundIdentifier([qualifier,
/// col])` qualifying `table` (by name or alias) collapsed to the
/// unqualified `Identifier(col)`. Used to align ON-condition AST
/// shape with the schema-side functional index expression so
/// fingerprint comparison works for either spelling.
fn strip_qualifier_for_table(expr: &Expr, table: &PlanTable) -> Expr {
    let mapped: Result<_, std::convert::Infallible> =
        crate::parser::visit::try_map_value_expr(expr, &mut |node| {
            Ok(match node {
                Expr::CompoundIdentifier(parts)
                    if parts.len() == 2
                        && matches_table(&parts[0], &table.schema.name, table.alias.as_deref()) =>
                {
                    Some(Expr::Identifier(parts[1].clone()))
                }
                // These were opaque to the index-expression normalizer.
                Expr::QuantifiedComparison { .. } | Expr::GroupKeyRef { .. } => Some(node.clone()),
                _ => None,
            })
        });
    match mapped {
        Ok(expr) => expr,
        Err(never) => match never {},
    }
}

/// Functional-part counterpart to `range_on_table_column_with_left_expr`.
/// `WHERE LOWER(t.c) > drivers.x` against an `INDEX ((LOWER(c)))`
/// becomes a range-driven IndexNestedLoop probe.
fn range_on_index_part_with_left_expr(
    expr: &Expr,
    table: &PlanTable,
    index: &IndexSchema,
    pos: usize,
    left: &PhysicalPlan,
) -> Option<(BinaryOp, Expr)> {
    let Expr::Binary {
        left: a,
        op,
        right: b,
    } = expr
    else {
        return None;
    };
    if !matches!(
        op,
        BinaryOp::Gt | BinaryOp::GtEq | BinaryOp::Lt | BinaryOp::LtEq
    ) {
        return None;
    }
    let matches_part = |side: &Expr| -> bool {
        match index.key_part(pos) {
            Some(crate::storage::IndexKeyPart::Expression(expr_part)) => {
                match expr_part.ast.as_ref() {
                    Some(ast) => {
                        let stripped = strip_qualifier_for_table(side, table);
                        crate::engine::expr_program::expr_fingerprint(&stripped)
                            == crate::engine::expr_program::expr_fingerprint(ast)
                    }
                    None => false,
                }
            }
            _ => false,
        }
    };
    if matches_part(a) && expr_can_resolve_in_plan(b, left) {
        Some((*op, (**b).clone()))
    } else if matches_part(b) && expr_can_resolve_in_plan(a, left) {
        Some((flip_cmp(*op), (**a).clone()))
    } else {
        None
    }
}

fn range_on_table_column_with_left_expr(
    expr: &Expr,
    table: &PlanTable,
    column: &str,
    left: &PhysicalPlan,
) -> Option<(BinaryOp, Expr)> {
    let Expr::Binary {
        left: a,
        op,
        right: b,
    } = expr
    else {
        return None;
    };
    if !matches!(
        op,
        BinaryOp::Gt | BinaryOp::GtEq | BinaryOp::Lt | BinaryOp::LtEq
    ) {
        return None;
    }
    if expr_matches_table_column(a, table, column) && expr_can_resolve_in_plan(b, left) {
        Some((*op, (**b).clone()))
    } else if expr_matches_table_column(b, table, column) && expr_can_resolve_in_plan(a, left) {
        Some((flip_cmp(*op), (**a).clone()))
    } else {
        None
    }
}

fn expr_matches_table_column(expr: &Expr, table: &PlanTable, column: &str) -> bool {
    match expr {
        Expr::Identifier(name) => name.eq_ignore_ascii_case(column),
        Expr::CompoundIdentifier(parts) => {
            let Some(expr_column) = parts.last() else {
                return false;
            };
            if !expr_column.eq_ignore_ascii_case(column) {
                return false;
            }
            let qualifier = if parts.len() >= 2 {
                Some(parts[parts.len() - 2].as_str())
            } else {
                None
            };
            qualifier.is_none_or(|qualifier| {
                matches_table(qualifier, &table.schema.name, table.alias.as_deref())
            })
        }
        _ => false,
    }
}

fn extract_scan_filter(plan: &PhysicalPlan) -> Option<Expr> {
    match plan {
        PhysicalPlan::SeqScan { filter, .. }
        | PhysicalPlan::ZoneMapScan { filter, .. }
        | PhysicalPlan::IndexMerge { filter, .. }
        | PhysicalPlan::IndexLookup { filter, .. }
        | PhysicalPlan::IndexMultiLookup { filter, .. }
        | PhysicalPlan::IndexRangeScan { filter, .. }
        | PhysicalPlan::IndexFastScan { filter, .. }
        | PhysicalPlan::IndexInLookup { filter, .. }
        | PhysicalPlan::SystemScan { filter, .. } => filter.clone(),
        _ => None,
    }
}

pub(crate) fn collect_physical_plan_tables(plan: &PhysicalPlan) -> Vec<PlanTable> {
    let mut tables = Vec::new();
    collect_physical_plan_tables_into(plan, &mut tables);
    tables
}

fn collect_physical_plan_tables_into(plan: &PhysicalPlan, tables: &mut Vec<PlanTable>) {
    match plan {
        PhysicalPlan::NullExtend { input, table, .. } => {
            let start = tables.len();
            collect_physical_plan_tables_into(input, tables);
            if !tables[start..]
                .iter()
                .any(|t| plan_table_matches(t, &table.schema.name, table.alias.as_deref()))
            {
                tables.push(table.clone());
            }
        }

        PhysicalPlan::SeqScan { table, .. }
        | PhysicalPlan::ZoneMapScan { table, .. }
        | PhysicalPlan::IndexMerge { table, .. }
        | PhysicalPlan::IndexLookup { table, .. }
        | PhysicalPlan::IndexMultiLookup { table, .. }
        | PhysicalPlan::IndexRangeScan { table, .. }
        | PhysicalPlan::IndexFastScan { table, .. }
        | PhysicalPlan::FulltextScan { table, .. }
        | PhysicalPlan::SpatialScan { table, .. }
        | PhysicalPlan::VectorIndexScan { table, .. }
        | PhysicalPlan::VectorBruteScan { table, .. }
        | PhysicalPlan::IndexInLookup { table, .. }
        | PhysicalPlan::CoveringIndexAggregate { table, .. }
        | PhysicalPlan::ParallelSeqScan { table, .. }
        | PhysicalPlan::ParallelZoneMapScan { table, .. }
        | PhysicalPlan::ParallelIndexFastScan { table, .. }
        | PhysicalPlan::ParallelIndexRangeScan { table, .. }
        | PhysicalPlan::ParallelIndexAggregate { table, .. } => tables.push(table.clone()),
        PhysicalPlan::IndexNestedLoopJoin {
            left, right_table, ..
        } => {
            collect_physical_plan_tables_into(left, tables);
            tables.push(right_table.clone());
        }
        PhysicalPlan::IndexAggregate { table, .. } => tables.push(table.clone()),
        // A CTE / FROM-subquery reference must report its alias + columns so
        // the join planner can bind equi-join predicates (`cte.k = base.k`)
        // to this side. Returning an empty set made every such predicate
        // unresolvable, demoting the join to a cartesian product that
        // OOM-kills the server on any non-trivial CTE.
        PhysicalPlan::CteScan {
            name,
            alias,
            columns,
            ..
        } => {
            let schema = crate::storage::schema::TableSchema::synthetic_for_planning(name, columns);
            tables.push(PlanTable::new(
                0,
                std::sync::Arc::new(schema),
                alias.clone(),
            ));
        }
        PhysicalPlan::ValuesScan { columns, .. } => {
            let schema =
                crate::storage::schema::TableSchema::synthetic_for_planning("values", columns);
            tables.push(PlanTable::new(0, std::sync::Arc::new(schema), None));
        }
        PhysicalPlan::JsonTableScan {
            relation, columns, ..
        } => {
            let schema =
                crate::storage::schema::TableSchema::synthetic_for_planning(relation, columns);
            tables.push(PlanTable::new(
                0,
                std::sync::Arc::new(schema),
                Some(relation.clone()),
            ));
        }
        PhysicalPlan::SystemScan {
            name,
            relation,
            columns,
            ..
        } => {
            let schema = crate::storage::schema::TableSchema::synthetic_for_planning(name, columns);
            tables.push(PlanTable::new(
                0,
                std::sync::Arc::new(schema),
                Some(relation.clone()),
            ));
        }
        _ => {
            for child in plan.children() {
                collect_physical_plan_tables_into(child, tables);
            }
        }
    }
}

pub(crate) fn expr_can_resolve_in_table_set(
    expr: &Expr,
    left_tables: &[PlanTable],
    right_tables: &[PlanTable],
) -> bool {
    let mut needs_left = false;
    let mut needs_right = false;
    check_expr_table_sides(
        expr,
        left_tables,
        right_tables,
        &mut needs_left,
        &mut needs_right,
    );
    needs_left && needs_right
}

/// Whether an expression references at least one column resolvable by this
/// table set. Join-graph construction uses this to cache predicate relation
/// masks once instead of re-walking the AST for every DP bipartition.
pub(crate) fn expr_touches_table_set(expr: &Expr, tables: &[PlanTable]) -> bool {
    let mut touches = false;
    let mut unused = false;
    check_expr_table_sides(expr, tables, &[], &mut touches, &mut unused);
    touches
}

fn check_expr_table_sides(
    expr: &Expr,
    left_tables: &[PlanTable],
    right_tables: &[PlanTable],
    needs_left: &mut bool,
    needs_right: &mut bool,
) {
    match expr {
        Expr::Identifier(name) => {
            if left_tables.iter().any(|t| {
                t.schema
                    .columns
                    .iter()
                    .any(|c| c.name.eq_ignore_ascii_case(name))
            }) {
                *needs_left = true;
            }
            if right_tables.iter().any(|t| {
                t.schema
                    .columns
                    .iter()
                    .any(|c| c.name.eq_ignore_ascii_case(name))
            }) {
                *needs_right = true;
            }
        }
        Expr::CompoundIdentifier(parts) => {
            if let Some(column) = parts.last() {
                let qualifier = if parts.len() >= 2 {
                    Some(parts[parts.len() - 2].as_str())
                } else {
                    None
                };
                let matches_set = |tables: &[PlanTable]| {
                    tables.iter().any(|t| {
                        qualifier
                            .is_none_or(|q| matches_table(q, &t.schema.name, t.alias.as_deref()))
                            && t.schema
                                .columns
                                .iter()
                                .any(|c| c.name.eq_ignore_ascii_case(column))
                    })
                };
                if matches_set(left_tables) {
                    *needs_left = true;
                }
                if matches_set(right_tables) {
                    *needs_right = true;
                }
            }
        }
        Expr::Literal(_)
        | Expr::BoundValue(_)
        | Expr::Placeholder(_)
        | Expr::Wildcard
        | Expr::UserVariable(_)
        | Expr::SystemVariable { .. } => {}
        Expr::Unary { expr: inner, .. } | Expr::IsNull(inner) | Expr::IsNotNull(inner) => {
            check_expr_table_sides(inner, left_tables, right_tables, needs_left, needs_right);
        }
        Expr::Binary { left, right, .. } => {
            check_expr_table_sides(left, left_tables, right_tables, needs_left, needs_right);
            check_expr_table_sides(right, left_tables, right_tables, needs_left, needs_right);
        }
        Expr::InList { expr, list, .. } => {
            check_expr_table_sides(expr, left_tables, right_tables, needs_left, needs_right);
            for item in list {
                check_expr_table_sides(item, left_tables, right_tables, needs_left, needs_right);
            }
        }
        Expr::Between {
            expr, low, high, ..
        } => {
            check_expr_table_sides(expr, left_tables, right_tables, needs_left, needs_right);
            check_expr_table_sides(low, left_tables, right_tables, needs_left, needs_right);
            check_expr_table_sides(high, left_tables, right_tables, needs_left, needs_right);
        }
        Expr::Function { args, .. } => {
            for arg in args {
                check_expr_table_sides(arg, left_tables, right_tables, needs_left, needs_right);
            }
        }
        Expr::Like { expr, pattern, .. } => {
            check_expr_table_sides(expr, left_tables, right_tables, needs_left, needs_right);
            check_expr_table_sides(pattern, left_tables, right_tables, needs_left, needs_right);
        }
        Expr::Case {
            operand,
            conditions,
            results,
            else_result,
        } => {
            if let Some(e) = operand {
                check_expr_table_sides(e, left_tables, right_tables, needs_left, needs_right);
            }
            for c in conditions {
                check_expr_table_sides(c, left_tables, right_tables, needs_left, needs_right);
            }
            for r in results {
                check_expr_table_sides(r, left_tables, right_tables, needs_left, needs_right);
            }
            if let Some(e) = else_result {
                check_expr_table_sides(e, left_tables, right_tables, needs_left, needs_right);
            }
        }
        _ => {}
    }
}

/// Whether *every* concrete column referenced by `expr` resolves to some table
/// in `left_tables ∪ right_tables`. Unlike `expr_can_resolve_in_table_set`
/// (which only asks whether the predicate *touches* both sides), this requires
/// full coverage, so a predicate that also references a third, not-yet-joined
/// table is reported as not-yet-resolvable.
///
/// This is the guard that keeps an equi-join buried inside an `OR` (e.g.
/// TPC-DS q13: `(ss_hdemo_sk=hd_demo_sk AND ...) OR (...)`) from being pinned
/// as the condition of a join below the table it needs — which otherwise
/// produced `1054 unknown column 'hd_demo_sk'`. With the guard the OR floats up
/// until every table it references is present, then lands as that join's
/// condition (or as a top-level residual filter).
///
/// Expression variants this walker cannot analyse (subqueries, window
/// functions) are treated as *resolved* — they do not force a `false`. The
/// existing side-detection (`check_expr_table_sides`) ignores them too, and
/// subquery-bearing conjuncts are handled by a separate mechanism; treating
/// them as resolved keeps this guard purely additive for the concrete-column
/// case it targets.
pub(crate) fn expr_columns_resolved_across(
    expr: &Expr,
    left_tables: &[PlanTable],
    right_tables: &[PlanTable],
) -> bool {
    let mut all_resolved = true;
    check_expr_columns_resolved(expr, left_tables, right_tables, &mut all_resolved);
    all_resolved
}

fn check_expr_columns_resolved(
    expr: &Expr,
    left_tables: &[PlanTable],
    right_tables: &[PlanTable],
    all_resolved: &mut bool,
) {
    let _ = crate::parser::visit::try_inspect_value_expr(expr, |node| {
        match node {
            Expr::Identifier(name) => {
                let resolves = |tables: &[PlanTable]| {
                    tables.iter().any(|t| {
                        t.schema
                            .columns
                            .iter()
                            .any(|c| c.name.eq_ignore_ascii_case(name))
                    })
                };
                if !resolves(left_tables) && !resolves(right_tables) {
                    *all_resolved = false;
                }
            }
            Expr::CompoundIdentifier(parts) => {
                if let Some(column) = parts.last() {
                    let qualifier = if parts.len() >= 2 {
                        Some(parts[parts.len() - 2].as_str())
                    } else {
                        None
                    };
                    let matches_set = |tables: &[PlanTable]| {
                        tables.iter().any(|t| {
                            qualifier.is_none_or(|q| {
                                matches_table(q, &t.schema.name, t.alias.as_deref())
                            }) && t
                                .schema
                                .columns
                                .iter()
                                .any(|c| c.name.eq_ignore_ascii_case(column))
                        })
                    };
                    if !matches_set(left_tables) && !matches_set(right_tables) {
                        *all_resolved = false;
                    }
                }
            }
            Expr::Subquery(_)
            | Expr::Exists { .. }
            | Expr::InSubquery { .. }
            | Expr::QuantifiedComparison { .. } => *all_resolved = false,
            // These were opaque to this conservative guard before sharing traversal.
            Expr::Tuple(_) | Expr::GroupKeyRef { .. } | Expr::WindowFunction { .. } => {
                return Ok(false)
            }
            _ => {}
        }
        if *all_resolved {
            Ok(true)
        } else {
            Err(())
        }
    });
}

/// The `JoinType` of a join node at the root of `plan`, if it is one.
fn physical_root_join_type(plan: &PhysicalPlan) -> Option<JoinType> {
    match plan {
        PhysicalPlan::NestedLoopJoin { join_type, .. }
        | PhysicalPlan::HashJoin { join_type, .. }
        | PhysicalPlan::IndexNestedLoopJoin { join_type, .. } => Some(*join_type),
        PhysicalPlan::AdaptiveJoin { join_type, .. } => Some(*join_type),
        _ => None,
    }
}

fn attach_filter(plan: PhysicalPlan, predicate: Expr) -> PhysicalPlan {
    // For an OUTER join at the root, folding `predicate` into the join's
    // `residual`/`condition` is WRONG: a join residual is a *match* condition,
    // so a preserved-side row failing it is NULL-extended rather than dropped.
    // A WHERE conjunct must instead drop the row. Wrap such joins in a genuine
    // `Filter` node. INNER/CROSS keep the fold (their residual is applied only
    // to emitted rows, which is exactly a filter). SEMI/ANTI must also
    // preserve the distinction between match conditions and output filters.
    if let Some(join_type) = physical_root_join_type(&plan) {
        if !matches!(join_type, JoinType::Inner | JoinType::Cross) {
            let estimated_rows = (estimated_rows(&plan) * 0.25).max(1.0);
            let cost = plan_cost(&plan);
            return PhysicalPlan::Filter {
                input: Box::new(plan),
                predicate,
                estimated_rows,
                cost,
            };
        }
    }
    match plan {
        PhysicalPlan::SeqScan {
            table,
            filter,
            estimated_rows,
            access_rows,
            cost,
            possible_keys,
            runtime_filters,
            hint_note,
            projected_columns,
        } => PhysicalPlan::SeqScan {
            table,
            filter: combine_optional(filter, predicate),
            estimated_rows: (estimated_rows * 0.25),
            access_rows,
            cost,
            possible_keys,
            runtime_filters,
            hint_note,
            projected_columns,
        },
        PhysicalPlan::ZoneMapScan {
            table,
            filter,
            estimated_rows,
            access_rows,
            cost,
            possible_keys,
            runtime_filters,
            hint_note,
            projected_columns,
            zone_map,
        } => PhysicalPlan::ZoneMapScan {
            table,
            filter: combine_optional(filter, predicate),
            estimated_rows: (estimated_rows * 0.25),
            access_rows,
            cost,
            possible_keys,
            runtime_filters,
            hint_note,
            projected_columns,
            zone_map,
        },
        PhysicalPlan::IndexLookup {
            table,
            index,
            key,
            exact,
            filter,
            estimated_rows,
            access_rows,
            cost,
            possible_keys,
            covering,
            hint_note,
            projected_columns,
        } => PhysicalPlan::IndexLookup {
            table,
            index,
            key,
            exact,
            filter: combine_optional(filter, predicate),
            estimated_rows: (estimated_rows * 0.25),
            access_rows,
            cost,
            possible_keys,
            covering,
            hint_note,
            projected_columns,
        },
        PhysicalPlan::IndexRangeScan {
            table,
            index,
            low,
            high,
            filter,
            estimated_rows,
            access_rows,
            cost,
            possible_keys,
            covering,
            reverse,
            hint_note,
            projected_columns,
        } => PhysicalPlan::IndexRangeScan {
            table,
            index,
            low,
            high,
            filter: combine_optional(filter, predicate),
            estimated_rows: (estimated_rows * 0.25),
            access_rows,
            cost,
            possible_keys,
            covering,
            reverse,
            hint_note,
            projected_columns,
        },
        PhysicalPlan::IndexFastScan {
            table,
            index,
            low,
            high,
            filter,
            estimated_rows,
            access_rows,
            cost,
            possible_keys,
            reverse,
            hint_note,
            projected_columns,
        } => PhysicalPlan::IndexFastScan {
            table,
            index,
            low,
            high,
            filter: combine_optional(filter, predicate),
            estimated_rows: (estimated_rows * 0.25),
            access_rows,
            cost,
            possible_keys,
            reverse,
            hint_note,
            projected_columns,
        },
        PhysicalPlan::NestedLoopJoin {
            left,
            right,
            join_type,
            condition,
            estimated_rows,
            cost,
        } => PhysicalPlan::NestedLoopJoin {
            left,
            right,
            join_type,
            condition: combine_optional(condition, predicate),
            estimated_rows: (estimated_rows * 0.25),
            cost,
        },
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
            estimated_rows,
            cost,
        } => PhysicalPlan::HashJoin {
            left,
            right,
            join_type,
            left_keys,
            right_keys,
            null_safe,
            build_side,
            residual: combine_optional(residual, predicate),
            output_projection,
            estimated_rows: (estimated_rows * 0.25),
            cost,
        },
        PhysicalPlan::AdaptiveJoin {
            left,
            right,
            join_type,
            candidates,
            preferred,
            estimated_rows,
            cost,
        } => {
            let candidates = candidates
                .into_iter()
                .map(|candidate| combine_adaptive_candidate_predicate(candidate, predicate.clone()))
                .collect();
            PhysicalPlan::AdaptiveJoin {
                left,
                right,
                join_type,
                candidates,
                preferred,
                estimated_rows: (estimated_rows * 0.25),
                cost,
            }
        }
        PhysicalPlan::IndexNestedLoopJoin {
            left,
            right_table,
            right_index,
            join_type,
            condition,
            eq_prefix_len,
            estimated_rows,
            cost,
            right_covering,
            right_projected_columns,
            right_runtime_filters,
        } => PhysicalPlan::IndexNestedLoopJoin {
            left,
            right_table,
            right_index,
            join_type,
            condition: combine_optional(condition, predicate),
            eq_prefix_len,
            estimated_rows: (estimated_rows * 0.25),
            cost,
            right_covering,
            right_projected_columns,
            right_runtime_filters,
        },
        PhysicalPlan::SystemScan {
            name,
            relation,
            columns,
            filter,
            estimated_rows,
            cost,
            projected_columns,
        } => PhysicalPlan::SystemScan {
            name,
            relation,
            columns,
            filter: combine_optional(filter, predicate),
            estimated_rows: estimated_rows * 0.25,
            cost,
            projected_columns,
        },
        other => {
            let rows = estimated_rows(&other) * 0.25;
            let cost = plan_cost(&other);
            PhysicalPlan::Filter {
                input: Box::new(other),
                predicate,
                estimated_rows: rows,
                cost,
            }
        }
    }
}

/// Cost-ordered greedy join selection. Default on; set
/// `MYDB_JOIN_GREEDY_COST_ORDER=0` (or `off`/`false`) to restore the legacy
/// first-connected (smallest-base-size) pick.
fn greedy_cost_order_enabled() -> bool {
    !matches!(
        crate::util::env::var("MYDB_JOIN_GREEDY_COST_ORDER")
            .ok()
            .as_deref(),
        Some("0") | Some("off") | Some("false")
    )
}

/// A table-level STRAIGHT_JOIN constrains only its own right operand. Keep
/// other inner-join sources eligible for cost-based placement, including
/// sources written after the constrained edge. Source positions are identities
/// here, so aliases and self-joins need no name-based matching.
fn plan_straight_inner_joins(
    ctx: &crate::engine::cbo::CboContext,
    scans: Vec<PhysicalPlan>,
    predicates: Vec<Expr>,
    select: &Select,
    hints: &[QueryHint],
) -> (PhysicalPlan, Vec<Expr>) {
    let mut pending: Vec<(usize, PhysicalPlan)> = scans.into_iter().enumerate().collect();
    let mut joined = vec![false; pending.len()];
    let const_sources: Vec<bool> = pending
        .iter()
        .map(|(_, scan)| is_mysql_const_scan(scan))
        .collect();
    let ready = |position: usize, joined: &[bool]| {
        if joined
            .iter()
            .enumerate()
            .any(|(index, placed)| const_sources[index] && !placed)
        {
            return const_sources[position];
        }
        position == 0
            || !select.joins[position - 1].straight
            || joined[..position].iter().all(|placed| *placed)
    };
    let first = pending
        .iter()
        .enumerate()
        .filter(|(_, (position, _))| ready(*position, &joined))
        .min_by(|(_, (_, left)), (_, (_, right))| {
            estimated_rows(left)
                .partial_cmp(&estimated_rows(right))
                .unwrap_or(std::cmp::Ordering::Equal)
        })
        .map(|(index, _)| index)
        .expect("an unconstrained first source must exist");
    let (position, mut head) = pending.remove(first);
    joined[position] = true;
    let mut plan_tables = collect_physical_plan_tables(&head);
    let mut remaining = predicates;

    while !pending.is_empty() {
        let mut best: Option<(usize, bool, f64, f64)> = None;
        for (index, (position, right)) in pending.iter().enumerate() {
            if !ready(*position, &joined) {
                continue;
            }
            let right_tables = collect_physical_plan_tables(right);
            let parts: Vec<Expr> = remaining
                .iter()
                .filter(|predicate| {
                    expr_can_resolve_in_table_set(predicate, &plan_tables, &right_tables)
                        && expr_columns_resolved_across(predicate, &plan_tables, &right_tables)
                })
                .cloned()
                .collect();
            let connected = !parts.is_empty();
            let join_type = if connected {
                JoinType::Inner
            } else {
                JoinType::Cross
            };
            let trial = choose_join_with_hints(
                ctx,
                head.clone(),
                right.clone(),
                join_type,
                combine_conjuncts(parts),
                hints,
            );
            let rows = estimated_rows(&trial);
            let cost = plan_cost_total(&trial);
            if best
                .as_ref()
                .is_none_or(|(_, best_connected, best_rows, best_cost)| {
                    (connected && !best_connected)
                        || (connected == *best_connected
                            && (rows < *best_rows - 1e-6
                                || ((rows - *best_rows).abs() <= 1e-6 && cost < *best_cost)))
                })
            {
                best = Some((index, connected, rows, cost));
            }
        }
        let (pick, _, _, _) = best.expect("STRAIGHT_JOIN dependencies must be acyclic");
        let (position, right) = pending.remove(pick);
        let right_tables = collect_physical_plan_tables(&right);
        let mut join_parts = Vec::new();
        let mut still = Vec::new();
        for predicate in remaining {
            if expr_can_resolve_in_table_set(&predicate, &plan_tables, &right_tables)
                && expr_columns_resolved_across(&predicate, &plan_tables, &right_tables)
            {
                join_parts.push(predicate);
            } else {
                still.push(predicate);
            }
        }
        remaining = still;
        plan_tables.extend(right_tables);
        let join_type = if join_parts.is_empty() {
            JoinType::Cross
        } else {
            JoinType::Inner
        };
        head = choose_join_with_hints(
            ctx,
            head,
            right,
            join_type,
            combine_conjuncts(join_parts),
            hints,
        );
        joined[position] = true;
    }
    (head, remaining)
}

fn is_mysql_const_scan(scan: &PhysicalPlan) -> bool {
    matches!(scan, PhysicalPlan::IndexLookup { index, key, exact: true, .. }
        if index.unique && key.0.len() >= index.columns.len()
            && !index.has_prefix_columns() && !index.has_expression_parts())
}

/// Choose the index into `scans` of the next relation to join onto `head`
/// (which already covers `plan_tables`). A *connected* candidate — one sharing
/// a resolvable equi-predicate with the placed set — is always preferred over a
/// cartesian product.
///
/// Among the connected candidates, when `cost_order` is on, the one whose trial
/// join yields the fewest estimated rows wins (a min-intermediate-cardinality
/// greedy), with cost as the tie-break. This places a *selective* join early:
/// e.g. TPC-DS Q64 (18 tables > `MAX_DP_TABLES`, so it lands here) joins `item`
/// — filtered to ≈90 of 102k rows — onto the 2.9M `store_sales` stream, cutting
/// it to ≈2.5k *before* the 14 neutral demographic/address/date joins, instead
/// of after every one of them has processed all 2.9M rows. The legacy
/// first-connected pick keys only on base table size, so it leaves `item` for
/// last and pays the 14 dimension joins at full fact-table width.
///
/// Falls back to the first connected scan (smallest, since `scans` is
/// size-sorted) when cost ordering is off or only one candidate is connected;
/// returns 0 (smallest remaining) for a genuinely disconnected component.
fn greedy_pick_next_join(
    ctx: &crate::engine::cbo::CboContext,
    head: &PhysicalPlan,
    plan_tables: &[PlanTable],
    scans: &[PhysicalPlan],
    remaining_preds: &[Expr],
    cost_order: bool,
) -> usize {
    let connecting_parts = |right: &PhysicalPlan| -> Vec<Expr> {
        let rt = collect_physical_plan_tables(right);
        remaining_preds
            .iter()
            .filter(|pred| {
                expr_can_resolve_in_table_set(pred, plan_tables, &rt)
                    && expr_columns_resolved_across(pred, plan_tables, &rt)
            })
            .cloned()
            .collect()
    };
    let first_connected = scans
        .iter()
        .position(|right| !connecting_parts(right).is_empty());
    let Some(first) = first_connected else {
        // Disconnected component: take the smallest remaining (size-sorted) and
        // accept the unavoidable cartesian product.
        return 0;
    };
    if !cost_order {
        return first;
    }
    let mut best_idx = first;
    let mut best_rows = f64::INFINITY;
    let mut best_cost = f64::INFINITY;
    for (i, right) in scans.iter().enumerate() {
        let join_parts = connecting_parts(right);
        if join_parts.is_empty() {
            continue;
        }
        // Trial-cost with no hints: we want the pure join-output cardinality for
        // ordering, and method hints (FORCE_HASH/NL) don't change it. Real hints
        // are applied when the chosen edge is committed by the caller.
        let trial = choose_join_with_hints(
            ctx,
            head.clone(),
            right.clone(),
            JoinType::Inner,
            combine_conjuncts(join_parts),
            &[],
        );
        let rows = estimated_rows(&trial);
        let cost = plan_cost_total(&trial);
        if rows < best_rows - 1e-6 || ((rows - best_rows).abs() <= 1e-6 && cost < best_cost) {
            best_idx = i;
            best_rows = rows;
            best_cost = cost;
        }
    }
    best_idx
}

fn reorder_inner_join_scans(scans: &mut [PhysicalPlan], select: &Select) {
    if select
        .joins
        .iter()
        .all(|join| join.join_type == JoinType::Inner)
    {
        scans.sort_by(|left, right| {
            estimated_rows(left)
                .partial_cmp(&estimated_rows(right))
                .unwrap_or(std::cmp::Ordering::Equal)
        });
    }
}

fn collect_index_merge_key_lengths(
    table: &PlanTable,
    node: &IndexMergeNode,
    out: &mut Vec<String>,
) {
    match node {
        IndexMergeNode::Leaf(leaf) => {
            let parts = match &leaf.access {
                IndexAccessKind::Lookup { key, .. } => key.0.len(),
                IndexAccessKind::MultiLookup { keys, .. } => {
                    keys.first().map(|key| key.0.len()).unwrap_or(0)
                }
                IndexAccessKind::Range { low, high } => low
                    .as_ref()
                    .or(high.as_ref())
                    .map(|bound| bound.key.0.len())
                    .unwrap_or(leaf.index.columns.len()),
            };
            out.push(table.schema.index_key_len(&leaf.index, parts).to_string());
        }
        IndexMergeNode::Union { children, .. } | IndexMergeNode::Intersection { children } => {
            for child in children {
                collect_index_merge_key_lengths(table, child, out);
            }
        }
    }
}

/// Phase 6 diagnostic: classify a table's statistics state for EXPLAIN's
/// `extra` column. Returns:
/// - `Some("stats: missing")` when `ANALYZE TABLE` has not been run
///   (or has been run but the table was empty, leaving `row_count = 0`),
/// - `Some("stats: stale")` when more than 20% of rows have been
///   inserted/updated/deleted since the last `ANALYZE`,
/// - `None` when stats are fresh enough to trust.
///
/// The 20% threshold matches PostgreSQL's autovacuum trigger and
/// MySQL 8.0 `innodb_stats_auto_recalc`. It's stable across phases —
/// future tuning lives in a `cbo_stats_stale_ratio` GUC.
fn stats_freshness_tag(schema: &TableSchema, sidecar_delta: u64) -> Option<&'static str> {
    let stats = &schema.stats;
    if stats.row_count == 0 {
        return Some("stats: missing");
    }
    if stats_are_stale(stats, sidecar_delta) {
        return Some("stats: stale");
    }
    None
}

/// Stats-backed index-order/heap-order correlation for range-scan
/// costing. Primary keys keep 1.0 (heap insert order tracks key order
/// in this engine); secondary indexes use the ANALYZE-sampled value.
/// Missing correlation is conservatively treated as uncorrelated.
fn index_cost_correlation(schema: &TableSchema, index: &IndexSchema) -> f64 {
    if index.primary {
        return 1.0;
    }
    schema
        .stats
        .indexes
        .get(&index.name)
        .and_then(|stats| stats.correlation)
        .map(|c| c.clamp(0.0, 1.0))
        .unwrap_or(0.0)
}

fn index_cost_height(schema: &TableSchema, index: &IndexSchema, entries: f64) -> f64 {
    schema
        .stats
        .indexes
        .get(&normalize_expr_column(&index.name))
        .and_then(|stats| stats.tree_height)
        .filter(|height| *height > 0)
        .map(|height| height as f64)
        .unwrap_or_else(|| {
            crate::engine::cbo::operator_cost::estimated_btree_height(entries.max(1.0))
        })
}

/// Scale observed total leaf pages by the fraction of index entries visited.
/// The full analyzed leaf population stays the source of truth; filtered
/// output rows never determine B-tree height.
fn index_cost_leaf_pages(
    schema: &TableSchema,
    index: &IndexSchema,
    visited_entries: f64,
    total_entries: f64,
) -> f64 {
    let analyzed = schema
        .stats
        .indexes
        .get(&normalize_expr_column(&index.name))
        .and_then(|stats| stats.leaf_page_count)
        .filter(|pages| *pages > 0)
        .map(|pages| pages as f64);
    match analyzed {
        Some(pages) => {
            if visited_entries <= 0.0 || total_entries <= 0.0 {
                0.0
            } else {
                (pages * (visited_entries / total_entries).clamp(0.0, 1.0))
                    .ceil()
                    .min(pages)
            }
        }
        None => crate::engine::cbo::operator_cost::estimated_index_leaf_pages(
            schema,
            index,
            visited_entries,
        ),
    }
}

fn stats_are_stale(stats: &crate::storage::TableStats, sidecar_delta: u64) -> bool {
    let modified = stats
        .modified_rows_since_analyze
        .saturating_add(sidecar_delta) as f64;
    let total = stats.row_count as f64;
    total > 0.0 && modified / total > 0.2
}

fn match_join_ref_pair(
    candidate_ref: &Expr,
    candidate_col: &Expr,
    index_col: &str,
    right_table: &str,
) -> Option<String> {
    let col_name = match candidate_col {
        Expr::Identifier(name) => name.as_str(),
        Expr::CompoundIdentifier(parts) if parts.len() == 2 => {
            if !parts[0].eq_ignore_ascii_case(right_table) {
                return None;
            }
            parts[1].as_str()
        }
        _ => return None,
    };
    if !col_name.eq_ignore_ascii_case(index_col) {
        return None;
    }
    match candidate_ref {
        Expr::Identifier(name) => Some(name.clone()),
        Expr::CompoundIdentifier(parts) => Some(parts.join(".")),
        Expr::Literal(_) | Expr::BoundValue(_) => Some("const".to_string()),
        _ => Some("func".to_string()),
    }
}

fn format_hash_join_extra(
    build_side: BuildSide,
    left_keys: &[Expr],
    right_keys: &[Expr],
    null_safe: &[bool],
    residual: Option<&Expr>,
) -> String {
    let build_label = match build_side {
        BuildSide::Left => "left",
        BuildSide::Right => "right",
    };
    let mut keys = Vec::with_capacity(left_keys.len());
    for ((l, r), ns) in left_keys
        .iter()
        .zip(right_keys.iter())
        .zip(null_safe.iter())
    {
        let op = if *ns { "<=>" } else { "=" };
        keys.push(format!(
            "{}{}{}",
            expr_to_explain_string(l),
            op,
            expr_to_explain_string(r)
        ));
    }
    let mut out = format!(
        "Hash join (build={}, keys=[{}]",
        build_label,
        keys.join(", ")
    );
    if let Some(residual) = residual {
        out.push_str(", residual=");
        out.push_str(&expr_to_explain_string(residual));
    }
    out.push(')');
    out
}

fn format_adaptive_join_candidates(candidates: &[AdaptiveJoinCandidate]) -> String {
    candidates
        .iter()
        .map(|candidate| {
            match adaptive_candidate_choice(candidate) {
                AdaptiveJoinChoice::Hash => "Hash join",
                AdaptiveJoinChoice::IndexNestedLoop => "Index nested loop",
                AdaptiveJoinChoice::NestedLoop => "Nested loop",
            }
            .to_string()
        })
        .collect::<Vec<_>>()
        .join(",")
}

pub(crate) fn expr_to_explain_string(expr: &Expr) -> String {
    match expr {
        Expr::Identifier(name) => name.clone(),
        Expr::CompoundIdentifier(parts) => parts.join("."),
        Expr::Literal(lit) => match lit {
            crate::parser::Literal::Null => "NULL".to_string(),
            crate::parser::Literal::Bool(value) => value.to_string(),
            crate::parser::Literal::Int(value) => value.to_string(),
            crate::parser::Literal::UInt(value) => value.to_string(),
            crate::parser::Literal::Float(value) => value.to_string(),
            crate::parser::Literal::Decimal(value) => value.clone(),
            crate::parser::Literal::String(value) => {
                format!("'{}'", value.replace('\\', "\\\\").replace('\'', "''"))
            }
            crate::parser::Literal::HexBytes(b) => {
                let mut s = String::with_capacity(2 + 2 * b.len());
                s.push_str("0x");
                crate::util::hex::append_upper(&mut s, b);
                s
            }
            crate::parser::Literal::BitBytes(b) => {
                let mut s = String::with_capacity(3 + 8 * b.len());
                s.push_str("b'");
                for byte in b {
                    for i in (0..8).rev() {
                        s.push(if (byte >> i) & 1 == 1 { '1' } else { '0' });
                    }
                }
                s.push('\'');
                s
            }
        },
        Expr::BoundValue(value) => value_to_string(value),
        Expr::Binary { left, op, right } => {
            let op_str = match op {
                BinaryOp::Eq => "=",
                BinaryOp::NullSafeEq => "<=>",
                BinaryOp::NotEq => "<>",
                BinaryOp::Lt => "<",
                BinaryOp::LtEq => "<=",
                BinaryOp::Gt => ">",
                BinaryOp::GtEq => ">=",
                BinaryOp::Add => "+",
                BinaryOp::Sub => "-",
                BinaryOp::Mul => "*",
                BinaryOp::Div => "/",
                BinaryOp::IntDiv => "DIV",
                BinaryOp::Mod => "%",
                BinaryOp::BitAnd => "&",
                BinaryOp::BitOr => "|",
                BinaryOp::BitXor => "^",
                BinaryOp::ShiftLeft => "<<",
                BinaryOp::ShiftRight => ">>",
                BinaryOp::And => " AND ",
                BinaryOp::Or => " OR ",
                BinaryOp::Xor => " XOR ",
                BinaryOp::Arrow => "->",
                BinaryOp::LongArrow => "->>",
                BinaryOp::Concat => "||",
            };
            format!(
                "{}{}{}",
                explain_binary_operand(left, *op),
                op_str,
                explain_binary_operand(right, *op)
            )
        }
        Expr::Tuple(items) => format!(
            "({})",
            items
                .iter()
                .map(expr_to_explain_string)
                .collect::<Vec<_>>()
                .join(",")
        ),
        Expr::InList {
            expr,
            list,
            negated,
        } => format!(
            "{} {}IN ({})",
            expr_to_explain_string(expr),
            if *negated { "NOT " } else { "" },
            list.iter()
                .map(expr_to_explain_string)
                .collect::<Vec<_>>()
                .join(",")
        ),
        Expr::Between {
            expr,
            low,
            high,
            negated,
        } => format!(
            "{} {}BETWEEN {} AND {}",
            expr_to_explain_string(expr),
            if *negated { "NOT " } else { "" },
            expr_to_explain_string(low),
            expr_to_explain_string(high)
        ),
        Expr::Placeholder(i) => format!("?{i}"),
        Expr::Function { name, args, .. } => format!(
            "{}({})",
            name,
            args.iter()
                .map(expr_to_explain_string)
                .collect::<Vec<_>>()
                .join(",")
        ),
        Expr::Unary { op, expr } => match op {
            UnaryOp::Not => format!("NOT {}", expr_to_explain_string(expr)),
            UnaryOp::Neg => format!("-{}", expr_to_explain_string(expr)),
            UnaryOp::BitNot => format!("~{}", expr_to_explain_string(expr)),
        },
        Expr::Subquery(_) | Expr::Exists { .. } | Expr::InSubquery { .. } => "<expr>".to_string(),
        _ => crate::parser::expr_to_sql(expr),
    }
}

fn append_extra(extra: &mut String, item: &str) {
    if !extra.is_empty() {
        extra.push_str("; ");
    }
    extra.push_str(item);
}

fn null_string(value: String) -> String {
    if value.is_empty() {
        "NULL".to_string()
    } else {
        value
    }
}

pub(crate) fn plan_local_cost(plan: &PhysicalPlan) -> crate::engine::cbo::Cost {
    let children = plan_children(plan)
        .into_iter()
        .map(plan_cost)
        .collect::<Vec<_>>();
    plan_cost(plan).exclusive_of(&children)
}

pub(crate) fn plan_rel_estimate(plan: &PhysicalPlan) -> crate::engine::cbo::RelEstimate {
    use crate::engine::cbo::{EstimateSource, RelEstimate};

    let table = match plan {
        PhysicalPlan::SeqScan { table, .. }
        | PhysicalPlan::ZoneMapScan { table, .. }
        | PhysicalPlan::IndexLookup { table, .. }
        | PhysicalPlan::IndexMultiLookup { table, .. }
        | PhysicalPlan::IndexRangeScan { table, .. }
        | PhysicalPlan::IndexFastScan { table, .. }
        | PhysicalPlan::FulltextScan { table, .. }
        | PhysicalPlan::SpatialScan { table, .. }
        | PhysicalPlan::VectorIndexScan { table, .. }
        | PhysicalPlan::VectorBruteScan { table, .. }
        | PhysicalPlan::IndexInLookup { table, .. }
        | PhysicalPlan::IndexAggregate { table, .. }
        | PhysicalPlan::CoveringIndexAggregate { table, .. }
        | PhysicalPlan::ParallelSeqScan { table, .. }
        | PhysicalPlan::ParallelZoneMapScan { table, .. }
        | PhysicalPlan::ParallelIndexFastScan { table, .. }
        | PhysicalPlan::ParallelIndexRangeScan { table, .. }
        | PhysicalPlan::ParallelIndexAggregate { table, .. } => Some(table),
        _ => None,
    };
    if let Some(table) = table {
        let analyzed = table.schema.stats.analyzed_at_unix_ms.is_some();
        return RelEstimate::new(
            estimated_rows(plan),
            plan_output_width(plan),
            if analyzed {
                EstimateSource::Analyzed
            } else {
                EstimateSource::Fallback
            },
            table
                .schema
                .stats
                .confidence
                .unwrap_or(if analyzed { 0.9 } else { 0.25 }),
        );
    }

    if matches!(plan, PhysicalPlan::GenerateSeries { .. }) {
        return RelEstimate::new(
            estimated_rows(plan),
            plan_output_width(plan),
            EstimateSource::Exact,
            1.0,
        );
    }

    let children = plan_children(plan);
    let confidence = children
        .iter()
        .map(|child| plan_rel_estimate(child).confidence)
        .fold(1.0_f64, f64::min);
    RelEstimate::new(
        estimated_rows(plan),
        plan_output_width(plan),
        if children.is_empty() {
            EstimateSource::Fallback
        } else {
            EstimateSource::Derived
        },
        if children.is_empty() {
            0.25
        } else {
            confidence
        },
    )
}

pub(crate) fn plan_physical_properties(
    plan: &PhysicalPlan,
) -> crate::engine::cbo::PhysicalProperties {
    use crate::engine::cbo::{ExecutionMode, PhysicalProperties};

    let ordering = match plan {
        PhysicalPlan::Sort { order_by, .. }
        | PhysicalPlan::IncrementalSort { order_by, .. }
        | PhysicalPlan::TopK { order_by, .. }
        | PhysicalPlan::TopWindow { order_by, .. }
        | PhysicalPlan::ParallelTopK { order_by, .. }
        | PhysicalPlan::ParallelSort { order_by, .. }
        | PhysicalPlan::VectorIndexScan { order_by, .. }
        | PhysicalPlan::VectorBruteScan { order_by, .. } => order_by
            .iter()
            .map(|item| expr_to_explain_string(&item.expr))
            .collect(),
        PhysicalPlan::Exchange {
            output_order: ExchangeOrder::Sorted,
            merge_keys,
            ..
        } => merge_keys
            .iter()
            .map(|item| expr_to_explain_string(&item.expr))
            .collect(),
        _ => Vec::new(),
    };
    let parallel_degree = match plan {
        PhysicalPlan::Exchange { degree, .. }
        | PhysicalPlan::ParallelSeqScan { degree, .. }
        | PhysicalPlan::ParallelZoneMapScan { degree, .. }
        | PhysicalPlan::ParallelIndexFastScan { degree, .. }
        | PhysicalPlan::ParallelIndexRangeScan { degree, .. }
        | PhysicalPlan::ParallelIndexAggregate { degree, .. }
        | PhysicalPlan::ParallelHashAggregate { degree, .. }
        | PhysicalPlan::ParallelRollupAggregate { degree, .. }
        | PhysicalPlan::ParallelGroupingSetsAggregate { degree, .. }
        | PhysicalPlan::ParallelTopK { degree, .. }
        | PhysicalPlan::ParallelSort { degree, .. } => *degree,
        _ => 1,
    };
    let streaming_join_depth = match plan {
        PhysicalPlan::HashJoin {
            left,
            right,
            build_side,
            ..
        } => {
            let probe = match build_side {
                BuildSide::Right => left,
                BuildSide::Left => right,
            };
            1u8.saturating_add(plan_physical_properties(probe).streaming_join_depth)
        }
        _ => 0,
    };
    PhysicalProperties {
        ordering,
        execution_mode: ExecutionMode::Row,
        parallel_degree,
        streaming_join_depth,
        parameterized_by: Vec::new(),
        rewindable: matches!(
            plan,
            PhysicalPlan::Sort { .. }
                | PhysicalPlan::HashAggregate { .. }
                | PhysicalPlan::HashRollupAggregate { .. }
                | PhysicalPlan::HashGroupingSetsAggregate { .. }
        ),
    }
}

/// Final bottom-up guard after all physical rewrites and projection pruning.
///
/// Cardinality and width live exclusively on relational plan properties.
/// Every cost vector is validated before it can enter the plan
/// cache; non-finite/negative vectors are model errors, not sortable values.
fn synchronize_and_validate_plan_costs(plan: &mut PhysicalPlan) -> Result<(), QueryError> {
    for child in plan.children_mut() {
        synchronize_and_validate_plan_costs(child)?;
    }

    let cost = plan_cost_mut(plan);
    if !cost.is_valid() {
        return Err(error(
            crate::error::catalog::ER_UNKNOWN_ERROR,
            format!(
                "CBO cost model produced an invalid cost vector: startup={}, total={}",
                cost.startup, cost.total
            ),
        ));
    }
    Ok(())
}

/// Convenience wrapper around `plan_cost(plan).weighted_total()` used in
/// legacy arithmetic that still adds f64 cost values directly. Phase 2+
/// migrates these callers to compose [`Cost`] vectors instead.
#[inline]
pub(crate) fn plan_cost_total(plan: &PhysicalPlan) -> f64 {
    plan_cost(plan).weighted_total()
}

/// Split hash join equality keys and residual predicates out of ON/WHERE conditions.
///
/// Collect every `=`- and `<=>`-shaped sub-expression in the AND tree and classify by the following rules:
/// - one side references only left-plan columns while the other references only right-plan columns → equality key
/// - otherwise kept as residual, on which the execution layer applies the final filter after a join hit
///
/// Returns `None` when not even one equality key can be found (should fall back to NLJ).
pub(crate) fn extract_join_keys_for_plans(
    left: &PhysicalPlan,
    right: &PhysicalPlan,
    expr: &Expr,
) -> Option<(Vec<Expr>, Vec<Expr>, Vec<bool>, Option<Expr>)> {
    let left_tables = collect_physical_plan_tables(left);
    let right_tables = collect_physical_plan_tables(right);
    let mut conjuncts = Vec::new();
    split_conjuncts(expr, &mut conjuncts);
    let mut left_keys = Vec::new();
    let mut right_keys = Vec::new();
    let mut null_safe = Vec::new();
    let mut residuals: Vec<Expr> = Vec::new();
    for conj in conjuncts {
        if let Expr::Binary {
            left: a,
            op,
            right: b,
        } = &conj
        {
            let is_null_safe = *op == BinaryOp::NullSafeEq;
            if *op == BinaryOp::Eq || is_null_safe {
                let a_side = expr_side_purity(a, &left_tables, &right_tables);
                let b_side = expr_side_purity(b, &left_tables, &right_tables);
                if !expr_is_deterministic(a) || !expr_is_deterministic(b) {
                    residuals.push(conj);
                    continue;
                }
                match (a_side, b_side) {
                    (Side::Left, Side::Right) => {
                        if left_keys.iter().zip(&right_keys).zip(&null_safe).any(
                            |((prior_left, prior_right), prior_null_safe)| {
                                *prior_null_safe == is_null_safe
                                    && exprs_equivalent_in_plan(left, a, prior_left)
                                    && exprs_equivalent_in_plan(right, b, prior_right)
                            },
                        ) {
                            continue;
                        }
                        left_keys.push((**a).clone());
                        right_keys.push((**b).clone());
                        null_safe.push(is_null_safe);
                        continue;
                    }
                    (Side::Right, Side::Left) => {
                        if left_keys.iter().zip(&right_keys).zip(&null_safe).any(
                            |((prior_left, prior_right), prior_null_safe)| {
                                *prior_null_safe == is_null_safe
                                    && exprs_equivalent_in_plan(left, b, prior_left)
                                    && exprs_equivalent_in_plan(right, a, prior_right)
                            },
                        ) {
                            continue;
                        }
                        left_keys.push((**b).clone());
                        right_keys.push((**a).clone());
                        null_safe.push(is_null_safe);
                        continue;
                    }
                    _ => {}
                }
            }
        }
        residuals.push(conj);
    }
    if left_keys.is_empty() {
        return None;
    }
    for ((left_key, right_key), is_null_safe) in
        left_keys.iter_mut().zip(&mut right_keys).zip(&null_safe)
    {
        let left_type = crate::engine::executor::select::plan::physical_expr_type(left, left_key);
        let right_type =
            crate::engine::executor::select::plan::physical_expr_type(right, right_key);
        if crate::engine::expr_type::equality_key_needs_recheck(left_type.kind, right_type.kind) {
            residuals.push(Expr::Binary {
                left: Box::new(left_key.clone()),
                op: if *is_null_safe {
                    BinaryOp::NullSafeEq
                } else {
                    BinaryOp::Eq
                },
                right: Box::new(right_key.clone()),
            });
        }
        crate::engine::expr_type::normalize_equality_key_exprs(
            left_key,
            right_key,
            left_type.kind,
            right_type.kind,
        );
    }
    let residual = combine_conjuncts(residuals);
    Some((left_keys, right_keys, null_safe, residual))
}

/// The "side" of an expression: left-only, right-only, spanning both, or referencing no column at all.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Side {
    Left,
    Right,
    Both,
    Neither,
}

fn expr_side_purity(expr: &Expr, left_tables: &[PlanTable], right_tables: &[PlanTable]) -> Side {
    let mut needs_left = false;
    let mut needs_right = false;
    check_expr_table_sides(
        expr,
        left_tables,
        right_tables,
        &mut needs_left,
        &mut needs_right,
    );
    match (needs_left, needs_right) {
        (true, true) => Side::Both,
        (true, false) => Side::Left,
        (false, true) => Side::Right,
        (false, false) => Side::Neither,
    }
}

/// Rejects equality keys carrying non-deterministic functions or subqueries from serving as hash keys
/// (each evaluation may yield a different result, which would break build/probe consistency).
pub(crate) fn expr_is_deterministic(expr: &Expr) -> bool {
    crate::parser::visit::try_inspect_value_expr(expr, |node| {
        match node {
            Expr::Function { name, .. } => {
                let upper = name.to_ascii_uppercase();
                if matches!(
                    upper.as_str(),
                    "RAND"
                        | "SYSDATE"
                        | "NOW"
                        | "CURRENT_TIMESTAMP"
                        | "LOCALTIME"
                        | "LOCALTIMESTAMP"
                        | "CURDATE"
                        | "CURRENT_DATE"
                        | "CURTIME"
                        | "CURRENT_TIME"
                        | "UUID"
                        | "UUID_SHORT"
                        | "RANDOM_BYTES"
                        | "CONNECTION_ID"
                        | "LAST_INSERT_ID"
                        | "SLEEP"
                        | "USER"
                        | "CURRENT_USER"
                        | "SESSION_USER"
                        | "CURRENT_ROLE"
                        | "NEXTVAL"
                        | "CURRVAL"
                        | "LASTVAL"
                        | "SETVAL"
                        | "__SET_USER_VAR"
                ) {
                    return Err(());
                }
            }
            Expr::Subquery(_)
            | Expr::Exists { .. }
            | Expr::InSubquery { .. }
            | Expr::WindowFunction { .. }
            | Expr::UserVariable(_)
            | Expr::SystemVariable { .. }
            | Expr::QuantifiedComparison {
                right: QuantifiedComparisonRight::Subquery(_),
                ..
            } => return Err(()),
            _ => {}
        }
        Ok(true)
    })
    .is_ok()
}

fn expr_can_resolve_in_plan(expr: &Expr, plan: &PhysicalPlan) -> bool {
    match expr {
        Expr::Identifier(name) => plan_has_unqualified_column(plan, name),
        Expr::CompoundIdentifier(parts) => {
            let Some(column) = parts.last() else {
                return false;
            };
            let qualifier = if parts.len() >= 2 {
                Some(parts[parts.len() - 2].as_str())
            } else {
                None
            };
            plan_has_column(plan, qualifier, column)
        }
        _ => false,
    }
}

fn plan_has_unqualified_column(plan: &PhysicalPlan, column: &str) -> bool {
    let mut matches = 0;
    count_plan_column_matches(plan, None, column, &mut matches);
    matches == 1
}

fn plan_has_column(plan: &PhysicalPlan, qualifier: Option<&str>, column: &str) -> bool {
    let mut matches = 0;
    count_plan_column_matches(plan, qualifier, column, &mut matches);
    matches > 0
}

fn count_plan_column_matches(
    plan: &PhysicalPlan,
    qualifier: Option<&str>,
    column: &str,
    matches: &mut usize,
) {
    match plan {
        PhysicalPlan::NullExtend { input, table, .. } => {
            // The restored schema contains each original relation once;
            // do not count the hidden right input of the anti join twice.
            let mut output_tables = collect_physical_plan_tables(input);
            output_tables
                .retain(|t| !plan_table_matches(t, &table.schema.name, table.alias.as_deref()));
            output_tables.push(table.clone());
            for table in output_tables {
                if qualifier
                    .is_none_or(|q| matches_table(q, &table.schema.name, table.alias.as_deref()))
                    && table_has_column_name(&table, &normalize_expr_column(column))
                {
                    *matches += 1;
                }
            }
        }

        PhysicalPlan::ValuesScan { columns, .. } => {
            let table_match =
                qualifier.is_none_or(|qualifier| qualifier.eq_ignore_ascii_case("values"));
            if table_match && columns.iter().any(|c| c.name.eq_ignore_ascii_case(column)) {
                *matches += 1;
            }
        }
        PhysicalPlan::JsonTableScan {
            relation, columns, ..
        } => {
            let table_match =
                qualifier.is_none_or(|qualifier| qualifier.eq_ignore_ascii_case(relation));
            if table_match && columns.iter().any(|c| c.name.eq_ignore_ascii_case(column)) {
                *matches += 1;
            }
        }
        PhysicalPlan::SystemScan {
            relation, columns, ..
        } => {
            let table_match =
                qualifier.is_none_or(|qualifier| qualifier.eq_ignore_ascii_case(relation));
            if table_match && columns.iter().any(|c| c.name.eq_ignore_ascii_case(column)) {
                *matches += 1;
            }
        }
        PhysicalPlan::SeqScan { table, .. }
        | PhysicalPlan::ZoneMapScan { table, .. }
        | PhysicalPlan::IndexMerge { table, .. }
        | PhysicalPlan::IndexLookup { table, .. }
        | PhysicalPlan::IndexMultiLookup { table, .. }
        | PhysicalPlan::IndexRangeScan { table, .. }
        | PhysicalPlan::IndexFastScan { table, .. }
        | PhysicalPlan::FulltextScan { table, .. }
        | PhysicalPlan::SpatialScan { table, .. }
        | PhysicalPlan::VectorIndexScan { table, .. }
        | PhysicalPlan::VectorBruteScan { table, .. }
        | PhysicalPlan::IndexInLookup { table, .. }
        | PhysicalPlan::CoveringIndexAggregate { table, .. }
        | PhysicalPlan::ParallelSeqScan { table, .. }
        | PhysicalPlan::ParallelZoneMapScan { table, .. }
        | PhysicalPlan::ParallelIndexFastScan { table, .. }
        | PhysicalPlan::ParallelIndexRangeScan { table, .. }
        | PhysicalPlan::ParallelIndexAggregate { table, .. } => {
            let table_matches = qualifier.is_none_or(|qualifier| {
                matches_table(qualifier, &table.schema.name, table.alias.as_deref())
            });
            if table_matches
                && table
                    .schema
                    .columns
                    .iter()
                    .any(|schema_col| schema_col.name.eq_ignore_ascii_case(column))
            {
                *matches += 1;
            }
        }
        PhysicalPlan::NestedLoopJoin { left, right, .. }
        | PhysicalPlan::HashJoin { left, right, .. }
        | PhysicalPlan::AdaptiveJoin { left, right, .. } => {
            count_plan_column_matches(left, qualifier, column, matches);
            count_plan_column_matches(right, qualifier, column, matches);
        }
        // A set operation exposes only the left branch's output names; the
        // right branch contributes values/types but not independently
        // addressable columns.
        PhysicalPlan::SetOp { left, .. } => {
            count_plan_column_matches(left, qualifier, column, matches);
        }
        PhysicalPlan::RecursiveUnion { anchor, .. } => {
            count_plan_column_matches(anchor, qualifier, column, matches);
        }
        PhysicalPlan::IndexNestedLoopJoin {
            left, right_table, ..
        } => {
            count_plan_column_matches(left, qualifier, column, matches);
            let table_matches = qualifier.is_none_or(|qualifier| {
                matches_table(
                    qualifier,
                    &right_table.schema.name,
                    right_table.alias.as_deref(),
                )
            });
            if table_matches
                && right_table
                    .schema
                    .columns
                    .iter()
                    .any(|schema_col| schema_col.name.eq_ignore_ascii_case(column))
            {
                *matches += 1;
            }
        }
        PhysicalPlan::HashAggregate { input, .. }
        | PhysicalPlan::HashRollupAggregate { input, .. }
        | PhysicalPlan::HashGroupingSetsAggregate { input, .. }
        | PhysicalPlan::Sort { input, .. }
        | PhysicalPlan::IncrementalSort { input, .. }
        | PhysicalPlan::TopK { input, .. }
        | PhysicalPlan::TopWindow { input, .. }
        | PhysicalPlan::ParallelTopK { input, .. }
        | PhysicalPlan::Projection { input, .. }
        | PhysicalPlan::Limit { input, .. }
        | PhysicalPlan::WindowAgg { input, .. }
        | PhysicalPlan::StreamingAggregate { input, .. }
        | PhysicalPlan::StreamingRollupAggregate { input, .. }
        | PhysicalPlan::StreamingGroupingSetsAggregate { input, .. }
        | PhysicalPlan::StreamingDistinct { input, .. }
        | PhysicalPlan::Exchange { input, .. }
        | PhysicalPlan::ParallelHashAggregate { input, .. }
        | PhysicalPlan::ParallelRollupAggregate { input, .. }
        | PhysicalPlan::ParallelGroupingSetsAggregate { input, .. }
        | PhysicalPlan::Filter { input, .. }
        | PhysicalPlan::Apply { outer: input, .. }
        | PhysicalPlan::Materialize { input, .. }
        | PhysicalPlan::ParallelSort { input, .. } => {
            count_plan_column_matches(input, qualifier, column, matches);
        }
        PhysicalPlan::CteScan {
            name,
            alias,
            columns,
            ..
        } => {
            // alias-shadows-name (see `matches_table`).
            let table_match = qualifier
                .map(|q| match alias.as_deref() {
                    Some(a) => a.eq_ignore_ascii_case(q),
                    None => q.eq_ignore_ascii_case(name),
                })
                .unwrap_or(true);
            if table_match && columns.iter().any(|c| c.name.eq_ignore_ascii_case(column)) {
                *matches += 1;
            }
        }
        PhysicalPlan::GenerateSeries {
            relation, columns, ..
        } => {
            let table_match = qualifier
                .map(|q| q.eq_ignore_ascii_case(relation))
                .unwrap_or(true);
            if table_match && columns.iter().any(|c| c.name.eq_ignore_ascii_case(column)) {
                *matches += 1;
            }
        }
        PhysicalPlan::IndexAggregate {
            table,
            output_alias,
            ..
        } => {
            let table_matches = qualifier.is_none_or(|qualifier| {
                matches_table(qualifier, &table.schema.name, table.alias.as_deref())
            });
            if table_matches && output_alias.eq_ignore_ascii_case(column) {
                *matches += 1;
            }
        }
    }
}

fn split_conjuncts(expr: &Expr, out: &mut Vec<Expr>) {
    match expr {
        Expr::Binary { left, op, right } if *op == BinaryOp::And => {
            split_conjuncts(left, out);
            split_conjuncts(right, out);
        }
        other => out.push(other.clone()),
    }
}

fn expr_has_volatile_evaluation(expr: &Expr) -> bool {
    // Volatility is query-block scoped: an uncorrelated scalar subquery is
    // materialized once, even when its body uses RAND. Do not disable the
    // outer ordered range lookup for that statement-scoped value.
    match expr {
        Expr::Function { name, args, .. } => {
            let function = Expr::Function {
                name: name.clone(),
                args: Vec::new(),
                distinct: false,
            };
            !expr_is_deterministic(&function) || args.iter().any(expr_has_volatile_evaluation)
        }
        Expr::UserVariable(_) | Expr::SystemVariable { .. } => true,
        Expr::Unary { expr, .. }
        | Expr::IsNull(expr)
        | Expr::IsNotNull(expr)
        | Expr::GroupKeyRef { expr, .. }
        | Expr::InSubquery { expr, .. } => expr_has_volatile_evaluation(expr),
        Expr::Binary { left, right, .. }
        | Expr::Like {
            expr: left,
            pattern: right,
            ..
        } => expr_has_volatile_evaluation(left) || expr_has_volatile_evaluation(right),
        Expr::InList { expr, list, .. } => {
            expr_has_volatile_evaluation(expr) || list.iter().any(expr_has_volatile_evaluation)
        }
        Expr::Between {
            expr, low, high, ..
        } => [expr, low, high]
            .into_iter()
            .any(|e| expr_has_volatile_evaluation(e)),
        Expr::Case {
            operand,
            conditions,
            results,
            else_result,
        } => {
            operand
                .iter()
                .chain(else_result.iter())
                .any(|e| expr_has_volatile_evaluation(e))
                || conditions
                    .iter()
                    .chain(results.iter())
                    .any(expr_has_volatile_evaluation)
        }
        Expr::Tuple(items) => items.iter().any(expr_has_volatile_evaluation),
        Expr::QuantifiedComparison { left, right, .. } => {
            expr_has_volatile_evaluation(left)
                || matches!(right, QuantifiedComparisonRight::List(items) if items.iter().any(expr_has_volatile_evaluation))
        }
        Expr::WindowFunction { .. } => true,
        _ => false,
    }
}

fn extract_table_predicate(
    db: &Database,
    predicates: &mut Vec<Expr>,
    table: &PlanTable,
) -> Option<Expr> {
    let mut taken = Vec::new();
    let mut remaining = Vec::new();
    for predicate in predicates.drain(..) {
        if !expr_has_volatile_evaluation(&predicate)
            && expr_can_resolve_in_table(db, &predicate, table)
        {
            taken.push(predicate);
        } else {
            remaining.push(predicate);
        }
    }
    *predicates = remaining;
    combine_conjuncts(taken)
}

/// Disjunctive predicate decomposition: for a top-level OR predicate
/// `(A1 AND A2 AND ...) OR (B1 AND B2 AND ...) OR ...`, derive a per-table
/// over-approximation `or(A_T, B_T, ...)` whenever every disjunct contains
/// at least one conjunct that resolves only in table T. The original OR
/// predicate is retained in the residual list so cross-table relationships
/// remain enforced after joins; the derived predicate is pushed down to T's
/// base scan as an extra filter, drastically shrinking the join inputs when
/// the original OR effectively constrains a single table to a handful of
/// values (e.g. TPC-H Q7's nation pair).
fn derive_or_pushdown_predicates(
    db: &Database,
    predicates: &[Expr],
    tables: &[PlanTable],
) -> Vec<Expr> {
    let mut derived = Vec::new();
    for pred in predicates {
        if expr_has_volatile_evaluation(pred) {
            continue;
        }
        let disjuncts = split_disjuncts_top(pred);
        if disjuncts.len() < 2 {
            continue;
        }
        for table in tables {
            if expr_can_resolve_in_table(db, pred, table) {
                continue;
            }
            let mut per_branch: Vec<Expr> = Vec::with_capacity(disjuncts.len());
            let mut ok = true;
            for branch in &disjuncts {
                let mut conjs = Vec::new();
                split_conjuncts(branch, &mut conjs);
                let table_only: Vec<Expr> = conjs
                    .into_iter()
                    .filter(|c| expr_can_resolve_in_table(db, c, table))
                    .collect();
                let Some(combined) = combine_conjuncts(table_only) else {
                    ok = false;
                    break;
                };
                per_branch.push(combined);
            }
            if ok {
                if let Some(d) = combine_disjuncts(per_branch) {
                    derived.push(d);
                }
            }
        }
    }
    derived
}

/// Derive `column IN (union-of-branch-values)` for equality/IN domains that
/// occur in every OR branch.  The cap prevents a generated predicate from
/// becoming larger than the expression it is intended to accelerate.
fn derive_or_column_domain_unions(branches: &[Vec<Expr>]) -> Vec<Expr> {
    const MAX_DERIVED_DOMAIN_VALUES: usize = 256;
    if branches.len() < 2 {
        return Vec::new();
    }

    type Domains = std::collections::BTreeMap<Vec<String>, (Expr, Vec<Expr>)>;
    fn branch_domains(conjuncts: &[Expr]) -> Domains {
        let mut domains = Domains::new();
        for conjunct in conjuncts {
            let (column, values) = match conjunct {
                Expr::Binary {
                    left,
                    op: BinaryOp::Eq,
                    right,
                } if expr_column_name(left).is_some()
                    && expr_to_plan_constraint(right).is_some() =>
                {
                    ((**left).clone(), vec![(**right).clone()])
                }
                Expr::Binary {
                    left,
                    op: BinaryOp::Eq,
                    right,
                } if expr_column_name(right).is_some()
                    && expr_to_plan_constraint(left).is_some() =>
                {
                    ((**right).clone(), vec![(**left).clone()])
                }
                Expr::InList {
                    expr,
                    list,
                    negated: false,
                } if expr_column_name(expr).is_some()
                    && list
                        .iter()
                        .all(|value| expr_to_plan_constraint(value).is_some()) =>
                {
                    ((**expr).clone(), list.clone())
                }
                _ => continue,
            };
            let Some(key) = qualified_column_identity(&column) else {
                continue;
            };
            let entry = domains.entry(key).or_insert_with(|| (column, Vec::new()));
            for value in values {
                if !entry.1.iter().any(|existing| existing == &value) {
                    entry.1.push(value);
                }
            }
        }
        domains
    }

    let per_branch: Vec<Domains> = branches
        .iter()
        .map(|branch| branch_domains(branch))
        .collect();
    let mut derived = Vec::new();
    for (key, (column, first_values)) in &per_branch[0] {
        if first_values.is_empty()
            || !per_branch[1..]
                .iter()
                .all(|branch| branch.contains_key(key))
        {
            continue;
        }
        let mut values = Vec::new();
        for branch in &per_branch {
            let Some((_, branch_values)) = branch.get(key) else {
                values.clear();
                break;
            };
            for value in branch_values {
                if !values.iter().any(|existing| existing == value) {
                    values.push(value.clone());
                    if values.len() > MAX_DERIVED_DOMAIN_VALUES {
                        break;
                    }
                }
            }
            if values.len() > MAX_DERIVED_DOMAIN_VALUES {
                break;
            }
        }
        if values.is_empty() || values.len() > MAX_DERIVED_DOMAIN_VALUES {
            continue;
        }
        derived.push(Expr::InList {
            expr: Box::new(column.clone()),
            list: values,
            negated: false,
        });
    }
    derived
}

fn split_disjuncts_top(expr: &Expr) -> Vec<Expr> {
    let mut out = Vec::new();
    fn walk(expr: &Expr, out: &mut Vec<Expr>) {
        match expr {
            Expr::Binary { left, op, right } if *op == BinaryOp::Or => {
                walk(left, out);
                walk(right, out);
            }
            other => out.push(other.clone()),
        }
    }
    walk(expr, &mut out);
    out
}

fn combine_disjuncts(mut predicates: Vec<Expr>) -> Option<Expr> {
    let mut expr = predicates.pop()?;
    while let Some(left) = predicates.pop() {
        expr = Expr::Binary {
            left: Box::new(left),
            op: BinaryOp::Or,
            right: Box::new(expr),
        };
    }
    Some(expr)
}

/// Distributive factoring of shared conjuncts out of a top-level OR:
/// `(K ∧ a) ∨ (K ∧ b) ∨ (K ∧ c)` → `K ∧ (a ∨ b ∨ c)`. The shared conjuncts
/// `K` (always the same in every disjunct, by structural equality) become
/// standalone AND predicates the planner can use as hash-join keys and scan
/// filters, instead of staying trapped inside the OR where the join planner
/// can only apply them as a nested-loop residual after the referenced tables
/// have already been cross-joined.
///
/// This is what lets TPC-DS q13 — whose equi-join keys (`cd_demo_sk =
/// ss_cdemo_sk`, `ss_hdemo_sk = hd_demo_sk`, `ss_addr_sk = ca_address_sk`,
/// `ca_country = 'United States'`) appear only inside three-way ORs — join via
/// hash joins instead of materialising a ~25-trillion-row cartesian (a 35 GB
/// single allocation). Applied per top-level AND conjunct; a no-op when the
/// disjuncts share nothing. The rewrite is a boolean tautology, so it is safe
/// in any predicate position (WHERE or ON, inner or outer join).
fn factor_common_or_conjuncts(expr: Expr) -> Expr {
    let mut conjuncts = Vec::new();
    split_conjuncts(&expr, &mut conjuncts);
    let factored: Vec<Expr> = conjuncts.into_iter().map(factor_one_top_level_or).collect();
    combine_conjuncts(factored).unwrap_or(expr)
}

fn factor_one_top_level_or(conj: Expr) -> Expr {
    let disjuncts = split_disjuncts_top(&conj);
    if disjuncts.len() < 2 {
        return conj; // not a top-level OR — nothing to factor
    }
    // Conjuncts of each disjunct branch.
    let branches: Vec<Vec<Expr>> = disjuncts
        .iter()
        .map(|d| {
            let mut v = Vec::new();
            split_conjuncts(d, &mut v);
            v
        })
        .collect();
    // Conjuncts present (by structural equality) in every branch.
    let mut common: Vec<Expr> = Vec::new();
    for cand in &branches[0] {
        if common.iter().any(|c| c == cand) {
            continue;
        }
        if branches.iter().all(|b| b.iter().any(|c| c == cand)) {
            common.push(cand.clone());
        }
    }
    if common.is_empty() {
        return conj;
    }
    // Surface the shared conjuncts as standalone AND predicates so the planner
    // can use the shared equi-join keys as join keys (and any shared filters as
    // scan filters), but KEEP the original OR intact rather than rebuilding a
    // residual-only `(a ∨ b ∨ c)`. The original OR is a multi-table predicate;
    // the planner's existing handling floats it up to the join where all its
    // tables are in scope and applies it there as the final residual. A
    // residual-only OR built here would instead be stranded on an earlier join
    // whose scope is missing some of its columns (observed as `1054 unknown
    // column` on TPC-DS q13). `K ∧ (original OR)` is logically equivalent to the
    // original OR and keeps the redundant copies of `K` harmless.
    common.push(conj);
    combine_conjuncts(common).unwrap_or(Expr::Literal(crate::parser::Literal::Bool(true)))
}

fn expr_can_resolve_in_table(db: &Database, expr: &Expr, table: &PlanTable) -> bool {
    match expr {
        Expr::Identifier(name) => table
            .schema
            .columns
            .iter()
            .any(|column| column.name.eq_ignore_ascii_case(name)),
        Expr::CompoundIdentifier(parts) => {
            let Some(column) = parts.last() else {
                return false;
            };
            let qualifier = if parts.len() >= 2 {
                Some(parts[parts.len() - 2].as_str())
            } else {
                None
            };
            qualifier.is_none_or(|qualifier| {
                matches_table(qualifier, &table.schema.name, table.alias.as_deref())
            }) && table
                .schema
                .columns
                .iter()
                .any(|schema_col| schema_col.name.eq_ignore_ascii_case(column))
        }
        Expr::Literal(_)
        | Expr::BoundValue(_)
        | Expr::Default(_)
        | Expr::DefaultQualified(_)
        | Expr::Placeholder(_)
        | Expr::Wildcard
        | Expr::Grouping(_)
        | Expr::UserVariable(_)
        | Expr::SystemVariable { .. } => true,
        Expr::Tuple(items) => items
            .iter()
            .all(|item| expr_can_resolve_in_table(db, item, table)),
        Expr::GroupKeyRef { expr, .. } => expr_can_resolve_in_table(db, expr, table),
        Expr::Unary { expr, .. } | Expr::IsNull(expr) | Expr::IsNotNull(expr) => {
            expr_can_resolve_in_table(db, expr, table)
        }
        Expr::Binary { left, right, .. } => {
            expr_can_resolve_in_table(db, left, table)
                && expr_can_resolve_in_table(db, right, table)
        }
        Expr::InList { expr, list, .. } => {
            expr_can_resolve_in_table(db, expr, table)
                && list
                    .iter()
                    .all(|item| expr_can_resolve_in_table(db, item, table))
        }
        // A subquery can only be safely pushed down to `table`'s scan
        // when its body has no correlated references to *other* tables.
        // Returning `true` unconditionally lets a predicate like
        //   `EXISTS (SELECT * FROM lineitem l2 WHERE l2.x = l1.y)`
        // (where l1 is a *different* table in the outer FROM) get
        // attached as a filter on the wrong scan, which then evaluates
        // the subquery against rows that don't carry l1's columns and
        // silently returns no matches.
        Expr::InSubquery { expr, subquery, .. } => {
            expr_can_resolve_in_table(db, expr, table)
                && subquery_correlation_only_uses_table(db, subquery, table)
        }
        Expr::QuantifiedComparison { left, right, .. } => {
            expr_can_resolve_in_table(db, left, table)
                && match right {
                    QuantifiedComparisonRight::Subquery(subquery) => {
                        subquery_correlation_only_uses_table(db, subquery, table)
                    }
                    QuantifiedComparisonRight::List(list) => list
                        .iter()
                        .all(|item| expr_can_resolve_in_table(db, item, table)),
                }
        }
        Expr::Subquery(subquery) | Expr::Exists { subquery, .. } => {
            subquery_correlation_only_uses_table(db, subquery, table)
        }
        Expr::Between {
            expr, low, high, ..
        } => {
            expr_can_resolve_in_table(db, expr, table)
                && expr_can_resolve_in_table(db, low, table)
                && expr_can_resolve_in_table(db, high, table)
        }
        Expr::Function { args, .. } => args
            .iter()
            .all(|arg| expr_can_resolve_in_table(db, arg, table)),
        Expr::Like { expr, pattern, .. } => {
            expr_can_resolve_in_table(db, expr, table)
                && expr_can_resolve_in_table(db, pattern, table)
        }
        Expr::Case {
            operand,
            conditions,
            results,
            else_result,
        } => {
            operand
                .as_ref()
                .map_or(true, |e| expr_can_resolve_in_table(db, e, table))
                && conditions
                    .iter()
                    .all(|c| expr_can_resolve_in_table(db, c, table))
                && results
                    .iter()
                    .all(|r| expr_can_resolve_in_table(db, r, table))
                && else_result
                    .as_ref()
                    .map_or(true, |e| expr_can_resolve_in_table(db, e, table))
        }
        Expr::WindowFunction { args, spec, .. } => {
            let crate::parser::WindowSpec {
                partition_by,
                order_by,
                ..
            } = spec.as_ref();

            args.iter().all(|a| expr_can_resolve_in_table(db, a, table))
                && partition_by
                    .iter()
                    .all(|e| expr_can_resolve_in_table(db, e, table))
                && order_by
                    .iter()
                    .all(|o| expr_can_resolve_in_table(db, &o.expr, table))
        }
    }
}

/// True when every correlated reference in `subquery` is to a column on
/// `table`. Uncorrelated subqueries trivially satisfy this. Used to gate
/// push-down of a predicate containing a subquery onto `table`'s scan —
/// other-table correlations would not resolve once the predicate runs
/// without those tables in scope.
fn subquery_correlation_only_uses_table(
    db: &Database,
    subquery: &Select,
    table: &PlanTable,
) -> bool {
    let mut sub_tables: HashSet<String> = HashSet::new();
    let mut sub_columns: HashSet<String> = HashSet::new();
    // Derived output names belong to this lexical scope. Validate every
    // producer branch separately before admitting those names; an outer
    // reference hidden inside a derived body must not migrate to another scan.
    let mut derived_sources = Vec::new();
    if let Some(source) = &subquery.derived {
        let Some(reference) = subquery.from.first() else {
            return false;
        };
        derived_sources.push((reference, source));
    }
    for join in &subquery.joins {
        if let Some(source) = &join.derived {
            if join.lateral {
                return false;
            }
            derived_sources.push((&join.table, source));
        }
    }
    for (reference, source) in &derived_sources {
        if !derived_correlation_only_uses_table(db, source, table) {
            return false;
        }
        let Some(select) = source.leftmost_select() else {
            return false;
        };
        let Ok(columns) = query_output_column_names(db, select) else {
            return false;
        };
        sub_tables.insert(
            reference
                .alias
                .as_deref()
                .unwrap_or(&reference.name)
                .to_ascii_lowercase(),
        );
        sub_columns.extend(columns.iter().map(|name| normalize_expr_column(name)));
    }
    if let Some(plan_tables) = subquery_plan_tables(db, subquery) {
        for sub_table in plan_tables {
            sub_tables.insert(sub_table.schema.name.to_ascii_lowercase());
            if let Some(alias) = &sub_table.alias {
                sub_tables.insert(alias.to_ascii_lowercase());
            }
            if let Some(bare) = sub_table.schema.name.rsplit('.').next() {
                sub_tables.insert(bare.to_ascii_lowercase());
            }
            for column in &sub_table.schema.columns {
                sub_columns.insert(normalize_expr_column(&column.name));
            }
        }
    } else if !derived_sources.is_empty() {
        for reference in subquery
            .from
            .iter()
            .chain(subquery.joins.iter().map(|join| &join.table))
        {
            if derived_sources
                .iter()
                .any(|(derived, _)| std::ptr::eq(*derived, reference))
            {
                continue;
            }
            let Ok(source) = plan_table(db, reference) else {
                return false;
            };
            sub_tables.insert(
                reference
                    .alias
                    .as_deref()
                    .unwrap_or(&reference.name)
                    .to_ascii_lowercase(),
            );
            sub_columns.extend(
                source
                    .schema
                    .columns
                    .iter()
                    .map(|column| normalize_expr_column(&column.name)),
            );
        }
    }
    let table_names: HashSet<String> = {
        let mut s = HashSet::new();
        s.insert(table.schema.name.to_ascii_lowercase());
        if let Some(alias) = &table.alias {
            s.insert(alias.to_ascii_lowercase());
        }
        s
    };
    let table_columns: HashSet<String> = table
        .schema
        .columns
        .iter()
        .map(|column| normalize_expr_column(&column.name))
        .collect();
    let mut ok = true;
    walk_subquery_expr(subquery, &mut |expr| {
        if !ok {
            return;
        }
        match expr {
            Expr::CompoundIdentifier(parts) if parts.len() >= 2 => {
                let qualifier = parts[parts.len() - 2].to_ascii_lowercase();
                if !sub_tables.contains(&qualifier) && !table_names.contains(&qualifier) {
                    ok = false;
                }
            }
            Expr::Identifier(name) => {
                let column = normalize_expr_column(name);
                if !sub_columns.contains(&column) && !table_columns.contains(&column) {
                    ok = false;
                }
            }
            _ => {}
        }
    });
    ok
}

fn derived_correlation_only_uses_table(
    db: &Database,
    source: &crate::parser::QueryExpr,
    table: &PlanTable,
) -> bool {
    match source {
        crate::parser::QueryExpr::Select(select) => {
            let mut volatile = false;
            walk_subquery_expr(select, &mut |expr| {
                if !volatile {
                    volatile = match expr {
                        Expr::WindowFunction { args, spec, .. } => {
                            let crate::parser::WindowSpec {
                                partition_by,
                                order_by,
                                frame,
                                ..
                            } = spec.as_ref();

                            // Window evaluation remains inside this producer;
                            // it is not being pushed into the outer scan.
                            // Explicit frame bounds need their own lexical
                            // proof, so keep them on the existing route.
                            frame.is_some()
                                || args
                                    .iter()
                                    .chain(partition_by)
                                    .any(expr_has_volatile_evaluation)
                                || order_by
                                    .iter()
                                    .any(|order| expr_has_volatile_evaluation(&order.expr))
                        }
                        _ => expr_has_volatile_evaluation(expr),
                    };
                }
            });
            !volatile && subquery_correlation_only_uses_table(db, select, table)
        }
        crate::parser::QueryExpr::SetOp(set) => {
            set.order_by.is_empty()
                && derived_correlation_only_uses_table(db, &set.left, table)
                && derived_correlation_only_uses_table(db, &set.right, table)
        }
        crate::parser::QueryExpr::WithCte(_) => false,
    }
}

fn walk_subquery_expr(select: &Select, f: &mut impl FnMut(&Expr)) {
    for item in &select.projection {
        if let SelectItem::Expr { expr, .. } = item {
            walk_expr_for_idents(expr, f);
        }
    }
    if let Some(e) = &select.selection {
        walk_expr_for_idents(e, f);
    }
    if let Some(e) = &select.having {
        walk_expr_for_idents(e, f);
    }
    for j in &select.joins {
        if let Some(c) = &j.condition {
            walk_expr_for_idents(c, f);
        }
    }
    for e in &select.group_by {
        walk_expr_for_idents(e, f);
    }
    for ob in &select.order_by {
        walk_expr_for_idents(&ob.expr, f);
    }
}

/// Planner value-expression traversal. Nested queries expose their value
/// clauses, while FROM/CTE definitions and window frames remain separate
/// scopes. The callback controls semantic pruning (aggregate, fulltext, etc.).
fn inspect_query_value_expr(expr: &Expr, inspect: &mut impl FnMut(&Expr) -> bool) {
    struct Inspect<'a, F>(&'a mut F, bool);
    impl<F: FnMut(&Expr) -> bool> crate::parser::visit::AstVisitor for Inspect<'_, F> {
        fn enter_expr(&mut self, expr: &Expr) {
            self.1 = (self.0)(expr);
        }
        fn walk_expr_children(&self, _: &Expr) -> bool {
            self.1
        }
        fn enter_select(&mut self, select: &Select) {
            use crate::parser::visit::visit_expr;
            for item in &select.projection {
                if let SelectItem::Expr { expr, .. } = item {
                    visit_expr(self, expr);
                }
            }
            for expr in select.selection.iter().chain(select.having.iter()) {
                visit_expr(self, expr);
            }
            for join in &select.joins {
                if let Some(expr) = &join.condition {
                    visit_expr(self, expr);
                }
            }
            for expr in &select.group_by {
                visit_expr(self, expr);
            }
            for order in &select.order_by {
                visit_expr(self, &order.expr);
            }
        }
        fn walk_select_children(&self, _: &Select) -> bool {
            false
        }
        fn walk_window_frame_children(&self, _: &crate::parser::WindowFrame) -> bool {
            false
        }
        fn case_results_interleaved(&self) -> bool {
            false
        }
    }
    crate::parser::visit::visit_expr(&mut Inspect(inspect, true), expr);
}

/// Visit column references that are evaluated outside aggregate arguments.
///
/// This is used when attributing correlated references to a grouped outer
/// query block.  An aggregate owned by the nested block consumes its argument
/// per nested input row, so that argument is not an outer block's hidden
/// non-grouped expression.  References in the nested block's predicates,
/// GROUP BY and ORDER BY remain visible to the caller.
fn walk_expr_for_nonaggregate_idents(expr: &Expr, f: &mut impl FnMut(&Expr)) {
    inspect_query_value_expr(expr, &mut |expr| {
        if matches!(expr, Expr::Identifier(_) | Expr::CompoundIdentifier(_)) {
            f(expr);
        }
        !matches!(expr, Expr::Function { name, .. } if is_aggregate_function(name))
    });
}

pub(crate) fn walk_expr_for_idents(expr: &Expr, f: &mut impl FnMut(&Expr)) {
    inspect_query_value_expr(expr, &mut |expr| {
        f(expr);
        !matches!(expr, Expr::Tuple(_))
    });
}

pub(crate) fn combine_conjuncts(predicates: Vec<Expr>) -> Option<Expr> {
    crate::parser::boolean::balanced_boolean_chain(predicates, BinaryOp::And)
}

fn combine_optional(left: Option<Expr>, right: Expr) -> Option<Expr> {
    Some(match left {
        None => right,
        Some(left) => Expr::Binary {
            left: Box::new(left),
            op: BinaryOp::And,
            right: Box::new(right),
        },
    })
}

fn combine_adaptive_candidate_predicate(
    candidate: AdaptiveJoinCandidate,
    predicate: Expr,
) -> AdaptiveJoinCandidate {
    match candidate {
        AdaptiveJoinCandidate::Hash {
            left_keys,
            right_keys,
            null_safe,
            build_side,
            residual,
            estimated_rows,
            cost,
        } => AdaptiveJoinCandidate::Hash {
            left_keys,
            right_keys,
            null_safe,
            build_side,
            residual: combine_optional(residual, predicate),
            estimated_rows,
            cost,
        },
        AdaptiveJoinCandidate::IndexNestedLoop {
            right_table,
            right_index,
            condition,
            eq_prefix_len,
            estimated_rows,
            cost,
            right_covering,
            right_projected_columns,
        } => AdaptiveJoinCandidate::IndexNestedLoop {
            right_table,
            right_index,
            condition: combine_optional(condition, predicate),
            eq_prefix_len,
            estimated_rows,
            cost,
            right_covering,
            right_projected_columns,
        },
        AdaptiveJoinCandidate::NestedLoop {
            condition,
            estimated_rows,
            cost,
        } => AdaptiveJoinCandidate::NestedLoop {
            condition: combine_optional(condition, predicate),
            estimated_rows,
            cost,
        },
    }
}

/// Phase A: walk projection / HAVING / ORDER BY in `select` and collect
/// every aggregate function call (deduplicated by semantic expression identity).
/// The list is attached to `PhysicalPlan::HashAggregate` so the executor
/// can build per-group accumulators without retaining input rows.
pub(crate) fn collect_aggregate_calls(select: &Select) -> Vec<Expr> {
    let mut seen: AggregateCallSet = HashMap::new();
    let mut out: Vec<Expr> = Vec::new();
    for item in &select.projection {
        if let SelectItem::Expr { expr, .. } = item {
            walk_collect_aggregates(expr, &mut seen, &mut out);
        }
    }
    if let Some(having) = &select.having {
        walk_collect_aggregates(having, &mut seen, &mut out);
    }
    for ob in &select.order_by {
        walk_collect_aggregates(&ob.expr, &mut seen, &mut out);
    }
    append_hidden_group_column_aggregates(select, &mut out);
    out
}

/// Include aggregates whose arguments bind entirely to this query block
/// even though the call is written inside a correlated scalar subquery.
fn collect_aggregate_calls_scoped(db: &Database, select: &Select) -> Vec<Expr> {
    let mut calls = collect_aggregate_calls(select);
    if !crate::parser::visit::select_body_contains_subquery(select) {
        return calls;
    }
    let Ok(outer) = build_column_scope(db, select, None) else {
        return calls;
    };
    struct Collector<'a> {
        db: &'a Database,
        outer: &'a ColumnScope<'a>,
        scopes: Vec<ColumnScope<'static>>,
        calls: &'a mut Vec<Expr>,
    }
    impl crate::parser::visit::AstVisitor for Collector<'_> {
        fn enter_select(&mut self, select: &Select) {
            self.scopes.push(
                build_column_scope(self.db, select, None).unwrap_or(ColumnScope {
                    tables: Vec::new(),
                    outer: None,
                }),
            );
        }
        fn exit_select(&mut self, _: &Select) {
            self.scopes.pop();
        }
        fn enter_expr(&mut self, expr: &Expr) {
            if self.scopes.is_empty()
                || !matches!(expr, Expr::Function { name, .. } if is_aggregate_function(name))
            {
                return;
            }
            let mut references = 0usize;
            let mut owned = true;
            crate::parser::visit::inspect_value_expr(
                expr,
                |node| {
                    let (qualifier, column) = match node {
                        Expr::Identifier(name) => (None, name.as_str()),
                        Expr::CompoundIdentifier(parts) if parts.len() >= 2 => (
                            Some(parts[parts.len() - 2].as_str()),
                            parts.last().unwrap().as_str(),
                        ),
                        Expr::Subquery(_) | Expr::Exists { .. } | Expr::InSubquery { .. } => {
                            owned = false;
                            return false;
                        }
                        _ => return true,
                    };
                    references += 1;
                    owned &= self.outer.column_matches(qualifier, column, false) == 1
                        && self
                            .scopes
                            .iter()
                            .all(|scope| scope.column_matches(qualifier, column, false) == 0);
                    false
                },
                |_| {},
            );
            if owned
                && references > 0
                && !self
                    .calls
                    .iter()
                    .any(|call| expr_semantically_eq(call, expr))
            {
                self.calls.push(expr.clone());
            }
        }
    }
    let mut collector = Collector {
        db,
        outer: &outer,
        scopes: Vec::new(),
        calls: &mut calls,
    };
    for item in &select.projection {
        if let SelectItem::Expr { expr, .. } = item {
            crate::parser::visit::visit_expr(&mut collector, expr);
        }
    }
    if let Some(expr) = &select.having {
        crate::parser::visit::visit_expr(&mut collector, expr);
    }
    for order in &select.order_by {
        crate::parser::visit::visit_expr(&mut collector, &order.expr);
    }
    calls
}

/// MySQL hidden column semantics (empty sql_mode, i.e. ONLY_FULL_GROUP_BY off): a column reference
/// appearing in an aggregate query's projection / HAVING / ORDER BY that is "non-aggregated and not
/// in GROUP BY" takes its value from some row of the group (the first row in this implementation).
/// One `__hidden_first_value(col)` aggregate slot is appended per such column reference, accumulating
/// row by row with the other aggregates on every execution path (generic / fused / parallel /
/// streaming / rollup); when such a reference is evaluated on an aggregate output row and the field
/// is missing or NULL-placed, it looks up the agg cache by the slot's fingerprint (see
/// `executor::select::eval::hidden_group_cache_value`). Vectorized aggregation does not recognize the function name and falls back to the row path automatically.
///
/// Shapes that are left unwrapped (existing behavior preserved):
/// - columns inside aggregate-function arguments (evaluated per row anyway);
/// - subtrees exactly equal to some GROUP BY expression (the grouping key itself);
/// - window functions and subquery bodies (`Subquery`/`Exists` etc., evaluated at a different time);
/// - bare identifiers in ORDER BY / HAVING sharing a name with a select alias (resolved as the
///   alias, not a table column).
fn append_hidden_group_column_aggregates(select: &Select, out: &mut Vec<Expr>) {
    // Only aggregate queries have the hidden column notion: explicit GROUP BY / GROUPING SETS, or
    // projection/HAVING/ORDER BY containing aggregate calls (implicit single group).
    if out.is_empty() && select.group_by.is_empty() && select.grouping_sets.is_empty() {
        return;
    }
    let order_having_aliases = projection_aliases(select);
    let no_aliases: HashSet<String> = HashSet::new();
    let mut seen_keys: HashSet<u64> = HashSet::new();
    let mut hidden: Vec<Expr> = Vec::new();
    for item in &select.projection {
        if let SelectItem::Expr { expr, .. } = item {
            collect_hidden_group_columns(expr, select, &no_aliases, &mut seen_keys, &mut hidden);
        }
    }
    if let Some(having) = &select.having {
        collect_hidden_group_columns(
            having,
            select,
            &order_having_aliases,
            &mut seen_keys,
            &mut hidden,
        );
    }
    for ob in &select.order_by {
        collect_hidden_group_columns(
            &ob.expr,
            select,
            &order_having_aliases,
            &mut seen_keys,
            &mut hidden,
        );
    }
    for col in hidden {
        out.push(Expr::Function {
            name: crate::parser::HIDDEN_FIRST_VALUE_FN.to_string(),
            args: vec![col],
            distinct: false,
        });
    }
}

/// Lowercased bare column name of a column-reference expression (the last
/// identifier component), or None for anything else.
fn column_bare_name(expr: &Expr) -> Option<String> {
    match expr {
        Expr::Identifier(name) => Some(name.to_ascii_lowercase()),
        Expr::CompoundIdentifier(parts) => parts.last().map(|p| p.to_ascii_lowercase()),
        _ => None,
    }
}

/// Human-readable rendering of a column reference for the 1055 message.
fn column_display_name(expr: &Expr) -> String {
    match expr {
        Expr::Identifier(name) => name.clone(),
        Expr::CompoundIdentifier(parts) => parts.join("."),
        _ => "?".to_string(),
    }
}

/// Whether an expression is a compile-time constant for FD purposes (a
/// literal, or a unary +/- over one). Conservative: unknown forms are not
/// treated as constant.
fn is_constant_fd_expr(expr: &Expr) -> bool {
    match expr {
        Expr::Literal(_) | Expr::BoundValue(_) | Expr::Placeholder(_) => true,
        Expr::Unary { expr, .. } => is_constant_fd_expr(expr),
        _ => false,
    }
}

fn expr_has_aggregate(expr: &Expr) -> bool {
    let mut seen: AggregateCallSet = HashMap::new();
    let mut out: Vec<Expr> = Vec::new();
    walk_collect_aggregates(expr, &mut seen, &mut out);
    !out.is_empty()
}

/// Add columns referenced by a nested query block that bind to the enclosing
/// grouped query. MySQL's ONLY_FULL_GROUP_BY check owns those correlated
/// references in the outer block even when an inner table reuses the same
/// alias but does not expose the referenced column.
fn collect_correlated_group_columns(
    db: &Database,
    expr: &Expr,
    outer_scope: &ColumnScope<'_>,
    seen_keys: &mut HashSet<u64>,
    out: &mut Vec<Expr>,
) -> Result<(), QueryError> {
    crate::util::stack::with_stack(
        crate::engine::executor::mem_budget::ControlledMemoryCategory::Executor,
        || collect_correlated_group_columns_checked(db, expr, outer_scope, seen_keys, out),
    )?
}

fn collect_correlated_group_columns_checked(
    db: &Database,
    expr: &Expr,
    outer_scope: &ColumnScope<'_>,
    seen_keys: &mut HashSet<u64>,
    out: &mut Vec<Expr>,
) -> Result<(), QueryError> {
    fn collect_one(
        db: &Database,
        subquery: &Select,
        outer_scope: &ColumnScope<'_>,
        seen_keys: &mut HashSet<u64>,
        out: &mut Vec<Expr>,
    ) -> Result<(), QueryError> {
        let local_scope = build_column_scope(db, subquery, None)?;
        let mut collect_candidate = |candidate: &Expr| {
            let (qualifier, column) = match candidate {
                Expr::Identifier(column) => (None, column.as_str()),
                Expr::CompoundIdentifier(parts) if !parts.is_empty() => (
                    (parts.len() >= 2).then(|| parts[parts.len() - 2].as_str()),
                    parts.last().map(String::as_str).unwrap_or_default(),
                ),
                _ => return,
            };
            if local_scope.column_matches(qualifier, column, false) == 0
                && outer_scope.column_matches(qualifier, column, false) == 1
            {
                push_hidden_column(candidate, seen_keys, out);
            }
        };
        let mut walk = |candidate: &Expr| {
            walk_expr_for_nonaggregate_idents(candidate, &mut collect_candidate);
        };
        for item in &subquery.projection {
            if let SelectItem::Expr { expr, .. } = item {
                walk(expr);
            }
        }
        if let Some(selection) = &subquery.selection {
            walk(selection);
        }
        if let Some(having) = &subquery.having {
            walk(having);
        }
        for join in &subquery.joins {
            if let Some(condition) = &join.condition {
                walk(condition);
            }
        }
        for group in &subquery.group_by {
            walk(group);
        }
        for order in &subquery.order_by {
            walk(&order.expr);
        }
        Ok(())
    }

    // Correlation inside an aggregate argument is evaluated per input row and
    // is legal (for example GROUP_CONCAT(x ORDER BY (SELECT ... outer.x))).
    // Stop at aggregate nodes; elsewhere recurse until a nested query block is
    // found and attribute its unresolved references to this outer block.
    match expr {
        Expr::Function { name, .. } if is_aggregate_function(name) => Ok(()),
        Expr::Subquery(subquery) | Expr::Exists { subquery, .. } => {
            collect_one(db, subquery, outer_scope, seen_keys, out)
        }
        Expr::InSubquery { expr, subquery, .. } => {
            collect_correlated_group_columns(db, expr, outer_scope, seen_keys, out)?;
            collect_one(db, subquery, outer_scope, seen_keys, out)
        }
        Expr::QuantifiedComparison { left, right, .. } => {
            collect_correlated_group_columns(db, left, outer_scope, seen_keys, out)?;
            match right {
                QuantifiedComparisonRight::Subquery(subquery) => {
                    collect_one(db, subquery, outer_scope, seen_keys, out)
                }
                QuantifiedComparisonRight::List(items) => {
                    for item in items {
                        collect_correlated_group_columns(db, item, outer_scope, seen_keys, out)?;
                    }
                    Ok(())
                }
            }
        }
        Expr::Unary { expr, .. }
        | Expr::IsNull(expr)
        | Expr::IsNotNull(expr)
        | Expr::GroupKeyRef { expr, .. } => {
            collect_correlated_group_columns(db, expr, outer_scope, seen_keys, out)
        }
        Expr::Binary { left, right, .. }
        | Expr::Like {
            expr: left,
            pattern: right,
            ..
        } => {
            collect_correlated_group_columns(db, left, outer_scope, seen_keys, out)?;
            collect_correlated_group_columns(db, right, outer_scope, seen_keys, out)
        }
        Expr::InList { expr, list, .. } => {
            collect_correlated_group_columns(db, expr, outer_scope, seen_keys, out)?;
            for item in list {
                collect_correlated_group_columns(db, item, outer_scope, seen_keys, out)?;
            }
            Ok(())
        }
        Expr::Between {
            expr, low, high, ..
        } => {
            for child in [expr.as_ref(), low.as_ref(), high.as_ref()] {
                collect_correlated_group_columns(db, child, outer_scope, seen_keys, out)?;
            }
            Ok(())
        }
        Expr::Function { args, .. } | Expr::Tuple(args) => {
            for arg in args {
                collect_correlated_group_columns(db, arg, outer_scope, seen_keys, out)?;
            }
            Ok(())
        }
        Expr::Case {
            operand,
            conditions,
            results,
            else_result,
        } => {
            if let Some(operand) = operand {
                collect_correlated_group_columns(db, operand, outer_scope, seen_keys, out)?;
            }
            for child in conditions.iter().chain(results) {
                collect_correlated_group_columns(db, child, outer_scope, seen_keys, out)?;
            }
            if let Some(else_result) = else_result {
                collect_correlated_group_columns(db, else_result, outer_scope, seen_keys, out)?;
            }
            Ok(())
        }
        Expr::WindowFunction { .. }
        | Expr::Wildcard
        | Expr::Identifier(_)
        | Expr::CompoundIdentifier(_)
        | Expr::UserVariable(_)
        | Expr::SystemVariable { .. }
        | Expr::Literal(_)
        | Expr::BoundValue(_)
        | Expr::Placeholder(_)
        | Expr::Default(_)
        | Expr::DefaultQualified(_)
        | Expr::Grouping(_) => Ok(()),
    }
}

/// P2-C2: enforce `ONLY_FULL_GROUP_BY`. In a grouped query every
/// non-aggregated column referenced by SELECT / HAVING / ORDER BY must be
/// functionally dependent on the GROUP BY columns, else error 1055. mydb's
/// default sql_mode enables this check. Explicitly disabling
/// `ONLY_FULL_GROUP_BY` preserves the permissive hidden-first-value semantics.
///
/// The functional-dependency closure is intentionally *generous* — matched by
/// bare column name and extended by WHERE equalities / constants and full
/// PK / UNIQUE-NOT-NULL keys that appear in GROUP BY. Erring permissive can
/// only miss a violation MySQL would catch (e.g. across two tables sharing a
/// column name); it never rejects a query MySQL accepts.
pub(crate) fn check_only_full_group_by(db: &Database, select: &Select) -> Result<(), QueryError> {
    use std::collections::HashSet;
    let has_agg = has_aggregate_select_items(&select.projection)
        || select.having.as_ref().is_some_and(expr_has_aggregate)
        || select
            .order_by
            .iter()
            .any(|ob| expr_has_aggregate(&ob.expr));
    let grouped = !select.group_by.is_empty() || !select.grouping_sets.is_empty() || has_agg;
    if !grouped {
        return Ok(());
    }
    // Candidate (hidden) columns: non-aggregated bare column refs not already
    // equal to a GROUP BY expression. Reuses the exact collector the
    // hidden-first-value rewrite uses, so the two stay in lockstep.
    let order_having_aliases = projection_aliases(select);
    let no_aliases: HashSet<String> = HashSet::new();
    let mut seen_keys: HashSet<u64> = HashSet::new();
    let mut hidden: Vec<Expr> = Vec::new();
    for item in &select.projection {
        if let SelectItem::Expr { expr, .. } = item {
            collect_hidden_group_columns(expr, select, &no_aliases, &mut seen_keys, &mut hidden);
        }
    }
    if let Some(having) = &select.having {
        collect_hidden_group_columns(
            having,
            select,
            &order_having_aliases,
            &mut seen_keys,
            &mut hidden,
        );
    }
    for ob in &select.order_by {
        collect_hidden_group_columns(
            &ob.expr,
            select,
            &order_having_aliases,
            &mut seen_keys,
            &mut hidden,
        );
    }
    let outer_scope = build_column_scope(db, select, None)?;
    // Diagnose an unknown grouping key before reporting unrelated SELECT
    // columns as not functionally dependent on that nonexistent key.
    for group in &select.group_by {
        validate_expr_columns(db, group, &outer_scope, Some(&order_having_aliases), true)?;
    }
    let is_grouped_expr = |expr: &Expr| {
        select.group_by.iter().any(|group| {
            expr_semantically_eq(group, expr)
                || matches!(
                    expr,
                    Expr::GroupKeyRef { expr: original, .. } if expr_semantically_eq(group, original)
                )
        })
    };
    for item in &select.projection {
        if let SelectItem::Expr { expr, .. } = item {
            // GROUP BY ordinals/aliases are resolved to their projection
            // expression before this check. A correlated column inside that
            // complete expression is part of the grouping key, not a hidden
            // non-grouped outer reference.
            if is_grouped_expr(expr) {
                continue;
            }
            collect_correlated_group_columns(db, expr, &outer_scope, &mut seen_keys, &mut hidden)?;
        }
    }
    if let Some(having) = &select.having {
        if !is_grouped_expr(having) {
            collect_correlated_group_columns(
                db,
                having,
                &outer_scope,
                &mut seen_keys,
                &mut hidden,
            )?;
        }
    }
    for order in &select.order_by {
        if is_grouped_expr(&order.expr) {
            continue;
        }
        collect_correlated_group_columns(
            db,
            &order.expr,
            &outer_scope,
            &mut seen_keys,
            &mut hidden,
        )?;
    }
    if hidden.is_empty() {
        return Ok(());
    }
    // Build the allowed bare-name closure. Seed with GROUP BY columns.
    let mut allowed: HashSet<String> = HashSet::new();
    for g in &select.group_by {
        if let Some(name) = column_bare_name(g) {
            allowed.insert(name);
        }
    }
    // WHERE equalities (col = col) and constants (col = literal).
    let mut equalities: Vec<(String, String)> = Vec::new();
    if let Some(selection) = &select.selection {
        let mut conjuncts = Vec::new();
        split_conjuncts(selection, &mut conjuncts);
        for c in &conjuncts {
            if let Expr::InList {
                expr,
                list,
                negated: false,
            } = c
            {
                if list.len() == 1 && is_constant_fd_expr(&list[0]) {
                    if let Some(column) = column_bare_name(expr) {
                        allowed.insert(column);
                    }
                }
            }
            if let Expr::Binary {
                left,
                op: BinaryOp::Eq,
                right,
            } = c
            {
                match (column_bare_name(left), column_bare_name(right)) {
                    (Some(l), Some(r)) => equalities.push((l, r)),
                    (Some(l), None) if is_constant_fd_expr(right) => {
                        allowed.insert(l);
                    }
                    (None, Some(r)) if is_constant_fd_expr(left) => {
                        allowed.insert(r);
                    }
                    _ => {}
                }
            }
        }
    }
    // Base tables in scope (primary + joins). Derived/subquery refs fail
    // `db.table` and are skipped.
    let mut base_tables: Vec<String> = Vec::new();
    for t in &select.from {
        base_tables.push(t.name.clone());
    }
    for j in &select.joins {
        base_tables.push(j.table.name.clone());
    }
    // Fixpoint: equality propagation + PK / UNIQUE-NOT-NULL key closure.
    loop {
        let before = allowed.len();
        for (l, r) in &equalities {
            if allowed.contains(l) {
                allowed.insert(r.clone());
            }
            if allowed.contains(r) {
                allowed.insert(l.clone());
            }
        }
        for tname in &base_tables {
            let Ok(schema) = db.table(tname) else {
                continue;
            };
            let determines = {
                let pk_ok = !schema.primary_key.is_empty()
                    && schema
                        .primary_key
                        .iter()
                        .all(|c| allowed.contains(&c.to_ascii_lowercase()));
                pk_ok
                    || schema.indexes.iter().any(|idx| {
                        idx.unique
                            && !idx.columns.is_empty()
                            && idx.columns.iter().all(|c| {
                                let lc = c.to_ascii_lowercase();
                                allowed.contains(&lc)
                                    && schema
                                        .columns
                                        .iter()
                                        .find(|col| col.name.eq_ignore_ascii_case(c))
                                        .map(|col| !col.nullable)
                                        .unwrap_or(false)
                            })
                    })
            };
            if determines {
                for col in &schema.columns {
                    allowed.insert(col.name.to_ascii_lowercase());
                }
            }
        }
        if allowed.len() == before {
            break;
        }
    }
    // Flag the first hidden column not covered by the closure. Columns whose
    // bare name can't be extracted are left alone (safe / permissive).
    for col in &hidden {
        if let Some(name) = column_bare_name(col) {
            if !allowed.contains(&name) {
                let disp = column_display_name(col);
                if select.group_by.is_empty() && select.grouping_sets.is_empty() && has_agg {
                    return Err(error(
                        crate::error::catalog::ER_MIX_OF_GROUP_FUNC_AND_FIELDS,
                        format!(
                            "In aggregated query without GROUP BY, expression of SELECT list \
                             contains nonaggregated column '{disp}'; this is incompatible with \
                             sql_mode=only_full_group_by"
                        ),
                    ));
                }
                return Err(error(
                    crate::error::catalog::ER_WRONG_FIELD_WITH_GROUP,
                    format!(
                        "Expression of SELECT list is not in GROUP BY clause and contains \
                         nonaggregated column '{disp}' which is not functionally dependent on \
                         columns in GROUP BY clause; this is incompatible with \
                         sql_mode=only_full_group_by"
                    ),
                ));
            }
        }
    }
    Ok(())
}

fn collect_hidden_group_columns(
    expr: &Expr,
    select: &Select,
    aliases: &HashSet<String>,
    seen_keys: &mut HashSet<u64>,
    out: &mut Vec<Expr>,
) {
    crate::parser::visit::inspect_value_expr(
        expr,
        |expr| {
            if select
                .group_by
                .iter()
                .any(|group| expr_semantically_eq(group, expr))
            {
                return false;
            }
            match expr {
                Expr::Identifier(name) => {
                    if !aliases.contains(&name.to_ascii_lowercase()) {
                        push_hidden_column(expr, seen_keys, out);
                    }
                    false
                }
                Expr::CompoundIdentifier(_) => {
                    push_hidden_column(expr, seen_keys, out);
                    false
                }
                Expr::Function { name, .. } => {
                    !is_aggregate_function(name) && name != crate::parser::FT_MATCH_FUNCTION
                }
                Expr::Subquery(_)
                | Expr::Exists { .. }
                | Expr::InSubquery { .. }
                | Expr::QuantifiedComparison { .. }
                | Expr::WindowFunction { .. }
                | Expr::GroupKeyRef { .. } => false,
                _ => true,
            }
        },
        |_| {},
    );
}

fn push_hidden_column(expr: &Expr, seen_keys: &mut HashSet<u64>, out: &mut Vec<Expr>) {
    let Some(key) = crate::engine::executor::select::aggregate::hidden_group_column_key(expr)
    else {
        return;
    };
    if seen_keys.insert(key) {
        out.push(expr.clone());
    }
}

type AggregateCallSet = HashMap<u64, smallvec::SmallVec<[usize; 1]>>;

fn walk_collect_aggregates(expr: &Expr, seen: &mut AggregateCallSet, out: &mut Vec<Expr>) {
    if let Expr::Function { name, .. } = expr {
        if is_aggregate_function(name) {
            let key = crate::engine::executor::select::aggregate::aggregate_key(expr);
            let positions = seen.entry(key).or_default();
            if !positions
                .iter()
                .any(|&index| expr_semantically_eq(&out[index], expr))
            {
                positions.push(out.len());
                out.push(expr.clone());
            }
            return;
        }
    }
    match expr {
        Expr::Unary { expr, .. } | Expr::IsNull(expr) | Expr::IsNotNull(expr) => {
            walk_collect_aggregates(expr, seen, out)
        }
        Expr::Binary { left, right, .. } => {
            walk_collect_aggregates(left, seen, out);
            walk_collect_aggregates(right, seen, out);
        }
        Expr::InList { expr, list, .. } => {
            walk_collect_aggregates(expr, seen, out);
            for item in list {
                walk_collect_aggregates(item, seen, out);
            }
        }
        Expr::QuantifiedComparison { left, right, .. } => {
            walk_collect_aggregates(left, seen, out);
            if let QuantifiedComparisonRight::List(list) = right {
                for item in list {
                    walk_collect_aggregates(item, seen, out);
                }
            }
        }
        Expr::Like { expr, pattern, .. } => {
            walk_collect_aggregates(expr, seen, out);
            walk_collect_aggregates(pattern, seen, out);
        }
        Expr::Between {
            expr, low, high, ..
        } => {
            walk_collect_aggregates(expr, seen, out);
            walk_collect_aggregates(low, seen, out);
            walk_collect_aggregates(high, seen, out);
        }
        Expr::Case {
            operand,
            conditions,
            results,
            else_result,
        } => {
            if let Some(operand) = operand {
                walk_collect_aggregates(operand, seen, out);
            }
            for c in conditions {
                walk_collect_aggregates(c, seen, out);
            }
            for r in results {
                walk_collect_aggregates(r, seen, out);
            }
            if let Some(else_result) = else_result {
                walk_collect_aggregates(else_result, seen, out);
            }
        }
        Expr::Function { args, .. } => {
            for a in args {
                walk_collect_aggregates(a, seen, out);
            }
        }
        Expr::Tuple(items) => {
            for item in items {
                walk_collect_aggregates(item, seen, out);
            }
        }
        Expr::Subquery(subquery)
            if subquery.table.is_none()
                && subquery.from.is_empty()
                && subquery.derived.is_none()
                && subquery.joins.is_empty()
                && subquery.selection.is_none()
                && subquery.group_by.is_empty()
                && subquery.having.is_none()
                && subquery.projection.len() == 1 =>
        {
            if let SelectItem::Expr { expr: inner, .. } = &subquery.projection[0] {
                for aggregate in source_free_outer_aggregates(inner) {
                    walk_collect_aggregates(&aggregate, seen, out);
                }
            }
        }
        _ => {}
    }
}

/// Aggregates owned by an enclosing group inside a source-free expression.
/// Local COUNT(*) / SUM(constants) retain the implicit inner one-row group.
pub(crate) fn source_free_outer_aggregates(expr: &Expr) -> Vec<Expr> {
    let mut aggregates = Vec::new();
    walk_collect_aggregates(expr, &mut AggregateCallSet::new(), &mut aggregates);
    aggregates.retain(|expr| {
        matches!(expr, Expr::Function { args, .. }
        if args.iter().any(expr_references_any_named_column))
    });
    aggregates
}

fn expr_references_any_named_column(expr: &Expr) -> bool {
    match expr {
        Expr::Identifier(_) | Expr::CompoundIdentifier(_) => true,
        Expr::Unary { expr, .. }
        | Expr::IsNull(expr)
        | Expr::IsNotNull(expr)
        | Expr::GroupKeyRef { expr, .. } => expr_references_any_named_column(expr),
        Expr::Binary { left, right, .. } => {
            expr_references_any_named_column(left) || expr_references_any_named_column(right)
        }
        Expr::Function { args, .. } | Expr::Tuple(args) => {
            args.iter().any(expr_references_any_named_column)
        }
        Expr::InList { expr, list, .. } => {
            expr_references_any_named_column(expr)
                || list.iter().any(expr_references_any_named_column)
        }
        Expr::Between {
            expr, low, high, ..
        } => {
            expr_references_any_named_column(expr)
                || expr_references_any_named_column(low)
                || expr_references_any_named_column(high)
        }
        Expr::Like { expr, pattern, .. } => {
            expr_references_any_named_column(expr) || expr_references_any_named_column(pattern)
        }
        Expr::Case {
            operand,
            conditions,
            results,
            else_result,
        } => {
            operand
                .as_deref()
                .is_some_and(expr_references_any_named_column)
                || conditions.iter().any(expr_references_any_named_column)
                || results.iter().any(expr_references_any_named_column)
                || else_result
                    .as_deref()
                    .is_some_and(expr_references_any_named_column)
        }
        _ => false,
    }
}

fn has_aggregate_select_items(items: &[SelectItem]) -> bool {
    items.iter().any(|item| match item {
        SelectItem::Expr { expr, .. } => has_aggregate_expr(expr),
        _ => false,
    })
}

pub(crate) fn has_aggregate_expr(expr: &Expr) -> bool {
    crate::parser::visit::any_expr_matching(
        expr,
        |node| match node {
            Expr::Function { name, .. } => is_aggregate_function(name),
            // Source-free scalar blocks may contain aggregates owned by this block.
            Expr::Subquery(subquery)
                if subquery.table.is_none()
                    && subquery.from.is_empty()
                    && subquery.derived.is_none()
                    && subquery.joins.is_empty()
                    && subquery.selection.is_none()
                    && subquery.group_by.is_empty()
                    && subquery.having.is_none()
                    && subquery.projection.len() == 1 =>
            {
                matches!(&subquery.projection[0], SelectItem::Expr { expr, .. }
                    if !source_free_outer_aggregates(expr).is_empty())
            }
            _ => false,
        },
        |node| {
            crate::parser::visit::scalar_expr_children(node)
                || matches!(node, Expr::Tuple(_) | Expr::InSubquery { .. })
        },
    )
}

// Moved to `crate::parser` (the parser now needs it for GROUP BY ordinal
// validation); re-exported here so the many `planner::is_aggregate_function`
// call sites keep working.
pub use crate::parser::is_aggregate_function;

fn expr_literal_value(expr: &Expr) -> Option<Value> {
    match expr {
        Expr::Literal(lit) => Some(match lit {
            crate::parser::Literal::Null => Value::Null,
            crate::parser::Literal::Bool(v) => Value::Int(*v as i64),
            crate::parser::Literal::Int(v) => Value::Int(*v),
            crate::parser::Literal::UInt(v) => Value::UInt(*v),
            crate::parser::Literal::Float(v) => Value::Double(*v),
            crate::parser::Literal::Decimal(v) => {
                crate::engine::expr::mysql_decimal_literal_value(v)
            }
            crate::parser::Literal::String(v) => Value::String(v.clone()),
            crate::parser::Literal::HexBytes(b) | crate::parser::Literal::BitBytes(b) => {
                Value::Bytes(b.clone())
            }
        }),
        Expr::BoundValue(value) => Some(value.clone()),
        // sqlparser represents `-5` as unary negation over the positive
        // literal. Treat it as a constant here too; otherwise predicates such
        // as `gmt_offset = -5` miss MCV/NDV statistics and fall back to the
        // default one-third selectivity, which can reverse index-candidate
        // ordering by two orders of magnitude.
        Expr::Unary {
            op: crate::parser::UnaryOp::Neg,
            expr,
        } => match expr_literal_value(expr)? {
            Value::Int(value) => value.checked_neg().map(Value::Int),
            Value::UInt(value) if value <= i64::MAX as u64 => Some(Value::Int(-(value as i64))),
            Value::UInt(value) if value == (i64::MAX as u64) + 1 => Some(Value::Int(i64::MIN)),
            Value::UInt(value) => Some(Value::Decimal(format!("-{value}"))),
            Value::Double(value) => Some(Value::Double(-value)),
            Value::Decimal(value) => Some(Value::Decimal(
                if let Some(rest) = value.strip_prefix('-') {
                    rest.to_string()
                } else {
                    format!("-{value}")
                },
            )),
            _ => None,
        },
        // Phase 2: when a placeholder is being planned under an active
        // parameter context (prepared EXECUTE or auto-parameterized
        // COM_QUERY), resolve it to the bound value so the planner can
        // build a real `IndexKey` and pick a point lookup. Without this,
        // every `WHERE pk = ?` would degrade to a full table scan,
        // which under a concurrent OLTP load (e.g. TPC-C) creates
        // catastrophic row-lock contention.
        //
        // This baking means the resulting plan is parameter-specific,
        // so the cache layer must NOT store plans whose tree still
        // references placeholders against the catalog (see
        // `plan_contains_placeholder` and the bypass in `execute_select`).
        Expr::Placeholder(idx) => crate::engine::params::current_param(*idx),
        // Typed DATE literals are retained as CAST nodes by constant folding
        // because CAST carries observable result metadata. They are still
        // valid constant index bounds: the index column's ParamTarget below
        // performs the same string-to-temporal key coercion as execution.
        // Without recognizing this shape, `d < DATE '2000-01-01'` and
        // `d BETWEEN DATE '...' AND ...` lose their range candidate entirely.
        Expr::Function { name, args, .. }
            if name.eq_ignore_ascii_case("CAST")
                && args.len() >= 2
                && matches!(
                    args.last(),
                    Some(Expr::Literal(crate::parser::Literal::String(target)))
                        if {
                            let target = target.trim().to_ascii_uppercase();
                            target.starts_with("DATE")
                                || target.starts_with("DATETIME")
                                || target.starts_with("TIMESTAMP")
                                || target.starts_with("TIME")
                                || target.starts_with("YEAR")
                        }
                ) =>
        {
            expr_literal_value(&args[0])
        }
        // TPC-H queries frequently bound ranges with
        // `DATE 'x' + INTERVAL 'n' MONTH` etc. The parser rewrites
        // that to `DATE_ADD(date_literal, __INTERVAL(n, unit))`. The
        // estimator (and IndexKey builders) need a concrete value to
        // produce accurate selectivity, so fold the constant arithmetic
        // here.
        Expr::Function { name, args, .. }
            if (name.eq_ignore_ascii_case("DATE_ADD") || name.eq_ignore_ascii_case("DATE_SUB"))
                && args.len() == 2 =>
        {
            let date = expr_literal_value(&args[0])?;
            let (n, unit) = interval_components(&args[1])?;
            let signed = if name.eq_ignore_ascii_case("DATE_SUB") {
                -n
            } else {
                n
            };
            apply_date_interval(&date, signed, &unit)
        }
        _ => None,
    }
}

fn interval_components(expr: &Expr) -> Option<(i64, String)> {
    let Expr::Function { name, args, .. } = expr else {
        return None;
    };
    if !name.eq_ignore_ascii_case("__INTERVAL") || args.len() != 2 {
        return None;
    }
    let n = match expr_literal_value(&args[0])? {
        Value::Int(v) => v,
        Value::UInt(v) => v as i64,
        Value::String(s) => s.trim().parse::<i64>().ok()?,
        _ => return None,
    };
    let unit = match &args[1] {
        Expr::Literal(crate::parser::Literal::String(s)) => s.clone(),
        _ => return None,
    };
    Some((n, unit))
}

fn apply_date_interval(date: &Value, n: i64, unit: &str) -> Option<Value> {
    let s = match date {
        Value::String(s) => s.as_str(),
        _ => return None,
    };
    let parsed = chrono::NaiveDate::parse_from_str(s.trim(), "%Y-%m-%d").ok()?;
    let result = match unit.to_ascii_uppercase().as_str() {
        "DAY" | "DAYS" => parsed.checked_add_signed(chrono::Duration::days(n))?,
        "WEEK" | "WEEKS" => parsed.checked_add_signed(chrono::Duration::weeks(n))?,
        "MONTH" | "MONTHS" => add_months(parsed, n)?,
        "YEAR" | "YEARS" => add_months(parsed, n.checked_mul(12)?)?,
        _ => return None,
    };
    Some(Value::String(result.format("%Y-%m-%d").to_string()))
}

fn add_months(date: chrono::NaiveDate, n: i64) -> Option<chrono::NaiveDate> {
    use chrono::Datelike;
    let total = date.year() as i64 * 12 + (date.month() as i64 - 1) + n;
    let new_year = i32::try_from(total.div_euclid(12)).ok()?;
    let new_month = total.rem_euclid(12) as u32 + 1;
    let day = date.day();
    // Clamp the day to the last valid day of the new month so we never
    // fail on `Jan 31 + 1 MONTH = Feb 28/29`.
    for d in (1..=day).rev() {
        if let Some(result) = chrono::NaiveDate::from_ymd_opt(new_year, new_month, d) {
            return Some(result);
        }
    }
    None
}

pub(crate) fn expr_column_name(expr: &Expr) -> Option<&str> {
    match expr {
        Expr::Identifier(name) => Some(name),
        Expr::CompoundIdentifier(parts) => parts.last().map(String::as_str),
        Expr::GroupKeyRef { expr, .. } => expr_column_name(expr),
        _ => None,
    }
}

fn normalize_expr_column(column: &str) -> String {
    column.to_ascii_lowercase()
}

fn matches_table(name: &str, table_name: &str, alias: Option<&str>) -> bool {
    // SQL alias-shadows-name: once a table has an alias, only the alias is a
    // valid qualifier; the base table/CTE name is hidden. This mirrors
    // `ScopeTable::matches_name` (planner scope resolution) and
    // `field_matches_qualifier` (runtime row resolution) so the three layers
    // agree. Allowing the base name to match through an alias makes
    // predicate pushdown stuff a self-join filter into a single CTE/table
    // scan (TPC-DS q47 / q57 / q24 self-joins).
    //
    // Note: `materialize_join_source` strips the `database.table` qualifier
    // from `table_ref.name` before defaulting the alias, so a user-written
    // bare `t.x` against unaliased `FROM t` matches via the alias slot
    // (alias = "t") under this rule.
    match alias {
        Some(alias) => name.eq_ignore_ascii_case(alias),
        None => name.eq_ignore_ascii_case(table_name),
    }
}

// ============================================================
// Tier 0 #1: Predicate normalization
//
// Canonicalizes WHERE / HAVING / JOIN-ON expression trees before
// planning, so that:
//   - OR-chains of equality on the same column become IN-lists
//     (better selectivity estimates via estimate_filter_rows)
//   - NOT is pushed through AND/OR/comparisons/IS NULL/BETWEEN/IN
//     so the resulting tree only has NOT applied to leaves planning
//     can't simplify further
//   - BETWEEN low>high (constants) collapses to FALSE
//   - AND/OR with a constant boolean side short-circuits
// All rewrites preserve SQL 3-valued NULL semantics.
// ============================================================

/// Variant list of one ENUM/SET column, for the numeric-semantics
/// rewrite below.
#[derive(Clone)]
struct EnumSetEntry {
    is_set: bool,
    collation: Option<String>,
    values: Vec<String>,
}

#[derive(Default)]
struct EnumSetColumns {
    /// `(table-or-alias, column)` both lowercase.
    qualified: HashMap<(String, String), Arc<EnumSetEntry>>,
    /// Bare column name → entry; `None` marks an ambiguous name seen in
    /// more than one FROM table (no rewrite then).
    unqualified: HashMap<String, Option<Arc<EnumSetEntry>>>,
}

impl EnumSetColumns {
    fn is_empty(&self) -> bool {
        self.qualified.is_empty()
    }

    fn add_table(&mut self, db: &Database, name: &str, alias: Option<&str>) {
        if crate::engine::executor::cte::has_cte(name) {
            return;
        }
        let Ok(schema) = db.table(name) else {
            return;
        };
        let mut qualifiers = vec![alias.unwrap_or(name).to_ascii_lowercase()];
        if alias.is_none() {
            qualifiers.push(schema.name.to_ascii_lowercase());
            qualifiers.push(schema.qualified_name().to_ascii_lowercase());
            qualifiers.sort_unstable();
            qualifiers.dedup();
        }
        for column in &schema.columns {
            let entry = match &column.data_type {
                crate::storage::DataTypeKind::Enum { values } => EnumSetEntry {
                    is_set: false,
                    collation: column.collation.clone(),
                    values: values.clone(),
                },
                crate::storage::DataTypeKind::Set { values } => EnumSetEntry {
                    is_set: true,
                    collation: column.collation.clone(),
                    values: values.clone(),
                },
                _ => continue,
            };
            let column_lower = column.name.to_ascii_lowercase();
            // Share member definitions across valid source spellings instead
            // of copying every ENUM/SET member for each qualifier.
            let entry = Arc::new(entry);
            for qualifier in &qualifiers {
                self.qualified.insert(
                    (qualifier.clone(), column_lower.clone()),
                    Arc::clone(&entry),
                );
            }
            match self.unqualified.entry(column_lower) {
                std::collections::hash_map::Entry::Vacant(slot) => {
                    slot.insert(Some(entry));
                }
                std::collections::hash_map::Entry::Occupied(mut slot) => {
                    slot.insert(None);
                }
            }
        }
    }

    fn resolve(&self, expr: &Expr) -> Option<&EnumSetEntry> {
        match expr {
            // GROUP BY binding wraps references used by projection
            // expressions before the ENUM numeric-context rewrite runs. The
            // wrapper changes where the value is read from, not the bound
            // column's type identity.
            Expr::GroupKeyRef { expr, .. } => self.resolve(expr),
            Expr::Identifier(name) => self
                .unqualified
                .get(&name.to_ascii_lowercase())
                .and_then(|entry| entry.as_deref()),
            Expr::CompoundIdentifier(parts) if parts.len() >= 2 => {
                let table = parts[..parts.len() - 1].join(".").to_ascii_lowercase();
                let column = parts[parts.len() - 1].to_ascii_lowercase();
                self.qualified.get(&(table, column)).map(Arc::as_ref)
            }
            _ => None,
        }
    }
}

fn ordinal_call(column_expr: Expr, entry: &EnumSetEntry) -> Expr {
    let binary_collation = entry
        .collation
        .as_deref()
        .and_then(|name| crate::charset::resolve_collation(name).ok())
        .is_some_and(|collation| collation.binary);
    let name = if entry.is_set && binary_collation {
        "__SET_ORDINAL_BINARY"
    } else if entry.is_set {
        "__SET_ORDINAL"
    } else {
        "__ENUM_ORDINAL"
    };
    let mut args = Vec::with_capacity(entry.values.len() + 1);
    args.push(column_expr);
    args.extend(
        entry
            .values
            .iter()
            .map(|v| Expr::Literal(crate::parser::Literal::String(v.clone()))),
    );
    Expr::Function {
        name: name.to_string(),
        args,
        distinct: false,
    }
}

/// Operand shapes eligible for the runtime comparison: numeric
/// literals, and parameters whose type only the bound value knows.
fn cmp_operand_shape(expr: &Expr) -> bool {
    matches!(
        expr,
        Expr::Literal(crate::parser::Literal::Int(_))
            | Expr::Literal(crate::parser::Literal::UInt(_))
            | Expr::Literal(crate::parser::Literal::Float(_))
            | Expr::Literal(crate::parser::Literal::Decimal(_))
            | Expr::Placeholder(_)
    )
}

fn cmp_op_token(op: BinaryOp) -> &'static str {
    match op {
        BinaryOp::Eq => "=",
        BinaryOp::NullSafeEq => "<=>",
        BinaryOp::NotEq => "<>",
        BinaryOp::Lt => "<",
        BinaryOp::LtEq => "<=",
        BinaryOp::Gt => ">",
        BinaryOp::GtEq => ">=",
        _ => "=",
    }
}

fn integer_literal(expr: &Expr) -> Option<i64> {
    match expr {
        Expr::Literal(crate::parser::Literal::Int(v)) => Some(*v),
        Expr::Literal(crate::parser::Literal::UInt(v)) => i64::try_from(*v).ok(),
        _ => None,
    }
}

pub(crate) fn normalize_select_predicates(db: &Database, select: &mut Select) {
    rewrite_not_null_temporal_is_null_in_where(db, select);
    if let Some(e) = select.selection.take() {
        select.selection = drop_trivially_true(factor_common_or_conjuncts(normalize_expr(e)));
    }
    for join in &mut select.joins {
        if let Some(c) = join.condition.take() {
            join.condition = drop_trivially_true(factor_common_or_conjuncts(normalize_expr(c)));
        }
    }
    if let Some(e) = select.having.take() {
        select.having = drop_trivially_true(normalize_expr(e));
    }
    // Outer-join → inner-join simplification: when a WHERE predicate is
    // null-rejecting on an outer join's null-supplied side, the null-extended
    // rows are always filtered out, so the outer join is equivalent to an
    // inner (or, for FULL, a one-sided outer) join. Runs *before* equivalence
    // propagation so a freshly-inner join's ON equalities feed the constraint
    // analysis, and *before* join-order planning so the all-inner DP path can
    // engage. Recurses into derived sources. See
    // docs/outer-join-to-inner-conversion-design.md.
    simplify_outer_joins(db, select);
    // Tier 0 #6: equivalence-class propagation. Extends single-column
    // equality constraints across `colA = colB` chains so the planner
    // sees an explicit `b = literal` whenever `a = literal AND b = a`,
    // including across INNER JOIN ON clauses.
    propagate_equivalence_constraints(db, select);
    // Phase B #B1: rewrite uncorrelated IN/EXISTS subqueries in WHERE
    // into Semi joins, and NOT EXISTS into Anti joins. Avoids the
    // "evaluate subquery per outer row" cost of the generic
    // subquery path. NOT IN is intentionally NOT rewritten yet —
    // SQL NULL semantics make it unsafe without column-nullability
    // information.
    rewrite_subqueries_to_semi_anti(db, select);
}

/// MySQL's legacy zero-date compatibility rule is clause-sensitive:
/// `WHERE not_null_date IS NULL` is an indexable lookup for the all-zero
/// date sentinel, while the same predicate in an inner-join ON clause is
/// simply false. Rewrite only the WHERE tree and only catalog NOT NULL
/// temporal columns. This preserves ordinary nullable-column `IS NULL` and
/// outer-join null-extension semantics.
fn rewrite_not_null_temporal_is_null_in_where(db: &Database, select: &mut Select) {
    #[derive(Clone)]
    struct Candidate {
        qualifiers: Vec<String>,
        column: String,
        zero: Value,
        null_extended: bool,
    }

    fn add_table(
        db: &Database,
        table_ref: &crate::parser::TableRef,
        null_extended: bool,
        candidates: &mut Vec<Candidate>,
    ) {
        let Ok(table) = db.table(&table_ref.name) else {
            return;
        };
        let mut qualifiers = vec![table_ref.name.to_ascii_lowercase()];
        if let Some(base) = table_ref.name.rsplit('.').next() {
            qualifiers.push(base.to_ascii_lowercase());
        }
        if let Some(alias) = &table_ref.alias {
            qualifiers.push(alias.to_ascii_lowercase());
        }
        qualifiers.sort();
        qualifiers.dedup();
        for column in &table.columns {
            if column.nullable {
                continue;
            }
            let zero = match column.data_type {
                crate::storage::DataTypeKind::Date => Value::Date(i32::MIN),
                crate::storage::DataTypeKind::DateTime { .. } => Value::DateTime(i64::MIN),
                _ => continue,
            };
            candidates.push(Candidate {
                qualifiers: qualifiers.clone(),
                column: column.name.to_ascii_lowercase(),
                zero,
                null_extended,
            });
        }
    }

    let mut candidates = Vec::new();
    for table_ref in &select.from {
        add_table(db, table_ref, false, &mut candidates);
    }
    for join in &select.joins {
        if matches!(join.join_type, JoinType::Right | JoinType::Full) {
            for candidate in &mut candidates {
                candidate.null_extended = true;
            }
        }
        add_table(
            db,
            &join.table,
            matches!(join.join_type, JoinType::Left | JoinType::Full),
            &mut candidates,
        );
    }

    fn zero_for(expr: &Expr, candidates: &[Candidate]) -> Option<(Value, bool)> {
        let (qualifier, column) = match expr {
            Expr::Identifier(column) => (None, column.as_str()),
            Expr::CompoundIdentifier(parts) => {
                let column = parts.last()?.as_str();
                let qualifier = (parts.len() >= 2).then(|| parts[parts.len() - 2].as_str());
                (qualifier, column)
            }
            _ => return None,
        };
        let mut found = candidates.iter().filter(|candidate| {
            candidate.column.eq_ignore_ascii_case(column)
                && qualifier.is_none_or(|qualifier| {
                    candidate
                        .qualifiers
                        .iter()
                        .any(|known| known.eq_ignore_ascii_case(qualifier))
                })
        });
        let first = found.next()?;
        found
            .next()
            .is_none()
            .then(|| (first.zero.clone(), first.null_extended))
    }

    fn rewrite(expr: &mut Expr, candidates: &[Candidate]) {
        match expr {
            Expr::IsNull(inner) => {
                if let Some((zero, null_extended)) = zero_for(inner, candidates) {
                    let left = std::mem::replace(
                        inner.as_mut(),
                        Expr::Literal(crate::parser::Literal::Null),
                    );
                    let equality = Expr::Binary {
                        left: Box::new(left.clone()),
                        op: BinaryOp::Eq,
                        right: Box::new(Expr::BoundValue(zero)),
                    };
                    *expr = if null_extended {
                        Expr::Binary {
                            left: Box::new(equality),
                            op: BinaryOp::Or,
                            right: Box::new(Expr::IsNull(Box::new(left))),
                        }
                    } else {
                        equality
                    };
                }
            }
            Expr::Binary { left, op, right } if matches!(op, BinaryOp::And | BinaryOp::Or) => {
                rewrite(left, candidates);
                rewrite(right, candidates);
            }
            // The legacy rule applies to a predicate item itself, not to an
            // IS NULL nested inside NOT/IS TRUE/arithmetic. Those forms see
            // the column's declared NOT NULL property and fold normally.
            _ => {}
        }
    }

    if !candidates.is_empty() {
        if let Some(selection) = &mut select.selection {
            rewrite(selection, &candidates);
        }
    }
}

/// Outer-join simplification gate. Default on; set
/// `MYDB_OUTER_JOIN_SIMPLIFY={off|0|false|no}` to restore the legacy
/// outer-join path (differential testing / regression triage).
fn outer_join_simplify_enabled() -> bool {
    let resolve = || {
        !matches!(
            crate::util::env::var("MYDB_OUTER_JOIN_SIMPLIFY")
                .unwrap_or_default()
                .to_ascii_lowercase()
                .as_str(),
            "off" | "0" | "false" | "no"
        )
    };
    #[cfg(not(test))]
    {
        static ENABLED: OnceLock<bool> = OnceLock::new();
        *ENABLED.get_or_init(resolve)
    }
    #[cfg(test)]
    {
        resolve()
    }
}

/// Convert LEFT/RIGHT/FULL OUTER joins whose null-supplied side carries a
/// null-rejecting WHERE predicate into the equivalent INNER (or, for FULL,
/// one-sided OUTER) join. Pure semantic-preserving rewrite — a null-rejecting
/// predicate on the null-extended side eliminates exactly the rows the outer
/// join adds, so the result set is identical while the executor avoids
/// materialising the unmatched rows. Recurses into derived/subquery sources.
fn simplify_outer_joins(db: &Database, select: &mut Select) {
    // Most SELECTs contain no outer-join tree at all. Reject that shape before
    // consulting configuration or walking expressions/subqueries.
    if select.derived.is_none() && select.joins.is_empty() {
        return;
    }
    if !outer_join_simplify_enabled() {
        return;
    }
    // Recurse into nested sources first so inner derived selects are
    // simplified regardless of whether they are independently re-normalised
    // later. The rewrite is idempotent, so a double pass is harmless.
    if let Some(sub) = select
        .derived
        .as_mut()
        .and_then(crate::parser::SubQuery::as_select_mut)
    {
        simplify_outer_joins(db, sub);
    }
    for join in &mut select.joins {
        if let Some(sub) = join
            .derived
            .as_mut()
            .and_then(crate::parser::SubQuery::as_select_mut)
        {
            simplify_outer_joins(db, sub);
        }
    }
    convert_outer_joins_one_level(db, select);
}

/// Apply the outer→inner conversion to the joins of a single SELECT level.
fn convert_outer_joins_one_level(db: &Database, select: &mut Select) {
    let has_outer = select.joins.iter().any(|j| {
        matches!(
            j.join_type,
            JoinType::Left | JoinType::Right | JoinType::Full
        )
    });
    if !has_outer {
        return;
    }
    let Some(selection) = &select.selection else {
        return;
    };
    let mut where_conjuncts = Vec::new();
    split_conjuncts(selection, &mut where_conjuncts);
    if where_conjuncts.is_empty() {
        return;
    }

    // Resolve a PlanTable for every source (index 0 = base, index k = right
    // table of joins[k-1]). Derived/CTE sources that can't be resolved stay
    // `None`; any join whose null side touches an unresolved source is left
    // unchanged (conservative — we cannot prove null-rejection there).
    let Some(base) = select.from.first() else {
        return;
    };
    let mut plan_tables: Vec<Option<PlanTable>> = Vec::with_capacity(select.joins.len() + 1);
    plan_tables.push(plan_table(db, base).ok());
    for join in &select.joins {
        plan_tables.push(plan_table(db, &join.table).ok());
    }

    for idx in 0..select.joins.len() {
        let right_pos = idx + 1;
        let left_positions: Vec<usize> = (0..=idx).collect();
        match select.joins[idx].join_type {
            JoinType::Left => {
                if null_side_rejected(&where_conjuncts, &plan_tables, &[right_pos]) {
                    select.joins[idx].join_type = JoinType::Inner;
                }
            }
            JoinType::Right => {
                if null_side_rejected(&where_conjuncts, &plan_tables, &left_positions) {
                    select.joins[idx].join_type = JoinType::Inner;
                }
            }
            JoinType::Full => {
                let reject_right = null_side_rejected(&where_conjuncts, &plan_tables, &[right_pos]);
                let reject_left =
                    null_side_rejected(&where_conjuncts, &plan_tables, &left_positions);
                select.joins[idx].join_type = match (reject_left, reject_right) {
                    (true, true) => JoinType::Inner,
                    // Only left side filtered → right-only rows gone → keep
                    // matched + left-only = LEFT JOIN.
                    (true, false) => JoinType::Left,
                    // Only right side filtered → left-only rows gone → keep
                    // matched + right-only = RIGHT JOIN.
                    (false, true) => JoinType::Right,
                    (false, false) => JoinType::Full,
                };
            }
            _ => {}
        }
    }
}

/// True if any WHERE conjunct is null-rejecting for the null-extended side
/// identified by `null_positions` (positions into `plan_tables`). Returns
/// false if any null-side position is unresolved (derived/CTE), so the join
/// is conservatively left untouched.
fn null_side_rejected(
    conjuncts: &[Expr],
    plan_tables: &[Option<PlanTable>],
    null_positions: &[usize],
) -> bool {
    let mut null_tables: Vec<&PlanTable> = Vec::with_capacity(null_positions.len());
    for &pos in null_positions {
        match plan_tables.get(pos).and_then(|t| t.as_ref()) {
            Some(t) => null_tables.push(t),
            None => return false,
        }
    }
    let other_tables: Vec<&PlanTable> = plan_tables
        .iter()
        .enumerate()
        .filter(|(i, _)| !null_positions.contains(i))
        .filter_map(|(_, t)| t.as_ref())
        .collect();
    conjuncts
        .iter()
        .any(|c| conjunct_rejects_nulls(c, &null_tables, &other_tables))
}

/// True if WHERE conjunct `c` is guaranteed to be FALSE/UNKNOWN whenever every
/// column of the `null_tables` is NULL (i.e. null-rejecting on that side).
/// Conservative: only forms whose null-intolerance is provable are accepted.
fn conjunct_rejects_nulls(c: &Expr, null_tables: &[&PlanTable], others: &[&PlanTable]) -> bool {
    match c {
        Expr::Binary { left, op, right } => match op {
            BinaryOp::And => {
                conjunct_rejects_nulls(left, null_tables, others)
                    || conjunct_rejects_nulls(right, null_tables, others)
            }
            BinaryOp::Or => {
                conjunct_rejects_nulls(left, null_tables, others)
                    && conjunct_rejects_nulls(right, null_tables, others)
            }
            // Standard comparisons are NULL when an operand is NULL, so a
            // strict-null column reference on either side rejects. NullSafeEq
            // (`<=>`) is excluded: `NULL <=> NULL` is TRUE.
            BinaryOp::Eq
            | BinaryOp::NotEq
            | BinaryOp::Lt
            | BinaryOp::LtEq
            | BinaryOp::Gt
            | BinaryOp::GtEq => {
                strict_null_on(left, null_tables, others)
                    || strict_null_on(right, null_tables, others)
            }
            _ => false,
        },
        Expr::IsNotNull(e) => strict_null_on(e, null_tables, others),
        Expr::InList {
            expr,
            negated: false,
            ..
        } => strict_null_on(expr, null_tables, others),
        Expr::Between {
            expr,
            negated: false,
            ..
        } => strict_null_on(expr, null_tables, others),
        Expr::Like {
            expr,
            negated: false,
            ..
        } => strict_null_on(expr, null_tables, others),
        _ => false,
    }
}

/// True if `e` is a bare column reference that resolves to one of `null_tables`
/// — i.e. it is guaranteed NULL when that side is null-extended. Does not
/// descend through functions/arithmetic (e.g. `COALESCE(col, 0)` is NOT
/// strict-null), so the analysis never produces a false positive.
fn strict_null_on(e: &Expr, null_tables: &[&PlanTable], others: &[&PlanTable]) -> bool {
    match e {
        Expr::Identifier(name) => {
            let in_null = null_tables.iter().any(|t| table_has_column(t, name));
            if !in_null {
                return false;
            }
            // Disambiguate: an unqualified name that also exists on a
            // preserved-side table could bind there instead — too risky.
            let in_other = others.iter().any(|t| table_has_column(t, name));
            !in_other
        }
        Expr::CompoundIdentifier(parts) => {
            let Some(col) = parts.last() else {
                return false;
            };
            if parts.len() < 2 {
                return false;
            }
            let qualifier = parts[parts.len() - 2].as_str();
            null_tables.iter().any(|t| {
                matches_table(qualifier, &t.schema.name, t.alias.as_deref())
                    && table_has_column(t, col)
            })
        }
        _ => false,
    }
}

fn table_has_column(table: &PlanTable, column: &str) -> bool {
    table
        .schema
        .columns
        .iter()
        .any(|c| c.name.eq_ignore_ascii_case(column))
}

type EqColumnKey = (Option<String>, String);

fn equivalence_column_key(expr: &Expr) -> Option<EqColumnKey> {
    match expr {
        Expr::Identifier(name) => Some((None, normalize_expr_column(name))),
        Expr::CompoundIdentifier(parts) => {
            let column = parts.last()?.clone();
            let qualifier = if parts.len() >= 2 {
                Some(parts[parts.len() - 2].to_ascii_lowercase())
            } else {
                None
            };
            Some((qualifier, normalize_expr_column(&column)))
        }
        _ => None,
    }
}

fn equivalence_column_to_expr(key: &EqColumnKey) -> Expr {
    match &key.0 {
        Some(qualifier) => Expr::CompoundIdentifier(vec![qualifier.clone(), key.1.clone()]),
        None => Expr::Identifier(key.1.clone()),
    }
}

fn plan_constraint_to_expr(value: &PlanConstraintValue) -> Expr {
    match value {
        PlanConstraintValue::Concrete(v) => Expr::BoundValue(v.clone()),
        PlanConstraintValue::Param(idx) => Expr::Placeholder(*idx),
    }
}

fn propagate_equivalence_constraints(db: &Database, select: &mut Select) {
    let sources: Vec<_> = select
        .from
        .iter()
        .chain(select.joins.iter().map(|j| &j.table))
        .filter_map(|t| db.table(&t.name).ok().map(|schema| (t.clone(), schema)))
        .collect();
    let domain = |key: &EqColumnKey| {
        let mut matches = sources
            .iter()
            .filter(|(t, _)| {
                key.0
                    .as_ref()
                    .is_none_or(|q| matches_table(q, &t.name, t.alias.as_deref()))
            })
            .filter_map(|(_, schema)| schema.column_index(&key.1).ok().map(|i| &schema.columns[i]));
        let c = matches.next()?;
        if matches.next().is_some() {
            return None;
        }
        Some(crate::storage::ParamTarget::for_column_with_collation(
            &c.data_type,
            c.unsigned,
            c.collation.as_deref(),
        ))
    };
    let mut where_conjuncts: Vec<Expr> = Vec::new();
    if let Some(e) = select.selection.take() {
        split_conjuncts(&e, &mut where_conjuncts);
    }
    // Inner JOINs share their ON clauses' equality information with the
    // WHERE-level analysis; OUTER JOINs are excluded because an ON-side
    // value can't be assumed to hold for the preserved side's rows.
    let mut join_conjunct_storage: Vec<Vec<Expr>> = Vec::new();
    for join in &select.joins {
        if join.join_type != JoinType::Inner {
            continue;
        }
        let mut tmp = Vec::new();
        if let Some(c) = &join.condition {
            split_conjuncts(c, &mut tmp);
        }
        join_conjunct_storage.push(tmp);
    }

    let mut direct: HashMap<EqColumnKey, PlanConstraintValue> = HashMap::new();
    let mut adj: HashMap<EqColumnKey, Vec<EqColumnKey>> = HashMap::new();
    let mut existing_eq_pairs: HashSet<(EqColumnKey, EqColumnKey)> = HashSet::new();
    let analysis = where_conjuncts
        .iter()
        .chain(join_conjunct_storage.iter().flatten());
    for c in analysis {
        let Expr::Binary {
            left,
            op: BinaryOp::Eq,
            right,
        } = c
        else {
            continue;
        };
        let lkey = equivalence_column_key(left);
        let rkey = equivalence_column_key(right);
        let lcv = expr_to_plan_constraint(left);
        let rcv = expr_to_plan_constraint(right);
        match (lkey, rkey, lcv, rcv) {
            (Some(lk), Some(rk), _, _) => {
                if domain(&lk).is_none() || domain(&lk) != domain(&rk) {
                    continue;
                }
                adj.entry(lk.clone()).or_default().push(rk.clone());
                adj.entry(rk.clone()).or_default().push(lk.clone());
                existing_eq_pairs.insert((lk.clone(), rk.clone()));
                existing_eq_pairs.insert((rk, lk));
            }
            (Some(lk), None, _, Some(v)) => {
                direct.entry(lk).or_insert(v);
            }
            (None, Some(rk), Some(v), _) => {
                direct.entry(rk).or_insert(v);
            }
            _ => {}
        }
    }

    if !direct.is_empty() && !adj.is_empty() {
        let mut emitted: HashSet<EqColumnKey> = direct.keys().cloned().collect();
        let mut new_constraints: Vec<(EqColumnKey, PlanConstraintValue)> = Vec::new();
        for (root, value) in &direct {
            let mut visited: HashSet<EqColumnKey> = HashSet::new();
            let mut stack = vec![root.clone()];
            while let Some(cur) = stack.pop() {
                if !visited.insert(cur.clone()) {
                    continue;
                }
                if !emitted.contains(&cur) {
                    new_constraints.push((cur.clone(), value.clone()));
                    emitted.insert(cur.clone());
                }
                if let Some(nbrs) = adj.get(&cur) {
                    for n in nbrs {
                        stack.push(n.clone());
                    }
                }
            }
        }

        for (col_key, val) in new_constraints {
            where_conjuncts.push(Expr::Binary {
                left: Box::new(equivalence_column_to_expr(&col_key)),
                op: BinaryOp::Eq,
                right: Box::new(plan_constraint_to_expr(&val)),
            });
        }
    }

    // ---- Transitive `col = col` closure synthesis ----
    //
    // For any equivalence class {c1, c2, ..., ck} formed by `col = col`
    // chains, emit every pairwise equality the user didn't write. This
    // gives the join-graph builder explicit edges between tables that
    // were only transitively connected, letting DPsub consider plans
    // like `(customer ⋈ nation)` directly instead of routing through a
    // bloated cross product. Safe under INNER JOIN semantics because
    // both sides must produce equal values for the original equalities
    // to hold.
    synthesize_transitive_equalities(&adj, &existing_eq_pairs, &mut where_conjuncts);

    select.selection = combine_conjuncts(where_conjuncts);
}

fn synthesize_transitive_equalities(
    adj: &HashMap<EqColumnKey, Vec<EqColumnKey>>,
    existing: &HashSet<(EqColumnKey, EqColumnKey)>,
    out: &mut Vec<Expr>,
) {
    if adj.len() < 3 {
        return;
    }
    let mut visited: HashSet<EqColumnKey> = HashSet::new();
    // Walk keys in sorted order so synthesized predicates land in a
    // deterministic position — stable AST shape keeps the plan-cache
    // fingerprint stable across runs.
    let mut roots: Vec<&EqColumnKey> = adj.keys().collect();
    roots.sort();
    for root in roots {
        if visited.contains(root) {
            continue;
        }
        let mut component: Vec<EqColumnKey> = Vec::new();
        let mut stack = vec![root.clone()];
        while let Some(cur) = stack.pop() {
            if !visited.insert(cur.clone()) {
                continue;
            }
            component.push(cur.clone());
            if let Some(nbrs) = adj.get(&cur) {
                for n in nbrs {
                    if !visited.contains(n) {
                        stack.push(n.clone());
                    }
                }
            }
        }
        if component.len() < 3 {
            continue;
        }
        component.sort();
        for i in 0..component.len() {
            for j in (i + 1)..component.len() {
                let pair = (component[i].clone(), component[j].clone());
                if existing.contains(&pair) {
                    continue;
                }
                out.push(Expr::Binary {
                    left: Box::new(equivalence_column_to_expr(&component[i])),
                    op: BinaryOp::Eq,
                    right: Box::new(equivalence_column_to_expr(&component[j])),
                });
            }
        }
    }
}

// ============================================================
// Sideways predicate propagation (SIP) across inner-join chains.
//
// `propagate_equivalence_constraints` already pushes column=column
// equalities through equivalence classes, but it can't move a
// non-equality dimension filter (e.g. `date_dim.d_year IN (...)` or
// `region.r_name = 'ASIA'`) onto a fact table reached via one or more
// equi-join hops.
// Without that propagation the CBO sees no filter on the fact
// table and happily picks `INLJ via fact's PK`, paying for every
// fact-table row even though 80 % will be discarded later by an
// upstream dimension join. PG avoids this naturally because its
// join-order DP considers (dim × dim ⋈ fact) and the small
// dim-side build pushes c_nationkey filter onto customer via
// `idx_customer_nation`.
//
// This pass mirrors that by adding a synthetic
//   `fact.fcol IN (SELECT dcol FROM dim WHERE <dim filters>)`
// (nested for multi-hop paths) onto the outer WHERE. The CBO then
// sees the fact table's effective cardinality drop and naturally
// picks a HashJoin / index lookup path on the fact side. Each
// synthetic IN-subquery is uncorrelated so mydb's existing
// IndexInLookup / IN-subquery HashSet machinery executes it once
// and reuses the lookup set.
//
// Knobs:
//   `MYDB_SIP_DISABLE=1` — kill switch (off).
//   `MYDB_SIP_MAX_HOPS=N` — cap path length (default 3).
//
// Safety:
//   - Only INNER/CROSS joins participate; OUTER joins are skipped (a
//     filter pushed onto the preserved side would be unsound).
//   - Targets must be reached from a filtered dimension source via
//     equality joins; the original predicates remain in place.
//   - The synthetic predicate is appended; the original AST and
//     CBO are free to ignore it if a different plan is cheaper.
// ============================================================

#[derive(Clone)]
struct SipStep {
    from_alias: String,
    from_col: String,
    // `to_alias` mirrors the next hop's `from_alias`; we keep it for
    // a clearer BFS path representation and future use (e.g. logging
    // the synthesized predicate's provenance).
    #[allow(dead_code)]
    to_alias: String,
    to_col: String,
}

struct SipTable {
    alias: String,
    table_name: String,
    columns: HashSet<String>,
    row_count: u64,
    schema: Arc<TableSchema>,
}

fn build_col_to_alias(tables: &[SipTable]) -> HashMap<String, String> {
    let mut owners: HashMap<String, Vec<String>> = HashMap::new();
    for t in tables {
        for c in &t.columns {
            owners.entry(c.clone()).or_default().push(t.alias.clone());
        }
    }
    owners
        .into_iter()
        .filter_map(|(c, mut o)| {
            o.sort();
            o.dedup();
            if o.len() == 1 {
                Some((c, o.into_iter().next().unwrap()))
            } else {
                None
            }
        })
        .collect()
}

fn drop_trivially_true(expr: Expr) -> Option<Expr> {
    if matches!(&expr, Expr::Literal(crate::parser::Literal::Bool(true))) {
        None
    } else {
        Some(expr)
    }
}

fn normalize_expr(expr: Expr) -> Expr {
    crate::util::stack::ensure_stack(
        crate::engine::executor::mem_budget::ControlledMemoryCategory::Executor,
        || normalize_expr_checked(expr),
    )
}

fn normalize_expr_checked(expr: Expr) -> Expr {
    let expr = recurse_normalize_children(expr);
    apply_local_rewrites(expr)
}

fn recurse_normalize_children(expr: Expr) -> Expr {
    match expr {
        Expr::Unary { op, expr } => Expr::Unary {
            op,
            expr: Box::new(normalize_expr(*expr)),
        },
        Expr::Binary { left, op, right } => Expr::Binary {
            left: Box::new(normalize_expr(*left)),
            op,
            right: Box::new(normalize_expr(*right)),
        },
        Expr::IsNull(e) => Expr::IsNull(Box::new(normalize_expr(*e))),
        Expr::IsNotNull(e) => Expr::IsNotNull(Box::new(normalize_expr(*e))),
        Expr::InList {
            expr,
            list,
            negated,
        } => Expr::InList {
            expr: Box::new(normalize_expr(*expr)),
            list: list.into_iter().map(normalize_expr).collect(),
            negated,
        },
        Expr::Between {
            expr,
            low,
            high,
            negated,
        } => Expr::Between {
            expr: Box::new(normalize_expr(*expr)),
            low: Box::new(normalize_expr(*low)),
            high: Box::new(normalize_expr(*high)),
            negated,
        },
        Expr::Like {
            expr,
            pattern,
            negated,
            escape_char,
        } => Expr::Like {
            expr: Box::new(normalize_expr(*expr)),
            pattern: Box::new(normalize_expr(*pattern)),
            negated,
            escape_char,
        },
        Expr::Case {
            operand,
            conditions,
            results,
            else_result,
        } => Expr::Case {
            operand: operand.map(|e| Box::new(normalize_expr(*e))),
            conditions: conditions
                .into_vec()
                .into_iter()
                .map(normalize_expr)
                .collect(),
            results: results.into_iter().map(normalize_expr).collect(),
            else_result: else_result.map(|e| Box::new(normalize_expr(*e))),
        },
        Expr::Function {
            name,
            args,
            distinct,
        } => Expr::Function {
            name,
            args: args.into_iter().map(normalize_expr).collect(),
            distinct,
        },
        // Subqueries, placeholders, identifiers, literals: leave intact.
        other => other,
    }
}

fn apply_local_rewrites(expr: Expr) -> Expr {
    match expr {
        Expr::Unary {
            op: UnaryOp::Not,
            expr: inner,
        } => apply_not(*inner),
        Expr::Binary {
            left,
            op: BinaryOp::Or,
            right,
        } => apply_or(*left, *right),
        Expr::Binary {
            left,
            op: BinaryOp::And,
            right,
        } => apply_and(*left, *right),
        other => other,
    }
}

fn apply_not(inner: Expr) -> Expr {
    match inner {
        Expr::Unary {
            op: UnaryOp::Not,
            expr,
        } => *expr,
        Expr::Binary {
            left,
            op: BinaryOp::And,
            right,
        } => apply_local_rewrites(Expr::Binary {
            left: Box::new(apply_not(*left)),
            op: BinaryOp::Or,
            right: Box::new(apply_not(*right)),
        }),
        Expr::Binary {
            left,
            op: BinaryOp::Or,
            right,
        } => apply_local_rewrites(Expr::Binary {
            left: Box::new(apply_not(*left)),
            op: BinaryOp::And,
            right: Box::new(apply_not(*right)),
        }),
        Expr::Binary { left, op, right } => {
            // SQL 3-valued logic: NOT(NULL op X) = NULL,
            // and (X inverse-op NULL) is also NULL — semantics preserved.
            let inverted = match op {
                BinaryOp::Eq => Some(BinaryOp::NotEq),
                BinaryOp::NotEq => Some(BinaryOp::Eq),
                BinaryOp::Lt => Some(BinaryOp::GtEq),
                BinaryOp::LtEq => Some(BinaryOp::Gt),
                BinaryOp::Gt => Some(BinaryOp::LtEq),
                BinaryOp::GtEq => Some(BinaryOp::Lt),
                _ => None,
            };
            match inverted {
                Some(new_op) => Expr::Binary {
                    left,
                    op: new_op,
                    right,
                },
                None => Expr::Unary {
                    op: UnaryOp::Not,
                    expr: Box::new(Expr::Binary { left, op, right }),
                },
            }
        }
        Expr::IsNull(e) => Expr::IsNotNull(e),
        Expr::IsNotNull(e) => Expr::IsNull(e),
        Expr::Between {
            expr,
            low,
            high,
            negated,
        } => Expr::Between {
            expr,
            low,
            high,
            negated: !negated,
        },
        Expr::InList {
            expr,
            list,
            negated,
        } => Expr::InList {
            expr,
            list,
            negated: !negated,
        },
        Expr::Literal(crate::parser::Literal::Bool(b)) => {
            Expr::Literal(crate::parser::Literal::Bool(!b))
        }
        other => Expr::Unary {
            op: UnaryOp::Not,
            expr: Box::new(other),
        },
    }
}

fn apply_and(left: Expr, right: Expr) -> Expr {
    if let Some(b) = expr_bool_literal(&left) {
        return if b {
            right
        } else {
            Expr::Literal(crate::parser::Literal::Bool(false))
        };
    }
    if let Some(b) = expr_bool_literal(&right) {
        return if b {
            left
        } else {
            Expr::Literal(crate::parser::Literal::Bool(false))
        };
    }
    Expr::Binary {
        left: Box::new(left),
        op: BinaryOp::And,
        right: Box::new(right),
    }
}

fn apply_or(left: Expr, right: Expr) -> Expr {
    if let Some(b) = expr_bool_literal(&left) {
        return if b {
            Expr::Literal(crate::parser::Literal::Bool(true))
        } else {
            right
        };
    }
    if let Some(b) = expr_bool_literal(&right) {
        return if b {
            Expr::Literal(crate::parser::Literal::Bool(true))
        } else {
            left
        };
    }
    let combined = Expr::Binary {
        left: Box::new(left),
        op: BinaryOp::Or,
        right: Box::new(right),
    };
    let mut disjuncts = Vec::new();
    split_disjuncts(&combined, &mut disjuncts);
    if let Some(rewritten) = try_or_chain_to_in_list(&disjuncts) {
        return rewritten;
    }
    combined
}

fn split_disjuncts(expr: &Expr, out: &mut Vec<Expr>) {
    match expr {
        Expr::Binary {
            left,
            op: BinaryOp::Or,
            right,
        } => {
            split_disjuncts(left, out);
            split_disjuncts(right, out);
        }
        other => out.push(other.clone()),
    }
}

fn qualified_column_identity(expr: &Expr) -> Option<Vec<String>> {
    match expr {
        Expr::Identifier(name) => Some(vec![name.to_ascii_lowercase()]),
        Expr::CompoundIdentifier(parts) => {
            Some(parts.iter().map(|part| part.to_ascii_lowercase()).collect())
        }
        _ => None,
    }
}

fn try_or_chain_to_in_list(disjuncts: &[Expr]) -> Option<Expr> {
    if disjuncts.len() < 2 {
        return None;
    }
    let mut column_expr: Option<Expr> = None;
    let mut column_key: Option<Vec<String>> = None;
    let mut values: Vec<Expr> = Vec::new();
    // Each disjunct must reference the same column with one of:
    //   col = value  |  value = col  |  col IN (..)  (non-negated)
    // and `value`s must be literal / bound / placeholder so the merged
    // IN-list keeps the same param-cache friendliness as the input.
    for d in disjuncts {
        let (col_expr, vals) = match d {
            Expr::Binary {
                left,
                op: BinaryOp::Eq,
                right,
            } => {
                if expr_column_name(left).is_some() && expr_to_plan_constraint(right).is_some() {
                    ((**left).clone(), vec![(**right).clone()])
                } else if expr_column_name(right).is_some()
                    && expr_to_plan_constraint(left).is_some()
                {
                    ((**right).clone(), vec![(**left).clone()])
                } else {
                    return None;
                }
            }
            Expr::InList {
                expr,
                list,
                negated: false,
            } => {
                if expr_column_name(expr).is_some()
                    && list.iter().all(|v| expr_to_plan_constraint(v).is_some())
                {
                    ((**expr).clone(), list.clone())
                } else {
                    return None;
                }
            }
            _ => return None,
        };
        let key = qualified_column_identity(&col_expr)?;
        match column_key.as_ref() {
            None => {
                column_key = Some(key);
                column_expr = Some(col_expr);
            }
            Some(prev) if prev == &key => {}
            _ => return None,
        }
        values.extend(vals);
    }
    Some(Expr::InList {
        expr: Box::new(column_expr?),
        list: values,
        negated: false,
    })
}

fn expr_bool_literal(expr: &Expr) -> Option<bool> {
    if let Expr::Literal(crate::parser::Literal::Bool(b)) = expr {
        Some(*b)
    } else {
        None
    }
}

/// Attempt to rewrite a single-table aggregate into a covering-index
/// aggregate. Unlike the legacy `IndexAggregate` special case below,
/// this accepts multiple aggregate calls and simple GROUP BY columns,
/// but still requires every needed value to live in one index key.
/// The base table of a single-table scan plan, if `plan` is one of the
/// scan/lookup node kinds. Used to detect partitioned tables before
/// choosing index-only aggregate pushdowns that assume base-table
/// storage.
fn scan_plan_table(plan: &PhysicalPlan) -> Option<&PlanTable> {
    match plan {
        PhysicalPlan::SeqScan { table, .. }
        | PhysicalPlan::ZoneMapScan { table, .. }
        | PhysicalPlan::IndexLookup { table, .. }
        | PhysicalPlan::IndexMultiLookup { table, .. }
        | PhysicalPlan::IndexRangeScan { table, .. }
        | PhysicalPlan::IndexFastScan { table, .. } => Some(table),
        _ => None,
    }
}

fn aggregate_has_fixed_scale_approx_sum(
    aggregates: &[Expr],
    schema: &crate::storage::TableSchema,
) -> bool {
    aggregates.iter().any(|expr| {
        let Expr::Function { name, args, .. } = expr else {
            return false;
        };
        if !name.eq_ignore_ascii_case("SUM") {
            return false;
        }
        let column = match args.first() {
            Some(Expr::Identifier(column)) => column,
            Some(Expr::CompoundIdentifier(parts)) => match parts.last() {
                Some(column) => column,
                None => return false,
            },
            _ => return false,
        };
        schema.column_index(column).ok().is_some_and(|index| {
            matches!(
                schema.columns[index].data_type,
                crate::storage::schema::DataTypeKind::Float { scale: Some(_), .. }
                    | crate::storage::schema::DataTypeKind::Double { scale: Some(_), .. }
            )
        })
    })
}

/// Describe a scan whose predicate is fully enforced by its index bounds.
/// Scalar edge aggregates require an unbounded scan when no predicate exists.
fn aggregate_scan_input(
    select: &Select,
    plan: &PhysicalPlan,
    allow_bounded: bool,
) -> Option<(
    PlanTable,
    Option<IndexSchema>,
    Option<IndexBound>,
    Option<IndexBound>,
    f64,
)> {
    let (table, index, low, high, access_rows) = match plan {
        PhysicalPlan::SeqScan {
            table,
            filter,
            access_rows,
            ..
        } if filter.is_none() && select.selection.is_none() => {
            (table.clone(), None, None, None, *access_rows)
        }
        PhysicalPlan::IndexRangeScan {
            table,
            index,
            filter,
            low,
            high,
            access_rows,
            ..
        }
        | PhysicalPlan::IndexFastScan {
            table,
            index,
            filter,
            low,
            high,
            access_rows,
            ..
        } if filter.is_none()
            && select.selection.is_none()
            && (allow_bounded || (low.is_none() && high.is_none())) =>
        {
            (
                table.clone(),
                Some(index.clone()),
                low.clone(),
                high.clone(),
                *access_rows,
            )
        }
        PhysicalPlan::IndexRangeScan {
            table,
            index,
            filter: Some(filter),
            low,
            high,
            access_rows,
            ..
        }
        | PhysicalPlan::IndexFastScan {
            table,
            index,
            filter: Some(filter),
            low,
            high,
            access_rows,
            ..
        } if select.selection.as_ref() == Some(filter)
            && index_residual::all_covered(
                filter,
                &table.schema,
                index,
                low.as_ref(),
                high.as_ref(),
            ) =>
        {
            (
                table.clone(),
                Some(index.clone()),
                low.clone(),
                high.clone(),
                *access_rows,
            )
        }
        PhysicalPlan::IndexLookup {
            table,
            index,
            key,
            exact: true,
            filter: Some(filter),
            access_rows,
            ..
        } if select.selection.as_ref() == Some(filter)
            && index_residual::lookup_covered(filter, &table.schema, index, key) =>
        {
            let bound = IndexBound {
                key: key.clone(),
                inclusive: true,
            };
            (
                table.clone(),
                Some(index.clone()),
                Some(bound.clone()),
                Some(bound),
                *access_rows,
            )
        }
        _ => return None,
    };
    Some((table, index, low, high, access_rows))
}

fn try_covering_index_aggregate_pushdown(
    _ctx: &crate::engine::cbo::CboContext,
    select: &Select,
    plan: &PhysicalPlan,
) -> Option<PhysicalPlan> {
    if select.distinct
        || !select.joins.is_empty()
        || select.from.len() != 1
        || !select.order_by.is_empty()
        || select.limit.is_some()
        || select.offset.is_some()
    {
        return None;
    }
    // Partitioned tables keep one index/heap per partition; a covering
    // index aggregate reads the base index and cannot merge per-
    // partition partials. Fall back to a hash aggregate over the
    // partition-aware sequential scan.
    if scan_plan_table(plan).is_some_and(|t| t.schema.is_partitioned()) {
        return None;
    }
    let aggregates = collect_aggregate_calls(select);
    if aggregates.is_empty() {
        return None;
    }
    if aggregates
        .iter()
        .any(|expr| !covering_index_aggregate_call_supported(expr))
    {
        return None;
    }
    // Preserve the O(1) edge-walk path for the simple MIN/MAX cases the
    // existing specialised operator handles better than a full leaf scan.
    if aggregates.len() == 1
        && select.group_by.is_empty()
        && select.having.is_none()
        && select.projection.len() == 1
        && matches!(
            &aggregates[0],
            Expr::Function { name, args, distinct: false }
                if matches!(name.to_ascii_uppercase().as_str(), "MIN" | "MAX") && args.len() == 1
        )
    {
        return None;
    }

    let (table, scan_index, low, high, input_access_rows) =
        aggregate_scan_input(select, plan, true)?;

    if aggregate_has_fixed_scale_approx_sum(&aggregates, &table.schema) {
        // The covering aggregate stores only index values and drops the
        // catalog scale needed to normalize FLOAT(M,D)/DOUBLE(M,D) SUM
        // before HAVING and protocol rendering. Keep the scan/index choice
        // but finalize through the schema-preserving hash aggregate.
        return None;
    }
    // The covering path currently feeds raw index keys into DISTINCT state.
    // Numeric keys are already canonical; character keys require the
    // expression evaluator's explicit collation-key wrapper, so retain the
    // semantic oracle for those until that wrapper is compiled into the index
    // sink.
    for aggregate in &aggregates {
        let Expr::Function {
            args,
            distinct: true,
            ..
        } = aggregate
        else {
            continue;
        };
        let column = expr_column_name(args.first()?)?;
        let column_index = table.schema.column_index(column).ok()?;
        if !matches!(
            table.schema.columns[column_index].data_type,
            crate::storage::schema::DataTypeKind::TinyInt
                | crate::storage::schema::DataTypeKind::SmallInt
                | crate::storage::schema::DataTypeKind::MediumInt
                | crate::storage::schema::DataTypeKind::Int
                | crate::storage::schema::DataTypeKind::BigInt
                | crate::storage::schema::DataTypeKind::Bool
                | crate::storage::schema::DataTypeKind::Float { .. }
                | crate::storage::schema::DataTypeKind::Double { .. }
                | crate::storage::schema::DataTypeKind::Decimal { .. }
        ) {
            return None;
        }
    }

    let table_ref = &select.from[0];
    let hint_table_name = table
        .alias
        .as_deref()
        .or(table_ref.alias.as_deref())
        .unwrap_or(table_ref.name.as_str());
    if query_hint_full_scan(&select.query_hints, hint_table_name, &table.schema.name) {
        return None;
    }
    let no_index_hints =
        query_hint_no_index(&select.query_hints, hint_table_name, &table.schema.name);
    let use_index_hints =
        query_hint_use_index(&select.query_hints, hint_table_name, &table.schema.name);
    let required = covering_index_aggregate_required_columns(select, &table.schema)?;
    let covers_required = |idx: &IndexSchema| {
        if idx.has_prefix_columns()
            || idx.is_fulltext()
            || idx.is_spatial()
            || idx.is_vector()
            || idx.has_multi_valued_parts()
        {
            return false;
        }
        let cols: HashSet<String> = idx
            .effective_columns()
            .iter()
            .map(|c| normalize_expr_column(c))
            .collect();
        required.iter().all(|c| cols.contains(c))
    };
    let index = if let Some(index) = scan_index {
        if !index_allowed_by_hints(&index.name, &table_ref.index_hints, IndexHintScope::Join)
            || (!use_index_hints.is_empty()
                && !use_index_hints
                    .iter()
                    .any(|name| index.name.eq_ignore_ascii_case(name)))
            || no_index_hints
                .iter()
                .any(|name| index.name.eq_ignore_ascii_case(name))
            || !covers_required(&index)
        {
            return None;
        }
        index
    } else {
        table
            .schema
            .iter_queryable_indexes()
            .filter(|idx| {
                index_allowed_by_hints(&idx.name, &table_ref.index_hints, IndexHintScope::Join)
                    && (use_index_hints.is_empty()
                        || use_index_hints
                            .iter()
                            .any(|name| idx.name.eq_ignore_ascii_case(name)))
                    && !no_index_hints
                        .iter()
                        .any(|name| idx.name.eq_ignore_ascii_case(name))
                    && covers_required(idx)
            })
            .min_by_key(|idx| (idx.columns.len(), idx.effective_columns().len()))
            .cloned()?
    };

    let group_count = if select.group_by.is_empty() {
        1.0
    } else {
        estimate_group_by_rows_for_plan(plan, &select.group_by, input_access_rows)
    };
    Some(PhysicalPlan::CoveringIndexAggregate {
        table,
        index,
        low,
        high,
        group_by: select.group_by.clone(),
        having: select.having.clone(),
        aggregates,
        estimated_rows: group_count,
        access_rows: input_access_rows.max(1.0),
        cost: plan_cost(plan).clone(),
    })
}

fn covering_index_aggregate_call_supported(expr: &Expr) -> bool {
    let Expr::Function {
        name,
        args,
        distinct,
    } = expr
    else {
        return false;
    };
    match name.to_ascii_uppercase().as_str() {
        "COUNT" => {
            args.len() == 1
                && (!*distinct && matches!(args[0], Expr::Wildcard)
                    || expr_column_name(&args[0]).is_some())
        }
        "SUM" | "AVG" if *distinct => args.len() == 1 && expr_column_name(&args[0]).is_some(),
        "SUM" | "AVG" | "MIN" | "MAX" | "BIT_AND" | "BIT_OR" | "BIT_XOR" | "STD" | "STDDEV"
        | "STDDEV_POP" | "STDDEV_SAMP" | "VARIANCE" | "VAR_POP" | "VAR_SAMP" => {
            !*distinct && args.len() == 1 && expr_column_name(&args[0]).is_some()
        }
        _ => false,
    }
}

fn covering_index_aggregate_required_columns(
    select: &Select,
    table: &TableSchema,
) -> Option<HashSet<String>> {
    let mut required = HashSet::new();
    let mut group_cols = HashSet::new();
    for expr in &select.group_by {
        let col = expr_column_name(expr)?;
        table.column_index(col).ok()?;
        let normalized = normalize_expr_column(col);
        group_cols.insert(normalized.clone());
        required.insert(normalized);
    }
    for item in &select.projection {
        let SelectItem::Expr { expr, .. } = item else {
            return None;
        };
        collect_covering_index_aggregate_expr_columns(expr, table, &group_cols, &mut required)?;
    }
    if let Some(having) = &select.having {
        collect_covering_index_aggregate_expr_columns(having, table, &group_cols, &mut required)?;
    }
    Some(required)
}

fn collect_covering_index_aggregate_expr_columns(
    expr: &Expr,
    table: &TableSchema,
    group_cols: &HashSet<String>,
    required: &mut HashSet<String>,
) -> Option<()> {
    crate::parser::visit::try_inspect_value_expr(expr, |expr| {
        match expr {
            Expr::Function { name, args, .. } if is_aggregate_function(name) => {
                if !covering_index_aggregate_call_supported(expr) {
                    return Err(());
                }
                if !args.is_empty() && !matches!(args[0], Expr::Wildcard) {
                    let col = expr_column_name(&args[0]).ok_or(())?;
                    table.column_index(col).map_err(|_| ())?;
                    required.insert(normalize_expr_column(col));
                }
                return Ok(false);
            }
            Expr::Identifier(_) | Expr::CompoundIdentifier(_) => {
                let col = match expr {
                    Expr::Identifier(col) => col.as_str(),
                    Expr::CompoundIdentifier(parts) => parts.last().ok_or(())?.as_str(),
                    _ => unreachable!(),
                };
                let normalized = normalize_expr_column(col);
                if !group_cols.contains(&normalized) {
                    return Err(());
                }
                table.column_index(col).map_err(|_| ())?;
                required.insert(normalized);
            }
            Expr::Function { .. }
            | Expr::Subquery(_)
            | Expr::Exists { .. }
            | Expr::InSubquery { .. }
            | Expr::QuantifiedComparison {
                right: QuantifiedComparisonRight::Subquery(_),
                ..
            }
            | Expr::GroupKeyRef { .. }
            | Expr::Grouping(_)
            | Expr::Default(_)
            | Expr::DefaultQualified(_)
            | Expr::WindowFunction { .. } => return Err(()),
            _ => {}
        }
        Ok(true)
    })
    .ok()
}

/// Use the aggregate column's ordered index for a filtered scalar MIN/MAX.
///
/// The generic aggregate path must consume every qualifying row. An ordered
/// B-tree can instead start at the requested edge, apply the residual WHERE
/// predicate to heap rows, and stop at the first qualifying non-NULL value.
/// This is the aggregate analogue of ORDER BY key LIMIT 1, but remains a
/// distinct operator so SQL aggregate empty-input semantics still yield NULL.
///
/// A residual can be correlated with index order, so marginal selectivity is
/// not trusted literally. We use the same stable correlation-uncertainty floor
/// as ordered LIMIT costing and only replace the existing scan when the
/// resulting index+heap cost is lower. No runtime cache/RSS/load signal enters
/// this decision.
fn try_filtered_min_max_index_aggregate_pushdown(
    ctx: &crate::engine::cbo::CboContext,
    select: &Select,
    plan: &PhysicalPlan,
) -> Option<PhysicalPlan> {
    if select.group_by.len() != 0
        || select.having.is_some()
        || select.distinct
        || !select.joins.is_empty()
        || select.from.len() != 1
        || !select.order_by.is_empty()
        || select.limit.is_some()
        || select.offset.is_some()
        || select.projection.len() != 1
    {
        return None;
    }
    let filter = select.selection.clone()?;
    let table = scan_plan_table(plan)?.clone();
    if table.schema.is_partitioned() {
        return None;
    }
    let (expr, alias) = match &select.projection[0] {
        SelectItem::Expr { expr, alias } => (expr, alias.clone()),
        _ => return None,
    };
    let Expr::Function {
        name,
        args,
        distinct: false,
    } = expr
    else {
        return None;
    };
    if args.len() != 1 {
        return None;
    }
    let upper = name.to_ascii_uppercase();
    if !matches!(upper.as_str(), "MIN" | "MAX") {
        return None;
    }
    let col = match &args[0] {
        Expr::Identifier(name) => name.clone(),
        Expr::CompoundIdentifier(parts) => parts.last()?.clone(),
        _ => return None,
    };
    let normalized = col.to_ascii_lowercase();
    let col_idx = table
        .schema
        .columns
        .iter()
        .position(|column| column.name.eq_ignore_ascii_case(&col))?;
    let op = if upper == "MIN" {
        IndexAggOp::Min(normalized.clone())
    } else {
        IndexAggOp::Max(normalized.clone())
    };

    let table_ref = &select.from[0];
    let hint_table_name = table
        .alias
        .as_deref()
        .or(table_ref.alias.as_deref())
        .unwrap_or(table_ref.name.as_str());
    if query_hint_full_scan(&select.query_hints, hint_table_name, &table.schema.name) {
        return None;
    }
    let no_index_hints =
        query_hint_no_index(&select.query_hints, hint_table_name, &table.schema.name);
    let use_index_hints =
        query_hint_use_index(&select.query_hints, hint_table_name, &table.schema.name);
    let index = pick_index_for_pushdown(
        &table.schema,
        &op,
        &table_ref.index_hints,
        use_index_hints.as_slice(),
        no_index_hints.as_slice(),
    )?;

    let total_rows = (table.schema.stats.row_count as f64)
        .max(estimated_rows(plan))
        .max(1.0);
    let qualifying_rows = estimated_rows(plan).clamp(0.0, total_rows);
    let expected = expected_rows_to_reach_goal(total_rows, qualifying_rows, 1);
    let access_rows = if total_rows >= 10_000.0 {
        let correlation_uncertainty = (total_rows.sqrt() * 16.0).max(16_384.0);
        expected.max(correlation_uncertainty.min(total_rows))
    } else {
        expected
    };
    let row_width = crate::engine::cbo::operator_cost::estimated_row_width(&table.schema);
    let height = index_cost_height(&table.schema, &index, total_rows);
    let leaf_pages = index_cost_leaf_pages(&table.schema, &index, access_rows, total_rows);
    let cost = crate::engine::cbo::operator_cost::index_range_scan(
        ctx,
        height,
        leaf_pages,
        access_rows,
        total_rows,
        table_heap_pages_for_cost(&table.schema),
        row_width,
        index_cost_correlation(&table.schema, &index),
    );
    if cost.weighted_total() >= plan_cost(plan).weighted_total() {
        return None;
    }

    let agg_key = crate::engine::executor::select::aggregate::aggregate_key(expr);
    let output_alias = alias.unwrap_or_else(|| format!("{upper}({normalized})"));
    Some(PhysicalPlan::IndexAggregate {
        table,
        index,
        low: None,
        high: None,
        filter: Some(filter),
        op,
        agg_key,
        output_alias,
        output_type: scan_plan_table(plan)?.schema.result_type(col_idx),
        estimated_rows: 1.0,
        access_rows,
        cost,
    })
}

/// Attempt to rewrite `HashAggregate(input=base scan)` into a single
/// `IndexAggregate` operator. Recognised shapes:
///   * `SELECT COUNT(*) FROM t`                 — any index works
///   * `SELECT COUNT(*) FROM t WHERE k >= c`    — bounded index count
///   * `SELECT COUNT(DISTINCT col) FROM t`       — `col` must be the
///     complete first index column
///   * `SELECT MIN(col) FROM t`                 — `col` must be the
///     first index column
///   * `SELECT MAX(col) FROM t`                 — same constraint
///
/// Returns `None` when any simplifying assumption fails. For WHERE
/// pushdown we only accept predicates that the chosen index bounds
/// fully express; residual filters stay on the generic aggregate path.
fn try_index_aggregate_pushdown(
    _ctx: &crate::engine::cbo::CboContext,
    select: &Select,
    plan: &PhysicalPlan,
) -> Option<PhysicalPlan> {
    if !select.group_by.is_empty()
        || select.having.is_some()
        || select.distinct
        || !select.joins.is_empty()
        || select.from.len() != 1
        || !select.order_by.is_empty()
        || select.limit.is_some()
        || select.offset.is_some()
        || select.projection.len() != 1
    {
        return None;
    }
    // Partitioned tables keep one index/heap per partition; an index
    // aggregate reads the base index (empty) and cannot merge per-
    // partition partials (COUNT would sum wrong, MIN/MAX would miss
    // other partitions). Fall back to a regular aggregate over the
    // partition-aware sequential scan.
    if scan_plan_table(plan).is_some_and(|t| t.schema.is_partitioned()) {
        return None;
    }
    let (expr, alias) = match &select.projection[0] {
        SelectItem::Expr { expr, alias } => (expr, alias.clone()),
        _ => return None,
    };
    let Expr::Function {
        name,
        args,
        distinct,
    } = expr
    else {
        return None;
    };
    let upper = name.to_ascii_uppercase();
    let (table, scan_index, low, high, input_access_rows) =
        aggregate_scan_input(select, plan, false)?;
    let table_ref = &select.from[0];
    let hint_table_name = table
        .alias
        .as_deref()
        .or(table_ref.alias.as_deref())
        .unwrap_or(table_ref.name.as_str());
    if query_hint_full_scan(&select.query_hints, hint_table_name, &table.schema.name) {
        return None;
    }
    let (op, output_type) = match upper.as_str() {
        "COUNT" if !*distinct && args.len() == 1 && matches!(args[0], Expr::Wildcard) => {
            (IndexAggOp::CountStar, crate::types::ColumnType::LongLong)
        }
        "COUNT" if *distinct && args.len() == 1 => {
            let col = match &args[0] {
                Expr::Identifier(n) => n.clone(),
                Expr::CompoundIdentifier(parts) => parts.last()?.clone(),
                _ => return None,
            };
            table
                .schema
                .columns
                .iter()
                .position(|c| c.name.eq_ignore_ascii_case(&col))?;
            (
                IndexAggOp::CountDistinctColumn(col.to_ascii_lowercase()),
                crate::types::ColumnType::LongLong,
            )
        }
        "COUNT" if !*distinct && args.len() == 1 => {
            // `COUNT(col)`: equivalent to `COUNT(*)` for NOT NULL columns
            // (no row contributes a NULL); for nullable columns we still
            // push down but require the chosen index to cover `col` so
            // we can read the value out of the leaf entry and skip NULLs.
            let col = match &args[0] {
                Expr::Identifier(n) => n.clone(),
                Expr::CompoundIdentifier(parts) => parts.last()?.clone(),
                _ => return None,
            };
            let col_idx = table
                .schema
                .columns
                .iter()
                .position(|c| c.name.eq_ignore_ascii_case(&col))?;
            let normalized = col.to_ascii_lowercase();
            let op = if !table.schema.columns[col_idx].nullable {
                IndexAggOp::CountStar
            } else {
                IndexAggOp::CountColumn(normalized)
            };
            (op, crate::types::ColumnType::LongLong)
        }
        "MIN" | "MAX" if args.len() == 1 => {
            let col = match &args[0] {
                Expr::Identifier(n) => n.clone(),
                Expr::CompoundIdentifier(parts) => parts.last()?.clone(),
                _ => return None,
            };
            let normalized = col.to_ascii_lowercase();
            let col_idx = table
                .schema
                .columns
                .iter()
                .position(|c| c.name.eq_ignore_ascii_case(&col))?;
            let result_type = table.schema.result_type(col_idx);
            let op = if upper == "MIN" {
                IndexAggOp::Min(normalized)
            } else {
                IndexAggOp::Max(normalized)
            };
            (op, result_type)
        }
        _ => return None,
    };
    if select.selection.is_some()
        && !matches!(
            op,
            IndexAggOp::CountStar | IndexAggOp::CountColumn(_) | IndexAggOp::CountDistinctColumn(_)
        )
    {
        return None;
    }
    let no_index_hints =
        query_hint_no_index(&select.query_hints, hint_table_name, &table.schema.name);
    let use_index_hints =
        query_hint_use_index(&select.query_hints, hint_table_name, &table.schema.name);
    let index = if let Some(index) = scan_index {
        if !index_allowed_for_pushdown(
            &index,
            &op,
            &table_ref.index_hints,
            use_index_hints.as_slice(),
            no_index_hints.as_slice(),
        ) {
            return None;
        }
        index
    } else {
        pick_index_for_pushdown(
            &table.schema,
            &op,
            &table_ref.index_hints,
            use_index_hints.as_slice(),
            no_index_hints.as_slice(),
        )?
    };
    let agg_key = crate::engine::executor::select::aggregate::aggregate_key(expr);
    let output_alias = alias.unwrap_or_else(|| crate::engine::expr::expr_name(expr));
    // Cost model: covering range scan over the chosen index, no heap
    // touches. Tiny rows estimate (1) since aggregates always emit one
    // row.
    let cost = plan_cost(plan).clone();
    let access_rows = match &op {
        // MIN/MAX stop at the first visible non-NULL leaf entry in the
        // requested direction, so the scan work is normally a point at
        // the edge of the index rather than a full leaf walk.
        IndexAggOp::Min(_) | IndexAggOp::Max(_) => 1.0,
        IndexAggOp::CountStar | IndexAggOp::CountColumn(_) | IndexAggOp::CountDistinctColumn(_) => {
            input_access_rows.max(1.0)
        }
    };
    Some(PhysicalPlan::IndexAggregate {
        table,
        index,
        low,
        high,
        filter: None,
        op,
        agg_key,
        output_alias,
        output_type,
        estimated_rows: 1.0,
        access_rows,
        cost,
    })
}

fn index_allowed_for_pushdown(
    index: &IndexSchema,
    op: &IndexAggOp,
    index_hints: &[IndexHint],
    use_index_hints: &[&str],
    no_index_hints: &[&str],
) -> bool {
    if index.is_fulltext()
        || index.is_spatial()
        || index.is_vector()
        || index.has_multi_valued_parts()
        || !index_allowed_by_hints(&index.name, index_hints, IndexHintScope::Join)
        || (!use_index_hints.is_empty()
            && !use_index_hints
                .iter()
                .any(|name| index.name.eq_ignore_ascii_case(name)))
        || no_index_hints
            .iter()
            .any(|name| index.name.eq_ignore_ascii_case(name))
    {
        return false;
    }
    match op {
        IndexAggOp::CountStar => true,
        IndexAggOp::CountColumn(col) => index
            .effective_columns()
            .iter()
            .any(|c| c.eq_ignore_ascii_case(col)),
        IndexAggOp::CountDistinctColumn(col) => {
            !index.has_prefix_columns()
                && index
                    .columns
                    .first()
                    .is_some_and(|c| c.eq_ignore_ascii_case(col))
        }
        IndexAggOp::Min(col) | IndexAggOp::Max(col) => {
            index.column_prefix_len(0).is_none()
                && index
                    .columns
                    .first()
                    .is_some_and(|c| c.eq_ignore_ascii_case(col))
        }
    }
}

pub(crate) fn index_aggregate_filter_covered_by_bounds(expr: &Expr, index: &IndexSchema) -> bool {
    if index.has_prefix_columns() {
        return false;
    }
    let Some(first_column) = index.columns.first() else {
        return false;
    };
    expr_is_single_index_column_range_only(expr, first_column)
}

fn expr_is_single_index_column_range_only(expr: &Expr, column: &str) -> bool {
    match expr {
        Expr::Binary {
            left,
            op: BinaryOp::And,
            right,
        } => {
            expr_is_single_index_column_range_only(left, column)
                && expr_is_single_index_column_range_only(right, column)
        }
        Expr::Binary { left, op, right }
            if matches!(
                op,
                BinaryOp::Eq | BinaryOp::Gt | BinaryOp::GtEq | BinaryOp::Lt | BinaryOp::LtEq
            ) =>
        {
            comparison_is_index_column_constraint(left, right, column)
                || comparison_is_index_column_constraint(right, left, column)
        }
        Expr::Between {
            expr,
            negated: false,
            low,
            high,
        } => {
            expr_column_name(expr).is_some_and(|name| name.eq_ignore_ascii_case(column))
                && expr_to_plan_constraint(low).is_some()
                && expr_to_plan_constraint(high).is_some()
        }
        _ => false,
    }
}

fn comparison_is_index_column_constraint(
    column_expr: &Expr,
    value_expr: &Expr,
    column: &str,
) -> bool {
    expr_column_name(column_expr).is_some_and(|name| name.eq_ignore_ascii_case(column))
        && expr_to_plan_constraint(value_expr).is_some()
}

fn pick_index_for_pushdown(
    table: &TableSchema,
    op: &IndexAggOp,
    index_hints: &[IndexHint],
    use_index_hints: &[&str],
    no_index_hints: &[&str],
) -> Option<IndexSchema> {
    let hint_allows = |index: &IndexSchema| {
        !index.is_fulltext()
            && !index.is_spatial()
            && !index.is_vector()
            && !index.has_multi_valued_parts()
            && index_allowed_by_hints(&index.name, index_hints, IndexHintScope::Join)
            && (use_index_hints.is_empty()
                || use_index_hints
                    .iter()
                    .any(|name| index.name.eq_ignore_ascii_case(name)))
            && !no_index_hints
                .iter()
                .any(|name| index.name.eq_ignore_ascii_case(name))
    };
    match op {
        IndexAggOp::CountStar => {
            // Any index counts every row; prefer the narrowest one
            // (smallest leaves -> fewest visibility checks).
            table
                .iter_queryable_indexes()
                .filter(|i| hint_allows(i))
                .min_by_key(|i| i.columns.len())
                .cloned()
        }
        IndexAggOp::CountColumn(col) => {
            // Need to read `col` from the index leaf to skip NULL
            // entries — pick the narrowest index that *covers* `col`.
            table
                .iter_queryable_indexes()
                .filter(|i| hint_allows(i))
                .filter(|i| {
                    i.effective_columns()
                        .iter()
                        .any(|c| c.eq_ignore_ascii_case(col))
                })
                .min_by_key(|i| i.columns.len())
                .cloned()
        }
        IndexAggOp::CountDistinctColumn(col) => {
            // Prefix indexes can collapse unequal values, and a
            // non-leading column is not globally ordered.
            table
                .iter_queryable_indexes()
                .filter(|i| hint_allows(i))
                .filter(|i| !i.has_prefix_columns())
                .filter(|i| {
                    i.columns
                        .first()
                        .is_some_and(|c| c.eq_ignore_ascii_case(col))
                })
                .min_by_key(|i| i.columns.len())
                .cloned()
        }
        IndexAggOp::Min(col) | IndexAggOp::Max(col) => {
            // MIN/MAX needs `col` to be the first index column so
            // forward/reverse iteration sees it in sorted order.
            table
                .iter_queryable_indexes()
                .filter(|i| hint_allows(i))
                .find(|i| {
                    i.column_prefix_len(0).is_none()
                        && i.columns
                            .first()
                            .is_some_and(|c| c.eq_ignore_ascii_case(col))
                })
                .cloned()
        }
    }
}

/// Cheap upfront check used by `choose_scan` to decide whether a
/// candidate index will end up covering. Returns false when the caller
/// hasn't computed `required_columns` yet (multi-table path) — those
/// nodes get their `covering` flag flipped later by
/// `mark_covering_index_recursive`, so the cost model just falls back to
/// the conservative non-covering estimate. Mirrors the same column-set
/// check so the two stay in sync.
fn index_would_cover(index: &IndexSchema, hints: &ScanHints<'_>) -> bool {
    if index.has_prefix_columns() {
        return false;
    }
    // Functional indexes do not participate in covering scans today
    // (see `apply_covering_check` for the rationale).
    if index.has_expression_parts() {
        return false;
    }
    let Some(required) = hints.required_columns.as_ref() else {
        return false;
    };
    if required.is_empty() {
        return false;
    }
    let cols: HashSet<String> = index
        .effective_columns()
        .iter()
        .map(|c| normalize_expr_column(c))
        .collect();
    required.iter().all(|col| cols.contains(col))
}

// Walk the assembled physical plan and attach per-scan `projected_columns`
// lists so storage decodes only the columns referenced anywhere in the
// query — at any plan node, not just the outer SELECT AST.
//
// Design notes:
// * Pruning is computed **per base-scan node**, using the node's own
//   `PlanTable` (or, for `CteScan`, its emitted `columns` list) as the
//   universe. This avoids the false-positive over-retention that a
//   single global column-name set would cause when two tables share
//   the same column name (e.g. `id`).
// * The set of "referenced columns" is collected by walking the **plan
//   tree**, not the SELECT AST. Decorrelation passes routinely rewrite
//   correlated subqueries into joins whose `left_keys` / `right_keys`
//   / `residual` reference columns that don't textually appear in the
//   outer SELECT. A column-pruning pass that only looked at the AST
//   would drop those columns and crash the hash-join probe with
//   "hash join key field index out of bounds". Plan-tree collection
//   sees every expression the executor will evaluate.
// * `SELECT *` and `SELECT t.*` no longer disable pruning globally —
//   only the table(s) the wildcard targets are kept at full width.
//   Other tables in the same query still get pruned. This matters in
//   shapes like `SELECT * FROM small JOIN big ON …`, where keeping
//   `big` at full width was the original cost.
// * Pruning runs for **single-scan plans too**. The previous
//   "scan_count < 2 ⇒ no benefit" gate was wrong: the legacy
//   reasoning was that an upstream projection layer "reads only what
//   it needs", but that layer reads off an already-decoded `PlanRow`
//   — the unread columns were already decoded from the heap row, at
//   full per-row cost. Pruning at the scan eliminates that decode.
// ===================== S2: join-level output projection pruning =====================
//
// `apply_column_pruning` only pushes column pruning down to leaf scans; a multi-level join's
// intermediate schema = left columns ++ right columns accumulated level by level, so leaf columns
// along a K-level chain get cloned K times. This pass carries the "set of columns still referenced
// above" top-down and writes `output_projection` (the kept column positions of the output) for each
// HashJoin in pure join/scan subtrees; every executor hash join path honors it uniformly,
// `collect_plan_fields` reflects it in sync, and position compilation for upper layers (key plans / residual templates of outer joins) stays consistent.
//
// Safety rule: on any node whose output columns cannot be enumerated exactly (aggregate/window/CTE/
// derived table etc.), give up on that subtree as a whole; a SELECT containing a wildcard prunes
// nothing for the whole query. An empty projection defensively keeps the first column (zero-column rows never reach the executor).

/// The set of upper-layer references. A qualified key is `"<qualifier>\u{1}<column>"`, where the
/// qualifier may be a table name or an alias (both are looked up when matching); all lowercase.
#[derive(Default, Clone)]
struct JoinPruneNeed {
    qualified: HashSet<String>,
    bare: HashSet<String>,
}

#[derive(Clone)]
struct JoinPruneField {
    table: String,
    alias: Option<String>,
    column: String,
}

impl JoinPruneNeed {
    fn add_expr(&mut self, expr: &Expr) {
        // Unlike `walk_expr_for_idents`, descend into row constructors: the
        // executor still reads every column of `(a, b) IN (SELECT ...)`.
        inspect_query_value_expr(expr, &mut |e| {
            self.add_column_ref(e);
            true
        });
    }

    fn add_column_ref(&mut self, expr: &Expr) {
        match expr {
            Expr::Identifier(name) | Expr::Default(Some(name)) => {
                self.bare.insert(name.to_ascii_lowercase());
            }
            Expr::CompoundIdentifier(parts) | Expr::DefaultQualified(parts) => {
                if parts.len() >= 2 {
                    if let Some(col) = parts.last() {
                        let qualifier = parts[parts.len() - 2].to_ascii_lowercase();
                        self.qualified
                            .insert(format!("{qualifier}\u{1}{}", col.to_ascii_lowercase()));
                    }
                } else if let Some(col) = parts.last() {
                    self.bare.insert(col.to_ascii_lowercase());
                }
            }
            _ => {}
        }
    }

    fn matches(&self, field: &JoinPruneField) -> bool {
        if self.bare.contains(&field.column) {
            return true;
        }
        if self
            .qualified
            .contains(&format!("{}\u{1}{}", field.table, field.column))
        {
            return true;
        }
        if let Some(alias) = &field.alias {
            if self
                .qualified
                .contains(&format!("{alias}\u{1}{}", field.column))
            {
                return true;
            }
        }
        false
    }
}

fn plan_table_matches(t: &PlanTable, name: &str, alias: Option<&str>) -> bool {
    t.schema.name.eq_ignore_ascii_case(name)
        && match (&t.alias, alias) {
            (Some(a), Some(b)) => a.eq_ignore_ascii_case(b),
            (None, None) => true,
            _ => false,
        }
}

/// Collect both pruning target kinds in one walk, retaining the pruning
/// pass's scope boundaries (notably IndexInLookup and materialized queries).
fn collect_scan_targets(
    plan: &PhysicalPlan,
    out: &mut Vec<PlanTable>,
    ctes: &mut Vec<(String, Option<String>, Vec<crate::types::Column>)>,
) {
    let push_if_new = |out: &mut Vec<PlanTable>, t: &PlanTable| {
        if !out
            .iter()
            .any(|x| plan_table_matches(x, &t.schema.name, t.alias.as_deref()))
        {
            out.push(t.clone());
        }
    };
    match plan {
        PhysicalPlan::CteScan {
            name,
            alias,
            columns,
            ..
        } => {
            if !ctes.iter().any(|(n, a, _)| {
                n.eq_ignore_ascii_case(name)
                    && match (a, alias) {
                        (Some(a), Some(b)) => a.eq_ignore_ascii_case(b),
                        (None, None) => true,
                        _ => false,
                    }
            }) {
                ctes.push((name.clone(), alias.clone(), columns.clone()));
            }
        }
        PhysicalPlan::SeqScan { table, .. }
        | PhysicalPlan::ZoneMapScan { table, .. }
        | PhysicalPlan::IndexRangeScan { table, .. }
        | PhysicalPlan::IndexFastScan { table, .. }
        | PhysicalPlan::ParallelSeqScan { table, .. }
        | PhysicalPlan::ParallelZoneMapScan { table, .. }
        | PhysicalPlan::ParallelIndexFastScan { table, .. }
        | PhysicalPlan::ParallelIndexRangeScan { table, .. }
        | PhysicalPlan::IndexLookup { table, .. }
        | PhysicalPlan::IndexMultiLookup { table, .. }
        | PhysicalPlan::IndexInLookup { table, .. }
        | PhysicalPlan::FulltextScan { table, .. } => push_if_new(out, table),
        PhysicalPlan::IndexNestedLoopJoin {
            left, right_table, ..
        } => {
            collect_scan_targets(left, out, ctes);
            push_if_new(out, right_table);
        }
        PhysicalPlan::HashJoin { left, right, .. }
        | PhysicalPlan::NestedLoopJoin { left, right, .. }
        | PhysicalPlan::AdaptiveJoin { left, right, .. } => {
            collect_scan_targets(left, out, ctes);
            collect_scan_targets(right, out, ctes);
        }
        PhysicalPlan::HashAggregate { input, .. }
        | PhysicalPlan::HashRollupAggregate { input, .. }
        | PhysicalPlan::HashGroupingSetsAggregate { input, .. }
        | PhysicalPlan::StreamingAggregate { input, .. }
        | PhysicalPlan::StreamingRollupAggregate { input, .. }
        | PhysicalPlan::StreamingGroupingSetsAggregate { input, .. }
        | PhysicalPlan::Sort { input, .. }
        | PhysicalPlan::IncrementalSort { input, .. }
        | PhysicalPlan::TopK { input, .. }
        | PhysicalPlan::TopWindow { input, .. }
        | PhysicalPlan::ParallelTopK { input, .. }
        | PhysicalPlan::Limit { input, .. }
        | PhysicalPlan::Projection { input, .. }
        | PhysicalPlan::WindowAgg { input, .. }
        | PhysicalPlan::StreamingDistinct { input, .. }
        | PhysicalPlan::Exchange { input, .. }
        | PhysicalPlan::ParallelHashAggregate { input, .. }
        | PhysicalPlan::ParallelRollupAggregate { input, .. }
        | PhysicalPlan::ParallelGroupingSetsAggregate { input, .. }
        | PhysicalPlan::ParallelSort { input, .. } => collect_scan_targets(input, out, ctes),
        _ => {}
    }
}

fn indices_from_referenced(
    table: &PlanTable,
    referenced: &HashSet<String>,
) -> Option<Arc<Vec<usize>>> {
    use crate::storage::schema::ColumnState;
    let visible_total = table
        .schema
        .columns
        .iter()
        .filter(|c| c.state != ColumnState::Dropped)
        .count();
    if referenced.is_empty() {
        for (i, col) in table.schema.columns.iter().enumerate() {
            if col.state != ColumnState::Dropped {
                if visible_total == 1 {
                    return None;
                }
                return Some(Arc::new(vec![i]));
            }
        }
        return None;
    }
    let mut indices = Vec::with_capacity(referenced.len());
    for (i, col) in table.schema.columns.iter().enumerate() {
        if col.state == ColumnState::Dropped {
            continue;
        }
        if referenced.contains(&normalize_expr_column(&col.name)) {
            indices.push(i);
        }
    }
    if indices.is_empty() || indices.len() >= visible_total {
        return None;
    }
    Some(Arc::new(indices))
}

fn projection_covers_visible_columns(table: &PlanTable, projection: &[usize]) -> bool {
    use crate::storage::schema::ColumnState;
    let visible_total = table
        .schema
        .columns
        .iter()
        .filter(|c| c.state != ColumnState::Dropped)
        .count();
    projection.len() >= visible_total
}

fn referenced_covers_visible_columns(table: &PlanTable, referenced: &HashSet<String>) -> bool {
    use crate::storage::schema::ColumnState;
    !referenced.is_empty()
        && table
            .schema
            .columns
            .iter()
            .filter(|c| c.state != ColumnState::Dropped)
            .all(|c| referenced.contains(&normalize_expr_column(&c.name)))
}

fn indices_from_cte_referenced(
    cte_columns: &[crate::types::Column],
    referenced: &HashSet<String>,
) -> Option<Arc<Vec<usize>>> {
    let total = cte_columns.len();
    if referenced.is_empty() {
        if total <= 1 {
            return None;
        }
        return Some(Arc::new(vec![0]));
    }
    let mut indices = Vec::with_capacity(referenced.len());
    for (i, col) in cte_columns.iter().enumerate() {
        if referenced.contains(&normalize_expr_column(&col.name)) {
            indices.push(i);
        }
    }
    if indices.is_empty() || indices.len() >= total {
        return None;
    }
    Some(Arc::new(indices))
}

fn collect_expr_cte_column_refs(
    expr: &Expr,
    name: &str,
    alias: Option<&str>,
    cte_column_names: &HashSet<String>,
    out: &mut HashSet<String>,
) {
    inspect_query_value_expr(expr, &mut |expr| {
        match expr {
            Expr::Identifier(c) => {
                let n = normalize_expr_column(c);
                if cte_column_names.contains(&n) {
                    out.insert(n);
                }
            }
            Expr::CompoundIdentifier(parts) | Expr::DefaultQualified(parts) => {
                let Some(last) = parts.last() else {
                    return false;
                };
                let n = normalize_expr_column(last);
                let qualifier_ok = if parts.len() >= 2 {
                    let q = parts[parts.len() - 2].as_str();
                    // alias-shadows-name (see `matches_table`).
                    match alias {
                        Some(a) => q.eq_ignore_ascii_case(a),
                        None => q.eq_ignore_ascii_case(name),
                    }
                } else {
                    true
                };
                if qualifier_ok && cte_column_names.contains(&n) {
                    out.insert(n);
                }
            }
            Expr::Default(Some(column)) => {
                let normalized = normalize_expr_column(column);
                if cte_column_names.contains(&normalized) {
                    out.insert(normalized);
                }
            }
            _ => {}
        }
        true
    });
}

fn projected_indices_for_referenced(
    table: &PlanTable,
    referenced: &HashSet<String>,
) -> Option<Vec<usize>> {
    let total = table.schema.columns.len();
    let mut indices = Vec::with_capacity(total);
    for (i, col) in table.schema.columns.iter().enumerate() {
        if referenced.contains(&normalize_expr_column(&col.name)) {
            indices.push(i);
        }
    }
    if !indices.is_empty() && indices.len() < total {
        Some(indices)
    } else {
        None
    }
}

/// Replace an `IndexRangeScan` node in place with an `IndexFastScan` whose
/// cost is recomputed via `operator_cost::index_fast_scan` (no heap I/O
/// term). Used by `mark_covering_index_recursive` when a range scan turns
/// out to be covering after the post-pass column-set check, and by initial
/// planning helpers that need to swap a freshly-built range scan for a
/// fast scan once `will_cover` is decided.
fn convert_range_to_fast_scan(ctx: &crate::engine::cbo::CboContext, plan: &mut PhysicalPlan) {
    use crate::engine::cbo::operator_cost;
    let placeholder = PhysicalPlan::CteScan {
        name: String::new(),
        alias: None,
        columns: Vec::new(),
        filter: None,
        estimated_rows: 0.0,
        cost: crate::engine::cbo::Cost::default(),
        projected_columns: None,
    };
    let owned = std::mem::replace(plan, placeholder);
    if let PhysicalPlan::IndexRangeScan {
        table,
        index,
        low,
        high,
        filter,
        estimated_rows,
        access_rows,
        cost: _,
        possible_keys,
        reverse,
        hint_note,
        projected_columns,
        ..
    } = owned
    {
        let row_width = operator_cost::estimated_row_width(&table.schema);
        let total_rows = table.schema.stats.row_count as f64;
        let height = index_cost_height(&table.schema, &index, total_rows);
        let leaf_pages = index_cost_leaf_pages(&table.schema, &index, estimated_rows, total_rows);
        let cost =
            operator_cost::index_fast_scan(ctx, height, leaf_pages, estimated_rows, row_width);
        *plan = PhysicalPlan::IndexFastScan {
            table,
            index,
            low,
            high,
            filter,
            estimated_rows,
            access_rows,
            cost,
            possible_keys,
            reverse,
            hint_note,
            projected_columns,
        };
    } else {
        // Caller contract: only IndexRangeScan should reach this branch.
        unreachable!("convert_range_to_fast_scan called with non-range scan")
    }
}

/// Decide whether a single index access node can avoid heap fetches.
/// `extra_columns_expr` is for INLJ: the join condition references columns
/// that the right-side scan must materialise; we conservatively add every
/// identifier in it to the required set so a covering decision is safe
/// even when we can't tell which side a column belongs to.
fn apply_covering_check(
    table: &PlanTable,
    index: &IndexSchema,
    covering: &mut bool,
    select: &Select,
    extra_columns_expr: Option<&Expr>,
) {
    // Revalidate an earlier covering choice after physical predicate
    // placement; newly required columns can invalidate that choice.
    *covering = false;
    if index.has_prefix_columns() {
        return;
    }
    // Functional indexes can't satisfy a covering scan today: the
    // leaf stores the *computed* key value, and the IndexFastScan
    // executor would re-evaluate the residual filter / projection
    // against a reconstructed row that lacks the underlying base
    // columns. Treat them as non-covering until the executor learns
    // to substitute matching expressions with direct leaf reads.
    if index.has_expression_parts() {
        return;
    }
    let mut required = collect_required_columns(select, table);
    if let Some(expr) = extra_columns_expr {
        collect_expr_columns(expr, &mut required);
    }
    required.retain(|col| table_has_column_name(table, col));
    if required.is_empty() {
        return;
    }
    // Both declared key columns and INCLUDE columns can satisfy a
    // covering check.
    let index_columns: HashSet<String> = index
        .effective_columns()
        .iter()
        .map(|c| normalize_expr_column(c))
        .collect();
    if required.iter().all(|col| index_columns.contains(col)) {
        *covering = true;
    }
}

fn combine_optional_exprs(left: Option<Expr>, right: Option<Expr>) -> Option<Expr> {
    match (left, right) {
        (None, None) => None,
        (Some(expr), None) | (None, Some(expr)) => Some(expr),
        (Some(left), Some(right)) => Some(Expr::Binary {
            left: Box::new(left),
            op: BinaryOp::And,
            right: Box::new(right),
        }),
    }
}

fn table_has_column_name(table: &PlanTable, normalized_column: &str) -> bool {
    table
        .schema
        .columns
        .iter()
        .any(|column| normalize_expr_column(&column.name) == normalized_column)
}

fn select_has_wildcard_projection(select: &Select) -> bool {
    select.projection.iter().any(|item| {
        matches!(
            item,
            SelectItem::Wildcard | SelectItem::QualifiedWildcard(_)
        )
    })
}

fn collect_required_columns_by_table(
    select: &Select,
    tables: &[PlanTable],
    base_predicates: &[Option<Expr>],
) -> Vec<Option<HashSet<String>>> {
    let mut columns = vec![HashSet::new(); tables.len()];

    for item in &select.projection {
        if let SelectItem::Expr { expr, .. } = item {
            collect_expr_columns_by_table(expr, tables, &mut columns);
        }
    }
    for aggregate in collect_aggregate_calls(select) {
        collect_expr_columns_by_table(&aggregate, tables, &mut columns);
    }
    if let Some(expr) = &select.selection {
        collect_expr_columns_by_table(expr, tables, &mut columns);
    }
    for join in &select.joins {
        if let Some(expr) = &join.condition {
            collect_expr_columns_by_table(expr, tables, &mut columns);
        }
    }
    for predicate in base_predicates.iter().flatten() {
        collect_expr_columns_by_table(predicate, tables, &mut columns);
    }
    for item in &select.order_by {
        collect_expr_columns_by_table(&item.expr, tables, &mut columns);
    }
    for expr in &select.group_by {
        collect_expr_columns_by_table(expr, tables, &mut columns);
    }
    if let Some(expr) = &select.having {
        collect_expr_columns_by_table(expr, tables, &mut columns);
    }

    columns
        .into_iter()
        .map(|set| if set.is_empty() { None } else { Some(set) })
        .collect()
}

fn collect_expr_columns_by_table(
    expr: &Expr,
    tables: &[PlanTable],
    columns: &mut [HashSet<String>],
) {
    inspect_query_value_expr(expr, &mut |expr| {
        match expr {
            Expr::Identifier(name) => {
                let normalized = normalize_expr_column(name);
                for (idx, table) in tables.iter().enumerate() {
                    if table_has_column_name(table, &normalized) {
                        columns[idx].insert(normalized.clone());
                    }
                }
            }
            Expr::CompoundIdentifier(parts) | Expr::DefaultQualified(parts) => {
                let Some(name) = parts.last() else {
                    return false;
                };
                let normalized = normalize_expr_column(name);
                let qualifier = if parts.len() >= 2 {
                    Some(parts[parts.len() - 2].as_str())
                } else {
                    None
                };
                for (idx, table) in tables.iter().enumerate() {
                    let table_matches = qualifier.is_none_or(|q| {
                        matches_table(q, &table.schema.name, table.alias.as_deref())
                    });
                    if table_matches && table_has_column_name(table, &normalized) {
                        columns[idx].insert(normalized.clone());
                    }
                }
            }
            Expr::Default(Some(name)) => {
                let normalized = normalize_expr_column(name);
                for (idx, table) in tables.iter().enumerate() {
                    if table_has_column_name(table, &normalized) {
                        columns[idx].insert(normalized.clone());
                    }
                }
            }
            Expr::Function { name, args, .. }
                if name.eq_ignore_ascii_case(crate::parser::FT_MATCH_FUNCTION) =>
            {
                // Fulltext column arguments are supplied by the inverted index.
                for arg in args.iter().take(2) {
                    collect_expr_columns_by_table(arg, tables, columns);
                }
                return false;
            }
            _ => {}
        }
        true
    });
}

fn collect_select_columns_by_table(
    select: &Select,
    tables: &[PlanTable],
    columns: &mut [HashSet<String>],
) {
    for item in &select.projection {
        if let SelectItem::Expr { expr, .. } = item {
            collect_expr_columns_by_table(expr, tables, columns);
        }
    }
    if let Some(e) = &select.selection {
        collect_expr_columns_by_table(e, tables, columns);
    }
    for j in &select.joins {
        if let Some(c) = &j.condition {
            collect_expr_columns_by_table(c, tables, columns);
        }
    }
    for e in &select.group_by {
        collect_expr_columns_by_table(e, tables, columns);
    }
    if let Some(h) = &select.having {
        collect_expr_columns_by_table(h, tables, columns);
    }
    for ob in &select.order_by {
        collect_expr_columns_by_table(&ob.expr, tables, columns);
    }
}

fn collect_required_columns(select: &Select, table: &PlanTable) -> HashSet<String> {
    let mut columns = HashSet::new();
    for item in &select.projection {
        match item {
            SelectItem::Wildcard | SelectItem::QualifiedWildcard(_) => {
                // SELECT * expands to the table's logically-visible
                // columns; tombstones left by INSTANT DROP COLUMN
                // must not surface to the user.
                for (_, col) in table.schema.iter_visible_columns() {
                    columns.insert(normalize_expr_column(&col.name));
                }
                return columns;
            }
            SelectItem::Expr { expr, .. } => collect_expr_columns(expr, &mut columns),
        }
    }
    if let Some(expr) = &select.selection {
        collect_expr_columns(expr, &mut columns);
    }
    for item in &select.order_by {
        collect_expr_columns(&item.expr, &mut columns);
    }
    for expr in &select.group_by {
        collect_expr_columns(expr, &mut columns);
    }
    if let Some(expr) = &select.having {
        collect_expr_columns(expr, &mut columns);
    }
    columns
}

fn collect_expr_columns(expr: &Expr, columns: &mut HashSet<String>) {
    inspect_query_value_expr(expr, &mut |expr| {
        match expr {
            Expr::Identifier(name) => {
                columns.insert(normalize_expr_column(name));
            }
            Expr::CompoundIdentifier(parts) | Expr::DefaultQualified(parts) => {
                if let Some(name) = parts.last() {
                    columns.insert(normalize_expr_column(name));
                }
            }
            Expr::Default(Some(name)) => {
                columns.insert(normalize_expr_column(name));
            }
            _ => {}
        }
        true
    });
}

/// Lowercased set of column names a window's `partition_by`, `order_by`
/// and argument expressions reference. The window executor uses this to
/// populate only the columns it will actually read into its working copy
/// of the input (`value_rows`), leaving the rest NULL — a wide
/// rollup/join result fed to a window that only touches a couple of key
/// columns no longer pays to clone its long-string payload columns
/// (TPC-DS Q67: 178 MiB → ~36 MiB working copy). Uses the shared exhaustive AST traversal through
/// [`collect_expr_columns`], with query scopes handled explicitly.
pub(crate) fn window_referenced_columns(
    partition_by: &[Expr],
    order_by: &[OrderBy],
    args: &[Expr],
) -> HashSet<String> {
    let mut cols = HashSet::new();
    for e in partition_by {
        collect_expr_columns(e, &mut cols);
    }
    for o in order_by {
        collect_expr_columns(&o.expr, &mut cols);
    }
    for a in args {
        collect_expr_columns(a, &mut cols);
    }
    cols
}

#[cfg(test)]
mod tests;

#[cfg(test)]
mod audit_arithmetic_tests {
    use super::*;
    #[test]
    fn planner_audit_series_extreme_distance_does_not_overflow() {
        let series = SeriesKind::Dec {
            start: i128::MIN,
            stop: i128::MAX,
            step: 1,
            scale: 1,
        };
        assert!(series_row_count(&series).is_finite());
        assert!(series_row_count(&series) > i128::MAX as f64);
        let descending = SeriesKind::Dec {
            start: i128::MAX,
            stop: i128::MIN,
            step: i128::MIN,
            scale: 1,
        };
        assert_eq!(series_row_count(&descending), 2.0);
    }
}
