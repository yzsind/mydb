mod column_batch;
use crate::engine::expr::expect_arg_count as expect_len;
use crate::engine::expr::{null_or, ComparisonProvenance};
use crate::parser::boolean::flip_cmp as flip;
use std::cell::{Cell, RefCell};
use std::cmp::Ordering;
use std::collections::{HashMap, HashSet};
use std::hash::{BuildHasher, Hash, Hasher};
use std::sync::atomic::{AtomicU64, Ordering as AtomicOrdering};
use std::sync::{Arc, OnceLock};

use crate::engine::expr::{
    coerce_hex_bytes_to_uint, compare_values, eval_arithmetic, eval_expr_legacy,
    eval_in_precomputed_values, eval_json_arrow, expr_column_index, expr_is_numeric_hexbit_literal,
    int_value, like_match_chars, literal_value, mysql_left, mysql_right, mysql_substring,
    null_cmp_binary, numeric, string_value, truthy, ValueKey,
};
use crate::parser::{BinaryOp, Expr, Literal, QuantifiedComparisonRight, UnaryOp};
use crate::storage::{error, TableSchema};
use crate::types::{ColumnType, QueryError, Value};

#[derive(Debug, Clone, Default)]
#[allow(dead_code)]
pub(crate) struct ExprCapability {
    pub(crate) supports_batch: bool,
    pub(crate) deterministic: bool,
    pub(crate) depends_on_session: bool,
    pub(crate) may_error: bool,
    pub(crate) legacy_escape: bool,
}

impl ExprCapability {
    /// Conservative scheduling contract, shared by filters, sort, joins and
    /// aggregation. `may_error` also protects diagnostic-producing kernels;
    /// individual typed shapes may separately prove stronger guarantees.
    pub(crate) fn requires_ordered_evaluation(&self) -> bool {
        self.may_error || !self.deterministic || self.depends_on_session
    }
}

#[derive(Debug, Clone)]
pub(crate) struct InputColumn {
    pub(crate) table: String,
    pub(crate) alias: Option<String>,
    pub(crate) name: String,
    pub(crate) column_type: ColumnType,
    /// MySQL result signedness, independent of the runtime `Value` lane.
    /// DECIMAL values do not encode this bit, but subtraction semantics and
    /// plan-cache identity depend on it.
    pub(crate) unsigned: bool,
    /// Resolved MySQL collation id for this column (`ColumnSchema.collation`
    /// with fallback to the table then database collation). `None` means
    /// the column carries no collation (numeric / date / binary types) and
    /// the caller should use the process default.
    ///
    /// Feeds LIKE's collation-aware matcher (B1 in the LIKE audit) and is
    /// mixed into `compute_fingerprint` so a compiled plan built under
    /// one column collation cannot be replayed against another.
    #[allow(dead_code)]
    pub(crate) collation_id: Option<u16>,
}

#[derive(Debug, Clone, Default)]
pub(crate) struct InputSchema {
    columns: Vec<InputColumn>,
    /// Complete bind-time descriptors parallel to `columns`. Execution still
    /// reads the compact InputColumn fields, while inference retains FSP,
    /// nullability, exact precision and declared byte bounds.
    bound_types: Vec<crate::engine::expr_type::ExprType>,
    by_column: HashMap<String, Vec<usize>>,
    by_qualified: HashMap<(String, String), usize>,
    bitwise_silent_string_columns: HashSet<usize>,
    fingerprint: u64,
}

impl InputSchema {
    pub(crate) fn from_table(table: &TableSchema) -> Self {
        let columns = table
            .columns
            .iter()
            .enumerate()
            .map(|(idx, column)| InputColumn {
                table: table.name.clone(),
                alias: None,
                name: column.name.clone(),
                column_type: table.result_type(idx),
                unsigned: column.unsigned
                    || matches!(
                        column.data_type,
                        crate::storage::DataTypeKind::Year
                            | crate::storage::DataTypeKind::Bit { .. }
                    ),
                collation_id: resolve_column_collation_id(table, column),
            })
            .collect();
        let bound_types = table
            .columns
            .iter()
            .enumerate()
            .map(|(idx, column)| {
                let source = table.wire_column_source(idx);
                crate::engine::expr_type::ExprType::from_data_type(
                    &column.data_type,
                    column.unsigned,
                    column.nullable,
                    source.length,
                    source.collation_id,
                )
            })
            .collect();
        // Single source of truth with `table_input_fingerprint`: the
        // fingerprint is computed the same way whether or not this schema
        // is materialised, so a `compile_once` cache probe can key on
        // `table_input_fingerprint(table)` and skip building the schema at
        // all on a cache hit.
        let fingerprint = table_input_fingerprint(table);
        let mut schema = Self::with_fingerprint(columns, fingerprint);
        schema.bound_types = bound_types;
        schema.bitwise_silent_string_columns = table
            .columns
            .iter()
            .enumerate()
            .filter_map(|(index, column)| {
                matches!(
                    column.data_type,
                    crate::storage::DataTypeKind::Text { .. }
                        | crate::storage::DataTypeKind::Blob { .. }
                )
                .then_some(index)
            })
            .collect();
        schema
    }

    fn with_fingerprint(columns: Vec<InputColumn>, fingerprint: u64) -> Self {
        let bound_types = columns
            .iter()
            .map(|column| {
                let mut source = crate::types::ColumnSource::default();
                if column.unsigned {
                    source.flags |= crate::mysql::protocol::UNSIGNED_FLAG;
                }
                source.collation_id = column.collation_id;
                crate::engine::expr_type::ExprType::from_column_type(column.column_type, &source)
            })
            .collect();
        let mut schema = Self {
            columns,
            bound_types,
            by_column: HashMap::new(),
            by_qualified: HashMap::new(),
            bitwise_silent_string_columns: HashSet::new(),
            fingerprint: 0,
        };
        schema.rebuild_lookup();
        schema.fingerprint = fingerprint;
        schema
    }

    pub(crate) fn new(columns: Vec<InputColumn>) -> Self {
        let mut schema = Self::with_fingerprint(columns, 0);
        schema.fingerprint = schema.compute_fingerprint();
        schema
    }

    pub(crate) fn new_with_bound_types(
        columns: Vec<InputColumn>,
        bound_types: Vec<crate::engine::expr_type::ExprType>,
    ) -> Self {
        debug_assert_eq!(columns.len(), bound_types.len());
        let mut schema = Self::with_fingerprint(columns, 0);
        schema.bound_types = bound_types;
        schema.fingerprint = schema.compute_fingerprint();
        schema
    }

    /// Preserve the catalog distinction between TEXT/BLOB and their bounded
    /// CHAR/VARCHAR/BINARY counterparts when an InputSchema is reconstructed
    /// from a batch/PlanRow schema. Runtime `Value` and protocol ColumnType do
    /// not carry that distinction, but MySQL warning semantics do.
    pub(crate) fn mark_silent_string_columns(&mut self, columns: impl IntoIterator<Item = usize>) {
        self.bitwise_silent_string_columns.extend(columns);
        // This attribute changes observable warning/error semantics, so it is
        // part of the compiled-program cache identity just like collation and
        // signedness. Recompute after late BatchSchema/PlanRow enrichment.
        self.fingerprint = self.compute_fingerprint();
    }

    pub(crate) fn fingerprint(&self) -> u64 {
        self.fingerprint
    }

    /// Capacity-based heap ownership used when a stable BatchSchema retains
    /// this object as optional derived-cache state. Hash-table bucket control
    /// bytes are implementation-private, so charge one extra byte per usable
    /// slot in addition to the stored key/value capacity.
    pub(crate) fn heap_capacity_bytes(&self) -> usize {
        let columns = self
            .columns
            .capacity()
            .saturating_mul(std::mem::size_of::<InputColumn>())
            .saturating_add(self.columns.iter().fold(0usize, |bytes, column| {
                bytes
                    .saturating_add(column.table.capacity())
                    .saturating_add(column.alias.as_ref().map_or(0, String::capacity))
                    .saturating_add(column.name.capacity())
            }));
        let bound_types = self
            .bound_types
            .capacity()
            .saturating_mul(std::mem::size_of::<crate::engine::expr_type::ExprType>());
        let by_column = self.by_column.capacity().saturating_mul(
            std::mem::size_of::<String>()
                .saturating_add(std::mem::size_of::<Vec<usize>>())
                .saturating_add(1),
        );
        let by_column_payload = self.by_column.iter().fold(0usize, |bytes, (key, indexes)| {
            bytes.saturating_add(key.capacity()).saturating_add(
                indexes
                    .capacity()
                    .saturating_mul(std::mem::size_of::<usize>()),
            )
        });
        let by_qualified = self.by_qualified.capacity().saturating_mul(
            std::mem::size_of::<(String, String)>()
                .saturating_add(std::mem::size_of::<usize>())
                .saturating_add(1),
        );
        let by_qualified_payload =
            self.by_qualified
                .keys()
                .fold(0usize, |bytes, (qualifier, column)| {
                    bytes
                        .saturating_add(qualifier.capacity())
                        .saturating_add(column.capacity())
                });
        let silent = self
            .bitwise_silent_string_columns
            .capacity()
            .saturating_mul(std::mem::size_of::<usize>().saturating_add(1));
        columns
            .saturating_add(bound_types)
            .saturating_add(by_column)
            .saturating_add(by_column_payload)
            .saturating_add(by_qualified)
            .saturating_add(by_qualified_payload)
            .saturating_add(silent)
    }

    pub(crate) fn column_type(&self, index: usize) -> Option<ColumnType> {
        self.columns.get(index).map(|column| column.column_type)
    }

    pub(crate) fn column_unsigned(&self, index: usize) -> Option<bool> {
        self.columns.get(index).map(|column| column.unsigned)
    }

    /// Resolve a column reference to its effective collation info, or
    /// `None` when the expression isn't a plain column ref (function
    /// call, arithmetic, subquery), the column doesn't carry a
    /// collation (numeric / temporal types), or the id fails
    /// the `resolve_collation_id` sanity check.
    ///
    /// Feeds LIKE's collation-aware classifier (B1) so `col LIKE ...`
    /// picks up `ColumnSchema.collation` instead of the process default.
    pub(crate) fn column_collation_info(
        &self,
        expr: &Expr,
    ) -> Option<&'static crate::charset::CollationInfo> {
        let idx = match expr {
            Expr::Identifier(name) => self.column_index(None, name).ok()?,
            Expr::CompoundIdentifier(parts) => {
                let column = parts.last()?;
                let qualifier = (parts.len() >= 2).then(|| parts[parts.len() - 2].as_str());
                self.column_index(qualifier, column).ok()?
            }
            _ => return None,
        };
        let cid = self.columns.get(idx)?.collation_id?;
        crate::charset::resolve_collation_id(cid)
    }

    pub(crate) fn column_index(
        &self,
        qualifier: Option<&str>,
        column: &str,
    ) -> Result<usize, QueryError> {
        let col_key = column.to_ascii_lowercase();
        if let Some(qualifier) = qualifier {
            let index = self
                .by_qualified
                .get(&(qualifier.to_ascii_lowercase(), col_key))
                .copied();
            if index == Some(usize::MAX) {
                return Err(error(
                    crate::error::catalog::ER_NON_UNIQ_ERROR,
                    format!("column '{column}' is ambiguous"),
                ));
            }
            return index.ok_or_else(|| {
                error(
                    crate::error::catalog::ER_BAD_FIELD_ERROR,
                    format!("unknown column '{column}'"),
                )
            });
        }
        match self.by_column.get(&col_key) {
            Some(indices) if indices.len() == 1 => Ok(indices[0]),
            Some(_) => Err(error(
                crate::error::catalog::ER_NON_UNIQ_ERROR,
                format!("column '{column}' is ambiguous"),
            )),
            None => Err(error(
                crate::error::catalog::ER_BAD_FIELD_ERROR,
                format!("unknown column '{column}'"),
            )),
        }
    }

    fn rebuild_lookup(&mut self) {
        for (idx, column) in self.columns.iter().enumerate() {
            let col_key = column.name.to_ascii_lowercase();
            self.by_column.entry(col_key.clone()).or_default().push(idx);
            // SQL alias-shadows-name: once a column comes from an aliased
            // source, only the alias is a valid qualifier. Mixing both
            // would have the second column with the same table name
            // silently overwrite the first's qualifier entry, then
            // resolve both `t.x` and `t_alias.x` to whichever field came
            // last (TPC-DS q47 / q57 self-joined CTEs).
            match &column.alias {
                Some(alias) => {
                    let qualifier = alias.to_ascii_lowercase();
                    self.by_qualified
                        .entry((qualifier.clone(), col_key))
                        .and_modify(|prior| {
                            *prior = if qualifier == "$group" {
                                idx
                            } else {
                                usize::MAX
                            }
                        })
                        .or_insert(idx);
                }
                None if !column.table.is_empty() => {
                    let qualifier = column.table.to_ascii_lowercase();
                    self.by_qualified
                        .entry((qualifier.clone(), col_key))
                        .and_modify(|prior| {
                            *prior = if qualifier == "$group" {
                                idx
                            } else {
                                usize::MAX
                            }
                        })
                        .or_insert(idx);
                }
                None => {}
            }
        }
    }

    fn compute_fingerprint(&self) -> u64 {
        // Fixed-seed ahash. SipHash-1-3 (std DefaultHasher) was the
        // previous backing algorithm — ahash matches its fixed-seed
        // behaviour (so cross-call stability is preserved) but is
        // roughly 2-5× faster on the short keys typical here.
        let mut hasher = schema_fingerprint_state().build_hasher();
        self.columns.len().hash(&mut hasher);
        for (index, column) in self.columns.iter().enumerate() {
            column.table.to_ascii_lowercase().hash(&mut hasher);
            column
                .alias
                .as_ref()
                .map(|alias| alias.to_ascii_lowercase())
                .hash(&mut hasher);
            column.name.to_ascii_lowercase().hash(&mut hasher);
            column.column_type.hash(&mut hasher);
            column.unsigned.hash(&mut hasher);
            // B1: mixing the resolved collation id into the fingerprint
            // means a compiled plan built when `c` was `utf8mb4_bin`
            // won't be replayed after `ALTER TABLE ... MODIFY c ...
            // COLLATE utf8mb4_0900_ai_ci` — the CI/CS decision of any
            // baked LIKE plan depends on this id.
            column.collation_id.hash(&mut hasher);
            self.bound_types[index].hash(&mut hasher);
            self.bitwise_silent_string_columns
                .contains(&index)
                .hash(&mut hasher);
        }
        hasher.finish()
    }
}

/// Resolve the effective collation id for a column, walking the
/// column → table fallback chain MySQL uses. Non-string columns
/// (numeric, temporal, spatial, vector) carry no
/// collation and yield `None`.
///
/// Callers use the id to key plan-cache entries and to drive
/// collation-aware LIKE (`match_like_pattern_with_info`); an unknown
/// collation name falls back to the process default rather than
/// silently corrupting the fingerprint.
pub(crate) fn resolve_column_collation_id(
    table: &TableSchema,
    column: &crate::storage::ColumnSchema,
) -> Option<u16> {
    use crate::storage::DataTypeKind;
    if matches!(
        column.data_type,
        DataTypeKind::Binary { .. } | DataTypeKind::VarBinary { .. } | DataTypeKind::Blob { .. }
    ) {
        return Some(crate::charset::BINARY_COLLATION_ID);
    }
    let is_string_kind = matches!(
        column.data_type,
        DataTypeKind::Char { .. }
            | DataTypeKind::VarChar { .. }
            | DataTypeKind::Text { .. }
            | DataTypeKind::Enum { .. }
            | DataTypeKind::Set { .. }
            | DataTypeKind::Json
    );
    if !is_string_kind {
        return None;
    }
    let name = column
        .collation
        .as_deref()
        .filter(|s| !s.is_empty())
        .unwrap_or(&table.collation);
    if name.is_empty() {
        return None;
    }
    crate::charset::resolve_collation(name).ok().map(|c| c.id)
}

/// Fingerprint identical to `InputSchema::from_table(table).fingerprint()`
/// but without materialising the `InputColumn` vec or the lookup maps.
///
/// `CompiledTableExpr::compile_once` used to build the full `InputSchema`
/// on every call purely to obtain `schema.fingerprint()` for the program
/// cache key — yet the compiled program is cached (≈258k hits / 5 misses in
/// a TPC-C profile), so the schema was rebuilt-and-dropped on essentially
/// every call. Keying the cache probe on this cheap fingerprint lets the
/// hit path skip the build entirely; `from_table` sets the same value so
/// the stored entry (built on a miss) and the probe agree by construction.
///
/// Must stay byte-for-byte hash-compatible with `InputSchema::compute_fingerprint`
/// applied to the columns `from_table` produces — a unit test enforces this.
pub(crate) fn table_input_fingerprint(table: &TableSchema) -> u64 {
    thread_local! {
        static TBL_BUF: RefCell<String> = const { RefCell::new(String::new()) };
        static NAME_BUF: RefCell<String> = const { RefCell::new(String::new()) };
    }
    let mut hasher = schema_fingerprint_state().build_hasher();
    table.columns.len().hash(&mut hasher);
    TBL_BUF.with(|tbl| {
        NAME_BUF.with(|name| {
            let mut tbl = tbl.borrow_mut();
            let mut name = name.borrow_mut();
            // `from_table` sets every column's `table` to `table.name`, so
            // the lowercased table name is constant across the loop.
            tbl.clear();
            tbl.push_str(&table.name);
            tbl.make_ascii_lowercase();
            for (idx, column) in table.columns.iter().enumerate() {
                // Hashing a `&str` is identical to hashing the owned
                // `String` the original `compute_fingerprint` produced
                // (String::hash delegates to str::hash).
                tbl.as_str().hash(&mut hasher);
                None::<String>.hash(&mut hasher); // alias is always None here
                name.clear();
                name.push_str(&column.name);
                name.make_ascii_lowercase();
                name.as_str().hash(&mut hasher);
                table.result_type(idx).hash(&mut hasher);
                (column.unsigned
                    || matches!(
                        column.data_type,
                        crate::storage::DataTypeKind::Year
                            | crate::storage::DataTypeKind::Bit { .. }
                    ))
                .hash(&mut hasher);
                // Must match `InputSchema::compute_fingerprint` — B1
                // plan-cache key includes the per-column collation id
                // so LIKE plans built under one collation don't leak
                // across `ALTER TABLE ... COLLATE ...`.
                resolve_column_collation_id(table, column).hash(&mut hasher);
                let source = table.wire_column_source(idx);
                crate::engine::expr_type::ExprType::from_data_type(
                    &column.data_type,
                    column.unsigned,
                    column.nullable,
                    source.length,
                    source.collation_id,
                )
                .hash(&mut hasher);
                matches!(
                    column.data_type,
                    crate::storage::DataTypeKind::Text { .. }
                        | crate::storage::DataTypeKind::Blob { .. }
                )
                .hash(&mut hasher);
            }
        })
    });
    hasher.finish()
}

fn schema_fingerprint_state() -> &'static ahash::RandomState {
    static STATE: OnceLock<ahash::RandomState> = OnceLock::new();
    STATE.get_or_init(|| {
        ahash::RandomState::with_seeds(
            0x4D59_4442_5343_4845u64, // "MYDBSCHE"
            0x4D41_4669_6E67_7270u64, // "MAFingrp"
            0x517A_4F8E_B62D_4413u64,
            0x9E37_79B9_7F4A_7C15u64,
        )
    })
}

pub(crate) trait RowSource {
    fn value_ref(&self, idx: usize) -> Option<&Value>;
    fn len(&self) -> usize;

    /// Contiguous `&[Value]` backing this row, when one exists. Slice- and
    /// `RowView`-backed sources return `Some` so the `LegacyEscape` path
    /// can hand the slice straight to `eval_expr_legacy` with no copy;
    /// composite sources (e.g. a joined left/right pair) return `None` and
    /// the caller materialises via `collect_values`.
    fn as_value_slice(&self) -> Option<&[Value]> {
        None
    }

    /// Materialise every column into an owned `Vec<Value>`, positionally
    /// aligned with `value_ref`. Used only by the `LegacyEscape` slow path
    /// for row sources that aren't slice-backed.
    fn collect_values(&self) -> Vec<Value> {
        (0..self.len())
            .map(|idx| self.value_ref(idx).cloned().unwrap_or(Value::Null))
            .collect()
    }
}

impl RowSource for &[Value] {
    fn value_ref(&self, idx: usize) -> Option<&Value> {
        self.get(idx)
    }

    fn len(&self) -> usize {
        <[Value]>::len(self)
    }

    fn as_value_slice(&self) -> Option<&[Value]> {
        Some(self)
    }
}

pub(crate) struct RowView<'a> {
    values: &'a [Value],
}

impl<'a> RowView<'a> {
    pub(crate) fn from_values(values: &'a [Value]) -> Self {
        Self { values }
    }
}

impl RowSource for RowView<'_> {
    fn value_ref(&self, idx: usize) -> Option<&Value> {
        self.values.get(idx)
    }

    fn len(&self) -> usize {
        self.values.len()
    }

    fn as_value_slice(&self) -> Option<&[Value]> {
        Some(self.values)
    }
}

/// Invocation-owned evaluation marker. Shared regex caches live in
/// `func_cache`; this empty state needs neither allocation nor a TLS borrow.
#[derive(Default)]
pub(crate) struct EvalScratch {}

thread_local! {
    /// When set, `compile_node` refuses sub-expression `LegacyEscape` and
    /// propagates `Unsupported` instead — restoring pre-escape compile
    /// semantics for static-validity / routing probes (CHECK and generated-
    /// column validation, join-residual compilability checks) that read
    /// "does it compile?" as "is this fully lowerable without legacy?".
    static COMPILE_NO_ESCAPE: Cell<bool> = const { Cell::new(false) };
    /// CHECK validation can admit known comparison semantics implemented by
    /// the scalar oracle without weakening optimizer compilability probes.
    static COMPILE_CHECK_COMPARISONS: Cell<bool> = const { Cell::new(false) };
    /// Aggregate projection/HAVING in the PlanRow oracle applies conditional
    /// functions to already-finalized runtime Values without table-column
    /// result coercion. The batch aggregate boundary selects the same mode so
    /// `COALESCE(SUM(x), 0)` on an empty group remains an integer zero rather
    /// than being shaped to SUM's declared DECIMAL type mid-expression.
    static COMPILE_DYNAMIC_CONDITIONAL: Cell<bool> = const { Cell::new(false) };
    /// L1 compiled-program cache: (generation seen at last access, map).
    /// The generation pairs with `PROGRAM_CACHE_GENERATION` so a global
    /// `FLUSH SQL_CACHE` clears the caller's L1 immediately and other
    /// threads' L1 caches lazily on next use.
    static PROGRAM_CACHE: RefCell<(u64, ProgramCache)> =
        RefCell::new((0, ProgramCache::default()));
}

/// Bumped by `flush_program_caches` (ALTER SYSTEM FLUSH SQL_CACHE).
/// Thread-local L1 caches compare-and-clear against this on access.
static PROGRAM_CACHE_GENERATION: AtomicU64 = AtomicU64::new(0);

/// Shared L2 for compiled programs. Worker threads spawned for parallel
/// execution start with an empty L1; without the L2 each of them
/// re-compiled the same expressions (N workers × M exprs per query).
/// 8 shards keep the mutex cold; values are `Arc` so a hit is one
/// refcount bump instead of a deep `ExprNode`-tree clone.
static PROGRAM_CACHE_L2: OnceLock<Vec<parking_lot::Mutex<ProgramCache>>> = OnceLock::new();

pub(crate) const PROGRAM_CACHE_L2_SHARDS: usize = 8;
pub(crate) const MAX_PROGRAM_CACHE_L2_SHARD_ENTRIES: usize = 2048;
const MAX_PROGRAM_CACHE_L2_SHARD_BYTES: usize = 8 * 1024 * 1024;
const MAX_PROGRAM_CACHE_L1_BYTES: usize = 8 * 1024 * 1024;

fn program_cache_l2() -> &'static [parking_lot::Mutex<ProgramCache>] {
    PROGRAM_CACHE_L2.get_or_init(|| {
        (0..PROGRAM_CACHE_L2_SHARDS)
            .map(|_| parking_lot::Mutex::new(ProgramCache::default()))
            .collect()
    })
}

/// Drop cached programs from this thread and shared L2. Other threads clear
/// their L1 on next use; active evaluations retain their own Arc references.
/// Cache statistics remain observational and do not perform this cleanup.
pub(crate) fn flush_program_caches() {
    let generation = PROGRAM_CACHE_GENERATION
        .fetch_add(1, AtomicOrdering::AcqRel)
        .wrapping_add(1);
    PROGRAM_CACHE.with(|cache| {
        *cache.borrow_mut() = (generation, ProgramCache::default());
    });
    for shard in program_cache_l2() {
        *shard.lock() = ProgramCache::default();
    }
}

#[inline]
fn program_cache_for_generation(
    cache: &mut (u64, ProgramCache),
    generation: u64,
) -> &mut ProgramCache {
    if cache.0 != generation {
        *cache = (generation, ProgramCache::default());
    }
    &mut cache.1
}

static COMPILE_CACHE_HITS: AtomicU64 = AtomicU64::new(0);
static COMPILE_CACHE_L2_HITS: AtomicU64 = AtomicU64::new(0);
static COMPILE_CACHE_MISSES: AtomicU64 = AtomicU64::new(0);
static COMPILE_UNSUPPORTED: AtomicU64 = AtomicU64::new(0);
static PROGRAM_EVAL_ROWS: AtomicU64 = AtomicU64::new(0);
static COLUMN_EVAL_ROWS: AtomicU64 = AtomicU64::new(0);
static LEGACY_FALLBACK_ROWS: AtomicU64 = AtomicU64::new(0);
/// Counts evaluations of an in-program `LegacyEscape` node — i.e. a
/// sub-expression the compiler couldn't lower, evaluated by the legacy
/// interpreter while the *rest* of the expression stays on the compiled
/// hot path. Distinct from `LEGACY_FALLBACK_ROWS`, which counts whole
/// expressions that fell back wholesale (`CompiledTableExpr::Legacy`).
/// Watching escape rise while fallback falls is how Phase 1 of the
/// expression-engine improvement plan is verified.
static LEGACY_ESCAPE_ROWS: AtomicU64 = AtomicU64::new(0);
static SOURCE_SENSITIVE_REJECTIONS: AtomicU64 = AtomicU64::new(0);
static LOCAL_ESCAPE_COMPILES: AtomicU64 = AtomicU64::new(0);
static UNESCAPABLE_REJECTIONS: AtomicU64 = AtomicU64::new(0);

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct ExprProgramStats {
    pub(crate) compile_cache_hits: u64,
    pub(crate) compile_cache_l2_hits: u64,
    pub(crate) compile_cache_misses: u64,
    pub(crate) compile_unsupported: u64,
    pub(crate) program_eval_rows: u64,
    pub(crate) column_eval_rows: u64,
    pub(crate) legacy_fallback_rows: u64,
    pub(crate) legacy_escape_rows: u64,
    pub(crate) l1_entries: u64,
    pub(crate) l1_bytes: u64,
    pub(crate) l2_entries: u64,
    pub(crate) l2_bytes: u64,
    pub(crate) source_sensitive_rejections: u64,
    pub(crate) local_escape_compiles: u64,
    pub(crate) unescapable_rejections: u64,
}

impl ExprProgramStats {
    /// Fraction (0..=100) of evaluated rows that left the compiled hot
    /// path entirely (`CompiledTableExpr::Legacy`). Sub-expression escapes
    /// are *not* counted here — they keep most of the tree compiled.
    pub(crate) fn legacy_fallback_pct(&self) -> f64 {
        let denom = self.program_eval_rows + self.legacy_fallback_rows;
        if denom == 0 {
            0.0
        } else {
            (self.legacy_fallback_rows as f64) * 100.0 / (denom as f64)
        }
    }
}

pub(crate) fn record_column_eval_rows(rows: u64) {
    COLUMN_EVAL_ROWS.fetch_add(rows, AtomicOrdering::Relaxed);
    PROGRAM_EVAL_ROWS.fetch_add(rows, AtomicOrdering::Relaxed);
}

pub(crate) fn record_legacy_fallback_row() {
    LEGACY_FALLBACK_ROWS.fetch_add(1, AtomicOrdering::Relaxed);
}

fn record_legacy_escape_row() {
    LEGACY_ESCAPE_ROWS.fetch_add(1, AtomicOrdering::Relaxed);
}

pub(crate) fn expr_program_stats() -> ExprProgramStats {
    let (l1_entries, l1_bytes) = PROGRAM_CACHE.with(|cache| {
        // Stale generations still own memory until the next cache lookup.
        // Sampling must neither evict those objects nor hide their residency.
        let cache = cache.borrow();
        let entries = cache.1.entry_count as u64;
        let bytes = cache.1.logical_bytes as u64;
        (entries, bytes)
    });
    let (l2_entries, l2_bytes) =
        program_cache_l2()
            .iter()
            .fold((0_u64, 0_u64), |(entries, bytes), shard| {
                let shard = shard.lock();
                (
                    entries.saturating_add(shard.entry_count as u64),
                    bytes.saturating_add(shard.logical_bytes as u64),
                )
            });
    ExprProgramStats {
        compile_cache_hits: COMPILE_CACHE_HITS.load(AtomicOrdering::Relaxed),
        compile_cache_l2_hits: COMPILE_CACHE_L2_HITS.load(AtomicOrdering::Relaxed),
        compile_cache_misses: COMPILE_CACHE_MISSES.load(AtomicOrdering::Relaxed),
        compile_unsupported: COMPILE_UNSUPPORTED.load(AtomicOrdering::Relaxed),
        program_eval_rows: PROGRAM_EVAL_ROWS.load(AtomicOrdering::Relaxed),
        column_eval_rows: COLUMN_EVAL_ROWS.load(AtomicOrdering::Relaxed),
        legacy_fallback_rows: LEGACY_FALLBACK_ROWS.load(AtomicOrdering::Relaxed),
        legacy_escape_rows: LEGACY_ESCAPE_ROWS.load(AtomicOrdering::Relaxed),
        l1_entries,
        l1_bytes,
        l2_entries,
        l2_bytes,
        source_sensitive_rejections: SOURCE_SENSITIVE_REJECTIONS.load(AtomicOrdering::Relaxed),
        local_escape_compiles: LOCAL_ESCAPE_COMPILES.load(AtomicOrdering::Relaxed),
        unescapable_rejections: UNESCAPABLE_REJECTIONS.load(AtomicOrdering::Relaxed),
    }
}

#[inline]
pub(crate) fn with_eval_scratch<R>(
    f: impl FnOnce(&mut EvalScratch) -> Result<R, QueryError>,
) -> Result<R, QueryError> {
    f(&mut EvalScratch::default())
}

#[derive(Debug, Clone)]
pub(crate) struct ExprProgram {
    root: ExprNode,
    // Initialized only for consumers that need dependencies. Cold scalar
    // compilation must not allocate a dependency set it may never use.
    referenced_columns: OnceLock<Box<[usize]>>,
    // Counted in the existing capability walk; bounds lazy dependency storage
    // so cached programs reserve it before publication, without another walk.
    column_reference_bound: usize,
    capability: ExprCapability,
    /// Bound SQL descriptor retained across constant folding. The folded
    /// `ExprNode::Const(Value)` is only a payload and must never become the
    /// metadata oracle for compiled execution.
    result_type: crate::engine::expr_type::ExprType,
    /// Ordinary table expressions apply MySQL's statically inferred result
    /// domain to CASE/IF/IFNULL/COALESCE. Aggregate finalization is different:
    /// the row oracle selects from already-finalized runtime Values, so an
    /// empty `COALESCE(SUM(decimal_col), 0)` returns the integer literal 0.
    coerce_conditional_result: bool,
    /// One shared reservation covers the compiled tree and its L1/L2 map
    /// metadata. Because it is held by the program Arc, eviction cannot
    /// refund memory while an executing query still owns the program.
    cache_charge: Option<Arc<crate::engine::executor::mem_budget::ControlledMemoryCharge>>,
}

/// Cache key for the thread-local compiled-program LRU.
///
/// The previous shape included `expr_ptr: *const Expr as usize`. Every
/// re-parse (notably every `COM_QUERY` once auto-param is on, and every
/// `COM_STMT_PREPARE`) produced a fresh `Expr` allocation at a new
/// address, so the cache hit rate was effectively zero in production —
/// the structural fingerprint already determined equality, the pointer
/// just sharded the entries by allocation. The pointer is dropped here
/// in favour of pure structural identity (`expr_fingerprint`), which
/// matches what the rest of the project assumes about plan / statement
/// caches.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
struct ProgramCacheKey {
    expr_fingerprint: u64,
    schema_fingerprint: u64,
    semantics: ExprProgramSemantics,
}

/// A fingerprint is only an index into a collision bucket, never proof of
/// identity. Keeping the cloned AST beside the program makes cache reuse
/// exact (`Expr::PartialEq`) without formatting/allocating on the hit path.
#[derive(Clone)]
struct CachedProgram {
    expression: Arc<Expr>,
    value: Result<Arc<ExprProgram>, CompileError>,
    last_used: u64,
    logical_bytes: usize,
}

#[derive(Default)]
struct ProgramCache {
    buckets: HashMap<ProgramCacheKey, Vec<CachedProgram>>,
    clock: u64,
    entry_count: usize,
    logical_bytes: usize,
}

impl ProgramCache {
    fn get(
        &mut self,
        key: &ProgramCacheKey,
        expression: &Expr,
    ) -> Option<Result<Arc<ExprProgram>, CompileError>> {
        self.clock = self.clock.wrapping_add(1).max(1);
        let entry = self
            .buckets
            .get_mut(key)?
            .iter_mut()
            .find(|entry| entry.expression.as_ref() == expression)?;
        entry.last_used = self.clock;
        Some(entry.value.clone())
    }

    fn insert(
        &mut self,
        key: ProgramCacheKey,
        expression: Arc<Expr>,
        value: Result<Arc<ExprProgram>, CompileError>,
        max_entries: usize,
        max_bytes: usize,
    ) {
        self.clock = self.clock.wrapping_add(1).max(1);
        let logical_bytes = std::mem::size_of::<ProgramCacheKey>()
            .saturating_add(std::mem::size_of::<CachedProgram>())
            .saturating_add(value.as_ref().map_or(
                std::mem::size_of::<CompileError>(),
                |program| {
                    program
                        .charged_bytes()
                        .max(std::mem::size_of::<ExprProgram>())
                },
            ));
        if logical_bytes > max_bytes {
            return;
        }

        if let Some(existing) = self.buckets.get_mut(&key).and_then(|bucket| {
            bucket
                .iter_mut()
                .find(|entry| entry.expression.as_ref() == expression.as_ref())
        }) {
            self.logical_bytes = self.logical_bytes.saturating_sub(existing.logical_bytes);
            existing.value = value;
            existing.logical_bytes = logical_bytes;
            existing.last_used = self.clock;
            self.logical_bytes = self.logical_bytes.saturating_add(logical_bytes);
            return;
        }

        if self.entry_count >= max_entries
            || self.logical_bytes.saturating_add(logical_bytes) > max_bytes
        {
            self.evict_lru_batch(max_entries, max_bytes.saturating_sub(logical_bytes));
        }
        self.buckets.entry(key).or_default().push(CachedProgram {
            expression,
            value,
            last_used: self.clock,
            logical_bytes,
        });
        self.entry_count = self.entry_count.saturating_add(1);
        self.logical_bytes = self.logical_bytes.saturating_add(logical_bytes);
    }

    /// Evict the least recently used eighth of the entries (and more while
    /// above `byte_budget`) in one pass. A full cache then costs amortized
    /// O(log n) per insert instead of a full scan for every admission.
    fn evict_lru_batch(&mut self, max_entries: usize, byte_budget: usize) {
        let mut ages: Vec<(u64, usize)> = self
            .buckets
            .values()
            .flatten()
            .map(|entry| (entry.last_used, entry.logical_bytes))
            .collect();
        ages.sort_unstable_by_key(|(last_used, _)| *last_used);
        let target_entries = max_entries.saturating_sub((max_entries / 8).max(1));
        let mut remaining_entries = self.entry_count;
        let mut remaining_bytes = self.logical_bytes;
        let mut evicted = 0;
        while evicted < ages.len()
            && (remaining_entries > target_entries || remaining_bytes > byte_budget)
        {
            remaining_entries -= 1;
            remaining_bytes = remaining_bytes.saturating_sub(ages[evicted].1);
            evicted += 1;
        }
        let Some(&(threshold, _)) = evicted.checked_sub(1).and_then(|last| ages.get(last)) else {
            return;
        };
        self.buckets.retain(|_, bucket| {
            bucket.retain(|entry| entry.last_used > threshold);
            !bucket.is_empty()
        });
        self.entry_count = self.buckets.values().map(Vec::len).sum();
        self.logical_bytes = self
            .buckets
            .values()
            .flatten()
            .map(|entry| entry.logical_bytes)
            .sum();
    }
}

/// Session inputs consumed while binding or folding an expression program.
///
/// The statement/physical-plan cache has a wider semantics key, but the
/// expression L2 is process-global and can be reached independently by
/// workers from different sessions.  It therefore needs its own complete
/// identity instead of relying on an enclosing cache having partitioned the
/// request first.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
struct ExprProgramSemantics {
    div_precision_increment: u8,
    group_concat_max_len: u64,
    connection_collation_id: u16,
    connection_mbmaxlen: u8,
    no_unsigned_subtraction: bool,
    /// `Placeholder` result descriptors are derived from the live binding.
    /// This signature prevents a program compiled for (say) VARCHAR from
    /// being reused by a DECIMAL execution of the same prepared AST.
    parameter_types: u64,
}

impl ExprProgramSemantics {
    fn current() -> Self {
        Self {
            div_precision_increment: crate::engine::expr::session_div_precision_increment(),
            group_concat_max_len: crate::engine::executor::select::aggregate::group_concat_max_len()
                as u64,
            connection_collation_id: crate::engine::expr::connection_collation_id(),
            connection_mbmaxlen: crate::engine::expr::connection_charset_max_len(),
            no_unsigned_subtraction: crate::engine::expr::session_no_unsigned_subtraction(),
            parameter_types: crate::engine::expr_type::current_parameter_type_fingerprint(),
        }
    }
}

/// Temporal extraction shapes the vectorized executor can run directly
/// on a `Date32` / `DateTime64` column lane without materialising
/// per-row `Value`s. See `ExprProgram::as_temporal_extract`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum TemporalExtractKind {
    Year,
    Month,
    Day,
    Hour,
    Minute,
    Second,
    Quarter,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum StringLengthKind {
    Octets,
    Characters,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum StringTransformKind {
    Lower,
    Upper,
    Ltrim,
    Rtrim,
    Trim,
    Reverse,
}

/// Shared exact-numeric batch IR. Both fused aggregate expressions and the
/// general projection/order/group expression boundary consume this shape, so
/// arithmetic no longer has two independently recognized AST subsets.
#[derive(Debug, Clone)]
pub(crate) enum CompiledNumericExpr {
    Column(usize),
    Const(Value),
    Binary {
        left: Box<CompiledNumericExpr>,
        op: BinaryOp,
        right: Box<CompiledNumericExpr>,
    },
}

impl ExprProgram {
    #[inline]
    pub(crate) const fn result_type(&self) -> crate::engine::expr_type::ExprType {
        self.result_type
    }

    fn charged_bytes(&self) -> usize {
        self.cache_charge
            .as_ref()
            .map_or(0, |charge| charge.bytes())
    }

    /// Column dependencies of the lowered tree. Batch execution uses this to
    /// materialise only lanes actually read by the expression while retaining
    /// positional row indexing for legacy escapes and FIELD_COUNT().
    pub(crate) fn referenced_columns(&self) -> &[usize] {
        self.referenced_columns.get_or_init(|| {
            let mut columns = HashSet::new();
            collect_columns_in_node(&self.root, &mut columns);
            let mut columns: Vec<_> = columns.into_iter().collect();
            columns.sort_unstable();
            debug_assert!(columns.len() <= self.column_reference_bound);
            columns.into_boxed_slice()
        })
    }

    /// These string kernels never numerically coerce a source string. Their
    /// TEXT/BLOB operands therefore do not need the catalog-aware numeric
    /// warning oracle. Keep this proof separate from `may_error`: CONCAT can
    /// publish packet-size warnings and slicing still needs ordered evaluation.
    pub(crate) fn has_only_value_string_operations(&self) -> bool {
        fn integer_extent(node: &ExprNode) -> bool {
            match node {
                ExprNode::Const(value) => {
                    matches!(value, Value::Int(_) | Value::UInt(_) | Value::Null)
                }
                ExprNode::Placeholder(index) => crate::engine::params::current_param(*index)
                    .is_some_and(|value| {
                        matches!(value, Value::Int(_) | Value::UInt(_) | Value::Null)
                    }),
                _ => false,
            }
        }
        fn visit(node: &ExprNode) -> bool {
            match node {
                ExprNode::Column(_) | ExprNode::Const(_) | ExprNode::Placeholder(_) => true,
                ExprNode::SubstringConst { expr, .. } => visit(expr),
                ExprNode::Function { kernel, args, .. } => match kernel {
                    FunctionKernel::Concat
                    | FunctionKernel::ConcatWs
                    | FunctionKernel::Crc32
                    | FunctionKernel::Length
                    | FunctionKernel::CharLength
                    | FunctionKernel::Lower
                    | FunctionKernel::Upper
                    | FunctionKernel::Ltrim
                    | FunctionKernel::Rtrim
                    | FunctionKernel::Reverse => args.iter().all(visit),
                    FunctionKernel::Left | FunctionKernel::Right => {
                        matches!(args.as_slice(), [input, extent]
                            if visit(input) && integer_extent(extent))
                    }
                    FunctionKernel::Substring => {
                        (2..=3).contains(&args.len())
                            && visit(&args[0])
                            && args[1..].iter().all(integer_extent)
                    }
                    _ => false,
                },
                _ => false,
            }
        }
        visit(&self.root)
    }

    /// Columns whose every use in this program needs at most a prefix.
    /// Lengths count characters for text and bytes for binary values. Resolve
    /// parameters per execution; never cache a previous execution's extent.
    pub(crate) fn column_prefix_limits(&self) -> Vec<(usize, usize)> {
        fn extent(node: &ExprNode) -> Option<usize> {
            let parameter;
            let value = match node {
                ExprNode::Const(value) => value,
                ExprNode::Placeholder(index) => {
                    parameter = crate::engine::params::current_param(*index)?;
                    &parameter
                }
                _ => return None,
            };
            match value {
                Value::Int(n) => usize::try_from((*n).max(0)).ok(),
                Value::UInt(n) => usize::try_from(*n).ok(),
                _ => None,
            }
        }
        fn visit(node: &ExprNode, limits: &mut HashMap<usize, usize>, full: &mut HashSet<usize>) {
            let prefix = match node {
                ExprNode::Function {
                    kernel: FunctionKernel::Left,
                    args,
                    ..
                } => match args.as_slice() {
                    [ExprNode::Column(column), count] => extent(count).map(|n| (*column, n)),
                    _ => None,
                },
                ExprNode::SubstringConst {
                    expr,
                    start,
                    len: Some(len),
                } if *start > 0 && *len >= 0 => match expr.as_ref() {
                    ExprNode::Column(column) => usize::try_from(*start - 1)
                        .ok()
                        .and_then(|start| start.checked_add(*len as usize))
                        .map(|n| (*column, n)),
                    _ => None,
                },
                _ => None,
            };
            if let Some((column, n)) = prefix.filter(|(_, n)| n.checked_mul(4).is_some()) {
                limits
                    .entry(column)
                    .and_modify(|old| *old = (*old).max(n))
                    .or_insert(n);
            } else if let ExprNode::Function { args, .. } = node {
                for arg in args {
                    visit(arg, limits, full);
                }
            } else {
                // Conservatively retain full values for other IR shapes,
                // including schema-aware legacy escapes.
                collect_columns_in_node(node, full);
            }
        }
        let mut limits = HashMap::new();
        let mut full = HashSet::new();
        visit(&self.root, &mut limits, &mut full);
        let mut limits: Vec<_> = limits
            .into_iter()
            .filter(|(column, _)| !full.contains(column))
            .collect();
        limits.sort_unstable();
        limits
    }

    pub(crate) fn as_direct_column(&self) -> Option<usize> {
        match &self.root {
            ExprNode::Column(index) => Some(*index),
            _ => None,
        }
    }

    pub(crate) fn as_string_length_column(&self) -> Option<(StringLengthKind, usize)> {
        let ExprNode::Function { kernel, args, .. } = &self.root else {
            return None;
        };
        match (kernel, args.as_slice()) {
            (FunctionKernel::Length, [ExprNode::Column(index)]) => {
                Some((StringLengthKind::Octets, *index))
            }
            (FunctionKernel::CharLength, [ExprNode::Column(index)]) => {
                Some((StringLengthKind::Characters, *index))
            }
            _ => None,
        }
    }

    pub(crate) fn as_string_transform_column(&self) -> Option<(StringTransformKind, usize)> {
        let ExprNode::Function { kernel, args, .. } = &self.root else {
            return None;
        };
        let [ExprNode::Column(index)] = args.as_slice() else {
            return None;
        };
        let kind = match kernel {
            FunctionKernel::Lower => StringTransformKind::Lower,
            FunctionKernel::Upper => StringTransformKind::Upper,
            FunctionKernel::Ltrim => StringTransformKind::Ltrim,
            FunctionKernel::Rtrim => StringTransformKind::Rtrim,
            FunctionKernel::Trim => StringTransformKind::Trim,
            FunctionKernel::Reverse => StringTransformKind::Reverse,
            _ => return None,
        };
        Some((kind, *index))
    }

    /// Recognize the common three-argument REGEXP_REPLACE shape over one
    /// UTF-8 column. The pattern is compiled once through the governed regex
    /// cache; batch execution can then write all results into one contiguous
    /// UTF-8 lane instead of allocating a `Value::String` per row.
    pub(crate) fn as_regexp_replace_const_column(
        &self,
    ) -> Option<(
        usize,
        Arc<crate::engine::func_cache::Governed<regex::Regex>>,
        &str,
    )> {
        let ExprNode::Function {
            kernel: FunctionKernel::RegexpReplace,
            args,
            collation_id,
            ..
        } = &self.root
        else {
            return None;
        };
        let [ExprNode::Column(index), ExprNode::Const(pattern), ExprNode::Const(replacement)] =
            args.as_slice()
        else {
            return None;
        };
        let pattern = crate::types::value_str(pattern)?;
        let replacement = crate::types::value_str(replacement)?;
        let flags = if collation_id
            .and_then(crate::charset::resolve_collation_id)
            .is_some_and(|info| info.case_sensitive)
        {
            "c"
        } else {
            "i"
        };
        let regex = crate::engine::func_cache::cached_regex(pattern, flags).ok()?;
        Some((*index, regex, replacement))
    }

    /// Return the schema-bound source of the private character DISTINCT-key
    /// projection. Batch consumers use this to transform a complete Utf8 lane
    /// into one contiguous Bytes lane, avoiding one `Vec<Value>` and one heap
    /// allocation per input row.
    pub(crate) fn as_distinct_collation_key_column(&self) -> Option<(usize, u16)> {
        let ExprNode::Function {
            kernel: FunctionKernel::DistinctCollationKey,
            args,
            collation_id: Some(collation_id),
            ..
        } = &self.root
        else {
            return None;
        };
        let [ExprNode::Column(index)] = args.as_slice() else {
            return None;
        };
        Some((*index, *collation_id))
    }

    pub(crate) fn as_batch_numeric_expr(&self) -> Option<CompiledNumericExpr> {
        fn lower(node: &ExprNode) -> Option<CompiledNumericExpr> {
            match node {
                ExprNode::Column(index) => Some(CompiledNumericExpr::Column(*index)),
                ExprNode::Const(value) => Some(CompiledNumericExpr::Const(value.clone())),
                ExprNode::Placeholder(index) => Some(CompiledNumericExpr::Const(
                    crate::engine::params::current_param(*index)?,
                )),
                ExprNode::Binary {
                    left,
                    op,
                    right,
                    semantics,
                    ..
                } if matches!(
                    op,
                    BinaryOp::Add | BinaryOp::Sub | BinaryOp::Mul | BinaryOp::Div | BinaryOp::Mod
                ) && !(semantics.unsigned_subtraction && matches!(op, BinaryOp::Sub)) =>
                {
                    Some(CompiledNumericExpr::Binary {
                        left: Box::new(lower(left)?),
                        op: *op,
                        right: Box::new(lower(right)?),
                    })
                }
                _ => None,
            }
        }
        lower(&self.root)
    }
    /// Shape introspection for the vectorized executor: `Some((kind,
    /// column))` when the whole program is exactly `YEAR(col)`-style
    /// single-column temporal extraction, so `expr_batch` can extract
    /// straight from the typed micros/days lane.
    pub(crate) fn as_temporal_extract(&self) -> Option<(TemporalExtractKind, usize)> {
        let ExprNode::Function { kernel, args, .. } = &self.root else {
            return None;
        };
        match (kernel, args.as_slice()) {
            (FunctionKernel::Year, [ExprNode::Column(index)]) => {
                Some((TemporalExtractKind::Year, *index))
            }
            (FunctionKernel::Month, [ExprNode::Column(index)]) => {
                Some((TemporalExtractKind::Month, *index))
            }
            (FunctionKernel::Day, [ExprNode::Column(index)]) => {
                Some((TemporalExtractKind::Day, *index))
            }
            (FunctionKernel::Hour, [ExprNode::Column(index)]) => {
                Some((TemporalExtractKind::Hour, *index))
            }
            (FunctionKernel::Minute, [ExprNode::Column(index)]) => {
                Some((TemporalExtractKind::Minute, *index))
            }
            (FunctionKernel::Second, [ExprNode::Column(index)]) => {
                Some((TemporalExtractKind::Second, *index))
            }
            (FunctionKernel::Quarter, [ExprNode::Column(index)]) => {
                Some((TemporalExtractKind::Quarter, *index))
            }
            (
                FunctionKernel::LegacyDeterministic("EXTRACT"),
                [ExprNode::Const(Value::String(field)), ExprNode::Column(index)],
            ) => {
                let kind = match field.to_ascii_uppercase().as_str() {
                    "YEAR" => TemporalExtractKind::Year,
                    "MONTH" => TemporalExtractKind::Month,
                    "DAY" => TemporalExtractKind::Day,
                    "HOUR" => TemporalExtractKind::Hour,
                    "MINUTE" => TemporalExtractKind::Minute,
                    "SECOND" => TemporalExtractKind::Second,
                    "QUARTER" => TemporalExtractKind::Quarter,
                    _ => return None,
                };
                Some((kind, *index))
            }
            _ => None,
        }
    }

    /// Shape introspection for the vectorized predicate fast path (P6-3 / C2):
    /// `Some((column, op, const))` when the whole program is exactly a
    /// comparison between one column and one constant — `col <op> const` or
    /// the flipped `const <op> col` (op is normalised to the column-on-left
    /// orientation). Returns `None` for `<=>` (NULL-safe) and any other
    /// shape, so the caller keeps the row-by-row oracle for those. The
    /// constant's NULL-ness and type are checked by the caller, which only
    /// SIMD-dispatches when the lane type and const type line up exactly.
    /// Root program is `Column(idx) [NOT] LIKE <const-pattern>` — the shape
    /// vec's `try_column_like_const_bitmap` fast-paths straight from a Utf8
    /// lane. Returns `(col, plan, negated)`; `None` for every other shape
    /// (composite predicates, non-column expressions, runtime patterns).
    /// The row-by-row `ExprProgram::eval_row` path remains the oracle when
    /// this shape isn't matched.
    pub(crate) fn as_column_like_const(&self) -> Option<(usize, &LikePlan, bool)> {
        let ExprNode::LikeConst {
            expr,
            plan,
            negated,
        } = &self.root
        else {
            return None;
        };
        let ExprNode::Column(idx) = expr.as_ref() else {
            return None;
        };
        Some((*idx, plan, *negated))
    }

    /// Root program is `Column(idx) [NOT] IN (<constant strings...>)` and the
    /// compiler proved that every non-NULL member shares the string hash
    /// domain. The returned keys are the exact `ValueKey`s used by the scalar
    /// evaluator, so a batch lane can probe them without re-deriving PAD SPACE
    /// or default-collation semantics.
    pub(crate) fn as_column_string_in_const_set(
        &self,
    ) -> Option<(
        usize,
        &std::collections::HashSet<crate::engine::expr::ValueKey>,
        bool,
        bool,
    )> {
        let ExprNode::InConstSet {
            expr,
            keys,
            hash_class: Some(InHashClass::String),
            has_null,
            negated,
            ..
        } = &self.root
        else {
            return None;
        };
        let ExprNode::Column(column) = expr.as_ref() else {
            return None;
        };
        Some((*column, keys, *has_null, *negated))
    }

    /// Root program is
    /// `SUBSTRING(Column(idx), const_start[, const_len]) [NOT] IN
    /// (<constant strings...>)`.  The scalar program remains the semantic
    /// oracle; the batch executor uses this shape to compare a borrowed UTF-8
    /// slice directly against the precomputed equality keys.
    pub(crate) fn as_substring_column_string_in_const_set(
        &self,
    ) -> Option<(
        usize,
        i64,
        Option<i64>,
        &std::collections::HashSet<crate::engine::expr::ValueKey>,
        bool,
        bool,
    )> {
        let ExprNode::InConstSet {
            expr,
            keys,
            hash_class: Some(InHashClass::String),
            has_null,
            negated,
            ..
        } = &self.root
        else {
            return None;
        };
        let ExprNode::SubstringConst { expr, start, len } = expr.as_ref() else {
            return None;
        };
        let ExprNode::Column(column) = expr.as_ref() else {
            return None;
        };
        Some((*column, *start, *len, keys, *has_null, *negated))
    }

    /// Root program is an integer column `[NOT] IN` a fixed integral set.
    /// `Int` and `UInt` constants share `ValueKey::Int`, so signed/unsigned
    /// equality stays exact without invoking the scalar coercion oracle.
    pub(crate) fn as_column_integer_in_const_set(
        &self,
    ) -> Option<(
        usize,
        &std::collections::HashSet<crate::engine::expr::ValueKey>,
        bool,
        bool,
    )> {
        let ExprNode::InConstSet {
            expr,
            keys,
            hash_class: Some(InHashClass::Integer),
            has_null,
            negated,
            ..
        } = &self.root
        else {
            return None;
        };
        let ExprNode::Column(column) = expr.as_ref() else {
            return None;
        };
        Some((*column, keys, *has_null, *negated))
    }

    /// Runtime-pattern sibling of `as_column_like_const`. A prepared or
    /// auto-parameterized `col [NOT] LIKE ?` can classify the bound pattern
    /// once for the whole batch and then use the same UTF-8 lane matcher as a
    /// literal pattern. More complex runtime patterns retain the scalar
    /// evaluator so warning, coercion and evaluation-order semantics do not
    /// move.
    pub(crate) fn as_column_like_param(
        &self,
    ) -> Option<(usize, usize, bool, Option<char>, Option<u16>)> {
        let ExprNode::Like {
            expr,
            pattern,
            negated,
            escape_char,
            collation_id,
        } = &self.root
        else {
            return None;
        };
        let (ExprNode::Column(column), ExprNode::Placeholder(param)) =
            (expr.as_ref(), pattern.as_ref())
        else {
            return None;
        };
        Some((*column, *param, *negated, *escape_char, *collation_id))
    }

    /// Detailed sibling used by the typed UTF-8 batch kernel. Collation is
    /// resolved by the expression compiler once and carried with the reusable
    /// program, so a batch does not redo coercibility/collation lookup per
    /// row. Dynamic string operands are deliberately rejected: their numeric
    /// coercion depends on the runtime bound value and remains on the oracle.
    pub(crate) fn as_column_const_cmp_with_collation(
        &self,
    ) -> Option<(usize, BinaryOp, &Value, Option<u16>)> {
        let (column, op, value, collation_id, needs_runtime_context) =
            self.as_column_const_cmp_shape()?;
        if needs_runtime_context {
            return None;
        }
        Some((column, op, value, collation_id))
    }

    /// All typed comparison shapes share the ordinary SQL comparison gate.
    /// Null-safe equality has a different null bitmap contract and stays out.
    fn comparison_parts(&self) -> Option<(&ExprNode, &BinaryOp, &ExprNode, &BinarySemantics)> {
        let ExprNode::Binary {
            left,
            op,
            right,
            semantics,
        } = &self.root
        else {
            return None;
        };
        matches!(
            op,
            BinaryOp::Eq
                | BinaryOp::NotEq
                | BinaryOp::Lt
                | BinaryOp::LtEq
                | BinaryOp::Gt
                | BinaryOp::GtEq
        )
        .then_some((left, op, right, semantics))
    }

    /// Structural column/constant comparison shape, including expressions
    /// whose string operand was marked dynamic by auto-parameterization.
    /// Callers must still prove their concrete lane/coercion is safe. The
    /// batch executor uses this only for DATE/DATETIME/TIME lanes: a valid
    /// bound string is parsed to that exact temporal representation, while an
    /// invalid value stays on the scalar warning/error oracle.
    pub(crate) fn as_column_const_cmp_shape(
        &self,
    ) -> Option<(usize, BinaryOp, &Value, Option<u16>, bool)> {
        let (left, op, right, semantics) = self.comparison_parts()?;
        match (left, right) {
            (ExprNode::Column(idx), ExprNode::Const(v)) => Some((
                *idx,
                *op,
                v,
                semantics.collation_id,
                semantics.comparison.needs_runtime_context(),
            )),
            (ExprNode::Const(v), ExprNode::Column(idx)) => Some((
                *idx,
                flip(*op),
                v,
                semantics.collation_id,
                semantics.comparison.needs_runtime_context(),
            )),
            _ => None,
        }
    }

    /// Exact `COALESCE(column, const) <cmp> const` / `IFNULL(column, const)
    /// <cmp> const` shape used by the typed predicate executor. The
    /// comparison is normalised to keep the conditional expression on the
    /// left. More general conditional expressions retain the scalar oracle:
    /// additional arguments, dynamic operands and non-constant defaults may
    /// have evaluation-order or coercion semantics that a bitmap kernel must
    /// not approximate.
    pub(crate) fn as_coalesce_column_const_cmp(
        &self,
    ) -> Option<(usize, BinaryOp, &Value, &Value, Option<u16>)> {
        fn conditional_parts(node: &ExprNode) -> Option<(usize, &Value)> {
            let ExprNode::Function { kernel, args, .. } = node else {
                return None;
            };
            if !matches!(kernel, FunctionKernel::IfNull | FunctionKernel::Coalesce) {
                return None;
            }
            let [ExprNode::Column(column), ExprNode::Const(fallback)] = args.as_slice() else {
                return None;
            };
            Some((*column, fallback))
        }

        let (left, op, right, semantics) = self.comparison_parts()?;
        if semantics.comparison.needs_runtime_context() {
            return None;
        }
        match (conditional_parts(left), right) {
            (Some((column, fallback)), ExprNode::Const(bound)) => {
                Some((column, *op, fallback, bound, semantics.collation_id))
            }
            _ => match (left, conditional_parts(right)) {
                (ExprNode::Const(bound), Some((column, fallback))) => {
                    Some((column, flip(*op), fallback, bound, semantics.collation_id))
                }
                _ => None,
            },
        }
    }

    /// Column/placeholder sibling of `as_column_const_cmp_shape`. Resolving
    /// the placeholder is deliberately left to the execution worker, where
    /// the captured parameter context is installed. This lets a batch kernel
    /// convert one bound temporal value per batch instead of once per row.
    pub(crate) fn as_column_param_cmp_shape(
        &self,
    ) -> Option<(usize, BinaryOp, usize, Option<u16>, bool)> {
        let (left, op, right, semantics) = self.comparison_parts()?;
        match (left, right) {
            (ExprNode::Column(column), ExprNode::Placeholder(param)) => Some((
                *column,
                *op,
                *param,
                semantics.collation_id,
                semantics.comparison.needs_runtime_context(),
            )),
            (ExprNode::Placeholder(param), ExprNode::Column(column)) => Some((
                *column,
                flip(*op),
                *param,
                semantics.collation_id,
                semantics.comparison.needs_runtime_context(),
            )),
            _ => None,
        }
    }

    /// Exact two-column comparison shape used by typed batch predicates.
    /// Dynamic string operands remain on the scalar oracle because their
    /// coercion is value-dependent; fixed-width lanes can compare their
    /// native representations directly with identical NULL semantics.
    pub(crate) fn as_column_column_cmp(&self) -> Option<(usize, BinaryOp, usize, Option<u16>)> {
        let (left, op, right, semantics) = self.comparison_parts()?;
        if semantics.comparison.needs_runtime_context() {
            return None;
        }
        match (left, right) {
            (ExprNode::Column(left), ExprNode::Column(right)) => {
                Some((*left, *op, *right, semantics.collation_id))
            }
            _ => None,
        }
    }

    /// Evaluate conjunctions/disjunctions of column/constant comparisons
    /// without acquiring expression scratch space or materialising values for
    /// the boolean nodes. Unsupported shapes return `None` and keep the full
    /// ExprProgram evaluator as the semantics oracle.
    pub(crate) fn eval_simple_predicate_source(
        &self,
        row: &dyn RowSource,
    ) -> Option<Result<bool, QueryError>> {
        self.root.try_eval_simple_predicate(row).map(|result| {
            PROGRAM_EVAL_ROWS.fetch_add(1, AtomicOrdering::Relaxed);
            result.map(|value| value.unwrap_or(false))
        })
    }

    pub(crate) fn eval_source(
        &self,
        row: &dyn RowSource,
        scratch: &mut EvalScratch,
    ) -> Result<Value, QueryError> {
        PROGRAM_EVAL_ROWS.fetch_add(1, AtomicOrdering::Relaxed);
        let value = self.root.eval(row, scratch)?;
        if self.coerce_conditional_result
            && matches!(
                self.root,
                ExprNode::Case {
                    result_coercion: None,
                    ..
                } | ExprNode::Function {
                    kernel: FunctionKernel::If
                        | FunctionKernel::IfNull
                        | FunctionKernel::Coalesce
                        | FunctionKernel::NullIf,
                    result_coercion: None,
                    ..
                }
            )
        {
            crate::engine::expr::coerce_result_to_expr_type(value, self.result_type)
        } else {
            Ok(value)
        }
    }

    pub(crate) fn eval_row(
        &self,
        row: &RowView<'_>,
        scratch: &mut EvalScratch,
    ) -> Result<Value, QueryError> {
        self.eval_source(row, scratch)
    }

    #[allow(dead_code)]
    pub(crate) fn capability(&self) -> &ExprCapability {
        &self.capability
    }
}

#[derive(Debug, Clone)]
pub(crate) enum CompiledTableExpr {
    SimpleColumn(usize),
    Program(Arc<ExprProgram>),
    Legacy(Expr),
}

impl CompiledTableExpr {
    pub(crate) fn source_charset(&self, table: &TableSchema) -> &'static str {
        let collation = match self {
            Self::SimpleColumn(index) => resolve_column_collation_id(table, &table.columns[*index]),
            Self::Program(program) => program.result_type().collation_id,
            Self::Legacy(expr) => {
                crate::engine::expr::expr_collation(expr, table).map(|c| c.collation_id)
            }
        };
        collation
            .and_then(crate::charset::resolve_collation_id)
            .map_or_else(crate::engine::expr::connection_charset, |c| c.charset)
    }

    pub(crate) fn compile_once(expr: &Expr, table: &TableSchema) -> Self {
        if let Some(idx) = expr_column_index(expr, table) {
            return Self::SimpleColumn(idx);
        }
        // Cache-hit fast path: key on the cheap `table_input_fingerprint`
        // and only build the full `InputSchema` if we actually miss and
        // need to compile. `from_table` sets the identical fingerprint, so
        // the probe and any stored entry agree.
        let fingerprint = table_input_fingerprint(table);
        match ExprCompiler::compile_cached_lazy(expr, fingerprint, || {
            InputSchema::from_table(table)
        }) {
            Ok(program) => Self::Program(program),
            Err(_) => Self::Legacy(expr.clone()),
        }
    }

    /// Append every input-column index this expression reads to `out`.
    /// Returns `false` when the expression is in `Legacy` form (the
    /// compiler couldn't lower it and we have no structural tree to
    /// walk) — callers that want to narrow row reads to a subset of
    /// columns should treat that as "must read everything".
    ///
    /// The walk traverses the compiled `ExprNode` tree, not the source
    /// `Expr`, so any constant-folding / `LikeConst` / `InConstSet`
    /// rewrites the compiler did remain accurate. A `SimpleColumn`
    /// expression contributes a single index; complex sub-trees walk
    /// their operands recursively.
    pub(crate) fn try_collect_referenced_columns(&self, out: &mut HashSet<usize>) -> bool {
        match self {
            Self::SimpleColumn(idx) => {
                out.insert(*idx);
                true
            }
            Self::Program(program) => {
                out.extend(program.referenced_columns());
                true
            }
            Self::Legacy(_) => false,
        }
    }

    pub(crate) fn eval(&self, table: &TableSchema, values: &[Value]) -> Result<Value, QueryError> {
        match self {
            Self::SimpleColumn(idx) => values.get(*idx).cloned().ok_or_else(|| {
                error(
                    crate::error::catalog::ER_BAD_FIELD_ERROR,
                    "row value is missing for compiled expression",
                )
            }),
            Self::Program(program) => {
                with_eval_scratch(|scratch| program.eval_source(&values, scratch))
            }
            Self::Legacy(expr) => {
                record_legacy_fallback_row();
                eval_expr_legacy(expr, table, values)
            }
        }
    }
    pub(crate) fn eval_bool(
        &self,
        table: &TableSchema,
        values: &[Value],
    ) -> Result<bool, QueryError> {
        Ok(truthy(&self.eval(table, values)?))
    }
}

/// Walk a compiled `ExprNode` tree and record every input-column index
/// it reads into `out`. Used by `CompiledTableExpr::try_collect_referenced_columns`
/// to drive narrowed row reads when only a subset of schema columns
/// participates in projection / filter / ORDER BY.
fn collect_columns_in_node(node: &ExprNode, out: &mut HashSet<usize>) {
    match node {
        ExprNode::Column(idx) => {
            out.insert(*idx);
        }
        ExprNode::Const(_) | ExprNode::Placeholder(_) | ExprNode::FieldCount => {}
        ExprNode::Unary { expr, .. }
        | ExprNode::IsNull(expr)
        | ExprNode::IsNotNull(expr)
        | ExprNode::SubstringConst { expr, .. }
        | ExprNode::LikeConst { expr, .. } => collect_columns_in_node(expr, out),
        ExprNode::Binary { left, right, .. } => {
            collect_columns_in_node(left, out);
            collect_columns_in_node(right, out);
        }
        ExprNode::InList { expr, list, .. } => {
            collect_columns_in_node(expr, out);
            for e in list {
                collect_columns_in_node(e, out);
            }
        }
        ExprNode::InConstSet { expr, .. } => collect_columns_in_node(expr, out),
        ExprNode::Between {
            expr, low, high, ..
        } => {
            collect_columns_in_node(expr, out);
            collect_columns_in_node(low, out);
            collect_columns_in_node(high, out);
        }
        ExprNode::Like { expr, pattern, .. } => {
            collect_columns_in_node(expr, out);
            collect_columns_in_node(pattern, out);
        }
        ExprNode::Case {
            operand,
            conditions,
            results,
            else_result,
            result_coercion: _,
        } => {
            if let Some(o) = operand {
                collect_columns_in_node(o, out);
            }
            for c in conditions {
                collect_columns_in_node(c, out);
            }
            for r in results {
                collect_columns_in_node(r, out);
            }
            if let Some(e) = else_result {
                collect_columns_in_node(e, out);
            }
        }
        ExprNode::Function { args, .. } => {
            for a in args {
                collect_columns_in_node(a, out);
            }
        }
        ExprNode::LegacyEscape { columns, .. } => {
            out.extend(columns.iter().copied());
        }
    }
}

pub(crate) struct ExprCompiler;

impl ExprCompiler {
    /// Bind successful, diagnostic-free vector constants at operator open.
    /// The cached AST is never mutated; the resulting native value participates
    /// in the ordinary bounded program-cache key, including datatype/dimension.
    pub(crate) fn bind_vector_constants(expr: &Expr) -> Option<Expr> {
        fn constructor(expr: &Expr) -> bool {
            matches!(expr, Expr::Function { name, distinct: false, .. }
                if crate::engine::vector::sql::returns_vector(&name.to_ascii_uppercase()))
        }
        if expression_depth_exceeds(expr, MAX_COMPILED_EXPRESSION_DEPTH)
            || !crate::parser::visit::any_expr(expr, constructor)
        {
            return None;
        }
        struct Bind {
            changed: bool,
        }
        impl crate::parser::visit::AstVisitorMut for Bind {
            fn exit_expr(&mut self, expr: &mut Expr) {
                if !constructor(expr) {
                    return;
                }
                struct Classify {
                    valid: bool,
                }
                impl crate::parser::visit::AstVisitorMut for Classify {
                    fn enter_expr(&mut self, expr: &mut Expr) {
                        match expr {
                            Expr::Placeholder(index) => {
                                self.valid &= crate::engine::params::has_active_param(*index);
                                // Classification only: evaluate the original AST
                                // below, retaining auto-parameter literal provenance.
                                *expr = Expr::BoundValue(Value::Null);
                            }
                            Expr::Function { distinct: true, .. } => self.valid = false,
                            _ => {}
                        }
                    }
                }
                let mut classification = expr.clone();
                let mut classify = Classify { valid: true };
                crate::parser::visit::visit_expr_mut(&mut classify, &mut classification);
                if !classify.valid || !crate::engine::expr::expr_is_constant(&classification) {
                    return;
                }
                let diagnostics = Arc::new(parking_lot::Mutex::new(Default::default()));
                static EMPTY: OnceLock<TableSchema> = OnceLock::new();
                let table = EMPTY.get_or_init(|| TableSchema::synthetic_for_planning("", &[]));
                let value =
                    crate::engine::api::with_statement_warning_sink(diagnostics.clone(), || {
                        eval_expr_legacy(expr, table, &[])
                    });
                if let Ok(value @ Value::Vector(_)) = value {
                    if diagnostics.lock().is_empty() {
                        *expr = Expr::BoundValue(value);
                        self.changed = true;
                    }
                }
            }
        }
        let mut bound = expr.clone();
        let mut bind = Bind { changed: false };
        crate::parser::visit::visit_expr_mut(&mut bind, &mut bound);
        bind.changed.then_some(bound)
    }

    pub(crate) fn compile(expr: &Expr, schema: &InputSchema) -> Result<ExprProgram, CompileError> {
        Self::compile_with(
            expr,
            schema,
            !COMPILE_DYNAMIC_CONDITIONAL.with(Cell::get),
            |expr, schema| compile_node(expr, schema).map(fold_constants),
        )
    }

    fn compile_with(
        expr: &Expr,
        schema: &InputSchema,
        coerce_conditional_result: bool,
        lower: impl FnOnce(&Expr, &InputSchema) -> Result<ExprNode, CompileError>,
    ) -> Result<ExprProgram, CompileError> {
        if expression_depth_exceeds(expr, MAX_COMPILED_EXPRESSION_DEPTH) {
            return Err(CompileError::TooDeep);
        }
        // CAST(... AS JSON) is source-type sensitive for ENUM/SET/BIT and
        // binary strings. InputSchema deliberately carries only the compact
        // execution type, so a LegacyEscape would synthesize a TableSchema
        // and erase exactly the catalog provenance needed by MySQL's JSON
        // opaque-scalar rules. Fall back for the whole expression; table and
        // PlanRow legacy evaluators retain the real source descriptor.
        if contains_source_sensitive_function(expr) {
            SOURCE_SENSITIVE_REJECTIONS.fetch_add(1, AtomicOrdering::Relaxed);
            return Err(CompileError::Unsupported);
        }
        let resolve_column = |column_expr: &Expr| {
            let index = match column_expr {
                Expr::Identifier(name) => schema.column_index(None, name).ok()?,
                Expr::CompoundIdentifier(parts) => {
                    let column = parts.last()?;
                    let qualifier = (parts.len() >= 2).then(|| parts[parts.len() - 2].as_str());
                    schema.column_index(qualifier, column).ok()?
                }
                _ => return None,
            };
            schema.bound_types.get(index).copied()
        };
        let resolve_placeholder = |_: usize| None;
        let context = crate::engine::expr_type::TypeContext::context_free(
            &resolve_column,
            &resolve_placeholder,
        );
        let result_type = crate::engine::expr_type::infer_expr_type(expr, &context);
        if matches!(
            result_type.kind,
            crate::engine::expr_type::ExprTypeKind::Vector { .. }
        ) {
            crate::engine::expr_type::validate_vector_control_flow(expr, &|branch| {
                crate::engine::expr_type::infer_expr_type(branch, &context)
            })?;
        }
        let root = lower(expr, schema)?;
        let (capability, column_reference_bound) = infer_program_metadata(&root);
        Ok(ExprProgram {
            root,
            referenced_columns: OnceLock::new(),
            column_reference_bound,
            capability,
            result_type,
            coerce_conditional_result,
            cache_charge: None,
        })
    }

    /// Compile with sub-expression escape disabled: `Unsupported` if the
    /// expression isn't *fully* lowerable to the compiled tree. Used by
    /// validity / routing probes (CHECK + generated-column validation,
    /// join-residual compilability) that must keep treating "compiles" as
    /// "no legacy fallback needed" — so an unknown function still fails
    /// them. Uncached, so it never contaminates the execution program cache.
    pub(crate) fn compile_strict(
        expr: &Expr,
        schema: &InputSchema,
    ) -> Result<ExprProgram, CompileError> {
        let prev = COMPILE_NO_ESCAPE.with(|c| c.replace(true));
        let result = Self::compile(expr, schema);
        COMPILE_NO_ESCAPE.with(|c| c.set(prev));
        result
    }

    pub(crate) fn compile_check_constraint(
        expr: &Expr,
        schema: &InputSchema,
    ) -> Result<ExprProgram, CompileError> {
        let previous = COMPILE_CHECK_COMPARISONS.with(|cell| cell.replace(true));
        let result = Self::compile_strict(expr, schema);
        COMPILE_CHECK_COMPARISONS.with(|cell| cell.set(previous));
        result
    }

    pub(crate) fn compile_dynamic_conditional(
        expr: &Expr,
        schema: &InputSchema,
    ) -> Result<ExprProgram, CompileError> {
        let prev = COMPILE_DYNAMIC_CONDITIONAL.with(|cell| cell.replace(true));
        let result = Self::compile(expr, schema);
        COMPILE_DYNAMIC_CONDITIONAL.with(|cell| cell.set(prev));
        result
    }

    /// Compile the entire expression as one legacy-oracle node over a typed
    /// batch row. This is used at batch boundaries when a partially lowered
    /// program is conservatively marked session/legacy dependent: keeping the
    /// whole scalar expression together preserves the dynamic coercion and
    /// warning behavior of `eval_expr_legacy` without leaving the batch
    /// pipeline or rescanning its input.
    pub(crate) fn compile_legacy_oracle(
        expr: &Expr,
        schema: &InputSchema,
    ) -> Result<ExprProgram, CompileError> {
        Self::compile_with(expr, schema, true, |expr, schema| {
            try_escape_to_legacy(expr, schema).ok_or(CompileError::Unsupported)
        })
    }

    pub(crate) const MAX_PROGRAM_CACHE_ENTRIES: usize = 1024;

    /// Probe L1 (thread-local) then L2 (shared) for a compiled program.
    /// Returns `Some` on a hit (and mirrors an L2 hit back into L1), `None`
    /// on a miss. Bumps the same hit counters the inline code used to.
    fn cache_probe(
        key: &ProgramCacheKey,
        expr: &Expr,
        generation: u64,
    ) -> Option<Result<Arc<ExprProgram>, CompileError>> {
        let l1_hit = PROGRAM_CACHE.with(|cache| {
            let mut cache = cache.borrow_mut();
            program_cache_for_generation(&mut cache, generation).get(key, expr)
        });
        if let Some(cached) = l1_hit {
            COMPILE_CACHE_HITS.fetch_add(1, AtomicOrdering::Relaxed);
            return Some(cached);
        }
        let shard = &program_cache_l2()[(key.expr_fingerprint as usize) % PROGRAM_CACHE_L2_SHARDS];
        if let Some(cached) = shard.lock().get(key, expr) {
            COMPILE_CACHE_L2_HITS.fetch_add(1, AtomicOrdering::Relaxed);
            Self::store_l1(
                generation,
                *key,
                Arc::new(expr.clone()),
                cached.clone(),
                Self::MAX_PROGRAM_CACHE_ENTRIES,
            );
            return Some(cached);
        }
        None
    }

    fn cache_store(
        key: ProgramCacheKey,
        expr: &Expr,
        generation: u64,
        compiled: &Result<Arc<ExprProgram>, CompileError>,
    ) {
        let expression = Arc::new(expr.clone());
        let shard = &program_cache_l2()[(key.expr_fingerprint as usize) % PROGRAM_CACHE_L2_SHARDS];
        {
            let mut guard = shard.lock();
            guard.insert(
                key,
                expression.clone(),
                compiled.clone(),
                MAX_PROGRAM_CACHE_L2_SHARD_ENTRIES,
                MAX_PROGRAM_CACHE_L2_SHARD_BYTES,
            );
        }
        Self::store_l1(
            generation,
            key,
            expression,
            compiled.clone(),
            Self::MAX_PROGRAM_CACHE_ENTRIES,
        );
    }

    fn compile_for_cache(
        expr: &Expr,
        schema: &InputSchema,
    ) -> (Result<Arc<ExprProgram>, CompileError>, bool) {
        let mut program = match Self::compile(expr, schema) {
            Ok(program) => program,
            Err(error) => return (Err(error), false),
        };
        let bytes = std::mem::size_of::<ExprProgram>()
            .saturating_add(program.root.heap_capacity_bytes())
            .saturating_add(
                program
                    .column_reference_bound
                    .saturating_mul(std::mem::size_of::<usize>()),
            )
            // Collision verification retains one cloned AST. Count the root
            // allocation plus every capacity below it; no Debug formatting
            // or arbitrary multiplier is involved.
            .saturating_add(std::mem::size_of::<Expr>())
            .saturating_add(expr_ast_heap_bytes(expr))
            .saturating_add(
                2 * (std::mem::size_of::<ProgramCacheKey>() + std::mem::size_of::<CachedProgram>()),
            )
            .max(std::mem::size_of::<ExprProgram>());
        let Some(charge) =
            crate::engine::executor::mem_budget::ControlledMemoryCharge::try_new_optional(
                crate::engine::executor::mem_budget::ControlledMemoryCategory::DerivedCache,
                bytes,
            )
        else {
            return (Ok(Arc::new(program)), false);
        };
        program.cache_charge = Some(Arc::new(charge));
        (Ok(Arc::new(program)), true)
    }

    pub(crate) fn compile_cached(
        expr: &Expr,
        schema: &InputSchema,
    ) -> Result<Arc<ExprProgram>, CompileError> {
        if expression_depth_exceeds(expr, MAX_COMPILED_EXPRESSION_DEPTH) {
            return Err(CompileError::TooDeep);
        }
        let generation = PROGRAM_CACHE_GENERATION.load(AtomicOrdering::Acquire);
        let key = ProgramCacheKey {
            expr_fingerprint: expr_fingerprint(expr),
            schema_fingerprint: schema.fingerprint(),
            semantics: ExprProgramSemantics::current(),
        };
        if let Some(hit) = Self::cache_probe(&key, expr, generation) {
            return hit;
        }
        COMPILE_CACHE_MISSES.fetch_add(1, AtomicOrdering::Relaxed);
        let (compiled, admitted) = Self::compile_for_cache(expr, schema);
        if compiled.is_err() {
            COMPILE_UNSUPPORTED.fetch_add(1, AtomicOrdering::Relaxed);
        }
        if admitted {
            Self::cache_store(key, expr, generation, &compiled);
        }
        compiled
    }

    /// Like `compile_cached`, but the caller supplies the schema fingerprint
    /// directly and only materialises the `InputSchema` (via `build_schema`)
    /// on a cache miss. This keeps the hot cache-hit path allocation-free —
    /// see `table_input_fingerprint`. The fingerprint MUST equal
    /// `build_schema().fingerprint()` or the probe and the stored entry would
    /// disagree; `table_input_fingerprint` / `InputSchema::from_table`
    /// guarantee this by construction.
    pub(crate) fn compile_cached_lazy(
        expr: &Expr,
        schema_fingerprint: u64,
        build_schema: impl FnOnce() -> InputSchema,
    ) -> Result<Arc<ExprProgram>, CompileError> {
        if expression_depth_exceeds(expr, MAX_COMPILED_EXPRESSION_DEPTH) {
            return Err(CompileError::TooDeep);
        }
        let generation = PROGRAM_CACHE_GENERATION.load(AtomicOrdering::Acquire);
        let key = ProgramCacheKey {
            expr_fingerprint: expr_fingerprint(expr),
            schema_fingerprint,
            semantics: ExprProgramSemantics::current(),
        };
        if let Some(hit) = Self::cache_probe(&key, expr, generation) {
            return hit;
        }
        COMPILE_CACHE_MISSES.fetch_add(1, AtomicOrdering::Relaxed);
        let schema = build_schema();
        debug_assert_eq!(
            schema.fingerprint(),
            schema_fingerprint,
            "compile_cached_lazy fingerprint must match the built schema"
        );
        let (compiled, admitted) = Self::compile_for_cache(expr, &schema);
        if compiled.is_err() {
            COMPILE_UNSUPPORTED.fetch_add(1, AtomicOrdering::Relaxed);
        }
        if admitted {
            Self::cache_store(key, expr, generation, &compiled);
        }
        compiled
    }

    fn store_l1(
        generation: u64,
        key: ProgramCacheKey,
        expression: Arc<Expr>,
        value: Result<Arc<ExprProgram>, CompileError>,
        max_entries: usize,
    ) {
        PROGRAM_CACHE.with(|cache| {
            let mut cache = cache.borrow_mut();
            program_cache_for_generation(&mut cache, generation).insert(
                key,
                expression,
                value,
                max_entries,
                MAX_PROGRAM_CACHE_L1_BYTES,
            );
        });
    }
}

fn contains_source_sensitive_function(expr: &Expr) -> bool {
    crate::parser::visit::try_inspect_value_expr(expr, |expr| {
        if let Expr::Function { name, args, .. } = expr {
            let is_json_cast = name.eq_ignore_ascii_case("CAST")
                && matches!(
                    args.get(1),
                    Some(Expr::Literal(Literal::String(target)))
                        if target.eq_ignore_ascii_case("JSON")
                )
                && matches!(
                    args.first(),
                    Some(Expr::Identifier(_) | Expr::CompoundIdentifier(_))
                );
            let consumes_source_octets = name.eq_ignore_ascii_case("AES_ENCRYPT")
                || name.eq_ignore_ascii_case("AES_DECRYPT");
            let needs_source_type = name.eq_ignore_ascii_case("UNIX_TIMESTAMP");
            if is_json_cast || consumes_source_octets || needs_source_type {
                return Err(());
            }
        }
        Ok(!matches!(
            expr,
            Expr::Tuple(_)
                | Expr::WindowFunction { .. }
                | Expr::InSubquery { .. }
                | Expr::QuantifiedComparison { .. }
        ))
    })
    .is_err()
}

/// Compare SQL expression structure while ignoring identifier/function case.
/// Ordinary binding/sort matches borrow their trees without allocations. The
/// uncommon subquery/window forms use the shared canonical AST collision guard;
/// a fingerprint alone is never accepted as proof of equality.
pub(crate) fn expr_semantically_eq(left: &Expr, right: &Expr) -> bool {
    fn list(left: &[Expr], right: &[Expr]) -> bool {
        left.len() == right.len()
            && left
                .iter()
                .zip(right)
                .all(|(a, b)| expr_semantically_eq(a, b))
    }
    fn optional(left: Option<&Expr>, right: Option<&Expr>) -> bool {
        match (left, right) {
            (Some(a), Some(b)) => expr_semantically_eq(a, b),
            (None, None) => true,
            _ => false,
        }
    }
    if left == right {
        return true;
    }
    match (left, right) {
        (Expr::Identifier(a), Expr::Identifier(b))
        | (Expr::UserVariable(a), Expr::UserVariable(b)) => a.eq_ignore_ascii_case(b),
        (Expr::CompoundIdentifier(a), Expr::CompoundIdentifier(b)) => {
            a.len() == b.len() && a.iter().zip(b).all(|(a, b)| a.eq_ignore_ascii_case(b))
        }
        (
            Expr::Function {
                name: a,
                args: aa,
                distinct: ad,
            },
            Expr::Function {
                name: b,
                args: ba,
                distinct: bd,
            },
        ) => ad == bd && a.eq_ignore_ascii_case(b) && list(aa, ba),
        (Expr::Unary { op: a, expr: ae }, Expr::Unary { op: b, expr: be }) => {
            a == b && expr_semantically_eq(ae, be)
        }
        (
            Expr::Binary {
                left: al,
                op: a,
                right: ar,
            },
            Expr::Binary {
                left: bl,
                op: b,
                right: br,
            },
        ) => a == b && expr_semantically_eq(al, bl) && expr_semantically_eq(ar, br),
        (Expr::IsNull(a), Expr::IsNull(b)) | (Expr::IsNotNull(a), Expr::IsNotNull(b)) => {
            expr_semantically_eq(a, b)
        }
        (Expr::Tuple(a), Expr::Tuple(b)) => list(a, b),
        (
            Expr::InList {
                expr: a,
                list: al,
                negated: an,
            },
            Expr::InList {
                expr: b,
                list: bl,
                negated: bn,
            },
        ) => an == bn && expr_semantically_eq(a, b) && list(al, bl),
        (
            Expr::Between {
                expr: a,
                low: al,
                high: ah,
                negated: an,
            },
            Expr::Between {
                expr: b,
                low: bl,
                high: bh,
                negated: bn,
            },
        ) => {
            an == bn
                && expr_semantically_eq(a, b)
                && expr_semantically_eq(al, bl)
                && expr_semantically_eq(ah, bh)
        }
        (
            Expr::Like {
                expr: a,
                pattern: ap,
                negated: an,
                escape_char: ae,
            },
            Expr::Like {
                expr: b,
                pattern: bp,
                negated: bn,
                escape_char: be,
            },
        ) => an == bn && ae == be && expr_semantically_eq(a, b) && expr_semantically_eq(ap, bp),
        (
            Expr::Case {
                operand: a,
                conditions: ac,
                results: ar,
                else_result: ae,
            },
            Expr::Case {
                operand: b,
                conditions: bc,
                results: br,
                else_result: be,
            },
        ) => {
            optional(a.as_deref(), b.as_deref())
                && list(ac, bc)
                && list(ar, br)
                && optional(ae.as_deref(), be.as_deref())
        }
        (Expr::GroupKeyRef { index: a, expr: ae }, Expr::GroupKeyRef { index: b, expr: be }) => {
            a == b && expr_semantically_eq(ae, be)
        }
        (Expr::Default(Some(a)), Expr::Default(Some(b))) => a.eq_ignore_ascii_case(b),
        (
            Expr::SystemVariable { scope: a, name: an },
            Expr::SystemVariable { scope: b, name: bn },
        ) => a == b && an.eq_ignore_ascii_case(bn),
        _ => {
            if expr_fingerprint(left) != expr_fingerprint(right) {
                return false;
            }
            let (mut a, mut b) = (left.clone(), right.clone());
            crate::parser::canonicalize_expr_for_plan_cache(&mut a);
            crate::parser::canonicalize_expr_for_plan_cache(&mut b);
            a == b
        }
    }
}

pub(crate) fn expr_fingerprint(expr: &Expr) -> u64 {
    // Fixed-seed ahash for the same reasons as `compute_fingerprint`:
    // callers (planner.rs) use the u64 output as a HashMap key across
    // calls in the same process, so the seeds must be process-stable.
    // A separate seed pool from the schema hasher (different domain,
    // no need to share state).
    static STATE: OnceLock<ahash::RandomState> = OnceLock::new();
    let rs = STATE.get_or_init(|| {
        ahash::RandomState::with_seeds(
            0x4D59_4442_4558_5052u64, // "MYDBEXPR"
            0x4669_6E67_6572_5072u64, // "FingerPr"
            0x3C4F_A9D2_1E76_B85Au64,
            0xBB67_AE85_84CA_A73Bu64,
        )
    });
    let mut hasher = rs.build_hasher();
    hash_expr(expr, &mut hasher);
    hasher.finish()
}

/// Bound recursive compiler/type-inference/evaluator stack use before any of
/// those recursive passes start. The check itself is iterative, so even an
/// AST produced by a non-SQL caller cannot overflow the Rust thread stack
/// while we are trying to reject it.
// Left-deep operator chains (`a + b + ...`) count one level per operator, so
// this is also the longest arithmetic chain. Recursive passes that can reach
// this depth enter `util::stack::with_stack` at their recursion points, which
// extends the stack on demand instead of relying on the thread's own size.
pub(crate) const MAX_EXPRESSION_DEPTH: usize = 1024;

/// Compiled programs are analysed and evaluated recursively, per row and on
/// fixed-size parallel worker stacks. Deeper expressions are rejected by the
/// compiler and run on the legacy evaluator, which extends its stack per level.
pub(crate) const MAX_COMPILED_EXPRESSION_DEPTH: usize = 64;

fn expression_depth_exceeds(root: &Expr, maximum: usize) -> bool {
    let mut pending = vec![(root, 1_usize)];
    while let Some((expr, depth)) = pending.pop() {
        if depth > maximum {
            return true;
        }
        let child_depth = depth + 1;
        macro_rules! push {
            ($child:expr) => {
                pending.push(($child, child_depth))
            };
        }
        match expr {
            Expr::Unary { expr, .. }
            | Expr::IsNull(expr)
            | Expr::IsNotNull(expr)
            | Expr::GroupKeyRef { expr, .. } => push!(expr),
            Expr::Binary { left, right, .. } => {
                push!(left);
                push!(right);
            }
            Expr::InList { expr, list, .. } => {
                push!(expr);
                for item in list {
                    push!(item);
                }
            }
            Expr::InSubquery { expr, .. } => push!(expr),
            Expr::QuantifiedComparison { left, right, .. } => {
                push!(left);
                if let QuantifiedComparisonRight::List(list) = right {
                    for item in list {
                        push!(item);
                    }
                }
            }
            Expr::Between {
                expr, low, high, ..
            } => {
                push!(expr);
                push!(low);
                push!(high);
            }
            Expr::Like { expr, pattern, .. } => {
                push!(expr);
                push!(pattern);
            }
            Expr::Case {
                operand,
                conditions,
                results,
                else_result,
            } => {
                if let Some(operand) = operand {
                    push!(operand);
                }
                for condition in conditions {
                    push!(condition);
                }
                for result in results {
                    push!(result);
                }
                if let Some(result) = else_result {
                    push!(result);
                }
            }
            Expr::Function { args, .. } => {
                for arg in args {
                    push!(arg);
                }
            }
            Expr::Tuple(items) => {
                for item in items {
                    push!(item);
                }
            }
            Expr::WindowFunction { args, spec, .. } => {
                let crate::parser::WindowSpec {
                    partition_by,
                    order_by,
                    frame,
                    ..
                } = spec.as_ref();

                for arg in args {
                    push!(arg);
                }
                for item in partition_by {
                    push!(item);
                }
                for item in order_by {
                    push!(&item.expr);
                }
                if let Some(frame) = frame {
                    use crate::parser::WindowFrameBound::{Following, Preceding};
                    if let Preceding(expr) | Following(expr) = &frame.start {
                        push!(expr);
                    }
                    if let Preceding(expr) | Following(expr) = &frame.end {
                        push!(expr);
                    }
                }
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
            | Expr::Subquery(_)
            | Expr::Exists { .. }
            | Expr::Grouping(_) => {}
        }
    }
    false
}

// M2/B6: hash a `JSON_EXTRACT(left, right)` synthetic function-call form.
// Shared by the `Arrow` (`->`) fast path and the outer wrapper of the
// `LongArrow` (`->>`) fast path so all three writing styles collapse to a
// single fingerprint.
fn hash_json_extract_call(left: &Expr, right: &Expr, state: &mut impl Hasher) {
    // Mirror the Function arm's discriminator layout so a synthetic
    // JSON_EXTRACT call hashes byte-for-byte identically to an explicitly
    // written one.
    19_u8.hash(state);
    "JSON_EXTRACT".hash(state);
    false.hash(state);
    2_usize.hash(state);
    hash_expr(left, state);
    hash_expr(right, state);
}

// SQL function/column names are ASCII case-insensitive. Keep the byte stream
// identical to an uppercase str hash (including synthetic JSON calls), without
// allocating for ordinary names in aggregate-result lookups.
fn hash_case_insensitive_name(name: &str, state: &mut impl Hasher) {
    if !name.bytes().any(|byte| byte.is_ascii_lowercase()) {
        name.hash(state);
    } else if name.len() <= 256 {
        let mut folded = [0u8; 256];
        for (dst, src) in folded.iter_mut().zip(name.bytes()) {
            *dst = src.to_ascii_uppercase();
        }
        std::str::from_utf8(&folded[..name.len()])
            .expect("ASCII case folding preserves UTF-8")
            .hash(state);
    } else {
        name.to_ascii_uppercase().hash(state);
    }
}

fn hash_optional_name(name: Option<&str>, state: &mut impl Hasher) {
    name.is_some().hash(state);
    if let Some(name) = name {
        hash_case_insensitive_name(name, state);
    }
}

/// Subqueries are not lowered by `ExprProgram`, but expression fingerprints
/// are also used by planner/executor memo tables.  Pointer identity made the
/// same parsed SQL produce a different fingerprint after every reparse.
/// These uncommon nodes retain the complete AST hash, using the same name
/// canonicalization as the plan-cache collision guard. Literals stay exact.
fn hash_select(select: &crate::parser::Select, state: &mut impl Hasher) {
    let mut canonical = select.clone();
    crate::parser::canonicalize_select_for_plan_cache(&mut canonical);
    format_args!("{canonical:?}").to_string().hash(state);
}

fn hash_window_bound(bound: &crate::parser::WindowFrameBound, state: &mut impl Hasher) {
    use crate::parser::WindowFrameBound;
    match bound {
        WindowFrameBound::UnboundedPreceding => 0_u8.hash(state),
        WindowFrameBound::UnboundedFollowing => 1_u8.hash(state),
        WindowFrameBound::CurrentRow => 2_u8.hash(state),
        WindowFrameBound::Preceding(expr) => {
            3_u8.hash(state);
            hash_expr(expr, state);
        }
        WindowFrameBound::Following(expr) => {
            4_u8.hash(state);
            hash_expr(expr, state);
        }
    }
}

fn hash_window_frame(frame: &crate::parser::WindowFrame, state: &mut impl Hasher) {
    use crate::parser::{WindowFrameExclusion, WindowFrameUnits};
    let units = match frame.units {
        WindowFrameUnits::Rows => 0_u8,
        WindowFrameUnits::Range => 1,
        WindowFrameUnits::Groups => 2,
    };
    units.hash(state);
    hash_window_bound(&frame.start, state);
    hash_window_bound(&frame.end, state);
    let exclusion = match frame.exclusion {
        WindowFrameExclusion::NoOthers => 0_u8,
        WindowFrameExclusion::CurrentRow => 1,
        WindowFrameExclusion::Group => 2,
        WindowFrameExclusion::Ties => 3,
    };
    exclusion.hash(state);
}

fn hash_expr(expr: &Expr, state: &mut impl Hasher) {
    match expr {
        Expr::Wildcard => 0_u8.hash(state),
        Expr::Identifier(name) => {
            1_u8.hash(state);
            hash_case_insensitive_name(name, state);
        }
        Expr::CompoundIdentifier(parts) => {
            2_u8.hash(state);
            parts.len().hash(state);
            for part in parts {
                hash_case_insensitive_name(part, state);
            }
        }
        Expr::UserVariable(name) => {
            3_u8.hash(state);
            hash_case_insensitive_name(name, state);
        }
        Expr::SystemVariable { scope, name } => {
            6_u8.hash(state);
            scope.hash(state);
            hash_case_insensitive_name(name, state);
        }
        Expr::Literal(lit) => {
            4_u8.hash(state);
            hash_literal(lit, state);
        }
        Expr::BoundValue(value) => {
            5_u8.hash(state);
            hash_value(value, state);
        }
        Expr::Placeholder(idx) => {
            5_u8.hash(state);
            idx.hash(state);
        }
        Expr::DefaultQualified(parts) => {
            41u8.hash(state);
            for part in parts {
                part.to_ascii_lowercase().hash(state);
            }
        }
        Expr::Default(column) => {
            23_u8.hash(state);
            hash_optional_name(column.as_deref(), state);
        }
        Expr::Tuple(items) => {
            24_u8.hash(state);
            items.len().hash(state);
            for item in items {
                hash_expr(item, state);
            }
        }
        Expr::Unary { op, expr } => {
            6_u8.hash(state);
            (*op as u8).hash(state);
            hash_expr(expr, state);
        }
        Expr::Binary { left, op, right } => {
            // M2/B6: normalize `->` and `->>` to the JSON_EXTRACT /
            // JSON_UNQUOTE(JSON_EXTRACT) function forms so functional
            // indexes and generated columns match across writing styles.
            // `WHERE j->'$.a'=1`, `WHERE JSON_EXTRACT(j,'$.a')=1`, and
            // `WHERE j->>'$.a'='1'` all fingerprint identically to the
            // corresponding function-form index expression.
            match op {
                BinaryOp::Arrow => {
                    hash_json_extract_call(left, right, state);
                    return;
                }
                BinaryOp::LongArrow => {
                    // Function("JSON_UNQUOTE", [Function("JSON_EXTRACT", ..)])
                    19_u8.hash(state);
                    "JSON_UNQUOTE".hash(state);
                    false.hash(state);
                    1_usize.hash(state);
                    hash_json_extract_call(left, right, state);
                    return;
                }
                _ => {}
            }
            7_u8.hash(state);
            (*op as u8).hash(state);
            hash_expr(left, state);
            hash_expr(right, state);
        }
        Expr::IsNull(expr) => {
            8_u8.hash(state);
            hash_expr(expr, state);
        }
        Expr::IsNotNull(expr) => {
            9_u8.hash(state);
            hash_expr(expr, state);
        }
        Expr::InList {
            expr,
            list,
            negated,
        } => {
            10_u8.hash(state);
            negated.hash(state);
            hash_expr(expr, state);
            list.len().hash(state);
            for item in list {
                hash_expr(item, state);
            }
        }
        Expr::InSubquery {
            expr,
            subquery,
            negated,
        } => {
            11_u8.hash(state);
            negated.hash(state);
            hash_expr(expr, state);
            hash_select(subquery, state);
        }
        Expr::QuantifiedComparison {
            left,
            op,
            quantifier,
            right,
        } => {
            25_u8.hash(state);
            (*op as u8).hash(state);
            (*quantifier as u8).hash(state);
            hash_expr(left, state);
            match right {
                QuantifiedComparisonRight::Subquery(subquery) => {
                    0_u8.hash(state);
                    hash_select(subquery, state);
                }
                QuantifiedComparisonRight::List(list) => {
                    1_u8.hash(state);
                    list.len().hash(state);
                    for item in list {
                        hash_expr(item, state);
                    }
                }
            }
        }
        Expr::Subquery(subquery) => {
            12_u8.hash(state);
            hash_select(subquery, state);
        }
        Expr::Exists { subquery, negated } => {
            13_u8.hash(state);
            negated.hash(state);
            hash_select(subquery, state);
        }
        Expr::Between {
            expr,
            low,
            high,
            negated,
        } => {
            14_u8.hash(state);
            negated.hash(state);
            hash_expr(expr, state);
            hash_expr(low, state);
            hash_expr(high, state);
        }
        Expr::Like {
            expr,
            pattern,
            negated,
            escape_char,
        } => {
            15_u8.hash(state);
            negated.hash(state);
            escape_char.hash(state);
            hash_expr(expr, state);
            hash_expr(pattern, state);
        }
        Expr::Case {
            operand,
            conditions,
            results,
            else_result,
        } => {
            16_u8.hash(state);
            operand.is_some().hash(state);
            if let Some(operand) = operand {
                hash_expr(operand, state);
            }
            conditions.len().hash(state);
            for condition in conditions {
                hash_expr(condition, state);
            }
            17_u8.hash(state);
            results.len().hash(state);
            for result in results {
                hash_expr(result, state);
            }
            18_u8.hash(state);
            else_result.is_some().hash(state);
            if let Some(else_result) = else_result {
                hash_expr(else_result, state);
            }
        }
        Expr::Function {
            name,
            args,
            distinct,
        } => {
            // M2/B6, fixes P2-14: normalize function name to ASCII uppercase
            // so `json_extract` and `JSON_EXTRACT` (and every other case
            // permutation) fingerprint identically. MySQL identifiers are
            // case-insensitive; the previous byte-exact hash silently broke
            // functional-index matching whenever a query and its index used
            // different case conventions.
            19_u8.hash(state);
            hash_case_insensitive_name(name, state);
            distinct.hash(state);
            args.len().hash(state);
            for arg in args {
                hash_expr(arg, state);
            }
        }
        Expr::GroupKeyRef { index, expr } => {
            26_u8.hash(state);
            index.hash(state);
            hash_expr(expr, state);
        }
        Expr::Grouping(indices) => {
            27_u8.hash(state);
            indices.hash(state);
        }
        Expr::WindowFunction { name, args, spec } => {
            let crate::parser::WindowSpec {
                partition_by,
                order_by,
                frame,
                inherits,
                ..
            } = spec.as_ref();

            20_u8.hash(state);
            hash_case_insensitive_name(name, state);
            hash_optional_name(inherits.as_deref(), state);
            args.len().hash(state);
            for arg in args {
                hash_expr(arg, state);
            }
            21_u8.hash(state);
            partition_by.len().hash(state);
            for expr in partition_by {
                hash_expr(expr, state);
            }
            22_u8.hash(state);
            order_by.len().hash(state);
            for item in order_by {
                item.asc.hash(state);
                item.nulls_first.hash(state);
                hash_expr(&item.expr, state);
            }
            match frame {
                Some(frame) => {
                    true.hash(state);
                    hash_window_frame(frame, state);
                }
                None => false.hash(state),
            }
        }
    }
}

fn hash_literal(lit: &crate::parser::Literal, state: &mut impl Hasher) {
    match lit {
        crate::parser::Literal::Null => 0_u8.hash(state),
        crate::parser::Literal::Bool(v) => {
            1_u8.hash(state);
            v.hash(state);
        }
        crate::parser::Literal::Int(v) => {
            2_u8.hash(state);
            v.hash(state);
        }
        crate::parser::Literal::UInt(v) => {
            3_u8.hash(state);
            v.hash(state);
        }
        crate::parser::Literal::Float(v) => {
            4_u8.hash(state);
            v.to_bits().hash(state);
        }
        crate::parser::Literal::String(v) => {
            5_u8.hash(state);
            v.hash(state);
        }
        crate::parser::Literal::Decimal(v) => {
            6_u8.hash(state);
            v.hash(state);
        }
        crate::parser::Literal::HexBytes(b) => {
            7_u8.hash(state);
            b.hash(state);
        }
        crate::parser::Literal::BitBytes(b) => {
            8_u8.hash(state);
            b.hash(state);
        }
    }
}

fn hash_value(value: &Value, state: &mut impl Hasher) {
    match value {
        Value::Null => 0_u8.hash(state),
        Value::Int(v) => {
            1_u8.hash(state);
            v.hash(state);
        }
        Value::UInt(v) => {
            2_u8.hash(state);
            v.hash(state);
        }
        Value::Float(v) => {
            3_u8.hash(state);
            v.to_bits().hash(state);
        }
        Value::Double(v) => {
            4_u8.hash(state);
            v.to_bits().hash(state);
        }
        Value::Decimal(v) => {
            5_u8.hash(state);
            v.hash(state);
        }
        Value::DecimalCompact(..) | Value::Decimal128(..) | Value::DecimalWide(..) => {
            5_u8.hash(state);
            value.to_display_string().hash(state);
        }
        Value::Bytes(v) => {
            6_u8.hash(state);
            v.hash(state);
        }
        Value::String(v) => {
            7_u8.hash(state);
            v.hash(state);
        }
        Value::SmallString(b) => {
            // Same tag + same `str::Hash` path as Value::String so the two
            // variants hash to identical bytes when their payloads agree.
            7_u8.hash(state);
            let s: &str = unsafe { std::str::from_utf8_unchecked(b) };
            s.hash(state);
        }
        Value::Date(v) => {
            9_u8.hash(state);
            v.hash(state);
        }
        Value::Time(v) => {
            10_u8.hash(state);
            v.hash(state);
        }
        Value::DateTime(v) => {
            11_u8.hash(state);
            v.hash(state);
        }
        Value::Timestamp(v) => {
            16_u8.hash(state);
            v.hash(state);
        }
        Value::Year(v) => {
            12_u8.hash(state);
            v.hash(state);
        }
        Value::Json(v) => {
            13_u8.hash(state);
            v.hash(state);
        }
        Value::Geometry(v) => {
            14_u8.hash(state);
            v.hash(state);
        }
        Value::Vector(v) => {
            15_u8.hash(state);
            v.hash(state);
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CompileError {
    Unsupported,
    TooDeep,
}

impl From<QueryError> for CompileError {
    fn from(_value: QueryError) -> Self {
        CompileError::Unsupported
    }
}

/// Bind-time binary semantics travel together through folding and execution.
/// Comparison provenance and arithmetic warning policies are separate: the
/// former must never double as a flag for integer-division diagnostics.
#[derive(Debug, Clone, Copy, Default)]
struct BinarySemantics {
    unsigned_subtraction: bool,
    collation_id: Option<u16>,
    suppress_left_string_truncation: bool,
    suppress_right_string_truncation: bool,
    comparison: ComparisonProvenance,
    suppress_int_div_truncation: bool,
    exact_left_numeric: bool,
    exact_right_numeric: bool,
}

impl BinarySemantics {
    fn compare(&self, left: &Value, op: BinaryOp, right: &Value) -> Result<Value, QueryError> {
        let evaluate = || {
            crate::engine::expr::with_numeric_comparison_context_from_provenance(
                self.comparison,
                left,
                right,
                || {
                    crate::engine::expr::with_compare_collation(self.collation_id, || {
                        null_cmp_binary(left, op, right)
                    })
                },
            )
        };
        if self.suppress_left_string_truncation || self.suppress_right_string_truncation {
            crate::engine::expr::with_suppressed_expression_warnings(evaluate)
        } else {
            evaluate()
        }
    }
}

#[derive(Debug, Clone)]
enum ExprNode {
    Column(usize),
    FieldCount,
    Const(Value),
    Placeholder(usize),
    Unary {
        op: UnaryOp,
        expr: Box<ExprNode>,
        /// Whether unary minus must apply the runtime UNSIGNED BIGINT domain.
        /// A bare positive UInt literal is the one exception: MySQL promotes
        /// `-9223372036854775809` to exact DECIMAL instead of overflowing.
        enforce_unsigned_runtime_domain: bool,
        /// TEXT/BLOB columns use MySQL's legacy silent implicit numeric
        /// conversion. The runtime Value lane no longer carries that declared
        /// source-type distinction.
        suppress_string_truncation: bool,
        duplicate_div_zero_warning: bool,
    },
    Binary {
        left: Box<ExprNode>,
        op: BinaryOp,
        right: Box<ExprNode>,
        semantics: BinarySemantics,
    },
    IsNull(Box<ExprNode>),
    IsNotNull(Box<ExprNode>),
    InList {
        expr: Box<ExprNode>,
        list: Vec<ExprNode>,
        negated: bool,
    },
    InConstSet {
        expr: Box<ExprNode>,
        values: Vec<Value>,
        keys: HashSet<ValueKey>,
        hash_class: Option<InHashClass>,
        has_null: bool,
        negated: bool,
    },
    SubstringConst {
        expr: Box<ExprNode>,
        start: i64,
        len: Option<i64>,
    },
    Between {
        expr: Box<ExprNode>,
        low: Box<ExprNode>,
        high: Box<ExprNode>,
        negated: bool,
        /// Effective column collation for all three ordered comparisons.
        /// BETWEEN must not fall back to the connection default when its
        /// tested value comes from a PAD SPACE or binary-collated column.
        collation_id: Option<u16>,
    },
    Like {
        expr: Box<ExprNode>,
        pattern: Box<ExprNode>,
        negated: bool,
        escape_char: Option<char>,
        /// Column collation id resolved at compile time for the value
        /// operand. B1: `col LIKE ?` must honour `ColumnSchema.collation`
        /// even when the pattern isn't compile-time constant — the
        /// resolved id survives fold_constants (which has no schema)
        /// and gets forwarded to `eval_like_values_with_collation` at
        /// eval time. `None` means the operand carries no collation
        /// (numeric / computed / no column ref) and the caller falls
        /// back to the process default.
        collation_id: Option<u16>,
    },
    /// `expr LIKE <const-pattern>` — pattern was a compile-time constant
    /// so the matcher is pre-classified into a byte-level fast path
    /// (`Literal`/`Prefix`/`Suffix`/`Contains`/`WildcardAny`) or a
    /// pre-decoded char slice for the general NFA. Skips per-row
    /// `chars().collect()` on the pattern entirely.
    LikeConst {
        expr: Box<ExprNode>,
        plan: LikePlan,
        negated: bool,
    },
    Case {
        operand: Option<Box<ExprNode>>,
        conditions: Vec<ExprNode>,
        results: Vec<ExprNode>,
        else_result: Option<Box<ExprNode>>,
        result_coercion: Option<crate::engine::expr_type::ExprType>,
    },
    Function {
        kernel: FunctionKernel,
        args: Vec<ExprNode>,
        /// Aggregated MySQL collation/coercibility domain for N-ary string
        /// comparisons. This is derived from every source operand, not just
        /// the first column, and remains available after AST lowering.
        collation_id: Option<u16>,
        /// Numeric functions convert declared TEXT/BLOB inputs through
        /// MySQL's legacy no-diagnostic lane.  Runtime values alone cannot
        /// distinguish those catalog types from bounded strings.
        suppress_string_truncation: bool,
        /// A conditional nested inside a larger expression still has a
        /// statically bound result domain.  Carry that domain on the compiled
        /// node instead of escaping the whole COALESCE/IFNULL/IF subtree to
        /// the row interpreter for every input row.
        result_coercion: Option<crate::engine::expr_type::ExprType>,
    },
    /// A sub-expression the compiler couldn't lower, but which the legacy
    /// interpreter *can* evaluate as a pure scalar (an unsupported
    /// function such as `CAST`/`POW`, a `||` concat, …). The surrounding
    /// expression stays compiled; only this node drops to legacy. Column
    /// references inside `expr` were rewritten at compile time to synthetic
    /// `\0c{idx}` identifiers that `schema` resolves positionally, so the
    /// node is self-contained and session-cacheable. `columns` records the
    /// input indices it reads (for `collect_columns_in_node`).
    LegacyEscape {
        expr: Box<Expr>,
        schema: Arc<TableSchema>,
        columns: Vec<usize>,
        diagnostic_sql: Option<String>,
    },
}

/// Heap owned by a cloned parser expression below its inline root. This is
/// used for the collision-verification AST retained by the cache.
pub(crate) fn expr_ast_heap_bytes(expr: &Expr) -> usize {
    let boxed = |expr: &Expr| std::mem::size_of::<Expr>() + expr_ast_heap_bytes(expr);
    let vector = |exprs: &[Expr], capacity: usize| {
        capacity * std::mem::size_of::<Expr>()
            + exprs.iter().map(expr_ast_heap_bytes).sum::<usize>()
    };
    match expr {
        Expr::Identifier(name) | Expr::UserVariable(name) => name.capacity(),
        Expr::CompoundIdentifier(parts) => {
            parts.capacity() * std::mem::size_of::<String>()
                + parts.iter().map(String::capacity).sum::<usize>()
        }
        Expr::SystemVariable { name, .. } => name.capacity(),
        Expr::Default(name) => name.as_ref().map_or(0, String::capacity),
        Expr::DefaultQualified(parts) => {
            parts.capacity() * std::mem::size_of::<String>()
                + parts.iter().map(String::capacity).sum::<usize>()
        }
        Expr::Literal(literal) => match literal {
            Literal::String(value) | Literal::Decimal(value) => value.capacity(),
            Literal::HexBytes(value) | Literal::BitBytes(value) => value.capacity(),
            _ => 0,
        },
        Expr::BoundValue(value) => Value::heap_capacity(value),
        Expr::Unary { expr, .. }
        | Expr::IsNull(expr)
        | Expr::IsNotNull(expr)
        | Expr::GroupKeyRef { expr, .. } => boxed(expr),
        Expr::Binary { left, right, .. } => boxed(left) + boxed(right),
        Expr::InList { expr, list, .. } => boxed(expr) + vector(list, list.capacity()),
        Expr::InSubquery { expr, subquery, .. } => {
            boxed(expr) + std::mem::size_of_val(subquery.as_ref())
        }
        Expr::QuantifiedComparison { left, right, .. } => {
            boxed(left)
                + match right {
                    QuantifiedComparisonRight::Subquery(subquery) => {
                        std::mem::size_of_val(subquery.as_ref())
                    }
                    QuantifiedComparisonRight::List(list) => vector(list, list.capacity()),
                }
        }
        Expr::Subquery(subquery) | Expr::Exists { subquery, .. } => {
            std::mem::size_of_val(subquery.as_ref())
        }
        Expr::Between {
            expr, low, high, ..
        } => boxed(expr) + boxed(low) + boxed(high),
        Expr::Like { expr, pattern, .. } => boxed(expr) + boxed(pattern),
        Expr::Case {
            operand,
            conditions,
            results,
            else_result,
        } => {
            operand.as_deref().map_or(0, boxed)
                + vector(conditions, conditions.len())
                + vector(results, results.capacity())
                + else_result.as_deref().map_or(0, boxed)
        }
        Expr::Function { name, args, .. } => name.capacity() + vector(args, args.capacity()),
        Expr::Tuple(items) => vector(items, items.capacity()),
        Expr::Grouping(indices) => indices.capacity() * std::mem::size_of::<usize>(),
        Expr::WindowFunction { name, args, spec } => {
            let crate::parser::WindowSpec {
                partition_by,
                order_by,
                frame,
                inherits,
                ..
            } = spec.as_ref();

            let order_bytes = order_by.capacity() * std::mem::size_of::<crate::parser::OrderBy>()
                + order_by
                    .iter()
                    .map(|item| expr_ast_heap_bytes(&item.expr))
                    .sum::<usize>();
            let frame_bytes = frame.as_ref().map_or(0, |frame| {
                use crate::parser::WindowFrameBound::{Following, Preceding};
                [&frame.start, &frame.end]
                    .into_iter()
                    .map(|bound| match bound {
                        Preceding(expr) | Following(expr) => boxed(expr),
                        _ => 0,
                    })
                    .sum()
            });
            std::mem::size_of::<crate::parser::WindowSpec>()
                + name.capacity()
                + inherits.as_ref().map_or(0, String::capacity)
                + vector(args, args.capacity())
                + vector(partition_by, partition_by.capacity())
                + order_bytes
                + frame_bytes
        }
        Expr::Wildcard | Expr::Placeholder(_) => 0,
    }
}

fn like_plan_heap_bytes(plan: &LikePlan) -> usize {
    match plan {
        LikePlan::Literal(value)
        | LikePlan::Prefix(value)
        | LikePlan::Suffix(value)
        | LikePlan::Contains(value) => value.capacity(),
        LikePlan::WildcardAny => 0,
        LikePlan::MultiSegment { segments, .. } => segments
            .len()
            .saturating_mul(std::mem::size_of::<LikeSegment>())
            .saturating_add(
                segments
                    .iter()
                    // Finder::into_owned retains its own needle copy.
                    .map(|segment| segment.bytes.len().saturating_mul(2))
                    .sum::<usize>(),
            ),
        LikePlan::General { chars, .. } => chars.len() * std::mem::size_of::<char>(),
        LikePlan::CiAscii {
            folded,
            chars,
            raw,
            char_cache,
            ..
        } => std::mem::size_of::<LikePlan>()
            .saturating_add(like_plan_heap_bytes(folded))
            .saturating_add(chars.len() * std::mem::size_of::<char>())
            .saturating_add(raw.len())
            .saturating_add(char_cache.heap_capacity_bytes()),
        LikePlan::CiGeneral { chars, raw, .. } => chars
            .len()
            .saturating_mul(std::mem::size_of::<char>())
            .saturating_add(raw.len()),
    }
}

impl ExprNode {
    /// Heap owned below this node. Container capacities are charged, not
    /// lengths, so spare allocation retained by a cached program is visible
    /// to the global memory governor.
    fn heap_capacity_bytes(&self) -> usize {
        let boxed = |node: &ExprNode| {
            std::mem::size_of::<ExprNode>().saturating_add(node.heap_capacity_bytes())
        };
        let vector = |nodes: &[ExprNode], capacity: usize| {
            capacity
                .saturating_mul(std::mem::size_of::<ExprNode>())
                .saturating_add(
                    nodes
                        .iter()
                        .map(ExprNode::heap_capacity_bytes)
                        .sum::<usize>(),
                )
        };
        match self {
            ExprNode::Column(_) | ExprNode::FieldCount | ExprNode::Placeholder(_) => 0,
            ExprNode::Const(value) => Value::heap_capacity(value),
            ExprNode::Unary { expr, .. }
            | ExprNode::IsNull(expr)
            | ExprNode::IsNotNull(expr)
            | ExprNode::SubstringConst { expr, .. } => boxed(expr),
            ExprNode::Binary { left, right, .. } => boxed(left).saturating_add(boxed(right)),
            ExprNode::InList { expr, list, .. } => {
                boxed(expr).saturating_add(vector(list, list.capacity()))
            }
            ExprNode::InConstSet {
                expr, values, keys, ..
            } => boxed(expr)
                .saturating_add(values.capacity() * std::mem::size_of::<Value>())
                .saturating_add(values.iter().map(Value::heap_capacity).sum::<usize>())
                .saturating_add(
                    keys.capacity()
                        * (std::mem::size_of::<ValueKey>() + std::mem::size_of::<usize>()),
                )
                .saturating_add(keys.iter().map(ValueKey::payload_size).sum::<usize>()),
            ExprNode::Between {
                expr, low, high, ..
            } => boxed(expr)
                .saturating_add(boxed(low))
                .saturating_add(boxed(high)),
            ExprNode::Like { expr, pattern, .. } => boxed(expr).saturating_add(boxed(pattern)),
            ExprNode::LikeConst { expr, plan, .. } => {
                boxed(expr).saturating_add(like_plan_heap_bytes(plan))
            }
            ExprNode::Case {
                operand,
                conditions,
                results,
                else_result,
                result_coercion: _,
            } => operand
                .as_deref()
                .map_or(0, boxed)
                .saturating_add(vector(conditions, conditions.capacity()))
                .saturating_add(vector(results, results.capacity()))
                .saturating_add(else_result.as_deref().map_or(0, boxed)),
            ExprNode::Function { args, .. } => vector(args, args.capacity()),
            ExprNode::LegacyEscape {
                expr,
                schema,
                columns,
                diagnostic_sql,
            } => std::mem::size_of::<Expr>()
                .saturating_add(diagnostic_sql.as_ref().map_or(0, String::capacity))
                .saturating_add(expr_ast_heap_bytes(expr))
                .saturating_add(std::mem::size_of_val(schema.as_ref()))
                .saturating_add(columns.capacity() * std::mem::size_of::<usize>()),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum InHashClass {
    /// Exact integral values share the canonical `ValueKey::Int` domain.
    /// Keep floating point / DECIMAL outside this class: those families have
    /// coercion and NaN corners that still require `compare_values`.
    Integer,
    String,
    Bytes,
    Date,
    Time,
    DateTime,
    Timestamp,
    Year,
}

const LIKE_CHAR_ASCII_CACHE_SLOTS: usize = 256;

/// Fixed-size, lock-free memo table for the collation weight of non-ASCII
/// characters encountered by an ASCII LIKE plan. A slot stores the Unicode
/// scalar plus either its one-byte ASCII weight or a negative result. Direct
/// mapping deliberately tolerates collisions: they only cause recomputation,
/// never a semantic change.
#[derive(Debug)]
pub(crate) struct LikeCharAsciiCache {
    slots: Box<[AtomicU64]>,
}

impl LikeCharAsciiCache {
    fn new() -> Self {
        Self {
            slots: (0..LIKE_CHAR_ASCII_CACHE_SLOTS)
                .map(|_| AtomicU64::new(0))
                .collect(),
        }
    }

    #[inline]
    fn weight(&self, info: &crate::charset::CollationInfo, value: char) -> Option<u8> {
        debug_assert!(!value.is_ascii());
        let scalar = value as u32;
        let slot = &self.slots[scalar as usize & (LIKE_CHAR_ASCII_CACHE_SLOTS - 1)];
        let cached = slot.load(AtomicOrdering::Relaxed);
        if (cached >> 8) as u32 == scalar + 1 {
            let encoded = cached as u8;
            return if encoded >= 2 {
                Some(encoded - 2)
            } else {
                None
            };
        }
        let weight = crate::charset::like_char_ascii_weight_for_info(info, value);
        let encoded = weight.map_or(1u8, |byte| byte + 2);
        slot.store(
            ((u64::from(scalar) + 1) << 8) | u64::from(encoded),
            AtomicOrdering::Relaxed,
        );
        weight
    }

    fn heap_capacity_bytes(&self) -> usize {
        self.slots.len() * std::mem::size_of::<AtomicU64>()
    }
}

impl Clone for LikeCharAsciiCache {
    fn clone(&self) -> Self {
        // A cloned cached plan starts cold rather than copying transient
        // execution observations. Both instances retain identical semantics.
        Self::new()
    }
}

/// Pre-classified plan for `expr LIKE <const-pattern>`.
///
/// All non-`General` variants short-circuit `match_like_pattern`'s
/// per-row `chars().collect()` on both sides — `starts_with` /
/// `ends_with` / `contains` / `==` work directly on the value's UTF-8
/// bytes via `str` methods. `General` still falls back to the char-NFA
/// matcher but with the pattern's chars decoded once at compile time.
#[derive(Debug, Clone)]
pub(crate) enum LikePlan {
    /// No wildcards in pattern: value must be byte-equal to `lit`.
    Literal(String),
    /// `lit%`: value must start with `lit`.
    Prefix(String),
    /// `%lit`: value must end with `lit`.
    Suffix(String),
    /// `%lit%`: value must contain `lit`.
    Contains(String),
    /// One or more `%` with nothing else: any value matches (after the
    /// existing `value_to_string` coercion — empty included, see
    /// `like_match` for the legacy ordering).
    WildcardAny,
    /// Two or more literal ASCII segments separated by `%` (no `_`, no
    /// escape collision) — e.g. `%special%requests%`. Each segment
    /// carries a pre-built SIMD-accelerated `memchr::memmem::Finder`
    /// so the per-row match path skips the per-call Finder build
    /// (which on aarch64 + x86_64 walks needle bytes to pick the
    /// "rare byte" pair and seed the searcher). On a 1.4M-row Q13
    /// probe that's ~3M skipped Finder constructions.
    MultiSegment {
        anchored_start: bool,
        anchored_end: bool,
        /// Segment with the rarest ASCII anchor byte. Probing it first is a
        /// cheap necessary-condition rejection before the ordered matcher.
        reject_segment: usize,
        segments: Box<[LikeSegment]>,
    },
    /// Anything else (contains `_`, non-ASCII pattern, escape clause,
    /// …): pre-decoded char slice fed to the legacy `like_match` NFA.
    General {
        chars: Box<[char]>,
        escape: Option<char>,
    },
    /// Default-collation case-insensitive pattern (the default
    /// collation is CI, so this is the common shape). `folded` is the
    /// byte plan classified over the ASCII-lowercased pattern: a pure
    /// ASCII value folds into thread-local scratch and runs it at byte
    /// speed; a non-ASCII value falls back to the per-char collation
    /// NFA over `chars` (the accent-insensitive default collation can
    /// equate non-ASCII value chars with ASCII pattern chars). `raw`
    /// keeps the original pattern for binary-string operands, which
    /// stay byte-exact.
    CiAscii {
        folded: Box<LikePlan>,
        chars: Box<[char]>,
        raw: Box<str>,
        collation_id: u16,
        char_cache: LikeCharAsciiCache,
    },
    /// CI pattern that can't pre-fold to ASCII bytes (non-ASCII
    /// pattern or explicit ESCAPE): always the per-char collation NFA.
    CiGeneral {
        chars: Box<[char]>,
        escape: Option<char>,
        raw: Box<str>,
        collation_id: u16,
    },
}

/// One literal segment of a `LikePlan::MultiSegment` pattern. Owns
/// the segment bytes AND a pre-built `memchr::memmem::Finder` keyed
/// to those bytes — pre-compiled at classify time so the per-row
/// match path goes straight to `Finder::find` without rebuilding the
/// searcher state.
#[derive(Debug, Clone)]
pub(crate) struct LikeSegment {
    bytes: Box<[u8]>,
    finder: memchr::memmem::Finder<'static>,
    /// Rarest case-folded byte used by the ASCII CI matcher.  Pattern
    /// classification is statement scoped, so deriving this once here avoids
    /// rescanning every needle for every input row.
    ci_anchor: usize,
}

impl LikeSegment {
    fn new(bytes: Vec<u8>) -> Self {
        let bytes = bytes.into_boxed_slice();
        // `Finder::new(&bytes[..]).into_owned()` clones the needle into
        // the Finder's internal storage so `finder` has the same
        // lifetime as `Self`. The cloned copy is what the searcher
        // actually drives; `bytes` stays owned by us for cheap
        // `as_ref()` access in the head/tail anchor branches.
        let finder = memchr::memmem::Finder::new(bytes.as_ref()).into_owned();
        let ci_anchor = ci_needle_anchor(bytes.as_ref());
        LikeSegment {
            bytes,
            finder,
            ci_anchor,
        }
    }

    #[inline]
    pub(crate) fn bytes(&self) -> &[u8] {
        &self.bytes
    }

    #[inline]
    pub(crate) fn find(&self, haystack: &[u8]) -> Option<usize> {
        self.finder.find(haystack)
    }

    #[inline]
    pub(crate) fn len(&self) -> usize {
        self.bytes.len()
    }

    #[inline]
    fn find_ci_ascii(&self, haystack: &[u8]) -> Option<usize> {
        ci_find_with_anchor(haystack, self.bytes(), self.ci_anchor)
    }
}

thread_local! {
    /// Reusable haystack buffer for the char-level LIKE fallbacks
    /// (`General` / `CiGeneral`): kills the per-row
    /// `chars().collect::<Vec<char>>()` allocation. `try_borrow_mut`
    /// guards (hypothetical) reentrancy by degrading to a fresh Vec.
    static LIKE_CHARS_SCRATCH: std::cell::RefCell<Vec<char>> =
        const { std::cell::RefCell::new(Vec::new()) };
}

fn with_haystack_chars<R>(value: &str, f: impl FnOnce(&[char]) -> R) -> R {
    LIKE_CHARS_SCRATCH.with(|cell| match cell.try_borrow_mut() {
        Ok(mut buf) => {
            buf.clear();
            buf.extend(value.chars());
            f(&buf)
        }
        Err(_) => {
            let value_chars: Vec<char> = value.chars().collect();
            f(&value_chars)
        }
    })
}

impl LikePlan {
    /// Match a UTF-8 string against this pre-classified pattern. The
    /// `MultiSegment` arm and the literal/prefix/suffix/contains arms
    /// all stay on byte slices — only the rare `General` fallback
    /// pays a `chars().collect()` on the value, and only when the
    /// pattern itself forced char-level semantics at compile time.
    #[inline]
    pub(crate) fn matches_str(&self, value: &str) -> bool {
        match self {
            LikePlan::Literal(p) => value == p.as_str(),
            LikePlan::Prefix(p) => value.starts_with(p.as_str()),
            LikePlan::Suffix(p) => value.ends_with(p.as_str()),
            LikePlan::Contains(p) => value.contains(p.as_str()),
            LikePlan::WildcardAny => true,
            LikePlan::MultiSegment {
                anchored_start,
                anchored_end,
                reject_segment,
                segments,
            } => match_like_multi_segment_compiled(
                value.as_bytes(),
                *anchored_start,
                *anchored_end,
                *reject_segment,
                segments,
            ),
            LikePlan::General { chars, escape } => with_haystack_chars(value, |value_chars| {
                like_match_chars(value_chars, chars, *escape)
            }),
            LikePlan::CiAscii {
                folded,
                chars,
                collation_id,
                char_cache,
                ..
            } => {
                // Read-only ASCII gate (SIMD `is_ascii`), then a
                // zero-copy case-insensitive byte match against the
                // folded plan — no per-row haystack fold. Non-ASCII
                // values need the collation NFA (accent folding).
                if value.is_ascii() {
                    folded.matches_ci_ascii(value.as_bytes())
                } else {
                    let info = crate::charset::resolve_collation_id(*collation_id)
                        .unwrap_or_else(|| crate::charset::default_collation_info());
                    folded
                        .matches_ci_ascii_utf8(value, info, char_cache)
                        .unwrap_or_else(|| {
                            with_haystack_chars(value, |value_chars| {
                                crate::engine::expr::like_match_chars_with_info(
                                    value_chars,
                                    chars,
                                    None,
                                    info,
                                )
                            })
                        })
                }
            }
            LikePlan::CiGeneral {
                chars,
                escape,
                collation_id,
                ..
            } => {
                let info = crate::charset::resolve_collation_id(*collation_id)
                    .unwrap_or_else(|| crate::charset::default_collation_info());
                with_haystack_chars(value, |value_chars| {
                    crate::engine::expr::like_match_chars_with_info(
                        value_chars,
                        chars,
                        *escape,
                        info,
                    )
                })
            }
        }
    }

    /// Match a haystack that the caller has already proved is ASCII.  The
    /// case-insensitive ASCII plan normally performs its own `is_ascii()`
    /// gate because general expression callers can pass arbitrary UTF-8.
    /// Predicate-first heap decoding must validate the borrowed bytes before
    /// constructing `&str`, so reusing that proof avoids a second full scan of
    /// every string while preserving the ordinary matcher as the oracle for
    /// all other plan families.
    #[inline]
    pub(crate) fn matches_ascii_bytes(&self, value: &[u8]) -> bool {
        if let LikePlan::CiAscii { folded, .. } = self {
            return folded.matches_ci_ascii(value);
        }
        // SAFETY: this entry point's contract requires ASCII, which is a
        // strict subset of UTF-8.
        self.matches_str(unsafe { std::str::from_utf8_unchecked(value) })
    }

    /// Case-insensitive matcher for the *inner* (folded-pattern) plan
    /// of a `CiAscii` wrapper, over a pure-ASCII haystack, zero-copy:
    /// candidate positions come from `memchr2` on the needle head's
    /// two cases and windows confirm with `eq_ignore_ascii_case`.
    /// Needles are already lowercase (classification folds them).
    fn matches_ci_ascii(&self, value: &[u8]) -> bool {
        match self {
            LikePlan::Literal(p) => value.eq_ignore_ascii_case(p.as_bytes()),
            LikePlan::Prefix(p) => {
                value.len() >= p.len() && value[..p.len()].eq_ignore_ascii_case(p.as_bytes())
            }
            LikePlan::Suffix(p) => {
                value.len() >= p.len()
                    && value[value.len() - p.len()..].eq_ignore_ascii_case(p.as_bytes())
            }
            LikePlan::Contains(p) => ci_find(value, p.as_bytes()).is_some(),
            LikePlan::WildcardAny => true,
            LikePlan::MultiSegment {
                anchored_start,
                anchored_end,
                reject_segment,
                segments,
            } => {
                if segments[*reject_segment].find_ci_ascii(value).is_none() {
                    return false;
                }
                let mut pos = 0usize;
                let mut idx = 0usize;
                if *anchored_start {
                    let first = segments[0].bytes();
                    if value.len() < first.len()
                        || !value[..first.len()].eq_ignore_ascii_case(first)
                    {
                        return false;
                    }
                    pos = first.len();
                    idx = 1;
                }
                let last_idx = segments.len() - 1;
                let end_idx = if *anchored_end {
                    last_idx
                } else {
                    segments.len()
                };
                while idx < end_idx {
                    let seg = &segments[idx];
                    match seg.find_ci_ascii(&value[pos..]) {
                        Some(off) => pos += off + seg.len(),
                        None => return false,
                    }
                    idx += 1;
                }
                if *anchored_end {
                    let tail = segments[last_idx].bytes();
                    if value.len() < pos + tail.len() {
                        return false;
                    }
                    let tail_start = value.len() - tail.len();
                    if !value[tail_start..].eq_ignore_ascii_case(tail) {
                        return false;
                    }
                }
                true
            }
            // `_`-bearing folded pattern: char NFA with the per-char
            // collation equality (ASCII pairs short-circuit inside).
            LikePlan::General { chars, escape } => {
                LIKE_CHARS_SCRATCH.with(|cell| match cell.try_borrow_mut() {
                    Ok(mut buf) => {
                        buf.clear();
                        buf.extend(value.iter().map(|&b| b as char));
                        crate::engine::expr::like_match_chars_ci(&buf, chars, *escape)
                    }
                    Err(_) => {
                        let value_chars: Vec<char> = value.iter().map(|&b| b as char).collect();
                        crate::engine::expr::like_match_chars_ci(&value_chars, chars, *escape)
                    }
                })
            }
            // CI wrappers never nest.
            LikePlan::CiAscii { .. } | LikePlan::CiGeneral { .. } => {
                debug_assert!(false, "nested CI LikePlan");
                false
            }
        }
    }

    /// Case-insensitive matching for an ASCII constant pattern over a UTF-8
    /// haystack. Simple literal/prefix/suffix/contains plans can compare one
    /// decoded input character with one pre-folded ASCII pattern byte, so a
    /// non-ASCII byte elsewhere in the value no longer sends the whole row
    /// through the allocating char-NFA/collation-key path.
    fn matches_ci_ascii_utf8(
        &self,
        value: &str,
        info: &crate::charset::CollationInfo,
        cache: &LikeCharAsciiCache,
    ) -> Option<bool> {
        match self {
            LikePlan::Literal(pattern) => Some(ci_ascii_literal_utf8(value, pattern, info, cache)),
            LikePlan::Prefix(pattern) => Some(ci_ascii_prefix_utf8(value, pattern, info, cache)),
            LikePlan::Suffix(pattern) => Some(ci_ascii_suffix_utf8(value, pattern, info, cache)),
            LikePlan::Contains(pattern) => {
                Some(ci_ascii_contains_utf8(value, pattern, info, cache))
            }
            LikePlan::WildcardAny => Some(true),
            LikePlan::MultiSegment { .. } | LikePlan::General { .. } => None,
            LikePlan::CiAscii { .. } | LikePlan::CiGeneral { .. } => {
                debug_assert!(false, "nested CI LikePlan");
                Some(false)
            }
        }
    }

    /// Value-level entry: binary strings (`Value::Bytes`) keep
    /// byte-exact semantics against the original pattern even when the
    /// plan itself was folded for the CI default collation. NULL is the
    /// caller's job (compiled predicates and `LikeConst` differ there).
    #[inline]
    pub(crate) fn matches_value(&self, value: &Value) -> bool {
        // `str_value` borrows the String/SmallString/valid-UTF-8-Bytes
        // lanes — the haystack copy this used to pay per row (via
        // `value_to_string`) dominated the fast Literal/Prefix plans.
        if matches!(value, Value::Bytes(_)) {
            match self {
                LikePlan::CiAscii { raw, .. } => {
                    return crate::engine::expr::match_like_pattern(
                        &crate::engine::expr::str_value(value),
                        raw,
                        None,
                    );
                }
                LikePlan::CiGeneral { raw, escape, .. } => {
                    return crate::engine::expr::match_like_pattern(
                        &crate::engine::expr::str_value(value),
                        raw,
                        *escape,
                    );
                }
                _ => {}
            }
        }
        self.matches_str(&crate::engine::expr::str_value(value))
    }
}

/// Rough corpus frequency for ASCII bytes (higher = more common in
/// text), used to anchor the CI search on the needle's *rarest* byte —
/// the same idea as memmem's rare-byte heuristic. Case-folded: upper
/// and lower share a rank.
const CI_BYTE_FREQ: [u8; 128] = {
    let mut f = [40u8; 128];
    f[b' ' as usize] = 255;
    let ranks: &[(u8, u8)] = &[
        (b'e', 250),
        (b't', 245),
        (b'a', 240),
        (b'o', 235),
        (b'i', 230),
        (b'n', 225),
        (b's', 220),
        (b'r', 215),
        (b'h', 210),
        (b'l', 205),
        (b'd', 200),
        (b'c', 195),
        (b'u', 190),
        (b'm', 185),
        (b'f', 180),
        (b'p', 175),
        (b'g', 170),
        (b'w', 165),
        (b'y', 160),
        (b'b', 155),
        (b'v', 150),
        (b'k', 145),
        (b'x', 60),
        (b'j', 55),
        (b'q', 50),
        (b'z', 45),
    ];
    let mut i = 0;
    while i < ranks.len() {
        let (b, r) = ranks[i];
        f[b as usize] = r;
        f[(b - 32) as usize] = r; // matching uppercase letter
        i += 1;
    }
    let mut d = b'0';
    while d <= b'9' {
        f[d as usize] = 120;
        d += 1;
    }
    f
};

/// Case-insensitive substring search over ASCII bytes, zero-copy:
/// `memchr2` over both cases of the needle's rarest byte yields
/// candidates, the window confirms with `eq_ignore_ascii_case`.
/// `needle` must already be lowercase.
fn ci_find(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    if needle.is_empty() {
        return Some(0);
    }
    if needle.len() > haystack.len() {
        return None;
    }
    ci_find_with_anchor(haystack, needle, ci_needle_anchor(needle))
}

#[inline(always)]
fn ci_ascii_char_equal(
    info: &crate::charset::CollationInfo,
    cache: &LikeCharAsciiCache,
    value: char,
    pattern: u8,
) -> bool {
    if value.is_ascii()
        || (info.pad_space && pattern == b' ')
        || (info.uca_0900 && pattern == b'\0')
    {
        return crate::charset::like_char_equals_ascii_for_info(info, value, pattern);
    }
    cache.weight(info, value) == Some(pattern.to_ascii_lowercase())
}

#[inline]
fn ci_ascii_prefix_utf8(
    value: &str,
    pattern: &str,
    info: &crate::charset::CollationInfo,
    cache: &LikeCharAsciiCache,
) -> bool {
    let mut chars = value.chars();
    pattern.bytes().all(|byte| {
        chars
            .next()
            .is_some_and(|ch| ci_ascii_char_equal(info, cache, ch, byte))
    })
}

#[inline]
fn ci_ascii_literal_utf8(
    value: &str,
    pattern: &str,
    info: &crate::charset::CollationInfo,
    cache: &LikeCharAsciiCache,
) -> bool {
    let mut chars = value.chars();
    pattern.bytes().all(|byte| {
        chars
            .next()
            .is_some_and(|ch| ci_ascii_char_equal(info, cache, ch, byte))
    }) && chars.next().is_none()
}

#[inline]
fn ci_ascii_suffix_utf8(
    value: &str,
    pattern: &str,
    info: &crate::charset::CollationInfo,
    cache: &LikeCharAsciiCache,
) -> bool {
    let mut chars = value.chars().rev();
    pattern.bytes().rev().all(|byte| {
        chars
            .next()
            .is_some_and(|ch| ci_ascii_char_equal(info, cache, ch, byte))
    })
}

fn ci_ascii_contains_utf8(
    value: &str,
    pattern: &str,
    info: &crate::charset::CollationInfo,
    cache: &LikeCharAsciiCache,
) -> bool {
    let needle = pattern.as_bytes();
    if needle.is_empty() {
        return true;
    }
    // Most matches are wholly ASCII even inside an otherwise Unicode value.
    // UTF-8 continuation/leading bytes have their high bit set, so this raw
    // ASCII search cannot manufacture a match across a codepoint boundary.
    if ci_find(value.as_bytes(), needle).is_some() {
        return true;
    }
    // For the default collation, U+0400..U+047F never has a one-byte
    // ASCII weight (including after case/compatibility/accent folding).
    // If those are the only non-ASCII characters, the byte search above was
    // exhaustive. A vector byte-class check avoids decoding and probing the
    // shared per-character cache for every character of a rejected value.
    // This is a necessary-condition shortcut only; all other Unicode ranges
    // retain the exact character matcher below.
    if info.id == crate::charset::UTF8MB4_0900_AI_CI_ID && ascii_or_cyrillic_utf8(value.as_bytes())
    {
        return false;
    }
    let mut candidates = value.chars();
    while let Some(first) = candidates.next() {
        // The raw-byte search above already handled fully ASCII windows. Keep
        // the Unicode fallback cheap on mixed strings by rejecting ordinary
        // ASCII starting characters inline; only a matching head byte or a
        // genuinely non-ASCII collation equivalent needs the tail walk.
        let first_matches = if first.is_ascii() {
            let first = first as u8;
            if info.binary || info.case_sensitive {
                first == needle[0]
            } else {
                first.eq_ignore_ascii_case(&needle[0])
            }
        } else {
            ci_ascii_char_equal(info, cache, first, needle[0])
        };
        if !first_matches {
            continue;
        }
        let mut tail = candidates.clone();
        if needle[1..].iter().all(|&byte| {
            tail.next()
                .is_some_and(|ch| ci_ascii_char_equal(info, cache, ch, byte))
        }) {
            return true;
        }
    }
    false
}

/// The caller has already validated UTF-8. Continuation bytes plus D0/D1
/// leaders therefore describe exactly ASCII and U+0400..U+047F, not an
/// approximation of arbitrary Unicode. No temporary allocation is needed.
fn ascii_or_cyrillic_utf8(bytes: &[u8]) -> bool {
    #[cfg(target_arch = "x86_64")]
    if bytes.len() >= 32 && std::arch::is_x86_feature_detected!("avx2") {
        // SAFETY: feature detection and full-chunk bounds precede all loads.
        return unsafe { ascii_or_cyrillic_utf8_avx2(bytes) };
    }
    bytes
        .iter()
        .all(|&byte| byte <= 0xbf || byte & 0xfe == 0xd0)
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2")]
unsafe fn ascii_or_cyrillic_utf8_avx2(bytes: &[u8]) -> bool {
    use std::arch::x86_64::*;
    let mut chunks = bytes.chunks_exact(32);
    for chunk in chunks.by_ref() {
        let value = unsafe { _mm256_loadu_si256(chunk.as_ptr().cast()) };
        let low = _mm256_cmpeq_epi8(
            _mm256_min_epu8(value, _mm256_set1_epi8(0xbf_u8 as i8)),
            value,
        );
        let leader = _mm256_cmpeq_epi8(
            _mm256_and_si256(value, _mm256_set1_epi8(0xfe_u8 as i8)),
            _mm256_set1_epi8(0xd0_u8 as i8),
        );
        if _mm256_movemask_epi8(_mm256_or_si256(low, leader)) != -1 {
            return false;
        }
    }
    chunks
        .remainder()
        .iter()
        .all(|&byte| byte <= 0xbf || byte & 0xfe == 0xd0)
}

#[inline]
fn ci_needle_anchor(needle: &[u8]) -> usize {
    needle
        .iter()
        .enumerate()
        .min_by_key(|(_, &b)| CI_BYTE_FREQ[(b & 0x7f) as usize])
        .map(|(i, _)| i)
        .unwrap_or(0)
}

#[inline]
fn ci_find_with_anchor(haystack: &[u8], needle: &[u8], anchor: usize) -> Option<usize> {
    if needle.is_empty() {
        return Some(0);
    }
    if needle.len() > haystack.len() {
        return None;
    }
    debug_assert!(anchor < needle.len());
    let lo = needle[anchor];
    let up = lo.to_ascii_uppercase();
    let limit = haystack.len() - needle.len();
    // Anchored scan: look for the rare byte at candidate offset
    // `start + anchor`, i.e. in haystack[anchor ..= limit + anchor].
    let mut scan = anchor;
    let scan_end = limit + anchor;
    while scan <= scan_end {
        let off = if lo != up {
            memchr::memchr2(lo, up, &haystack[scan..=scan_end])
        } else {
            memchr::memchr(lo, &haystack[scan..=scan_end])
        }?;
        let hit = scan + off;
        let start = hit - anchor;
        if hit >= anchor && haystack[start..start + needle.len()].eq_ignore_ascii_case(needle) {
            return Some(start);
        }
        scan = hit + 1;
    }
    None
}

/// Byte-level matcher specialised for the `LikePlan::MultiSegment`
/// variant — each segment carries a pre-built `Finder`, so the loop
/// dispatches through `LikeSegment::find` rather than constructing a
/// fresh Finder per call (the `match_like_multi_segment` flavour does).
fn match_like_multi_segment_compiled(
    value: &[u8],
    anchored_start: bool,
    anchored_end: bool,
    reject_segment: usize,
    segments: &[LikeSegment],
) -> bool {
    debug_assert!(segments.len() >= 2);
    if segments[reject_segment].find(value).is_none() {
        return false;
    }
    let mut pos = 0usize;
    let mut start_idx = 0usize;
    if anchored_start {
        let first = segments[0].bytes();
        if !value.starts_with(first) {
            return false;
        }
        pos = first.len();
        start_idx = 1;
    }
    let last_idx = segments.len() - 1;
    let end_idx = if anchored_end {
        last_idx
    } else {
        segments.len()
    };
    let mut idx = start_idx;
    while idx < end_idx {
        let seg = &segments[idx];
        match seg.find(&value[pos..]) {
            Some(off) => pos += off + seg.len(),
            None => return false,
        }
        idx += 1;
    }
    if anchored_end {
        let last = &segments[last_idx];
        let tail = last.bytes();
        if value.len() < pos + tail.len() {
            return false;
        }
        let tail_start = value.len() - tail.len();
        if !value[tail_start..].eq(tail) {
            return false;
        }
    }
    true
}

/// Public wrapper around `classify_like_plan` so callers outside this
/// module (notably `CompiledRowPredicate::LikeConst` compilation in
/// `executor::select::plan`) can share the same byte-level classifier
/// — guarantees compiled-expression and compiled-row-predicate paths
/// agree on multi-segment, escape, and `_`-fallback semantics.
///
/// `collation` is the resolved column collation (from
/// `resolve_column_collation_id` mapped back through
/// `charset::resolve_collation_id`) or `None` when the LIKE operand
/// carries no column collation (constant vs. constant, computed
/// expression). `None` yields the process default — matching the
/// legacy behaviour before B1.
pub(crate) fn classify_like_plan_public(
    pattern: &str,
    escape: Option<char>,
    collation: Option<&crate::charset::CollationInfo>,
) -> LikePlan {
    classify_like_plan(pattern, escape, collation)
}

fn classify_like_plan(
    pattern: &str,
    escape: Option<char>,
    collation: Option<&crate::charset::CollationInfo>,
) -> LikePlan {
    // Escape chars that never occur in the pattern are semantically
    // absent; dropping them here keeps the byte fast paths available
    // under the default `\` escape.
    let escape = escape.filter(|&c| pattern.contains(c));
    // Pick the collation to reason about: the caller's when known,
    // otherwise the process default. `=` comparisons use the same
    // resolution, so `col = 'abc'` and `col LIKE 'abc'` agree on case
    // folding for the same operand pair. Classification happens once
    // per (cached) plan so the CI wrapper costs nothing per row.
    let info = collation.unwrap_or_else(|| crate::charset::default_collation_info());
    let is_ci = !info.binary && !info.case_sensitive;
    if is_ci {
        if escape.is_none() && pattern.is_ascii() {
            let folded = pattern.to_ascii_lowercase();
            let chars: Box<[char]> = folded.chars().collect();
            let plan = classify_like_plan_exact(&folded, None);
            return LikePlan::CiAscii {
                folded: Box::new(plan),
                chars,
                raw: pattern.into(),
                collation_id: info.id,
                char_cache: LikeCharAsciiCache::new(),
            };
        }
        return LikePlan::CiGeneral {
            chars: pattern.chars().collect(),
            escape,
            raw: pattern.into(),
            collation_id: info.id,
        };
    }
    classify_like_plan_exact(pattern, escape)
}

fn classify_like_plan_exact(pattern: &str, escape: Option<char>) -> LikePlan {
    let pat_chars: Vec<char> = pattern.chars().collect();

    #[derive(Clone, Copy)]
    enum Tok {
        Lit(char),
        Pct,
        Und,
    }
    let mut tokens: Vec<Tok> = Vec::with_capacity(pat_chars.len());
    let mut i = 0;
    let mut had_dangling_escape = false;
    while i < pat_chars.len() {
        let c = pat_chars[i];
        if Some(c) == escape {
            // Escape consumes the next char as literal. A dangling
            // escape at end matches the legacy `like_match` which
            // returns false for any non-empty input — handled below
            // by falling through to General which delegates to it.
            i += 1;
            if i < pat_chars.len() {
                tokens.push(Tok::Lit(pat_chars[i]));
                i += 1;
            } else {
                had_dangling_escape = true;
                break;
            }
        } else if c == '%' {
            tokens.push(Tok::Pct);
            i += 1;
        } else if c == '_' {
            tokens.push(Tok::Und);
            i += 1;
        } else {
            tokens.push(Tok::Lit(c));
            i += 1;
        }
    }

    let to_general = || LikePlan::General {
        chars: pat_chars.clone().into_boxed_slice(),
        escape,
    };

    if had_dangling_escape {
        return to_general();
    }
    // `_` matches exactly one char — using byte-level `starts_with` etc.
    // would silently treat it as a literal underscore. Delegate to the
    // char-level NFA.
    if tokens.iter().any(|t| matches!(t, Tok::Und)) {
        return to_general();
    }
    if tokens.is_empty() {
        // `Empty pattern matches only the empty value.
        return LikePlan::Literal(String::new());
    }
    if tokens.iter().all(|t| matches!(t, Tok::Pct)) {
        return LikePlan::WildcardAny;
    }
    let pct_count = tokens.iter().filter(|t| matches!(t, Tok::Pct)).count();
    if pct_count == 0 {
        let s: String = tokens
            .iter()
            .map(|t| match t {
                Tok::Lit(c) => *c,
                _ => unreachable!("pct_count==0 implies no %/_"),
            })
            .collect();
        return LikePlan::Literal(s);
    }
    let leading = tokens.iter().take_while(|t| matches!(t, Tok::Pct)).count();
    let trailing = tokens
        .iter()
        .rev()
        .take_while(|t| matches!(t, Tok::Pct))
        .count();
    let middle = &tokens[leading..tokens.len() - trailing];
    if middle.iter().any(|t| matches!(t, Tok::Pct)) {
        // `%` inside the literal body (e.g. `a%b%c`). When the whole
        // pattern is ASCII and `_`-free (and no escape was set —
        // already covered by Tok::Und guard above and the dangling
        // escape early exit), every literal run is a contiguous slice
        // of pattern bytes and matches byte-for-byte. Lift onto the
        // SIMD-friendly MultiSegment matcher; fall back to the
        // char-NFA `General` only when non-ASCII pattern chars or an
        // escape clause force char-level semantics.
        if escape.is_none() && pattern.is_ascii() {
            let bytes = pattern.as_bytes();
            let anchored_start = bytes[0] != b'%';
            let anchored_end = *bytes.last().unwrap() != b'%';
            let mut segments: Vec<LikeSegment> = Vec::new();
            let mut i = 0;
            while i < bytes.len() {
                if bytes[i] == b'%' {
                    i += 1;
                    continue;
                }
                let start = i;
                while i < bytes.len() && bytes[i] != b'%' {
                    i += 1;
                }
                segments.push(LikeSegment::new(bytes[start..i].to_vec()));
            }
            let reject_segment = segments
                .iter()
                .enumerate()
                .min_by_key(|(_, segment)| {
                    (
                        CI_BYTE_FREQ[(segment.bytes()[segment.ci_anchor] & 0x7f) as usize],
                        std::cmp::Reverse(segment.len()),
                    )
                })
                .map(|(index, _)| index)
                .unwrap_or(0);
            // `middle.iter().any(Tok::Pct)` above guarantees ≥2
            // segments here (one before the middle `%`, one after).
            return LikePlan::MultiSegment {
                anchored_start,
                anchored_end,
                reject_segment,
                segments: segments.into_boxed_slice(),
            };
        }
        return to_general();
    }
    let lit: String = middle
        .iter()
        .map(|t| match t {
            Tok::Lit(c) => *c,
            _ => unreachable!(),
        })
        .collect();
    match (leading > 0, trailing > 0) {
        (false, true) => LikePlan::Prefix(lit),
        (true, false) => LikePlan::Suffix(lit),
        (true, true) => LikePlan::Contains(lit),
        // pct_count > 0 was asserted, so at least one side has `%`.
        (false, false) => unreachable!("at least one leading or trailing %"),
    }
}

pub(crate) fn in_hash_class(value: &Value) -> Option<InHashClass> {
    match value {
        Value::Null => None,
        // Signed and unsigned integers canonicalise to the same exact i128
        // key, so this subset is safe to hash. Other numeric families retain
        // the comparison fallback for MySQL coercion and NaN semantics.
        Value::Int(_) | Value::UInt(_) => Some(InHashClass::Integer),
        Value::Float(_)
        | Value::Double(_)
        | Value::Decimal(_)
        | Value::DecimalCompact(..)
        | Value::Decimal128(..)
        | Value::DecimalWide(..) => None,
        Value::String(_) | Value::SmallString(_) => Some(InHashClass::String),
        Value::Bytes(_) => Some(InHashClass::Bytes),
        Value::Date(_) => Some(InHashClass::Date),
        Value::Time(_) => Some(InHashClass::Time),
        Value::DateTime(_) => Some(InHashClass::DateTime),
        Value::Timestamp(_) => Some(InHashClass::Timestamp),
        Value::Year(_) => Some(InHashClass::Year),
        // JSON IN-membership routes through `compare_values` (which knows
        // jsonb ordering); never narrow it onto the hash fast path.
        Value::Json(_) => None,
        // Geometry IN-membership routes through `compare_values` too.
        Value::Geometry(_) => None,
        // Vector IN-membership routes through `compare_values` too.
        Value::Vector(_) => None,
    }
}

fn common_in_hash_class(values: &[Value]) -> Option<InHashClass> {
    let mut class = None;
    for value in values {
        if matches!(value, Value::Null) {
            continue;
        }
        let value_class = in_hash_class(value)?;
        match class {
            None => class = Some(value_class),
            Some(prev) if prev == value_class => {}
            Some(_) => return None,
        }
    }
    class
}

fn const_compile_value(expr: &Expr) -> Option<Value> {
    match expr {
        Expr::Literal(lit) => Some(literal_value(lit)),
        Expr::BoundValue(value) => Some(value.clone()),
        _ => None,
    }
}

fn try_compile_in_const_set(
    expr: &Expr,
    list: &[Expr],
    negated: bool,
    schema: &InputSchema,
) -> Result<Option<ExprNode>, CompileError> {
    const MIN_HASH_LIST_LEN: usize = 4;
    let values = list
        .iter()
        .map(const_compile_value)
        .collect::<Option<Vec<_>>>();
    let Some(values) = values else {
        return Ok(None);
    };
    let has_null = values.iter().any(|value| matches!(value, Value::Null));
    let hash_class = if values.len() >= MIN_HASH_LIST_LEN {
        common_in_hash_class(&values)
    } else {
        None
    };
    let keys = if hash_class.is_some() {
        values
            .iter()
            .filter(|value| !matches!(value, Value::Null))
            .map(ValueKey::from)
            .collect()
    } else {
        HashSet::new()
    };
    Ok(Some(ExprNode::InConstSet {
        expr: Box::new(compile_node(expr, schema)?),
        values,
        keys,
        hash_class,
        has_null,
        negated,
    }))
}

fn try_compile_substring_const(
    name: &str,
    args: &[Expr],
    schema: &InputSchema,
) -> Result<Option<ExprNode>, CompileError> {
    let upper = name.to_ascii_uppercase();
    if upper != "SUBSTRING" && upper != "SUBSTR" && upper != "MID" {
        return Ok(None);
    }
    if !(2..=3).contains(&args.len()) {
        return Ok(None);
    }
    // Only bind diagnostic-free, non-NULL integer extents. Other constants
    // must retain runtime conversion/NULL propagation and replay warnings
    // on every execution of a cached program.
    let extent = |expr: &Expr| match const_compile_value(expr)? {
        Value::Int(value) => Some(value),
        Value::UInt(value) => Some(value.min(i64::MAX as u64) as i64),
        _ => None,
    };
    let Some(start) = extent(&args[1]) else {
        return Ok(None);
    };
    let len = if let Some(len_expr) = args.get(2) {
        let Some(len) = extent(len_expr) else {
            return Ok(None);
        };
        Some(len)
    } else {
        None
    };
    Ok(Some(ExprNode::SubstringConst {
        expr: Box::new(compile_node(&args[0], schema)?),
        start,
        len,
    }))
}

#[inline]
pub(crate) fn input_expr_column_index(expr: &Expr, schema: &InputSchema) -> Option<usize> {
    match expr {
        Expr::Identifier(name) => schema.column_index(None, name).ok(),
        Expr::CompoundIdentifier(parts) => {
            let column = parts.last()?;
            let qualifier = (parts.len() >= 2).then(|| parts[parts.len() - 2].as_str());
            schema.column_index(qualifier, column).ok()
        }
        _ => None,
    }
}

fn input_expr_result_type(expr: &Expr, schema: &InputSchema) -> ColumnType {
    crate::engine::expr_type::column_type::schema_expr_result_type(expr, &mut |column| {
        input_expr_column_index(column, schema)
            .and_then(|index| schema.columns.get(index))
            .map(|column| column.column_type)
            .unwrap_or(ColumnType::VarString)
    })
}

/// Bind both descriptor and wire projections against the same column scope.
pub(crate) fn input_expr_type(
    expr: &Expr,
    schema: &InputSchema,
) -> crate::engine::expr_type::ExprType {
    let resolve_column = |column: &Expr| {
        input_expr_column_index(column, schema)
            .and_then(|index| schema.bound_types.get(index).copied())
    };
    let resolve_placeholder = |_: usize| None;
    let context =
        crate::engine::expr_type::TypeContext::context_free(&resolve_column, &resolve_placeholder);
    crate::engine::expr_type::infer_expr_type(expr, &context)
}

/// Fold a deterministic text constant to the binary representation of the
/// temporal operand it is compared with.  The row evaluator performs this
/// coercion implicitly, but doing it there reparses the same literal for every
/// input row and prevents the typed batch comparison kernel from seeing a
/// matching `Date32`/`DateTime64` constant.
///
/// Invalid constants deliberately retain their original node: statement
/// validation and the legacy evaluator remain the diagnostic oracle.  Runtime
/// parameters and session-dependent expressions are not constant according to
/// `expr_is_constant`, so their value is never frozen into a cached program.
fn coerce_temporal_comparison_node(expr: &Expr, node: ExprNode, target: ColumnType) -> ExprNode {
    if !matches!(target, ColumnType::Date | ColumnType::DateTime)
        || !crate::engine::expr::expr_is_constant(expr)
    {
        return node;
    }
    // A NULL result may still carry diagnostics (for example 1 / 0).
    // Use the same isolated, warning-free evaluation as ordinary folding;
    // otherwise comparison binding would freeze NULL and lose runtime warnings.
    let Some(value) = try_eval_const_node(&node) else {
        return node;
    };
    if matches!(value, Value::Null) {
        return ExprNode::Const(Value::Null);
    }
    let is_text = matches!(
        value,
        Value::String(_) | Value::SmallString(_) | Value::Bytes(_) | Value::Decimal(_)
    );
    let coerced = match target {
        ColumnType::Date => crate::engine::expr::coerce_comparison_constant_for_type(
            &value,
            &crate::storage::DataTypeKind::Date,
        ),
        ColumnType::DateTime if is_text || matches!(value, Value::DateTime(_)) => {
            crate::engine::expr::coerce_comparison_constant_for_type(
                &value,
                &crate::storage::DataTypeKind::DateTime { fsp: 6 },
            )
        }
        // TIMESTAMP text coercion depends on the session time zone.  The
        // compact ExprProgram cache is structural and intentionally has no
        // session key, so keep that lane on the runtime oracle.
        ColumnType::Timestamp => None,
        _ => None,
    };
    coerced.map(ExprNode::Const).unwrap_or(node)
}

fn bound_result_coercion(
    expr: &Expr,
    schema: &InputSchema,
) -> Option<crate::engine::expr_type::ExprType> {
    // Bind the full result domain, including unsigned aggregation. Equal
    // branch descriptors do not guarantee equal runtime Value variants.
    let result_type = input_expr_type(expr, schema);
    matches!(
        result_type.column_type(),
        ColumnType::Decimal { .. }
            | ColumnType::Float
            | ColumnType::Double
            | ColumnType::Blob
            | ColumnType::Date
            | ColumnType::DateTime
            | ColumnType::Timestamp
    )
    .then_some(result_type)
}

/// Lower one `Expr` node to an `ExprNode`. When the structural lowering
/// can't handle a sub-expression (`Unsupported`), but the legacy
/// interpreter can evaluate it as a pure scalar, wrap it in a
/// `LegacyEscape` node instead of failing the whole compile — so the rest
/// of the expression keeps running on the compiled hot path. A genuinely
/// un-escapable shape (subquery, window/aggregate, grouping, …) still
/// returns `Unsupported`, which bubbles up to `CompiledTableExpr::Legacy`.
fn compile_node(expr: &Expr, schema: &InputSchema) -> Result<ExprNode, CompileError> {
    match compile_node_inner(expr, schema) {
        Ok(node) => Ok(node),
        Err(CompileError::Unsupported) => {
            if COMPILE_NO_ESCAPE.with(Cell::get) {
                UNESCAPABLE_REJECTIONS.fetch_add(1, AtomicOrdering::Relaxed);
                return Err(CompileError::Unsupported);
            }
            match try_escape_to_legacy(expr, schema) {
                Some(node) => {
                    LOCAL_ESCAPE_COMPILES.fetch_add(1, AtomicOrdering::Relaxed);
                    Ok(node)
                }
                None => {
                    UNESCAPABLE_REJECTIONS.fetch_add(1, AtomicOrdering::Relaxed);
                    Err(CompileError::Unsupported)
                }
            }
        }
        Err(error @ CompileError::TooDeep) => Err(error),
    }
}

/// Build a `LegacyEscape` node for `expr`, or `None` when `expr`'s shape
/// can't be evaluated row-wise by the legacy scalar interpreter (it needs
/// planner/aggregation context) or references a column that doesn't
/// resolve in `schema`.
fn try_escape_to_legacy(expr: &Expr, schema: &InputSchema) -> Option<ExprNode> {
    if !can_escape_shape(expr) {
        return None;
    }
    // Rewrite column leaves to unique synthetic names the synthetic table
    // resolves positionally; bail if any column doesn't resolve.
    let mut columns: Vec<usize> = Vec::new();
    let rewritten = rewrite_columns_for_escape(expr, schema, &mut columns)?;
    let synthetic = synthetic_escape_table(schema);
    Some(ExprNode::LegacyEscape {
        expr: Box::new(rewritten),
        schema: synthetic,
        columns,
        diagnostic_sql: crate::parser::visit::any_expr(expr, |node| {
            matches!(
                node,
                Expr::Binary {
                    op: BinaryOp::IntDiv,
                    ..
                }
            )
        })
        .then(|| crate::parser::expr_to_sql(expr)),
    })
}

/// Synthetic per-program table whose column `idx` is named `\0c{idx}`, so a
/// rewritten escaped expression resolves each leaf positionally with no
/// risk of name collision / ambiguity (the `\0` prefix can't appear in a
/// real SQL identifier). Preserve the compact source metadata needed by the
/// legacy semantic oracle: comparison collation and numeric signedness.
fn synthetic_escape_table(schema: &InputSchema) -> Arc<TableSchema> {
    // Every compile of a string comparison or legacy escape over the same
    // input layout needs the identical table, and building it (catalog types,
    // column index, row codec) costs far more than the compile itself. Reuse
    // it per thread, keyed by the schema fingerprint and confirmed against the
    // exact descriptors read below so a fingerprint collision cannot alias.
    const MAX_CACHED: usize = 256;
    thread_local! {
        static CACHE: RefCell<HashMap<u64, (Vec<SyntheticEscapeColumn>, Arc<TableSchema>)>> =
            RefCell::new(HashMap::new());
    }
    let key = synthetic_escape_columns(schema);
    let fingerprint = schema.fingerprint();
    if let Some(table) = CACHE.with(|cache| {
        cache
            .borrow()
            .get(&fingerprint)
            .filter(|(columns, _)| *columns == key)
            .map(|(_, table)| Arc::clone(table))
    }) {
        return table;
    }
    let table = build_synthetic_escape_table(schema);
    CACHE.with(|cache| {
        let mut cache = cache.borrow_mut();
        if cache.len() >= MAX_CACHED {
            cache.clear();
        }
        cache.insert(fingerprint, (key, Arc::clone(&table)));
    });
    table
}

/// The InputSchema attributes that determine a synthetic escape table.
#[derive(PartialEq, Eq)]
struct SyntheticEscapeColumn {
    bound_type: crate::engine::expr_type::ExprType,
    column_type: ColumnType,
    unsigned: bool,
    collation_id: Option<u16>,
    silent: bool,
    max_octets: Option<u32>,
}

fn synthetic_escape_columns(schema: &InputSchema) -> Vec<SyntheticEscapeColumn> {
    schema
        .columns
        .iter()
        .enumerate()
        .map(|(index, column)| SyntheticEscapeColumn {
            bound_type: schema.bound_types[index],
            column_type: column.column_type,
            unsigned: column.unsigned,
            collation_id: column.collation_id,
            silent: schema.bitwise_silent_string_columns.contains(&index),
            max_octets: schema.bound_types[index].max_octets,
        })
        .collect()
}

fn build_synthetic_escape_table(schema: &InputSchema) -> Arc<TableSchema> {
    let cols: Vec<crate::types::Column> = (0..schema.columns.len())
        .map(|idx| crate::types::Column {
            name: synthetic_escape_name(idx),
            column_type: schema.columns[idx].column_type.clone(),
            source: crate::types::ColumnSource {
                flags: if schema.columns[idx].unsigned {
                    crate::mysql::protocol::UNSIGNED_FLAG
                } else {
                    0
                },
                collation_id: schema.columns[idx].collation_id,
                vector_type: match schema.bound_types[idx].kind {
                    crate::engine::expr_type::ExprTypeKind::Vector { dim, datatype } => {
                        Some((datatype, dim))
                    }
                    _ => None,
                },
                ..Default::default()
            },
        })
        .collect();
    let mut table = TableSchema::synthetic_for_planning("\0escape", &cols);
    for (index, column) in table.columns.iter_mut().enumerate() {
        let silent = schema.bitwise_silent_string_columns.contains(&index);
        // `synthetic_for_planning` maps protocol VarString/Blob to their
        // unbounded TEXT/BLOB catalog forms. That default is too lossy for
        // the legacy oracle: VARCHAR and VARBINARY must retain conversion
        // warnings. Reconstruct the bounded family unless the input schema
        // explicitly marked a real TEXT/BLOB origin.
        if let crate::engine::expr_type::ExprTypeKind::Temporal { kind, fsp } =
            schema.bound_types[index].kind
        {
            use crate::engine::expr_type::TemporalKind;
            column.data_type = match kind {
                TemporalKind::Date => crate::storage::DataTypeKind::Date,
                TemporalKind::Time => crate::storage::DataTypeKind::Time { fsp },
                TemporalKind::DateTime => crate::storage::DataTypeKind::DateTime { fsp },
                TemporalKind::Timestamp => crate::storage::DataTypeKind::Timestamp { fsp },
            };
            continue;
        }
        column.data_type = match schema.columns[index].column_type {
            ColumnType::Blob if silent => crate::storage::DataTypeKind::Blob {
                max_len: crate::storage::schema::DEFAULT_LOB_MAX,
            },
            ColumnType::Blob => crate::storage::DataTypeKind::VarBinary {
                len: schema.bound_types[index]
                    .max_octets
                    .map(|length| length as usize),
            },
            ColumnType::VarString if silent => crate::storage::DataTypeKind::Text {
                max_len: crate::storage::schema::DEFAULT_LOB_MAX,
            },
            ColumnType::VarString => crate::storage::DataTypeKind::VarChar {
                len: schema.bound_types[index]
                    .max_octets
                    .map(|length| length as usize),
            },
            _ => continue,
        };
    }
    table.build_column_index();
    Arc::new(table)
}

fn aggregate_input_collation(
    args: &[Expr],
    schema: &InputSchema,
    operation: &str,
) -> Result<Option<u16>, CompileError> {
    let mut columns = Vec::new();
    let rewritten = args
        .iter()
        .map(|arg| {
            rewrite_columns_for_escape(arg, schema, &mut columns).ok_or(CompileError::Unsupported)
        })
        .collect::<Result<Vec<_>, _>>()?;
    let table = synthetic_escape_table(schema);
    crate::engine::expr::aggregate_expr_collations(rewritten.iter(), &table, operation)
        .map_err(CompileError::from)
}

fn binary_comparison_needs_charset_binding(
    args: &[Expr],
    schema: &InputSchema,
    collation: Option<u16>,
) -> bool {
    collation == Some(crate::charset::BINARY_COLLATION_ID)
        && args.iter().any(|expr| {
            input_expr_type(expr, schema)
                .collation_id
                .and_then(crate::charset::resolve_collation_id)
                .is_some_and(|info| !matches!(info.charset, "binary" | "utf8mb4" | "utf8mb3"))
        })
}

fn comparison_input_collation(
    args: &[Expr],
    schema: &InputSchema,
    operation: &str,
) -> Result<Option<u16>, CompileError> {
    if args.iter().all(|expr| {
        matches!(
            input_expr_result_type(expr, schema),
            ColumnType::Tiny
                | ColumnType::Short
                | ColumnType::MediumInt
                | ColumnType::Long
                | ColumnType::LongLong
                | ColumnType::Float
                | ColumnType::Double
                | ColumnType::Decimal { .. }
                | ColumnType::Date
                | ColumnType::DateTime
                | ColumnType::Timestamp
                | ColumnType::Time
                | ColumnType::Year
        )
    }) {
        return Ok(None);
    }
    aggregate_input_collation(args, schema, operation)
}

#[inline]
fn synthetic_escape_name(idx: usize) -> String {
    format!("\0c{idx}")
}

/// Returns `false` if `expr` (or any descendant) is a shape the legacy
/// scalar interpreter can't evaluate without planner/aggregation context.
/// These must keep failing to `Unsupported` so the whole expression falls
/// back wholesale rather than being mis-evaluated mid-tree.
fn can_escape_shape(expr: &Expr) -> bool {
    match expr {
        Expr::Subquery(_)
        | Expr::InSubquery { .. }
        | Expr::Exists { .. }
        | Expr::QuantifiedComparison { .. }
        | Expr::WindowFunction { .. }
        | Expr::GroupKeyRef { .. }
        | Expr::Grouping(_)
        | Expr::Default(_)
        | Expr::DefaultQualified(_)
        | Expr::Tuple(_)
        | Expr::Wildcard => false,
        Expr::Function {
            name,
            args,
            distinct,
        } => {
            // Aggregates / fulltext-`MATCH` aren't pure scalars; the legacy
            // path resolves them through planner / FulltextScan context.
            if *distinct
                || crate::planner::is_aggregate_function(name)
                || name == crate::parser::FT_MATCH_FUNCTION
                // CRC32 observes declared source representation (BIT raw
                // bytes and FLOAT/DOUBLE display scale). The compact program
                // schema intentionally omits those presentation attributes,
                // so retain the schema-aware legacy evaluator for this node.
                || name.eq_ignore_ascii_case("CRC32")
            {
                return false;
            }
            args.iter().all(can_escape_shape)
        }
        Expr::Identifier(_)
        | Expr::CompoundIdentifier(_)
        | Expr::Literal(_)
        | Expr::BoundValue(_)
        | Expr::Placeholder(_)
        | Expr::UserVariable(_)
        | Expr::SystemVariable { .. } => true,
        Expr::Unary { expr, .. } | Expr::IsNull(expr) | Expr::IsNotNull(expr) => {
            can_escape_shape(expr)
        }
        Expr::Binary { left, right, .. } => can_escape_shape(left) && can_escape_shape(right),
        Expr::InList { expr, list, .. } => {
            can_escape_shape(expr) && list.iter().all(can_escape_shape)
        }
        Expr::Between {
            expr, low, high, ..
        } => can_escape_shape(expr) && can_escape_shape(low) && can_escape_shape(high),
        Expr::Like { expr, pattern, .. } => can_escape_shape(expr) && can_escape_shape(pattern),
        Expr::Case {
            operand,
            conditions,
            results,
            else_result,
        } => {
            operand.as_deref().map(can_escape_shape).unwrap_or(true)
                && conditions.iter().all(can_escape_shape)
                && results.iter().all(can_escape_shape)
                && else_result.as_deref().map(can_escape_shape).unwrap_or(true)
        }
    }
}

/// Clone `expr`, replacing every column reference with a synthetic
/// `\0c{idx}` identifier resolved against `schema` (recording each `idx` in
/// `columns`). Returns `None` if any column fails to resolve — so the
/// escape is refused and the caller propagates `Unsupported` (matching the
/// pre-escape behaviour for unknown columns).
fn rewrite_columns_for_escape(
    expr: &Expr,
    schema: &InputSchema,
    columns: &mut Vec<usize>,
) -> Option<Expr> {
    crate::parser::visit::try_map_value_expr(expr, &mut |node| {
        let index = match node {
            Expr::Identifier(name) => schema.column_index(None, name).map_err(|_| ())?,
            Expr::CompoundIdentifier(parts) => {
                let column = parts.last().ok_or(())?;
                let qualifier = (parts.len() >= 2).then(|| parts[parts.len() - 2].as_str());
                schema.column_index(qualifier, column).map_err(|_| ())?
            }
            Expr::Literal(_)
            | Expr::BoundValue(_)
            | Expr::Placeholder(_)
            | Expr::UserVariable(_)
            | Expr::SystemVariable { .. }
            | Expr::Unary { .. }
            | Expr::Binary { .. }
            | Expr::IsNull(_)
            | Expr::IsNotNull(_)
            | Expr::InList { .. }
            | Expr::Between { .. }
            | Expr::Like { .. }
            | Expr::Case { .. }
            | Expr::Function { .. } => return Ok(None),
            // can_escape_shape rejects nodes requiring query/aggregate context.
            _ => return Err(()),
        };
        columns.push(index);
        Ok(Some(Expr::Identifier(synthetic_escape_name(index))))
    })
    .ok()
}

/// Static numeric classification of a comparison operand, used to decide
/// whether a bit/hex literal on the other side should be coerced to its
/// numeric value at compile time.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum NumericClass {
    /// Provably numeric (numeric literal or a numeric-typed column).
    Numeric,
    /// Provably non-numeric (a string/bytes-typed column or string literal).
    NonNumeric,
    /// Type not known at compile time — defer to the legacy evaluator.
    Unknown,
}

fn compiled_node_is_dynamic_string_source(expr: &Expr, node: &ExprNode) -> bool {
    match expr {
        Expr::Placeholder(_) | Expr::UserVariable(_) => true,
        // Auto-parameterized literals can arrive as BoundValue.  Once a
        // deterministic temporal/numeric value has been compiled to a typed
        // Const it no longer needs string-provenance coercion on every row.
        Expr::BoundValue(_) => matches!(
            node,
            ExprNode::Const(
                Value::String(_) | Value::SmallString(_) | Value::Bytes(_) | Value::Decimal(_)
            )
        ),
        _ => false,
    }
}

/// Classify a comparison operand's numeric-ness from the compile-time schema.
fn operand_numeric_class(expr: &Expr, schema: &InputSchema) -> NumericClass {
    match expr {
        Expr::Literal(
            Literal::Int(_) | Literal::UInt(_) | Literal::Float(_) | Literal::Bool(_),
        )
        | Expr::Literal(Literal::Decimal(_)) => NumericClass::Numeric,
        Expr::Literal(Literal::String(_)) => NumericClass::NonNumeric,
        Expr::Identifier(name) => match schema.column_index(None, name) {
            Ok(idx) => column_numeric_class(schema.columns[idx].column_type),
            Err(_) => NumericClass::Unknown,
        },
        Expr::CompoundIdentifier(parts) => {
            let Some(column) = parts.last() else {
                return NumericClass::Unknown;
            };
            let qualifier = (parts.len() >= 2).then(|| parts[parts.len() - 2].as_str());
            match schema.column_index(qualifier, column) {
                Ok(idx) => column_numeric_class(schema.columns[idx].column_type),
                Err(_) => NumericClass::Unknown,
            }
        }
        Expr::Function { name, .. } if name.eq_ignore_ascii_case("CONCAT") => {
            NumericClass::NonNumeric
        }
        Expr::Function { name, args, .. } if name.eq_ignore_ascii_case("COALESCE") => {
            let mut class = None;
            for arg in args {
                let next = operand_numeric_class(arg, schema);
                class = match (class, next) {
                    (None, next) => Some(next),
                    (Some(current), next) if current == next => Some(current),
                    _ => return NumericClass::Unknown,
                };
            }
            class.unwrap_or(NumericClass::Unknown)
        }
        _ => NumericClass::Unknown,
    }
}

fn operand_unsigned(expr: &Expr, schema: &InputSchema) -> bool {
    crate::engine::expr::expr_result_unsigned(expr, &|column_expr| {
        let index = match column_expr {
            Expr::Identifier(name) => schema.column_index(None, name).ok()?,
            Expr::CompoundIdentifier(parts) => {
                let column = parts.last()?;
                let qualifier = (parts.len() >= 2).then(|| parts[parts.len() - 2].as_str());
                schema.column_index(qualifier, column).ok()?
            }
            _ => return None,
        };
        schema.columns.get(index).map(|column| column.unsigned)
    })
}

fn operand_uses_silent_bitwise_string_conversion(expr: &Expr, schema: &InputSchema) -> bool {
    let index = match expr {
        Expr::Identifier(name) => schema.column_index(None, name).ok(),
        Expr::CompoundIdentifier(parts) => {
            let column = parts.last();
            let qualifier = (parts.len() >= 2).then(|| parts[parts.len() - 2].as_str());
            column.and_then(|column| schema.column_index(qualifier, column).ok())
        }
        _ => None,
    };
    index.is_some_and(|index| schema.bitwise_silent_string_columns.contains(&index))
}

fn column_numeric_class(ct: ColumnType) -> NumericClass {
    match ct {
        ColumnType::Tiny
        | ColumnType::Short
        | ColumnType::MediumInt
        | ColumnType::Long
        | ColumnType::LongLong
        | ColumnType::Float
        | ColumnType::Double
        | ColumnType::Decimal { .. }
        | ColumnType::Year
        | ColumnType::Bit { .. } => NumericClass::Numeric,
        _ => NumericClass::NonNumeric,
    }
}

/// Coerce a `Const(Value::Bytes)` node (a compiled bit/hex literal) to
/// `Const(Value::UInt)`; any other node is returned unchanged.
fn coerce_hexbit_const_node(node: ExprNode) -> ExprNode {
    match node {
        ExprNode::Const(value) => ExprNode::Const(coerce_hex_bytes_to_uint(value)),
        other => other,
    }
}

/// When `expr` is statically numeric, return a rewritten IN-list whose
/// bit/hex literal items are folded to numeric (`UInt`) literals; returns
/// `None` when no rewrite is needed so the caller keeps the borrowed list.
fn coerce_hexbit_in_list(expr: &Expr, list: &[Expr], schema: &InputSchema) -> Option<Vec<Expr>> {
    if operand_numeric_class(expr, schema) != NumericClass::Numeric {
        return None;
    }
    if !list.iter().any(expr_is_numeric_hexbit_literal) {
        return None;
    }
    Some(
        list.iter()
            .map(|item| {
                if expr_is_numeric_hexbit_literal(item) {
                    if let Expr::Literal(lit) = item {
                        if let Value::UInt(n) = coerce_hex_bytes_to_uint(literal_value(lit)) {
                            return Expr::Literal(Literal::UInt(n));
                        }
                    }
                }
                item.clone()
            })
            .collect(),
    )
}

fn compile_node_inner(expr: &Expr, schema: &InputSchema) -> Result<ExprNode, CompileError> {
    match expr {
        Expr::Wildcard => Ok(ExprNode::FieldCount),
        Expr::Identifier(name) => Ok(ExprNode::Column(schema.column_index(None, name)?)),
        Expr::CompoundIdentifier(parts) => {
            let column = parts.last().ok_or_else(|| {
                error(crate::error::catalog::ER_BAD_FIELD_ERROR, "unknown column")
            })?;
            let qualifier = if parts.len() >= 2 {
                Some(parts[parts.len() - 2].as_str())
            } else {
                None
            };
            Ok(ExprNode::Column(schema.column_index(qualifier, column)?))
        }
        Expr::Literal(lit) => Ok(ExprNode::Const(literal_value(lit))),
        Expr::BoundValue(value) => Ok(ExprNode::Const(value.clone())),
        Expr::Placeholder(idx) => Ok(ExprNode::Placeholder(*idx)),
        Expr::UserVariable(_) | Expr::SystemVariable { .. } => Err(CompileError::Unsupported),
        Expr::Default(_) | Expr::DefaultQualified(_) | Expr::Tuple(_) => {
            Err(CompileError::Unsupported)
        }
        Expr::Unary { op, expr } => Ok(ExprNode::Unary {
            op: *op,
            expr: Box::new(compile_node(expr, schema)?),
            enforce_unsigned_runtime_domain: matches!(op, UnaryOp::Neg)
                && !matches!(
                    expr.as_ref(),
                    Expr::Literal(Literal::UInt(_) | Literal::Int(_))
                ),
            suppress_string_truncation: operand_uses_silent_bitwise_string_conversion(expr, schema),
            duplicate_div_zero_warning: matches!(
                expr.as_ref(),
                Expr::Binary {
                    op: BinaryOp::IntDiv,
                    ..
                }
            ),
        }),
        Expr::Binary { left, op, right } => {
            // Character DIV carries its original SQL expression in overflow
            // diagnostics. Keep this rare conversion lane on the legacy
            // oracle; numeric DIV remains natively compiled.
            if *op == BinaryOp::IntDiv
                && matches!(
                    (left.as_ref(), right.as_ref()),
                    (Expr::Literal(Literal::String(_)), _)
                        | (_, Expr::Literal(Literal::String(_)))
                        | (
                            Expr::BoundValue(Value::String(_) | Value::SmallString(_)),
                            _
                        )
                        | (
                            _,
                            Expr::BoundValue(Value::String(_) | Value::SmallString(_))
                        )
                )
            {
                return Err(CompileError::Unsupported);
            }
            let is_cmp = crate::parser::boolean::is_comparison!(*op);
            if is_cmp {
                // The compact compiler evaluates literals before it knows
                // the comparison context, which would clamp an overwide
                // exact DECIMAL operand to the 65-digit result domain.
                // Preserve the legacy evaluator as the precision oracle.
                if crate::engine::expr::exact_decimal_comparison_literal(left)
                    .is_some_and(|value| matches!(value, Value::DecimalWide(_)))
                    || crate::engine::expr::exact_decimal_comparison_literal(right)
                        .is_some_and(|value| matches!(value, Value::DecimalWide(_)))
                {
                    return Err(CompileError::Unsupported);
                }
                // YEAR equality has MySQL's historical two-digit conversion
                // and assignment-style rounding. ExprNode::Binary carries
                // neither source type nor literal provenance, so preserve the
                // legacy evaluator as the semantic oracle for this rare lane.
                if matches!(input_expr_result_type(left, schema), ColumnType::Year)
                    || matches!(input_expr_result_type(right, schema), ColumnType::Year)
                {
                    return Err(CompileError::Unsupported);
                }
                let left_class = operand_numeric_class(left, schema);
                let right_class = operand_numeric_class(right, schema);
                if matches!(
                    (left_class, right_class),
                    (NumericClass::Numeric, NumericClass::NonNumeric)
                        | (NumericClass::NonNumeric, NumericClass::Numeric)
                ) {
                    if !COMPILE_CHECK_COMPARISONS.with(Cell::get) {
                        return Err(CompileError::Unsupported);
                    }
                    // Preserve MySQL coercion diagnostics in a supported oracle
                    // node. Validate both children first so strict schema validation
                    // still rejects unknown functions and session variables.
                    compile_node(left, schema)?;
                    compile_node(right, schema)?;
                    return try_escape_to_legacy(expr, schema).ok_or(CompileError::Unsupported);
                }
            }
            let coerces_hexbit_to_numeric = matches!(
                op,
                BinaryOp::Add
                    | BinaryOp::Sub
                    | BinaryOp::Mul
                    | BinaryOp::Div
                    | BinaryOp::IntDiv
                    | BinaryOp::Mod
                    | BinaryOp::ShiftLeft
                    | BinaryOp::ShiftRight
            );
            // Bit/hex literal numeric coercion (parity with
            // `eval_expr_legacy`). A `b'…'` / `0x…` literal compiles to a
            // `Const(Value::Bytes)`; without coercion the compiled comparison
            // would compare those raw bytes against a numeric column
            // (`bitcol = b'101'`) via the lenient string path and drop rows.
            // Arithmetic and shifts coerce the literal; &, | and ^ preserve
            // binary strings when both operands are binary and let the runtime
            // evaluator decide. Comparison coerces
            // only against a statically numeric operand (so `charcol = x'41'`
            // stays a byte-string compare) and bails to the legacy evaluator
            // when the other side's type isn't known at compile time.
            if is_cmp {
                if expr_is_numeric_hexbit_literal(left) {
                    match operand_numeric_class(right, schema) {
                        NumericClass::Numeric => {}
                        NumericClass::NonNumeric => {}
                        NumericClass::Unknown => return Err(CompileError::Unsupported),
                    }
                }
                if expr_is_numeric_hexbit_literal(right)
                    && operand_numeric_class(left, schema) == NumericClass::Unknown
                {
                    return Err(CompileError::Unsupported);
                }
            }
            let mut lnode = compile_node(left, schema)?;
            let mut rnode = compile_node(right, schema)?;
            if is_cmp {
                // Resolve deterministic temporal constants once per compiled
                // program.  This also exposes column/constant comparisons to
                // the native batch bitmap kernel instead of reparsing a text
                // date for every visible row.
                lnode = coerce_temporal_comparison_node(
                    left,
                    lnode,
                    input_expr_result_type(right, schema),
                );
                rnode = coerce_temporal_comparison_node(
                    right,
                    rnode,
                    input_expr_result_type(left, schema),
                );
            }
            if coerces_hexbit_to_numeric {
                if expr_is_numeric_hexbit_literal(left) {
                    lnode = coerce_hexbit_const_node(lnode);
                }
                if expr_is_numeric_hexbit_literal(right) {
                    rnode = coerce_hexbit_const_node(rnode);
                }
            } else if matches!(op, BinaryOp::BitAnd | BinaryOp::BitOr | BinaryOp::BitXor) {
                let left_literal = expr_is_numeric_hexbit_literal(left);
                let right_literal = expr_is_numeric_hexbit_literal(right);
                // A hex/bit literal paired with CHAR/TEXT/JSON is numeric,
                // but remains bytes for BINARY/VARBINARY/BLOB. The input
                // descriptor preserves that protocol-family distinction.
                if left_literal
                    && (right_literal
                        || operand_numeric_class(right, schema) == NumericClass::Numeric
                        || input_expr_result_type(right, schema) != ColumnType::Blob)
                {
                    lnode = coerce_hexbit_const_node(lnode);
                }
                if right_literal
                    && (left_literal
                        || operand_numeric_class(left, schema) == NumericClass::Numeric
                        || input_expr_result_type(left, schema) != ColumnType::Blob)
                {
                    rnode = coerce_hexbit_const_node(rnode);
                }
            } else if is_cmp {
                if expr_is_numeric_hexbit_literal(left)
                    && operand_numeric_class(right, schema) == NumericClass::Numeric
                {
                    lnode = coerce_hexbit_const_node(lnode);
                }
                if expr_is_numeric_hexbit_literal(right)
                    && operand_numeric_class(left, schema) == NumericClass::Numeric
                {
                    rnode = coerce_hexbit_const_node(rnode);
                }
            }
            // P0-1 — resolve the effective collation ID for string
            // comparisons at compile time so the runtime path can
            // dispatch to a collation-aware string cmp.
            let collation_id = if is_cmp {
                comparison_input_collation(
                    &[(**left).clone(), (**right).clone()],
                    schema,
                    "comparison",
                )?
            } else {
                None
            };
            if is_cmp
                && binary_comparison_needs_charset_binding(
                    &[(**left).clone(), (**right).clone()],
                    schema,
                    collation_id,
                )
            {
                return Err(CompileError::Unsupported);
            }
            let comparison = if is_cmp {
                ComparisonProvenance {
                    left_dynamic_source: compiled_node_is_dynamic_string_source(left, &lnode),
                    right_dynamic_source: compiled_node_is_dynamic_string_source(right, &rnode),
                    ..ComparisonProvenance::from_exprs(left, right)
                }
            } else {
                ComparisonProvenance::default()
            };
            let exact_prepared_arithmetic = !crate::engine::params::auto_parameterized_active()
                && matches!(
                    op,
                    BinaryOp::Add | BinaryOp::Sub | BinaryOp::Mul | BinaryOp::Div | BinaryOp::Mod
                );
            Ok(ExprNode::Binary {
                left: Box::new(lnode),
                op: *op,
                right: Box::new(rnode),
                semantics: BinarySemantics {
                    unsigned_subtraction: *op == BinaryOp::Sub
                        && (operand_unsigned(left, schema) || operand_unsigned(right, schema)),
                    collation_id,
                    suppress_left_string_truncation: operand_uses_silent_bitwise_string_conversion(
                        left, schema,
                    ),
                    suppress_right_string_truncation: operand_uses_silent_bitwise_string_conversion(
                        right, schema,
                    ),
                    comparison,
                    suppress_int_div_truncation: *op == BinaryOp::IntDiv
                        && (crate::engine::expr::expr_contains_user_variable_assignment(left)
                            || crate::engine::expr::expr_contains_user_variable_assignment(right)),
                    exact_left_numeric: exact_prepared_arithmetic
                        && matches!(left.as_ref(), Expr::Placeholder(_) | Expr::BoundValue(_)),
                    exact_right_numeric: exact_prepared_arithmetic
                        && matches!(right.as_ref(), Expr::Placeholder(_) | Expr::BoundValue(_)),
                },
            })
        }
        Expr::IsNull(expr) => Ok(ExprNode::IsNull(Box::new(compile_node(expr, schema)?))),
        Expr::IsNotNull(expr) => Ok(ExprNode::IsNotNull(Box::new(compile_node(expr, schema)?))),
        Expr::InList {
            expr,
            list,
            negated,
        } => {
            // Mixed/effective collation derivation is the legacy evaluator's
            // correctness oracle. Until the compiled node carries the full
            // N-ary descriptor, fall back whenever a non-default column
            // collation participates; this prevents the fast path from
            // silently comparing with the process default or bypassing
            // ER_CANT_AGGREGATE_*.
            let default_collation = crate::charset::default_collation_info().id;
            if aggregate_input_collation(
                &std::iter::once((**expr).clone())
                    .chain(list.iter().cloned())
                    .collect::<Vec<_>>(),
                schema,
                "in",
            )?
            .is_some_and(|id| id != default_collation)
                || std::iter::once(expr.as_ref())
                    .chain(list.iter())
                    .filter_map(|operand| schema.column_collation_info(operand))
                    .any(|info| info.id != default_collation)
            {
                return Err(CompileError::Unsupported);
            }
            // When the tested operand is statically numeric (e.g. a BIT
            // column), coerce any bit/hex literal list items to their numeric
            // value so `bitcol IN (b'101', b'110')` matches the stored UInt
            // instead of comparing raw bytes. Rewrites to numeric literals so
            // both the const-set fast path and the general path see them.
            let coerced_list = coerce_hexbit_in_list(expr, list, schema);
            let list = coerced_list.as_deref().unwrap_or(list.as_slice());
            let operands = std::iter::once(expr.as_ref())
                .chain(list.iter())
                .cloned()
                .collect::<Vec<_>>();
            let collation = comparison_input_collation(&operands, schema, "in")?;
            if binary_comparison_needs_charset_binding(&operands, schema, collation) {
                return Err(CompileError::Unsupported);
            }
            if let Some(node) = try_compile_in_const_set(expr, list, *negated, schema)? {
                return Ok(node);
            }
            Ok(ExprNode::InList {
                expr: Box::new(compile_node(expr, schema)?),
                list: list
                    .iter()
                    .map(|item| compile_node(item, schema))
                    .collect::<Result<Vec<_>, _>>()?,
                negated: *negated,
            })
        }
        Expr::Between {
            expr,
            low,
            high,
            negated,
        } => {
            let collation_id = comparison_input_collation(
                &[(**expr).clone(), (**low).clone(), (**high).clone()],
                schema,
                "between",
            )?;
            if binary_comparison_needs_charset_binding(
                &[(**expr).clone(), (**low).clone(), (**high).clone()],
                schema,
                collation_id,
            ) {
                return Err(CompileError::Unsupported);
            }
            Ok(ExprNode::Between {
                expr: Box::new(compile_node(expr, schema)?),
                low: Box::new(compile_node(low, schema)?),
                high: Box::new(compile_node(high, schema)?),
                negated: *negated,
                collation_id,
            })
        }
        Expr::Like {
            expr,
            pattern,
            negated,
            escape_char,
        } => {
            // If the pattern is a compile-time constant, pre-classify it so
            // the eval path skips per-row `chars().collect()` on both sides
            // (Literal / Prefix / Suffix / Contains / WildcardAny) or at
            // least on the pattern (General). Any constant value works —
            // a non-string literal (`x LIKE 1`) routes through the same
            // `value_to_string` coercion the legacy path uses.
            if schema
                .column_collation_info(expr)
                .is_some_and(|info| info.id == 63)
                || crate::engine::expr::expr_forces_binary_comparison(expr)
                || matches!(const_compile_value(expr), Some(Value::Bytes(_)))
                || matches!(const_compile_value(pattern), Some(Value::Bytes(_)))
            {
                return Err(CompileError::Unsupported);
            }
            if let Some(const_pat) = const_compile_value(pattern) {
                // `x LIKE NULL` (and NOT LIKE NULL) is NULL for every
                // row — same as `eval_like_values` on the legacy path.
                if matches!(const_pat, Value::Null) {
                    return Ok(ExprNode::Const(Value::Null));
                }
                let pat_str = crate::storage::value_to_string(&const_pat);
                // B1: bake the value operand's collation into the
                // pre-classified plan so `col LIKE 'foo%'` respects
                // `ColumnSchema.collation` and doesn't leak the process
                // default. `column_collation_info` returns None for
                // computed expressions; classify_like_plan then falls
                // back to the process default, matching legacy behaviour.
                let info = aggregate_input_collation(
                    &[(**expr).clone(), (**pattern).clone()],
                    schema,
                    "like",
                )?
                .and_then(crate::charset::resolve_collation_id);
                let plan = classify_like_plan(&pat_str, *escape_char, info);
                return Ok(ExprNode::LikeConst {
                    expr: Box::new(compile_node(expr, schema)?),
                    plan,
                    negated: *negated,
                });
            }
            // Non-constant pattern (`col LIKE ?`, `col LIKE lower(x)`).
            // Resolve the value operand's collation once here and stash
            // it on the Like node so `fold_constants` (which has no
            // schema) can still pre-classify a plan when the pattern
            // later folds to Const, and so the eval-time path picks up
            // the column collation without a second schema lookup.
            let collation_id = aggregate_input_collation(
                &[(**expr).clone(), (**pattern).clone()],
                schema,
                "like",
            )?;
            Ok(ExprNode::Like {
                expr: Box::new(compile_node(expr, schema)?),
                pattern: Box::new(compile_node(pattern, schema)?),
                negated: *negated,
                escape_char: *escape_char,
                collation_id,
            })
        }
        Expr::Case {
            operand,
            conditions,
            results,
            else_result,
        } => Ok(ExprNode::Case {
            operand: operand
                .as_ref()
                .map(|expr| compile_node(expr, schema).map(Box::new))
                .transpose()?,
            conditions: conditions
                .iter()
                .map(|expr| compile_node(expr, schema))
                .collect::<Result<Vec<_>, _>>()?,
            results: results
                .iter()
                .map(|expr| compile_node(expr, schema))
                .collect::<Result<Vec<_>, _>>()?,
            else_result: else_result
                .as_ref()
                .map(|expr| compile_node(expr, schema).map(Box::new))
                .transpose()?,
            result_coercion: (!COMPILE_DYNAMIC_CONDITIONAL.with(Cell::get))
                .then(|| bound_result_coercion(expr, schema))
                .flatten(),
        }),
        Expr::Function {
            name,
            args,
            distinct,
        } => {
            let result_coercion = match name.to_ascii_uppercase().as_str() {
                "ABS" | "CEIL" | "CEILING" | "FLOOR" | "IF" | "IFNULL" | "COALESCE" | "NULLIF"
                    if !COMPILE_DYNAMIC_CONDITIONAL.with(Cell::get) =>
                {
                    bound_result_coercion(expr, schema)
                }
                _ => None,
            };
            let collation_id = if name.eq_ignore_ascii_case("GREATEST") {
                aggregate_input_collation(args, schema, "greatest")?
            } else if name.eq_ignore_ascii_case("LEAST") {
                aggregate_input_collation(args, schema, "least")?
            } else if name.eq_ignore_ascii_case("STRCMP")
                || (name.eq_ignore_ascii_case("FIELD")
                    && crate::engine::expr::field_comparison_domain(
                        args.iter().map(|arg| input_expr_type(arg, schema)),
                    ) == crate::engine::expr::FieldComparisonDomain::Strings)
            {
                aggregate_input_collation(args, schema, "comparison")?
            } else if crate::engine::expr::is_regexp_function(name) {
                aggregate_input_collation(&args[..args.len().min(2)], schema, "regexp")?
            } else if name.eq_ignore_ascii_case("__DISTINCT_COLLATION_KEY") {
                args.first()
                    .and_then(|arg| match arg {
                        Expr::Function { name, .. }
                            if name.eq_ignore_ascii_case("COLLATE")
                                || name.eq_ignore_ascii_case("__COLLATED_CONST") =>
                        {
                            crate::engine::expr::expression_metadata_collation_id(arg)
                        }
                        _ => input_expr_type(arg, schema).collation_id,
                    })
                    .or_else(|| Some(crate::engine::expr::connection_collation_id()))
            } else {
                None
            };
            let cast_json_column = name.eq_ignore_ascii_case("CAST")
                && matches!(
                    args.get(1),
                    Some(Expr::Literal(Literal::String(target)))
                        if target.eq_ignore_ascii_case("JSON")
                )
                && args.first().is_some_and(|source| match source {
                    Expr::Identifier(column) => schema.column_index(None, column).is_ok(),
                    Expr::CompoundIdentifier(parts) => parts.last().is_some_and(|column| {
                        let qualifier = (parts.len() >= 2).then(|| parts[parts.len() - 2].as_str());
                        schema.column_index(qualifier, column).is_ok()
                    }),
                    _ => false,
                });
            if cast_json_column {
                return Err(CompileError::Unsupported);
            }
            // `__match_against` is not a real scalar function — it must be
            // evaluated by the legacy path, which reads the FulltextScan's
            // hidden score pseudo-column by fingerprint. Force a fallback so
            // the compiled path never tries to resolve it as a kernel.
            if *distinct
                || crate::planner::is_aggregate_function(name)
                || name == crate::parser::FT_MATCH_FUNCTION
                // `__set_user_var` writes a session user variable as a side
                // effect and must run row-by-row on the legacy path, never as
                // a compiled/constant-folded kernel (issue #86).
                || name == crate::parser::USER_VAR_ASSIGN_FUNCTION
                // AES consumes the source character set's octets. The
                // compiled value-only kernel does not carry per-operand
                // collation metadata, so a latin1 character column would be
                // re-encoded as UTF-8 and ciphertext would be corrupted.
                || name.eq_ignore_ascii_case("AES_ENCRYPT")
                || name.eq_ignore_ascii_case("AES_DECRYPT")
                // A direct BIT/FLOAT/DOUBLE column hashes its declared
                // representation; PI() has a special six-decimal rendering.
                // Retain the schema-aware oracle for those operands, while
                // ordinary values and composed string expressions compile.
                || (name.eq_ignore_ascii_case("CRC32") && args.first().is_some_and(|arg| {
                    (matches!(arg, Expr::Identifier(_) | Expr::CompoundIdentifier(_))
                        && matches!(input_expr_result_type(arg, schema),
                            ColumnType::Bit { .. } | ColumnType::Float | ColumnType::Double))
                        || matches!(arg, Expr::Function { name, args, .. }
                            if name.eq_ignore_ascii_case("PI") && args.is_empty())
                }))
                // BIT is numeric in arithmetic contexts but contributes its
                // fixed-width binary representation to CONCAT/CONCAT_WS. The
                // value-only compiled kernel sees only `Value::UInt` and
                // cannot recover BIT(M), so retain the typed legacy oracle.
                || ((name.eq_ignore_ascii_case("CONCAT")
                    || name.eq_ignore_ascii_case("CONCAT_WS"))
                    && args.iter().any(|arg| {
                        matches!(input_expr_result_type(arg, schema), ColumnType::Bit { .. })
                    }))
            {
                return Err(CompileError::Unsupported);
            }
            if crate::engine::expr::is_charset_octet_function(name)
                && args
                    .first()
                    .and_then(|a| input_expr_type(a, schema).collation_id)
                    .or_else(|| Some(crate::engine::expr::connection_collation_id()))
                    .and_then(crate::charset::resolve_collation_id)
                    .is_some_and(|c| {
                        !matches!(c.charset, "binary" | "utf8mb4" | "utf8mb3" | "utf8")
                    })
            {
                return Err(CompileError::Unsupported);
            }
            if let Some(node) = try_compile_substring_const(name, args, schema)? {
                return Ok(node);
            }
            // `CAST(<hex/bit literal> AS SIGNED|UNSIGNED)` must observe MySQL's
            // "hex literal in numeric context is a 64-bit big-endian integer"
            // rule. Compiling the literal to `Const(Value::Bytes(..))` would
            // reach the CAST kernel as a BoundValue, dropping the literal
            // provenance — the kernel then falls through the `Value::Bytes`
            // arm of `cast_to_integer`, parsing raw bytes as a decimal string
            // and returning 0. Fold the coerced integer at compile time.
            if name.eq_ignore_ascii_case("CAST") && args.len() >= 2 {
                if let (
                    Some(Expr::Literal(
                        crate::parser::Literal::HexBytes(b) | crate::parser::Literal::BitBytes(b),
                    )),
                    Some(Expr::Literal(crate::parser::Literal::String(target))),
                ) = (args.first(), args.get(1))
                {
                    match target.to_ascii_uppercase().as_str() {
                        "SIGNED" => {
                            return Ok(ExprNode::Const(
                                crate::engine::expr::hexbit_literal_cast_to_integer(b, false),
                            ));
                        }
                        "UNSIGNED" => {
                            return Ok(ExprNode::Const(
                                crate::engine::expr::hexbit_literal_cast_to_integer(b, true),
                            ));
                        }
                        _ => {}
                    }
                }
            }
            let Some(mut kernel) = FunctionRegistry::global().resolve(name) else {
                return Err(CompileError::Unsupported);
            };
            if let FunctionKernel::LegacyDeterministic(known_name) = kernel {
                if let Some(function) =
                    crate::engine::expr::math::MathFunction::from_name(known_name)
                {
                    kernel = FunctionKernel::Math(function);
                }
            }
            if matches!(kernel, FunctionKernel::Field(_)) {
                if crate::engine::expr::field_comparison_domain(
                    args.iter().map(|arg| input_expr_type(arg, schema)),
                ) == crate::engine::expr::FieldComparisonDomain::Dynamic
                    && args
                        .iter()
                        .any(crate::engine::expr::expr_is_numeric_hexbit_literal)
                {
                    return Err(CompileError::Unsupported);
                }
                kernel = FunctionKernel::Field(crate::engine::expr::field_comparison_domain(
                    args.iter().map(|arg| input_expr_type(arg, schema)),
                ));
            }
            Ok(ExprNode::Function {
                kernel,
                args: args
                    .iter()
                    .map(|arg| {
                        if matches!(
                            kernel,
                            FunctionKernel::Field(
                                crate::engine::expr::FieldComparisonDomain::Double
                                    | crate::engine::expr::FieldComparisonDomain::ExactNumeric
                            )
                        ) || matches!(kernel, FunctionKernel::Math(function) if function.numeric_hex_literals()) {
                            if let Expr::Literal(
                                Literal::HexBytes(bytes) | Literal::BitBytes(bytes),
                            ) = arg
                            {
                                return Ok(ExprNode::Const(
                                    crate::engine::expr::coerce_hex_bytes_to_uint(Value::Bytes(
                                        bytes.clone(),
                                    )),
                                ));
                            }
                        }
                        compile_node(arg, schema)
                    })
                    .collect::<Result<Vec<_>, _>>()?,
                collation_id,
                suppress_string_truncation: matches!(
                    name.to_ascii_uppercase().as_str(),
                    "ABS" | "CEIL" | "CEILING" | "FLOOR" | "ROUND" | "TRUNCATE"
                ) && args
                    .iter()
                    .any(|arg| operand_uses_silent_bitwise_string_conversion(arg, schema)),
                result_coercion,
            })
        }
        Expr::Subquery(_)
        | Expr::GroupKeyRef { .. }
        | Expr::Grouping(_)
        | Expr::QuantifiedComparison { .. }
        | Expr::InSubquery { .. }
        | Expr::Exists { .. }
        | Expr::WindowFunction { .. } => Err(CompileError::Unsupported),
    }
}

#[inline]
fn is_const_node(node: &ExprNode) -> bool {
    matches!(node, ExprNode::Const(_))
}

/// Evaluate a subtree against an empty row + fresh scratch. Returns the
/// constant value on success; `None` if the eval errors (column access,
/// unbound placeholder, divide-by-zero, SUBSTRING out of range, etc.).
///
/// On `None`, callers preserve the original node so the runtime evaluator
/// reproduces the error per row with MySQL semantics. Folding never
/// changes whether a query errors — only when the work happens.
fn try_eval_const_node(node: &ExprNode) -> Option<Value> {
    let empty: &[Value] = &[];
    let mut scratch = EvalScratch::default();
    // A cached folded constant cannot replay statement diagnostics. Evaluate
    // into an isolated sink and fold only when the subtree is both successful
    // and diagnostic-free; warning-producing constants stay executable so
    // every statement/cache hit observes the same warning contract.
    let diagnostics = std::sync::Arc::new(parking_lot::Mutex::new(Default::default()));
    let value = crate::engine::api::with_statement_warning_sink(
        std::sync::Arc::clone(&diagnostics),
        || node.eval(&empty, &mut scratch),
    )
    .ok()?;
    let is_warning_free = diagnostics.lock().is_empty();
    is_warning_free.then_some(value)
}

/// Self-bottom-up constant folding for the compiled expression tree.
///
/// After [`compile_node`] produces the program tree, this pass replaces
/// any subtree whose inputs are all `Const` and whose operator/kernel is
/// deterministic with a single `Const` carrying the precomputed value.
/// Common shapes that benefit: `WHERE c = 1 + 2` (Binary), `c BETWEEN 0
/// AND 100*1000` (Between), `c IN (1, 2+3, 4)` (InList/InConstSet),
/// `LENGTH('abc')+1` (Function), `CASE WHEN 1=1 THEN 'x' ELSE … END`.
///
/// Subtrees that contain a [`ExprNode::Column`] or [`ExprNode::Placeholder`]
/// fail the all-const child check and are left intact, so the cached
/// program remains correct across rows and across `EXECUTE` calls with
/// different parameter bindings.
///
/// Subtrees whose runtime eval errors (overflow, div-zero, JSON path
/// errors, …) preserve the original node — the runtime evaluator then
/// raises the MySQL-flavoured error per row, exactly as before.
///
/// Non-deterministic kernels (e.g. `NOW()`, `DATABASE()`) opt out via
/// [`FunctionKernel::is_deterministic`]; their constant-arg invocations
/// stay in the tree and are re-evaluated each row.
fn fold_constants(node: ExprNode) -> ExprNode {
    match node {
        leaf @ (ExprNode::Column(_)
        | ExprNode::FieldCount
        | ExprNode::Const(_)
        | ExprNode::Placeholder(_)
        | ExprNode::LegacyEscape { .. }) => leaf,

        ExprNode::Unary {
            op,
            expr,
            enforce_unsigned_runtime_domain,
            suppress_string_truncation,
            duplicate_div_zero_warning,
        } => {
            let expr = Box::new(fold_constants(*expr));
            let all_const = is_const_node(&expr);
            let node = ExprNode::Unary {
                op,
                expr,
                enforce_unsigned_runtime_domain,
                suppress_string_truncation,
                duplicate_div_zero_warning,
            };
            if all_const {
                if let Some(v) = try_eval_const_node(&node) {
                    return ExprNode::Const(v);
                }
            }
            node
        }

        ExprNode::Binary {
            left,
            op,
            right,
            semantics,
        } => {
            let left = Box::new(fold_constants(*left));
            let right = Box::new(fold_constants(*right));
            let all_const = is_const_node(&left) && is_const_node(&right);
            let node = ExprNode::Binary {
                left,
                op,
                right,
                semantics,
            };
            // Do not evaluate a literal zero divisor while compiling: DIV's
            // statement diagnostics must be published on every execution,
            // including cache hits.  It also preserves the child shape for
            // unary-minus' second divide-by-zero condition.
            if op == BinaryOp::IntDiv
                && matches!(
                    &node,
                    ExprNode::Binary {
                        right: value,
                        ..
                    } if matches!(
                        value.as_ref(),
                        ExprNode::Const(Value::Int(0))
                            | ExprNode::Const(Value::UInt(0))
                            | ExprNode::Const(Value::DecimalCompact(0, _))
                            | ExprNode::Const(Value::Decimal128(0, _))
                    )
                )
            {
                return node;
            }
            if all_const {
                if let Some(v) = try_eval_const_node(&node) {
                    return ExprNode::Const(v);
                }
            }
            node
        }

        ExprNode::IsNull(expr) => {
            let expr = Box::new(fold_constants(*expr));
            if is_const_node(&expr) {
                return ExprNode::Const(Value::Int(matches!(
                    expr.as_ref(),
                    ExprNode::Const(Value::Null)
                ) as i64));
            }
            ExprNode::IsNull(expr)
        }

        ExprNode::IsNotNull(expr) => {
            let expr = Box::new(fold_constants(*expr));
            if is_const_node(&expr) {
                return ExprNode::Const(Value::Int(
                    (!matches!(expr.as_ref(), ExprNode::Const(Value::Null))) as i64,
                ));
            }
            ExprNode::IsNotNull(expr)
        }

        ExprNode::InList {
            expr,
            list,
            negated,
        } => {
            let expr = Box::new(fold_constants(*expr));
            let list: Vec<ExprNode> = list.into_iter().map(fold_constants).collect();
            let all_const = is_const_node(&expr) && list.iter().all(is_const_node);
            let node = ExprNode::InList {
                expr,
                list,
                negated,
            };
            if all_const {
                if let Some(v) = try_eval_const_node(&node) {
                    return ExprNode::Const(v);
                }
            }
            node
        }

        ExprNode::InConstSet {
            expr,
            values,
            keys,
            hash_class,
            has_null,
            negated,
        } => {
            let expr = Box::new(fold_constants(*expr));
            let expr_const = is_const_node(&expr);
            let node = ExprNode::InConstSet {
                expr,
                values,
                keys,
                hash_class,
                has_null,
                negated,
            };
            if expr_const {
                if let Some(v) = try_eval_const_node(&node) {
                    return ExprNode::Const(v);
                }
            }
            node
        }

        ExprNode::SubstringConst { expr, start, len } => {
            let expr = Box::new(fold_constants(*expr));
            let expr_const = is_const_node(&expr);
            let node = ExprNode::SubstringConst { expr, start, len };
            if expr_const {
                if let Some(v) = try_eval_const_node(&node) {
                    return ExprNode::Const(v);
                }
            }
            node
        }

        ExprNode::Between {
            expr,
            low,
            high,
            negated,
            collation_id,
        } => {
            let expr = Box::new(fold_constants(*expr));
            let low = Box::new(fold_constants(*low));
            let high = Box::new(fold_constants(*high));
            let all_const = is_const_node(&expr) && is_const_node(&low) && is_const_node(&high);
            let node = ExprNode::Between {
                expr,
                low,
                high,
                negated,
                collation_id,
            };
            if all_const {
                if let Some(v) = try_eval_const_node(&node) {
                    return ExprNode::Const(v);
                }
            }
            node
        }

        ExprNode::Like {
            expr,
            pattern,
            negated,
            escape_char,
            collation_id,
        } => {
            let expr = Box::new(fold_constants(*expr));
            let pattern = Box::new(fold_constants(*pattern));
            // Both children folded to Const → evaluate the whole LIKE
            // once and replace with the boolean Const.
            if is_const_node(&expr) && is_const_node(&pattern) {
                let node = ExprNode::Like {
                    expr,
                    pattern,
                    negated,
                    escape_char,
                    collation_id,
                };
                if let Some(v) = try_eval_const_node(&node) {
                    return ExprNode::Const(v);
                }
                return node;
            }
            // Only the pattern folded to Const (e.g.
            // `c LIKE CONCAT('foo', '%')`) → promote to LikeConst so the
            // per-row hot path takes the byte-level fast path classified
            // once here instead of going through `match_like_pattern`'s
            // char NFA each row.
            if is_const_node(&pattern) {
                if let ExprNode::Const(pat_value) = *pattern {
                    // Folded-NULL pattern: the whole predicate is NULL.
                    if matches!(pat_value, Value::Null) {
                        return ExprNode::Const(Value::Null);
                    }
                    let pat_str = crate::storage::value_to_string(&pat_value);
                    // Recover the value operand's collation stashed at
                    // compile time so the late-fold plan agrees with
                    // the compile-time-const plan (B1).
                    let info = if matches!(pat_value, Value::Bytes(_)) {
                        crate::charset::resolve_collation_id(63)
                    } else {
                        collation_id.and_then(crate::charset::resolve_collation_id)
                    };
                    let plan = classify_like_plan(&pat_str, escape_char, info);
                    return ExprNode::LikeConst {
                        expr,
                        plan,
                        negated,
                    };
                }
                unreachable!("is_const_node returned true but pattern was not Const");
            }
            ExprNode::Like {
                expr,
                pattern,
                negated,
                escape_char,
                collation_id,
            }
        }

        ExprNode::LikeConst {
            expr,
            plan,
            negated,
        } => {
            let expr = Box::new(fold_constants(*expr));
            let expr_const = is_const_node(&expr);
            let node = ExprNode::LikeConst {
                expr,
                plan,
                negated,
            };
            if expr_const {
                if let Some(v) = try_eval_const_node(&node) {
                    return ExprNode::Const(v);
                }
            }
            node
        }

        ExprNode::Case {
            operand,
            conditions,
            results,
            else_result,
            result_coercion,
        } => {
            let operand = operand.map(|o| Box::new(fold_constants(*o)));
            let conditions: Vec<ExprNode> = conditions.into_iter().map(fold_constants).collect();
            let results: Vec<ExprNode> = results.into_iter().map(fold_constants).collect();
            let else_result = else_result.map(|e| Box::new(fold_constants(*e)));
            let all_const = operand.as_ref().map_or(true, |o| is_const_node(o))
                && conditions.iter().all(is_const_node)
                && results.iter().all(is_const_node)
                && else_result.as_ref().map_or(true, |e| is_const_node(e));
            let node = ExprNode::Case {
                operand,
                conditions,
                results,
                else_result,
                result_coercion,
            };
            if all_const {
                if let Some(v) = try_eval_const_node(&node) {
                    return ExprNode::Const(v);
                }
            }
            node
        }

        ExprNode::Function {
            kernel,
            args,
            collation_id,
            suppress_string_truncation,
            result_coercion,
        } => {
            let args: Vec<ExprNode> = args.into_iter().map(fold_constants).collect();
            let all_const = args.iter().all(is_const_node);
            let det = kernel.is_deterministic();
            let node = ExprNode::Function {
                kernel,
                args,
                collation_id,
                suppress_string_truncation,
                result_coercion,
            };
            if det && all_const {
                if let Some(v) = try_eval_const_node(&node) {
                    return ExprNode::Const(v);
                }
            }
            node
        }
    }
}

/// Borrowed-or-owned result of evaluating a node for a *read-only*
/// consumer (comparison, truthiness, NULL test, set probe). `Column` /
/// `Const` leaves borrow — eliminating the per-row deep clone they pay
/// through `eval` — while composite nodes still materialise an owned
/// `Value`. The single lifetime unifies `&self` (Const) with the row
/// borrow (Column).
enum ValueRef<'a> {
    Borrowed(&'a Value),
    Owned(Value),
}

impl ValueRef<'_> {
    #[inline]
    fn as_value(&self) -> &Value {
        match self {
            ValueRef::Borrowed(v) => v,
            ValueRef::Owned(v) => v,
        }
    }

    #[inline]
    fn into_owned(self) -> Value {
        match self {
            ValueRef::Borrowed(v) => v.clone(),
            ValueRef::Owned(v) => v,
        }
    }

    #[inline]
    fn is_null(&self) -> bool {
        matches!(self.as_value(), Value::Null)
    }
}

impl ExprNode {
    fn try_eval_simple_predicate(
        &self,
        row: &dyn RowSource,
    ) -> Option<Result<Option<bool>, QueryError>> {
        let ExprNode::Binary {
            left,
            op,
            right,
            semantics,
            ..
        } = self
        else {
            return None;
        };
        if matches!(op, BinaryOp::And | BinaryOp::Or) {
            let left_value = match left.try_eval_simple_predicate(row)? {
                Ok(value) => value,
                Err(err) => return Some(Err(err)),
            };
            if (*op == BinaryOp::And && left_value == Some(false))
                || (*op == BinaryOp::Or && left_value == Some(true))
            {
                return Some(Ok(left_value));
            }
            let right_value = match right.try_eval_simple_predicate(row)? {
                Ok(value) => value,
                Err(err) => return Some(Err(err)),
            };
            return Some(Ok(crate::engine::expr::logical_value(
                &crate::engine::expr::combine_logical(*op, left_value, right_value),
            )));
        }
        if !crate::parser::boolean::is_comparison!(*op) {
            return None;
        }
        fn leaf_value<'a>(node: &'a ExprNode, row: &'a dyn RowSource) -> Option<&'a Value> {
            match node {
                ExprNode::Column(idx) => row.value_ref(*idx),
                ExprNode::Const(value) => Some(value),
                _ => None,
            }
        }
        let left_value = leaf_value(left, row)?;
        let right_value = leaf_value(right, row)?;
        let result = semantics.compare(left_value, *op, right_value);
        Some(result.map(|value| crate::engine::expr::logical_value(&value)))
    }

    /// Evaluate for a read-only consumer: leaves borrow, composites fall
    /// back to `eval`. Callers that need to keep the result use
    /// `ValueRef::into_owned`, paying the clone only on that path.
    #[inline]
    fn eval_ref<'a>(
        &'a self,
        row: &'a dyn RowSource,
        scratch: &mut EvalScratch,
    ) -> Result<ValueRef<'a>, QueryError> {
        match self {
            ExprNode::Column(idx) => row.value_ref(*idx).map(ValueRef::Borrowed).ok_or_else(|| {
                error(
                    crate::error::catalog::ER_BAD_FIELD_ERROR,
                    "row value is missing for compiled expression",
                )
            }),
            ExprNode::Const(value) => Ok(ValueRef::Borrowed(value)),
            other => other.eval(row, scratch).map(ValueRef::Owned),
        }
    }

    fn eval(&self, row: &dyn RowSource, scratch: &mut EvalScratch) -> Result<Value, QueryError> {
        match self {
            ExprNode::Column(idx) => row.value_ref(*idx).cloned().ok_or_else(|| {
                error(
                    crate::error::catalog::ER_BAD_FIELD_ERROR,
                    "row value is missing for compiled expression",
                )
            }),
            ExprNode::FieldCount => Ok(Value::Int(row.len() as i64)),
            ExprNode::Const(value) => Ok(value.clone()),
            ExprNode::Placeholder(idx) => {
                crate::engine::params::current_param(*idx).ok_or_else(|| {
                    error(
                        crate::error::catalog::ER_WRONG_ARGUMENTS,
                        "parameter is not bound",
                    )
                })
            }
            ExprNode::Unary {
                op,
                expr,
                enforce_unsigned_runtime_domain,
                suppress_string_truncation,
                duplicate_div_zero_warning,
            } => match op {
                UnaryOp::Neg => {
                    let value = expr.eval(row, scratch)?;
                    if *duplicate_div_zero_warning && matches!(value, Value::Null) {
                        crate::engine::api::push_statement_warning(
                            crate::error::ConditionLevel::Warning,
                            crate::error::catalog::ER_DIVISION_BY_ZERO,
                            "Division by 0",
                        );
                    }
                    let evaluate = || {
                        crate::engine::expr::eval_negate_typed(
                            value,
                            *enforce_unsigned_runtime_domain,
                        )
                    };
                    if *suppress_string_truncation {
                        crate::engine::expr::with_suppressed_expression_warnings(evaluate)
                    } else {
                        evaluate()
                    }
                }
                UnaryOp::Not => Ok(crate::engine::expr::eval_logical_not(
                    expr.eval_ref(row, scratch)?.as_value(),
                )),
                UnaryOp::BitNot => crate::engine::expr::eval_bit_not(expr.eval(row, scratch)?),
            },
            ExprNode::Binary {
                left,
                op,
                right,
                semantics,
            } => eval_binary_node(left, *op, right, semantics, row, scratch),
            ExprNode::IsNull(expr) => Ok(Value::Int(expr.eval_ref(row, scratch)?.is_null() as i64)),
            ExprNode::IsNotNull(expr) => {
                Ok(Value::Int((!expr.eval_ref(row, scratch)?.is_null()) as i64))
            }
            ExprNode::InList {
                expr,
                list,
                negated,
            } => eval_in_list_node(expr, list, *negated, row, scratch),
            ExprNode::InConstSet {
                expr,
                values,
                keys,
                hash_class,
                has_null,
                negated,
            } => eval_in_const_set_node(
                expr,
                values,
                keys,
                *hash_class,
                *has_null,
                *negated,
                row,
                scratch,
            ),
            ExprNode::Between {
                expr,
                low,
                high,
                negated,
                collation_id,
            } => eval_between_node(expr, low, high, *negated, *collation_id, row, scratch),
            ExprNode::Like {
                expr,
                pattern,
                negated,
                escape_char,
                collation_id,
            } => {
                // Read-only operands: borrow so a `String`/`Bytes` column
                // isn't cloned per row on this common filter shape.
                let value = expr.eval_ref(row, scratch)?;
                let pat = pattern.eval_ref(row, scratch)?;
                if value.is_null() || pat.is_null() {
                    return Ok(Value::Null);
                }
                // B9: when the pattern is a plain string (typical for
                // `col LIKE ?` prepared / auto-param plans), route
                // through the per-thread LikePlanCache so the classify
                // happens once per unique (pattern, escape, collation)
                // instead of once per row.
                let pat_val = pat.as_value();
                let use_cache = matches!(pat_val, Value::String(_));
                if use_cache {
                    let pat_str = crate::engine::expr::str_value(pat_val);
                    let plan = crate::engine::func_cache::cached_like_plan(
                        &pat_str,
                        *escape_char,
                        *collation_id,
                    );
                    let matched = plan.matches_value(value.as_value());
                    return Ok(Value::Int(
                        (if *negated { !matched } else { matched }) as i64,
                    ));
                }
                // B1: forward the compile-time-resolved column collation
                // so `col LIKE ?` case-folds the same way `col = ?` does.
                let info = collation_id.and_then(crate::charset::resolve_collation_id);
                Ok(crate::engine::expr::eval_like_values_with_collation(
                    value.as_value(),
                    pat_val,
                    *negated,
                    *escape_char,
                    info,
                ))
            }
            ExprNode::LikeConst {
                expr,
                plan,
                negated,
            } => {
                let value = expr.eval_ref(row, scratch)?;
                if value.is_null() {
                    return Ok(Value::Null);
                }
                let matched = plan.matches_value(value.as_value());
                Ok(Value::Int(
                    (if *negated { !matched } else { matched }) as i64,
                ))
            }
            ExprNode::Case {
                operand,
                conditions,
                results,
                else_result,
                result_coercion,
            } => {
                let value =
                    eval_case_node(operand, conditions, results, else_result, row, scratch)?;
                match result_coercion {
                    Some(result_type) => {
                        crate::engine::expr::coerce_result_to_expr_type(value, *result_type)
                    }
                    None => Ok(value),
                }
            }
            ExprNode::SubstringConst { expr, start, len } => {
                let input_v = expr.eval_ref(row, scratch)?;
                if input_v.is_null() {
                    return Ok(Value::Null);
                }
                if let Value::Bytes(bytes) = input_v.as_value() {
                    return Ok(Value::Bytes(crate::engine::expr::mysql_substring_bytes(
                        bytes, *start, *len,
                    )));
                }
                let input = crate::engine::expr::str_value(input_v.as_value());
                Ok(Value::String(mysql_substring(&input, *start, *len)))
            }
            ExprNode::Function {
                kernel,
                args,
                collation_id,
                suppress_string_truncation,
                result_coercion,
            } => {
                let mut evaluate = || {
                    crate::engine::expr::with_compare_collation(*collation_id, || {
                        kernel.eval(args, row, scratch)
                    })
                };
                let value = if *suppress_string_truncation {
                    crate::engine::expr::with_suppressed_expression_warnings(evaluate)
                } else {
                    evaluate()
                }?;
                match result_coercion {
                    Some(result_type) => {
                        crate::engine::expr::coerce_result_to_expr_type(value, *result_type)
                    }
                    None => Ok(value),
                }
            }
            ExprNode::LegacyEscape {
                expr,
                schema,
                diagnostic_sql,
                ..
            } => {
                record_legacy_escape_row();
                // Prefer a zero-copy slice; composite row sources (joins)
                // materialise once into an owned row.
                match row.as_value_slice() {
                    Some(slice) => eval_expr_legacy(expr, schema, slice),
                    None => {
                        let owned = row.collect_values();
                        eval_expr_legacy(expr, schema, &owned)
                    }
                }
                .map_err(|err| {
                    if let Some(sql) = diagnostic_sql
                        .as_ref()
                        .filter(|_| err.is(crate::error::catalog::ER_DATA_OUT_OF_RANGE))
                    {
                        err.map_message(|message| {
                            format!(
                                "{} in '{}'",
                                message.split(" in '").next().unwrap_or(message),
                                sql
                            )
                        })
                    } else {
                        err
                    }
                })
            }
        }
    }
}

fn eval_binary_node(
    left: &ExprNode,
    op: BinaryOp,
    right: &ExprNode,
    semantics: &BinarySemantics,
    row: &dyn RowSource,
    scratch: &mut EvalScratch,
) -> Result<Value, QueryError> {
    if matches!(op, BinaryOp::And | BinaryOp::Or) {
        let l = crate::engine::expr::logical_value(left.eval_ref(row, scratch)?.as_value());
        if op == BinaryOp::And && l == Some(false) {
            return Ok(Value::Int(0));
        }
        if op == BinaryOp::Or && l == Some(true) {
            return Ok(Value::Int(1));
        }
        let r = crate::engine::expr::logical_value(right.eval_ref(row, scratch)?.as_value());
        return Ok(crate::engine::expr::combine_logical(op, l, r));
    }
    match op {
        // Read-only consumers: comparison / XOR / arithmetic helpers all
        // take `&Value`, so column and constant operands borrow instead
        // of deep-cloning per row.
        BinaryOp::Xor
        | BinaryOp::Eq
        | BinaryOp::NullSafeEq
        | BinaryOp::NotEq
        | BinaryOp::Lt
        | BinaryOp::LtEq
        | BinaryOp::Gt
        | BinaryOp::GtEq
        | BinaryOp::Add
        | BinaryOp::Sub
        | BinaryOp::Mul
        | BinaryOp::Div
        | BinaryOp::IntDiv
        | BinaryOp::Mod => {
            let l = left.eval_ref(row, scratch)?;
            let r = right.eval_ref(row, scratch)?;
            match op {
                BinaryOp::Xor => crate::engine::expr::eval_logical_xor(l.as_value(), r.as_value()),
                BinaryOp::Add | BinaryOp::Sub | BinaryOp::Mul | BinaryOp::Div | BinaryOp::Mod => {
                    let left_numeric = (semantics.exact_left_numeric)
                        .then(|| crate::engine::expr::exact_numeric_string_operand(l.as_value()))
                        .flatten();
                    let right_numeric = (semantics.exact_right_numeric)
                        .then(|| crate::engine::expr::exact_numeric_string_operand(r.as_value()))
                        .flatten();
                    let evaluate = || {
                        eval_arithmetic(
                            left_numeric.as_ref().unwrap_or(l.as_value()),
                            op,
                            right_numeric.as_ref().unwrap_or(r.as_value()),
                        )
                    };
                    let value = if semantics.suppress_left_string_truncation
                        || semantics.suppress_right_string_truncation
                    {
                        crate::engine::expr::with_suppressed_expression_warnings(evaluate)?
                    } else {
                        evaluate()?
                    };
                    crate::engine::expr::enforce_unsigned_subtraction_result(
                        value,
                        semantics.unsigned_subtraction,
                    )
                }
                BinaryOp::IntDiv => {
                    let evaluate =
                        || crate::engine::expr::eval_int_div_values(l.as_value(), r.as_value());
                    if semantics.suppress_int_div_truncation {
                        crate::engine::expr::with_suppressed_expression_warning_code(
                            crate::error::catalog::ER_TRUNCATED_WRONG_VALUE.code().get(),
                            evaluate,
                        )
                    } else {
                        evaluate()
                    }
                }
                _ => semantics.compare(l.as_value(), op, r.as_value()),
            }
        }
        BinaryOp::BitAnd
        | BinaryOp::BitOr
        | BinaryOp::BitXor
        | BinaryOp::ShiftLeft
        | BinaryOp::ShiftRight => crate::engine::expr::eval_bitwise_with_string_modes(
            left.eval(row, scratch)?,
            op,
            right.eval(row, scratch)?,
            semantics.suppress_left_string_truncation,
            semantics.suppress_right_string_truncation,
        ),
        BinaryOp::Arrow | BinaryOp::LongArrow => eval_json_arrow(
            left.eval(row, scratch)?,
            right.eval(row, scratch)?,
            op == BinaryOp::LongArrow,
        ),
        BinaryOp::Concat => crate::engine::expr::binary_string_concat(
            left.eval(row, scratch)?,
            right.eval(row, scratch)?,
        ),
        BinaryOp::And | BinaryOp::Or => unreachable!(),
    }
}

fn eval_in_list_node(
    expr: &ExprNode,
    list: &[ExprNode],
    negated: bool,
    row: &dyn RowSource,
    scratch: &mut EvalScratch,
) -> Result<Value, QueryError> {
    let value = expr.eval(row, scratch)?;
    let mut iter = list.iter();
    let eval = || {
        crate::engine::expr::eval_in_list(value, negated, || match iter.next() {
            Some(item) => Ok(Some(item.eval(row, scratch)?)),
            None => Ok(None),
        })
    };
    if list.iter().all(is_const_node) {
        crate::engine::expr::with_expression_warning_dedup(list.as_ptr() as usize as u64, eval)
    } else {
        eval()
    }
}

fn eval_in_const_set_node(
    expr: &ExprNode,
    values: &[Value],
    keys: &HashSet<ValueKey>,
    hash_class: Option<InHashClass>,
    has_null: bool,
    negated: bool,
    row: &dyn RowSource,
    scratch: &mut EvalScratch,
) -> Result<Value, QueryError> {
    let value = expr.eval_ref(row, scratch)?;
    if values.is_empty() {
        return Ok(Value::Int(negated as i64));
    }
    if value.is_null() {
        return Ok(Value::Null);
    }
    if hash_class.is_some() && in_hash_class(value.as_value()) == hash_class {
        let found = keys.contains(&ValueKey::from(value.as_value()));
        if !found && has_null {
            return Ok(Value::Null);
        }
        return Ok(Value::Int((if negated { !found } else { found }) as i64));
    }
    // InConstSet replaces the source Expr list, but conversion diagnostics
    // still have statement/expression scope.  Install a stable context so
    // all constants are validated after a match and cloned compiled
    // programs do not duplicate the same warning.
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    b"IN_CONST_SET".hash(&mut hasher);
    values.len().hash(&mut hasher);
    for item in values {
        ValueKey::from(item).hash(&mut hasher);
    }
    let all_string = values.iter().all(|item| {
        matches!(
            item,
            Value::String(_) | Value::SmallString(_) | Value::Bytes(_)
        )
    });
    crate::engine::expr::with_in_validate_all_constants(all_string, || {
        crate::engine::expr::with_expression_warning_dedup(hasher.finish(), || {
            eval_in_precomputed_values(value.into_owned(), values, negated)
        })
    })
}

fn eval_between_node(
    expr: &ExprNode,
    low: &ExprNode,
    high: &ExprNode,
    negated: bool,
    collation_id: Option<u16>,
    row: &dyn RowSource,
    scratch: &mut EvalScratch,
) -> Result<Value, QueryError> {
    let value = expr.eval_ref(row, scratch)?;
    let low_val = low.eval_ref(row, scratch)?;
    let high_val = high.eval_ref(row, scratch)?;
    crate::engine::expr::with_compare_collation(collation_id, || {
        crate::engine::expr::eval_between(
            value.as_value(),
            low_val.as_value(),
            high_val.as_value(),
            negated,
        )
    })
}

fn eval_case_node(
    operand: &Option<Box<ExprNode>>,
    conditions: &[ExprNode],
    results: &[ExprNode],
    else_result: &Option<Box<ExprNode>>,
    row: &dyn RowSource,
    scratch: &mut EvalScratch,
) -> Result<Value, QueryError> {
    if let Some(operand) = operand {
        let op_val = operand.eval_ref(row, scratch)?;
        for (cond, result) in conditions.iter().zip(results.iter()) {
            if crate::engine::expr::compare_case_values(
                op_val.as_value(),
                cond.eval_ref(row, scratch)?.as_value(),
            )? == Ordering::Equal
            {
                return result.eval(row, scratch);
            }
        }
    } else {
        for (cond, result) in conditions.iter().zip(results.iter()) {
            if truthy(cond.eval_ref(row, scratch)?.as_value()) {
                return result.eval(row, scratch);
            }
        }
    }
    if let Some(else_result) = else_result {
        else_result.eval(row, scratch)
    } else {
        Ok(Value::Null)
    }
}

crate::engine::builtin_catalog::declare_function_kernel!();

#[cfg(test)]
pub(crate) fn catalog_kernel_deterministic(name: &str) -> bool {
    FunctionRegistry::global()
        .resolve(name)
        .unwrap()
        .is_deterministic()
}

#[derive(Default)]
pub(crate) struct FunctionRegistry {
    kernels: HashMap<&'static str, FunctionKernel>,
}

impl FunctionRegistry {
    pub(crate) fn global() -> &'static Self {
        static REGISTRY: std::sync::OnceLock<FunctionRegistry> = std::sync::OnceLock::new();
        REGISTRY.get_or_init(FunctionRegistry::build)
    }

    fn build() -> Self {
        let mut registry = FunctionRegistry::default();
        for &(name, kernel) in native_kernels() {
            registry.add(&[name], kernel);
        }
        // Plain-value delegation keeps the surrounding compiled tree intact.
        // Session-dependent delegation remains ineligible for cached folding.
        for (name, kernel) in delegated_kernels() {
            registry.add(&[name], kernel);
        }
        registry
    }

    fn add(&mut self, names: &[&'static str], kernel: FunctionKernel) {
        for name in names {
            let previous = self.kernels.insert(*name, kernel);
            debug_assert!(
                previous.is_none(),
                "duplicate expression builtin registration: {name}"
            );
        }
    }

    fn resolve(&self, name: &str) -> Option<FunctionKernel> {
        let name = crate::engine::sys_schema::NormalizedFuncName::uppercase(name);
        self.kernels.get(name.as_str()).copied()
    }
}

impl FunctionKernel {
    fn eval(
        self,
        args: &[ExprNode],
        row: &dyn RowSource,
        scratch: &mut EvalScratch,
    ) -> Result<Value, QueryError> {
        match self {
            FunctionKernel::Math(function) => {
                function.check_arity(function.name(), args)?;
                let mut values = smallvec::SmallVec::<[Value; 2]>::new();
                for arg in args {
                    values.push(arg.eval(row, scratch)?);
                }
                function.evaluate(&values)
            }
            FunctionKernel::Field(domain) => {
                let values = args
                    .iter()
                    .map(|arg| arg.eval(row, scratch))
                    .collect::<Result<Vec<_>, _>>()?;
                crate::engine::expr::eval_field_values(&values, domain)
            }
            FunctionKernel::If => {
                expect_len("IF", args, 3)?;
                let condition = args[0].eval_ref(row, scratch)?;
                let condition = if matches!(
                    condition.as_value(),
                    Value::String(_) | Value::SmallString(_) | Value::Bytes(_)
                ) {
                    crate::engine::expr::numeric(condition.as_value())? != 0.0
                } else {
                    truthy(condition.as_value())
                };
                if condition {
                    args[1].eval(row, scratch)
                } else {
                    args[2].eval(row, scratch)
                }
            }
            FunctionKernel::IfNull => {
                expect_len("IFNULL", args, 2)?;
                let first = args[0].eval(row, scratch)?;
                if matches!(first, Value::Null) {
                    args[1].eval(row, scratch)
                } else {
                    Ok(first)
                }
            }
            FunctionKernel::Coalesce => {
                if args.is_empty() {
                    return Err(error(
                        crate::error::catalog::ER_WRONG_PARAMCOUNT_TO_NATIVE_FCT,
                        "Incorrect parameter count in the call to native function 'COALESCE'",
                    ));
                }
                for arg in args {
                    let value = arg.eval(row, scratch)?;
                    if !matches!(value, Value::Null) {
                        return Ok(value);
                    }
                }
                Ok(Value::Null)
            }
            FunctionKernel::NullIf => {
                expect_len("NULLIF", args, 2)?;
                let first_for_comparison = args[0].eval(row, scratch)?;
                let second = args[1].eval_ref(row, scratch)?;
                let equal = if crate::engine::params::has_active_params()
                    && matches!(args[0], ExprNode::Placeholder(_))
                {
                    crate::charset::default_compare_strings(
                        &crate::engine::expr::string_value(&first_for_comparison),
                        &crate::engine::expr::string_value(second.as_value()),
                    ) == Ordering::Equal
                } else {
                    compare_values(&first_for_comparison, second.as_value())? == Ordering::Equal
                };
                if equal {
                    Ok(Value::Null)
                } else {
                    args[0].eval(row, scratch)
                }
            }
            FunctionKernel::Length => {
                expect_len("LENGTH", args, 1)?;
                // Value stores temporal microseconds but not the declared
                // fractional precision. Compact display intentionally trims
                // trailing zeroes, while MySQL's LENGTH(NOW(fsp)) and
                // LENGTH(CURTIME(fsp)) observe the fixed declared width.
                // Preserve that expression-level fact here so results do not
                // depend on whether the sampled clock happens to end in zero.
                if let Some(len) = fixed_current_temporal_display_len(&args[0]) {
                    return Ok(Value::Int(len as i64));
                }
                let value = args[0].eval(row, scratch)?;
                null_or(value, |value| {
                    let len = match &value {
                        Value::Bytes(bytes) => bytes.len(),
                        _ => crate::engine::expr::str_value(&value).len(),
                    };
                    Ok(Value::Int(len as i64))
                })
            }
            FunctionKernel::CharLength => {
                expect_len("CHAR_LENGTH", args, 1)?;
                let value = args[0].eval(row, scratch)?;
                crate::engine::expr::eval_char_length_value(value)
            }
            FunctionKernel::Lower => {
                expect_len("LOWER", args, 1)?;
                let value = args[0].eval(row, scratch)?;
                crate::engine::expr::eval_case_map_value(value, false)
            }
            FunctionKernel::Upper => {
                expect_len("UPPER", args, 1)?;
                let value = args[0].eval(row, scratch)?;
                crate::engine::expr::eval_case_map_value(value, true)
            }
            FunctionKernel::Ltrim | FunctionKernel::Rtrim => {
                let leading = matches!(self, FunctionKernel::Ltrim);
                expect_len(if leading { "LTRIM" } else { "RTRIM" }, args, 1)?;
                let value = args[0].eval(row, scratch)?;
                crate::engine::expr::eval_edge_trim_value(value, leading)
            }
            FunctionKernel::Replace => {
                expect_len("REPLACE", args, 3)?;
                let mut values = Vec::with_capacity(3);
                for arg in args {
                    let value = arg.eval(row, scratch)?;
                    if matches!(value, Value::Null) {
                        return Ok(Value::Null);
                    }
                    values.push(value);
                }
                crate::engine::expr::eval_replace_values(&values)
            }
            FunctionKernel::Lpad | FunctionKernel::Rpad => {
                let pad_left = matches!(self, FunctionKernel::Lpad);
                let fname = if pad_left { "LPAD" } else { "RPAD" };
                if args.len() < 2 || args.len() > 3 {
                    return Err(error(
                        crate::error::catalog::ER_WRONG_PARAMCOUNT_TO_NATIVE_FCT,
                        format!(
                            "Incorrect parameter count in the call to native function '{fname}'"
                        ),
                    ));
                }
                let mut values = Vec::with_capacity(args.len());
                for arg in args {
                    let value = arg.eval(row, scratch)?;
                    if matches!(value, Value::Null) {
                        return Ok(Value::Null);
                    }
                    values.push(value);
                }
                let target_len = int_value(&values[1])?;
                if matches!(values[0], Value::Bytes(_)) {
                    let s = crate::engine::expr::value_bytes(&values[0]);
                    let pad = values
                        .get(2)
                        .map(crate::engine::expr::value_bytes)
                        .unwrap_or_else(|| vec![b' ']);
                    return Ok(crate::engine::expr::eval_pad_bytes_core(
                        s, target_len, pad, pad_left,
                    ));
                }
                let s = crate::engine::expr::string_value_for_connection(&values[0])?;
                let pad = values
                    .get(2)
                    .map(crate::engine::expr::string_value_for_connection)
                    .transpose()?
                    .unwrap_or_else(|| " ".to_string());
                Ok(crate::engine::expr::eval_pad_core(
                    s, target_len, pad, pad_left,
                ))
            }
            FunctionKernel::Repeat => {
                expect_len("REPEAT", args, 2)?;
                let s = args[0].eval(row, scratch)?;
                let n = args[1].eval(row, scratch)?;
                if matches!(s, Value::Null) || matches!(n, Value::Null) {
                    return Ok(Value::Null);
                }
                Ok(crate::engine::expr::eval_repeat_core(&s, int_value(&n)?))
            }
            FunctionKernel::Space => {
                expect_len("SPACE", args, 1)?;
                let value = args[0].eval(row, scratch)?;
                null_or(value, |value| {
                    Ok(crate::engine::expr::eval_space_core(int_value(&value)?))
                })
            }
            FunctionKernel::Reverse => {
                expect_len("REVERSE", args, 1)?;
                let value = args[0].eval(row, scratch)?;
                crate::engine::expr::eval_reverse_value(value)
            }
            FunctionKernel::Ascii => {
                expect_len("ASCII", args, 1)?;
                let value = args[0].eval(row, scratch)?;
                null_or(value, |value| {
                    let s = crate::engine::expr::str_value(&value);
                    Ok(Value::Int(s.bytes().next().unwrap_or(0) as i64))
                })
            }
            FunctionKernel::Ord => {
                expect_len("ORD", args, 1)?;
                let value = args[0].eval(row, scratch)?;
                crate::engine::expr::eval_ord_value(value)
            }
            FunctionKernel::Strcmp => {
                expect_len("STRCMP", args, 2)?;
                let left = args[0].eval_ref(row, scratch)?;
                let right = args[1].eval_ref(row, scratch)?;
                if left.is_null() || right.is_null() {
                    return Ok(Value::Null);
                }
                let (left, right) = (left.as_value(), right.as_value());
                let cmp = crate::engine::expr::compare_values_as_strings(left, right)?;
                Ok(Value::Int(match cmp {
                    std::cmp::Ordering::Less => -1,
                    std::cmp::Ordering::Equal => 0,
                    std::cmp::Ordering::Greater => 1,
                }))
            }
            FunctionKernel::TemporalComparisonKey => {
                expect_len("__TEMPORAL_COMPARISON_KEY", args, 1)?;
                crate::engine::expr::temporal_comparison_key(args[0].eval(row, scratch)?)
            }
            FunctionKernel::DistinctCollationKey => {
                expect_len("__DISTINCT_COLLATION_KEY", args, 1)?;
                let value = args[0].eval(row, scratch)?;
                match value {
                    Value::Null => Ok(Value::Null),
                    Value::String(text) => {
                        let collation_id = crate::engine::expr::COMPARE_COLLATION_ID
                            .with(std::cell::Cell::get)
                            .unwrap_or_else(crate::engine::expr::connection_collation_id);
                        Ok(Value::Bytes(crate::charset::collation_key_by_id(
                            collation_id,
                            &text,
                        )))
                    }
                    Value::SmallString(bytes) => {
                        // SAFETY: SmallString maintains the UTF-8 invariant.
                        let text = unsafe { std::str::from_utf8_unchecked(bytes.as_slice()) };
                        let collation_id = crate::engine::expr::COMPARE_COLLATION_ID
                            .with(std::cell::Cell::get)
                            .unwrap_or_else(crate::engine::expr::connection_collation_id);
                        Ok(Value::Bytes(crate::charset::collation_key_by_id(
                            collation_id,
                            text,
                        )))
                    }
                    value => Ok(value),
                }
            }
            FunctionKernel::Abs => {
                expect_len("ABS", args, 1)?;
                let value = args[0].eval(row, scratch)?;
                // Shared with the legacy evaluator: type follows the
                // argument, ABS(i64::MIN) is error 1690.
                null_or(value, |value| crate::engine::expr::eval_abs(&value))
            }
            FunctionKernel::Round | FunctionKernel::Truncate => {
                let name = if matches!(self, FunctionKernel::Round) {
                    "ROUND"
                } else {
                    "TRUNCATE"
                };
                let round = matches!(self, FunctionKernel::Round);
                if round {
                    if args.is_empty() || args.len() > 2 {
                        return Err(error(
                            crate::error::catalog::ER_WRONG_PARAMCOUNT_TO_NATIVE_FCT,
                            format!(
                                "Incorrect parameter count in the call to native function '{name}'"
                            ),
                        ));
                    }
                } else {
                    expect_len(name, args, 2)?;
                }
                // TRUNCATE resolves D before its value expression. Preserve
                // this ordering for scalar-subquery errors and side effects.
                let d = if !round {
                    let d_value = args[1].eval(row, scratch)?;
                    if matches!(d_value, Value::Null) {
                        return Ok(Value::Null);
                    }
                    crate::engine::expr::round_decimals_value(&d_value)?
                } else {
                    match args.get(1) {
                        Some(arg) => {
                            let d_value = arg.eval(row, scratch)?;
                            if matches!(d_value, Value::Null) {
                                return Ok(Value::Null);
                            }
                            // Saturating cast to i32: a bare `as i32` from
                            // i64 wraps and flips the sign.
                            crate::engine::expr::round_decimals_value(&d_value)?
                        }
                        None => 0,
                    }
                };
                let value = args[0].eval(row, scratch)?;
                null_or(value, |value| {
                    let result = crate::engine::expr::round_or_truncate_value(&value, d, round)?;
                    Ok(crate::engine::expr::preserve_dynamic_round_scale(
                        &value,
                        result,
                        round
                            && args
                                .get(1)
                                .is_some_and(|arg| !matches!(arg, ExprNode::Const(_))),
                    ))
                })
            }
            FunctionKernel::Ceil | FunctionKernel::Floor => {
                let ceil = matches!(self, FunctionKernel::Ceil);
                // Preserve the original spelling so error 1582 reports
                // 'CEILING' when the user typed CEILING, matching MySQL.
                expect_len(if ceil { "CEILING" } else { "FLOOR" }, args, 1)?;
                let value = args[0].eval(row, scratch)?;
                null_or(value, |value| {
                    crate::engine::expr::eval_ceil_floor(&value, ceil)
                })
            }
            FunctionKernel::Sqrt => {
                expect_len("SQRT", args, 1)?;
                let value = args[0].eval(row, scratch)?;
                null_or(value, |value| {
                    let x = numeric(&value)?;
                    if x < 0.0 {
                        return Ok(Value::Null);
                    }
                    Ok(Value::Double(x.sqrt()))
                })
            }
            FunctionKernel::Sign => {
                expect_len("SIGN", args, 1)?;
                let value = args[0].eval(row, scratch)?;
                null_or(value, |value| {
                    let value = numeric(&value)?;
                    Ok(Value::Int(if value > 0.0 {
                        1
                    } else if value < 0.0 {
                        -1
                    } else {
                        0
                    }))
                })
            }
            FunctionKernel::Mod => {
                expect_len("MOD", args, 2)?;
                let left = args[0].eval(row, scratch)?;
                let right = args[1].eval(row, scratch)?;
                if matches!(left, Value::Null) || matches!(right, Value::Null) {
                    return Ok(Value::Null);
                }
                // Same kernel as the `%` operator and the legacy MOD().
                crate::engine::expr::eval_arithmetic(&left, BinaryOp::Mod, &right)
            }
            FunctionKernel::Year
            | FunctionKernel::Month
            | FunctionKernel::Day
            | FunctionKernel::Hour
            | FunctionKernel::Minute
            | FunctionKernel::Second => eval_temporal_part(self, args, row, scratch),
            FunctionKernel::JsonExtract => eval_json_extract(args, row, scratch),
            FunctionKernel::LegacyDeterministic(name) => {
                eval_legacy_by_name(name, args, row, scratch)
            }
            FunctionKernel::LegacySession(name) => eval_legacy_by_name(name, args, row, scratch),
            FunctionKernel::RegexpLike => eval_regexp_like(args, row, scratch),
            FunctionKernel::RegexpReplace => eval_regexp_replace(args, row, scratch),
            FunctionKernel::Trim => {
                if !(1..=3).contains(&args.len()) {
                    return Err(crate::error::catalog::ER_WRONG_PARAMCOUNT_TO_NATIVE_FCT
                        .error("Incorrect parameter count in the call to native function 'TRIM'"));
                }
                let values = args
                    .iter()
                    .map(|arg| arg.eval(row, scratch))
                    .collect::<Result<Vec<_>, _>>()?;
                crate::engine::expr::eval_trim_values(&values)
            }
            FunctionKernel::Substring => eval_substring_kernel(args, row, scratch),
            FunctionKernel::Left => eval_left_kernel(args, row, scratch),
            FunctionKernel::Right => eval_right_kernel(args, row, scratch),
            FunctionKernel::Concat => eval_concat_kernel(args, row, scratch),
            FunctionKernel::ConcatWs => eval_concat_ws_kernel(args, row, scratch),
            FunctionKernel::Crc32 => {
                expect_len("CRC32", args, 1)?;
                let value = args[0].eval_ref(row, scratch)?;
                Ok(crate::engine::expr::crc32_value(value.as_value()))
            }
            FunctionKernel::AnyValue => {
                expect_len("ANY_VALUE", args, 1)?;
                args[0].eval(row, scratch)
            }
            FunctionKernel::IsNull => {
                expect_len("ISNULL", args, 1)?;
                let value = args[0].eval_ref(row, scratch)?;
                Ok(Value::Int(if value.is_null() { 1 } else { 0 }))
            }
            FunctionKernel::SubstringIndex => {
                expect_len("SUBSTRING_INDEX", args, 3)?;
                let s = args[0].eval(row, scratch)?;
                if matches!(s, Value::Null) {
                    return Ok(Value::Null);
                }
                let delim = args[1].eval(row, scratch)?;
                if matches!(delim, Value::Null) {
                    return Ok(Value::Null);
                }
                let count_val = args[2].eval(row, scratch)?;
                if matches!(count_val, Value::Null) {
                    return Ok(Value::Null);
                }
                let count = int_value(&count_val)?;
                if matches!(s, Value::Bytes(_)) {
                    return Ok(Value::Bytes(crate::engine::expr::substring_index_bytes(
                        &crate::engine::expr::value_bytes(&s),
                        &crate::engine::expr::value_bytes(&delim),
                        count,
                    )));
                }
                let s = crate::engine::expr::string_value_for_connection(&s)?;
                let delim = crate::engine::expr::string_value_for_connection(&delim)?;
                Ok(Value::String(crate::engine::expr::substring_index(
                    &s, &delim, count,
                )))
            }
            FunctionKernel::Datediff => {
                expect_len("DATEDIFF", args, 2)?;
                let a = args[0].eval(row, scratch)?;
                if matches!(a, Value::Null) {
                    return Ok(Value::Null);
                }
                let b = args[1].eval(row, scratch)?;
                if matches!(b, Value::Null) {
                    return Ok(Value::Null);
                }
                crate::engine::expr::eval_datediff_values(&a, &b)
            }
            FunctionKernel::Quarter
            | FunctionKernel::Dayofweek
            | FunctionKernel::Weekday
            | FunctionKernel::Dayofyear
            | FunctionKernel::LastDay
            | FunctionKernel::ToDays
            | FunctionKernel::Microsecond
            | FunctionKernel::Weekofyear => {
                expect_len("temporal function", args, 1)?;
                let value = args[0].eval(row, scratch)?;
                null_or(value, |value| match self {
                    FunctionKernel::Quarter => crate::engine::expr::eval_quarter_value(&value),
                    FunctionKernel::Dayofweek => crate::engine::expr::eval_dayofweek_value(&value),
                    FunctionKernel::Weekday => crate::engine::expr::eval_weekday_value(&value),
                    FunctionKernel::Dayofyear => crate::engine::expr::eval_dayofyear_value(&value),
                    FunctionKernel::LastDay => crate::engine::expr::eval_last_day_value(&value),
                    FunctionKernel::ToDays => crate::engine::expr::eval_to_days_value(&value),
                    FunctionKernel::Microsecond => {
                        crate::engine::expr::eval_microsecond_value(&value)
                    }
                    FunctionKernel::Weekofyear => {
                        crate::engine::expr::eval_week_value(&value, Some(3))
                    }
                    _ => unreachable!(),
                })
            }
            FunctionKernel::Week => {
                if args.is_empty() || args.len() > 2 {
                    return Err(error(
                        crate::error::catalog::ER_WRONG_PARAMCOUNT_TO_NATIVE_FCT,
                        "Incorrect parameter count in the call to native function 'WEEK'",
                    ));
                }
                let value = args[0].eval(row, scratch)?;
                if matches!(value, Value::Null) {
                    return Ok(Value::Null);
                }
                let mode = match args.get(1) {
                    Some(node) => {
                        let v = node.eval(row, scratch)?;
                        if matches!(v, Value::Null) {
                            return Ok(Value::Null);
                        }
                        Some(int_value(&v)?)
                    }
                    None => None,
                };
                crate::engine::expr::eval_week_value(&value, mode)
            }
            FunctionKernel::DateAdd | FunctionKernel::DateSub => {
                expect_len("DATE_ADD", args, 2)?;
                let date_val = args[0].eval(row, scratch)?;
                if matches!(date_val, Value::Null) {
                    return Ok(Value::Null);
                }
                let (amount, unit) = match &args[1] {
                    ExprNode::Function {
                        kernel: FunctionKernel::IntervalPair,
                        args: interval_args,
                        ..
                    } if interval_args.len() == 2 => {
                        let amount = interval_args[0].eval(row, scratch)?;
                        let unit = string_value(&interval_args[1].eval(row, scratch)?)
                            .to_ascii_uppercase();
                        (amount, unit)
                    }
                    other => (other.eval(row, scratch)?, "DAY".to_string()),
                };
                if matches!(amount, Value::Null) {
                    return Ok(Value::Null);
                }
                crate::engine::expr::eval_date_add_values(
                    &date_val,
                    &amount,
                    &unit,
                    matches!(self, FunctionKernel::DateSub),
                )
            }
            FunctionKernel::Date => {
                expect_len("DATE", args, 1)?;
                crate::engine::expr::eval_date_function_value(args[0].eval(row, scratch)?)
            }
            FunctionKernel::IntervalPair => Err(error(
                crate::error::catalog::ER_PARSE_ERROR,
                "INTERVAL expression is only valid inside DATE_ADD/DATE_SUB",
            )),
            FunctionKernel::DateFormat => {
                expect_len("DATE_FORMAT", args, 2)?;
                let value = args[0].eval(row, scratch)?;
                if matches!(value, Value::Null) {
                    return Ok(Value::Null);
                }
                let fmt = args[1].eval(row, scratch)?;
                if matches!(fmt, Value::Null) {
                    return Ok(Value::Null);
                }
                crate::engine::expr::eval_date_format_values(&value, &fmt)
            }
            FunctionKernel::UnixTimestamp => {
                if args.len() > 1 {
                    return Err(error(
                        crate::error::catalog::ER_WRONG_PARAMCOUNT_TO_NATIVE_FCT,
                        "Incorrect parameter count in the call to native function 'UNIX_TIMESTAMP'",
                    ));
                }
                match args.first() {
                    None => Ok(crate::engine::expr::eval_unix_timestamp_noarg()),
                    Some(node) => {
                        let value = node.eval(row, scratch)?;
                        null_or(value, |value| {
                            crate::engine::expr::eval_unix_timestamp_scalar_value(&value)
                        })
                    }
                }
            }
            // -- Session / time kernels --
            //
            // Implementations delegate to `expr.rs` helpers so the
            // compiled kernel and the legacy interpreter share one
            // source of truth — any future tweak (e.g. timezone
            // handling) only needs to land in one place. Each kernel
            // is also marked non-deterministic above so
            // `fold_constants` never collapses it at compile time.
            FunctionKernel::Now => {
                let fsp = optional_fsp_arg("NOW", args, row, scratch)?;
                Ok(crate::engine::expr::eval_now_fsp(fsp))
            }
            FunctionKernel::Curdate => {
                expect_len("CURDATE", args, 0)?;
                Ok(crate::engine::expr::eval_curdate_func())
            }
            FunctionKernel::Curtime => {
                let fsp = optional_fsp_arg("CURTIME", args, row, scratch)?;
                Ok(crate::engine::expr::eval_curtime_fsp(fsp))
            }
            FunctionKernel::UtcTimestamp => {
                let fsp = optional_fsp_arg("UTC_TIMESTAMP", args, row, scratch)?;
                Ok(crate::engine::expr::eval_utc_timestamp_fsp(fsp))
            }
            FunctionKernel::UtcDate => {
                expect_len("UTC_DATE", args, 0)?;
                Ok(crate::engine::expr::eval_utc_date_func())
            }
            FunctionKernel::UtcTime => {
                let fsp = optional_fsp_arg("UTC_TIME", args, row, scratch)?;
                Ok(crate::engine::expr::eval_utc_time_fsp(fsp))
            }
            FunctionKernel::Version => {
                expect_len("VERSION", args, 0)?;
                Ok(crate::engine::expr::eval_version_func())
            }
            FunctionKernel::Database => {
                expect_len("DATABASE", args, 0)?;
                Ok(crate::engine::expr::eval_database_func())
            }
            FunctionKernel::User => {
                expect_len("USER", args, 0)?;
                Ok(crate::engine::expr::eval_user_session_func())
            }
            FunctionKernel::CurrentUser => {
                expect_len("CURRENT_USER", args, 0)?;
                Ok(crate::engine::expr::eval_current_user_func())
            }
            FunctionKernel::ConnectionId => {
                expect_len("CONNECTION_ID", args, 0)?;
                Ok(crate::engine::expr::eval_connection_id_func())
            }
            FunctionKernel::FoundRows => {
                expect_len("FOUND_ROWS", args, 0)?;
                Ok(crate::engine::expr::eval_found_rows_func())
            }
            FunctionKernel::RowCount => {
                expect_len("ROW_COUNT", args, 0)?;
                Ok(crate::engine::expr::eval_row_count_func())
            }
        }
    }
}

fn fixed_current_temporal_display_len(node: &ExprNode) -> Option<usize> {
    let ExprNode::Function { kernel, args, .. } = node else {
        return None;
    };
    let base = match kernel {
        FunctionKernel::Now | FunctionKernel::UtcTimestamp => 19,
        FunctionKernel::Curtime | FunctionKernel::UtcTime => 8,
        _ => return None,
    };
    let fsp = match args.as_slice() {
        [] => 0,
        [ExprNode::Const(Value::Int(value))] if (0..=6).contains(value) => *value as usize,
        [ExprNode::Const(Value::UInt(value))] if *value <= 6 => *value as usize,
        _ => return None,
    };
    Some(base + usize::from(fsp > 0) + fsp)
}

/// Validate the optional fractional-seconds-precision argument shared by
/// `NOW`, `CURTIME`, `UTC_TIMESTAMP`, `UTC_TIME`. Accepts 0 or 1 args;
/// the value (when present) must coerce to `0..=6` per MySQL. Larger
/// arg counts return MySQL error 1582 to match the legacy path.
fn optional_fsp_arg(
    name: &'static str,
    args: &[ExprNode],
    row: &dyn RowSource,
    scratch: &mut EvalScratch,
) -> Result<usize, QueryError> {
    match args.len() {
        0 => Ok(0),
        1 => {
            let value = args[0].eval(row, scratch)?;
            crate::engine::expr::validate_fsp_value(Some(&value))
        }
        _ => Err(error(
            crate::error::catalog::ER_WRONG_PARAMCOUNT_TO_NATIVE_FCT,
            format!("Incorrect parameter count in the call to native function '{name}'"),
        )),
    }
}

fn eval_concat_kernel(
    args: &[ExprNode],
    row: &dyn RowSource,
    scratch: &mut EvalScratch,
) -> Result<Value, QueryError> {
    if args.is_empty() {
        return Err(error(
            crate::error::catalog::ER_WRONG_PARAMCOUNT_TO_NATIVE_FCT,
            "Incorrect parameter count in the call to native function 'CONCAT'",
        ));
    }
    let mut out = Vec::new();
    let mut binary_result = false;
    for arg in args {
        let value = arg.eval_ref(row, scratch)?;
        let value = value.as_value();
        if matches!(value, Value::Null) {
            return Ok(Value::Null);
        }
        binary_result |= matches!(value, Value::Bytes(_));
        let part = crate::engine::expr::value_bytes_ref(value);
        if crate::engine::expr::string_result_exceeds_packet(
            "concat",
            (out.len() + part.len()) as u64,
        ) {
            return Ok(Value::Null);
        }
        out.extend_from_slice(&part);
    }
    if binary_result {
        Ok(Value::Bytes(out))
    } else {
        Ok(Value::String(concat_utf8_string(out)))
    }
}

fn eval_concat_ws_kernel(
    args: &[ExprNode],
    row: &dyn RowSource,
    scratch: &mut EvalScratch,
) -> Result<Value, QueryError> {
    if args.len() < 2 {
        return Err(error(
            crate::error::catalog::ER_WRONG_PARAMCOUNT_TO_NATIVE_FCT,
            "Incorrect parameter count in the call to native function 'CONCAT_WS'",
        ));
    }
    let sep = args[0].eval_ref(row, scratch)?;
    let sep = sep.as_value();
    if matches!(sep, Value::Null) {
        return Ok(Value::Null);
    }
    // Borrow source values and their octets; only formatted values and the
    // final concatenation need owned storage.
    let mut binary_result = matches!(sep, Value::Bytes(_));
    let sep = crate::engine::expr::value_bytes_ref(sep);
    let mut out = Vec::new();
    let mut first = true;
    for arg in &args[1..] {
        let value = arg.eval_ref(row, scratch)?;
        let value = value.as_value();
        if matches!(value, Value::Null) {
            continue;
        }
        binary_result |= matches!(value, Value::Bytes(_));
        let part = crate::engine::expr::value_bytes_ref(value);
        let added = part.len() + if first { 0 } else { sep.len() };
        if crate::engine::expr::string_result_exceeds_packet(
            "concat_ws",
            (out.len() + added) as u64,
        ) {
            return Ok(Value::Null);
        }
        if !first {
            out.extend_from_slice(&sep);
        }
        out.extend_from_slice(&part);
        first = false;
    }
    if binary_result {
        Ok(Value::Bytes(out))
    } else {
        Ok(Value::String(concat_utf8_string(out)))
    }
}

fn concat_utf8_string(bytes: Vec<u8>) -> String {
    simdutf8::basic::from_utf8(&bytes).expect("non-binary CONCAT operands render as UTF-8");
    // SAFETY: validated above; moving the Vec preserves its bytes.
    unsafe { String::from_utf8_unchecked(bytes) }
}

fn eval_substring_kernel(
    args: &[ExprNode],
    row: &dyn RowSource,
    scratch: &mut EvalScratch,
) -> Result<Value, QueryError> {
    if !(2..=3).contains(&args.len()) {
        return Err(error(
            crate::error::catalog::ER_WRONG_PARAMCOUNT_TO_NATIVE_FCT,
            "Incorrect parameter count in the call to native function 'SUBSTRING'",
        ));
    }
    let input_v = args[0].eval_ref(row, scratch)?;
    if input_v.is_null() {
        return Ok(Value::Null);
    }
    let start_v = args[1].eval(row, scratch)?;
    if matches!(start_v, Value::Null) {
        return Ok(Value::Null);
    }
    let len_v = if args.len() == 3 {
        let v = args[2].eval(row, scratch)?;
        if matches!(v, Value::Null) {
            return Ok(Value::Null);
        }
        Some(v)
    } else {
        None
    };
    let start = crate::engine::expr::string_extent_arg(&start_v);
    let len = len_v.as_ref().map(crate::engine::expr::string_extent_arg);
    if let Value::Bytes(bytes) = input_v.as_value() {
        return Ok(Value::Bytes(crate::engine::expr::mysql_substring_bytes(
            bytes, start, len,
        )));
    }
    let input = crate::engine::expr::str_value(input_v.as_value());
    Ok(Value::String(mysql_substring(&input, start, len)))
}

fn eval_left_kernel(
    args: &[ExprNode],
    row: &dyn RowSource,
    scratch: &mut EvalScratch,
) -> Result<Value, QueryError> {
    expect_len("LEFT", args, 2)?;
    let input_v = args[0].eval_ref(row, scratch)?;
    if input_v.is_null() {
        return Ok(Value::Null);
    }
    let len_v = args[1].eval(row, scratch)?;
    if matches!(len_v, Value::Null) {
        return Ok(Value::Null);
    }
    let len = crate::engine::expr::string_extent_arg(&len_v).max(0) as usize;
    if let Value::Bytes(bytes) = input_v.as_value() {
        return Ok(Value::Bytes(bytes[..len.min(bytes.len())].to_vec()));
    }
    let input = crate::engine::expr::str_value(input_v.as_value());
    Ok(Value::String(mysql_left(&input, len)))
}

fn eval_right_kernel(
    args: &[ExprNode],
    row: &dyn RowSource,
    scratch: &mut EvalScratch,
) -> Result<Value, QueryError> {
    expect_len("RIGHT", args, 2)?;
    let input_v = args[0].eval_ref(row, scratch)?;
    if input_v.is_null() {
        return Ok(Value::Null);
    }
    let len_v = args[1].eval(row, scratch)?;
    if matches!(len_v, Value::Null) {
        return Ok(Value::Null);
    }
    let len = crate::engine::expr::string_extent_arg(&len_v).max(0) as usize;
    if let Value::Bytes(bytes) = input_v.as_value() {
        let start = bytes.len().saturating_sub(len);
        return Ok(Value::Bytes(bytes[start..].to_vec()));
    }
    let input = crate::engine::expr::str_value(input_v.as_value());
    Ok(Value::String(mysql_right(&input, len)))
}

fn eval_temporal_part(
    kernel: FunctionKernel,
    args: &[ExprNode],
    row: &dyn RowSource,
    scratch: &mut EvalScratch,
) -> Result<Value, QueryError> {
    expect_len("datetime part", args, 1)?;
    let value = args[0].eval(row, scratch)?;
    // Delegate to the legacy `extract_datetime_part` single source rather
    // than re-implementing extraction here. The compiled path used to call
    // `parse_datetime` directly, which fails for TIME values outside
    // 00:00:00..=23:59:59 (e.g. `HOUR(TIME'838:59:59')` or negative times)
    // and raised 1366 — whereas legacy falls back to `parse_time_components`
    // so time parts work like TIME_TO_SEC (issue #75). Routing through the
    // oracle keeps the two paths bit-identical (zero-date handling included).
    let part = match kernel {
        FunctionKernel::Year => "YEAR",
        FunctionKernel::Month => "MONTH",
        FunctionKernel::Day => "DAY",
        FunctionKernel::Hour => "HOUR",
        FunctionKernel::Minute => "MINUTE",
        FunctionKernel::Second => "SECOND",
        _ => unreachable!(),
    };
    null_or(value, |value| {
        crate::engine::expr::extract_datetime_part(part, part, &value)
    })
}

fn eval_json_extract(
    args: &[ExprNode],
    row: &dyn RowSource,
    scratch: &mut EvalScratch,
) -> Result<Value, QueryError> {
    if args.len() < 2 {
        return Err(error(
            crate::error::catalog::ER_WRONG_PARAMCOUNT_TO_NATIVE_FCT,
            "Incorrect parameter count in the call to native function 'JSON_EXTRACT'",
        ));
    }
    let mut bound_args = smallvec::SmallVec::<[Expr; 4]>::with_capacity(args.len());
    for arg in args {
        bound_args.push(Expr::BoundValue(arg.eval(row, scratch)?));
    }
    crate::engine::expr::eval_function("JSON_EXTRACT", &bound_args, |arg| match arg {
        Expr::BoundValue(value) => Ok(value.clone()),
        _ => unreachable!(),
    })
}

/// Evaluate a deterministic scalar function by delegating to the legacy
/// `eval_function` body, with each argument pre-evaluated through its
/// *compiled* node and handed in as a `BoundValue`. Keeps legacy as the
/// single semantic oracle while resolving column leaves positionally.
/// Only valid for functions whose legacy body reads arguments solely via
/// the `eval_arg` closure (no structural `match &args[i]` on literals).
fn eval_legacy_by_name(
    name: &str,
    args: &[ExprNode],
    row: &dyn RowSource,
    scratch: &mut EvalScratch,
) -> Result<Value, QueryError> {
    let mut bound_args = smallvec::SmallVec::<[Expr; 4]>::with_capacity(args.len());
    for arg in args {
        bound_args.push(Expr::BoundValue(arg.eval(row, scratch)?));
    }
    crate::engine::expr::eval_function(name, &bound_args, |arg| match arg {
        Expr::BoundValue(value) => Ok(value.clone()),
        _ => unreachable!(),
    })
}

fn eval_regexp_like(
    args: &[ExprNode],
    row: &dyn RowSource,
    scratch: &mut EvalScratch,
) -> Result<Value, QueryError> {
    if !(2..=3).contains(&args.len()) {
        return Err(error(
            crate::error::catalog::ER_WRONG_PARAMCOUNT_TO_NATIVE_FCT,
            "Incorrect parameter count in the call to native function 'REGEXP_LIKE'",
        ));
    }
    let text = args[0].eval(row, scratch)?;
    let pattern = args[1].eval(row, scratch)?;
    if matches!(text, Value::Null) || matches!(pattern, Value::Null) {
        return Ok(Value::Null);
    }
    let match_type = args.get(2).map(|arg| arg.eval(row, scratch)).transpose()?;
    if matches!(match_type, Some(Value::Null)) {
        return Ok(Value::Null);
    }
    let binary = matches!(text, Value::Bytes(_)) || matches!(pattern, Value::Bytes(_));
    let text = string_value(&text);
    let pattern = string_value(&pattern);
    let re = crate::engine::expr::mysql_regex_for_operand(&pattern, match_type.as_ref(), binary)?;
    Ok(Value::Int(re.is_match(&text) as i64))
}

fn eval_regexp_replace(
    args: &[ExprNode],
    row: &dyn RowSource,
    scratch: &mut EvalScratch,
) -> Result<Value, QueryError> {
    if !(3..=6).contains(&args.len()) {
        return Err(error(
            crate::error::catalog::ER_WRONG_PARAMCOUNT_TO_NATIVE_FCT,
            "Incorrect parameter count in the call to native function 'REGEXP_REPLACE'",
        ));
    }
    let mut values = Vec::with_capacity(args.len());
    for arg in args {
        values.push(arg.eval(row, scratch)?);
    }
    crate::engine::expr::eval_regexp_replace_values(values)
}

fn infer_program_metadata(root: &ExprNode) -> (ExprCapability, usize) {
    let mut cap = ExprCapability {
        supports_batch: true,
        deterministic: true,
        depends_on_session: false,
        may_error: false,
        legacy_escape: false,
    };
    let mut column_references = 0;
    infer_capability_node(root, &mut cap, &mut column_references);
    (cap, column_references)
}

fn regexp_replace_const_is_infallible(args: &[ExprNode]) -> bool {
    let [_, ExprNode::Const(pattern), ExprNode::Const(replacement)] = args else {
        return false;
    };
    let Some(pattern) = crate::types::value_str(pattern) else {
        return false;
    };
    if crate::types::value_str(replacement).is_none() {
        return false;
    }
    // With the default position/occurrence and string constants, regex
    // compilation is the only SQL diagnostic this function can produce.
    // Validate both text and binary flag variants before allowing aggregate
    // scheduling to evaluate the expression on unordered worker batches.
    crate::engine::func_cache::cached_regex(pattern, "i").is_ok()
        && crate::engine::func_cache::cached_regex(pattern, "").is_ok()
}

fn infer_capability_node(node: &ExprNode, cap: &mut ExprCapability, column_references: &mut usize) {
    match node {
        ExprNode::Column(_) => *column_references += 1,
        ExprNode::FieldCount | ExprNode::Const(_) | ExprNode::Placeholder(_) => {}
        ExprNode::Unary { expr, .. } | ExprNode::IsNull(expr) | ExprNode::IsNotNull(expr) => {
            infer_capability_node(expr, cap, column_references)
        }
        ExprNode::Binary {
            left, op, right, ..
        } => {
            if matches!(
                op,
                BinaryOp::Div
                    | BinaryOp::IntDiv
                    | BinaryOp::Mod
                    | BinaryOp::Arrow
                    | BinaryOp::LongArrow
                    | BinaryOp::Add
                    | BinaryOp::Sub
                    | BinaryOp::Mul
                    | BinaryOp::BitAnd
                    | BinaryOp::BitOr
                    | BinaryOp::BitXor
                    | BinaryOp::ShiftLeft
                    | BinaryOp::ShiftRight
            ) {
                cap.may_error = true;
            }
            infer_capability_node(left, cap, column_references);
            infer_capability_node(right, cap, column_references);
        }
        ExprNode::InList { expr, list, .. } => {
            infer_capability_node(expr, cap, column_references);
            for item in list {
                infer_capability_node(item, cap, column_references);
            }
        }
        ExprNode::InConstSet { expr, .. } | ExprNode::SubstringConst { expr, .. } => {
            infer_capability_node(expr, cap, column_references);
        }
        ExprNode::Between {
            expr, low, high, ..
        } => {
            infer_capability_node(expr, cap, column_references);
            infer_capability_node(low, cap, column_references);
            infer_capability_node(high, cap, column_references);
        }
        ExprNode::Like { expr, pattern, .. } => {
            cap.supports_batch = false;
            infer_capability_node(expr, cap, column_references);
            infer_capability_node(pattern, cap, column_references);
        }
        ExprNode::LikeConst { expr, .. } => {
            cap.supports_batch = false;
            infer_capability_node(expr, cap, column_references);
        }
        ExprNode::Case {
            operand,
            conditions,
            results,
            else_result,
            result_coercion,
        } => {
            cap.may_error |= result_coercion.is_some();
            if let Some(operand) = operand {
                infer_capability_node(operand, cap, column_references);
            }
            for node in conditions.iter().chain(results.iter()) {
                infer_capability_node(node, cap, column_references);
            }
            if let Some(else_result) = else_result {
                infer_capability_node(else_result, cap, column_references);
            }
        }
        ExprNode::Function {
            kernel,
            args,
            result_coercion,
            ..
        } => {
            cap.supports_batch = false;
            cap.may_error |= result_coercion.is_some();
            cap.may_error |= if matches!(kernel, FunctionKernel::RegexpReplace) {
                !regexp_replace_const_is_infallible(args)
            } else {
                kernel.may_error(args.len())
            };
            if !kernel.is_deterministic() {
                cap.deterministic = false;
                cap.depends_on_session = true;
            }
            for arg in args {
                infer_capability_node(arg, cap, column_references);
            }
        }
        ExprNode::LegacyEscape { columns, .. } => {
            *column_references += columns.len();
            // Opaque legacy sub-tree: no batch lane, may error, and treated
            // as session-dependent / non-deterministic conservatively.
            cap.supports_batch = false;
            cap.may_error = true;
            cap.deterministic = false;
            cap.depends_on_session = true;
            cap.legacy_escape = true;
        }
    }
}

#[cfg(test)]
mod cache_tests {
    use super::*;
    use crate::parser::{BinaryOp, Expr, Literal};

    fn nested_not(depth: usize) -> Expr {
        (0..depth).fold(Expr::Literal(Literal::Bool(true)), |expr, _| Expr::Unary {
            op: UnaryOp::Not,
            expr: Box::new(expr),
        })
    }

    #[test]
    fn dependency_cache_is_lazy_and_prepaid_for_shared_programs() {
        let schema = InputSchema::new(vec![InputColumn {
            table: "t".into(),
            alias: None,
            name: "x".into(),
            column_type: ColumnType::LongLong,
            unsigned: false,
            collation_id: None,
        }]);
        let expr = Expr::Binary {
            left: Box::new(Expr::Identifier("x".into())),
            op: BinaryOp::Add,
            right: Box::new(Expr::Identifier("x".into())),
        };
        for program in [
            ExprCompiler::compile(&expr, &schema).unwrap(),
            ExprCompiler::compile_legacy_oracle(&expr, &schema).unwrap(),
        ] {
            assert!(program.referenced_columns.get().is_none());
            assert!(program.column_reference_bound >= 1);
            assert_eq!(program.referenced_columns(), [0]);
            assert!(
                std::mem::size_of_val(program.referenced_columns())
                    <= program.column_reference_bound * std::mem::size_of::<usize>()
            );
        }
        let shared = ExprCompiler::compile_cached(&expr, &schema).unwrap();
        assert!(shared.referenced_columns.get().is_none());
        let before = shared.charged_bytes();
        assert!(before >= shared.column_reference_bound * std::mem::size_of::<usize>());
        std::thread::scope(|scope| {
            for _ in 0..4 {
                scope.spawn(|| assert_eq!(shared.referenced_columns(), [0]));
            }
        });
        assert_eq!(shared.charged_bytes(), before);
    }

    #[test]
    fn parameter_descriptor_partitions_compiled_program_cache() {
        flush_program_caches();
        let schema = InputSchema::new(Vec::new());
        let expr = Expr::Placeholder(0);
        let integer = crate::engine::params::with_params(vec![Value::Int(7)], || {
            ExprCompiler::compile_cached(&expr, &schema).expect("compile integer parameter")
        });
        let string =
            crate::engine::params::with_params(vec![Value::String("seven".into())], || {
                ExprCompiler::compile_cached(&expr, &schema).expect("compile string parameter")
            });
        assert!(!Arc::ptr_eq(&integer, &string));
        assert_ne!(integer.result_type(), string.result_type());
    }

    #[test]
    fn cache_fingerprint_collision_requires_structural_equality() {
        let schema = InputSchema::new(Vec::new());
        let first_expr = Expr::Literal(Literal::Int(1));
        let second_expr = Expr::Literal(Literal::Int(2));
        let first = Arc::new(ExprCompiler::compile(&first_expr, &schema).unwrap());
        let second = Arc::new(ExprCompiler::compile(&second_expr, &schema).unwrap());
        let key = ProgramCacheKey {
            expr_fingerprint: 42,
            schema_fingerprint: schema.fingerprint(),
            semantics: ExprProgramSemantics::current(),
        };
        let mut cache = ProgramCache::default();
        cache.insert(
            key,
            Arc::new(first_expr.clone()),
            Ok(first.clone()),
            8,
            1024 * 1024,
        );
        cache.insert(
            key,
            Arc::new(second_expr.clone()),
            Ok(second.clone()),
            8,
            1024 * 1024,
        );
        let first_hit = cache.get(&key, &first_expr).unwrap().unwrap();
        let second_hit = cache.get(&key, &second_expr).unwrap().unwrap();
        assert!(Arc::ptr_eq(&first, &first_hit));
        assert!(Arc::ptr_eq(&second, &second_hit));
    }

    #[test]
    fn program_cache_evicts_least_recently_used_entry() {
        let schema = InputSchema::new(Vec::new());
        let expressions = [1_i64, 2, 3].map(|value| Expr::Literal(Literal::Int(value)));
        let programs = expressions.each_ref().map(|expr| {
            Arc::new(ExprCompiler::compile(expr, &schema).expect("compile cache value"))
        });
        let semantics = ExprProgramSemantics::current();
        let key = |fingerprint| ProgramCacheKey {
            expr_fingerprint: fingerprint,
            schema_fingerprint: schema.fingerprint(),
            semantics,
        };
        let mut cache = ProgramCache::default();
        for index in 0..2 {
            cache.insert(
                key(index as u64),
                Arc::new(expressions[index].clone()),
                Ok(programs[index].clone()),
                2,
                1024 * 1024,
            );
        }
        assert!(cache.get(&key(0), &expressions[0]).is_some());
        cache.insert(
            key(2),
            Arc::new(expressions[2].clone()),
            Ok(programs[2].clone()),
            2,
            1024 * 1024,
        );
        assert!(cache.get(&key(0), &expressions[0]).is_some());
        assert!(cache.get(&key(1), &expressions[1]).is_none());
        assert!(cache.get(&key(2), &expressions[2]).is_some());
    }

    #[test]
    fn excessive_expression_depth_is_rejected_without_recursing() {
        let expr = nested_not(MAX_EXPRESSION_DEPTH + 1);
        let schema = InputSchema::new(Vec::new());
        assert_eq!(
            ExprCompiler::compile_cached(&expr, &schema).unwrap_err(),
            CompileError::TooDeep
        );

        let columns: Vec<crate::types::Column> = Vec::new();
        let table = TableSchema::synthetic_for_planning("depth", &columns);
        let error = crate::engine::expr::eval_expr_legacy(&expr, &table, &[]).unwrap_err();
        assert_eq!(error.code(), 1436);
    }

    #[test]
    fn date_column_comparison_precoerces_deterministic_text_once() {
        let schema = InputSchema::new(vec![InputColumn {
            table: "t".into(),
            alias: None,
            name: "d".into(),
            column_type: ColumnType::Date,
            unsigned: false,
            collation_id: None,
        }]);
        let compile = |text: &str| {
            ExprCompiler::compile(
                &Expr::Binary {
                    left: Box::new(Expr::Identifier("d".into())),
                    op: BinaryOp::GtEq,
                    right: Box::new(Expr::Literal(Literal::String(text.into()))),
                },
                &schema,
            )
            .expect("compile date comparison")
        };

        let valid = compile("1994-01-01");
        let (_, _, value, _) = valid
            .as_column_const_cmp_with_collation()
            .expect("typed column/constant comparison");
        assert!(matches!(value, Value::Date(_)));

        let bound = ExprCompiler::compile(
            &Expr::Binary {
                left: Box::new(Expr::Identifier("d".into())),
                op: BinaryOp::GtEq,
                right: Box::new(Expr::BoundValue(Value::String("1994-01-01".into()))),
            },
            &schema,
        )
        .expect("compile auto-parameterized date comparison");
        let (_, _, value, _) = bound
            .as_column_const_cmp_with_collation()
            .expect("typed BoundValue must expose the batch comparison shape");
        assert!(matches!(value, Value::Date(_)));

        let invalid = compile("not-a-date");
        let (_, _, value, _) = invalid
            .as_column_const_cmp_with_collation()
            .expect("invalid text keeps runtime comparison shape");
        assert!(matches!(value, Value::String(text) if text == "not-a-date"));
    }

    #[test]
    fn flush_releases_calling_threads_programs_without_sampling_stats() {
        flush_program_caches();
        let schema = InputSchema::new(vec![]);
        let expr = Expr::Literal(Literal::Int(8127));
        let program = ExprCompiler::compile_cached(&expr, &schema).unwrap();
        let weak = Arc::downgrade(&program);
        drop(program);
        PROGRAM_CACHE.with(|cache| assert!(cache.borrow().1.entry_count > 0));
        flush_program_caches();
        // Do not sample stats or perform another lookup: FLUSH itself must
        // release cache ownership, even when this worker goes idle afterward.
        assert!(weak.upgrade().is_none());
        PROGRAM_CACHE.with(|cache| {
            let cache = cache.borrow();
            assert_eq!(
                cache.0,
                PROGRAM_CACHE_GENERATION.load(AtomicOrdering::Acquire)
            );
            assert_eq!(cache.1.entry_count, 0);
            assert_eq!(cache.1.logical_bytes, 0);
        });
    }

    #[test]
    fn memory_stats_do_not_evict_stale_thread_cache() {
        flush_program_caches();
        let schema = InputSchema::new(vec![]);
        let expr = Expr::Literal(Literal::Int(718));
        let _program = ExprCompiler::compile_cached(&expr, &schema).unwrap();
        let before = expr_program_stats();
        assert!(before.l1_entries > 0);
        // A different worker invalidates the cache, but this thread still owns
        // its old objects until its next compilation. Reading must not free them.
        std::thread::spawn(flush_program_caches).join().unwrap();
        let after = expr_program_stats();
        assert_eq!(after.l1_entries, before.l1_entries);
        assert_eq!(after.l1_bytes, before.l1_bytes);
        PROGRAM_CACHE.with(|cache| {
            assert_eq!(cache.borrow().1.entry_count as u64, before.l1_entries);
        });
    }

    /// Two `Expr` values that are structurally identical but allocated
    /// at different addresses (the previous pointer-keyed cache treated
    /// these as different entries and missed). We now expect the second
    /// `compile_cached` call to be a hit.
    #[test]
    fn structurally_identical_exprs_share_cache_entry() {
        // Reset stats — other tests in the suite may have warmed the
        // counters, but since they're process-wide we just take a
        // snapshot before / after instead of zeroing.
        let baseline = expr_program_stats();

        let schema = InputSchema::new(vec![InputColumn {
            table: "t".into(),
            alias: None,
            name: "x".into(),
            column_type: ColumnType::LongLong,
            unsigned: false,
            collation_id: None,
        }]);

        let make_expr = || Expr::Binary {
            left: Box::new(Expr::Identifier("x".into())),
            op: BinaryOp::Gt,
            right: Box::new(Expr::Literal(Literal::Int(10))),
        };
        let a = make_expr();
        let b = make_expr();
        assert!(!std::ptr::eq(&a, &b), "test setup: distinct allocations");

        let _ = ExprCompiler::compile_cached(&a, &schema).expect("compile a");
        let _ = ExprCompiler::compile_cached(&b, &schema).expect("compile b");

        let after = expr_program_stats();
        assert!(
            after.compile_cache_hits > baseline.compile_cache_hits,
            "expected the second compile_cached on a structurally-identical Expr to be a hit; \
             baseline hits={}, after hits={}",
            baseline.compile_cache_hits,
            after.compile_cache_hits,
        );
    }

    #[test]
    fn cache_key_partitions_every_binding_semantics_input() {
        let base = ExprProgramSemantics {
            div_precision_increment: 4,
            group_concat_max_len: 1_024,
            connection_collation_id: 255,
            connection_mbmaxlen: 4,
            no_unsigned_subtraction: false,
            parameter_types: 7,
        };
        let variants = [
            ExprProgramSemantics {
                div_precision_increment: 5,
                ..base
            },
            ExprProgramSemantics {
                group_concat_max_len: 2_048,
                ..base
            },
            ExprProgramSemantics {
                connection_collation_id: 63,
                ..base
            },
            ExprProgramSemantics {
                connection_mbmaxlen: 1,
                ..base
            },
            ExprProgramSemantics {
                no_unsigned_subtraction: true,
                ..base
            },
            ExprProgramSemantics {
                parameter_types: 8,
                ..base
            },
        ];
        for semantics in variants {
            assert_ne!(semantics, base);
        }
    }

    #[test]
    fn cached_warning_expression_replays_warning_on_every_execution() {
        let expr = Expr::Binary {
            left: Box::new(Expr::Literal(Literal::Int(7))),
            op: BinaryOp::Div,
            right: Box::new(Expr::Literal(Literal::Int(0))),
        };
        let schema = InputSchema::new(Vec::new());
        let first = ExprCompiler::compile_cached(&expr, &schema).unwrap();
        crate::engine::api::clear_statement_warnings();
        assert_eq!(eval_no_row(&first).unwrap(), Value::Null);
        assert_eq!(crate::engine::api::statement_warnings_snapshot().len(), 1);

        let second = ExprCompiler::compile_cached(&expr, &schema).unwrap();
        assert!(Arc::ptr_eq(&first, &second));
        crate::engine::api::clear_statement_warnings();
        assert_eq!(eval_no_row(&second).unwrap(), Value::Null);
        assert_eq!(crate::engine::api::statement_warnings_snapshot().len(), 1);
    }

    #[test]
    fn constant_in_list_compiles_to_hashable_set() {
        let schema = InputSchema::new(vec![InputColumn {
            table: "t".into(),
            alias: None,
            name: "b".into(),
            column_type: ColumnType::VarString,
            unsigned: false,
            collation_id: None,
        }]);
        let expr = Expr::InList {
            expr: Box::new(Expr::Function {
                name: "SUBSTRING".into(),
                args: vec![
                    Expr::Identifier("b".into()),
                    Expr::Literal(Literal::Int(1)),
                    Expr::Literal(Literal::Int(1)),
                ],
                distinct: false,
            }),
            list: ["a", "b", "c", "d"]
                .into_iter()
                .map(|s| Expr::Literal(Literal::String(s.into())))
                .collect(),
            negated: false,
        };
        let program = ExprCompiler::compile(&expr, &schema).expect("compile in-list");
        let ExprNode::InConstSet {
            expr,
            keys,
            hash_class,
            ..
        } = &program.root
        else {
            panic!("expected InConstSet, got {:?}", program.root);
        };
        assert_eq!(*hash_class, Some(InHashClass::String));
        assert_eq!(keys.len(), 4);
        assert!(matches!(expr.as_ref(), ExprNode::SubstringConst { .. }));

        let values = [Value::String("beta".into())];
        let row = RowView::from_values(&values);
        let value = with_eval_scratch(|scratch| program.eval_row(&row, scratch)).unwrap();
        assert_eq!(value, Value::Int(1));
    }

    #[test]
    fn integral_in_list_uses_exact_shared_hash_domain() {
        let schema = InputSchema::new(vec![InputColumn {
            table: "t".into(),
            alias: None,
            name: "k".into(),
            column_type: ColumnType::LongLong,
            unsigned: false,
            collation_id: None,
        }]);
        let expr = Expr::InList {
            expr: Box::new(Expr::Identifier("k".into())),
            list: vec![
                Expr::BoundValue(Value::Int(-1)),
                Expr::BoundValue(Value::UInt(2)),
                Expr::BoundValue(Value::Int(3)),
                Expr::BoundValue(Value::Null),
            ],
            negated: false,
        };
        let program = ExprCompiler::compile(&expr, &schema).expect("compile integer IN set");
        let (column, keys, has_null, negated) = program
            .as_column_integer_in_const_set()
            .expect("integer IN list should expose the exact batch hash domain");
        assert_eq!(column, 0);
        assert_eq!(keys.len(), 3);
        assert!(keys.contains(&ValueKey::Int(-1)));
        assert!(keys.contains(&ValueKey::Int(2)));
        assert!(has_null);
        assert!(!negated);

        let values = [Value::Int(2)];
        let row = RowView::from_values(&values);
        assert_eq!(
            with_eval_scratch(|scratch| program.eval_row(&row, scratch)).unwrap(),
            Value::Int(1)
        );
    }

    #[test]
    fn expr_fingerprint_is_deterministic_and_shape_sensitive() {
        // `expr_fingerprint` outputs a u64 used as a HashMap key in
        // the planner. Switching the underlying hasher (SipHash →
        // ahash) preserves this contract only if (a) repeat calls
        // on equal `Expr`s return the same u64 and (b) different
        // shapes get different u64s. The seeds are process-stable
        // by construction; this test guards the contract.
        let make = || Expr::Binary {
            left: Box::new(Expr::Identifier("x".into())),
            op: BinaryOp::Gt,
            right: Box::new(Expr::Literal(Literal::Int(10))),
        };
        let a = make();
        let b = make();
        let fp_a = expr_fingerprint(&a);
        let fp_b = expr_fingerprint(&b);
        assert_eq!(
            fp_a, fp_b,
            "structurally identical Exprs must produce the same fingerprint"
        );

        // Different column → different fingerprint.
        let other_col = Expr::Binary {
            left: Box::new(Expr::Identifier("y".into())),
            op: BinaryOp::Gt,
            right: Box::new(Expr::Literal(Literal::Int(10))),
        };
        assert_ne!(fp_a, expr_fingerprint(&other_col));

        // Different operator → different fingerprint.
        let other_op = Expr::Binary {
            left: Box::new(Expr::Identifier("x".into())),
            op: BinaryOp::Lt,
            right: Box::new(Expr::Literal(Literal::Int(10))),
        };
        assert_ne!(fp_a, expr_fingerprint(&other_op));

        // Different literal value → different fingerprint
        // (expr_fingerprint operates on the *raw* AST; auto-param's
        // placeholder substitution is what makes plan-cache keys
        // literal-insensitive, not this function).
        let other_lit = Expr::Binary {
            left: Box::new(Expr::Identifier("x".into())),
            op: BinaryOp::Gt,
            right: Box::new(Expr::Literal(Literal::Int(11))),
        };
        assert_ne!(fp_a, expr_fingerprint(&other_lit));
    }

    #[test]
    fn fingerprint_covers_window_frame_and_reparsed_subquery_structure() {
        let window_one = crate::parser::parse_expr_fragment(
            "SUM(x) OVER (ORDER BY x ROWS BETWEEN 1 PRECEDING AND CURRENT ROW)",
        )
        .unwrap();
        let window_two = crate::parser::parse_expr_fragment(
            "SUM(x) OVER (ORDER BY x ROWS BETWEEN 2 PRECEDING AND CURRENT ROW)",
        )
        .unwrap();
        assert_ne!(expr_fingerprint(&window_one), expr_fingerprint(&window_two));

        let sql = "EXISTS (SELECT 1 FROM t WHERE t.x = 7)";
        let first = crate::parser::parse_expr_fragment(sql).unwrap();
        let reparsed = crate::parser::parse_expr_fragment(sql).unwrap();
        assert_eq!(expr_fingerprint(&first), expr_fingerprint(&reparsed));
    }

    #[test]
    fn schema_fingerprint_is_deterministic_and_shape_sensitive() {
        // Same lever for `InputSchema::compute_fingerprint`, exposed
        // through `InputSchema::fingerprint()`.
        let s1 = InputSchema::new(vec![InputColumn {
            table: "t".into(),
            alias: None,
            name: "x".into(),
            column_type: ColumnType::LongLong,
            unsigned: false,
            collation_id: None,
        }]);
        let s1b = InputSchema::new(vec![InputColumn {
            table: "t".into(),
            alias: None,
            name: "x".into(),
            column_type: ColumnType::LongLong,
            unsigned: false,
            collation_id: None,
        }]);
        assert_eq!(s1.fingerprint(), s1b.fingerprint());

        let s2 = InputSchema::new(vec![InputColumn {
            table: "t".into(),
            alias: None,
            name: "y".into(),
            column_type: ColumnType::LongLong,
            unsigned: false,
            collation_id: None,
        }]);
        assert_ne!(s1.fingerprint(), s2.fingerprint());

        let s3 = InputSchema::new(vec![InputColumn {
            table: "t".into(),
            alias: None,
            name: "x".into(),
            column_type: ColumnType::VarString,
            unsigned: false,
            collation_id: None,
        }]);
        assert_ne!(s1.fingerprint(), s3.fingerprint());

        let unsigned = InputSchema::new(vec![InputColumn {
            table: "t".into(),
            alias: None,
            name: "x".into(),
            column_type: ColumnType::LongLong,
            unsigned: true,
            collation_id: None,
        }]);
        assert_ne!(s1.fingerprint(), unsigned.fingerprint());
    }

    fn like_const_program(pattern: &str, escape: Option<char>, negated: bool) -> ExprProgram {
        let schema = InputSchema::new(vec![InputColumn {
            table: "t".into(),
            alias: None,
            name: "s".into(),
            column_type: ColumnType::VarString,
            unsigned: false,
            collation_id: None,
        }]);
        let expr = Expr::Like {
            expr: Box::new(Expr::Identifier("s".into())),
            pattern: Box::new(Expr::Literal(Literal::String(pattern.to_string()))),
            negated,
            escape_char: escape,
        };
        ExprCompiler::compile(&expr, &schema).expect("compile LIKE")
    }

    fn run_like_const(program: &ExprProgram, value: &str) -> i64 {
        let values = [Value::String(value.into())];
        let row = RowView::from_values(&values);
        match with_eval_scratch(|scratch| program.eval_row(&row, scratch)).unwrap() {
            Value::Int(v) => v,
            other => panic!("LIKE eval must return Int, got {:?}", other),
        }
    }

    /// The default collation is CI, so ASCII no-escape patterns compile
    /// into `CiAscii` with the byte plan over the folded pattern inside.
    /// Pulls that inner plan out (and asserts the wrapper shape).
    fn unwrap_ci_plan(root: &ExprNode) -> &LikePlan {
        match root {
            ExprNode::LikeConst {
                plan: LikePlan::CiAscii { folded, .. },
                ..
            } => folded,
            other => panic!("expected CiAscii-wrapped LikeConst, got {:?}", other),
        }
    }

    #[test]
    fn like_cyrillic_rejection_matches_unicode_oracle_and_vector_boundaries() {
        let info = crate::charset::default_collation_info();
        assert_eq!(info.id, crate::charset::UTF8MB4_0900_AI_CI_ID);
        for scalar in 0x400..=0x47f {
            let ch = char::from_u32(scalar).unwrap();
            assert_eq!(
                crate::charset::like_char_ascii_weight_for_info(info, ch),
                None
            );
        }
        for length in [0, 1, 15, 16, 17, 31, 32, 33, 63, 64, 65, 127] {
            for suffix in [
                "",
                "текст",
                "GAMMA",
                "ĝamma",
                "ＧＡＭＭＡ",
                "K",
                "中文",
                "\u{483}",
            ] {
                let value = format!("{}{}", "Яx".repeat(length), suffix);
                let bytes = value.as_bytes();
                let expected_class = value
                    .chars()
                    .all(|ch| ch.is_ascii() || ('\u{400}'..='\u{47f}').contains(&ch));
                assert_eq!(ascii_or_cyrillic_utf8(bytes), expected_class);
                #[cfg(target_arch = "x86_64")]
                if std::arch::is_x86_feature_detected!("avx2") {
                    assert_eq!(
                        unsafe { ascii_or_cyrillic_utf8_avx2(bytes) },
                        expected_class
                    );
                }
                for pattern in ["%gamma%", "%k%", "%xyz%", "%xЯ%", "% %", "%_%"] {
                    let plan = classify_like_plan_public(pattern, None, Some(info));
                    let expected = crate::engine::expr::like_match_chars_with_info(
                        &value.chars().collect::<Vec<_>>(),
                        &pattern.chars().collect::<Vec<_>>(),
                        None,
                        info,
                    );
                    assert_eq!(
                        plan.matches_str(&value),
                        expected,
                        "{value:?} LIKE {pattern:?}"
                    );
                }
            }
        }
    }

    #[test]
    fn like_const_classifies_into_fast_paths() {
        // Each row asserts both the LikePlan variant chosen at compile
        // time and a couple of representative value matches.
        let prog = like_const_program("foo", None, false);
        match unwrap_ci_plan(&prog.root) {
            LikePlan::Literal(p) => assert_eq!(p, "foo"),
            other => panic!("expected Literal, got {:?}", other),
        }

        // Folding happens at classification time: an uppercase pattern
        // stores its lowercase form.
        let prog = like_const_program("FOO%", None, false);
        match unwrap_ci_plan(&prog.root) {
            LikePlan::Prefix(p) => assert_eq!(p, "foo"),
            other => panic!("expected Prefix, got {:?}", other),
        }

        let prog = like_const_program("%bar", None, false);
        match unwrap_ci_plan(&prog.root) {
            LikePlan::Suffix(p) => assert_eq!(p, "bar"),
            other => panic!("expected Suffix, got {:?}", other),
        }

        let prog = like_const_program("%mid%", None, false);
        match unwrap_ci_plan(&prog.root) {
            LikePlan::Contains(p) => assert_eq!(p, "mid"),
            other => panic!("expected Contains, got {:?}", other),
        }

        let prog = like_const_program("%", None, false);
        assert!(matches!(unwrap_ci_plan(&prog.root), LikePlan::WildcardAny));
        let prog = like_const_program("%%%", None, false);
        assert!(matches!(unwrap_ci_plan(&prog.root), LikePlan::WildcardAny));

        let prog = like_const_program("", None, false);
        match unwrap_ci_plan(&prog.root) {
            LikePlan::Literal(p) => assert!(p.is_empty()),
            other => panic!("expected Literal(\"\") for empty pattern, got {:?}", other),
        }

        // `_` is length-sensitive → General path inside the CI wrapper.
        let prog = like_const_program("_oo%", None, false);
        assert!(matches!(
            unwrap_ci_plan(&prog.root),
            LikePlan::General { .. }
        ));

        // `%` in the middle of literal → MultiSegment byte path
        // (ASCII, no `_`, no escape) — falls back to General only when
        // one of those preconditions is violated.
        let prog = like_const_program("a%b%c", None, false);
        match unwrap_ci_plan(&prog.root) {
            LikePlan::MultiSegment {
                anchored_start,
                anchored_end,
                reject_segment: _,
                segments,
            } => {
                assert!(*anchored_start);
                assert!(*anchored_end);
                let segs: Vec<&[u8]> = segments.iter().map(|s| s.bytes()).collect();
                assert_eq!(segs, vec![b"a".as_slice(), b"b", b"c"]);
            }
            other => panic!("expected MultiSegment, got {:?}", other),
        }

        // Non-ASCII pattern → CiGeneral (per-char collation NFA; bytes
        // can't pre-fold).
        let prog = like_const_program("é%中%c", None, false);
        assert!(matches!(
            prog.root,
            ExprNode::LikeConst {
                plan: LikePlan::CiGeneral { .. },
                ..
            }
        ));

        // An escape char that actually occurs in the pattern forces
        // char-NFA semantics even for ASCII…
        let prog = like_const_program("a\\%b%c", Some('\\'), false);
        assert!(matches!(
            prog.root,
            ExprNode::LikeConst {
                plan: LikePlan::CiGeneral { .. },
                ..
            }
        ));
        // …but an unused escape config (the `\` default) is normalized
        // away so the byte fast path stays available.
        let prog = like_const_program("a%b%c", Some('\\'), false);
        match unwrap_ci_plan(&prog.root) {
            LikePlan::MultiSegment { .. } => {}
            other => panic!("expected MultiSegment with unused escape, got {:?}", other),
        }

        // `%lit1%lit2%` (Q13-shape) → MultiSegment, both unanchored.
        let prog = like_const_program("%special%requests%", None, false);
        match unwrap_ci_plan(&prog.root) {
            LikePlan::MultiSegment {
                anchored_start,
                anchored_end,
                reject_segment,
                segments,
            } => {
                assert!(!*anchored_start);
                assert!(!*anchored_end);
                assert_eq!(*reject_segment, 1, "'q' is the rare rejection anchor");
                let segs: Vec<&[u8]> = segments.iter().map(|s| s.bytes()).collect();
                assert_eq!(segs, vec![b"special".as_slice(), b"requests"]);
            }
            other => panic!("expected MultiSegment, got {:?}", other),
        }
    }

    #[test]
    fn like_const_matches_legacy_match_like_pattern() {
        // Differential fixture: each (pattern, escape) is run against
        // a set of values; the compiled LikeConst result must equal the
        // legacy `match_like_pattern` result. This catches any
        // mismatch between byte-level fast paths and the char-NFA.
        let fixtures: &[(&str, Option<char>, &[&str])] = &[
            (
                "foo",
                None,
                &["foo", "foox", "xfoo", "FOO", "fo", "", "foofoo"],
            ),
            ("foo%", None, &["foo", "fooz", "foobar", "fo", "", "barfoo"]),
            ("%bar", None, &["bar", "abar", "barx", "", "barbar"]),
            ("%mid%", None, &["mid", "amidz", "midx", "", "no"]),
            (
                "%Google%",
                None,
                &[
                    "普通标题",
                    "普通 Google 标题",
                    "前缀 GÓÓGLE 后缀",
                    "Gøogle",
                    "Gßogle",
                    "完全无关的标题",
                ],
            ),
            ("Cafe%", None, &["Café noir", "CAFETERIA", "xCafé", "茶館"]),
            ("%Cafe", None, &["Le Café", "CAFÉ", "Café noir", "茶館"]),
            ("Cafe", None, &["Café", "CAFE", "Café noir", "茶館"]),
            ("%o%", None, &["ø", "Ö", "中", "foo"]),
            ("%s%", None, &["ß", "s", "Ś", "中"]),
            (
                "%",
                None,
                &["", "a", "abc", "very long string with many chars"],
            ),
            ("%%%", None, &["", "x"]),
            ("", None, &["", "x"]),
            ("_oo%", None, &["foo", "foox", "boo", "ooo", "fo", ""]),
            ("a%b%c", None, &["abc", "axbxc", "abxc", "abxxxc", "xx"]),
            // Escape: `a\%b` with escape='\' matches literal `a%b`,
            // not `a%anything b`.
            ("a\\%b", Some('\\'), &["a%b", "aXb", "ab", "a%xb", "a%%b"]),
            // Escape='!' lets the matcher see `!_` as literal `_`.
            ("!_!_", Some('!'), &["__", "_x", "ab", ""]),
            // Trailing wildcard with `_`: still General.
            ("a_b%", None, &["axb", "axbcd", "ab", "axxb"]),
            // Unicode literal — exercises char decoding consistency.
            ("café%", None, &["café", "café au lait", "caf", ""]),
        ];

        for (pattern, escape, values) in fixtures {
            let prog = like_const_program(pattern, *escape, false);
            for value in *values {
                let compiled = run_like_const(&prog, value);
                // The legacy oracle for string operands is the
                // default-collation matcher (`eval_like_values` routes
                // non-binary values through it).
                let legacy =
                    crate::engine::expr::match_like_pattern_default(value, pattern, *escape) as i64;
                assert_eq!(
                    compiled, legacy,
                    "LIKE mismatch: value={:?} pattern={:?} escape={:?} (compiled={} legacy={})",
                    value, pattern, escape, compiled, legacy,
                );
            }
        }
    }

    #[test]
    fn like_const_negated_inverts_match() {
        let prog = like_const_program("foo", None, true);
        assert_eq!(run_like_const(&prog, "foo"), 0);
        assert_eq!(run_like_const(&prog, "bar"), 1);

        let prog = like_const_program("foo%", None, true);
        assert_eq!(run_like_const(&prog, "foobar"), 0);
        assert_eq!(run_like_const(&prog, "barfoo"), 1);
    }

    #[test]
    fn like_const_null_pattern_treated_as_literal_null() {
        // MySQL ternary logic: `x LIKE NULL` is NULL for every row.
        // (The historical quirk matched the literal string "NULL".)
        let schema = InputSchema::new(vec![InputColumn {
            table: "t".into(),
            alias: None,
            name: "s".into(),
            column_type: ColumnType::VarString,
            unsigned: false,
            collation_id: None,
        }]);
        let expr = Expr::Like {
            expr: Box::new(Expr::Identifier("s".into())),
            pattern: Box::new(Expr::Literal(Literal::Null)),
            negated: false,
            escape_char: None,
        };
        let prog = ExprCompiler::compile(&expr, &schema).expect("compile");
        assert!(
            matches!(prog.root, ExprNode::Const(Value::Null)),
            "NULL pattern folds the whole predicate to NULL, got {:?}",
            prog.root
        );
        let values = [Value::String("NULL".into())];
        let row = RowView::from_values(&values);
        assert_eq!(
            with_eval_scratch(|scratch| prog.eval_row(&row, scratch)).unwrap(),
            Value::Null
        );
    }

    fn compile_with_int_schema(expr: Expr) -> ExprProgram {
        let schema = InputSchema::new(vec![InputColumn {
            table: "t".into(),
            alias: None,
            name: "x".into(),
            column_type: ColumnType::LongLong,
            unsigned: false,
            collation_id: None,
        }]);
        ExprCompiler::compile(&expr, &schema).expect("compile")
    }

    #[test]
    fn fold_binary_all_const_to_const() {
        // `1 + 2` → Const(3).
        let prog = compile_with_int_schema(Expr::Binary {
            left: Box::new(Expr::Literal(Literal::Int(1))),
            op: BinaryOp::Add,
            right: Box::new(Expr::Literal(Literal::Int(2))),
        });
        match &prog.root {
            ExprNode::Const(Value::Int(3)) => {}
            other => panic!("expected Const(Int(3)), got {:?}", other),
        }
    }

    #[test]
    fn fold_preserves_column_subtree() {
        // `x + 1` must stay a Binary; column eval is per-row.
        let prog = compile_with_int_schema(Expr::Binary {
            left: Box::new(Expr::Identifier("x".into())),
            op: BinaryOp::Add,
            right: Box::new(Expr::Literal(Literal::Int(1))),
        });
        assert!(
            matches!(prog.root, ExprNode::Binary { .. }),
            "binary with a column child must not fold, got {:?}",
            prog.root,
        );
    }

    #[test]
    fn fold_div_by_zero_keeps_warning_producing_tree() {
        // `1 / 0` returns NULL *and* emits a statement warning. Folding it to
        // a Const would emit the warning at compile time (or lose it on cache
        // hits), so warning-producing constants must stay executable.
        crate::engine::api::clear_statement_warnings();
        let prog = compile_with_int_schema(Expr::Binary {
            left: Box::new(Expr::Literal(Literal::Int(1))),
            op: BinaryOp::Div,
            right: Box::new(Expr::Literal(Literal::Int(0))),
        });
        assert!(matches!(prog.root, ExprNode::Binary { .. }));
        assert!(
            crate::engine::api::statement_warnings_snapshot().is_empty(),
            "speculative folding must not leak diagnostics"
        );
        let value = eval_no_row(&prog).expect("runtime division");
        assert_eq!(value, Value::Null);
        assert_eq!(crate::engine::api::statement_warnings_snapshot().len(), 1);
    }

    #[test]
    fn fuzz_round1_comparison_constant_diagnostics_survive_repeated_execution() {
        for sql in [
            "1 = (1 / 0)",
            "(1 / 0) < 2",
            "1 = (1 % 0)",
            "(1 / 0) = (2 / 0)",
            "COALESCE(1, 1 / 0) = 1",
        ] {
            let expr = crate::parser::parse_expr_fragment(sql).unwrap();
            crate::engine::api::clear_statement_warnings();
            let prog = compile_with_int_schema(expr);
            assert!(
                crate::engine::api::statement_warnings_snapshot().is_empty(),
                "compile leaked: {sql}"
            );
            let expected = if sql.contains("COALESCE") {
                0
            } else if sql.starts_with("(1 / 0) =") {
                2
            } else {
                1
            };
            for _ in 0..3 {
                crate::engine::api::clear_statement_warnings();
                eval_no_row(&prog).unwrap();
                assert_eq!(
                    crate::engine::api::statement_warnings_snapshot().len(),
                    expected,
                    "{sql}"
                );
            }
        }
        crate::engine::api::clear_statement_warnings();
    }

    #[test]
    fn fold_integer_overflow_keeps_tree_for_runtime_error() {
        // `i64::MAX + 1` overflows and `try_integer_arithmetic` returns
        // MySQL error 1690. Fold must preserve the tree so the runtime
        // raises that error on actual rows (rather than at compile).
        let prog = compile_with_int_schema(Expr::Binary {
            left: Box::new(Expr::Literal(Literal::Int(i64::MAX))),
            op: BinaryOp::Add,
            right: Box::new(Expr::Literal(Literal::Int(1))),
        });
        assert!(
            matches!(prog.root, ExprNode::Binary { .. }),
            "overflow must not silently collapse to Const, got {:?}",
            prog.root,
        );
    }

    #[test]
    fn fold_placeholder_subtree_preserved() {
        // `? + 1` — placeholder is bound per EXECUTE, must not fold.
        let prog = compile_with_int_schema(Expr::Binary {
            left: Box::new(Expr::Placeholder(0)),
            op: BinaryOp::Add,
            right: Box::new(Expr::Literal(Literal::Int(1))),
        });
        assert!(
            matches!(prog.root, ExprNode::Binary { .. }),
            "binary containing a placeholder must not fold, got {:?}",
            prog.root,
        );
    }

    #[test]
    fn fold_is_null_const_to_const() {
        let prog = compile_with_int_schema(Expr::IsNull(Box::new(Expr::Literal(Literal::Null))));
        assert!(matches!(prog.root, ExprNode::Const(Value::Int(1))));
        let prog = compile_with_int_schema(Expr::IsNull(Box::new(Expr::Literal(Literal::Int(7)))));
        assert!(matches!(prog.root, ExprNode::Const(Value::Int(0))));

        let prog =
            compile_with_int_schema(Expr::IsNotNull(Box::new(Expr::Literal(Literal::Int(7)))));
        assert!(matches!(prog.root, ExprNode::Const(Value::Int(1))));
    }

    #[test]
    fn fold_between_all_const_to_const() {
        // `5 BETWEEN 1 AND 10` → 1; `5 BETWEEN 10 AND 100` → 0.
        let in_range = compile_with_int_schema(Expr::Between {
            expr: Box::new(Expr::Literal(Literal::Int(5))),
            low: Box::new(Expr::Literal(Literal::Int(1))),
            high: Box::new(Expr::Literal(Literal::Int(10))),
            negated: false,
        });
        assert!(matches!(in_range.root, ExprNode::Const(Value::Int(1))));

        let out_range = compile_with_int_schema(Expr::Between {
            expr: Box::new(Expr::Literal(Literal::Int(5))),
            low: Box::new(Expr::Literal(Literal::Int(10))),
            high: Box::new(Expr::Literal(Literal::Int(100))),
            negated: false,
        });
        assert!(matches!(out_range.root, ExprNode::Const(Value::Int(0))));
    }

    #[test]
    fn fold_deterministic_function_with_const_args() {
        // `LENGTH('abc') + 1` → 4. Verifies the Function arm folds when
        // its kernel is deterministic and all args are Const, and then
        // the surrounding Binary picks up the result and folds further.
        let prog = compile_with_int_schema(Expr::Binary {
            left: Box::new(Expr::Function {
                name: "LENGTH".into(),
                args: vec![Expr::Literal(Literal::String("abc".into()))],
                distinct: false,
            }),
            op: BinaryOp::Add,
            right: Box::new(Expr::Literal(Literal::Int(1))),
        });
        match &prog.root {
            ExprNode::Const(Value::Int(4)) => {}
            other => panic!("expected Const(Int(4)), got {:?}", other),
        }
    }

    #[test]
    fn strcmp_keeps_string_domain_for_numeric_arguments() {
        let expr = Expr::Function {
            name: "STRCMP".into(),
            args: vec![
                Expr::Literal(Literal::Int(10)),
                Expr::Literal(Literal::Int(2)),
            ],
            distinct: false,
        };
        let program = ExprCompiler::compile(&expr, &InputSchema::new(Vec::new())).unwrap();
        assert_eq!(eval_no_row(&program).unwrap(), Value::Int(-1));
    }

    #[test]
    fn fold_case_when_const_to_const() {
        // `CASE WHEN 1=1 THEN 'a' ELSE 'b' END` → Const('a').
        let prog = compile_with_int_schema(Expr::Case {
            operand: None,
            conditions: vec![Expr::Binary {
                left: Box::new(Expr::Literal(Literal::Int(1))),
                op: BinaryOp::Eq,
                right: Box::new(Expr::Literal(Literal::Int(1))),
            }]
            .into_boxed_slice(),
            results: vec![Expr::Literal(Literal::String("a".into()))],
            else_result: Some(Box::new(Expr::Literal(Literal::String("b".into())))),
        });
        match &prog.root {
            ExprNode::Const(Value::String(s)) if s == "a" => {}
            other => panic!("expected Const(String('a')), got {:?}", other),
        }
    }

    #[test]
    fn fold_like_const_pattern_to_bool_const() {
        // `'foobar' LIKE 'foo%'` should collapse to Const(1) since both
        // sides are constant and LIKE is deterministic.
        let schema = InputSchema::new(vec![InputColumn {
            table: "t".into(),
            alias: None,
            name: "x".into(),
            column_type: ColumnType::LongLong,
            unsigned: false,
            collation_id: None,
        }]);
        let prog = ExprCompiler::compile(
            &Expr::Like {
                expr: Box::new(Expr::Literal(Literal::String("foobar".into()))),
                pattern: Box::new(Expr::Literal(Literal::String("foo%".into()))),
                negated: false,
                escape_char: None,
            },
            &schema,
        )
        .expect("compile");
        match &prog.root {
            ExprNode::Const(Value::Int(1)) => {}
            other => panic!("expected Const(Int(1)) for matching LIKE, got {:?}", other),
        }
    }

    #[test]
    fn fold_promotes_like_with_folded_const_pattern_to_like_const() {
        // `x LIKE CONCAT('foo', '%')` — at compile_node time the pattern
        // is a Function call, so the Like arm cannot pre-classify. After
        // fold_constants resolves the pattern to Const("foo%"), the fold
        // pass promotes the Like into a LikeConst (Prefix).
        let schema = InputSchema::new(vec![InputColumn {
            table: "t".into(),
            alias: None,
            name: "x".into(),
            column_type: ColumnType::VarString,
            unsigned: false,
            collation_id: None,
        }]);
        let prog = ExprCompiler::compile(
            &Expr::Like {
                expr: Box::new(Expr::Identifier("x".into())),
                pattern: Box::new(Expr::Function {
                    name: "CONCAT".into(),
                    args: vec![
                        Expr::Literal(Literal::String("foo".into())),
                        Expr::Literal(Literal::String("%".into())),
                    ],
                    distinct: false,
                }),
                negated: false,
                escape_char: None,
            },
            &schema,
        )
        .expect("compile");
        match unwrap_ci_plan(&prog.root) {
            LikePlan::Prefix(p) if p == "foo" => {}
            other => panic!(
                "expected CiAscii(Prefix(\"foo\")) after folding, got {:?}",
                other
            ),
        }
    }

    fn compile_session_function(name: &str, args: Vec<Expr>) -> ExprProgram {
        let schema = InputSchema::new(vec![InputColumn {
            table: "t".into(),
            alias: None,
            name: "x".into(),
            column_type: ColumnType::LongLong,
            unsigned: false,
            collation_id: None,
        }]);
        let expr = Expr::Function {
            name: name.into(),
            args,
            distinct: false,
        };
        ExprCompiler::compile(&expr, &schema).expect("compile session function")
    }

    fn eval_no_row(prog: &ExprProgram) -> Result<Value, QueryError> {
        let values: [Value; 0] = [];
        let row = RowView::from_values(&values);
        with_eval_scratch(|scratch| prog.eval_row(&row, scratch))
    }

    #[test]
    fn temporal_kernels_match_mysql_reference_values() {
        fn lit_s(s: &str) -> Expr {
            Expr::Literal(Literal::String(s.into()))
        }
        fn lit_i(i: i64) -> Expr {
            Expr::Literal(Literal::Int(i))
        }
        // Reference outputs cross-checked against MySQL 8.0. The same
        // inputs flow through the legacy interpreter in the SQL-level
        // suite (`mysql80_compat_temporal.rs` / `mysql80_functions.rs`),
        // so a kernel/legacy divergence fails one of the two.
        let cases: Vec<(&str, Vec<Expr>, Value)> = vec![
            (
                "DATEDIFF",
                vec![lit_s("2024-02-01"), lit_s("2024-01-01")],
                Value::Int(31),
            ),
            ("QUARTER", vec![lit_s("2024-05-01")], Value::Int(2)),
            ("DAYOFWEEK", vec![lit_s("2024-01-15")], Value::Int(2)),
            ("WEEKDAY", vec![lit_s("2024-01-15")], Value::Int(0)),
            ("DAYOFYEAR", vec![lit_s("2024-02-01")], Value::Int(32)),
            (
                "LAST_DAY",
                vec![lit_s("2024-02-10")],
                Value::Date(
                    (chrono::NaiveDate::from_ymd_opt(2024, 2, 29).unwrap()
                        - chrono::NaiveDate::from_ymd_opt(1970, 1, 1).unwrap())
                    .num_days() as i32,
                ),
            ),
            ("TO_DAYS", vec![lit_s("2000-01-01")], Value::Int(730_485)),
            (
                "MICROSECOND",
                vec![lit_s("2024-01-01 10:00:00.123456")],
                Value::Int(123_456),
            ),
            ("WEEKOFYEAR", vec![lit_s("2024-01-15")], Value::Int(3)),
            ("WEEK", vec![lit_s("2024-01-15"), lit_i(3)], Value::Int(3)),
            (
                "DATE_FORMAT",
                vec![lit_s("2024-01-15"), lit_s("%Y/%m %W")],
                Value::String("2024/01 Monday".into()),
            ),
            // Zero dates through the kernels: extraction → 0 / NULL.
            ("QUARTER", vec![lit_s("0000-00-00")], Value::Int(0)),
            ("DAYOFWEEK", vec![lit_s("0000-00-00")], Value::Null),
            ("TO_DAYS", vec![lit_s("0000-00-00")], Value::Null),
        ];
        for (name, args, expected) in cases {
            let prog = compile_session_function(name, args);
            assert_eq!(eval_no_row(&prog).unwrap(), expected, "{name}");
        }

        // DATE_ADD / DATE_SUB with the parser's __INTERVAL marker.
        let interval = |amount: i64, unit: &str| Expr::Function {
            name: "__INTERVAL".into(),
            args: vec![lit_i(amount), lit_s(unit)],
            distinct: false,
        };
        let prog =
            compile_session_function("DATE_ADD", vec![lit_s("2024-01-31"), interval(1, "MONTH")]);
        assert_eq!(
            eval_no_row(&prog).unwrap(),
            Value::String("2024-02-29".into())
        );
        let prog =
            compile_session_function("DATE_SUB", vec![lit_s("2024-03-01"), interval(1, "DAY")]);
        assert_eq!(
            eval_no_row(&prog).unwrap(),
            Value::String("2024-02-29".into())
        );
        let prog =
            compile_session_function("DATE_ADD", vec![lit_s("0000-00-00"), interval(1, "DAY")]);
        assert_eq!(eval_no_row(&prog).unwrap(), Value::Null);

        // UNIX_TIMESTAMP is intentionally source-sensitive: its no-argument
        // form reads the session clock, while typed arguments need their
        // catalog source type. The program compiler must defer it to the
        // legacy correctness oracle instead of erasing that provenance.
        let schema = InputSchema::new(vec![]);
        let expr = Expr::Function {
            name: "UNIX_TIMESTAMP".into(),
            args: vec![],
            distinct: false,
        };
        assert!(matches!(
            ExprCompiler::compile(&expr, &schema),
            Err(CompileError::Unsupported)
        ));
    }

    #[test]
    fn week_and_date_format_are_not_folded_to_const() {
        // WEEK(date) reads default_week_format and DATE_FORMAT reads
        // lc_time_names — both session state, so constant folding must
        // leave the Function node intact.
        let prog = compile_session_function(
            "WEEK",
            vec![Expr::Literal(Literal::String("2024-01-15".into()))],
        );
        assert!(matches!(prog.root, ExprNode::Function { .. }));
        let prog = compile_session_function(
            "DATE_FORMAT",
            vec![
                Expr::Literal(Literal::String("2024-01-15".into())),
                Expr::Literal(Literal::String("%W".into())),
            ],
        );
        assert!(matches!(prog.root, ExprNode::Function { .. }));
    }

    #[test]
    fn session_functions_resolve_into_kernel_not_legacy() {
        // Each of these used to compile to CompileError::Unsupported and
        // make the whole expression tree fall back to the legacy
        // interpreter. With the new kernels they stay on the compiled
        // path; assert each one materialises as ExprNode::Function.
        for name in &[
            "NOW",
            "CURRENT_TIMESTAMP",
            "CURDATE",
            "CURRENT_DATE",
            "CURTIME",
            "CURRENT_TIME",
            "UTC_TIMESTAMP",
            "UTC_DATE",
            "UTC_TIME",
            "VERSION",
            "DATABASE",
            "SCHEMA",
            "USER",
            "SESSION_USER",
            "CURRENT_USER",
            "CONNECTION_ID",
            "FOUND_ROWS",
            "ROW_COUNT",
        ] {
            let prog = compile_session_function(name, vec![]);
            assert!(
                matches!(prog.root, ExprNode::Function { .. }),
                "{name}() should compile to ExprNode::Function, got {:?}",
                prog.root,
            );
        }
    }

    #[test]
    fn session_functions_are_not_folded_to_const() {
        // Critical: fold_constants must leave non-deterministic kernels
        // alone, otherwise every EXECUTE would return the same NOW().
        // All-const args plus is_deterministic()==false should keep the
        // Function node intact.
        for name in &["NOW", "CURDATE", "DATABASE", "CONNECTION_ID", "VERSION"] {
            let prog = compile_session_function(name, vec![]);
            assert!(
                matches!(prog.root, ExprNode::Function { .. }),
                "{name}() must not be folded to Const, got {:?}",
                prog.root,
            );
        }
        // NOW(3) is also const-args (Int(3)) but Now is non-deterministic.
        let prog = compile_session_function("NOW", vec![Expr::Literal(Literal::Int(3))]);
        assert!(matches!(prog.root, ExprNode::Function { .. }));
    }

    #[test]
    fn now_returns_datetime_value() {
        // We don't pin the timestamp — just assert the native temporal value
        // and requested fractional-second precision.
        let prog = compile_session_function("NOW", vec![]);
        let value = eval_no_row(&prog).expect("NOW eval");
        assert!(matches!(value, Value::DateTime(_)));

        // NOW(3) rounds/truncates the microsecond payload to milliseconds.
        let prog = compile_session_function("NOW", vec![Expr::Literal(Literal::Int(3))]);
        let value = eval_no_row(&prog).expect("NOW(3) eval");
        match value {
            Value::DateTime(micros) => assert_eq!(micros % 1_000, 0),
            other => panic!("NOW(3) must return DateTime, got {:?}", other),
        }
    }

    #[test]
    fn now_invalid_fsp_returns_mysql_error() {
        // NOW(7) is out of MySQL's 0..=6 fsp range; the kernel must
        // surface error 1426 just like the legacy path does.
        let prog = compile_session_function("NOW", vec![Expr::Literal(Literal::Int(7))]);
        let err = eval_no_row(&prog).expect_err("NOW(7) must error");
        assert_eq!(err.code(), 1426, "expected MySQL error 1426, got {:?}", err);
    }

    #[test]
    fn curdate_with_extra_args_returns_arg_count_error() {
        // CURDATE() must take no args; passing one should produce
        // MySQL error 1582 ("incorrect parameter count").
        let prog = compile_session_function("CURDATE", vec![Expr::Literal(Literal::Int(1))]);
        let err = eval_no_row(&prog).expect_err("CURDATE(1) must error");
        assert_eq!(err.code(), 1582, "expected MySQL error 1582, got {:?}", err);
    }

    #[test]
    fn database_without_session_returns_null() {
        // No SESSION_EXPR_INFO bound on this thread, so DATABASE()
        // should evaluate to NULL — same as the legacy semantics in
        // expr.rs.
        let prog = compile_session_function("DATABASE", vec![]);
        let value = eval_no_row(&prog).expect("DATABASE eval");
        assert!(
            matches!(value, Value::Null),
            "DATABASE() with no session must return NULL, got {:?}",
            value
        );
    }

    #[test]
    fn connection_id_returns_unsigned_int() {
        // Client drivers (JDBC, mysqlclient) read this as an unsigned
        // bigint; keep the type contract intact.
        let prog = compile_session_function("CONNECTION_ID", vec![]);
        let value = eval_no_row(&prog).expect("CONNECTION_ID eval");
        assert!(
            matches!(value, Value::UInt(_)),
            "CONNECTION_ID() must return UInt, got {:?}",
            value
        );
    }

    #[test]
    fn version_kernel_matches_legacy() {
        let prog = compile_session_function("VERSION", vec![]);
        let value = eval_no_row(&prog).expect("VERSION eval");
        assert_eq!(
            value,
            Value::String(crate::server_version::MYSQL_COMPAT_VERSION.to_string())
        );
    }

    #[test]
    fn distinct_collation_key_is_native_and_schema_bound() {
        let collation_id = crate::charset::resolve_collation("utf8mb4_0900_ai_ci")
            .unwrap()
            .id;
        let schema = InputSchema::new(vec![InputColumn {
            table: "t".into(),
            alias: None,
            name: "v".into(),
            column_type: ColumnType::VarString,
            unsigned: false,
            collation_id: Some(collation_id),
        }]);
        let expr = Expr::Function {
            name: "__DISTINCT_COLLATION_KEY".into(),
            args: vec![Expr::Identifier("v".into())],
            distinct: false,
        };
        let before = expr_program_stats().legacy_escape_rows;
        let program = ExprCompiler::compile(&expr, &schema).expect("distinct key compiles");
        assert!(matches!(
            program.root,
            ExprNode::Function {
                kernel: FunctionKernel::DistinctCollationKey,
                collation_id: Some(id),
                ..
            } if id == collation_id
        ));

        let evaluate = |value: Value| {
            let values = [value];
            let row = RowView::from_values(&values);
            with_eval_scratch(|scratch| program.eval_row(&row, scratch)).unwrap()
        };
        assert_eq!(
            evaluate(Value::String("A".into())),
            evaluate(Value::String("á".into()))
        );
        assert_eq!(expr_program_stats().legacy_escape_rows, before);
    }
}

/// Phase 1 (expr-engine-improvement): sub-expression `LegacyEscape`. An
/// expression containing one function the compiler can't lower keeps the
/// rest of the tree compiled and drops only that sub-tree to the legacy
/// interpreter — verified here against the legacy whole-expression oracle.
#[cfg(test)]
mod legacy_escape_tests {
    use super::*;
    use crate::parser::{BinaryOp, Expr, Literal};
    use crate::types::{Column, ColumnType};

    fn input_schema(cols: &[(&str, ColumnType)]) -> InputSchema {
        InputSchema::new(
            cols.iter()
                .map(|(name, ty)| InputColumn {
                    table: "t".into(),
                    alias: None,
                    name: (*name).into(),
                    column_type: ty.clone(),
                    unsigned: false,
                    collation_id: None,
                })
                .collect(),
        )
    }

    fn oracle_table(cols: &[(&str, ColumnType)]) -> TableSchema {
        let columns: Vec<Column> = cols
            .iter()
            .map(|(name, ty)| Column {
                name: (*name).into(),
                column_type: ty.clone(),
                source: Default::default(),
            })
            .collect();
        let mut t = TableSchema::synthetic_for_planning("t", &columns);
        t.build_column_index();
        t
    }

    #[test]
    fn fuzz_round1_conditional_unsigned_signed_coercion() {
        let mut table = oracle_table(&[("u", ColumnType::LongLong), ("i", ColumnType::LongLong)]);
        table.columns[0].unsigned = true;
        let schema = InputSchema::from_table(&table);
        for sql in [
            "COALESCE(u, i)",
            "COALESCE(i, u)",
            "IFNULL(u, i)",
            "IFNULL(i, u)",
            "IF(1, u, i)",
            "IF(0, u, i)",
            "CASE WHEN 1 THEN u ELSE i END",
            "CASE WHEN 0 THEN u ELSE i END",
            "COALESCE(0xffffffffffffffff, NOT 10)",
        ] {
            let expr = crate::parser::parse_expr_fragment(sql).unwrap();
            let program = ExprCompiler::compile(&expr, &schema).unwrap();
            if !matches!(program.root, ExprNode::Const(_)) {
                assert!(
                    program.capability.requires_ordered_evaluation(),
                    "{sql} must preserve conversion diagnostics"
                );
            }
            for values in [
                [Value::UInt(10), Value::Int(-1)],
                [Value::UInt(u64::MAX), Value::Int(i64::MIN)],
                [Value::Null, Value::Int(-1)],
                [Value::UInt(u64::MAX), Value::Null],
            ] {
                let row = RowView::from_values(&values);
                let compiled = with_eval_scratch(|scratch| program.eval_row(&row, scratch));
                let legacy = eval_expr_legacy(&expr, &table, &values);
                assert_eq!(compiled, legacy, "{sql} / {values:?}");
            }
            if matches!(expr, Expr::Function { ref name, .. } if name != "COALESCE" || !sql.contains("0x"))
            {
                assert!(
                    matches!(
                        program.root,
                        ExprNode::Function {
                            result_coercion: Some(_),
                            ..
                        }
                    ),
                    "{sql} must retain a compiled kernel"
                );
            }
        }
    }

    #[test]
    fn fuzz_round1_numeric_function_bound_result_type() {
        let table = oracle_table(&[("i", ColumnType::LongLong)]);
        let schema = InputSchema::from_table(&table);
        for name in ["ABS", "CEIL", "FLOOR"] {
            let expr = crate::parser::parse_expr_fragment(&format!("{name}(i + COALESCE(1, 'x'))"))
                .unwrap();
            let program = ExprCompiler::compile(&expr, &schema).unwrap();
            for value in [Value::Int(-2), Value::Int(0), Value::Int(7), Value::Null] {
                let values = [value];
                let row = RowView::from_values(&values);
                let result = with_eval_scratch(|scratch| program.eval_row(&row, scratch));
                assert_eq!(result, eval_expr_legacy(&expr, &table, &values), "{name}");
            }
        }
    }

    #[test]
    fn fuzz_round1_temporal_constant_binding_keeps_diagnostics() {
        for (column_type, value) in [
            (ColumnType::Date, Value::Date(0)),
            (ColumnType::DateTime, Value::DateTime(0)),
        ] {
            let table = oracle_table(&[("d", column_type)]);
            let schema = InputSchema::from_table(&table);
            for sql in ["d = (1 / 0)", "(1 / 0) = d", "d = COALESCE(1 / 0, NULL)"] {
                let expr = crate::parser::parse_expr_fragment(sql).unwrap();
                crate::engine::api::clear_statement_warnings();
                let program = ExprCompiler::compile(&expr, &schema).unwrap();
                assert!(
                    crate::engine::api::statement_warnings_snapshot().is_empty(),
                    "{sql}"
                );
                for _ in 0..3 {
                    crate::engine::api::clear_statement_warnings();
                    let values = [value.clone()];
                    let row = RowView::from_values(&values);
                    assert_eq!(
                        with_eval_scratch(|scratch| program.eval_row(&row, scratch)).unwrap(),
                        Value::Null
                    );
                    assert_eq!(
                        crate::engine::api::statement_warnings_snapshot().len(),
                        1,
                        "{sql}"
                    );
                }
            }
        }
        crate::engine::api::clear_statement_warnings();
    }

    #[test]
    fn fuzz_round1_string_family_warning_parity() {
        use crate::storage::DataTypeKind;
        for (data_type, binary, silent) in [
            (DataTypeKind::Text { max_len: 65535 }, false, true),
            (DataTypeKind::Blob { max_len: 65535 }, true, true),
            (DataTypeKind::VarChar { len: Some(40) }, false, false),
            (DataTypeKind::Char { len: Some(40) }, false, false),
            (DataTypeKind::VarBinary { len: Some(40) }, true, false),
            (DataTypeKind::Binary { len: Some(40) }, true, false),
        ] {
            let mut table = oracle_table(&[("s", ColumnType::VarString)]);
            table.columns[0].data_type = data_type;
            let schema = InputSchema::from_table(&table);
            for sql in ["-s", "s - s", "ABS(s)", "s = 10", "COALESCE(-s, 0)"] {
                let expr = crate::parser::parse_expr_fragment(sql).unwrap();
                let program = ExprCompiler::compile(&expr, &schema).unwrap();
                let values = [if binary {
                    Value::Bytes(b"Ab 6 ".to_vec())
                } else {
                    Value::String("Ab 6 ".into())
                }];
                let row = RowView::from_values(&values);
                crate::engine::api::clear_statement_warnings();
                let compiled = with_eval_scratch(|scratch| program.eval_row(&row, scratch));
                let compiled_warnings = crate::engine::api::statement_warnings_snapshot();
                crate::engine::api::clear_statement_warnings();
                let legacy = eval_expr_legacy(&expr, &table, &values);
                let legacy_warnings = crate::engine::api::statement_warnings_snapshot();
                assert_eq!(compiled, legacy, "{sql} / {:?}", table.columns[0].data_type);
                assert_eq!(
                    compiled_warnings, legacy_warnings,
                    "{sql} / {:?}",
                    table.columns[0].data_type
                );
                assert_eq!(
                    compiled_warnings.is_empty(),
                    silent,
                    "{sql} / {:?}",
                    table.columns[0].data_type
                );
            }
        }
        crate::engine::api::clear_statement_warnings();
    }

    #[test]
    fn table_input_fingerprint_matches_materialized_schema() {
        // The cheap, allocation-free `table_input_fingerprint` used by
        // `compile_once`'s cache probe must be byte-for-byte identical to the
        // fingerprint of a fully materialised `from_table` schema, otherwise
        // the probe key and the stored entry key diverge and the program
        // cache silently stops hitting.
        for cols in [
            vec![
                ("Id", ColumnType::LongLong),
                ("Name", ColumnType::VarString),
                ("qTY", ColumnType::LongLong),
            ],
            vec![("a", ColumnType::LongLong)],
            vec![
                ("MixEdCase", ColumnType::VarString),
                ("MixEdCase", ColumnType::LongLong),
            ],
        ] {
            let table = oracle_table(&cols);
            let materialized = InputSchema::from_table(&table);
            let recomputed = materialized.compute_fingerprint();
            assert_eq!(
                table_input_fingerprint(&table),
                recomputed,
                "cheap fingerprint must equal materialized compute_fingerprint"
            );
            assert_eq!(
                InputSchema::from_table(&table).fingerprint(),
                recomputed,
                "from_table must keep the materialized fingerprint"
            );
        }
    }

    /// Compile `expr`, eval the compiled program, and assert it equals the
    /// legacy whole-expression oracle on the same row.
    fn assert_parity(expr: &Expr, cols: &[(&str, ColumnType)], values: Vec<Value>) -> ExprProgram {
        let schema = input_schema(cols);
        let program = ExprCompiler::compile(expr, &schema).expect("expr should compile via escape");
        let row = RowView::from_values(&values);
        let compiled = with_eval_scratch(|scratch| program.eval_row(&row, scratch))
            .map_err(|e| e.message().to_string());
        let table = oracle_table(cols);
        let legacy = eval_expr_legacy(expr, &table, &values).map_err(|e| e.message().to_string());
        assert_eq!(
            compiled, legacy,
            "compiled-with-escape diverged from legacy oracle for {expr:?}"
        );
        program
    }

    #[test]
    fn refactor_edge_trim_preserves_binary_conversion_errors() {
        for name in ["LTRIM", "RTRIM"] {
            let expression = Expr::Function {
                name: name.into(),
                args: vec![Expr::Identifier("value".into())],
                distinct: false,
            };
            for value in [
                Value::Null,
                Value::String("  \t汉字\u{a0}  ".into()),
                Value::Bytes(vec![b' ', 0xff, b' ']),
                Value::Bytes(b"  text  ".to_vec()),
            ] {
                assert_parity(&expression, &[("value", ColumnType::Blob)], vec![value]);
            }
        }
    }

    #[test]
    fn concat_borrowed_operands_match_legacy() {
        let columns = [
            ("separator", ColumnType::VarString),
            ("a", ColumnType::VarString),
            ("b", ColumnType::Blob),
            ("doc", ColumnType::Json),
        ];
        let docs = [
            Value::Null,
            Value::Json(
                crate::engine::json::parse_text(
                    r#"{"a":"中🙂\n","n":1.25,"z":[true,null]}"#,
                    1,
                    "test",
                )
                .unwrap(),
            ),
        ];
        for name in ["CONCAT", "CONCAT_WS"] {
            let expr = Expr::Function {
                name: name.into(),
                args: columns
                    .iter()
                    .map(|(name, _)| Expr::Identifier((*name).into()))
                    .collect(),
                distinct: false,
            };
            for separator in [
                Value::Null,
                Value::String(String::new()),
                Value::String("中|".into()),
                Value::Bytes(vec![0xff, 0]),
            ] {
                for text in [
                    Value::Null,
                    Value::String(String::new()),
                    Value::String("é🙂\0abc".repeat(4096)),
                    Value::SmallString(smallvec::SmallVec::from_slice(b"small")),
                ] {
                    for bytes in [
                        Value::Null,
                        Value::Bytes(Vec::new()),
                        Value::Bytes(vec![0xff, 0, 0xc3, 0x80]),
                    ] {
                        for doc in &docs {
                            let program = assert_parity(
                                &expr,
                                &columns,
                                vec![separator.clone(), text.clone(), bytes.clone(), doc.clone()],
                            );
                            assert!(!program.capability().legacy_escape);
                        }
                    }
                }
            }
        }
    }

    #[test]
    fn crc32_compiles_value_operands_and_matches_legacy() {
        for (ty, values) in [
            (
                ColumnType::VarString,
                vec![
                    Value::Null,
                    Value::String(String::new()),
                    Value::String("中🙂é\0".into()),
                ],
            ),
            (
                ColumnType::Blob,
                vec![Value::Bytes(vec![0xff, 0x00, 0xc3, 0x80])],
            ),
            (
                ColumnType::LongLong,
                vec![Value::Int(-123), Value::UInt(u64::MAX)],
            ),
            (
                ColumnType::Decimal {
                    precision: 12,
                    scale: 3,
                },
                vec![Value::DecimalCompact(12340, 3)],
            ),
        ] {
            for value in values {
                let expr = Expr::Function {
                    name: "CRC32".into(),
                    args: vec![Expr::Identifier("x".into())],
                    distinct: false,
                };
                let program = assert_parity(&expr, &[("x", ty)], vec![value]);
                assert!(!program.capability().legacy_escape);
            }
        }
    }

    #[test]
    fn crc32_preserves_source_sensitive_fallbacks() {
        for ty in [
            ColumnType::Bit { bits: 9 },
            ColumnType::Float,
            ColumnType::Double,
        ] {
            let expr = Expr::Function {
                name: "CRC32".into(),
                args: vec![Expr::Identifier("x".into())],
                distinct: false,
            };
            assert!(matches!(
                ExprCompiler::compile(&expr, &input_schema(&[("x", ty)])),
                Err(CompileError::Unsupported)
            ));
        }
        let expr = Expr::Function {
            name: "CRC32".into(),
            args: vec![Expr::Function {
                name: "PI".into(),
                args: vec![],
                distinct: false,
            }],
            distinct: false,
        };
        assert!(matches!(
            ExprCompiler::compile(&expr, &input_schema(&[])),
            Err(CompileError::Unsupported)
        ));
    }

    #[test]
    fn value_string_operations_proof_rechecks_parameter_extents() {
        let crate::parser::Statement::Select(select) =
            crate::parser::parse_statement("SELECT CRC32(CONCAT_WS(',', x, LEFT(x, ?)))").unwrap()
        else {
            unreachable!()
        };
        let crate::parser::SelectItem::Expr { expr, .. } = &select.projection[0] else {
            unreachable!()
        };
        let schema = input_schema(&[("x", ColumnType::VarString)]);
        let program = crate::engine::params::with_params(vec![Value::Int(100)], || {
            ExprCompiler::compile(expr, &schema).unwrap()
        });
        for value in [
            Value::Int(0),
            Value::Int(-1),
            Value::UInt(u64::MAX),
            Value::Null,
        ] {
            crate::engine::params::with_params(vec![value], || {
                assert!(program.has_only_value_string_operations());
            });
        }
        for value in [Value::String("bad".into()), Value::Double(2.5)] {
            crate::engine::params::with_params(vec![value], || {
                assert!(!program.has_only_value_string_operations());
            });
        }
        let crate::parser::Statement::Select(select) =
            crate::parser::parse_statement("SELECT CRC32(CONCAT_WS(',', x + 0))").unwrap()
        else {
            unreachable!()
        };
        let crate::parser::SelectItem::Expr { expr, .. } = &select.projection[0] else {
            unreachable!()
        };
        assert!(!ExprCompiler::compile(expr, &schema)
            .unwrap()
            .has_only_value_string_operations());
    }

    #[test]
    fn string_slice_kernels_match_legacy_for_wide_and_dynamic_extents() {
        let values = [
            Value::String("中🙂éabc".repeat(4096)),
            Value::Bytes(vec![0xff, 0x00, 0xc3, 0x80]),
            Value::String(String::new()),
            Value::Null,
        ];
        for name in ["LEFT", "RIGHT", "SUBSTRING", "SUBSTR", "MID"] {
            for input in &values {
                for extent in [
                    Value::Int(-2),
                    Value::Int(0),
                    Value::Int(3),
                    Value::UInt(u64::MAX),
                    Value::Null,
                ] {
                    let mut args = vec![Expr::Identifier("x".into())];
                    if !matches!(name, "LEFT" | "RIGHT") {
                        args.push(Expr::Literal(Literal::Int(2)));
                    }
                    args.push(Expr::Identifier("n".into()));
                    let expr = Expr::Function {
                        name: name.into(),
                        args,
                        distinct: false,
                    };
                    assert_parity(
                        &expr,
                        &[("x", ColumnType::VarString), ("n", ColumnType::LongLong)],
                        vec![input.clone(), extent],
                    );
                }
            }
        }
    }

    #[test]
    fn binary_string_kernels_use_octets_and_preserve_bytes() {
        let bytes = Value::Bytes(vec![0xc3, 0x80, 0xff, b'a']);
        let col = Expr::Identifier("b".into());
        let int = |n| Expr::Literal(Literal::Int(n));
        let call = |name: &str, args: Vec<Expr>| Expr::Function {
            name: name.into(),
            args,
            distinct: false,
        };

        let cases = [
            (call("CHAR_LENGTH", vec![col.clone()]), Value::Int(4)),
            (
                call("REVERSE", vec![col.clone()]),
                Value::Bytes(vec![b'a', 0xff, 0x80, 0xc3]),
            ),
            (
                call("SUBSTRING", vec![col.clone(), int(2), int(2)]),
                Value::Bytes(vec![0x80, 0xff]),
            ),
            (
                call("LEFT", vec![col.clone(), int(2)]),
                Value::Bytes(vec![0xc3, 0x80]),
            ),
            (
                call("RIGHT", vec![col.clone(), int(2)]),
                Value::Bytes(vec![0xff, b'a']),
            ),
        ];
        for (expr, expected) in cases {
            let program = assert_parity(&expr, &[("b", ColumnType::Blob)], vec![bytes.clone()]);
            let row = RowView::from_values(std::slice::from_ref(&bytes));
            let actual = with_eval_scratch(|scratch| program.eval_row(&row, scratch)).unwrap();
            assert_eq!(actual, expected, "binary kernel mismatch for {expr:?}");
        }
    }

    #[test]
    fn source_collation_function_escapes_only_its_subtree() {
        let locate = Expr::Function {
            name: "LOCATE".into(),
            args: vec![
                Expr::Literal(Literal::String("a".into())),
                Expr::Identifier("s".into()),
            ],
            distinct: false,
        };
        let expr = Expr::Binary {
            left: Box::new(locate),
            op: BinaryOp::Add,
            right: Box::new(Expr::Literal(Literal::Int(1))),
        };
        let program = assert_parity(
            &expr,
            &[("s", ColumnType::VarString)],
            vec![Value::String("Alpha".into())],
        );
        let ExprNode::Binary { left, .. } = &program.root else {
            panic!("surrounding expression should remain compiled");
        };
        assert!(matches!(left.as_ref(), ExprNode::LegacyEscape { .. }));
    }

    #[test]
    fn nested_mixed_decimal_coalesce_stays_compiled_and_matches_legacy() {
        let decimal = ColumnType::Decimal {
            precision: 9,
            scale: 2,
        };
        let coalesce = Expr::Function {
            name: "COALESCE".into(),
            args: vec![
                Expr::Identifier("returned".into()),
                Expr::Literal(Literal::Decimal("0.0".into())),
            ],
            distinct: false,
        };
        let expr = Expr::Binary {
            left: Box::new(Expr::Identifier("sales".into())),
            op: BinaryOp::Sub,
            right: Box::new(coalesce),
        };
        let program = assert_parity(
            &expr,
            &[("sales", decimal), ("returned", decimal)],
            vec![Value::DecimalCompact(12_345, 2), Value::Null],
        );
        let ExprNode::Binary { right, .. } = &program.root else {
            panic!("expected compiled subtraction, got {:?}", program.root);
        };
        assert!(
            matches!(
                right.as_ref(),
                ExprNode::Function {
                    kernel: FunctionKernel::Coalesce,
                    result_coercion: Some(_),
                    ..
                }
            ),
            "mixed DECIMAL COALESCE should carry one bound coercion, not a per-row legacy escape: {right:?}"
        );
    }

    // An unregistered, legacy-only scalar function (no compiled kernel) so
    // it exercises the `LegacyEscape` path. `LOCATE` stays legacy-only because
    // its collation-sensitive search stays on the legacy oracle.
    fn pow(base: Expr, _exp: i64) -> Expr {
        Expr::Function {
            name: "LOCATE".into(),
            args: vec![Expr::Literal(Literal::String("x".into())), base],
            distinct: false,
        }
    }

    #[test]
    fn unsupported_function_escapes_and_matches_legacy() {
        let expr = pow(Expr::Identifier("x".into()), 2);
        let program = assert_parity(
            &expr,
            &[("x", ColumnType::VarString)],
            vec![Value::String(" hi ".into())],
        );
        assert!(
            matches!(program.root, ExprNode::LegacyEscape { .. }),
            "LOCATE(x) should compile to a single LegacyEscape node, got {:?}",
            program.root
        );
    }

    #[test]
    fn common_temporal_scalars_compile_without_legacy_escape() {
        let cases = [
            (
                Expr::Function {
                    name: "STR_TO_DATE".into(),
                    args: vec![
                        Expr::Identifier("x".into()),
                        Expr::Literal(Literal::String("%Y-%m-%d".into())),
                    ],
                    distinct: false,
                },
                ColumnType::VarString,
                Value::String("2024-02-29".into()),
            ),
            (
                Expr::Function {
                    name: "TIME_TO_SEC".into(),
                    args: vec![Expr::Identifier("x".into())],
                    distinct: false,
                },
                ColumnType::VarString,
                Value::String("25:01:02".into()),
            ),
            (
                Expr::Function {
                    name: "SEC_TO_TIME".into(),
                    args: vec![Expr::Identifier("x".into())],
                    distinct: false,
                },
                ColumnType::LongLong,
                Value::Int(90_061),
            ),
        ];
        for (expr, column_type, value) in cases {
            let program = assert_parity(&expr, &[("x", column_type)], vec![value]);
            assert!(
                matches!(program.root, ExprNode::Function { .. }),
                "expected registered kernel for {expr:?}, got {:?}",
                program.root
            );
        }
    }

    #[test]
    fn nary_and_strcmp_kernels_preserve_binary_column_collation() {
        let collation_id = crate::charset::resolve_collation("utf8mb4_bin")
            .expect("registered binary collation")
            .id;
        let schema = InputSchema::new(vec![InputColumn {
            table: "t".into(),
            alias: None,
            name: "v".into(),
            column_type: ColumnType::VarString,
            unsigned: false,
            collation_id: Some(collation_id),
        }]);
        let mut table = TableSchema::synthetic_for_planning(
            "t",
            &[Column {
                name: "v".into(),
                column_type: ColumnType::VarString,
                source: crate::types::ColumnSource {
                    collation_id: Some(collation_id),
                    ..Default::default()
                },
            }],
        );
        table.build_column_index();

        for (expr, row, expected) in [
            (
                Expr::Function {
                    name: "STRCMP".into(),
                    args: vec![
                        Expr::Identifier("v".into()),
                        Expr::Literal(Literal::String("A".into())),
                    ],
                    distinct: false,
                },
                Value::String("a".into()),
                Value::Int(1),
            ),
            (
                Expr::Function {
                    name: "GREATEST".into(),
                    args: vec![
                        Expr::Identifier("v".into()),
                        Expr::Literal(Literal::String("a".into())),
                    ],
                    distinct: false,
                },
                Value::String("A".into()),
                Value::String("a".into()),
            ),
        ] {
            let program = ExprCompiler::compile(&expr, &schema).expect("compiled collation kernel");
            assert!(matches!(program.root, ExprNode::Function { .. }));
            let values = [row.clone()];
            let compiled = with_eval_scratch(|scratch| {
                program.eval_row(&RowView::from_values(&values), scratch)
            });
            let legacy = eval_expr_legacy(&expr, &table, &[row]);
            assert_eq!(compiled, Ok(expected));
            assert_eq!(compiled, legacy);
        }
    }

    #[test]
    fn placeholder_comparison_compiles_and_preserves_pad_space_collation() {
        let collation_id = crate::charset::resolve_collation("latin1_swedish_ci")
            .expect("registered PAD SPACE collation")
            .id;
        let schema = InputSchema::new(vec![InputColumn {
            table: "t".into(),
            alias: None,
            name: "v".into(),
            column_type: ColumnType::VarString,
            unsigned: false,
            collation_id: Some(collation_id),
        }]);
        let expr = Expr::Binary {
            left: Box::new(Expr::Identifier("v".into())),
            op: BinaryOp::Eq,
            right: Box::new(Expr::Placeholder(0)),
        };
        let program = ExprCompiler::compile(&expr, &schema).expect("placeholder comparison");
        assert!(
            matches!(
                program.root,
                ExprNode::Binary {
                    semantics: BinarySemantics {
                        comparison: ComparisonProvenance {
                            right_dynamic_source: true,
                            ..
                        },
                        ..
                    },
                    ..
                }
            ),
            "placeholder comparison should compile with right-side dynamic provenance"
        );

        let values = [Value::String("Chevy ".into())];
        let row = RowView::from_values(&values);
        let result =
            crate::engine::params::with_params(vec![Value::String("Chevy".into())], || {
                with_eval_scratch(|scratch| program.eval_row(&row, scratch))
            })
            .expect("compiled comparison");
        assert_eq!(result, Value::Int(1));
    }

    #[test]
    fn placeholder_comparison_matches_legacy_exact_dynamic_numeric_semantics() {
        let cols = [("v", ColumnType::LongLong)];
        let schema = input_schema(&cols);
        let table = oracle_table(&cols);
        let values = [Value::Int(9_007_199_254_740_992)];

        for op in [
            BinaryOp::Eq,
            BinaryOp::NullSafeEq,
            BinaryOp::NotEq,
            BinaryOp::Lt,
            BinaryOp::LtEq,
            BinaryOp::Gt,
            BinaryOp::GtEq,
        ] {
            let expr = Expr::Binary {
                left: Box::new(Expr::Identifier("v".into())),
                op,
                right: Box::new(Expr::Placeholder(0)),
            };
            let program = ExprCompiler::compile(&expr, &schema).expect("placeholder comparison");
            assert!(
                matches!(program.root, ExprNode::Binary { .. }),
                "{op:?} should stay compiled"
            );

            let param = Value::String("9007199254740993".into());
            crate::engine::api::clear_statement_warnings();
            let compiled = crate::engine::params::with_params(vec![param.clone()], || {
                let row = RowView::from_values(&values);
                with_eval_scratch(|scratch| program.eval_row(&row, scratch))
            });
            let compiled_warnings = crate::engine::api::statement_warnings_snapshot();

            crate::engine::api::clear_statement_warnings();
            let legacy = crate::engine::params::with_params(vec![param], || {
                eval_expr_legacy(&expr, &table, &values)
            });
            let legacy_warnings = crate::engine::api::statement_warnings_snapshot();

            assert_eq!(compiled, legacy, "result mismatch for {op:?}");
            assert_eq!(
                compiled_warnings, legacy_warnings,
                "warning mismatch for {op:?}"
            );
        }
    }

    #[test]
    fn auto_parameterized_string_keeps_literal_comparison_provenance() {
        let cols = [("v", ColumnType::LongLong)];
        let schema = input_schema(&cols);
        let table = oracle_table(&cols);
        let values = [Value::Int(9_007_199_254_740_992)];
        let expr = Expr::Binary {
            left: Box::new(Expr::Identifier("v".into())),
            op: BinaryOp::Eq,
            right: Box::new(Expr::Placeholder(0)),
        };
        let program = ExprCompiler::compile(&expr, &schema).expect("placeholder comparison");
        let param = Value::String("9007199254740993".into());

        let compiled = crate::engine::params::with_auto_params(vec![param.clone()], || {
            let row = RowView::from_values(&values);
            with_eval_scratch(|scratch| program.eval_row(&row, scratch))
        });
        let legacy = crate::engine::params::with_auto_params(vec![param], || {
            eval_expr_legacy(&expr, &table, &values)
        });
        assert_eq!(compiled, legacy);
    }

    #[test]
    fn placeholder_decimal_and_null_comparisons_match_legacy() {
        let cols = [(
            "v",
            ColumnType::Decimal {
                precision: 12,
                scale: 2,
            },
        )];
        let schema = input_schema(&cols);
        let table = oracle_table(&cols);
        let values = [Value::DecimalCompact(12_345, 2)];

        for (op, param) in [
            (BinaryOp::Eq, Value::String("123.45".into())),
            (BinaryOp::NotEq, Value::String("123.450".into())),
            (BinaryOp::Lt, Value::String("123.46".into())),
            (BinaryOp::GtEq, Value::String("123.45".into())),
            (BinaryOp::Eq, Value::Null),
            (BinaryOp::NullSafeEq, Value::Null),
        ] {
            let expr = Expr::Binary {
                left: Box::new(Expr::Identifier("v".into())),
                op,
                right: Box::new(Expr::Placeholder(0)),
            };
            let program = ExprCompiler::compile(&expr, &schema).expect("decimal comparison");
            assert!(matches!(program.root, ExprNode::Binary { .. }));

            let compiled = crate::engine::params::with_params(vec![param.clone()], || {
                let row = RowView::from_values(&values);
                with_eval_scratch(|scratch| program.eval_row(&row, scratch))
            });
            let legacy = crate::engine::params::with_params(vec![param], || {
                eval_expr_legacy(&expr, &table, &values)
            });
            assert_eq!(compiled, legacy, "decimal result mismatch for {op:?}");
        }
    }

    #[test]
    fn placeholder_comparison_preserves_unsigned_and_binary_string_domains() {
        let cases = [
            (
                InputColumn {
                    table: "t".into(),
                    alias: None,
                    name: "v".into(),
                    column_type: ColumnType::LongLong,
                    unsigned: true,
                    collation_id: None,
                },
                Value::UInt(u64::MAX),
                Value::String(u64::MAX.to_string()),
            ),
            (
                InputColumn {
                    table: "t".into(),
                    alias: None,
                    name: "v".into(),
                    column_type: ColumnType::Blob,
                    unsigned: false,
                    collation_id: None,
                },
                Value::Bytes(vec![b'A', 0]),
                Value::Bytes(vec![b'A']),
            ),
        ];

        for (column, row_value, param) in cases {
            let schema = InputSchema::new(vec![column.clone()]);
            let source = crate::types::ColumnSource {
                flags: if column.unsigned {
                    crate::mysql::protocol::UNSIGNED_FLAG
                } else {
                    0
                },
                collation_id: column.collation_id,
                ..Default::default()
            };
            let mut table = TableSchema::synthetic_for_planning(
                "t",
                &[Column {
                    name: "v".into(),
                    column_type: column.column_type,
                    source,
                }],
            );
            table.build_column_index();
            let expr = Expr::Binary {
                left: Box::new(Expr::Identifier("v".into())),
                op: BinaryOp::Eq,
                right: Box::new(Expr::Placeholder(0)),
            };
            let program = ExprCompiler::compile(&expr, &schema).expect("placeholder comparison");
            assert!(matches!(program.root, ExprNode::Binary { .. }));

            let compiled = crate::engine::params::with_params(vec![param.clone()], || {
                let values = [row_value.clone()];
                let row = RowView::from_values(&values);
                with_eval_scratch(|scratch| program.eval_row(&row, scratch))
            });
            let legacy = crate::engine::params::with_params(vec![param], || {
                eval_expr_legacy(&expr, &table, &[row_value])
            });
            assert_eq!(compiled, legacy);
        }
    }

    #[test]
    fn placeholder_comparison_preserves_case_sensitive_collation() {
        let collation_id = crate::charset::resolve_collation("utf8mb4_bin")
            .expect("registered binary collation")
            .id;
        let schema = InputSchema::new(vec![InputColumn {
            table: "t".into(),
            alias: None,
            name: "v".into(),
            column_type: ColumnType::VarString,
            unsigned: false,
            collation_id: Some(collation_id),
        }]);
        let expr = Expr::Binary {
            left: Box::new(Expr::Identifier("v".into())),
            op: BinaryOp::Eq,
            right: Box::new(Expr::Placeholder(0)),
        };
        let program = ExprCompiler::compile(&expr, &schema).expect("placeholder comparison");
        let values = [Value::String("Alpha".into())];
        let result =
            crate::engine::params::with_params(vec![Value::String("alpha".into())], || {
                let row = RowView::from_values(&values);
                with_eval_scratch(|scratch| program.eval_row(&row, scratch))
            })
            .expect("case-sensitive comparison");
        assert_eq!(result, Value::Int(0));
    }

    #[test]
    fn exact_decimal_literal_comparison_remains_on_legacy_oracle() {
        let schema = input_schema(&[(
            "v",
            ColumnType::Decimal {
                precision: 65,
                scale: 0,
            },
        )]);
        let expr = Expr::Binary {
            left: Box::new(Expr::Identifier("v".into())),
            op: BinaryOp::Eq,
            right: Box::new(Expr::Literal(Literal::Decimal("9".repeat(66)))),
        };
        let program = ExprCompiler::compile(&expr, &schema).expect("DECIMAL legacy escape");
        assert!(
            matches!(program.root, ExprNode::LegacyEscape { .. }),
            "overwide exact DECIMAL comparison needs the legacy precision oracle"
        );
    }

    #[test]
    fn placeholder_year_comparison_remains_on_legacy_oracle() {
        let schema = input_schema(&[("y", ColumnType::Year)]);
        let expr = Expr::Binary {
            left: Box::new(Expr::Identifier("y".into())),
            op: BinaryOp::Eq,
            right: Box::new(Expr::Placeholder(0)),
        };
        let program = ExprCompiler::compile(&expr, &schema).expect("YEAR legacy escape");
        assert!(
            matches!(program.root, ExprNode::LegacyEscape { .. }),
            "YEAR comparison needs its compatibility coercion oracle"
        );
    }

    #[test]
    fn rest_of_tree_stays_compiled_around_escape() {
        // POWER(x,2) + 1 : Binary stays compiled, only POWER escapes.
        let expr = Expr::Binary {
            left: Box::new(pow(Expr::Identifier("x".into()), 2)),
            op: BinaryOp::Add,
            right: Box::new(Expr::Literal(Literal::Int(1))),
        };
        let program = assert_parity(
            &expr,
            &[("x", ColumnType::Double)],
            vec![Value::Double(4.0)],
        );
        match &program.root {
            ExprNode::Binary { left, right, .. } => {
                assert!(
                    matches!(**left, ExprNode::LegacyEscape { .. }),
                    "left operand should be the escaped POWER, got {left:?}"
                );
                assert!(
                    matches!(**right, ExprNode::Const(_)),
                    "right operand should stay a compiled constant, got {right:?}"
                );
            }
            other => panic!("expected Binary root, got {other:?}"),
        }
    }

    #[test]
    fn supported_kernel_wraps_escaped_arg() {
        // UPPER(LOCATE(s)) : UPPER is a kernel; LOCATE is legacy-only -> escapes.
        let expr = Expr::Function {
            name: "UPPER".into(),
            args: vec![Expr::Function {
                name: "LOCATE".into(),
                args: vec![
                    Expr::Literal(Literal::String("x".into())),
                    Expr::Identifier("s".into()),
                ],
                distinct: false,
            }],
            distinct: false,
        };
        let program = assert_parity(
            &expr,
            &[("s", ColumnType::VarString)],
            vec![Value::String("  hi  ".into())],
        );
        match &program.root {
            ExprNode::Function { args, .. } => assert!(
                matches!(args.as_slice(), [ExprNode::LegacyEscape { .. }]),
                "UPPER should compile with an escaped LOCATE arg, got {args:?}"
            ),
            other => panic!("expected Function root, got {other:?}"),
        }
    }

    #[test]
    fn cast_preserves_structural_type_args() {
        // CAST encodes its target type as a literal arg. The legacy body reads
        // it via the eval closure (as a string Value), not by matching the AST,
        // so P6-1 registers CAST as a LegacyDeterministic kernel: the type arg
        // survives as a folded Const and must still drive the right cast.
        let expr = Expr::Function {
            name: "CAST".into(),
            args: vec![
                Expr::Identifier("x".into()),
                Expr::Literal(Literal::String("CHAR".into())),
            ],
            distinct: false,
        };
        assert_parity(&expr, &[("x", ColumnType::LongLong)], vec![Value::Int(42)]);
    }

    #[test]
    fn structured_cast_operators_compile_to_kernel_not_escape() {
        // P6-1: CAST / EXTRACT / CONVERT_USING / COLLATE resolve to a kernel,
        // so their *value* argument is lowered natively instead of the whole
        // subtree escaping to legacy. `CAST(a + b AS SIGNED)` must compile the
        // `a + b` to a native Binary node and still match the legacy oracle.
        let s = |v: &str| Expr::Literal(Literal::String(v.into()));
        let cast_sum = Expr::Function {
            name: "CAST".into(),
            args: vec![
                Expr::Binary {
                    left: Box::new(Expr::Identifier("a".into())),
                    op: BinaryOp::Add,
                    right: Box::new(Expr::Identifier("b".into())),
                },
                s("SIGNED"),
            ],
            distinct: false,
        };
        let schema = input_schema(&[("a", ColumnType::Double), ("b", ColumnType::Double)]);
        let program = ExprCompiler::compile(&cast_sum, &schema).expect("compile CAST(a+b)");
        match &program.root {
            ExprNode::Function {
                kernel: FunctionKernel::LegacyDeterministic("CAST"),
                args,
                ..
            } => assert!(
                matches!(args[0], ExprNode::Binary { .. }),
                "CAST value arg should lower to a native Binary node, got {:?}",
                args[0]
            ),
            other => panic!("CAST should compile to a LegacyDeterministic kernel, got {other:?}"),
        }
        assert_parity(
            &cast_sum,
            &[("a", ColumnType::Double), ("b", ColumnType::Double)],
            vec![Value::Double(2.5), Value::Double(3.0)],
        );

        // EXTRACT(YEAR FROM dt), CONVERT(... USING utf8mb4), x COLLATE ...
        let extract = func("EXTRACT", vec![s("YEAR"), Expr::Identifier("dt".into())]);
        assert_parity(
            &extract,
            &[("dt", ColumnType::DateTime)],
            vec![Value::String("2026-06-19 10:00:00".into())],
        );
        let convert = func(
            "CONVERT_USING",
            vec![Expr::Identifier("s".into()), s("utf8mb4")],
        );
        assert_parity(
            &convert,
            &[("s", ColumnType::VarString)],
            vec![Value::String("héllo".into())],
        );
        let collate = func(
            "COLLATE",
            vec![Expr::Identifier("s".into()), s("utf8mb4_bin")],
        );
        assert_parity(
            &collate,
            &[("s", ColumnType::VarString)],
            vec![Value::String("abc".into())],
        );

        // NULL propagation + a value arg that itself errors per row stays
        // faithful (CAST of a non-const arg must not fold away the per-row work).
        assert_parity(
            &func("CAST", vec![Expr::Identifier("a".into()), s("SIGNED")]),
            &[("a", ColumnType::VarString)],
            vec![Value::Null],
        );
    }

    #[test]
    fn concat_compiles_with_legacy_parity() {
        let expr = Expr::Binary {
            left: Box::new(Expr::Identifier("a".into())),
            op: BinaryOp::Concat,
            right: Box::new(Expr::Identifier("b".into())),
        };
        assert_parity(
            &expr,
            &[("a", ColumnType::VarString), ("b", ColumnType::VarString)],
            vec![Value::String("x".into()), Value::String("y".into())],
        );
    }

    #[test]
    fn escape_null_propagation_matches_legacy() {
        let expr = pow(Expr::Identifier("x".into()), 2);
        assert_parity(&expr, &[("x", ColumnType::Double)], vec![Value::Null]);
    }

    #[test]
    fn aggregate_does_not_escape() {
        // SUM is an aggregate — must NOT escape (no planner context here);
        // the whole expression stays Unsupported.
        let schema = input_schema(&[("x", ColumnType::Double)]);
        let expr = Expr::Function {
            name: "SUM".into(),
            args: vec![Expr::Identifier("x".into())],
            distinct: false,
        };
        assert!(
            ExprCompiler::compile(&expr, &schema).is_err(),
            "aggregate SUM must not be escaped to legacy"
        );
    }

    #[test]
    fn unknown_column_in_escape_is_refused() {
        // POWER(nope, 2): the column doesn't resolve, so the escape is
        // refused and compilation fails (matching pre-escape behaviour).
        let schema = input_schema(&[("x", ColumnType::Double)]);
        let expr = pow(Expr::Identifier("nope".into()), 2);
        assert!(
            ExprCompiler::compile(&expr, &schema).is_err(),
            "escape over an unknown column must be refused"
        );
    }

    fn func(name: &str, args: Vec<Expr>) -> Expr {
        Expr::Function {
            name: name.into(),
            args,
            distinct: false,
        }
    }

    #[test]
    fn registered_deterministic_kernels_match_legacy() {
        let id = |n: &str| Expr::Identifier(n.into());
        let lit = |i: i64| Expr::Literal(Literal::Int(i));
        let s = |v: &str| Expr::Literal(Literal::String(v.into()));

        // (expr, columns, row) tuples; each compiles to a kernel (not an
        // escape) and must equal the legacy whole-expression oracle.
        let dbl = &[("x", ColumnType::Double)][..];
        assert_parity(
            &func("POWER", vec![id("x"), lit(2)]),
            dbl,
            vec![Value::Double(3.0)],
        );
        assert_parity(&func("EXP", vec![id("x")]), dbl, vec![Value::Double(1.0)]);
        assert_parity(&func("LOG", vec![id("x")]), dbl, vec![Value::Double(2.5)]);
        assert_parity(
            &func("GREATEST", vec![id("x"), lit(5)]),
            dbl,
            vec![Value::Double(3.0)],
        );
        assert_parity(
            &func("LEAST", vec![id("x"), lit(5)]),
            dbl,
            vec![Value::Double(9.0)],
        );

        let strc = &[("s", ColumnType::VarString)][..];
        assert_parity(
            &func("FIELD", vec![id("s"), s("a"), s("b"), s("c")]),
            strc,
            vec![Value::String("b".into())],
        );
        assert_parity(
            &func("FIND_IN_SET", vec![id("s"), s("a,b,c")]),
            strc,
            vec![Value::String("c".into())],
        );
        assert_parity(
            &func("MD5", vec![id("s")]),
            strc,
            vec![Value::String("hi".into())],
        );
        assert_parity(
            &func("HEX", vec![id("s")]),
            strc,
            vec![Value::String("AB".into())],
        );
        assert_parity(
            &func("CONV", vec![id("s"), lit(16), lit(2)]),
            strc,
            vec![Value::String("ff".into())],
        );
    }

    #[test]
    fn phase5_kernels_match_legacy() {
        let id = |n: &str| Expr::Identifier(n.into());
        let lit = |i: i64| Expr::Literal(Literal::Int(i));
        let dbl = &[("x", ColumnType::Double)][..];
        let strc = &[("s", ColumnType::VarString)][..];

        assert_parity(
            &func("FORMAT", vec![id("x"), lit(2)]),
            dbl,
            vec![Value::Double(1234.5678)],
        );
        assert_parity(
            &func("BIT_LENGTH", vec![id("s")]),
            strc,
            vec![Value::String("hi".into())],
        );
        assert_parity(
            &func("SOUNDEX", vec![id("s")]),
            strc,
            vec![Value::String("Robert".into())],
        );
        assert_parity(&func("PI", vec![]), dbl, vec![Value::Double(0.0)]);
        assert_parity(
            &func("INET_ATON", vec![id("s")]),
            strc,
            vec![Value::String("10.0.0.1".into())],
        );
        assert_parity(
            &func("IS_IPV4", vec![id("s")]),
            strc,
            vec![Value::String("1.2.3.4".into())],
        );
        assert_parity(
            &func("FROM_DAYS", vec![id("x")]),
            dbl,
            vec![Value::Double(730669.0)],
        );
    }

    #[test]
    fn deterministic_kernel_with_const_args_folds() {
        let schema = input_schema(&[("x", ColumnType::Double)]);
        // POWER(2, 3) — both args constant + deterministic -> folds to 8.0.
        let expr = func(
            "POWER",
            vec![
                Expr::Literal(Literal::Int(2)),
                Expr::Literal(Literal::Int(3)),
            ],
        );
        let program = ExprCompiler::compile(&expr, &schema).expect("compile POWER(2,3)");
        assert!(
            matches!(program.root, ExprNode::Const(Value::Double(d)) if (d - 8.0).abs() < 1e-9),
            "POWER(2,3) should constant-fold to Double(8.0), got {:?}",
            program.root
        );
    }

    #[test]
    fn registered_function_does_not_escape() {
        // A registered deterministic function compiles to a kernel, not a
        // LegacyEscape — so it benefits from folding / future vectorization.
        let schema = input_schema(&[("x", ColumnType::Double)]);
        let program = ExprCompiler::compile(
            &func(
                "POWER",
                vec![Expr::Identifier("x".into()), Expr::Literal(Literal::Int(2))],
            ),
            &schema,
        )
        .expect("compile");
        assert!(
            matches!(
                program.root,
                ExprNode::Function {
                    kernel: FunctionKernel::Math(crate::engine::expr::math::MathFunction::Power),
                    ..
                }
            ),
            "POWER(x,2) should compile to a LegacyDeterministic kernel, got {:?}",
            program.root
        );
    }

    #[test]
    fn escape_counter_increments() {
        let before = expr_program_stats().legacy_escape_rows;
        let expr = pow(Expr::Identifier("x".into()), 2);
        assert_parity(
            &expr,
            &[("x", ColumnType::Double)],
            vec![Value::Double(2.0)],
        );
        assert!(
            expr_program_stats().legacy_escape_rows > before,
            "evaluating an escaped node should bump legacy_escape_rows"
        );
    }
}

#[cfg(test)]
mod empty_scratch_scope_tests {
    use super::*;
    #[test]
    fn empty_scratch_allows_nested_evaluation() {
        let value = with_eval_scratch(|_| with_eval_scratch(|_| Ok(7usize))).unwrap();
        assert_eq!(value, 7);
    }
}

#[cfg(test)]
mod native_catalog_contract_tests {
    use super::*;
    #[test]
    fn native_aliases_keep_fold_policy_and_qualified_names() {
        let registry = FunctionRegistry::global();
        for name in [
            "LENGTH",
            "octet_length",
            "cHaRaCtEr_LeNgTh",
            "CEILING",
            "ADDDATE",
        ] {
            assert!(registry.resolve(name).unwrap().is_deterministic(), "{name}");
        }
        for name in [
            "CURRENT_TIMESTAMP",
            "DATE_FORMAT",
            "UNIX_TIMESTAMP",
            "WEEK",
            "LPAD",
            "SPACE",
        ] {
            assert!(
                !registry.resolve(name).unwrap().is_deterministic(),
                "{name}"
            );
        }
        for name in ["sys.abs", "test.length", "未知函数", &"a".repeat(80)] {
            assert!(registry.resolve(name).is_none(), "{name}");
        }
    }
}

#[cfg(test)]
pub(crate) fn catalog_kernel_names() -> Vec<&'static str> {
    native_kernels()
        .iter()
        .copied()
        .chain(delegated_kernels())
        .map(|(name, _)| name)
        .collect()
}
