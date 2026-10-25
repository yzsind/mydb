//! Aggregate binding and streaming accumulators shared by row execution and
//! its fallback evaluator. Slots own DISTINCT/order-key normalization; states
//! own update, merge and finalization, including exact numeric arithmetic.
//! Finalized values are indexed by the canonical aggregate key in PlanRow.

use std::cmp::Ordering;
use std::collections::{BTreeMap, HashMap, HashSet};

use super::plan::PlanField;
use crate::engine::executor::sort::{compare_value_for_sort, SortField};
use crate::engine::expr::{compare_values, numeric, value_to_jsonb_typed, ValueKey};
use crate::engine::geo::types::{Geom, Geometry, WKB_LINESTRING, WKB_POINT, WKB_POLYGON};
use crate::engine::geo::{invalid_gis_data, read_geometry, srid_mismatch, write_geometry};
use crate::parser::{Expr, Literal, SelectItem, GROUP_CONCAT_ORDERED_FN};
use crate::planner;
use crate::storage::error;
use crate::storage::value_to_string;
use crate::types::{ColumnType, QueryError, Value};

pub(super) const ROLLUP_MASK_AGG_KEY: u64 = u64::MAX;

/// One `ORDER BY` term parsed out of a `GROUP_CONCAT(... ORDER BY ...)`
/// call. Stored on [`AggregateSlot`] so the executor can buffer sort keys
/// per row and sort at finalize time.
#[derive(Debug, Clone)]
pub(super) struct AggregateOrderKey {
    pub expr: Expr,
    pub asc: bool,
    /// Explicit NULLS FIRST/LAST from the SQL text (mydb extension);
    /// `None` keeps MySQL's direction-derived default.
    pub nulls_first: Option<bool>,
}

/// Unwrap the `__gc_ordered(value, ord1, dir1, ord2, dir2, ...)` synthetic
/// wrapper the parser builds for `GROUP_CONCAT(... ORDER BY ...)`. Returns
/// `Some((value_expr, order_keys))` when `expr` is the wrapper, else `None`.
fn unwrap_group_concat_ordered(expr: &Expr) -> Option<(Expr, Vec<AggregateOrderKey>)> {
    let Expr::Function {
        name,
        args,
        distinct: _,
    } = expr
    else {
        return None;
    };
    if !name.eq_ignore_ascii_case(GROUP_CONCAT_ORDERED_FN) {
        return None;
    }
    if args.is_empty() || (args.len() - 1) % 2 != 0 {
        return None;
    }
    let value = args[0].clone();
    let mut keys = Vec::with_capacity((args.len() - 1) / 2);
    let mut i = 1;
    while i + 1 < args.len() {
        let ord_expr = args[i].clone();
        let (asc, nulls_first) = match &args[i + 1] {
            Expr::Literal(Literal::String(s)) => match s.to_ascii_uppercase().as_str() {
                "DESC" => (false, None),
                "ASC_NULLS_FIRST" => (true, Some(true)),
                "ASC_NULLS_LAST" => (true, Some(false)),
                "DESC_NULLS_FIRST" => (false, Some(true)),
                "DESC_NULLS_LAST" => (false, Some(false)),
                _ => (true, None),
            },
            _ => (true, None),
        };
        keys.push(AggregateOrderKey {
            expr: ord_expr,
            asc,
            nulls_first,
        });
        i += 2;
    }
    Some((value, keys))
}

/// Stable, separate hash domain for synthetic hidden-column aggregates.
fn aggregate_key_state() -> &'static ahash::RandomState {
    static STATE: std::sync::OnceLock<ahash::RandomState> = std::sync::OnceLock::new();
    STATE.get_or_init(|| {
        ahash::RandomState::with_seeds(
            0xA773_0BEA_C0DE_F00Du64,
            0xDEAD_BEEF_CAFE_BABEu64,
            0x1234_5678_9ABC_DEF0u64,
            0x0FED_CBA9_8765_4321u64,
        )
    })
}

/// Cache fingerprint of the hidden column slot (`__hidden_first_value(col)`),
/// computed directly from the wrapped column reference; matches what
/// [`aggregate_key`] yields for the wrapped whole. This lets the executor look up
/// the agg cache on aggregate output rows with a bare column reference, without
/// constructing a wrapped `Expr::Function` on the fly. Only column-reference shapes are supported (the planner wraps only those two); identifiers are hashed with ASCII lowercase folding, matching case-insensitive column-name semantics.
pub(crate) fn hidden_group_column_key(expr: &Expr) -> Option<u64> {
    use std::hash::{BuildHasher, Hasher};
    let mut h = aggregate_key_state().build_hasher();
    h.write(b"__hidden_first_value\0");
    fn write_ident<H: Hasher>(h: &mut H, part: &str) {
        for b in part.bytes() {
            h.write_u8(b.to_ascii_lowercase());
        }
        h.write_u8(0);
    }
    match expr {
        Expr::Identifier(name) => {
            h.write_u8(b'I');
            write_ident(&mut h, name);
        }
        Expr::CompoundIdentifier(parts) => {
            h.write_u8(b'C');
            for part in parts {
                write_ident(&mut h, part);
            }
        }
        _ => return None,
    }
    Some(h.finish())
}

/// Canonical aggregate-result key shared by planning and projection.
/// Function and column names fold case; literal values and expression shape
/// remain distinct. Ordinary calls hash without allocating or formatting ASTs.
pub(crate) fn aggregate_key(expr: &Expr) -> u64 {
    if let Expr::Function {
        name,
        args,
        distinct: false,
    } = expr
    {
        if name == crate::parser::HIDDEN_FIRST_VALUE_FN && args.len() == 1 {
            if let Some(key) = hidden_group_column_key(&args[0]) {
                return key;
            }
        }
    }
    // Cached plans are shared across SQL case variants, while projection
    // expressions retain their source spelling for result-column labels.
    // Debug text distinguishes COUNT/couNT and makes a cache hit look like an
    // empty aggregate group. Use semantic structure, including nested calls,
    // column names, DISTINCT and literal bytes, on both sides of the lookup.
    crate::engine::expr_program::expr_fingerprint(expr)
}

/// One aggregate function call discovered in the projection / HAVING /
/// ORDER BY of a SELECT.
#[derive(Debug, Clone)]
pub(super) struct AggregateSlot {
    pub comparison_collation: Option<u16>,
    /// `aggregate_key(expr)` — used as the `PlanRow::agg_values` cache
    /// key. `u64` (Copy) instead of `String` so the per-row HAVING /
    /// projection lookup avoids a `format!()` allocation; see
    /// `aggregate_key` docs.
    pub key: u64,
    pub name: String,
    pub args: Vec<Expr>,
    pub distinct: bool,
    pub separator: Option<String>,
    /// True for `COUNT(*)`: argument evaluation is skipped and every
    /// input row counts as 1.
    pub is_count_star: bool,
    /// MySQL 8.0 `GROUP_CONCAT(... ORDER BY ord1 [ASC|DESC], ...)` and
    /// `JSON_ARRAYAGG(... ORDER BY ...)` sort their per-row contributions
    /// at finalize time. Empty for every other aggregate, and empty for
    /// `GROUP_CONCAT` calls that didn't include an `ORDER BY`.
    pub order_by: Vec<AggregateOrderKey>,
    pub binary_width_hint: Option<usize>,
}

/// Canonical aggregate operation shared by row, batch, parallel and spill
/// kernels.  Physical layouts may encode the state differently, but they must
/// not maintain independent capability lists: planning and finalization reason
/// about this semantic operation only.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum AggregateKind {
    CountStar,
    Count,
    Sum,
    Min,
    Max,
    Avg,
    BitAnd,
    BitOr,
    BitXor,
    VarPop,
    VarSamp,
    StddevPop,
    StddevSamp,
    CountDistinct,
    SumDistinct,
    AvgDistinct,
}

impl AggregateKind {
    pub(crate) fn from_sql(name: &str, distinct: bool, count_star: bool) -> Option<Self> {
        let name = name.to_ascii_uppercase();
        Some(match (name.as_str(), distinct, count_star) {
            ("COUNT", false, true) => Self::CountStar,
            ("COUNT", false, false) => Self::Count,
            ("COUNT", true, false) => Self::CountDistinct,
            ("SUM", false, false) => Self::Sum,
            ("SUM", true, false) => Self::SumDistinct,
            ("AVG", false, false) => Self::Avg,
            ("AVG", true, false) => Self::AvgDistinct,
            ("MIN", false, false) => Self::Min,
            ("MAX", false, false) => Self::Max,
            ("BIT_AND", false, false) => Self::BitAnd,
            ("BIT_OR", false, false) => Self::BitOr,
            ("BIT_XOR", false, false) => Self::BitXor,
            ("STD" | "STDDEV" | "STDDEV_POP", false, false) => Self::StddevPop,
            ("STDDEV_SAMP", false, false) => Self::StddevSamp,
            ("VARIANCE" | "VAR_POP", false, false) => Self::VarPop,
            ("VAR_SAMP", false, false) => Self::VarSamp,
            _ => return None,
        })
    }

    pub(crate) fn result_type(self, arg_type: Option<ColumnType>) -> ColumnType {
        let legacy_name = match self {
            Self::CountStar | Self::Count | Self::CountDistinct => "COUNT",
            Self::Sum | Self::SumDistinct => "SUM",
            Self::Avg | Self::AvgDistinct => "AVG",
            Self::Min => "MIN",
            Self::Max => "MAX",
            Self::BitAnd | Self::BitOr | Self::BitXor => return ColumnType::LongLong,
            Self::VarPop | Self::VarSamp | Self::StddevPop | Self::StddevSamp => {
                return ColumnType::Double;
            }
        };
        super::aggregate_function_result_type(legacy_name, arg_type)
    }

    #[inline]
    pub(crate) fn is_bit(self) -> bool {
        matches!(self, Self::BitAnd | Self::BitOr | Self::BitXor)
    }

    #[inline]
    pub(crate) fn is_distinct(self) -> bool {
        matches!(
            self,
            Self::CountDistinct | Self::SumDistinct | Self::AvgDistinct
        )
    }

    #[inline]
    pub(crate) fn variance_flags(self) -> Option<(bool, bool)> {
        match self {
            Self::VarPop => Some((false, false)),
            Self::VarSamp => Some((true, false)),
            Self::StddevPop => Some((false, true)),
            Self::StddevSamp => Some((true, true)),
            _ => None,
        }
    }

    /// Whether partial states can be merged without retaining input order.
    pub(crate) fn is_unordered_mergeable(self) -> bool {
        true
    }

    /// Whether the state owns a per-group de-duplication set.
    pub(crate) fn uses_distinct_state(self) -> bool {
        self.is_distinct()
    }

    /// Whether the vector state uses the extensible BIT/variance lane.
    #[inline]
    pub(crate) fn uses_extra(self) -> bool {
        self.is_bit() || self.variance_flags().is_some()
    }
}

thread_local! {
    static BIT_AGGREGATE_WIDTH_HINTS: std::cell::RefCell<HashMap<u64, usize>> =
        std::cell::RefCell::new(HashMap::new());
}

pub(super) fn clear_bit_aggregate_width_hints() {
    BIT_AGGREGATE_WIDTH_HINTS.with(|hints| hints.borrow_mut().clear());
}

pub(super) fn register_bit_aggregate_width_hint(key: u64, width: usize) {
    BIT_AGGREGATE_WIDTH_HINTS.with(|hints| {
        hints.borrow_mut().insert(key, width);
    });
}

impl AggregateSlot {
    pub(super) fn from_expr(expr: &Expr) -> Option<Self> {
        match expr {
            Expr::Function {
                name,
                args,
                distinct,
            } if planner::is_aggregate_function(name) => {
                let upper = name.to_ascii_uppercase();
                let is_count_star =
                    upper == "COUNT" && args.len() == 1 && matches!(args[0], Expr::Wildcard);
                let mut slot_args = args.clone();
                if upper == "JSON_OBJECTAGG" && args.len() == 2 {
                    slot_args = vec![Expr::Function {
                        name: "JSON_ARRAY".to_string(),
                        args: args.clone(),
                        distinct: false,
                    }];
                }
                // GROUP_CONCAT (and, by extension, ORDER-BY-aware aggregates)
                // wrap their value expression in a synthetic `__gc_ordered`
                // call when an `ORDER BY` clause is present — see
                // `unwrap_group_concat_ordered` and the parser site that
                // builds it. Pull the value back out so the per-row feed
                // path evaluates it directly; stash the order keys on the
                // slot for finalize-time sorting.
                let mut order_by: Vec<AggregateOrderKey> = Vec::new();
                if upper == "GROUP_CONCAT" || upper == "JSON_ARRAYAGG" {
                    if let Some(first) = slot_args.first().cloned() {
                        if let Some((value, keys)) = unwrap_group_concat_ordered(&first) {
                            // Resolve character comparison weights while the input
                            // row's column metadata is available. Accumulators keep
                            // byte keys; numeric, temporal, binary and NULL values
                            // pass through unchanged. This also covers compiled
                            // feeds, rollup and mergeable aggregate states.
                            order_by = keys
                                .into_iter()
                                .map(|mut key| {
                                    key.expr = Expr::Function {
                                        name: "__DISTINCT_COLLATION_KEY".to_string(),
                                        args: vec![key.expr],
                                        distinct: false,
                                    };
                                    key
                                })
                                .collect();
                            slot_args[0] = value;
                        }
                    }
                }
                if upper == "COUNT" && *distinct {
                    // DISTINCT equality for character values is defined by
                    // the argument's collation (including PAD SPACE), not by
                    // the connection default stored in `ValueKey`. Convert
                    // only string values to their resolved comparison key;
                    // numeric/binary values pass through unchanged.
                    slot_args = slot_args
                        .into_iter()
                        .map(|arg| Expr::Function {
                            name: "__DISTINCT_COLLATION_KEY".to_string(),
                            args: vec![arg],
                            distinct: false,
                        })
                        .collect();

                    // Issue #66: multi-argument COUNT(DISTINCT a, b, ...)
                    // must dedup on the whole tuple, not just the first
                    // argument. Any NULL element excludes the row.
                    if slot_args.len() > 1 {
                        slot_args = vec![Expr::Function {
                            name: "__DISTINCT_TUPLE".to_string(),
                            args: slot_args,
                            distinct: false,
                        }];
                    }
                }
                let separator = if upper == "GROUP_CONCAT" {
                    slot_args.get(1).and_then(|expr| match expr {
                        Expr::Literal(crate::parser::Literal::String(s)) => Some(s.clone()),
                        _ => None,
                    })
                } else {
                    None
                };
                // GROUP_CONCAT evaluates its value argument in string context.
                // Reuse CONCAT's context-aware conversion so BIT(M) becomes
                // its fixed-width big-endian byte representation while normal
                // numeric values become decimal text. This wrapper is applied
                // once at slot construction and therefore reaches every
                // aggregate execution path (row, compiled and parallel).
                if upper == "GROUP_CONCAT" {
                    if let Some(value_arg) = slot_args.first_mut() {
                        *value_arg = Expr::Function {
                            name: "CONCAT".to_string(),
                            args: vec![value_arg.clone()],
                            distinct: false,
                        };
                    }
                }
                if upper == "GROUP_CONCAT" && *distinct {
                    if let Some(value) = slot_args.first() {
                        // Carry the comparison key separately from the displayed
                        // text through every ordered-feed and partial-merge path.
                        order_by.push(AggregateOrderKey {
                            expr: Expr::Function {
                                name: "__DISTINCT_COLLATION_KEY".to_string(),
                                args: vec![value.clone()],
                                distinct: false,
                            },
                            asc: true,
                            nulls_first: None,
                        });
                    }
                }
                if matches!(upper.as_str(), "BIT_AND" | "BIT_OR" | "BIT_XOR") {
                    if let Some(Expr::Literal(
                        Literal::HexBytes(bytes) | Literal::BitBytes(bytes),
                    )) = slot_args.first()
                    {
                        let value = bytes
                            .iter()
                            .fold(0u64, |value, byte| (value << 8) | u64::from(*byte));
                        slot_args[0] = Expr::Literal(Literal::UInt(value));
                    }
                }
                Some(Self {
                    comparison_collation: None,
                    key: aggregate_key(expr),
                    name: name.clone(),
                    args: slot_args,
                    distinct: *distinct,
                    separator,
                    is_count_star,
                    order_by,
                    binary_width_hint: BIT_AGGREGATE_WIDTH_HINTS
                        .with(|hints| hints.borrow().get(&aggregate_key(expr)).copied()),
                })
            }
            _ => None,
        }
    }

    pub fn make_accumulator(&self) -> Box<dyn Accumulator> {
        self.make_accumulator_with_collation(self.comparison_collation)
    }

    pub(super) fn make_accumulator_for_row(
        &self,
        row: &super::plan::PlanRow,
    ) -> Box<dyn Accumulator> {
        if !matches!(self.kind(), Some(AggregateKind::Min | AggregateKind::Max))
            || self.comparison_collation.is_some()
        {
            return self.make_accumulator();
        }
        let collation = self.args.first().and_then(|expr| {
            let schema = super::eval::input_schema_for_plan_row(row);
            crate::engine::expr_program::input_expr_type(expr, &schema).collation_id
        });
        self.make_accumulator_with_collation(collation)
    }

    pub(super) fn make_accumulator_with_collation(
        &self,
        collation: Option<u16>,
    ) -> Box<dyn Accumulator> {
        if matches!(self.kind(), Some(AggregateKind::Min | AggregateKind::Max)) {
            return Box::new(MinMaxAcc {
                is_min: self.kind() == Some(AggregateKind::Min),
                best: None,
                collation,
            });
        }
        let order_fields: Vec<SortField> = self
            .order_by
            .iter()
            .map(|k| SortField {
                ascending: k.asc,
                nulls_first: crate::parser::effective_nulls_first(k.asc, k.nulls_first),
                collation_id: None,
            })
            .collect();
        accumulator_for(
            self.kind(),
            &self.name,
            self.distinct,
            self.separator.clone(),
            order_fields,
            self.binary_width_hint,
        )
    }

    pub(crate) fn kind(&self) -> Option<AggregateKind> {
        AggregateKind::from_sql(&self.name, self.distinct, self.is_count_star)
    }

    /// Returns `true` when this slot needs the executor to evaluate
    /// per-row sort keys (i.e. the `GROUP_CONCAT(... ORDER BY ...)`
    /// shape). The feed path uses this to decide whether to take the
    /// faster `update`/`update_ref` route or the order-aware
    /// `update_ordered` path.
    pub fn has_order_by(&self) -> bool {
        !self.order_by.is_empty()
    }

    /// Evaluate this slot's `ORDER BY` keys against `row`. Returns an
    /// empty vec when `order_by` is empty (callers can skip the call to
    /// `update_ordered` in that case for the fast path).
    pub fn eval_order_keys<F>(&self, mut eval: F) -> Result<Vec<Value>, QueryError>
    where
        F: FnMut(&Expr) -> Result<Value, QueryError>,
    {
        let mut keys = Vec::with_capacity(self.order_by.len());
        for key in &self.order_by {
            keys.push(eval(&key.expr)?);
        }
        Ok(keys)
    }
}

/// Walk one or more expressions and return a deduplicated list of
/// aggregate slots. Order matches first-seen order.
pub(super) fn bind_slot_comparisons_for_row(
    slots: &mut [AggregateSlot],
    row: &super::plan::PlanRow,
) {
    if !slots
        .iter()
        .any(|slot| matches!(slot.kind(), Some(AggregateKind::Min | AggregateKind::Max)))
    {
        return;
    }
    let schema = super::eval::input_schema_for_plan_row(row);
    for slot in slots {
        if matches!(slot.kind(), Some(AggregateKind::Min | AggregateKind::Max)) {
            slot.comparison_collation = slot.args.first().and_then(|expr| {
                crate::engine::expr_program::input_expr_type(expr, &schema).collation_id
            });
        }
    }
}

/// Bind comparison domains once, before creating groups or parallel partials.
pub(super) fn bind_slot_comparisons(
    slots: &mut [AggregateSlot],
    input: &crate::planner::PhysicalPlan,
) {
    for slot in slots {
        if matches!(slot.kind(), Some(AggregateKind::Min | AggregateKind::Max)) {
            slot.comparison_collation = slot
                .args
                .first()
                .and_then(|arg| super::plan::physical_expr_type(input, arg).collation_id);
        }
    }
}

pub(super) fn collect_aggregate_slots(exprs: &[&Expr]) -> Vec<AggregateSlot> {
    // Slot key population per query is typically <100 so a HashMap is
    // overkill, but ahash keeps it cheap. The legacy std HashMap (SipHash)
    // showed up as a stable ~3% sample under TPC-H Q1 / Q9 / Q10 plan-build
    // when the query plan was re-evaluated many times in a session.
    let mut seen: hashbrown::HashMap<u64, usize, ahash::RandomState> =
        hashbrown::HashMap::with_hasher(ahash::RandomState::new());
    let mut out: Vec<AggregateSlot> = Vec::new();
    for e in exprs {
        walk_expr_for_aggregates(e, &mut seen, &mut out);
    }
    out
}

fn walk_expr_for_aggregates(
    expr: &Expr,
    seen: &mut hashbrown::HashMap<u64, usize, ahash::RandomState>,
    out: &mut Vec<AggregateSlot>,
) {
    if let Some(slot) = AggregateSlot::from_expr(expr) {
        if !seen.contains_key(&slot.key) {
            seen.insert(slot.key, out.len());
            out.push(slot);
        }
        // Aggregate function arguments themselves are evaluated against
        // a single row, never recursively aggregated, so no descend.
        return;
    }
    match expr {
        Expr::Unary { expr, .. } | Expr::IsNull(expr) | Expr::IsNotNull(expr) => {
            walk_expr_for_aggregates(expr, seen, out)
        }
        Expr::Binary { left, right, .. } => {
            walk_expr_for_aggregates(left, seen, out);
            walk_expr_for_aggregates(right, seen, out);
        }
        Expr::InList { expr, list, .. } => {
            walk_expr_for_aggregates(expr, seen, out);
            for item in list {
                walk_expr_for_aggregates(item, seen, out);
            }
        }
        Expr::Like { expr, pattern, .. } => {
            walk_expr_for_aggregates(expr, seen, out);
            walk_expr_for_aggregates(pattern, seen, out);
        }
        Expr::Between {
            expr, low, high, ..
        } => {
            walk_expr_for_aggregates(expr, seen, out);
            walk_expr_for_aggregates(low, seen, out);
            walk_expr_for_aggregates(high, seen, out);
        }
        Expr::Case {
            operand,
            conditions,
            results,
            else_result,
        } => {
            if let Some(operand) = operand {
                walk_expr_for_aggregates(operand, seen, out);
            }
            for c in conditions {
                walk_expr_for_aggregates(c, seen, out);
            }
            for r in results {
                walk_expr_for_aggregates(r, seen, out);
            }
            if let Some(else_result) = else_result {
                walk_expr_for_aggregates(else_result, seen, out);
            }
        }
        Expr::Function { args, .. } => {
            for a in args {
                walk_expr_for_aggregates(a, seen, out);
            }
        }
        Expr::Tuple(items) => {
            for item in items {
                walk_expr_for_aggregates(item, seen, out);
            }
        }
        Expr::GroupKeyRef { expr, .. } => walk_expr_for_aggregates(expr, seen, out),
        Expr::Grouping(_) => {}
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
                for aggregate in planner::source_free_outer_aggregates(inner) {
                    walk_expr_for_aggregates(&aggregate, seen, out);
                }
            }
        }
        // Other subqueries are evaluated separately; do not descend.
        _ => {}
    }
}

/// Per-group accumulator state. Implementors hold whatever is needed
/// to incrementally compute their final value.
pub(crate) trait Accumulator: Send + Sync + 'static {
    fn count_distinct_keys(&self) -> Option<&hashbrown::HashSet<ValueKey, ahash::RandomState>> {
        None
    }
    fn update(&mut self, value: Value) -> Result<(), QueryError>;
    /// Order-aware variant used by `GROUP_CONCAT(... ORDER BY ...)` and
    /// `JSON_ARRAYAGG(... ORDER BY ...)`. The default implementation
    /// ignores the sort keys and falls through to `update(value)` — so
    /// any accumulator that doesn't care about ordering keeps working
    /// unchanged. `GroupConcatAcc`/`JsonArrayAggAcc` override this to
    /// buffer `(sort_keys, value)` pairs and sort at finalize time.
    fn update_ordered(&mut self, value: Value, sort_keys: Vec<Value>) -> Result<(), QueryError> {
        let _ = sort_keys;
        self.update(value)
    }
    /// Hot-path borrowed-value variant. Default implementation clones,
    /// but accumulators that only need to read the value (SUM/AVG/MIN/MAX
    /// over numeric inputs, plain COUNT) override this to skip the
    /// `Value::clone` that otherwise allocates per row when the input
    /// is a `Decimal(String)` / `String` -- meaningful on analytical
    /// scans of `DECIMAL` columns aggregated over millions of rows.
    fn update_ref(&mut self, value: &Value) -> Result<(), QueryError> {
        self.update(value.clone())
    }
    /// P4: batched update entry point. The default implementation just
    /// loops over `update_ref`, but accumulators that have an internal
    /// fast-path (CountAcc += N, SumAcc accumulating a fixed-magnitude
    /// run, MinMaxAcc skipping the per-row comparator) override this to
    /// drop per-row dyn-dispatch overhead. Callers that have a column
    /// of values ready (e.g. the implicit-single-group path of
    /// `aggregate_plan_rows`, or the vec-engine bridge) should prefer
    /// `update_batch` over a hand-rolled loop of `update_ref`.
    fn update_batch(&mut self, values: &[Value]) -> Result<(), QueryError> {
        for v in values {
            self.update_ref(v)?;
        }
        Ok(())
    }
    /// Apply one value with a known logical multiplicity. Join pipelines use
    /// this when a probe row matches several build rows, avoiding a synthetic
    /// joined-row batch. Implementations may collapse the repetition when it
    /// is algebraically safe; the default preserves the scalar update order.
    #[cfg(test)]
    fn update_repeated_ref(&mut self, value: &Value, n: usize) -> Result<(), QueryError> {
        for _ in 0..n {
            self.update_ref(value)?;
        }
        Ok(())
    }
    /// Hot-path entry that takes a decimal value as raw `(magnitude,
    /// scale)` parts, sidestepping `Value` construction and the
    /// per-variant clone in `update_ref`. SUM/AVG override this to feed
    /// the i128 magnitude straight into the `NumericAggState` exact
    /// path. Default implementation wraps the parts back into a
    /// `Value::DecimalCompact` and falls through to `update_ref`, which
    /// is correct for every other accumulator (MIN/MAX/COUNT/COUNT
    /// DISTINCT) and keeps the trait additive.
    fn update_decimal_parts(&mut self, magnitude: i128, scale: u8) -> Result<(), QueryError> {
        let value = if let Ok(mag) = i64::try_from(magnitude) {
            Value::DecimalCompact(mag, scale)
        } else {
            Value::Decimal(format_i128_decimal(magnitude, scale))
        };
        self.update_ref(&value)
    }
    fn update_i64(&mut self, value: i64) -> Result<(), QueryError> {
        self.update_ref(&Value::Int(value))
    }
    fn update_u64(&mut self, value: u64) -> Result<(), QueryError> {
        self.update_ref(&Value::UInt(value))
    }
    fn update_f64(&mut self, value: f64) -> Result<(), QueryError> {
        self.update_ref(&Value::Double(value))
    }
    /// Special case: COUNT(*) ignores its argument; the executor calls
    /// `update_count_star` on every input row instead of evaluating
    /// the argument.
    fn update_count_star(&mut self) {
        let _ = self.update(Value::Int(1));
    }
    /// P4 batched COUNT(*): adds `n` non-NULL "rows" at once. Default
    /// implementation calls `update_count_star` `n` times; CountAcc
    /// overrides this to bump its counter directly.
    fn update_count_star_batch(&mut self, n: usize) {
        for _ in 0..n {
            self.update_count_star();
        }
    }
    fn finalize(self: Box<Self>) -> Result<Value, QueryError>;
    /// Approximate memory used by this accumulator's variable-size
    /// state (HashSet for DISTINCT, etc.). Best-effort.
    #[allow(dead_code)]
    fn heap_bytes(&self) -> usize {
        0
    }
    /// The concrete state object is heap-allocated behind Box<dyn Accumulator>.
    /// Its vtable size is separate from the allocations reported by heap_bytes.
    fn resident_bytes(&self) -> usize {
        std::mem::size_of_val(self).saturating_add(self.heap_bytes())
    }
    /// Convert into Any for downcasting during parallel merge.
    fn into_any(self: Box<Self>) -> Box<dyn std::any::Any + Send>;
    /// Merge another accumulator of the same concrete type into this one.
    /// The argument is the result of `into_any()` on the source accumulator.
    fn merge_from(&mut self, other: Box<dyn std::any::Any + Send>);
    /// Duplicate this accumulator's full mergeable state into a fresh box.
    /// Used by the two-phase ("subtotal-derived") ROLLUP path to derive
    /// coarser super-aggregate levels from the finest grouping without
    /// re-scanning the input: a finest group's state is cloned and
    /// `merge_from`-ed up the prefix chain, leaving the finest group intact
    /// for its own output row. The clone is the same associative monoid
    /// `merge_from` already combines, so it is correct for every
    /// accumulator (AVG carries (sum,count); COUNT DISTINCT carries its
    /// set; variance carries n/mean/M2).
    fn clone_box(&self) -> Box<dyn Accumulator>;
}

fn accumulator_for(
    kind: Option<AggregateKind>,
    name: &str,
    distinct: bool,
    separator: Option<String>,
    order_fields: Vec<SortField>,
    binary_width_hint: Option<usize>,
) -> Box<dyn Accumulator> {
    match kind {
        Some(AggregateKind::CountStar | AggregateKind::Count) => Box::new(CountAcc::default()),
        Some(AggregateKind::CountDistinct) => Box::new(CountDistinctAcc::default()),
        Some(AggregateKind::Sum) => Box::new(SumAcc::default()),
        Some(AggregateKind::SumDistinct) => Box::new(DistinctReducer::new(SumAcc::default())),
        Some(AggregateKind::Avg) => Box::new(AvgAcc::default()),
        Some(AggregateKind::AvgDistinct) => Box::new(DistinctReducer::new(AvgAcc::default())),
        Some(AggregateKind::Min) => Box::new(MinMaxAcc::new(true)),
        Some(AggregateKind::Max) => Box::new(MinMaxAcc::new(false)),
        Some(AggregateKind::BitAnd) => Box::new(BitAcc::new("BIT_AND", binary_width_hint)),
        Some(AggregateKind::BitOr) => Box::new(BitAcc::new("BIT_OR", binary_width_hint)),
        Some(AggregateKind::BitXor) => Box::new(BitAcc::new("BIT_XOR", binary_width_hint)),
        Some(AggregateKind::VarPop) => Box::new(VarianceAcc::new(false, false)),
        Some(AggregateKind::VarSamp) => Box::new(VarianceAcc::new(true, false)),
        Some(AggregateKind::StddevPop) => Box::new(VarianceAcc::new(false, true)),
        Some(AggregateKind::StddevSamp) => Box::new(VarianceAcc::new(true, true)),
        None => match name.to_ascii_uppercase().as_str() {
            "GROUP_CONCAT" => Box::new(GroupConcatAcc::new(
                distinct,
                separator.unwrap_or_else(|| ",".to_string()),
                order_fields,
                group_concat_max_len(),
            )),
            "JSON_ARRAYAGG" => Box::new(JsonArrayAggAcc::new(order_fields)),
            "JSON_OBJECTAGG" => Box::new(JsonObjectAggAcc::default()),
            "ST_COLLECT" => Box::new(SpatialCollectAcc::new(distinct)),
            "__HIDDEN_FIRST_VALUE" => Box::new(FirstValueAcc::default()),
            _ => Box::new(UnsupportedAcc {
                name: name.to_string(),
            }),
        },
    }
}

#[derive(Clone)]
struct SpatialCollectAcc {
    distinct: bool,
    seen: HashSet<Vec<u8>>,
    values: Vec<Vec<u8>>,
    srid: Option<u32>,
    merge_srid_mismatch: Option<(u32, u32)>,
}

impl SpatialCollectAcc {
    fn new(distinct: bool) -> Self {
        Self {
            distinct,
            seen: HashSet::new(),
            values: Vec::new(),
            srid: None,
            merge_srid_mismatch: None,
        }
    }
}

impl Accumulator for SpatialCollectAcc {
    fn update(&mut self, value: Value) -> Result<(), QueryError> {
        let Value::Geometry(bytes) = value else {
            if matches!(value, Value::Null) {
                return Ok(());
            }
            return Err(invalid_gis_data("ST_Collect"));
        };
        let geometry = read_geometry(&bytes)?;
        if let Some(srid) = self.srid {
            if geometry.srid != srid {
                return Err(srid_mismatch("ST_Collect", srid, geometry.srid));
            }
        } else {
            self.srid = Some(geometry.srid);
        }
        if !self.distinct || self.seen.insert(bytes.clone()) {
            self.values.push(bytes);
        }
        Ok(())
    }

    fn finalize(self: Box<Self>) -> Result<Value, QueryError> {
        if let Some((left, right)) = self.merge_srid_mismatch {
            return Err(srid_mismatch("ST_Collect", left, right));
        }
        let Some(srid) = self.srid else {
            return Ok(Value::Null);
        };
        let mut geometries = Vec::with_capacity(self.values.len());
        for bytes in self.values {
            geometries.push(read_geometry(&bytes)?.geom);
        }
        let simple_type = geometries.first().map(Geom::type_code);
        let same_simple_type = simple_type.is_some_and(|kind| {
            matches!(kind, WKB_POINT | WKB_LINESTRING | WKB_POLYGON)
                && geometries
                    .iter()
                    .all(|geometry| geometry.type_code() == kind)
        });
        let geom = match (simple_type, same_simple_type) {
            (Some(WKB_POINT), true) => Geom::MultiPoint(
                geometries
                    .into_iter()
                    .map(|geometry| match geometry {
                        Geom::Point(point) => point,
                        _ => unreachable!(),
                    })
                    .collect(),
            ),
            (Some(WKB_LINESTRING), true) => Geom::MultiLineString(
                geometries
                    .into_iter()
                    .map(|geometry| match geometry {
                        Geom::LineString(line) => line,
                        _ => unreachable!(),
                    })
                    .collect(),
            ),
            (Some(WKB_POLYGON), true) => Geom::MultiPolygon(
                geometries
                    .into_iter()
                    .map(|geometry| match geometry {
                        Geom::Polygon(polygon) => polygon,
                        _ => unreachable!(),
                    })
                    .collect(),
            ),
            _ => Geom::GeometryCollection(geometries),
        };
        Ok(Value::Geometry(write_geometry(&Geometry::new(srid, geom))))
    }

    fn heap_bytes(&self) -> usize {
        self.values
            .capacity()
            .saturating_mul(std::mem::size_of::<Vec<u8>>())
            .saturating_add(
                self.values
                    .iter()
                    .map(|bytes| bytes.capacity())
                    .sum::<usize>(),
            )
            .saturating_add(
                self.seen
                    .capacity()
                    .saturating_mul(std::mem::size_of::<Vec<u8>>()),
            )
    }

    fn into_any(self: Box<Self>) -> Box<dyn std::any::Any + Send> {
        self
    }

    fn merge_from(&mut self, other: Box<dyn std::any::Any + Send>) {
        let other = *other
            .downcast::<Self>()
            .expect("SpatialCollectAcc type mismatch");
        if let (Some(left), Some(right)) = (self.srid, other.srid) {
            if left != right {
                // The merge trait is infallible; defer the MySQL-compatible
                // error until finalize instead of silently combining SRIDs.
                self.merge_srid_mismatch = Some((left, right));
                return;
            }
        } else if self.srid.is_none() {
            self.srid = other.srid;
        }
        for bytes in other.values {
            if !self.distinct || self.seen.insert(bytes.clone()) {
                self.values.push(bytes);
            }
        }
    }

    fn clone_box(&self) -> Box<dyn Accumulator> {
        Box::new(self.clone())
    }
}

/// Is this slot's aggregate function seedable from a vectorized finest
/// grouping's `(sum, count, min, max)` snapshot? Only the funcs whose
/// row accumulator is *fully determined* by those four fields qualify —
/// SUM/AVG via the numeric state + count, COUNT via the count, MIN/MAX
/// via the value. Excluded: DISTINCT (needs the dedup set), BIT_* /
/// variance (state lives in vec's `extra`, not carried here),
/// GROUP_CONCAT / JSON_*AGG (ordered byte buffers), and the hidden
/// `__hidden_first_value` column (FirstValueAcc). Those ROLLUP shapes
/// stay on the row two-phase path, which is the correctness oracle.
pub(super) fn rollup_vec_seedable_slot(slot: &AggregateSlot) -> bool {
    if slot.distinct {
        return false;
    }
    matches!(
        slot.name.to_ascii_uppercase().as_str(),
        "COUNT" | "SUM" | "MIN" | "MAX" | "AVG"
    )
}

/// Build a row accumulator already holding the state a vectorized finest
/// group accumulated, so the existing two-phase ROLLUP derive
/// (`clone_box` / `merge_from`) and `emit_rollup_group_state` run
/// unchanged on top of a vectorized phase 1. The seed is *exact*: the vec
/// path accumulates through the same `accumulate_numeric_state` /
/// `compare_values` kernels as the row accumulators, so the seeded state
/// is byte-identical to what the row path would have built for that group.
///
/// Precondition: `rollup_vec_seedable_slot(slot)` is true.
pub(super) fn seed_rollup_accumulator(
    slot: &AggregateSlot,
    sum: &NumericAggState,
    count: u64,
    min: &Option<Value>,
    max: &Option<Value>,
) -> Box<dyn Accumulator> {
    seed_rollup_accumulator_owned(slot, sum.clone(), count, min.clone(), max.clone())
}

pub(super) fn seed_rollup_accumulator_owned(
    slot: &AggregateSlot,
    sum: NumericAggState,
    count: u64,
    min: Option<Value>,
    max: Option<Value>,
) -> Box<dyn Accumulator> {
    match slot.name.to_ascii_uppercase().as_str() {
        // COUNT(*) and COUNT(col) both reduce to a running tally; the vec
        // snapshot's `count` is already the right value (total rows for
        // CountStar, non-NULL rows for Count).
        "COUNT" => Box::new(CountAcc { n: count as i64 }),
        "SUM" => Box::new(SumAcc { state: sum }),
        "AVG" => Box::new(AvgAcc {
            state: sum,
            n: count,
        }),
        "MIN" => Box::new(MinMaxAcc {
            collation: slot.comparison_collation,
            is_min: true,
            best: min,
        }),
        "MAX" => Box::new(MinMaxAcc {
            collation: slot.comparison_collation,
            is_min: false,
            best: max,
        }),
        other => unreachable!("seed_rollup_accumulator on unsupported {other}"),
    }
}

/// MySQL 8.0 `group_concat_max_len` — the maximum byte length of a
/// `GROUP_CONCAT` result before the executor truncates and surfaces a
/// warning. MySQL's default is `1024`; mydb mirrors that default.
///
/// The active value is published per session through
/// [`with_group_concat_max_len`] which runs the closure with the
/// supplied cap installed in a thread-local; `accumulator_for` reads
/// the cap when constructing `GroupConcatAcc` so each
/// `SET SESSION group_concat_max_len = N` takes effect on the next
/// query without re-threading SessionContext through every aggregator.
pub(crate) fn group_concat_max_len() -> usize {
    GROUP_CONCAT_MAX_LEN.with(|cell| cell.get())
}

thread_local! {
    static GROUP_CONCAT_MAX_LEN: std::cell::Cell<usize> = const { std::cell::Cell::new(1024) };
}

/// Restores the caller's aggregate limit at statement or worker scope exit.
pub(crate) struct GroupConcatLimitGuard(usize);

impl Drop for GroupConcatLimitGuard {
    fn drop(&mut self) {
        GROUP_CONCAT_MAX_LEN.with(|cell| cell.set(self.0));
    }
}

pub(crate) fn enter_group_concat_max_len(value: u64) -> GroupConcatLimitGuard {
    let value = value.try_into().unwrap_or(usize::MAX);
    GroupConcatLimitGuard(GROUP_CONCAT_MAX_LEN.with(|cell| cell.replace(value)))
}

/// Run a synchronous operation with the session's aggregate limit.
pub fn with_group_concat_max_len<F, R>(value: u64, f: F) -> R
where
    F: FnOnce() -> R,
{
    let _guard = enter_group_concat_max_len(value);
    f()
}

#[derive(Clone, Default, Debug)]
pub(crate) struct ExactDecimalAcc {
    magnitude: i128,
    /// Overflow storage is cold: ordinary DECIMAL SUM/AVG groups stay in
    /// `magnitude` for their whole lifetime. Keeping `BigInt` inline made
    /// every group pay its multi-word object size even when `wide` was None,
    /// inflating high-cardinality hash aggregates enough to force spill.
    /// Box only the exceptional promoted state; its allocation remains owned
    /// by the group and is included by `heap_bytes`.
    wide: Option<Box<num_bigint::BigInt>>,
    scale: u8,
    seen: bool,
}

impl ExactDecimalAcc {
    fn heap_bytes(&self) -> usize {
        self.wide.as_ref().map_or(0, |value| {
            let limbs = (value.bits().div_ceil(usize::BITS as u64) as usize)
                .max(1)
                .next_power_of_two();
            std::mem::size_of::<num_bigint::BigInt>()
                .saturating_add(limbs.saturating_mul(std::mem::size_of::<usize>()))
        })
    }

    fn add_value(&mut self, value: &Value) -> Option<()> {
        match value.exact_decimal()? {
            crate::util::decimal::ExactDecimal::I64(magnitude, scale) => {
                self.add_parts(i128::from(magnitude), scale)
            }
            crate::util::decimal::ExactDecimal::I128(magnitude, scale) => {
                self.add_parts(magnitude, scale)
            }
            crate::util::decimal::ExactDecimal::Big(magnitude, scale) => {
                self.add_big_parts(magnitude, scale)
            }
        }
    }

    #[inline]
    fn add_parts(&mut self, magnitude: i128, scale: u8) -> Option<()> {
        if !self.seen {
            self.magnitude = magnitude;
            self.wide = None;
            self.scale = scale;
            self.seen = true;
            return Some(());
        }
        if let Some(wide) = self.wide.as_mut() {
            let mut right = num_bigint::BigInt::from(magnitude);
            let target_scale = self.scale.max(scale);
            if self.scale < target_scale {
                **wide *= num_bigint::BigInt::from(10u8).pow((target_scale - self.scale) as u32);
            }
            if scale < target_scale {
                right *= num_bigint::BigInt::from(10u8).pow((target_scale - scale) as u32);
            }
            **wide += right;
            self.scale = target_scale;
            return Some(());
        }
        // Hot path: every contributing row has the same scale (true for
        // every TPC-H DECIMAL column and the common case for analytical
        // SUM/AVG over a single source column). Skip the
        // `scale_up_decimal` dispatch entirely — it would short-circuit
        // to `mag` anyway when `from == to`. Saving one cmp + branch +
        // mul of `10^0` per row is meaningful at 6M rows × 8 aggregates
        // on TPC-H Q1.
        if self.scale == scale {
            if let Some(sum) = self.magnitude.checked_add(magnitude) {
                self.magnitude = sum;
            } else {
                self.wide = Some(Box::new(
                    num_bigint::BigInt::from(self.magnitude) + num_bigint::BigInt::from(magnitude),
                ));
            }
            return Some(());
        }
        let target_scale = self.scale.max(scale);
        if let (Some(left), Some(right)) = (
            scale_up_decimal(self.magnitude, self.scale, target_scale),
            scale_up_decimal(magnitude, scale, target_scale),
        ) {
            if let Some(sum) = left.checked_add(right) {
                self.magnitude = sum;
                self.scale = target_scale;
                return Some(());
            }
        }
        let mut left = num_bigint::BigInt::from(self.magnitude);
        let mut right = num_bigint::BigInt::from(magnitude);
        if self.scale < target_scale {
            left *= num_bigint::BigInt::from(10u8).pow((target_scale - self.scale) as u32);
        }
        if scale < target_scale {
            right *= num_bigint::BigInt::from(10u8).pow((target_scale - scale) as u32);
        }
        self.wide = Some(Box::new(left + right));
        self.scale = target_scale;
        Some(())
    }

    fn add_big_parts(&mut self, magnitude: num_bigint::BigInt, scale: u8) -> Option<()> {
        if !self.seen {
            self.magnitude = 0;
            self.wide = Some(Box::new(magnitude));
            self.scale = scale;
            self.seen = true;
            return Some(());
        }
        let target_scale = self.scale.max(scale);
        let mut left = self
            .wide
            .take()
            .map(|value| *value)
            .unwrap_or_else(|| num_bigint::BigInt::from(self.magnitude));
        let mut right = magnitude;
        if self.scale < target_scale {
            left *= num_bigint::BigInt::from(10u8).pow((target_scale - self.scale) as u32);
        }
        if scale < target_scale {
            right *= num_bigint::BigInt::from(10u8).pow((target_scale - scale) as u32);
        }
        self.wide = Some(Box::new(left + right));
        self.scale = target_scale;
        Some(())
    }

    fn exact_value(&self) -> Option<crate::util::decimal::ExactDecimal> {
        if !self.seen {
            None
        } else if let Some(value) = &self.wide {
            Some(crate::util::decimal::ExactDecimal::from_big(
                value.as_ref().clone(),
                self.scale,
            ))
        } else {
            Some(crate::util::decimal::ExactDecimal::from_i128(
                self.magnitude,
                self.scale,
            ))
        }
    }

    fn to_f64(&self) -> f64 {
        if !self.seen {
            0.0
        } else if let Some(value) = &self.wide {
            crate::util::decimal::format_big_decimal_parts(value, self.scale)
                .parse::<f64>()
                .unwrap_or(if value.sign() == num_bigint::Sign::Minus {
                    f64::NEG_INFINITY
                } else {
                    f64::INFINITY
                })
        } else {
            self.magnitude as f64 / 10f64.powi(self.scale as i32)
        }
    }

    /// Inverse of [`add_parts`]. Used by the sliding-window aggregate to
    /// remove a value that has left the frame. The caller guarantees the
    /// value was previously added (so `seen` is already true) — exact
    /// i128 subtraction is the precise inverse of the i128 addition, so a
    /// slide produces the identical magnitude a fresh re-accumulation
    /// would (integer arithmetic is associative). Returns `None` on
    /// overflow / non-representable scale so the caller can degrade.
    #[inline]
    fn sub_parts(&mut self, magnitude: i128, scale: u8) -> Option<()> {
        if !self.seen {
            if let Some(value) = magnitude.checked_neg() {
                self.magnitude = value;
                self.wide = None;
            } else {
                self.magnitude = 0;
                self.wide = Some(Box::new(-num_bigint::BigInt::from(magnitude)));
            }
            self.scale = scale;
            self.seen = true;
            return Some(());
        }
        self.add_parts(magnitude.checked_neg().unwrap_or(magnitude), scale)?;
        if magnitude == i128::MIN {
            // `checked_neg` cannot represent +2^127; correct the provisional
            // add by promoting and adding 2^128.
            let wide = self
                .wide
                .get_or_insert_with(|| Box::new(num_bigint::BigInt::from(self.magnitude)));
            **wide += num_bigint::BigInt::from(1u8) << 128usize;
        }
        Some(())
    }
}

#[derive(Default, Debug, Clone)]
pub(crate) enum NumericAggState {
    #[default]
    Empty,
    Exact(ExactDecimalAcc),
    Float(f64),
}

pub(crate) fn accumulate_numeric_state(
    state: &mut NumericAggState,
    value: &Value,
) -> Result<(), QueryError> {
    if matches!(value, Value::Null) {
        return Ok(());
    }
    // Hot path: state has already been bootstrapped to Exact. Mutate in
    // place — skips the `mem::replace` round-trip and the enum
    // re-construction. Falls through to the slow path on overflow / a
    // non-exact operand that the exact accumulator can't absorb.
    if let NumericAggState::Exact(acc) = state {
        if acc.add_value(value).is_some() {
            return Ok(());
        }
        let degraded = acc.to_f64() + numeric(value)?;
        *state = NumericAggState::Float(degraded);
        return Ok(());
    }
    // Cold paths: Empty (first row in group) or already-degraded Float.
    match state {
        NumericAggState::Empty => {
            if value.exact_decimal().is_some() {
                let mut acc = ExactDecimalAcc::default();
                let _ = acc.add_value(value);
                *state = NumericAggState::Exact(acc);
            } else {
                *state = NumericAggState::Float(numeric(value)?);
            }
        }
        NumericAggState::Float(total) => {
            *total += numeric(value)?;
        }
        NumericAggState::Exact(_) => {
            debug_assert!(false, "Exact state should be handled above");
        }
    }
    Ok(())
}

/// Remove a previously-accumulated value from `state` — the inverse of
/// [`accumulate_numeric_state`]. Used only by the vectorized sliding-
/// window aggregate, which adds rows entering the frame and subtracts
/// rows leaving it. For an `Exact` state this is exact i128 subtraction
/// (the precise inverse of the add), so sliding yields the identical
/// magnitude a fresh per-frame re-accumulation would. A `Float` state
/// subtracts in f64 — only ever reached for float inputs, which the
/// window fast path does not route to vec for exactly this reason
/// (f64 add/subtract is not associative and would drift from the
/// legacy fresh-recompute path).
pub(crate) fn subtract_numeric_state(
    state: &mut NumericAggState,
    value: &Value,
) -> Result<(), QueryError> {
    if matches!(value, Value::Null) {
        return Ok(());
    }
    if let NumericAggState::Exact(acc) = state {
        if let Some(exact) = value.exact_decimal() {
            match exact {
                crate::util::decimal::ExactDecimal::I64(magnitude, scale) => {
                    let _ = acc.sub_parts(i128::from(magnitude), scale);
                }
                crate::util::decimal::ExactDecimal::I128(magnitude, scale) => {
                    let _ = acc.sub_parts(magnitude, scale);
                }
                crate::util::decimal::ExactDecimal::Big(magnitude, scale) => {
                    let _ = acc.add_big_parts(-magnitude, scale);
                }
            }
            return Ok(());
        }
        let degraded = acc.to_f64() - numeric(value)?;
        *state = NumericAggState::Float(degraded);
        return Ok(());
    }
    match state {
        NumericAggState::Empty => {
            // Subtracting before adding: only reachable via misuse. Treat
            // as starting from zero so the magnitude is the value's
            // negation (mirrors `sub_parts`'s unseen branch).
            if value.exact_decimal().is_some() {
                let mut acc = ExactDecimalAcc::default();
                if let Some(exact) = value.exact_decimal() {
                    let negated = match exact {
                        crate::util::decimal::ExactDecimal::I64(v, s) => {
                            crate::util::decimal::ExactDecimal::from_i128(-i128::from(v), s)
                        }
                        crate::util::decimal::ExactDecimal::I128(v, s) => {
                            crate::util::decimal::ExactDecimal::from_big(
                                -num_bigint::BigInt::from(v),
                                s,
                            )
                        }
                        crate::util::decimal::ExactDecimal::Big(v, s) => {
                            crate::util::decimal::ExactDecimal::from_big(-v, s)
                        }
                    };
                    let value = Value::from_exact_decimal(negated);
                    let _ = acc.add_value(&value);
                    *state = NumericAggState::Exact(acc);
                } else {
                    *state = NumericAggState::Float(-numeric(value)?);
                }
            } else {
                *state = NumericAggState::Float(-numeric(value)?);
            }
        }
        NumericAggState::Float(total) => {
            *total -= numeric(value)?;
        }
        NumericAggState::Exact(_) => {
            debug_assert!(false, "Exact state handled above");
        }
    }
    Ok(())
}

/// Parts-level fast path for callers that already have the decimal
/// magnitude / scale extracted — typically a vectorized operator that
/// pulled the value straight out of a `ColumnVector::Decimal{32,64,128}`
/// lane. Skips the `Value::DecimalCompact` boxing + `match` dispatch that
/// `accumulate_numeric_state` does once per row.
///
/// Semantics are identical to feeding `Value::DecimalCompact(mag, scale)`
/// (when it fits i64) or the equivalent `Value::Decimal(..)` (when it
/// doesn't) into `accumulate_numeric_state` — same overflow → f64 fall-
/// back, same scale alignment.
#[inline]
pub(crate) fn accumulate_numeric_state_decimal_parts(
    state: &mut NumericAggState,
    magnitude: i128,
    scale: u8,
) {
    // Hot path first: after the first row of every group the state is
    // already `Exact`, so the previous `mem::replace + match` was paying
    // a 24-byte enum swap per row. In-place mutation eliminates that;
    // `add_parts` itself is now a tight i128 add when scales match.
    if let NumericAggState::Exact(acc) = state {
        let _ = acc.add_parts(magnitude, scale);
        return;
    }
    // Cold paths: Empty (first row in this group) or already Float.
    match state {
        NumericAggState::Empty => {
            *state = NumericAggState::Exact(ExactDecimalAcc {
                magnitude,
                wide: None,
                scale,
                seen: true,
            });
        }
        NumericAggState::Float(total) => {
            *total += magnitude as f64 / 10f64.powi(scale as i32);
        }
        NumericAggState::Exact(_) => {
            // Already handled by the hot-path return above.
            debug_assert!(false, "Exact state should be handled above");
        }
    }
}

/// Float-lane counterpart of [`accumulate_numeric_state_decimal_parts`].
/// Vector operators already holding an `f64` must not materialise a
/// `Value::Double` merely to feed SUM/AVG. Once an approximate operand is
/// observed the aggregate follows the same degradation rule as the row
/// oracle: an exact prefix is converted once and all subsequent additions
/// stay in the floating state.
#[inline]
pub(crate) fn accumulate_numeric_state_float(state: &mut NumericAggState, value: f64) {
    match state {
        NumericAggState::Empty => *state = NumericAggState::Float(value),
        NumericAggState::Float(total) => *total += value,
        NumericAggState::Exact(acc) => {
            *state = NumericAggState::Float(acc.to_f64() + value);
        }
    }
}

pub(crate) fn numeric_state_to_f64(state: &NumericAggState) -> f64 {
    match state {
        NumericAggState::Empty => 0.0,
        NumericAggState::Exact(acc) => acc.to_f64(),
        NumericAggState::Float(total) => *total,
    }
}

/// Serialise an accumulator into a single `Value` that, when fed back
/// through `accumulate_numeric_state`, reproduces the same state. Used
/// by the vectorized HashAggregate spill path to round-trip a partial
/// SUM through a `Batch` column without losing exact-decimal precision:
/// an `Exact` state becomes a `DecimalCompact` (or `Decimal` string when
/// the i128 magnitude overflows i64), a `Float` state stays `Double`.
pub(crate) fn numeric_state_to_value(state: &NumericAggState) -> Value {
    match state {
        NumericAggState::Empty => Value::Null,
        NumericAggState::Float(total) => Value::Double(*total),
        NumericAggState::Exact(acc) => acc
            .exact_value()
            .map(Value::from_exact_decimal)
            .unwrap_or(Value::Null),
    }
}

/// Heap payload owned by a numeric aggregate state. Physical aggregate
/// layouts use this to charge cold exact-overflow storage to the unified
/// memory governor without exposing the accumulator's representation.
pub(crate) fn numeric_state_heap_bytes(state: &NumericAggState) -> usize {
    match state {
        NumericAggState::Exact(acc) => acc.heap_bytes(),
        NumericAggState::Empty | NumericAggState::Float(_) => 0,
    }
}

/// Finalise AVG with MySQL's result-type rule: an exact (integer /
/// DECIMAL) input keeps fixed-point precision and gains
/// `div_precision_increment` fractional digits (AVG over INT = scale 4,
/// over DECIMAL(10,2) = scale 6); approximate inputs stay DOUBLE. Shared
/// by the row-wise `AvgAcc`, the vectorized hash aggregate and the window
/// frame evaluator so all paths agree — when changing this, keep them in
/// sync with the `MYDB_VECTORIZED` routing oracle.
pub(crate) fn finalize_avg_value(state: &NumericAggState, n: u64) -> Value {
    if n == 0 {
        return Value::Null;
    }
    if let NumericAggState::Exact(acc) = state {
        if let Some(total) = acc.exact_value() {
            let incr = crate::engine::expr::session_div_precision_increment();
            let target_scale = (acc.scale as u32 + incr as u32)
                .min(crate::util::decimal::MAX_DECIMAL_SCALE as u32)
                as u8;
            let divisor = crate::util::decimal::ExactDecimal::from_big(n.into(), 0);
            let context = crate::util::decimal::DecimalContext {
                result: Some(crate::util::decimal::DecimalResultSpec::new(
                    65,
                    target_scale,
                )),
                div_precision_increment: incr,
                ..Default::default()
            };
            if let Ok(Some(result)) = crate::util::decimal::exact_binary(
                total.as_ref(),
                crate::util::decimal::DecimalBinaryOp::Div,
                divisor.as_ref(),
                context,
            ) {
                return Value::from_exact_decimal(result);
            }
            return Value::Null;
        }
    }
    Value::Double(numeric_state_to_f64(state) / n as f64)
}

/// Format an i128 magnitude / scale pair as a plain decimal string that
/// `parse_exact_decimal_string` can re-parse losslessly. Thin alias over
/// the shared decimal kernel so all paths print the same canonical form.
pub(crate) fn format_i128_decimal(magnitude: i128, scale: u8) -> String {
    crate::util::decimal::format_decimal_parts(magnitude, scale)
}

pub(crate) fn merge_numeric_state(left: &mut NumericAggState, right: NumericAggState) {
    let merged = match (std::mem::replace(left, NumericAggState::Empty), right) {
        (NumericAggState::Empty, state) => state,
        (NumericAggState::Exact(left_acc), NumericAggState::Empty) => {
            NumericAggState::Exact(left_acc)
        }
        (NumericAggState::Exact(mut left_acc), NumericAggState::Exact(right_acc)) => {
            if let Some(right) = right_acc.exact_value() {
                let right = Value::from_exact_decimal(right);
                let _ = left_acc.add_value(&right);
            }
            NumericAggState::Exact(left_acc)
        }
        (NumericAggState::Exact(left_acc), NumericAggState::Float(right_total)) => {
            NumericAggState::Float(left_acc.to_f64() + right_total)
        }
        (NumericAggState::Float(left_total), NumericAggState::Empty) => {
            NumericAggState::Float(left_total)
        }
        (NumericAggState::Float(left_total), NumericAggState::Exact(right_acc)) => {
            NumericAggState::Float(left_total + right_acc.to_f64())
        }
        (NumericAggState::Float(left_total), NumericAggState::Float(right_total)) => {
            NumericAggState::Float(left_total + right_total)
        }
    };
    *left = merged;
}

pub(crate) fn exact_decimal_parts(value: &Value) -> Option<(i128, u8)> {
    match value {
        Value::DecimalCompact(mag, scale) => Some((i128::from(*mag), *scale)),
        Value::Int(v) => Some((i128::from(*v), 0)),
        Value::UInt(v) => i128::try_from(*v).ok().map(|mag| (mag, 0)),
        Value::Decimal(s) | Value::String(s) => parse_exact_decimal_string(s),
        Value::SmallString(b) => {
            parse_exact_decimal_string(unsafe { std::str::from_utf8_unchecked(b) })
        }
        Value::Bytes(bytes) => std::str::from_utf8(bytes)
            .ok()
            .and_then(parse_exact_decimal_string),
        _ => None,
    }
}

fn parse_exact_decimal_string(s: &str) -> Option<(i128, u8)> {
    crate::util::decimal::parse_exact_decimal_str(s)
}

fn scale_up_decimal(magnitude: i128, from_scale: u8, to_scale: u8) -> Option<i128> {
    match from_scale.cmp(&to_scale) {
        std::cmp::Ordering::Equal => Some(magnitude),
        std::cmp::Ordering::Less => {
            let shift = 10i128.checked_pow((to_scale - from_scale) as u32)?;
            magnitude.checked_mul(shift)
        }
        std::cmp::Ordering::Greater => None,
    }
}

#[derive(Default, Clone)]
struct CountAcc {
    n: i64,
}
impl Accumulator for CountAcc {
    fn update(&mut self, value: Value) -> Result<(), QueryError> {
        if !matches!(value, Value::Null) {
            self.n = self.n.saturating_add(1);
        }
        Ok(())
    }
    fn update_ref(&mut self, value: &Value) -> Result<(), QueryError> {
        if !matches!(value, Value::Null) {
            self.n = self.n.saturating_add(1);
        }
        Ok(())
    }
    fn update_decimal_parts(&mut self, _magnitude: i128, _scale: u8) -> Result<(), QueryError> {
        // The decimal-parts hot path is only invoked once the caller has
        // already filtered NULL inputs (NumericExpr::eval_decimal_parts
        // returns `Some(None)` for NULL and skips dispatch).
        self.n = self.n.saturating_add(1);
        Ok(())
    }
    fn update_i64(&mut self, _value: i64) -> Result<(), QueryError> {
        self.n = self.n.saturating_add(1);
        Ok(())
    }
    fn update_u64(&mut self, _value: u64) -> Result<(), QueryError> {
        self.n = self.n.saturating_add(1);
        Ok(())
    }
    fn update_f64(&mut self, _value: f64) -> Result<(), QueryError> {
        self.n = self.n.saturating_add(1);
        Ok(())
    }
    fn update_batch(&mut self, values: &[Value]) -> Result<(), QueryError> {
        // P4: count non-NULL with a tight integer loop. Compiler
        // auto-vectorises the `matches!` predicate into a SIMD masked
        // popcount on both AVX2 and NEON.
        let nulls = values.iter().filter(|v| matches!(v, Value::Null)).count();
        self.n = self.n.saturating_add((values.len() - nulls) as i64);
        Ok(())
    }
    #[cfg(test)]
    fn update_repeated_ref(&mut self, value: &Value, n: usize) -> Result<(), QueryError> {
        if !matches!(value, Value::Null) {
            self.n = self.n.saturating_add(i64::try_from(n).unwrap_or(i64::MAX));
        }
        Ok(())
    }
    fn update_count_star(&mut self) {
        self.n = self.n.saturating_add(1);
    }
    fn update_count_star_batch(&mut self, n: usize) {
        // P4: COUNT(*) over a known row count is one add. The
        // implicit-single-group path of `aggregate_plan_rows` short-
        // circuits the per-row loop entirely.
        self.n = self.n.saturating_add(i64::try_from(n).unwrap_or(i64::MAX));
    }
    fn finalize(self: Box<Self>) -> Result<Value, QueryError> {
        Ok(Value::Int(self.n))
    }
    fn into_any(self: Box<Self>) -> Box<dyn std::any::Any + Send> {
        self
    }
    fn merge_from(&mut self, other: Box<dyn std::any::Any + Send>) {
        let other = *other.downcast::<Self>().expect("CountAcc type mismatch");
        self.n = self.n.saturating_add(other.n);
    }
    fn clone_box(&self) -> Box<dyn Accumulator> {
        Box::new(self.clone())
    }
}

struct CountDistinctAcc {
    seen: hashbrown::HashSet<ValueKey, ahash::RandomState>,
    payload_bytes: usize,
}
impl Clone for CountDistinctAcc {
    fn clone(&self) -> Self {
        use crate::engine::executor::memory_footprint::AllocatedBytes;
        let seen = self.seen.clone();
        let payload_bytes = seen.iter().map(AllocatedBytes::allocated_bytes).sum();
        Self {
            seen,
            payload_bytes,
        }
    }
}
impl Default for CountDistinctAcc {
    fn default() -> Self {
        Self {
            seen: hashbrown::HashSet::with_hasher(ahash::RandomState::new()),
            payload_bytes: 0,
        }
    }
}
impl CountDistinctAcc {
    fn insert(&mut self, key: ValueKey) -> Result<(), QueryError> {
        use crate::engine::executor::memory_footprint::AllocatedBytes;
        let bytes = key.allocated_bytes();
        if self.seen.len() == self.seen.capacity() && self.seen.contains(&key) {
            return Ok(());
        }
        self.seen.try_reserve(1).map_err(|_| {
            crate::engine::executor::mem_budget::allocation_error("COUNT DISTINCT state")
        })?;
        if self.seen.insert(key) {
            self.payload_bytes = self.payload_bytes.saturating_add(bytes);
        }
        Ok(())
    }
}
impl Accumulator for CountDistinctAcc {
    fn count_distinct_keys(&self) -> Option<&hashbrown::HashSet<ValueKey, ahash::RandomState>> {
        Some(&self.seen)
    }
    fn update(&mut self, value: Value) -> Result<(), QueryError> {
        self.update_ref(&value)
    }
    fn update_ref(&mut self, value: &Value) -> Result<(), QueryError> {
        if !matches!(value, Value::Null) {
            self.insert(ValueKey::from(value))?;
        }
        Ok(())
    }
    fn update_i64(&mut self, value: i64) -> Result<(), QueryError> {
        self.insert(ValueKey::Int(i128::from(value)))?;
        Ok(())
    }
    fn update_u64(&mut self, value: u64) -> Result<(), QueryError> {
        self.insert(ValueKey::Int(i128::from(value)))?;
        Ok(())
    }
    fn finalize(self: Box<Self>) -> Result<Value, QueryError> {
        Ok(Value::Int(self.seen.len() as i64))
    }
    fn heap_bytes(&self) -> usize {
        crate::engine::executor::memory_footprint::hash_table_capacity_bytes::<ValueKey>(
            self.seen.capacity(),
        )
        .saturating_add(self.payload_bytes)
    }
    fn into_any(self: Box<Self>) -> Box<dyn std::any::Any + Send> {
        self
    }
    fn merge_from(&mut self, other: Box<dyn std::any::Any + Send>) {
        let other = *other
            .downcast::<Self>()
            .expect("CountDistinctAcc type mismatch");
        use crate::engine::executor::memory_footprint::AllocatedBytes;
        for key in other.seen {
            let bytes = key.allocated_bytes();
            if self.seen.insert(key) {
                self.payload_bytes = self.payload_bytes.saturating_add(bytes);
            }
        }
    }
    fn clone_box(&self) -> Box<dyn Accumulator> {
        Box::new(self.clone())
    }
}

/// Terminal output only: every source key has already participated in the
/// exact cross-worker union. No further DISTINCT transitions may follow.
pub(super) fn finalized_distinct_count(count: usize) -> Box<dyn Accumulator> {
    Box::new(CountAcc { n: count as i64 })
}

#[derive(Default, Clone)]
struct SumAcc {
    state: NumericAggState,
}

fn integer_batch_sum(values: &[Value]) -> Option<(i128, u64)> {
    let mut sum = 0i128;
    let mut count = 0u64;
    // A Rust slice of Value cannot hold enough 64-bit integers to overflow
    // i128: even isize::MAX / size_of::<Value>() elements stay below 2^127.
    for value in values {
        match value {
            Value::Int(value) => sum += i128::from(*value),
            Value::UInt(value) => sum += i128::from(*value),
            Value::Null => continue,
            _ => return None,
        }
        count += 1;
    }
    Some((sum, count))
}

/// Shared exact numeric kernel. COUNT is compile-time so SUM retains no
/// count bookkeeping; floating inputs retain their original addition order.
#[inline]
fn numeric_update_batch<const COUNT: bool>(
    state: &mut NumericAggState,
    count_out: &mut u64,
    values: &[Value],
) -> Result<(), QueryError> {
    if !matches!(*state, NumericAggState::Float(_)) {
        if let Some((sum, count)) = integer_batch_sum(values) {
            if count != 0 {
                accumulate_numeric_state_decimal_parts(state, sum, 0);
            }
            if COUNT {
                *count_out += count;
            }
            return Ok(());
        }
    }
    // Same DecimalCompact same-scale fast path as SumAcc, plus the
    // count side. SQL AVG ignores NULL inputs, so NULLs contribute
    // neither to the sum nor to the denominator.
    let mut i = 0;
    while i < values.len() {
        if matches!(values[i], Value::Null) {
            i += 1;
            continue;
        }
        if let Value::DecimalCompact(_, scale) = values[i] {
            let mut sum: i128 = 0;
            let mut taken: u64 = 0;
            let mut j = i;
            while j < values.len() {
                if let Value::DecimalCompact(mag, s) = values[j] {
                    if s != scale {
                        break;
                    }
                    sum += mag as i128;
                    taken += 1;
                    j += 1;
                } else {
                    break;
                }
            }
            if j > i {
                accumulate_numeric_state_decimal_parts(state, sum, scale);
                if COUNT {
                    *count_out += taken;
                }
                i = j;
                continue;
            }
        }
        accumulate_numeric_state(state, &values[i])?;
        if COUNT {
            *count_out += 1;
        }
        i += 1;
    }
    Ok(())
}

/// Shared exact numeric kernel. COUNT is compile-time so SUM retains no
/// count bookkeeping; floating inputs retain their original addition order.
#[inline]
#[cfg(test)]
fn numeric_update_repeated_ref<const COUNT: bool>(
    state: &mut NumericAggState,
    count_out: &mut u64,
    value: &Value,
    n: usize,
) -> Result<(), QueryError> {
    if n == 0 || matches!(value, Value::Null) {
        return Ok(());
    }
    let multiplier = n as i128;
    let exact = match value {
        Value::Int(value) => i128::from(*value).checked_mul(multiplier),
        Value::UInt(value) => i128::from(*value).checked_mul(multiplier),
        Value::DecimalCompact(magnitude, _) => i128::from(*magnitude).checked_mul(multiplier),
        _ => None,
    };
    if let Some(magnitude) = exact {
        let scale = match value {
            Value::DecimalCompact(_, scale) => *scale,
            _ => 0,
        };
        accumulate_numeric_state_decimal_parts(state, magnitude, scale);
        if COUNT {
            *count_out += n as u64;
        }
        return Ok(());
    }
    // Preserve the scalar evaluator's addition order for floating and
    // dynamically represented numeric values.
    for _ in 0..n {
        accumulate_numeric_state(state, value)?;
        if COUNT {
            *count_out += 1;
        }
    }
    Ok(())
}

impl Accumulator for SumAcc {
    fn update(&mut self, value: Value) -> Result<(), QueryError> {
        self.update_ref(&value)
    }
    fn update_ref(&mut self, value: &Value) -> Result<(), QueryError> {
        accumulate_numeric_state(&mut self.state, value)
    }
    #[cfg(test)]
    fn update_repeated_ref(&mut self, value: &Value, n: usize) -> Result<(), QueryError> {
        numeric_update_repeated_ref::<false>(&mut self.state, &mut 0, value, n)
    }
    fn update_decimal_parts(&mut self, magnitude: i128, scale: u8) -> Result<(), QueryError> {
        // Feed the i128 magnitude straight into the exact accumulator —
        // skips the `match Value` dispatch in `accumulate_numeric_state`,
        // the `as_decimal_compact_parts` parse, and the `Value` enum
        // round-trip. Hot on TPC-H Q1 where SUM inputs are
        // `extprice * (1 - disc)` etc., all of which collapse to i128
        // magnitudes in the caller's `eval_decimal_parts`.
        accumulate_numeric_state_decimal_parts(&mut self.state, magnitude, scale);
        Ok(())
    }
    fn update_i64(&mut self, value: i64) -> Result<(), QueryError> {
        accumulate_numeric_state_decimal_parts(&mut self.state, value.into(), 0);
        Ok(())
    }
    fn update_u64(&mut self, value: u64) -> Result<(), QueryError> {
        accumulate_numeric_state_decimal_parts(&mut self.state, value.into(), 0);
        Ok(())
    }
    fn update_f64(&mut self, value: f64) -> Result<(), QueryError> {
        accumulate_numeric_state_float(&mut self.state, value);
        Ok(())
    }
    fn update_batch(&mut self, values: &[Value]) -> Result<(), QueryError> {
        numeric_update_batch::<false>(&mut self.state, &mut 0, values)
    }
    fn finalize(self: Box<Self>) -> Result<Value, QueryError> {
        // Preserve full DECIMAL precision when the inputs are all exact —
        // collapsing the i128 magnitude into f64 loses the LSB once the
        // running magnitude crosses ~16 significant digits (TPC-H Q1
        // sum_charge at sf>=5 sits exactly on that edge). `Float` inputs
        // stay Double; mixed runs already widened to Float in
        // `accumulate_numeric_state`.
        Ok(numeric_state_to_value(&self.state))
    }
    fn heap_bytes(&self) -> usize {
        match &self.state {
            NumericAggState::Exact(value) => value.heap_bytes(),
            NumericAggState::Empty | NumericAggState::Float(_) => 0,
        }
    }
    fn into_any(self: Box<Self>) -> Box<dyn std::any::Any + Send> {
        self
    }
    fn merge_from(&mut self, other: Box<dyn std::any::Any + Send>) {
        let other = *other.downcast::<Self>().expect("SumAcc type mismatch");
        merge_numeric_state(&mut self.state, other.state);
    }
    fn clone_box(&self) -> Box<dyn Accumulator> {
        Box::new(self.clone())
    }
}

#[derive(Default, Clone)]
struct AvgAcc {
    state: NumericAggState,
    n: u64,
}
impl Accumulator for AvgAcc {
    fn update(&mut self, value: Value) -> Result<(), QueryError> {
        self.update_ref(&value)
    }
    fn update_ref(&mut self, value: &Value) -> Result<(), QueryError> {
        if matches!(value, Value::Null) {
            return Ok(());
        }
        accumulate_numeric_state(&mut self.state, value)?;
        self.n += 1;
        Ok(())
    }
    #[cfg(test)]
    fn update_repeated_ref(&mut self, value: &Value, n: usize) -> Result<(), QueryError> {
        numeric_update_repeated_ref::<true>(&mut self.state, &mut self.n, value, n)
    }
    fn update_decimal_parts(&mut self, magnitude: i128, scale: u8) -> Result<(), QueryError> {
        accumulate_numeric_state_decimal_parts(&mut self.state, magnitude, scale);
        self.n += 1;
        Ok(())
    }
    fn update_i64(&mut self, value: i64) -> Result<(), QueryError> {
        accumulate_numeric_state_decimal_parts(&mut self.state, value.into(), 0);
        self.n += 1;
        Ok(())
    }
    fn update_u64(&mut self, value: u64) -> Result<(), QueryError> {
        accumulate_numeric_state_decimal_parts(&mut self.state, value.into(), 0);
        self.n += 1;
        Ok(())
    }
    fn update_f64(&mut self, value: f64) -> Result<(), QueryError> {
        accumulate_numeric_state_float(&mut self.state, value);
        self.n += 1;
        Ok(())
    }
    fn update_batch(&mut self, values: &[Value]) -> Result<(), QueryError> {
        numeric_update_batch::<true>(&mut self.state, &mut self.n, values)
    }
    fn finalize(self: Box<Self>) -> Result<Value, QueryError> {
        Ok(finalize_avg_value(&self.state, self.n))
    }
    fn heap_bytes(&self) -> usize {
        match &self.state {
            NumericAggState::Exact(value) => value.heap_bytes(),
            NumericAggState::Empty | NumericAggState::Float(_) => 0,
        }
    }
    fn into_any(self: Box<Self>) -> Box<dyn std::any::Any + Send> {
        self
    }
    fn merge_from(&mut self, other: Box<dyn std::any::Any + Send>) {
        let other = *other.downcast::<Self>().expect("AvgAcc type mismatch");
        merge_numeric_state(&mut self.state, other.state);
        self.n += other.n;
    }
    fn clone_box(&self) -> Box<dyn Accumulator> {
        Box::new(self.clone())
    }
}

#[derive(Clone)]
struct MinMaxAcc {
    collation: Option<u16>,
    is_min: bool,
    best: Option<Value>,
}
impl MinMaxAcc {
    fn new(is_min: bool) -> Self {
        Self {
            is_min,
            best: None,
            collation: None,
        }
    }
}
impl Accumulator for MinMaxAcc {
    fn update_batch(&mut self, values: &[Value]) -> Result<(), QueryError> {
        // Homogeneous exact lanes have the same order as their primitive
        // payload. Validate the whole batch before updating state; mixed or
        // tagged partial dates retain the per-value comparison oracle.
        fn candidate(values: &[Value], is_min: bool) -> Option<Option<Value>> {
            let Some(first) = values.iter().find(|value| !matches!(value, Value::Null)) else {
                return Some(None);
            };
            macro_rules! fold {
                ($kind:ident, $valid:expr) => {{
                    let mut best = None;
                    for value in values {
                        match value {
                            Value::Null => {}
                            Value::$kind(value) if ($valid)(*value) => {
                                best = Some(match best {
                                    None => *value,
                                    Some(old) => {
                                        if is_min {
                                            std::cmp::min(old, *value)
                                        } else {
                                            std::cmp::max(old, *value)
                                        }
                                    }
                                });
                            }
                            _ => return None,
                        }
                    }
                    Some(best.map(Value::$kind))
                }};
            }
            match first {
                Value::Int(_) => fold!(Int, |_| true),
                Value::UInt(_) => fold!(UInt, |_| true),
                Value::Date(_) => fold!(Date, |value| crate::types::decode_tagged_date(value)
                    .is_none()),
                Value::DateTime(_) => {
                    fold!(DateTime, |value| crate::types::decode_tagged_datetime(
                        value
                    )
                    .is_none())
                }
                Value::Timestamp(_) => fold!(Timestamp, |_| true),
                Value::Time(_) => fold!(Time, |_| true),
                Value::Year(_) => fold!(Year, |_| true),
                _ => None,
            }
        }
        if let Some(value) = candidate(values, self.is_min) {
            // A prior heterogeneous state may perform conversion-sensitive
            // comparisons. Preserve its per-row order and warning behavior.
            let compatible = self.best.as_ref().is_none_or(|best| {
                value.as_ref().is_none_or(|value| {
                    std::mem::discriminant(best) == std::mem::discriminant(value)
                }) && match best {
                    Value::Date(value) => crate::types::decode_tagged_date(*value).is_none(),
                    Value::DateTime(value) => {
                        crate::types::decode_tagged_datetime(*value).is_none()
                    }
                    _ => true,
                }
            });
            if compatible {
                return value
                    .as_ref()
                    .map_or(Ok(()), |value| self.update_ref(value));
            }
        }
        for value in values {
            self.update_ref(value)?;
        }
        Ok(())
    }

    fn update(&mut self, value: Value) -> Result<(), QueryError> {
        if matches!(value, Value::Null) {
            return Ok(());
        }
        let replace = match &self.best {
            None => true,
            Some(cur) => {
                let ord =
                    crate::engine::expr::with_optional_compare_collation(self.collation, || {
                        compare_values(&value, cur)
                    })?;
                (self.is_min && ord == Ordering::Less) || (!self.is_min && ord == Ordering::Greater)
            }
        };
        if replace {
            self.best = Some(value);
        }
        Ok(())
    }
    #[cfg(test)]
    fn update_repeated_ref(&mut self, value: &Value, n: usize) -> Result<(), QueryError> {
        if n != 0 {
            self.update_ref(value)?;
        }
        Ok(())
    }
    fn finalize(self: Box<Self>) -> Result<Value, QueryError> {
        Ok(self.best.unwrap_or(Value::Null))
    }
    fn heap_bytes(&self) -> usize {
        use crate::engine::executor::memory_footprint::AllocatedBytes;
        self.best
            .as_ref()
            .map_or(0, AllocatedBytes::allocated_bytes)
    }
    fn into_any(self: Box<Self>) -> Box<dyn std::any::Any + Send> {
        self
    }
    fn merge_from(&mut self, other: Box<dyn std::any::Any + Send>) {
        let other = *other.downcast::<Self>().expect("MinMaxAcc type mismatch");
        if let Some(v) = other.best {
            let _ = self.update(v);
        }
    }
    fn clone_box(&self) -> Box<dyn Accumulator> {
        Box::new(self.clone())
    }
}

/// O2 (q51): **inline** aggregate state for non-DISTINCT SUM/COUNT/MIN/MAX/AVG
/// slots, stored by value in the fused group map instead of one
/// `Box<dyn Accumulator>` per group. On near-distinct high-cardinality GROUP BY
/// (TPC-DS q51 store_v1: ~495k groups, one SUM each) the per-group box heap
/// allocation dominates the aggregate malloc/free profile. Finalization still
/// goes through the single source of truth (`numeric_state_to_value` /
/// `finalize_avg_value`), byte-identical to the boxed `SumAcc`/`AvgAcc`/`MinMaxAcc`; DISTINCT / ORDER BY / other functions make `for_slot` return `None` and the caller falls back to `slot.make_accumulator()`.
#[derive(Clone)]
pub(crate) enum InlineAggState {
    Count(i64),
    Sum(NumericAggState),
    Avg { state: NumericAggState, n: u64 },
    MinMax { is_min: bool, best: Option<Value> },
}

impl InlineAggState {
    pub(crate) fn heap_bytes(&self) -> usize {
        use crate::engine::executor::memory_footprint::AllocatedBytes;
        match self {
            InlineAggState::Count(_) => 0,
            InlineAggState::Sum(NumericAggState::Exact(value))
            | InlineAggState::Avg {
                state: NumericAggState::Exact(value),
                ..
            } => value.heap_bytes(),
            InlineAggState::Sum(NumericAggState::Empty | NumericAggState::Float(_))
            | InlineAggState::Avg {
                state: NumericAggState::Empty | NumericAggState::Float(_),
                ..
            } => 0,
            InlineAggState::MinMax { best, .. } => {
                best.as_ref().map_or(0, AllocatedBytes::allocated_bytes)
            }
        }
    }

    #[inline]
    pub(crate) fn update_count_star(&mut self) {
        if let InlineAggState::Count(n) = self {
            *n += 1;
        }
    }

    #[inline]
    pub(crate) fn update_ref(&mut self, value: &Value) -> Result<(), QueryError> {
        match self {
            InlineAggState::Count(n) => {
                if !matches!(value, Value::Null) {
                    *n += 1;
                }
            }
            InlineAggState::Sum(state) => accumulate_numeric_state(state, value)?,
            InlineAggState::Avg { state, n } => {
                if !matches!(value, Value::Null) {
                    accumulate_numeric_state(state, value)?;
                    *n += 1;
                }
            }
            InlineAggState::MinMax { is_min, best } => {
                if !matches!(value, Value::Null) {
                    let replace = match best {
                        None => true,
                        Some(cur) => {
                            let ord = compare_values(value, cur)?;
                            (*is_min && ord == Ordering::Less)
                                || (!*is_min && ord == Ordering::Greater)
                        }
                    };
                    if replace {
                        *best = Some(value.clone());
                    }
                }
            }
        }
        Ok(())
    }

    #[inline]
    pub(crate) fn update_decimal_parts(
        &mut self,
        magnitude: i128,
        scale: u8,
    ) -> Result<(), QueryError> {
        match self {
            // The decimal-parts hot path is only reached once the caller
            // has filtered NULL inputs (mirrors `CountAcc`).
            InlineAggState::Count(n) => *n += 1,
            InlineAggState::Sum(state) => {
                accumulate_numeric_state_decimal_parts(state, magnitude, scale)
            }
            InlineAggState::Avg { state, n } => {
                accumulate_numeric_state_decimal_parts(state, magnitude, scale);
                *n += 1;
            }
            InlineAggState::MinMax { .. } => {
                let value = if let Ok(mag) = i64::try_from(magnitude) {
                    Value::DecimalCompact(mag, scale)
                } else {
                    Value::Decimal(format_i128_decimal(magnitude, scale))
                };
                return self.update_ref(&value);
            }
        }
        Ok(())
    }

    pub(crate) fn finalize_ref(&self) -> Value {
        match self {
            InlineAggState::Count(n) => Value::Int(*n),
            InlineAggState::Sum(state) => numeric_state_to_value(state),
            InlineAggState::Avg { state, n } => finalize_avg_value(state, *n),
            InlineAggState::MinMax { best, .. } => best.clone().unwrap_or(Value::Null),
        }
    }
}

/// Build the synthetic group-key marker consumed by downstream projection
/// and ORDER BY evaluation. Metadata is cached per marker index so emitting a
/// large number of groups does not allocate the same name repeatedly.
pub(super) fn group_marker_field(idx: usize, value: Value) -> PlanField {
    PlanField {
        meta: group_marker_meta(idx),
        value,
    }
}

pub(super) fn group_marker_meta(idx: usize) -> super::plan::ColMetaRef {
    use std::cell::RefCell;
    thread_local! {
        static CACHE: RefCell<Vec<super::plan::ColMetaRef>> =
            const { RefCell::new(Vec::new()) };
    }
    if let Some(meta) = CACHE.with(|cache| cache.borrow().get(idx).cloned()) {
        return meta;
    }
    let meta = super::plan::intern_col_meta(super::plan::ColMeta {
        invisible: false,
        table: "$group".to_string(),
        alias: None,
        column: format!("group_{idx}"),
        column_type: ColumnType::VarString,
        source: Default::default(),
    });
    CACHE.with(|cache| {
        let mut cache = cache.borrow_mut();
        if cache.len() == idx {
            cache.push(meta.clone());
        }
    });
    meta
}

/// `__hidden_first_value`: take the first value fed into the group, including
/// NULL (a MySQL hidden column takes the first row's value; when that row is
/// NULL the result is NULL and must not be skipped like MIN/MAX). `set` distinguishes "no row seen yet" from "first row is NULL".
#[derive(Default, Clone)]
struct FirstValueAcc {
    value: Option<Value>,
}
impl Accumulator for FirstValueAcc {
    fn update(&mut self, value: Value) -> Result<(), QueryError> {
        if self.value.is_none() {
            self.value = Some(value);
        }
        Ok(())
    }
    fn update_ref(&mut self, value: &Value) -> Result<(), QueryError> {
        if self.value.is_none() {
            self.value = Some(value.clone());
        }
        Ok(())
    }
    fn update_batch(&mut self, values: &[Value]) -> Result<(), QueryError> {
        if self.value.is_none() {
            if let Some(first) = values.first() {
                self.value = Some(first.clone());
            }
        }
        Ok(())
    }
    fn finalize(self: Box<Self>) -> Result<Value, QueryError> {
        Ok(self.value.unwrap_or(Value::Null))
    }
    fn heap_bytes(&self) -> usize {
        use crate::engine::executor::memory_footprint::AllocatedBytes;
        self.value
            .as_ref()
            .map_or(0, AllocatedBytes::allocated_bytes)
    }
    fn into_any(self: Box<Self>) -> Box<dyn std::any::Any + Send> {
        self
    }
    fn merge_from(&mut self, other: Box<dyn std::any::Any + Send>) {
        let other = *other
            .downcast::<Self>()
            .expect("FirstValueAcc type mismatch");
        // Parallel merge takes the first non-empty partial; all hidden slots
        // merge partials in the same order, so a group's hidden columns still come from one row.
        if self.value.is_none() {
            self.value = other.value;
        }
    }
    fn clone_box(&self) -> Box<dyn Accumulator> {
        Box::new(self.clone())
    }
}

#[derive(Clone)]
struct GroupConcatPart {
    ordinal: u64,
    sort_keys: Vec<Value>,
    distinct_key: Option<Vec<u8>>,
    bytes: Vec<u8>,
    binary: bool,
}

#[derive(Clone)]
struct GroupConcatAcc {
    parts: Vec<GroupConcatPart>,
    logical_len: usize,
    next_ordinal: u64,
    retained_units: usize,
    pending_error: Option<QueryError>,
    distinct: bool,
    /// DISTINCT uses the argument's bound comparison key, independently
    /// of the retained display bytes and ORDER BY keys.
    seen: HashSet<Vec<u8>>,
    separator: Vec<u8>,
    /// Any binary-string contribution makes the aggregate result binary.
    /// This preserves arbitrary BLOB/BIT octets instead of lossy UTF-8.
    binary_result: bool,
    /// One entry per `ORDER BY` term (direction + NULL placement).
    /// Empty when the call had no `ORDER BY` clause; in that case
    /// `finalize` preserves first-seen insertion order to match MySQL.
    order_fields: Vec<SortField>,
    /// `group_concat_max_len` cap (MySQL 8.0 default 1024). The final
    /// joined string is truncated to at most this many bytes; the
    /// boundary is rounded back to a UTF-8 codepoint so we never split a
    /// multibyte sequence.
    max_len_bytes: usize,
    /// `true` when finalize truncated the joined result. Surfaced to
    /// the executor so the warning channel can carry a `1260` "Some
    /// rows were cut by GROUPCONCAT()" diagnostic when wiring is added.
    truncated: bool,
}

impl GroupConcatAcc {
    fn new(
        distinct: bool,
        separator: String,
        order_fields: Vec<SortField>,
        max_len_bytes: usize,
    ) -> Self {
        Self {
            parts: Vec::new(),
            logical_len: 0,
            next_ordinal: 0,
            retained_units: 0,
            pending_error: None,
            distinct,
            seen: HashSet::new(),
            separator: separator.into_bytes(),
            binary_result: false,
            order_fields,
            max_len_bytes,
            truncated: false,
        }
    }

    fn distinct_key(bytes: &[u8], binary: bool) -> Vec<u8> {
        if binary {
            let mut key = Vec::with_capacity(bytes.len() + 1);
            key.push(1);
            key.extend_from_slice(bytes);
            key
        } else {
            let text = std::str::from_utf8(bytes).expect("text GROUP_CONCAT input is UTF-8");
            let mut key = vec![0];
            crate::charset::with_default_string_key(text, |collation_key| {
                key.extend_from_slice(collation_key)
            });
            key
        }
    }

    fn compare_parts(fields: &[SortField], a: &GroupConcatPart, b: &GroupConcatPart) -> Ordering {
        for (i, field) in fields.iter().enumerate() {
            let lhs = a.sort_keys.get(i).unwrap_or(&Value::Null);
            let rhs = b.sort_keys.get(i).unwrap_or(&Value::Null);
            match compare_value_for_sort(lhs, rhs, *field) {
                Ordering::Equal => {}
                order => return order,
            }
        }
        a.ordinal.cmp(&b.ordinal)
    }

    fn append_prefix(out: &mut Vec<u8>, bytes: &[u8], cap: usize) -> Result<(), QueryError> {
        let count = cap.saturating_sub(out.len()).min(bytes.len());
        if count == 0 {
            return Ok(());
        }
        if out.capacity() - out.len() < count {
            // Cover the replacement while the old buffer is still resident.
            let _growth =
                crate::engine::executor::mem_budget::force_charge_scoped(out.len() + count)?;
            crate::engine::executor::mem_budget::try_reserve_vec_exact(
                out,
                count,
                "GROUP_CONCAT prefix",
            )?;
        }
        out.extend_from_slice(&bytes[..count]);
        Ok(())
    }

    fn reserve_part(&mut self) -> Result<(), QueryError> {
        if self.parts.len() == self.parts.capacity() {
            let target = self.parts.capacity().saturating_mul(2).max(1);
            let _growth = crate::engine::executor::mem_budget::force_charge_scoped(
                target.saturating_mul(std::mem::size_of::<GroupConcatPart>()),
            )?;
            let additional = target - self.parts.len();
            crate::engine::executor::mem_budget::try_reserve_vec_exact(
                &mut self.parts,
                additional,
                "GROUP_CONCAT candidates",
            )?;
        }
        Ok(())
    }

    fn push_part(
        &mut self,
        bytes: Vec<u8>,
        binary: bool,
        sort_keys: Vec<Value>,
    ) -> Result<(), QueryError> {
        let distinct_key = self.distinct.then(|| match sort_keys.last() {
            Some(Value::Bytes(key)) => key.clone(),
            _ => Self::distinct_key(&bytes, binary),
        });
        if let Some(key) = &distinct_key {
            if !self.seen.insert(key.clone()) {
                return Ok(());
            }
        }
        self.binary_result |= binary;
        if self.next_ordinal > 0 {
            self.logical_len = self.logical_len.saturating_add(self.separator.len());
        }
        self.logical_len = self.logical_len.saturating_add(bytes.len());
        self.truncated |= self.logical_len > self.max_len_bytes;
        let ordinal = self.next_ordinal;
        self.next_ordinal = self.next_ordinal.checked_add(1).ok_or_else(|| {
            error(
                crate::error::catalog::ER_UNKNOWN_ERROR,
                "GROUP_CONCAT input ordinal overflow",
            )
        })?;
        if !self.distinct && self.order_fields.is_empty() {
            if self.parts.is_empty() {
                self.reserve_part()?;
                self.parts.push(GroupConcatPart {
                    ordinal: 0,
                    bytes: Vec::new(),
                    sort_keys: Vec::new(),
                    distinct_key: None,
                    binary,
                });
            }
            let out = &mut self.parts[0].bytes;
            if ordinal > 0 {
                Self::append_prefix(out, &self.separator, self.max_len_bytes)?;
            }
            Self::append_prefix(out, &bytes, self.max_len_bytes)?;
            return Ok(());
        }
        if !self.distinct
            && (self.max_len_bytes == 0 || (bytes.is_empty() && self.separator.is_empty()))
        {
            return Ok(());
        }
        let mut part = GroupConcatPart {
            ordinal,
            distinct_key,
            sort_keys,
            bytes,
            binary,
        };
        if self.distinct {
            self.reserve_part()?;
            self.parts.push(part);
            return Ok(());
        }
        // Keep the best output prefix in a max heap. A contribution worse
        // than a complete retained prefix can never enter the final prefix,
        // even when later input changes the candidate set. Ties retain the
        // earlier input ordinal, matching stable finalize-time sorting.
        if self.retained_units.saturating_sub(self.separator.len()) >= self.max_len_bytes
            && self
                .parts
                .first()
                .is_some_and(|worst| Self::compare_parts(&self.order_fields, &part, worst).is_ge())
        {
            return Ok(());
        }
        if part.bytes.len() > self.max_len_bytes {
            let _copy =
                crate::engine::executor::mem_budget::force_charge_scoped(self.max_len_bytes)?;
            part.bytes = crate::engine::executor::mem_budget::try_copy_bytes(
                &part.bytes[..self.max_len_bytes],
                "GROUP_CONCAT ordered prefix",
            )?;
        }
        self.reserve_part()?;
        self.retained_units = self
            .retained_units
            .saturating_add(part.bytes.len())
            .saturating_add(self.separator.len());
        self.parts.push(part);
        let mut child = self.parts.len() - 1;
        while child > 0 {
            let parent = (child - 1) / 2;
            if Self::compare_parts(&self.order_fields, &self.parts[parent], &self.parts[child])
                .is_ge()
            {
                break;
            }
            self.parts.swap(parent, child);
            child = parent;
        }
        while self.parts.len() > 1 {
            let worst_units = self.parts[0]
                .bytes
                .len()
                .saturating_add(self.separator.len());
            if self
                .retained_units
                .saturating_sub(worst_units)
                .saturating_sub(self.separator.len())
                < self.max_len_bytes
            {
                break;
            }
            self.retained_units = self.retained_units.saturating_sub(worst_units);
            self.parts.swap_remove(0);
            let mut parent = 0;
            while parent * 2 + 1 < self.parts.len() {
                let left = parent * 2 + 1;
                let right = left + 1;
                let child = if right < self.parts.len()
                    && Self::compare_parts(
                        &self.order_fields,
                        &self.parts[right],
                        &self.parts[left],
                    )
                    .is_gt()
                {
                    right
                } else {
                    left
                };
                if Self::compare_parts(&self.order_fields, &self.parts[parent], &self.parts[child])
                    .is_ge()
                {
                    break;
                }
                self.parts.swap(parent, child);
                parent = child;
            }
        }
        Ok(())
    }
}

impl Accumulator for GroupConcatAcc {
    fn update(&mut self, value: Value) -> Result<(), QueryError> {
        if matches!(value, Value::Null) {
            return Ok(());
        }
        let (bytes, binary) = match value {
            Value::Bytes(bytes) => (bytes, true),
            Value::String(text) => (text.into_bytes(), false),
            other => (value_to_string(&other).into_bytes(), false),
        };
        self.push_part(bytes, binary, Vec::new())
    }
    fn update_ordered(&mut self, value: Value, sort_keys: Vec<Value>) -> Result<(), QueryError> {
        if matches!(value, Value::Null) {
            return Ok(());
        }
        let (bytes, binary) = match value {
            Value::Bytes(bytes) => (bytes, true),
            Value::String(text) => (text.into_bytes(), false),
            other => (value_to_string(&other).into_bytes(), false),
        };
        self.push_part(bytes, binary, sort_keys)
    }
    fn finalize(mut self: Box<Self>) -> Result<Value, QueryError> {
        if let Some(error) = self.pending_error.take() {
            return Err(error);
        }
        if self.next_ordinal == 0 {
            return Ok(Value::Null);
        }
        if !self.order_fields.is_empty() {
            self.parts
                .sort_unstable_by(|a, b| Self::compare_parts(&self.order_fields, a, b));
        }
        let total = self
            .parts
            .iter()
            .fold(0usize, |n, part| n.saturating_add(part.bytes.len()))
            .saturating_add(
                self.separator
                    .len()
                    .saturating_mul(self.parts.len().saturating_sub(1)),
            );
        self.truncated |= total > self.max_len_bytes;
        let _output = crate::engine::executor::mem_budget::force_charge_scoped(
            total.min(self.max_len_bytes),
        )?;
        let mut out = crate::engine::executor::mem_budget::try_vec_with_capacity(
            total.min(self.max_len_bytes),
            "GROUP_CONCAT final output",
        )?;
        for (index, part) in self.parts.iter().enumerate() {
            if index > 0 {
                Self::append_prefix(&mut out, &self.separator, self.max_len_bytes)?;
            }
            Self::append_prefix(&mut out, &part.bytes, self.max_len_bytes)?;
            if out.len() == self.max_len_bytes {
                break;
            }
        }
        if !self.binary_result {
            // A bounded prefix may end inside the last UTF-8 codepoint.
            // A later binary contribution still needs those exact raw bytes,
            // so round back only after the final result type is known.
            if let Err(error) = std::str::from_utf8(&out) {
                if error.error_len().is_some() {
                    return Err(crate::error::catalog::ER_UNKNOWN_ERROR
                        .error("invalid text GROUP_CONCAT prefix"));
                }
                out.truncate(error.valid_up_to());
                self.truncated = true;
            }
        }
        if self.truncated {
            let spec = crate::error::catalog::ER_CUT_VALUE_GROUP_CONCAT;
            let message = "Row was cut by GROUP_CONCAT()";
            if crate::engine::expr::strict_dml_expression() {
                return Err(spec.error(message));
            }
            crate::engine::api::push_statement_warning(
                crate::error::ConditionLevel::Warning,
                spec,
                message,
            );
        }
        if self.binary_result {
            Ok(Value::Bytes(out))
        } else {
            Ok(Value::String(
                String::from_utf8(out).expect("text GROUP_CONCAT output is UTF-8"),
            ))
        }
    }
    fn heap_bytes(&self) -> usize {
        use crate::engine::executor::memory_footprint::AllocatedBytes;
        self.parts
            .capacity()
            .saturating_mul(std::mem::size_of::<GroupConcatPart>())
            .saturating_add(self.separator.capacity())
            .saturating_add(
                self.order_fields
                    .capacity()
                    .saturating_mul(std::mem::size_of::<SortField>()),
            )
            .saturating_add(
                self.parts
                    .iter()
                    .map(|part| {
                        part.bytes
                            .capacity()
                            .saturating_add(
                                part.sort_keys
                                    .capacity()
                                    .saturating_mul(std::mem::size_of::<Value>()),
                            )
                            .saturating_add(
                                part.sort_keys
                                    .iter()
                                    .map(AllocatedBytes::allocated_bytes)
                                    .sum::<usize>(),
                            )
                            .saturating_add(part.distinct_key.as_ref().map_or(0, Vec::capacity))
                    })
                    .sum::<usize>(),
            )
            .saturating_add(
                self.seen
                    .capacity()
                    .saturating_mul(std::mem::size_of::<Vec<u8>>() + 8),
            )
            .saturating_add(self.seen.iter().map(Vec::capacity).sum::<usize>())
    }
    fn into_any(self: Box<Self>) -> Box<dyn std::any::Any + Send> {
        self
    }
    fn merge_from(&mut self, other: Box<dyn std::any::Any + Send>) {
        let mut other = *other
            .downcast::<Self>()
            .expect("GroupConcatAcc type mismatch");
        if self.pending_error.is_some() {
            return;
        }
        if let Some(error) = other.pending_error.take() {
            self.pending_error = Some(error);
            return;
        }
        let merged_len = self
            .logical_len
            .saturating_add(other.logical_len)
            .saturating_add(if self.next_ordinal > 0 && other.next_ordinal > 0 {
                self.separator.len()
            } else {
                0
            });
        let has_input = self.next_ordinal > 0 || other.next_ordinal > 0;
        self.binary_result |= other.binary_result;
        self.truncated |= other.truncated;
        // Heap order is unrelated to arrival order. Restore partial ordinals
        // before assigning this merge's stable tie breakers.
        other.parts.sort_unstable_by_key(|part| part.ordinal);
        for part in other.parts {
            if self.distinct {
                let key = part
                    .distinct_key
                    .clone()
                    .unwrap_or_else(|| Self::distinct_key(&part.bytes, part.binary));
                if !self.seen.insert(key) {
                    continue;
                }
                let mut part = part;
                part.ordinal = self.next_ordinal;
                self.next_ordinal = self.next_ordinal.saturating_add(1);
                if let Err(error) = self.reserve_part() {
                    self.pending_error = Some(error);
                    return;
                }
                self.parts.push(part);
            } else if let Err(error) = self.push_part(part.bytes, part.binary, part.sort_keys) {
                self.pending_error = Some(error);
                return;
            }
        }
        if !self.distinct {
            self.logical_len = merged_len;
            self.truncated |= merged_len > self.max_len_bytes;
            if has_input && self.next_ordinal == 0 {
                self.next_ordinal = 1;
            }
        }
    }
    fn clone_box(&self) -> Box<dyn Accumulator> {
        Box::new(self.clone())
    }
}

#[derive(Clone)]
struct JsonArrayAggEntry {
    sort_keys: Vec<Value>,
    value: Vec<u8>,
}

struct JsonArrayAggAcc {
    entries: Vec<JsonArrayAggEntry>,
    order_fields: Vec<SortField>,
    /// Cumulative payload bytes across all `entries`.
    payload_bytes: usize,
}
impl Clone for JsonArrayAggAcc {
    fn clone(&self) -> Self {
        use crate::engine::executor::memory_footprint::AllocatedBytes;
        let entries = self.entries.clone();
        let payload_bytes = entries
            .iter()
            .map(|entry| {
                entry
                    .value
                    .capacity()
                    .saturating_add(entry.sort_keys.allocated_bytes())
            })
            .sum();
        Self {
            entries,
            payload_bytes,
            order_fields: self.order_fields.clone(),
        }
    }
}
impl JsonArrayAggAcc {
    fn new(order_fields: Vec<SortField>) -> Self {
        Self {
            entries: Vec::new(),
            order_fields,
            payload_bytes: 0,
        }
    }
}
impl Accumulator for JsonArrayAggAcc {
    fn update(&mut self, value: Value) -> Result<(), QueryError> {
        self.push_entry(value, Vec::new());
        Ok(())
    }
    fn update_ordered(&mut self, value: Value, sort_keys: Vec<Value>) -> Result<(), QueryError> {
        self.push_entry(value, sort_keys);
        Ok(())
    }
    fn finalize(mut self: Box<Self>) -> Result<Value, QueryError> {
        if self.entries.is_empty() {
            return Ok(Value::Null);
        }
        if !self.order_fields.is_empty() {
            let fields = self.order_fields.clone();
            self.entries.sort_by(|a, b| {
                for (i, field) in fields.iter().enumerate() {
                    let lhs = a.sort_keys.get(i).unwrap_or(&Value::Null);
                    let rhs = b.sort_keys.get(i).unwrap_or(&Value::Null);
                    match compare_value_for_sort(lhs, rhs, *field) {
                        Ordering::Equal => continue,
                        ord => return ord,
                    }
                }
                Ordering::Equal
            });
        }
        let values = self
            .entries
            .into_iter()
            .map(|e| e.value)
            .collect::<Vec<_>>();
        checked_json_aggregate(crate::engine::json::build_array(&values))
    }
    fn heap_bytes(&self) -> usize {
        self.entries
            .capacity()
            .saturating_mul(std::mem::size_of::<JsonArrayAggEntry>())
            .saturating_add(
                self.order_fields
                    .capacity()
                    .saturating_mul(std::mem::size_of::<SortField>()),
            )
            .saturating_add(self.payload_bytes)
    }
    fn into_any(self: Box<Self>) -> Box<dyn std::any::Any + Send> {
        self
    }
    fn merge_from(&mut self, other: Box<dyn std::any::Any + Send>) {
        let other = *other
            .downcast::<Self>()
            .expect("JsonArrayAggAcc type mismatch");
        self.payload_bytes = self.payload_bytes.saturating_add(other.payload_bytes);
        self.entries.extend(other.entries);
    }
    fn clone_box(&self) -> Box<dyn Accumulator> {
        Box::new(self.clone())
    }
}

impl JsonArrayAggAcc {
    fn push_entry(&mut self, value: Value, sort_keys: Vec<Value>) {
        // Keep each element in JSONB form. A serde round-trip loses MySQL's
        // DATE/TIME/DATETIME/DECIMAL/OPAQUE scalar tags.
        let json = value_to_jsonb_typed(&value, false);
        use crate::engine::executor::memory_footprint::AllocatedBytes;
        let byte_cost = json
            .capacity()
            .saturating_add(
                sort_keys
                    .capacity()
                    .saturating_mul(std::mem::size_of::<Value>()),
            )
            .saturating_add(
                sort_keys
                    .iter()
                    .map(AllocatedBytes::allocated_bytes)
                    .sum::<usize>(),
            );
        self.payload_bytes = self.payload_bytes.saturating_add(byte_cost);
        self.entries.push(JsonArrayAggEntry {
            sort_keys,
            value: json,
        });
    }
}

#[derive(Default)]
struct JsonObjectAggAcc {
    // JSONB object keys are emitted in canonical lexical order.
    map: BTreeMap<String, Vec<u8>>,
    /// Cumulative key+value byte estimate across `map`.
    payload_bytes: usize,
}
impl Clone for JsonObjectAggAcc {
    fn clone(&self) -> Self {
        let map = self.map.clone();
        let payload_bytes = map
            .iter()
            .map(|(key, value)| key.capacity().saturating_add(value.capacity()))
            .sum();
        Self { map, payload_bytes }
    }
}
impl Accumulator for JsonObjectAggAcc {
    fn update(&mut self, value: Value) -> Result<(), QueryError> {
        if matches!(value, Value::Null) {
            return Ok(());
        }
        // The planner feeds each row as a typed JSONB `JSON_ARRAY(key, value)`.
        let encoded = value_to_jsonb_typed(&value, false);
        let root = crate::engine::json::JsonbRef::new(&encoded)?;
        let mut items = root.array_iter();
        let Some(key_node) = items.next() else {
            return Ok(());
        };
        let Some(value_node) = items.next() else {
            return Ok(());
        };
        // NULL key: MySQL raises 3158 (ER_JSON_DOCUMENT_NULL_KEY). The
        // window-function path already reports this correctly; keep both
        // paths in sync here.
        if key_node.is_null() {
            return Err(crate::error::catalog::ER_JSON_DOCUMENT_NULL_KEY
                .error("JSON documents may not contain NULL member names."));
        }
        let key = key_node
            .as_str()
            .map(str::to_string)
            .unwrap_or_else(|| crate::engine::json::unquote_text(&key_node.to_owned_bytes()));
        let json_value = value_node.to_owned_bytes();
        // Inserting an equal key replaces only the value. Preserve the
        // capacity of the key already owned by the map, including after clone.
        let (key_bytes, prior_cost) =
            self.map
                .get_key_value(&key)
                .map_or((key.capacity(), 0), |(key, prior)| {
                    (
                        key.capacity(),
                        key.capacity().saturating_add(prior.capacity()),
                    )
                });
        let entry_cost = key_bytes.saturating_add(json_value.capacity());
        self.payload_bytes = self
            .payload_bytes
            .saturating_sub(prior_cost)
            .saturating_add(entry_cost);
        self.map.insert(key, json_value);
        Ok(())
    }
    fn finalize(self: Box<Self>) -> Result<Value, QueryError> {
        if self.map.is_empty() {
            return Ok(Value::Null);
        }
        let pairs = self.map.into_iter().collect::<Vec<_>>();
        checked_json_aggregate(crate::engine::json::build_object(&pairs))
    }
    fn heap_bytes(&self) -> usize {
        self.map.len().saturating_mul(64) + self.payload_bytes
    }
    fn into_any(self: Box<Self>) -> Box<dyn std::any::Any + Send> {
        self
    }
    fn merge_from(&mut self, other: Box<dyn std::any::Any + Send>) {
        let other = *other
            .downcast::<Self>()
            .expect("JsonObjectAggAcc type mismatch");
        // Merge with best-effort dedup accounting: `map.extend` overwrites
        // duplicates so subtract the old cost and add the new one.
        for (k, v) in other.map {
            let (key_bytes, prior_cost) =
                self.map
                    .get_key_value(&k)
                    .map_or((k.capacity(), 0), |(key, prior)| {
                        (
                            key.capacity(),
                            key.capacity().saturating_add(prior.capacity()),
                        )
                    });
            self.payload_bytes = self
                .payload_bytes
                .saturating_sub(prior_cost)
                .saturating_add(key_bytes.saturating_add(v.capacity()));
            self.map.insert(k, v);
        }
    }
    fn clone_box(&self) -> Box<dyn Accumulator> {
        Box::new(self.clone())
    }
}

fn checked_json_aggregate(bytes: Vec<u8>) -> Result<Value, QueryError> {
    if crate::engine::json::JsonbRef::new(&bytes)?.depth() > crate::engine::json::MAX_JSON_DEPTH {
        return Err(crate::engine::json::json_too_deep());
    }
    Ok(Value::Json(bytes))
}

#[derive(Clone)]
struct BitAcc {
    state: crate::engine::expr::BitAggregateState,
}
impl BitAcc {
    fn new(mode: &'static str, binary_width: Option<usize>) -> Self {
        Self {
            state: crate::engine::expr::BitAggregateState::new_with_binary_width(
                mode,
                binary_width,
            ),
        }
    }
}
impl Accumulator for BitAcc {
    fn update(&mut self, value: Value) -> Result<(), QueryError> {
        self.state.update(&value)
    }
    fn update_i64(&mut self, value: i64) -> Result<(), QueryError> {
        self.state.update_u64(value as u64)
    }
    fn update_u64(&mut self, value: u64) -> Result<(), QueryError> {
        self.state.update_u64(value)
    }
    fn finalize(self: Box<Self>) -> Result<Value, QueryError> {
        self.state.finish()
    }
    fn heap_bytes(&self) -> usize {
        self.state.heap_bytes()
    }
    fn into_any(self: Box<Self>) -> Box<dyn std::any::Any + Send> {
        self
    }
    fn merge_from(&mut self, other: Box<dyn std::any::Any + Send>) {
        let other = *other.downcast::<Self>().expect("BitAcc type mismatch");
        self.state.merge(other.state);
    }
    fn clone_box(&self) -> Box<dyn Accumulator> {
        Box::new(self.clone())
    }
}

#[derive(Clone)]
struct VarianceAcc {
    sample: bool,
    sqrt: bool,
    n: u64,
    mean: f64,
    m2: f64,
}
impl VarianceAcc {
    fn new(sample: bool, sqrt: bool) -> Self {
        Self {
            sample,
            sqrt,
            n: 0,
            mean: 0.0,
            m2: 0.0,
        }
    }

    #[inline]
    fn update_number(&mut self, value: f64) {
        self.n += 1;
        let delta = value - self.mean;
        self.mean += delta / self.n as f64;
        let delta_after = value - self.mean;
        self.m2 += delta * delta_after;
    }
}
impl Accumulator for VarianceAcc {
    fn update(&mut self, value: Value) -> Result<(), QueryError> {
        if matches!(value, Value::Null) {
            return Ok(());
        }
        let value = numeric(&value)?;
        self.update_number(value);
        Ok(())
    }
    fn update_ref(&mut self, value: &Value) -> Result<(), QueryError> {
        self.update(value.clone())
    }
    fn update_i64(&mut self, value: i64) -> Result<(), QueryError> {
        self.update_number(value as f64);
        Ok(())
    }
    fn update_u64(&mut self, value: u64) -> Result<(), QueryError> {
        self.update_number(value as f64);
        Ok(())
    }
    fn update_f64(&mut self, value: f64) -> Result<(), QueryError> {
        self.update_number(value);
        Ok(())
    }
    fn finalize(self: Box<Self>) -> Result<Value, QueryError> {
        if self.n == 0 || (self.sample && self.n < 2) {
            return Ok(Value::Null);
        }
        let divisor = if self.sample {
            (self.n - 1) as f64
        } else {
            self.n as f64
        };
        let raw_variance = self.m2 / divisor;
        if !raw_variance.is_finite() {
            return Err(error(
                crate::error::catalog::ER_DATA_OUT_OF_RANGE,
                "DOUBLE value is out of range in variance aggregate",
            ));
        }
        // Floating-point cancellation can produce a tiny negative value for
        // otherwise valid inputs; clamp only after rejecting NaN/Infinity.
        let variance = raw_variance.max(0.0);
        let result = if self.sqrt { variance.sqrt() } else { variance };
        if !result.is_finite() {
            return Err(error(
                crate::error::catalog::ER_DATA_OUT_OF_RANGE,
                "DOUBLE value is out of range in variance aggregate",
            ));
        }
        Ok(Value::Double(result))
    }
    fn into_any(self: Box<Self>) -> Box<dyn std::any::Any + Send> {
        self
    }
    fn merge_from(&mut self, other: Box<dyn std::any::Any + Send>) {
        let other = *other.downcast::<Self>().expect("VarianceAcc type mismatch");
        if other.n == 0 {
            return;
        }
        if self.n == 0 {
            self.n = other.n;
            self.mean = other.mean;
            self.m2 = other.m2;
            return;
        }
        let left_n = self.n as f64;
        let right_n = other.n as f64;
        let total_n = left_n + right_n;
        let delta = other.mean - self.mean;
        self.m2 += other.m2 + delta * delta * left_n * right_n / total_n;
        self.mean += delta * right_n / total_n;
        self.n += other.n;
    }
    fn clone_box(&self) -> Box<dyn Accumulator> {
        Box::new(self.clone())
    }
}

/// Wrap a numeric accumulator to dedupe inputs first (DISTINCT).
///
/// The inner accumulator is **not** fed incrementally. Instead every distinct
/// value is buffered (keyed by its `ValueKey`) and only fed into a fresh inner
/// at `finalize`. This is what makes the reducer correct under `merge_from`:
/// merging is a plain union of the distinct-value maps, after which a single
/// pass feeds the union into the inner. An incremental inner (the old design)
/// double-counted any value present in both partials — wrong for
/// `SUM(DISTINCT)`/`AVG(DISTINCT)` whenever two partials are combined (parallel
/// aggregation, and the two-phase ROLLUP super-aggregate derivation), and
/// silently masked only when no value repeated across the merged groups.
struct DistinctReducer<A: Accumulator + Clone + 'static> {
    inner: A,
    seen: hashbrown::HashMap<ValueKey, Value, ahash::RandomState>,
    payload_bytes: usize,
}
impl<A: Accumulator + Clone> Clone for DistinctReducer<A> {
    fn clone(&self) -> Self {
        use crate::engine::executor::memory_footprint::AllocatedBytes;
        let seen = self.seen.clone();
        let payload_bytes = seen
            .iter()
            .map(|(key, value)| {
                key.allocated_bytes()
                    .saturating_add(value.allocated_bytes())
            })
            .sum();
        Self {
            inner: self.inner.clone(),
            seen,
            payload_bytes,
        }
    }
}
impl<A: Accumulator + Clone + 'static> DistinctReducer<A> {
    fn new(inner: A) -> Self {
        Self {
            inner,
            seen: hashbrown::HashMap::with_hasher(ahash::RandomState::new()),
            payload_bytes: 0,
        }
    }

    fn insert(&mut self, key: ValueKey, value: Value) -> Result<(), QueryError> {
        use crate::engine::executor::memory_footprint::AllocatedBytes;
        let bytes = key
            .allocated_bytes()
            .saturating_add(value.allocated_bytes());
        if self.seen.len() == self.seen.capacity() && self.seen.contains_key(&key) {
            return Ok(());
        }
        self.seen.try_reserve(1).map_err(|_| {
            crate::engine::executor::mem_budget::allocation_error("DISTINCT reducer state")
        })?;
        if let hashbrown::hash_map::Entry::Vacant(entry) = self.seen.entry(key) {
            self.payload_bytes = self.payload_bytes.saturating_add(bytes);
            entry.insert(value);
        }
        Ok(())
    }
}
impl<A: Accumulator + Clone> Accumulator for DistinctReducer<A> {
    fn update(&mut self, value: Value) -> Result<(), QueryError> {
        if matches!(value, Value::Null) {
            return Ok(());
        }
        let key = ValueKey::from(&value);
        self.insert(key, value)?;
        Ok(())
    }
    fn update_ref(&mut self, value: &Value) -> Result<(), QueryError> {
        if matches!(value, Value::Null) {
            return Ok(());
        }
        let key = ValueKey::from(value);
        if !self.seen.contains_key(&key) {
            let copy = crate::engine::executor::mem_budget::try_clone_value(
                value,
                "DISTINCT representative",
            )?;
            self.insert(key, copy)?;
        }
        Ok(())
    }
    fn finalize(self: Box<Self>) -> Result<Value, QueryError> {
        let DistinctReducer {
            mut inner, seen, ..
        } = *self;
        // Order is irrelevant: the inner is SUM/AVG over a deduplicated set,
        // both commutative (exact decimal is associative; float DISTINCT order
        // is unspecified in SQL anyway).
        for value in seen.into_values() {
            crate::engine::expr::with_distinct_aggregate_numeric_conversion(|| {
                inner.update(value)
            })?;
        }
        Box::new(inner).finalize()
    }
    fn heap_bytes(&self) -> usize {
        crate::engine::executor::memory_footprint::hash_table_capacity_bytes::<(ValueKey, Value)>(
            self.seen.capacity(),
        )
        .saturating_add(self.payload_bytes)
        .saturating_add(self.inner.heap_bytes())
    }
    fn into_any(self: Box<Self>) -> Box<dyn std::any::Any + Send> {
        self
    }
    fn merge_from(&mut self, other: Box<dyn std::any::Any + Send>) {
        let other = *other
            .downcast::<Self>()
            .expect("DistinctReducer type mismatch");
        use crate::engine::executor::memory_footprint::AllocatedBytes;
        for (key, value) in other.seen {
            let bytes = key
                .allocated_bytes()
                .saturating_add(value.allocated_bytes());
            if let hashbrown::hash_map::Entry::Vacant(entry) = self.seen.entry(key) {
                self.payload_bytes = self.payload_bytes.saturating_add(bytes);
                entry.insert(value);
            }
        }
        // No inner merge: the inner is empty until finalize feeds the union.
    }
    fn clone_box(&self) -> Box<dyn Accumulator> {
        Box::new(self.clone())
    }
}

#[derive(Clone)]
struct UnsupportedAcc {
    name: String,
}
impl Accumulator for UnsupportedAcc {
    fn update(&mut self, _value: Value) -> Result<(), QueryError> {
        Err(error(
            crate::error::catalog::ER_SP_DOES_NOT_EXIST,
            format!(
                "aggregate function '{}' is not supported in accumulator path",
                self.name
            ),
        ))
    }
    fn finalize(self: Box<Self>) -> Result<Value, QueryError> {
        Err(error(
            crate::error::catalog::ER_SP_DOES_NOT_EXIST,
            format!(
                "aggregate function '{}' is not supported in accumulator path",
                self.name
            ),
        ))
    }
    fn into_any(self: Box<Self>) -> Box<dyn std::any::Any + Send> {
        self
    }
    fn merge_from(&mut self, _other: Box<dyn std::any::Any + Send>) {
        // Defense-in-depth: `update`/`finalize` already return error 1305 for
        // this accumulator, so a parallel worker fails before any merge is
        // reached. Should the routing ever change, degrade to a no-op rather
        // than panicking — `finalize` still surfaces the clean 1305 error.
        debug_assert!(false, "UnsupportedAcc should never be merged");
    }
    fn clone_box(&self) -> Box<dyn Accumulator> {
        Box::new(self.clone())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn aggregate_keys_fold_names_but_preserve_literals_and_structure() {
        fn key(sql: &str) -> u64 {
            aggregate_key(&crate::parser::parse_expr_fragment(sql).unwrap())
        }
        for (a, b) in [
            ("COUNT(*)", "couNT(*)"),
            ("sum(abs(t.V))", "SuM(AbS(T.v))"),
            ("count(distinct V)", "CoUnT(distinct v)"),
            ("count(default(V))", "CoUnT(default(v))"),
            ("sum(@Amount)", "SuM(@amount)"),
            ("sum((select max(V) from T))", "SuM((select MaX(v) from t))"),
            (
                "sum(case when V>0 then abs(V) else 0 end)",
                "SUM(case when v>0 then ABS(v) else 0 end)",
            ),
        ] {
            assert_eq!(key(a), key(b), "{a} / {b}");
        }
        for (a, b) in [
            ("min('A')", "min('a')"),
            ("count(V)", "count(distinct V)"),
            ("sum(V)", "avg(V)"),
            ("sum(V+1)", "sum(V+2)"),
            ("sum(V)", "sum(W)"),
            ("sum(json_extract(J,'$.A'))", "sum(json_extract(J,'$.a'))"),
            ("sum((select length('A')))", "sum((select length('a')))"),
        ] {
            assert_ne!(key(a), key(b), "{a} / {b}");
        }
    }

    fn lit(n: i64) -> Value {
        Value::Int(n)
    }

    #[test]
    fn count_skips_nulls() {
        let mut a = CountAcc::default();
        a.update(lit(1)).unwrap();
        a.update(Value::Null).unwrap();
        a.update(lit(2)).unwrap();
        assert_eq!(Box::new(a).finalize().unwrap(), Value::Int(2));
    }

    #[test]
    fn terminal_distinct_union_preserves_mixed_exact_numeric_keys() {
        let mut accumulators = (0..4)
            .map(|_| CountDistinctAcc::default())
            .collect::<Vec<_>>();
        for (worker, accumulator) in accumulators.iter_mut().enumerate() {
            for key in 0..40_000 {
                let key = if key < 20_000 {
                    key
                } else {
                    key + worker * 40_000
                };
                accumulator.update_u64(key as u64).unwrap();
            }
            accumulator.update_u64(u64::MAX).unwrap();
            accumulator.update_i64(i64::MIN).unwrap();
            accumulator
                .update_ref(&Value::DecimalCompact(123, 0))
                .unwrap();
        }
        let refs = accumulators
            .iter()
            .map(|acc| acc.count_distinct_keys().unwrap())
            .collect::<Vec<_>>();
        let budget = std::sync::Arc::new(crate::engine::executor::mem_budget::MemBudget::new(
            32 * 1024 * 1024,
        ));
        let count =
            crate::engine::executor::hash_aggregate::parallel_distinct::count_union(&refs, &budget)
                .unwrap()
                .unwrap();
        let mut oracle = CountDistinctAcc::default();
        for accumulator in accumulators {
            oracle.merge_from(Box::new(accumulator));
        }
        assert_eq!(
            finalized_distinct_count(count).finalize().unwrap(),
            Box::new(oracle).finalize().unwrap()
        );
        assert_eq!(count, 100_002);
    }

    #[test]
    fn count_distinct_dedups() {
        let mut a = CountDistinctAcc::default();
        a.update(lit(1)).unwrap();
        a.update(lit(1)).unwrap();
        a.update(lit(2)).unwrap();
        a.update(Value::Null).unwrap();
        assert_eq!(Box::new(a).finalize().unwrap(), Value::Int(2));
    }

    #[test]
    fn integer_sum_avg_batches_preserve_exact_extremes_and_float_order() {
        let runs = vec![
            vec![Value::Null; 16],
            vec![Value::UInt(u64::MAX); 257],
            vec![
                Value::Int(i64::MIN),
                Value::UInt(u64::MAX),
                Value::Null,
                Value::Int(i64::MAX),
            ],
            vec![Value::Int(1), Value::Double(0.5), Value::Int(3)],
            vec![Value::Int(1), Value::Int(1), Value::Int(-2)],
        ];
        for prefix in [
            Value::Null,
            Value::Double(1e16),
            Value::Decimal(
                "99999999999999999999999999999999999999999999999999999999999999999".into(),
            ),
        ] {
            for values in &runs {
                let mut sum = SumAcc::default();
                let mut sum_oracle = SumAcc::default();
                let mut avg = AvgAcc::default();
                let mut avg_oracle = AvgAcc::default();
                sum.update_ref(&prefix).unwrap();
                sum_oracle.update_ref(&prefix).unwrap();
                avg.update_ref(&prefix).unwrap();
                avg_oracle.update_ref(&prefix).unwrap();
                sum.update_batch(values).unwrap();
                avg.update_batch(values).unwrap();
                for value in values {
                    sum_oracle.update_ref(value).unwrap();
                    avg_oracle.update_ref(value).unwrap();
                }
                assert_eq!(
                    Box::new(sum).finalize().unwrap(),
                    Box::new(sum_oracle).finalize().unwrap()
                );
                assert_eq!(
                    Box::new(avg).finalize().unwrap(),
                    Box::new(avg_oracle).finalize().unwrap()
                );
            }
        }
    }

    #[test]
    fn sum_returns_null_when_all_null() {
        let mut a = SumAcc::default();
        a.update(Value::Null).unwrap();
        assert_eq!(Box::new(a).finalize().unwrap(), Value::Null);
    }

    #[test]
    fn avg_correctness() {
        let mut a = AvgAcc::default();
        for n in [1, 2, 3, 4] {
            a.update(lit(n)).unwrap();
        }
        // MySQL: AVG over exact inputs is DECIMAL with scale
        // `div_precision_increment` (default 4) — 2.5000, not DOUBLE.
        assert_eq!(
            Box::new(a).finalize().unwrap(),
            Value::DecimalCompact(25000, 4)
        );
    }

    #[test]
    fn minmax_homogeneous_batches_match_scalar_with_tagged_dates_and_mixed_values() {
        let batches = vec![
            vec![Value::Null; 7],
            vec![Value::Int(i64::MIN), Value::Null, Value::Int(i64::MAX)],
            vec![Value::UInt(0), Value::UInt(u64::MAX), Value::Null],
            vec![
                Value::Date(crate::types::ZERO_DATE),
                Value::Date(20000),
                Value::Null,
            ],
            vec![
                Value::Date(crate::types::encode_tagged_date(2024, 0, 0).unwrap()),
                Value::Date(20000),
            ],
            vec![Value::Timestamp(i64::MIN), Value::Timestamp(i64::MAX)],
            vec![Value::Time(-1), Value::Time(0), Value::Time(99)],
            vec![Value::Year(0), Value::Year(255)],
            vec![Value::Int(1), Value::UInt(u64::MAX), Value::Double(2.5)],
            vec![Value::String("z".into()), Value::String("A".into())],
        ];
        for values in batches {
            for is_min in [true, false] {
                let mut scalar = MinMaxAcc::new(is_min);
                let mut batch = MinMaxAcc::new(is_min);
                for chunk in values.chunks(2) {
                    for value in chunk {
                        scalar.update_ref(value).unwrap();
                    }
                    batch.update_batch(chunk).unwrap();
                }
                assert_eq!(
                    Box::new(batch).finalize().unwrap(),
                    Box::new(scalar).finalize().unwrap()
                );
            }
        }
    }

    #[test]
    fn minmax() {
        let mut mn = MinMaxAcc::new(true);
        let mut mx = MinMaxAcc::new(false);
        for n in [3, 1, 4, 1, 5, 9, 2] {
            mn.update(lit(n)).unwrap();
            mx.update(lit(n)).unwrap();
        }
        assert_eq!(Box::new(mn).finalize().unwrap(), Value::Int(1));
        assert_eq!(Box::new(mx).finalize().unwrap(), Value::Int(9));
    }

    #[test]
    fn exact_aggregate_promotes_to_wide_without_float_state() {
        let mut state = NumericAggState::Empty;
        accumulate_numeric_state(&mut state, &Value::Decimal128(i128::MAX, 0)).unwrap();
        accumulate_numeric_state(&mut state, &Value::DecimalCompact(1, 0)).unwrap();
        assert!(matches!(state, NumericAggState::Exact(_)));
        let total = numeric_state_to_value(&state);
        assert!(matches!(total, Value::DecimalWide(_)));
        assert_eq!(
            total.to_display_string(),
            "170141183460469231731687303715884105728"
        );
    }

    #[test]
    fn mergeable_accumulators_repeated_and_partial_merge_match_scalar() {
        let values = [
            Value::Null,
            Value::DecimalCompact(125, 2),
            Value::DecimalCompact(-50, 2),
            Value::DecimalCompact(300, 2),
        ];
        for name in ["COUNT", "SUM", "AVG", "MIN", "MAX"] {
            let kind = AggregateKind::from_sql(name, false, name == "COUNT(*)");
            let mut scalar = accumulator_for(kind, name, false, None, Vec::new(), None);
            let mut repeated = accumulator_for(kind, name, false, None, Vec::new(), None);
            for value in &values {
                for _ in 0..3 {
                    scalar.update_ref(value).unwrap();
                }
                repeated.update_repeated_ref(value, 3).unwrap();
            }
            assert_eq!(
                scalar.finalize().unwrap(),
                repeated.finalize().unwrap(),
                "update_repeated_ref mismatch for {name}"
            );

            let mut merged = accumulator_for(kind, name, false, None, Vec::new(), None);
            let mut second = accumulator_for(kind, name, false, None, Vec::new(), None);
            let mut oracle = accumulator_for(kind, name, false, None, Vec::new(), None);
            for value in &values[..2] {
                merged.update_ref(value).unwrap();
                oracle.update_ref(value).unwrap();
            }
            for value in &values[2..] {
                second.update_ref(value).unwrap();
                oracle.update_ref(value).unwrap();
            }
            merged.merge_from(second.into_any());
            assert_eq!(
                oracle.finalize().unwrap(),
                merged.finalize().unwrap(),
                "partial merge mismatch for {name}"
            );
        }
    }

    #[test]
    fn aggregate_kernel_repeated_matches_scalar_oracle() {
        use crate::engine::executor::hash_aggregate::AggregateState;
        let values = [
            Value::Null,
            Value::DecimalCompact(125, 2),
            Value::DecimalCompact(-50, 2),
            Value::DecimalCompact(300, 2),
        ];
        for name in ["COUNT", "SUM", "AVG", "MIN", "MAX"] {
            let kind = AggregateKind::from_sql(name, false, false).unwrap();
            let mut scalar = AggregateState::default();
            let mut repeated = AggregateState::default();
            for value in &values {
                for _ in 0..3 {
                    scalar.update_ref(kind, value).unwrap();
                }
                repeated.update_repeated_ref(kind, value, 3).unwrap();
            }
            assert_eq!(
                scalar.finalize(kind),
                repeated.finalize(kind),
                "kernel repeated mismatch for {name}"
            );
        }

        let mut repeated = AggregateState::default();
        repeated.update_count_star_repeated(17);
        assert_eq!(repeated.finalize(AggregateKind::CountStar), Value::Int(17));
    }
}

#[test]
fn count_accumulator_saturates_without_wrapping() {
    let mut acc = CountAcc { n: i64::MAX - 1 };
    acc.update_batch(&[Value::Int(1), Value::Null, Value::Int(2)])
        .unwrap();
    acc.update_count_star();
    assert_eq!(acc.n, i64::MAX);
}

#[test]
fn shared_numeric_batches_and_repetitions_preserve_scalar_results() {
    let cases = [
        vec![Value::Null, Value::Int(-3), Value::UInt(8)],
        vec![
            Value::DecimalCompact(125, 2),
            Value::DecimalCompact(-25, 2),
            Value::Null,
            Value::DecimalCompact(30, 1),
        ],
        vec![Value::Double(0.5), Value::Int(2), Value::Double(-0.25)],
        vec![Value::Null, Value::Null],
        vec![],
    ];
    for values in cases {
        for name in ["SUM", "AVG"] {
            for repetitions in [0, 1, 7] {
                let make = || {
                    accumulator_for(
                        AggregateKind::from_sql(name, false, false),
                        name,
                        false,
                        None,
                        vec![],
                        None,
                    )
                };
                let mut scalar = make();
                let mut repeated = make();
                let mut batch = make();
                for value in &values {
                    for _ in 0..repetitions {
                        scalar.update_ref(value).unwrap();
                    }
                    repeated.update_repeated_ref(value, repetitions).unwrap();
                }
                let expanded = values
                    .iter()
                    .flat_map(|value| std::iter::repeat_n(value.clone(), repetitions))
                    .collect::<Vec<_>>();
                batch.update_batch(&expanded).unwrap();
                let expected = scalar.finalize().unwrap();
                assert_eq!(repeated.finalize().unwrap(), expected, "{name}, {values:?}");
                assert_eq!(batch.finalize().unwrap(), expected, "{name}, {values:?}");
            }
        }
    }
}

#[cfg(test)]
mod group_concat_memory_tests {
    use super::*;
    use crate::engine::executor::memory_footprint::AllocatedBytes;

    #[test]
    fn group_concat_final_output_never_keeps_an_oversized_part_allocation() {
        let mut state = GroupConcatAcc::new(false, ",".into(), Vec::new(), 16);
        state
            .update(Value::String("x".repeat(1024 * 1024)))
            .unwrap();
        let value = Box::new(state).finalize().unwrap();
        assert_eq!(crate::types::value_str(&value), Some("xxxxxxxxxxxxxxxx"));
        assert!(
            value.allocated_bytes() <= 64,
            "oversized final capacity: {}",
            value.allocated_bytes()
        );
    }

    #[test]
    fn unordered_group_concat_retains_a_bounded_prefix_including_empty_values() {
        for separator in ["", ",", "界"] {
            let mut state = GroupConcatAcc::new(false, separator.into(), Vec::new(), 16);
            for _ in 0..10000 {
                state.update(Value::String(String::new())).unwrap();
            }
            assert!(
                state.heap_bytes() < 2048,
                "unbounded empty input: {}",
                state.heap_bytes()
            );
            let value = Box::new(state).finalize().unwrap();
            let expected = separator.repeat(9999);
            let mut end = expected.len().min(16);
            while !expected.is_char_boundary(end) {
                end -= 1;
            }
            assert_eq!(crate::types::value_str(&value), Some(&expected[..end]));
        }
    }
}

#[cfg(test)]
mod group_concat_prefix_tests {
    use super::*;

    fn bytes(value: &Value) -> Vec<u8> {
        match value {
            Value::Bytes(value) => value.clone(),
            value => value_to_string(value).into_bytes(),
        }
    }

    fn reference(
        values: &[(Value, Vec<Value>)],
        separator: &str,
        fields: &[SortField],
        cap: usize,
    ) -> Value {
        let mut values = values
            .iter()
            .filter(|(value, _)| !matches!(value, Value::Null))
            .collect::<Vec<_>>();
        if values.is_empty() {
            return Value::Null;
        }
        values.sort_by(|(_, a), (_, b)| {
            for ((a, b), field) in a.iter().zip(b).zip(fields) {
                let order = compare_value_for_sort(a, b, *field);
                if order.is_ne() {
                    return order;
                }
            }
            Ordering::Equal
        });
        let binary = values
            .iter()
            .any(|(value, _)| matches!(value, Value::Bytes(_)));
        let mut result = Vec::new();
        for (index, (value, _)) in values.iter().enumerate() {
            if index > 0 {
                result.extend_from_slice(separator.as_bytes());
            }
            result.extend_from_slice(&bytes(value));
        }
        if result.len() > cap {
            let mut end = cap;
            if !binary {
                let text = std::str::from_utf8(&result).unwrap();
                while !text.is_char_boundary(end) {
                    end -= 1;
                }
            }
            result.truncate(end);
        }
        if binary {
            Value::Bytes(result)
        } else {
            Value::String(String::from_utf8(result).unwrap())
        }
    }

    #[test]
    fn ordered_prefix_heap_matches_independent_stable_reference_and_partial_merge() {
        for ascending in [false, true] {
            for separator in ["", ",", "界"] {
                for cap in [0, 1, 4, 16, 65, 1024] {
                    for binary in [false, true] {
                        let fields = vec![SortField {
                            ascending,
                            nulls_first: !ascending,
                            collation_id: None,
                        }];
                        let values = (0..257)
                            .map(|i| {
                                let value = if binary && i == 256 {
                                    Value::Bytes(vec![255, 0])
                                } else if i % 17 == 0 {
                                    Value::Null
                                } else {
                                    Value::String(["界a", "x", "", "éxyz"][i % 4].repeat(i % 5))
                                };
                                let key = if i % 13 == 0 {
                                    Value::Null
                                } else {
                                    Value::Int((i % 7) as i64)
                                };
                                (value, vec![key])
                            })
                            .collect::<Vec<_>>();
                        let expected = reference(&values, separator, &fields, cap);
                        for partials in [1, 2, 7] {
                            let mut merged =
                                GroupConcatAcc::new(false, separator.into(), fields.clone(), cap);
                            for chunk in values.chunks(values.len().div_ceil(partials)) {
                                let mut part = GroupConcatAcc::new(
                                    false,
                                    separator.into(),
                                    fields.clone(),
                                    cap,
                                );
                                for (value, keys) in chunk {
                                    part.update_ordered(value.clone(), keys.clone()).unwrap();
                                }
                                merged.merge_from(Box::new(part));
                            }
                            assert_eq!(Box::new(merged).finalize().unwrap(), expected,
                                "asc={ascending}, sep={separator}, cap={cap}, binary={binary}, partials={partials}");
                        }
                    }
                }
            }
        }
    }

    #[test]
    fn unordered_prefix_merge_preserves_separators_empty_values_and_late_binary_type() {
        for separator in ["", ",", "界"] {
            for cap in [0, 1, 4, 17, 64] {
                for binary in [false, true] {
                    let mut values = vec![
                        Value::String(String::new()),
                        Value::String("界éx".into()),
                        Value::Null,
                        Value::String("second".into()),
                        Value::String(String::new()),
                    ];
                    if binary {
                        values.push(Value::Bytes(vec![255]));
                    }
                    let values = values
                        .into_iter()
                        .map(|value| (value, Vec::new()))
                        .collect::<Vec<_>>();
                    let expected = reference(&values, separator, &[], cap);
                    let mut merged = GroupConcatAcc::new(false, separator.into(), Vec::new(), cap);
                    for chunk in values.chunks(2) {
                        let mut part =
                            GroupConcatAcc::new(false, separator.into(), Vec::new(), cap);
                        for (value, _) in chunk {
                            part.update(value.clone()).unwrap();
                        }
                        merged.merge_from(Box::new(part));
                    }
                    assert_eq!(Box::new(merged).finalize().unwrap(), expected);
                }
            }
        }
    }
}

#[cfg(test)]
mod retained_state_memory_tests {
    use super::*;
    use crate::engine::executor::memory_footprint::AllocatedBytes;

    #[test]
    fn first_value_counts_all_owned_payloads_and_spare_capacity() {
        for ctor in [Value::Bytes, Value::Json, Value::Geometry, |v: Vec<u8>| {
            Value::Vector(v.into())
        }] {
            let mut bytes = Vec::with_capacity(128 * 1024);
            bytes.push(1);
            let value = ctor(bytes);
            let expected = value.allocated_bytes();
            let mut acc = FirstValueAcc::default();
            acc.update(value).unwrap();
            assert!(
                acc.heap_bytes() >= expected,
                "first value retained {expected} bytes but reports {}",
                acc.heap_bytes()
            );
        }
    }

    #[test]
    fn scalar_count_distinct_counts_variable_keys_after_merge() {
        let mut acc = CountDistinctAcc::default();
        acc.update(Value::String("x".repeat(64 * 1024))).unwrap();
        assert!(
            acc.heap_bytes() >= 64 * 1024,
            "wide key is absent from heap accounting"
        );
        let mut other = CountDistinctAcc::default();
        other.update(Value::String("y".repeat(64 * 1024))).unwrap();
        acc.merge_from(Box::new(other));
        assert!(acc.heap_bytes() >= 128 * 1024);
        let unchanged = acc.heap_bytes();
        acc.update(Value::String("X".repeat(64 * 1024))).unwrap();
        assert_eq!(
            acc.heap_bytes(),
            unchanged,
            "duplicate key changed retained payload bytes"
        );
    }

    #[test]
    fn scalar_distinct_reducer_counts_key_and_original_representative() {
        let mut acc = DistinctReducer::new(SumAcc::default());
        acc.update(Value::Bytes(vec![b'1'; 64 * 1024])).unwrap();
        assert!(
            acc.heap_bytes() >= 128 * 1024,
            "DISTINCT reducer omitted the key or representative: {}",
            acc.heap_bytes()
        );
    }

    #[test]
    fn json_array_aggregate_counts_retained_order_key_payload() {
        let mut acc = JsonArrayAggAcc::new(vec![SortField {
            ascending: true,
            nulls_first: true,
            collation_id: None,
        }]);
        acc.update_ordered(Value::Int(1), vec![Value::Bytes(vec![1; 64 * 1024])])
            .unwrap();
        assert!(
            acc.heap_bytes() >= 64 * 1024,
            "JSON ordered key is unaccounted"
        );
    }
}

#[cfg(test)]
mod minmax_resident_memory_tests {
    use super::*;
    #[test]
    fn boxed_minmax_counts_its_retained_variable_value() {
        for is_min in [false, true] {
            let mut acc = MinMaxAcc::new(is_min);
            acc.update(Value::Bytes(vec![1; 64 * 1024])).unwrap();
            assert!(
                acc.heap_bytes() >= 64 * 1024,
                "MIN/MAX retained bytes are unaccounted"
            );
        }
    }
}
