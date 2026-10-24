//! Unified selectivity / cardinality estimation API.
//!
//! Phase 2 wave 1 establishes the **public contract** that Phase 3 operator
//! cost formulas will call into. The contract is layered so callers don't
//! need to know whether a stat is histogram-based, NDV-based, or a default
//! constant — that decision is made internally by each implementation.
//!
//! ## The three-tier fallback
//!
//! Every estimator walks the same source-of-truth ladder. The chosen tier
//! is reported alongside the estimate so EXPLAIN FORMAT=JSON (Phase 3+)
//! can surface "where did this number come from".
//!
//! 1. **L1 — Histogram** (`SelectivitySource::Histogram`): Phase B equi-depth
//!    histogram (32 buckets). Most accurate for ranges; equality lookups
//!    against the TOP-K table also count as L1.
//! 2. **L2 — NDV / TOP-K / min-max** (`SelectivitySource::Ndv`): used when
//!    the column has `distinct_count` and/or `min`/`max` but no histogram.
//! 3. **L3 — Default constant** (`SelectivitySource::Default`): no stats,
//!    fall back to `cbo_default_*_selectivity` GUCs.
//!
//! ## Why a trait
//!
//! The trait shape lets us:
//! - swap implementations in tests (e.g. inject a fixed-selectivity stub),
//! - migrate the actual selectivity logic out of `planner.rs` over later
//!   phases without re-touching callers,
//! - layer a Phase 4 *correlation-aware* implementation on top without
//!   forking call sites.
//!
//! Phase 2 wave 1 ships a single concrete impl: [`StatsBackedSelectivity`],
//! which wraps the proven estimators currently living in `planner.rs`. Wave
//! 2 (under Phase 3) will move those bodies into this module and add the
//! `SelectivitySource` reporting hook.

use crate::parser::{BinaryOp, Expr};
use crate::planner;
use crate::storage::TableSchema;
use crate::types::Value;

/// Where did an estimate come from? Reported in EXPLAIN FORMAT=JSON so
/// users can tell whether the planner had real statistics or fell back
/// to a default constant.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SelectivitySource {
    /// Phase B equi-depth histogram (32 buckets) hit.
    Histogram,
    /// Stats had `distinct_count` / TOP-K / min-max but no histogram, or
    /// the value fell outside the histogram's covered range.
    Ndv,
    /// No usable statistics — value comes from `cbo_default_*_selectivity`.
    Default,
}

/// Estimate the selectivity (in the [0, 1] range) or the absolute row
/// count of a predicate against a base table.
///
/// All methods take `total_rows` (the table's pre-filter cardinality) and
/// return an *absolute row count*. Callers that want pure selectivity
/// divide by `total_rows`.
///
/// Implementations clamp results to `[0.0, total_rows]`. Fractional expected
/// cardinalities are valid, and provably empty predicates must remain zero;
/// operators that need a non-zero loop count handle that locally.
pub trait Selectivity {
    /// Equality predicate on a single column (`col = val`).
    ///
    /// L1: TOP-K hit returns the recorded frequency; histogram bucket
    /// otherwise. L2: `(total - null) / ndv`. L3: `total * cbo_default_eq`.
    fn col_eq_rows(&self, table: &TableSchema, column: &str, value: &Value, total_rows: f64)
        -> f64;

    /// Range predicate via a single comparison op (`col > v`, `col <= v`,
    /// etc). L1: histogram cumulative; L2: `(v - min) / (max - min)`; L3:
    /// `total * cbo_default_range`.
    fn col_range_rows(
        &self,
        table: &TableSchema,
        column: &str,
        op: BinaryOp,
        value: &Value,
        total_rows: f64,
    ) -> f64;

    /// Closed range (`col BETWEEN low AND high`).
    fn col_between_rows(
        &self,
        table: &TableSchema,
        column: &str,
        low: &Value,
        high: &Value,
        total_rows: f64,
    ) -> f64;

    /// `col LIKE pattern`. Constant-prefix patterns route through
    /// [`Self::col_range_rows`]; everything else falls back to
    /// `cbo_default_like`.
    fn col_like_rows(
        &self,
        table: &TableSchema,
        column: &str,
        pattern: &str,
        total_rows: f64,
    ) -> f64;

    /// Top-level entry point: estimate the row count surviving an
    /// arbitrary boolean predicate `expr`. AND/OR/NOT/BETWEEN/IN are
    /// composed inside the implementation under independence assumption
    /// (Phase 4+ may layer in a correlation hook).
    fn predicate_rows(&self, table: &TableSchema, expr: &Expr, total_rows: f64) -> f64;

    /// Equi-join cardinality between two physical plan nodes joined on
    /// `(left_keys, right_keys)`. Zero is a valid estimate for disjoint
    /// domains or empty children.
    fn join_eq_rows(
        &self,
        left: &planner::PhysicalPlan,
        right: &planner::PhysicalPlan,
        left_keys: &[Expr],
        right_keys: &[Expr],
        left_rows: f64,
        right_rows: f64,
    ) -> f64;
}

/// Production [`Selectivity`] implementation backed by the table catalog
/// statistics (`row_count`, per-column `ColumnStats`, equi-depth
/// histogram). Wraps the battle-tested estimators currently living in
/// `planner.rs`; Phase 3 wave 2 migrates the bodies here so the entire
/// L1/L2/L3 ladder is local to this module.
#[derive(Default)]
pub struct StatsBackedSelectivity;

impl StatsBackedSelectivity {
    pub const fn new() -> Self {
        StatsBackedSelectivity
    }
}

impl Selectivity for StatsBackedSelectivity {
    fn col_eq_rows(
        &self,
        table: &TableSchema,
        column: &str,
        value: &Value,
        total_rows: f64,
    ) -> f64 {
        // Encode `column = value` as an Expr so the existing implementation
        // can dispatch through its three-tier ladder uniformly. The wrapper
        // disappears in wave 2 once we move the body inline.
        let expr = Expr::Binary {
            left: Box::new(Expr::Identifier(column.to_string())),
            op: BinaryOp::Eq,
            right: Box::new(Expr::Literal(value_to_literal(value))),
        };
        planner::estimate_filter_rows(table, &expr, total_rows)
    }

    fn col_range_rows(
        &self,
        table: &TableSchema,
        column: &str,
        op: BinaryOp,
        value: &Value,
        total_rows: f64,
    ) -> f64 {
        let expr = Expr::Binary {
            left: Box::new(Expr::Identifier(column.to_string())),
            op,
            right: Box::new(Expr::Literal(value_to_literal(value))),
        };
        planner::estimate_filter_rows(table, &expr, total_rows)
    }

    fn col_between_rows(
        &self,
        table: &TableSchema,
        column: &str,
        low: &Value,
        high: &Value,
        total_rows: f64,
    ) -> f64 {
        let expr = Expr::Between {
            expr: Box::new(Expr::Identifier(column.to_string())),
            low: Box::new(Expr::Literal(value_to_literal(low))),
            high: Box::new(Expr::Literal(value_to_literal(high))),
            negated: false,
        };
        planner::estimate_filter_rows(table, &expr, total_rows)
    }

    fn col_like_rows(
        &self,
        table: &TableSchema,
        column: &str,
        pattern: &str,
        total_rows: f64,
    ) -> f64 {
        let expr = Expr::Like {
            expr: Box::new(Expr::Identifier(column.to_string())),
            pattern: Box::new(Expr::Literal(crate::parser::Literal::String(
                pattern.to_string(),
            ))),
            negated: false,
            escape_char: None,
        };
        planner::estimate_filter_rows(table, &expr, total_rows)
    }

    fn predicate_rows(&self, table: &TableSchema, expr: &Expr, total_rows: f64) -> f64 {
        planner::estimate_filter_rows(table, expr, total_rows)
    }

    fn join_eq_rows(
        &self,
        left: &planner::PhysicalPlan,
        right: &planner::PhysicalPlan,
        left_keys: &[Expr],
        right_keys: &[Expr],
        left_rows: f64,
        right_rows: f64,
    ) -> f64 {
        planner::estimate_equijoin_rows(left, right, left_keys, right_keys, left_rows, right_rows)
    }
}

/// Convert a runtime [`Value`] into the parser's [`crate::parser::Literal`]
/// variant for synthetic Expr construction. Falls back to `Null` on
/// types the parser literal grammar doesn't directly model (the
/// downstream estimator treats these as opaque and routes to L3).
fn value_to_literal(v: &Value) -> crate::parser::Literal {
    use crate::parser::Literal;
    match v {
        Value::Null => Literal::Null,
        Value::Int(i) => Literal::Int(*i),
        Value::UInt(i) => Literal::UInt(*i),
        Value::Float(f) => Literal::Float(*f as f64),
        Value::Double(f) => Literal::Float(*f),
        Value::Decimal(s) => s
            .parse::<f64>()
            .map(Literal::Float)
            .unwrap_or(Literal::Null),
        Value::String(s) => Literal::String(s.clone()),
        Value::SmallString(b) => {
            Literal::String(unsafe { std::str::from_utf8_unchecked(b) }.to_string())
        }
        Value::Bytes(_) => Literal::Null,
        Value::Geometry(_) => Literal::Null,
        Value::Vector(_) => Literal::Null,
        Value::DecimalCompact(mag, scale) => {
            Literal::Float(*mag as f64 / 10f64.powi(*scale as i32))
        }
        Value::Decimal128(..) | Value::DecimalWide(..) => Literal::Decimal(v.to_display_string()),
        // Temporal values are routed as String literals (the
        // selectivity estimator runs string comparisons against
        // histogram boundaries, which are themselves the canonical
        // ASCII form).
        Value::Date(_)
        | Value::Time(_)
        | Value::DateTime(_)
        | Value::Timestamp(_)
        | Value::Year(_) => Literal::String(v.to_display_string()),
        // JSON is opaque to the selectivity estimator; route to L3.
        Value::Json(_) => Literal::Null,
    }
}

/// The default global selectivity instance the rest of the optimizer
/// reaches for. Stateless and cheap to clone.
pub fn default_selectivity() -> StatsBackedSelectivity {
    StatsBackedSelectivity::new()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::ColumnStats;
    use std::sync::Mutex;

    static CBO_SELECTIVITY_TEST_LOCK: Mutex<()> = Mutex::new(());

    fn empty_schema(col: &str) -> TableSchema {
        super::super::test_support::table_schema(&[(col, crate::storage::DataTypeKind::Int)])
    }

    fn schema_with_stats(col: &str, stats: ColumnStats, row_count: u64) -> TableSchema {
        let mut schema = empty_schema(col);
        schema.stats.row_count = row_count;
        schema.stats.columns.insert(col.to_string(), stats);
        schema
    }

    #[test]
    fn col_eq_uses_ndv_when_no_topk() {
        // 1000 rows, 100 distinct, no TOP-K → expect ~10 rows per equality.
        let stats = ColumnStats {
            null_count: 0,
            distinct_count: 100,
            min: None,
            max: None,
            top_values: Vec::new(),
            average_width_bytes: Some(8.0),
            histogram: Vec::new(),
            frequency: Default::default(),
        };
        let table = schema_with_stats("a", stats, 1000);
        let sel = default_selectivity();
        let rows = sel.col_eq_rows(&table, "a", &Value::Int(42), 1000.0);
        assert!(rows >= 9.0 && rows <= 11.0, "got {rows}");
    }

    #[test]
    fn col_eq_preserves_fractional_expected_cardinality() {
        let stats = ColumnStats {
            null_count: 0,
            distinct_count: 1_000_000,
            min: None,
            max: None,
            top_values: Vec::new(),
            average_width_bytes: Some(8.0),
            histogram: Vec::new(),
            frequency: Default::default(),
        };
        let table = schema_with_stats("a", stats, 100);
        let sel = default_selectivity();
        let rows = sel.col_eq_rows(&table, "a", &Value::Int(7), 100.0);
        assert!(rows > 0.0 && rows < 1.0, "got {rows}");
    }

    #[test]
    fn col_eq_with_no_stats_falls_back() {
        // No column stats at all → L3 default. Must produce something
        // sane (between 1 and total_rows), not panic, not return 0.
        let mut schema = empty_schema("a");
        schema.stats.row_count = 1000;
        let sel = default_selectivity();
        let rows = sel.col_eq_rows(&schema, "a", &Value::Int(42), 1000.0);
        assert!(rows >= 1.0 && rows <= 1000.0, "got {rows}");
    }

    #[test]
    fn col_eq_missing_stats_uses_cbo_default() {
        let _guard = CBO_SELECTIVITY_TEST_LOCK.lock().unwrap();
        let prior = crate::engine::cbo::cbo_consts().default_eq_selectivity();
        crate::engine::cbo::set_variable("cbo_default_eq_selectivity", 0.1).unwrap();
        let mut schema = empty_schema("a");
        schema.stats.row_count = 1000;

        let rows = default_selectivity().col_eq_rows(&schema, "a", &Value::Int(42), 1000.0);

        crate::engine::cbo::set_variable("cbo_default_eq_selectivity", prior).unwrap();
        assert_eq!(rows, 100.0);
    }

    #[test]
    fn predicate_rows_passes_through_compound_expr() {
        let stats = ColumnStats {
            null_count: 0,
            distinct_count: 50,
            min: Some(Value::Int(0)),
            max: Some(Value::Int(99)),
            top_values: Vec::new(),
            average_width_bytes: Some(8.0),
            histogram: Vec::new(),
            frequency: Default::default(),
        };
        let table = schema_with_stats("a", stats, 1000);
        // a > 50 AND a < 80 — independence assumption applies.
        let expr = Expr::Binary {
            left: Box::new(Expr::Binary {
                left: Box::new(Expr::Identifier("a".to_string())),
                op: BinaryOp::Gt,
                right: Box::new(Expr::Literal(crate::parser::Literal::Int(50))),
            }),
            op: BinaryOp::And,
            right: Box::new(Expr::Binary {
                left: Box::new(Expr::Identifier("a".to_string())),
                op: BinaryOp::Lt,
                right: Box::new(Expr::Literal(crate::parser::Literal::Int(80))),
            }),
        };
        let sel = default_selectivity();
        let rows = sel.predicate_rows(&table, &expr, 1000.0);
        // Result must be a valid count, not zero or larger than total.
        assert!(rows >= 1.0 && rows <= 1000.0, "got {rows}");
    }

    /// Build a two-column table `(a, b)` with a composite index `idx(a, b)`
    /// whose per-prefix NDV stats are supplied. Per-column NDVs are set so
    /// the independence product and the composite-prefix estimate diverge.
    fn schema_with_composite_index(
        row_count: u64,
        a_ndv: u64,
        b_ndv: u64,
        prefix_distinct_counts: &[u64],
    ) -> TableSchema {
        let json = format!(
            r#"{{
                "id": 9,
                "database": "test",
                "name": "ct",
                "columns": [
                    {{"name": "a", "data_type": "Int"}},
                    {{"name": "b", "data_type": "Int"}}
                ],
                "primary_key": [],
                "indexes": [
                    {{
                        "name": "idx_ab",
                        "columns": ["a", "b"],
                        "unique": false,
                        "primary": false
                    }}
                ],
                "stats": {{
                    "row_count": {row_count},
                    "columns": {{
                        "a": {{"null_count": 0, "distinct_count": {a_ndv},
                               "min": null, "max": null, "top_values": []}},
                        "b": {{"null_count": 0, "distinct_count": {b_ndv},
                               "min": null, "max": null, "top_values": []}}
                    }},
                    "indexes": {{
                        "idx_ab": {{"prefix_distinct_counts": {prefix_counts}}}
                    }},
                    "modified_rows_since_analyze": 0,
                    "analyzed_at_unix_ms": 1
                }}
            }}"#,
            prefix_counts = serde_json::to_string(prefix_distinct_counts).unwrap(),
        );
        let mut schema: TableSchema = serde_json::from_str(&json).unwrap();
        schema.build_column_index();
        schema
    }

    fn and_eq_eq(a_val: i64, b_val: i64) -> Expr {
        Expr::Binary {
            left: Box::new(Expr::Binary {
                left: Box::new(Expr::Identifier("a".to_string())),
                op: BinaryOp::Eq,
                right: Box::new(Expr::Literal(crate::parser::Literal::Int(a_val))),
            }),
            op: BinaryOp::And,
            right: Box::new(Expr::Binary {
                left: Box::new(Expr::Identifier("b".to_string())),
                op: BinaryOp::Eq,
                right: Box::new(Expr::Literal(crate::parser::Literal::Int(b_val))),
            }),
        }
    }

    #[test]
    fn and_eq_uses_composite_prefix_ndv_for_correlated_columns() {
        // 10_000 rows. a has 100 distinct, b has 100 distinct, but a and b
        // are *perfectly correlated*: the real composite (a,b) has only 100
        // distinct pairs (prefix NDV[2] = 100). Independence would estimate
        // 10000/100 * 10000/100 / 10000 = 1 row; the composite prefix gives
        // 10000/100 = 100 rows, which is the truth.
        let table = schema_with_composite_index(10_000, 100, 100, &[100, 100]);
        let expr = and_eq_eq(7, 7);
        let rows = crate::planner::estimate_filter_rows(&table, &expr, 10_000.0);
        assert!(
            (rows - 100.0).abs() <= 5.0,
            "composite prefix NDV should yield ~100 rows, got {rows}"
        );
    }

    #[test]
    fn and_eq_independence_when_no_composite_stats() {
        // Same column NDVs, but NO composite prefix NDV available for
        // prefix length 2 (only the single-column prefix is recorded).
        // Must fall back to independence: 10000/100 * 10000/100 / 10000 = 1.
        let table = schema_with_composite_index(10_000, 100, 100, &[100]);
        let expr = and_eq_eq(7, 7);
        let rows = crate::planner::estimate_filter_rows(&table, &expr, 10_000.0);
        assert!(
            (rows - 1.0).abs() <= 1.0,
            "no composite stats should fall back to independence (~1 row), got {rows}"
        );
    }

    #[test]
    fn and_eq_composite_estimate_scales_with_population() {
        // Sampled 1000 rows during ANALYZE, but the live table has 5000
        // rows (total_rows). prefix NDV[2] = 50 on the sample -> per-key
        // 1000/50 = 20 rows sampled, scaled x5 = 100 rows in the
        // population. Verifies stats_scale is applied.
        let table = schema_with_composite_index(1000, 200, 200, &[200, 50]);
        let expr = and_eq_eq(3, 9);
        let rows = crate::planner::estimate_filter_rows(&table, &expr, 5000.0);
        assert!(
            (rows - 100.0).abs() <= 10.0,
            "composite estimate should scale to population (~100), got {rows}"
        );
    }

    /// Build `(a, b)` carrying a set-keyed multi-column group stat (joint
    /// NDV) but NO composite-index prefix NDV — isolating the Phase-3-full
    /// `multi_column_stats` path from the leading-prefix path.
    fn schema_with_multi_column_group(
        row_count: u64,
        a_ndv: u64,
        b_ndv: u64,
        group_ndv: u64,
    ) -> TableSchema {
        let json = format!(
            r#"{{
                "id": 11,
                "database": "test",
                "name": "mc",
                "columns": [
                    {{"name": "a", "data_type": "Int"}},
                    {{"name": "b", "data_type": "Int"}}
                ],
                "primary_key": [],
                "indexes": [],
                "stats": {{
                    "row_count": {row_count},
                    "columns": {{
                        "a": {{"null_count": 0, "distinct_count": {a_ndv},
                               "min": null, "max": null, "top_values": []}},
                        "b": {{"null_count": 0, "distinct_count": {b_ndv},
                               "min": null, "max": null, "top_values": []}}
                    }},
                    "indexes": {{}},
                    "multi_column_stats": {{
                        "ab": {{"columns": ["a", "b"], "distinct_count": {group_ndv}}}
                    }},
                    "modified_rows_since_analyze": 0,
                    "analyzed_at_unix_ms": 1
                }}
            }}"#
        );
        let mut schema: TableSchema = serde_json::from_str(&json).unwrap();
        schema.build_column_index();
        schema
    }

    #[test]
    fn and_eq_uses_multi_column_group_ndv_order_independent() {
        // 10_000 rows; a and b each have 100 distinct values but are
        // perfectly correlated — the {a,b} group has joint NDV 100. There
        // is NO composite-index prefix stat, so only the Phase-3-full
        // multi_column path can beat independence (which would give ~1 row).
        let table = schema_with_multi_column_group(10_000, 100, 100, 100);

        let rows = crate::planner::estimate_filter_rows(&table, &and_eq_eq(7, 7), 10_000.0);
        assert!(
            (rows - 100.0).abs() <= 5.0,
            "multi-column group NDV should yield ~100 rows, got {rows}"
        );

        // `b = ? AND a = ?` — reversed predicate order must hit the same
        // group (the store is keyed by the canonical column *set*).
        let reversed = Expr::Binary {
            left: Box::new(Expr::Binary {
                left: Box::new(Expr::Identifier("b".to_string())),
                op: BinaryOp::Eq,
                right: Box::new(Expr::Literal(crate::parser::Literal::Int(7))),
            }),
            op: BinaryOp::And,
            right: Box::new(Expr::Binary {
                left: Box::new(Expr::Identifier("a".to_string())),
                op: BinaryOp::Eq,
                right: Box::new(Expr::Literal(crate::parser::Literal::Int(7))),
            }),
        };
        let rows_rev = crate::planner::estimate_filter_rows(&table, &reversed, 10_000.0);
        assert!(
            (rows_rev - 100.0).abs() <= 5.0,
            "order-independent group lookup should yield ~100 rows, got {rows_rev}"
        );
    }
}
