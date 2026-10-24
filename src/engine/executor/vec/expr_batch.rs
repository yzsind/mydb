//! Batch / column-at-a-time expression evaluation.
//!
//! Implements `eval_batch` for the subset of `ExprProgram` nodes that have
//! a typed hot-path. Anything outside the supported set is evaluated by
//! looping over rows and calling `ExprProgram::eval_row` — the legacy
//! interpreter remains the ground truth.

use crate::engine::expr_program::input_expr_column_index as resolve_batch_column;
use std::borrow::Cow;
use std::sync::Arc;

use crate::engine::expr::ValueKey;
use crate::engine::expr_program::{
    with_eval_scratch, ExprCompiler, ExprProgram, InputColumn, InputSchema, LikePlan, RowView,
    StringTransformKind, TemporalExtractKind,
};
use crate::parser::{BinaryOp, Expr};
use crate::types::{ColumnType, QueryError, Value};

use super::batch::{Batch, ColumnVector, Selection};
use super::bitmap::Bitmap;
use super::schema::BatchSchema;

/// Reusable hybrid batch IR. Boolean structure is evaluated with three-value
/// bitmaps; each safe leaf independently selects a typed lane kernel or falls
/// back to its own `ExprProgram`. Deterministic, non-erroring long-tail nodes
/// therefore materialise only their selected rows. Any subtree that may warn,
/// error, depend on session state or invoke volatile functions keeps the whole
/// predicate in the row-wise oracle so observable evaluation order is stable.
pub struct BatchExprProgram {
    root: BatchPredicateNode,
    native_rows: std::sync::atomic::AtomicU64,
    fallback_rows: std::sync::atomic::AtomicU64,
    _runtime_charge: Option<crate::engine::executor::mem_budget::OwnedMemCharge>,
}

enum BatchPredicateNode {
    And(Box<BatchPredicateNode>, Box<BatchPredicateNode>),
    Or(Box<BatchPredicateNode>, Box<BatchPredicateNode>),
    Not(Box<BatchPredicateNode>),
    TimestampDatePlusIntegerCmp {
        timestamp_column: usize,
        integer_column: usize,
        op: BinaryOp,
        bound: i64,
        oracle: Arc<ExprProgram>,
    },
    Leaf {
        program: Arc<ExprProgram>,
        kernel: PredicateKernel,
    },
}

/// Classify the immutable expression once. Each kernel still validates the
/// current parameter and physical lane before doing any observable work.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum PredicateKernel {
    SubstringIn,
    StringIn,
    IntegerIn,
    Coalesce,
    ColumnConstant,
    ColumnColumn,
    Like,
    Scalar,
}

impl PredicateKernel {
    fn select(program: &ExprProgram) -> Self {
        if program.as_substring_column_string_in_const_set().is_some() {
            Self::SubstringIn
        } else if program.as_column_string_in_const_set().is_some() {
            Self::StringIn
        } else if program.as_column_integer_in_const_set().is_some() {
            Self::IntegerIn
        } else if program.as_coalesce_column_const_cmp().is_some() {
            Self::Coalesce
        } else if program.as_column_const_cmp_shape().is_some()
            || program.as_column_param_cmp_shape().is_some()
        {
            Self::ColumnConstant
        } else if program.as_column_column_cmp().is_some() {
            Self::ColumnColumn
        } else if program.as_column_like_const().is_some()
            || program.as_column_like_param().is_some()
        {
            Self::Like
        } else {
            Self::Scalar
        }
    }
}

impl BatchPredicateNode {
    fn leaf(program: Arc<ExprProgram>) -> Self {
        Self::Leaf {
            kernel: PredicateKernel::select(&program),
            program,
        }
    }
}

struct TruthMask {
    truth: Bitmap,
    null: Bitmap,
}

impl BatchExprProgram {
    pub(crate) fn referenced_columns(&self) -> std::collections::HashSet<usize> {
        fn visit(node: &BatchPredicateNode, columns: &mut std::collections::HashSet<usize>) {
            match node {
                BatchPredicateNode::And(left, right) | BatchPredicateNode::Or(left, right) => {
                    visit(left, columns);
                    visit(right, columns);
                }
                BatchPredicateNode::Not(expr) => visit(expr, columns),
                BatchPredicateNode::Leaf { program, .. }
                | BatchPredicateNode::TimestampDatePlusIntegerCmp {
                    oracle: program, ..
                } => {
                    columns.extend(program.referenced_columns());
                }
            }
        }
        let mut columns = std::collections::HashSet::new();
        visit(&self.root, &mut columns);
        columns
    }

    pub fn compile(expr: &Expr, schema: &BatchSchema) -> Result<Option<Arc<Self>>, QueryError> {
        Self::compile_impl(expr, schema, true, None)
    }

    pub(crate) fn compile_runtime_constants(
        expr: &Expr,
        schema: &BatchSchema,
        charge: Option<crate::engine::executor::mem_budget::OwnedMemCharge>,
    ) -> Result<Option<Arc<Self>>, QueryError> {
        Self::compile_impl(expr, schema, false, charge)
    }

    fn compile_impl(
        expr: &Expr,
        schema: &BatchSchema,
        cache: bool,
        runtime_charge: Option<crate::engine::executor::mem_budget::OwnedMemCharge>,
    ) -> Result<Option<Arc<Self>>, QueryError> {
        let input = batch_schema_input(schema);
        // Splitting a boolean tree changes evaluation from row-major to
        // subtree-major order. That is safe only when every locally-fallback
        // leaf is deterministic and cannot raise a warning/error. Otherwise
        // keep the whole expression in one row-wise oracle program so volatile
        // call counts and warning/error order remain exactly unchanged. Try
        // the split first: compiling the complete predicate eagerly and then
        // compiling every safe leaf repeated the most expensive AST/schema
        // cache-key walk on every execution. The whole-program oracle is only
        // needed when at least one leaf proves unsafe or unsupported.
        let root = match compile_batch_predicate_node(expr, &input, cache)? {
            Some(root) => root,
            None => {
                let whole_program = match compile_program(expr, &input, cache) {
                    Ok(program) => program,
                    Err(_) => match ExprCompiler::compile_legacy_oracle(expr, &input) {
                        Ok(program) => Arc::new(program),
                        Err(_) => return Ok(None),
                    },
                };
                BatchPredicateNode::leaf(whole_program)
            }
        };
        Ok(Some(Arc::new(Self {
            root,
            native_rows: std::sync::atomic::AtomicU64::new(0),
            fallback_rows: std::sync::atomic::AtomicU64::new(0),
            _runtime_charge: runtime_charge,
        })))
    }

    /// HAVING consumes finalized aggregate Values. Conditional expressions in
    /// the PlanRow oracle use runtime (not table-column) coercion there, so
    /// compile the predicate in the corresponding post-aggregate mode. It is
    /// intentionally one leaf: row-wise evaluation inside each batch keeps
    /// short-circuit, warning and volatile-function order identical.
    pub fn compile_post_aggregate(
        expr: &Expr,
        schema: &BatchSchema,
    ) -> Result<Option<Arc<Self>>, QueryError> {
        Self::compile_post_aggregate_runtime_constants(expr, schema, None)
    }

    /// Compile a post-aggregate predicate after statement-constant subqueries
    /// have been replaced with runtime values.  The optional charge owns any
    /// bounded constant set retained by the compiled program and therefore
    /// follows the program until the aggregate output filter is dropped.
    pub(crate) fn compile_post_aggregate_runtime_constants(
        expr: &Expr,
        schema: &BatchSchema,
        runtime_charge: Option<crate::engine::executor::mem_budget::OwnedMemCharge>,
    ) -> Result<Option<Arc<Self>>, QueryError> {
        let input = batch_schema_input(schema);
        let program = match ExprCompiler::compile_dynamic_conditional(expr, &input) {
            Ok(program) => Arc::new(program),
            Err(_) => return Ok(None),
        };
        Ok(Some(Arc::new(Self {
            root: BatchPredicateNode::leaf(program),
            native_rows: std::sync::atomic::AtomicU64::new(0),
            fallback_rows: std::sync::atomic::AtomicU64::new(0),
            _runtime_charge: runtime_charge,
        })))
    }

    pub fn eval_predicate_bitmap(&self, batch: &Batch) -> Result<Bitmap, QueryError> {
        let mask =
            eval_batch_predicate_node(&self.root, batch, &self.native_rows, &self.fallback_rows)?;
        Ok(restrict_bitmap_to_selection(batch, mask.truth))
    }

    #[cfg_attr(not(test), allow(dead_code))]
    pub fn expression_rows(&self) -> (u64, u64) {
        use std::sync::atomic::Ordering;
        (
            self.native_rows.load(Ordering::Relaxed),
            self.fallback_rows.load(Ordering::Relaxed),
        )
    }
}

fn compile_batch_predicate_node(
    expr: &Expr,
    input: &InputSchema,
    cache: bool,
) -> Result<Option<BatchPredicateNode>, QueryError> {
    if let Some(node) = compile_timestamp_date_plus_integer_cmp(expr, input, cache) {
        return Ok(Some(node));
    }
    let node = match expr {
        Expr::Binary {
            left,
            op: BinaryOp::And,
            right,
        } => {
            let Some(left) = compile_batch_predicate_node(left, input, cache)? else {
                return Ok(None);
            };
            let Some(right) = compile_batch_predicate_node(right, input, cache)? else {
                return Ok(None);
            };
            BatchPredicateNode::And(Box::new(left), Box::new(right))
        }
        Expr::Binary {
            left,
            op: BinaryOp::Or,
            right,
        } => {
            let Some(left) = compile_batch_predicate_node(left, input, cache)? else {
                return Ok(None);
            };
            let Some(right) = compile_batch_predicate_node(right, input, cache)? else {
                return Ok(None);
            };
            BatchPredicateNode::Or(Box::new(left), Box::new(right))
        }
        Expr::Unary {
            op: crate::parser::UnaryOp::Not,
            expr,
        } => {
            let Some(expr) = compile_batch_predicate_node(expr, input, cache)? else {
                return Ok(None);
            };
            BatchPredicateNode::Not(Box::new(expr))
        }
        Expr::Between {
            expr: value,
            low,
            high,
            negated,
        } if matches!(
            value.as_ref(),
            Expr::Identifier(_) | Expr::CompoundIdentifier(_)
        ) && (crate::engine::expr::expr_is_constant(low)
            || matches!(low.as_ref(), Expr::Placeholder(_)))
            && (crate::engine::expr::expr_is_constant(high)
                || matches!(high.as_ref(), Expr::Placeholder(_))) =>
        {
            // A column is side-effect free, so BETWEEN can use the same two
            // column/constant bitmap leaves as the equivalent comparisons.
            // This preserves SQL three-valued logic while avoiding a row-wise
            // Value materialisation for fixed-width numeric/temporal lanes.
            let (low_op, high_op, combine_and) = if *negated {
                (BinaryOp::Lt, BinaryOp::Gt, false)
            } else {
                (BinaryOp::GtEq, BinaryOp::LtEq, true)
            };
            let lower = Expr::Binary {
                left: value.clone(),
                op: low_op,
                right: low.clone(),
            };
            let upper = Expr::Binary {
                left: value.clone(),
                op: high_op,
                right: high.clone(),
            };
            let lower = compile_program(&lower, input, cache).ok();
            let upper = compile_program(&upper, input, cache).ok();
            let (Some(lower), Some(upper)) = (lower, upper) else {
                return Ok(None);
            };
            if !leaf_is_safe_to_split(&lower) || !leaf_is_safe_to_split(&upper) {
                return Ok(None);
            }
            let lower = BatchPredicateNode::leaf(lower);
            let upper = BatchPredicateNode::leaf(upper);
            if combine_and {
                BatchPredicateNode::And(Box::new(lower), Box::new(upper))
            } else {
                BatchPredicateNode::Or(Box::new(lower), Box::new(upper))
            }
        }
        _ => {
            let mut program = compile_program(expr, input, cache).ok();
            if program
                .as_deref()
                .and_then(ExprProgram::as_coalesce_column_const_cmp)
                .is_none()
                && mentions_ifnull_or_coalesce(expr)
            {
                // Ordinary table-expression compilation may retain a mixed
                // numeric COALESCE/IFNULL as one legacy node to preserve its
                // static result coercion. For an exact numeric comparison the
                // dynamic compiled tree is equivalent and exposes the column
                // shape to the bitmap kernel. Keep this narrowly gated by the
                // bound input type; every other conditional stays on the
                // original row oracle.
                if let Ok(candidate) = ExprCompiler::compile_dynamic_conditional(expr, input) {
                    if candidate
                        .as_coalesce_column_const_cmp()
                        .is_some_and(|(column, ..)| {
                            matches!(
                                input.column_type(column),
                                Some(
                                    ColumnType::Tiny
                                        | ColumnType::Short
                                        | ColumnType::MediumInt
                                        | ColumnType::Long
                                        | ColumnType::LongLong
                                        | ColumnType::Decimal { .. }
                                )
                            )
                        })
                    {
                        program = Some(Arc::new(candidate));
                    }
                }
            }
            match program {
                Some(program) if leaf_is_safe_to_split(&program) => {
                    BatchPredicateNode::leaf(program)
                }
                Some(_) | None => return Ok(None),
            }
        }
    };
    Ok(Some(node))
}

/// The COALESCE/IFNULL comparison kernel can only be lowered from a source
/// call to one of those functions. Checking the AST first keeps every other
/// leaf from paying a second, uncached compile on each execution.
fn mentions_ifnull_or_coalesce(expr: &Expr) -> bool {
    crate::parser::visit::any_expr(expr, |node| {
        matches!(
            node,
            Expr::Function { name, .. }
                if name.eq_ignore_ascii_case("IFNULL") || name.eq_ignore_ascii_case("COALESCE")
        )
    })
}

fn flip_comparison(op: BinaryOp) -> Option<BinaryOp> {
    Some(match op {
        BinaryOp::Eq => BinaryOp::Eq,
        BinaryOp::NotEq => BinaryOp::NotEq,
        BinaryOp::Lt => BinaryOp::Gt,
        BinaryOp::LtEq => BinaryOp::GtEq,
        BinaryOp::Gt => BinaryOp::Lt,
        BinaryOp::GtEq => BinaryOp::LtEq,
        _ => return None,
    })
}

/// Recognize the general MySQL numeric-date predicate
/// `DATE(timestamp_column) + integer_column <cmp> statement_constant`.
/// The constant is accepted only after the normal expression compiler has
/// folded it to a temporal value, so STR_TO_DATE/typed DATE literals share one
/// conversion path. Everything dynamic or warning-prone stays on the oracle.
fn compile_timestamp_date_plus_integer_cmp(
    expr: &Expr,
    input: &InputSchema,
    cache: bool,
) -> Option<BatchPredicateNode> {
    if matches!(
        crate::engine::executor::vec::config::effective_mode(),
        crate::engine::executor::vec::config::VecMode::Off
    ) {
        return None;
    }
    let Expr::Binary { left, op, right } = expr else {
        return None;
    };
    let (arithmetic, constant, op) = if matches!(
        left.as_ref(),
        Expr::Binary {
            op: BinaryOp::Add,
            ..
        }
    ) {
        (left.as_ref(), right.as_ref(), *op)
    } else if matches!(
        right.as_ref(),
        Expr::Binary {
            op: BinaryOp::Add,
            ..
        }
    ) {
        (right.as_ref(), left.as_ref(), flip_comparison(*op)?)
    } else {
        return None;
    };
    let Expr::Binary {
        left: add_left,
        op: BinaryOp::Add,
        right: add_right,
    } = arithmetic
    else {
        return None;
    };
    let date_column = |candidate: &Expr| {
        let Expr::Function {
            name,
            args,
            distinct: false,
        } = candidate
        else {
            return None;
        };
        if !name.eq_ignore_ascii_case("DATE") {
            return None;
        }
        let [column] = args.as_slice() else {
            return None;
        };
        resolve_batch_column(column, input)
    };
    let (timestamp_column, integer_column) = if let Some(timestamp) = date_column(add_left) {
        (timestamp, resolve_batch_column(add_right, input)?)
    } else {
        (
            date_column(add_right)?,
            resolve_batch_column(add_left, input)?,
        )
    };
    if input.column_type(timestamp_column) != Some(ColumnType::Timestamp)
        || !matches!(
            input.column_type(integer_column),
            Some(
                ColumnType::Tiny
                    | ColumnType::Short
                    | ColumnType::MediumInt
                    | ColumnType::Long
                    | ColumnType::LongLong
            )
        )
    {
        return None;
    }
    if !crate::engine::expr::expr_is_constant(constant) {
        return None;
    }
    // Mirror ExprProgram constant folding: capture diagnostics and specialize
    // only a successful, warning-free constant. Invalid STR_TO_DATE values,
    // zero-date policy, parameters and session-dependent expressions retain
    // their runtime oracle behavior.
    let diagnostics = Arc::new(parking_lot::Mutex::new(
        crate::engine::api::SqlWarnings::new(),
    ));
    let constant_value =
        crate::engine::api::with_statement_warning_sink(Arc::clone(&diagnostics), || {
            crate::engine::expr::eval_const(constant)
        })
        .ok()?;
    if !diagnostics.lock().is_empty() {
        return None;
    }
    // Arithmetic is numeric: a string constant must retain numeric-prefix
    // comparison semantics, even if it could also be parsed as a date.
    if !matches!(
        constant_value,
        Value::Date(_) | Value::DateTime(_) | Value::Timestamp(_)
    ) {
        return None;
    }
    let bound = crate::types::temporal_to_mysql_number(&constant_value)?.to_i64();
    let oracle = compile_program(expr, input, cache).ok().or_else(|| {
        ExprCompiler::compile_legacy_oracle(expr, input)
            .ok()
            .map(Arc::new)
    })?;
    Some(BatchPredicateNode::TimestampDatePlusIntegerCmp {
        timestamp_column,
        integer_column,
        op,
        bound,
        oracle,
    })
}

fn compile_program(
    expr: &Expr,
    input: &InputSchema,
    cache: bool,
) -> Result<Arc<ExprProgram>, crate::engine::expr_program::CompileError> {
    if cache {
        ExprCompiler::compile_cached(expr, input)
    } else {
        ExprCompiler::compile(expr, input).map(Arc::new)
    }
}

fn leaf_is_safe_to_split(program: &ExprProgram) -> bool {
    if program.as_column_const_cmp_with_collation().is_some()
        || program.as_coalesce_column_const_cmp().is_some()
        || program.as_column_like_const().is_some()
        || program.as_column_like_param().is_some()
    {
        return true;
    }
    let capability = program.capability();
    !capability.requires_ordered_evaluation()
}

fn eval_batch_predicate_node(
    node: &BatchPredicateNode,
    batch: &Batch,
    native_rows: &std::sync::atomic::AtomicU64,
    fallback_rows: &std::sync::atomic::AtomicU64,
) -> Result<TruthMask, QueryError> {
    match node {
        BatchPredicateNode::TimestampDatePlusIntegerCmp {
            timestamp_column,
            integer_column,
            op,
            bound,
            oracle,
        } => {
            if let Some(mask) = try_timestamp_date_plus_integer_cmp_truth_mask(
                batch,
                *timestamp_column,
                *integer_column,
                *op,
                *bound,
            ) {
                native_rows.fetch_add(
                    batch.visible_rows() as u64,
                    std::sync::atomic::Ordering::Relaxed,
                );
                return Ok(mask);
            }
            eval_fallback_truth_mask(oracle, batch, fallback_rows)
        }
        BatchPredicateNode::Leaf { program, kernel } => {
            if let Some(mask) = try_native_truth_mask(*kernel, program, batch) {
                native_rows.fetch_add(
                    batch.visible_rows() as u64,
                    std::sync::atomic::Ordering::Relaxed,
                );
                return Ok(mask);
            }
            eval_fallback_truth_mask(program, batch, fallback_rows)
        }
        BatchPredicateNode::Not(expr) => {
            let value = eval_batch_predicate_node(expr, batch, native_rows, fallback_rows)?;
            let truth = restrict_bitmap_to_selection(batch, value.truth.or(&value.null).not());
            Ok(TruthMask {
                truth,
                null: value.null,
            })
        }
        BatchPredicateNode::And(left, right) | BatchPredicateNode::Or(left, right) => {
            let left = eval_batch_predicate_node(left, batch, native_rows, fallback_rows)?;
            let is_and = matches!(node, BatchPredicateNode::And(_, _));
            // SQL short circuit: AND evaluates its right side only for TRUE or
            // UNKNOWN left rows; OR only for FALSE or UNKNOWN left rows.
            // Carrying that subset as the child batch selection preserves both
            // volatile invocation counts and suppression of errors in rows that
            // have already decided the result.
            let right_rows = if is_and {
                left.truth.or(&left.null)
            } else {
                restrict_bitmap_to_selection(batch, left.truth.not())
            };
            let right_batch = batch.clone().with_selection(Selection::Mask(right_rows));
            let right = eval_batch_predicate_node(right, &right_batch, native_rows, fallback_rows)?;
            let truth = restrict_bitmap_to_selection(
                batch,
                if is_and {
                    left.truth.and(&right.truth)
                } else {
                    left.truth.or(&right.truth)
                },
            );
            let left_false = restrict_bitmap_to_selection(batch, left.truth.or(&left.null).not());
            let right_false =
                restrict_bitmap_to_selection(batch, right.truth.or(&right.null).not());
            let false_bits = restrict_bitmap_to_selection(
                batch,
                if is_and {
                    left_false.or(&right_false)
                } else {
                    left_false.and(&right_false)
                },
            );
            let null = restrict_bitmap_to_selection(batch, truth.or(&false_bits).not());
            Ok(TruthMask { truth, null })
        }
    }
}

fn eval_fallback_truth_mask(
    program: &ExprProgram,
    batch: &Batch,
    fallback_rows: &std::sync::atomic::AtomicU64,
) -> Result<TruthMask, QueryError> {
    fallback_rows.fetch_add(
        batch.visible_rows() as u64,
        std::sync::atomic::Ordering::Relaxed,
    );
    let mut truth = Bitmap::new_all_invalid(batch.num_rows());
    let mut null = Bitmap::new_all_valid(batch.num_rows());
    // Numeric predicates need no result array. String truthiness can warn,
    // however, and must run only after successful evaluation of all rows.
    let mut deferred: Option<Vec<Value>> = None;
    let mut emit = |row, value| {
        null.set(row, matches!(value, Value::Null));
        if crate::engine::expr::truthiness_may_warn(&value) {
            deferred.get_or_insert_with(|| vec![Value::Null; batch.num_rows()])[row] = value;
        } else {
            truth.set(row, crate::engine::expr::truthy(&value));
            // Repeated selection indices keep only the final value, including
            // replacing a deferred string with a numeric value or NULL.
            if let Some(values) = &mut deferred {
                values[row] = Value::Null;
            }
        }
    };
    if let Some(values) = try_temporal_value_rows(program, batch) {
        for (row, value) in values.into_iter().enumerate() {
            emit(row, value);
        }
    } else {
        visit_scalar_rows(program, batch, emit)?;
    }
    if let Some(values) = deferred {
        for (row, value) in values.iter().enumerate() {
            if !matches!(value, Value::Null) {
                truth.set(row, crate::engine::expr::truthy(value));
            }
        }
    }
    // Invisible rows previously occupied NULL slots in the materialized Vec.
    // Numeric results keep that alignment without allocating Values per row.
    Ok(TruthMask { truth, null })
}

fn try_timestamp_date_plus_integer_cmp_truth_mask(
    batch: &Batch,
    timestamp_column: usize,
    integer_column: usize,
    op: BinaryOp,
    bound: i64,
) -> Option<TruthMask> {
    let ColumnVector::Timestamp64 {
        data: timestamps,
        nulls: timestamp_nulls,
    } = batch.columns().get(timestamp_column)?
    else {
        return None;
    };
    let integer = batch.columns().get(integer_column)?;
    if timestamps.len() != batch.num_rows() || integer.len() != batch.num_rows() {
        return None;
    }
    let mut date_numbers = vec![0i64; batch.num_rows()];
    crate::engine::expr::fill_session_timestamp_date_numbers(
        &mut date_numbers,
        timestamps,
        batch.visible_indices().filter(|&row| {
            !timestamp_nulls
                .as_ref()
                .is_some_and(|valid| !valid.get(row))
                && !integer.is_null(row)
        }),
    )?;
    let mut truth = Bitmap::new_all_invalid(batch.num_rows());
    let mut null = Bitmap::new_all_invalid(batch.num_rows());
    for row in batch.visible_indices() {
        if timestamp_nulls
            .as_ref()
            .is_some_and(|valid| !valid.get(row))
            || integer.is_null(row)
        {
            null.set_true(row);
            continue;
        }
        let duration = match integer {
            ColumnVector::I64 { data, .. } => data[row],
            ColumnVector::U64 { data, .. } => i64::try_from(data[row]).ok()?,
            _ => return None,
        };
        let value = date_numbers[row].checked_add(duration)?;
        if ordered_cmp_matches(op, &value, &bound)? {
            truth.set_true(row);
        }
    }
    Some(TruthMask { truth, null })
}

fn try_native_truth_mask(
    kernel: PredicateKernel,
    program: &ExprProgram,
    batch: &Batch,
) -> Option<TruthMask> {
    if matches!(
        crate::engine::executor::vec::config::effective_mode(),
        crate::engine::executor::vec::config::VecMode::Off
    ) {
        return None;
    }
    match kernel {
        PredicateKernel::SubstringIn => {
            try_substring_column_string_in_const_set_truth_mask(program, batch)
        }
        PredicateKernel::StringIn => try_column_string_in_const_set_truth_mask(program, batch),
        PredicateKernel::IntegerIn => try_column_integer_in_const_set_truth_mask(program, batch),
        PredicateKernel::Coalesce => try_coalesce_column_const_cmp_truth_mask(program, batch),
        PredicateKernel::ColumnConstant => {
            let (column, op, value, collation_id) = column_const_cmp_for_batch(program, batch)?;
            let truth =
                try_column_const_cmp_bitmap_parts(column, op, value.as_ref(), collation_id, batch)?;
            let null = if matches!(value.as_ref(), Value::Null) {
                Bitmap::new_all_valid(batch.num_rows())
            } else {
                column_null_bitmap(batch.columns().get(column)?, batch.num_rows())
            };
            Some(TruthMask {
                truth,
                null: restrict_bitmap_to_selection(batch, null),
            })
        }
        PredicateKernel::ColumnColumn => {
            let truth = try_column_column_cmp_bitmap(program, batch)?;
            let (left, _, right, _) = program.as_column_column_cmp()?;
            let null = column_null_bitmap(batch.columns().get(left)?, batch.num_rows()).or(
                &column_null_bitmap(batch.columns().get(right)?, batch.num_rows()),
            );
            Some(TruthMask {
                truth,
                null: restrict_bitmap_to_selection(batch, null),
            })
        }
        PredicateKernel::Like => {
            let (column, truth) = try_column_like_bitmap(program, batch)?;
            let null = column_null_bitmap(batch.columns().get(column)?, batch.num_rows());
            Some(TruthMask {
                truth,
                null: restrict_bitmap_to_selection(batch, null),
            })
        }
        PredicateKernel::Scalar => {
            if !matches!(
                program.result_type().kind,
                crate::engine::expr_type::ExprTypeKind::Integer { .. }
            ) {
                return None;
            }
            let ColumnVector::I64 { data, nulls } = program.try_eval_column(batch)? else {
                return None;
            };
            let mut truth = Bitmap::new_all_invalid(data.len());
            let mut null = Bitmap::new_all_invalid(data.len());
            for (row, value) in data.into_iter().enumerate() {
                if nulls.as_ref().is_some_and(|valid| !valid.get(row)) {
                    null.set_true(row);
                } else if value != 0 {
                    truth.set_true(row);
                }
            }
            Some(TruthMask { truth, null })
        }
    }
}

/// Evaluate a character-counted `SUBSTRING(utf8_column, const[, const]) IN
/// (const strings...)` without materialising either a row `Value` or the
/// substring.  `mysql_substring_slice` is shared with the scalar implementation
/// so multibyte, negative-position, zero-length and out-of-range behavior stay
/// identical.  Equality probes the compiler's canonical `ValueKey` set, which
/// preserves the existing default collation and PAD SPACE semantics.
fn try_substring_column_string_in_const_set_truth_mask(
    program: &ExprProgram,
    batch: &Batch,
) -> Option<TruthMask> {
    let (column, substring_start, substring_len, keys, has_null, negated) =
        program.as_substring_column_string_in_const_set()?;
    let ColumnVector::Utf8 {
        offsets,
        data,
        nulls,
    } = batch.columns().get(column)?
    else {
        return None;
    };
    let rows = batch.num_rows();
    if offsets.len() != rows + 1 {
        return None;
    }
    let mut truth = Bitmap::new_all_invalid(rows);
    let mut null = Bitmap::new_all_invalid(rows);
    for row in batch.visible_indices() {
        if nulls.as_ref().is_some_and(|valid| !valid.get(row)) {
            null.set_true(row);
            continue;
        }
        let start = offsets[row] as usize;
        let end = offsets[row + 1] as usize;
        if start > end || end > data.len() {
            return None;
        }
        let input = std::str::from_utf8(&data[start..end]).ok()?;
        let substring =
            crate::engine::expr::mysql_substring_slice(input, substring_start, substring_len);
        let found = keys.contains(&ValueKey::string_from_str(substring));
        if found {
            if !negated {
                truth.set_true(row);
            }
        } else if has_null {
            null.set_true(row);
        } else if negated {
            truth.set_true(row);
        }
    }
    Some(TruthMask { truth, null })
}

/// Evaluate an exact integral `[NOT] IN` set directly on the fixed-width
/// column lane. The compiled set uses the same canonical `ValueKey::Int`
/// representation as the scalar oracle. Respecting the incoming selection is
/// important for short-circuited `AND`: later membership leaves touch only
/// rows that survived the earlier, more selective domain.
fn try_column_integer_in_const_set_truth_mask(
    program: &ExprProgram,
    batch: &Batch,
) -> Option<TruthMask> {
    let (column, keys, has_null, negated) = program.as_column_integer_in_const_set()?;
    match batch.columns().get(column)? {
        ColumnVector::I64 { data, nulls } => integer_in_const_set_truth_mask(
            batch,
            data.len(),
            nulls.as_ref(),
            keys,
            has_null,
            negated,
            |row| i128::from(data[row]),
        ),
        ColumnVector::U64 { data, nulls } => integer_in_const_set_truth_mask(
            batch,
            data.len(),
            nulls.as_ref(),
            keys,
            has_null,
            negated,
            |row| i128::from(data[row]),
        ),
        _ => return None,
    }
}

fn integer_in_const_set_truth_mask(
    batch: &Batch,
    len: usize,
    nulls: Option<&Bitmap>,
    keys: &std::collections::HashSet<ValueKey>,
    has_null: bool,
    negated: bool,
    value_at: impl Fn(usize) -> i128,
) -> Option<TruthMask> {
    if len != batch.num_rows() {
        return None;
    }
    let mut truth = Bitmap::new_all_invalid(len);
    let mut null = Bitmap::new_all_invalid(len);
    for row in batch.visible_indices() {
        if nulls.is_some_and(|valid| !valid.get(row)) {
            null.set_true(row);
            continue;
        }
        let found = keys.contains(&ValueKey::Int(value_at(row)));
        if found {
            if !negated {
                truth.set_true(row);
            }
        } else if has_null {
            null.set_true(row);
        } else if negated {
            truth.set_true(row);
        }
    }
    Some(TruthMask { truth, null })
}

/// Evaluate `VARCHAR [NOT] IN (<constant strings...>)` directly over the
/// UTF-8 lane. Membership probes the very same precomputed `ValueKey` set as
/// `ExprProgram::eval_row`; this preserves default collation and PAD SPACE
/// equality without constructing a complete row or cloning the column value.
/// SQL three-valued logic is retained independently in `truth` and `null`:
/// an input NULL is UNKNOWN, and a non-match is UNKNOWN when the list contains
/// NULL (including `NOT IN`).
fn try_column_string_in_const_set_truth_mask(
    program: &ExprProgram,
    batch: &Batch,
) -> Option<TruthMask> {
    let (column, keys, has_null, negated) = program.as_column_string_in_const_set()?;
    let ColumnVector::Utf8 {
        offsets,
        data,
        nulls,
    } = batch.columns().get(column)?
    else {
        return None;
    };
    let rows = batch.num_rows();
    if offsets.len() != rows + 1 {
        return None;
    }
    let mut truth = Bitmap::new_all_invalid(rows);
    let mut null = Bitmap::new_all_invalid(rows);
    for row in 0..rows {
        if nulls.as_ref().is_some_and(|valid| !valid.get(row)) {
            null.set_true(row);
            continue;
        }
        let start = offsets[row] as usize;
        let end = offsets[row + 1] as usize;
        if start > end || end > data.len() {
            return None;
        }
        let value = std::str::from_utf8(&data[start..end]).ok()?;
        let found = keys.contains(&ValueKey::string_from_str(value));
        if found {
            if !negated {
                truth.set_true(row);
            }
        } else if has_null {
            null.set_true(row);
        } else if negated {
            truth.set_true(row);
        }
    }
    Some(TruthMask {
        truth: restrict_bitmap_to_selection(batch, truth),
        null: restrict_bitmap_to_selection(batch, null),
    })
}

/// Return a column/constant shape accepted by the concrete batch lane. The
/// normal ExprProgram accessor excludes every dynamic string operand because
/// generic string/numeric coercion is runtime-dependent. Concrete temporal
/// and UTF-8 lanes are narrower: a runtime string can be compared with the
/// lane's exact temporal or collation-aware string semantics, while every
/// other type combination retains the scalar oracle.
fn column_const_cmp_for_batch<'a>(
    program: &'a ExprProgram,
    batch: &Batch,
) -> Option<(usize, BinaryOp, Cow<'a, Value>, Option<u16>)> {
    if let Some((column, op, value, collation_id, needs_runtime_context)) =
        program.as_column_const_cmp_shape()
    {
        if !needs_runtime_context
            || dynamic_cmp_value_matches_lane(value, batch.columns().get(column)?)
        {
            return Some((column, op, Cow::Borrowed(value), collation_id));
        }
    }
    let (column, op, param, collation_id, _) = program.as_column_param_cmp_shape()?;
    let value = crate::engine::params::current_param(param)?;
    dynamic_cmp_value_matches_lane(&value, batch.columns().get(column)?).then_some((
        column,
        op,
        Cow::Owned(value),
        collation_id,
    ))
}

fn dynamic_cmp_value_matches_lane(value: &Value, column: &ColumnVector) -> bool {
    let string = matches!(
        value,
        Value::String(_) | Value::SmallString(_) | Value::Bytes(_)
    );
    match column {
        ColumnVector::I64 { .. } | ColumnVector::U64 { .. } => {
            matches!(value, Value::Int(_) | Value::UInt(_))
        }
        ColumnVector::Decimal32 { .. }
        | ColumnVector::Decimal64 { .. }
        | ColumnVector::Decimal128 { .. } => exact_decimal_const_parts(value).is_some(),
        ColumnVector::Date32 { .. } => string || matches!(value, Value::Date(_)),
        ColumnVector::DateTime64 { .. } => string || matches!(value, Value::DateTime(_)),
        ColumnVector::Timestamp64 { .. } => {
            crate::engine::expr::coerce_comparison_constant_for_type(
                value,
                &crate::storage::DataTypeKind::Timestamp { fsp: 6 },
            )
            .is_some()
        }
        ColumnVector::Time64 { .. } => string || matches!(value, Value::Time(_)),
        ColumnVector::Utf8 { .. } | ColumnVector::Bytes { .. } | ColumnVector::Generic { .. } => {
            string
        }
        _ => false,
    }
}

fn column_null_bitmap(column: &ColumnVector, rows: usize) -> Bitmap {
    match column {
        ColumnVector::Null { .. } => Bitmap::new_all_valid(rows),
        ColumnVector::Generic { values } => {
            let mut null = Bitmap::new_all_invalid(rows);
            for (row, value) in values.iter().take(rows).enumerate() {
                if matches!(value, Value::Null) {
                    null.set_true(row);
                }
            }
            null
        }
        _ => column
            .nulls()
            .map(Bitmap::not)
            .unwrap_or_else(|| Bitmap::new_all_invalid(rows)),
    }
}

/// Evaluate the common nullable-measure predicate
/// `COALESCE(column, const) <cmp> const` (and its `IFNULL` spelling) without
/// constructing a `Value` per row. This is deliberately limited to exact
/// numeric lanes and constants: the existing column/constant kernel already
/// proves those comparisons warning-free, while string/temporal conditional
/// coercions remain on the scalar semantics oracle.
fn try_coalesce_column_const_cmp_truth_mask(
    program: &ExprProgram,
    batch: &Batch,
) -> Option<TruthMask> {
    let (column, op, fallback, bound, collation_id) = program.as_coalesce_column_const_cmp()?;
    if collation_id.is_some() {
        return None;
    }
    let lane = batch.columns().get(column)?;
    if !matches!(
        lane,
        ColumnVector::I64 { .. }
            | ColumnVector::U64 { .. }
            | ColumnVector::Decimal32 { .. }
            | ColumnVector::Decimal64 { .. }
            | ColumnVector::Decimal128 { .. }
    ) {
        return None;
    }
    if matches!(bound, Value::Null) {
        return Some(TruthMask {
            truth: Bitmap::new_all_invalid(batch.num_rows()),
            null: restrict_bitmap_to_selection(batch, Bitmap::new_all_valid(batch.num_rows())),
        });
    }
    let (bound_value, bound_scale) = exact_decimal_const_parts(bound)?;
    let mut truth = try_column_const_cmp_bitmap_parts(column, op, bound, None, batch)?;
    let source_null = column_null_bitmap(lane, batch.num_rows());
    let null = if matches!(fallback, Value::Null) {
        source_null
    } else {
        let (fallback_value, fallback_scale) = exact_decimal_const_parts(fallback)?;
        if decimal_order_matches(op, fallback_value, fallback_scale, bound_value, bound_scale)? {
            truth = truth.or(&source_null);
        }
        Bitmap::new_all_invalid(batch.num_rows())
    };
    Some(TruthMask {
        truth: restrict_bitmap_to_selection(batch, truth),
        null: restrict_bitmap_to_selection(batch, null),
    })
}

/// Compile an expression against a batch schema, returning a program that
/// can be reused across batches. `Result::Ok(None)` means the expression
/// shape isn't supported by the compiler and callers must fall back to
/// `eval_value_per_row` below.
pub fn compile_for_batch(
    expr: &Expr,
    schema: &BatchSchema,
) -> Result<Option<Arc<ExprProgram>>, QueryError> {
    let input = batch_schema_input(schema);
    match ExprCompiler::compile_cached(expr, &input) {
        Ok(program) => Ok(Some(program)),
        Err(_) => match ExprCompiler::compile_legacy_oracle(expr, &input) {
            Ok(program) => Ok(Some(Arc::new(program))),
            Err(_) => Ok(None),
        },
    }
}

/// Compile one whole scalar expression through the legacy semantic oracle,
/// but bind its columns to the current BatchSchema. The returned program is
/// still evaluated row-at-a-time inside the current batch; callers use this
/// when partial lowering would change dynamic coercion or warning order.
pub fn compile_legacy_for_batch(
    expr: &Expr,
    schema: &BatchSchema,
) -> Result<Option<Arc<ExprProgram>>, QueryError> {
    let input = batch_schema_input(schema);
    match ExprCompiler::compile_legacy_oracle(expr, &input) {
        Ok(program) => Ok(Some(Arc::new(program))),
        Err(_) => Ok(None),
    }
}

/// Compile a post-aggregate scalar expression with the same dynamic
/// conditional coercion used by the PlanRow aggregate oracle.
pub fn compile_post_aggregate(
    expr: &Expr,
    schema: &BatchSchema,
) -> Result<Option<Arc<ExprProgram>>, QueryError> {
    let input = batch_schema_input(schema);
    match ExprCompiler::compile_dynamic_conditional(expr, &input) {
        Ok(program) => Ok(Some(Arc::new(program))),
        Err(_) => Ok(None),
    }
}

/// Evaluate `program` against the visible rows of `batch` while keeping the
/// returned vector physically aligned with the input columns. Invisible rows
/// are represented by `Value::Null` and, critically, are never evaluated.
///
/// Physical alignment lets downstream operators keep indexing the result with
/// selection-vector row ids. Skipping invisible rows preserves the row
/// executor's evaluation count/order for volatile expressions and prevents an
/// error in an already-filtered row from leaking into the query.
pub fn eval_value_per_row(program: &ExprProgram, batch: &Batch) -> Result<Vec<Value>, QueryError> {
    if !matches!(super::config::effective_mode(), super::config::VecMode::Off) {
        if let Some(column) = program.try_eval_column(batch) {
            return Ok((0..batch.num_rows())
                .map(|row| column.value_at(row))
                .collect());
        }
    }
    if let Some(values) = try_temporal_value_rows(program, batch) {
        return Ok(values);
    }
    let mut values = vec![Value::Null; batch.num_rows()];
    visit_scalar_rows(program, batch, |row, value| values[row] = value)?;
    Ok(values)
}

/// Preserve the existing temporal projection fast path without allocating
/// an additional output vector in the caller.
fn try_temporal_value_rows(program: &ExprProgram, batch: &Batch) -> Option<Vec<Value>> {
    if batch.selection().is_some()
        || matches!(super::config::effective_mode(), super::config::VecMode::Off)
    {
        return None;
    }
    let (kind, column) = program.as_temporal_extract()?;
    try_temporal_extract_typed(kind, batch, column)
}

/// Stream visible rows to a projection or predicate, retaining selection and
/// diagnostic order. Numeric predicates avoid a materialized result Vec.
fn visit_scalar_rows(
    program: &ExprProgram,
    batch: &Batch,
    mut emit: impl FnMut(usize, Value),
) -> Result<(), QueryError> {
    // Reuse a single per-row scratch `Vec<Value>` across the batch instead
    // of allocating a fresh one per row: one allocation for the whole batch
    // rather than `num_rows()` of them. The cells are still cloned out of the
    // column lanes (a `RowView` needs owned `Value`s), but the Vec itself is
    // cleared and refilled in place.
    let ncols = batch.columns().len();
    let referenced_columns = program.referenced_columns();
    let mut row_values = vec![Value::Null; ncols];
    let mut eval_row = |r: usize| -> Result<(), QueryError> {
        for &column in referenced_columns {
            let Some(vector) = batch.columns().get(column) else {
                return Err(crate::error::catalog::ER_BAD_FIELD_ERROR
                    .error("row value is missing for compiled expression"));
            };
            row_values[column] =
                vector.typed_value_at(r, batch.schema().fields()[column].column_type);
        }
        let view = RowView::from_values(&row_values);
        let v = with_eval_scratch(|s| program.eval_row(&view, s))?;
        emit(r, v);
        Ok(())
    };
    match batch.selection() {
        Some(selection) => {
            for r in selection.iter() {
                eval_row(r)?;
            }
        }
        None => {
            for r in 0..batch.num_rows() {
                eval_row(r)?;
            }
        }
    }
    Ok(())
}

fn restrict_bitmap_to_selection(batch: &Batch, bits: Bitmap) -> Bitmap {
    let Some(selection) = batch.selection() else {
        return bits;
    };
    match selection {
        // Boolean expression trees invoke this helper several times per
        // node. Preserve the selection representation so a bit-packed mask
        // stays a word-wise AND instead of being scanned and rebuilt one bit
        // at a time for every invocation.
        Selection::Mask(visible) => bits.and(visible),
        Selection::Range { start, len } => {
            let mut bits = bits;
            bits.retain_range(*start as usize, *len as usize);
            bits
        }
        Selection::Indices(indices) => {
            let mut visible = Bitmap::new_all_invalid(batch.num_rows());
            for &row in indices {
                if (row as usize) < batch.num_rows() {
                    visible.set_true(row as usize);
                }
            }
            bits.and(&visible)
        }
    }
}

/// Run a temporal extraction directly on a typed column lane. Returns
/// `None` when the column isn't a temporal lane (caller falls back to
/// the row-by-row path). Zero-date sentinels extract 0, matching the
/// legacy `extract_datetime_part` semantics; NULL slots stay NULL.
fn try_temporal_extract_typed(
    kind: TemporalExtractKind,
    batch: &Batch,
    col: usize,
) -> Option<Vec<Value>> {
    try_temporal_extract_column(kind, batch, col)
        .map(|column| (0..column.len()).map(|row| column.value_at(row)).collect())
}

pub(crate) fn try_temporal_extract_column(
    kind: TemporalExtractKind,
    batch: &Batch,
    col: usize,
) -> Option<ColumnVector> {
    fn finish(data: Vec<i64>, nulls: &Option<Bitmap>) -> ColumnVector {
        if data.is_empty() || nulls.as_ref().is_some_and(|valid| valid.count_ones() == 0) {
            return ColumnVector::Null { len: data.len() };
        }
        let nulls = nulls
            .as_ref()
            .filter(|valid| valid.count_ones() != data.len())
            .cloned();
        ColumnVector::I64 { data, nulls }
    }

    #[inline]
    fn extract_from_days(kind: TemporalExtractKind, days: i32) -> i64 {
        if matches!(kind, TemporalExtractKind::Year) {
            return crate::types::decode_tagged_date(days)
                .map(|(year, _, _)| i64::from(year))
                .unwrap_or_else(|| i64::from(crate::types::civil_year_from_days(days)));
        }
        let (y, m, d) = crate::types::decode_tagged_date(days)
            .map(|(y, m, d)| (y as i32, m, d))
            .unwrap_or_else(|| crate::types::civil_from_days(days));
        match kind {
            TemporalExtractKind::Year => y as i64,
            TemporalExtractKind::Month => m as i64,
            TemporalExtractKind::Day => d as i64,
            TemporalExtractKind::Quarter => (if m == 0 { 0 } else { (m - 1) / 3 + 1 }) as i64,
            // Time-of-day parts of a DATE are zero.
            TemporalExtractKind::Hour
            | TemporalExtractKind::Minute
            | TemporalExtractKind::Second => 0,
        }
    }

    #[inline]
    fn extract_from_micros(kind: TemporalExtractKind, micros: i64) -> i64 {
        if micros == crate::types::ZERO_DATETIME {
            return 0;
        }
        if let Some((y, m, d, h, mi, se, _)) =
            crate::types::decode_tagged_datetime(micros.to_owned())
        {
            return match kind {
                TemporalExtractKind::Year => y as i64,
                TemporalExtractKind::Month => m as i64,
                TemporalExtractKind::Day => d as i64,
                TemporalExtractKind::Quarter => {
                    if m == 0 {
                        0
                    } else {
                        ((m - 1) / 3 + 1) as i64
                    }
                }
                TemporalExtractKind::Hour => h as i64,
                TemporalExtractKind::Minute => mi as i64,
                TemporalExtractKind::Second => se as i64,
            };
        }
        let days = micros.div_euclid(86_400_000_000) as i32;
        let v = match kind {
            TemporalExtractKind::Year
            | TemporalExtractKind::Month
            | TemporalExtractKind::Day
            | TemporalExtractKind::Quarter => extract_from_days(kind, days),
            TemporalExtractKind::Hour
            | TemporalExtractKind::Minute
            | TemporalExtractKind::Second => {
                let secs_of_day = micros.rem_euclid(86_400_000_000) / 1_000_000;
                match kind {
                    TemporalExtractKind::Hour => secs_of_day / 3600,
                    TemporalExtractKind::Minute => (secs_of_day / 60) % 60,
                    TemporalExtractKind::Second => secs_of_day % 60,
                    _ => unreachable!(),
                }
            }
        };
        v
    }

    match batch.columns().get(col)? {
        ColumnVector::Date32 { data, nulls } => {
            let mut out = Vec::with_capacity(data.len());
            for (i, days) in data.iter().enumerate() {
                if nulls.as_ref().is_some_and(|n| !n.get(i)) {
                    out.push(0);
                } else if *days == crate::types::ZERO_DATE {
                    out.push(0);
                } else {
                    out.push(extract_from_days(kind, *days));
                }
            }
            Some(finish(out, nulls))
        }
        ColumnVector::DateTime64 { data, nulls } => {
            let mut out = Vec::with_capacity(data.len());
            for (i, micros) in data.iter().enumerate() {
                if nulls.as_ref().is_some_and(|n| !n.get(i)) {
                    out.push(0);
                    continue;
                }
                out.push(extract_from_micros(kind, *micros));
            }
            Some(finish(out, nulls))
        }
        ColumnVector::Timestamp64 { data, nulls } => {
            let mut local = vec![0i64; data.len()];
            crate::engine::expr::fill_session_timestamp_local_datetime_micros(
                &mut local,
                data,
                (0..data.len()).filter(|&row| nulls.as_ref().is_none_or(|valid| valid.get(row))),
            )?;
            let mut out = Vec::with_capacity(data.len());
            for (i, micros) in local.into_iter().enumerate() {
                if nulls.as_ref().is_some_and(|n| !n.get(i)) {
                    out.push(0);
                    continue;
                }
                out.push(extract_from_micros(kind, micros));
            }
            Some(finish(out, nulls))
        }
        _ => None,
    }
}

/// Return a typed column for expression shapes whose batch kernel is both
/// deterministic and infallible on its accepted physical lane. Callers that
/// must preserve row-major error precedence may safely pre-compute these
/// expressions: `None` is only a routing decision and never a SQL error.
pub(crate) fn try_eval_infallible_typed_column(
    program: &ExprProgram,
    batch: &Batch,
) -> Option<ColumnVector> {
    if matches!(
        crate::engine::executor::vec::config::effective_mode(),
        crate::engine::executor::vec::config::VecMode::Off
    ) {
        return None;
    }
    if let Some((kind, index)) = program.as_temporal_extract() {
        let column = try_temporal_extract_column(kind, batch, index)?;
        crate::engine::expr_program::record_column_eval_rows(batch.visible_rows() as u64);
        return Some(column);
    }
    if program.column_parallel_safe(batch.schema()) {
        return program.try_eval_column(batch);
    }
    None
}

#[cfg(test)]
/// Evaluate a boolean predicate over a batch and return a row bitmap.
/// NULL is treated as falsy, mirroring the planner's `truthy` rule.
pub fn eval_predicate_bitmap(program: &ExprProgram, batch: &Batch) -> Result<Bitmap, QueryError> {
    // Typed fast path (P6-3 / C2): `col <cmp> const` produces the row bitmap
    // straight from the I64 / F64 lane via the SIMD const-compare kernels,
    // skipping per-row `Value` materialisation + `truthy`. Only fires when
    // the lane type and constant type line up exactly; every other shape
    // falls through to the row-by-row oracle below.
    if let Some(bits) = try_column_const_cmp_bitmap(program, batch) {
        return Ok(restrict_bitmap_to_selection(batch, bits));
    }
    if let Some(bits) = try_column_column_cmp_bitmap(program, batch) {
        return Ok(restrict_bitmap_to_selection(batch, bits));
    }
    // B14 (vec LIKE column kernel): `Column(idx) [NOT] LIKE <const-pattern>`
    // straight from a Utf8 lane into the row bitmap. Skips `Value::String`
    // materialisation (a heap allocation + memcpy per row inside
    // `ColumnVector::value_at`) and the per-row `ExprProgram::eval_row`
    // dispatch. `LikePlan::matches_str` is the same kernel the row-by-row
    // path calls, so `MYDB_VECTORIZED=off` remains a bit-exact oracle.
    if let Some((_, bits)) = try_column_like_bitmap(program, batch) {
        return Ok(restrict_bitmap_to_selection(batch, bits));
    }
    let mut bits = Bitmap::new_all_invalid(batch.num_rows());
    let values = eval_value_per_row(program, batch)?;
    for (i, v) in values.iter().enumerate() {
        if crate::engine::expr::truthy(v) {
            bits.set_true(i);
        }
    }
    Ok(restrict_bitmap_to_selection(batch, bits))
}

#[inline]
fn ordered_cmp_matches<T: Ord>(op: BinaryOp, left: &T, right: &T) -> Option<bool> {
    let ordering = left.cmp(right);
    Some(match op {
        BinaryOp::Eq => ordering == std::cmp::Ordering::Equal,
        BinaryOp::NotEq => ordering != std::cmp::Ordering::Equal,
        BinaryOp::Lt => ordering == std::cmp::Ordering::Less,
        BinaryOp::LtEq => ordering != std::cmp::Ordering::Greater,
        BinaryOp::Gt => ordering == std::cmp::Ordering::Greater,
        BinaryOp::GtEq => ordering != std::cmp::Ordering::Less,
        _ => return None,
    })
}

/// Fixed-width `column <cmp> column` lane. This is especially important for
/// fact-table temporal predicates such as `receiptdate > commitdate`: it
/// avoids constructing a complete `Vec<Value>` row and invoking the scalar
/// program millions of times while preserving SQL NULL/ordering semantics.
fn try_column_column_cmp_bitmap(program: &ExprProgram, batch: &Batch) -> Option<Bitmap> {
    let (left, op, right, collation_id) = program.as_column_column_cmp()?;
    if collation_id.is_some() {
        return None;
    }
    let left_col = batch.columns().get(left)?;
    let right_col = batch.columns().get(right)?;
    if left_col.len() != batch.num_rows() || right_col.len() != batch.num_rows() {
        return None;
    }
    let mut out = Bitmap::new_all_invalid(batch.num_rows());
    macro_rules! compare_lane {
        ($left:expr, $right:expr) => {{
            for row in 0..batch.num_rows() {
                if left_col.is_null(row) || right_col.is_null(row) {
                    continue;
                }
                if ordered_cmp_matches(op, &$left[row], &$right[row])? {
                    out.set_true(row);
                }
            }
        }};
    }
    match (left_col, right_col) {
        (
            ColumnVector::I64 {
                data: left_data, ..
            },
            ColumnVector::I64 {
                data: right_data, ..
            },
        )
        | (
            ColumnVector::Time64 {
                data: left_data, ..
            },
            ColumnVector::Time64 {
                data: right_data, ..
            },
        )
        | (
            ColumnVector::DateTime64 {
                data: left_data, ..
            },
            ColumnVector::DateTime64 {
                data: right_data, ..
            },
        )
        | (
            ColumnVector::Timestamp64 {
                data: left_data, ..
            },
            ColumnVector::Timestamp64 {
                data: right_data, ..
            },
        ) => compare_lane!(left_data, right_data),
        (
            ColumnVector::Date32 {
                data: left_data, ..
            },
            ColumnVector::Date32 {
                data: right_data, ..
            },
        ) => compare_lane!(left_data, right_data),
        _ => return None,
    }
    Some(out)
}

/// `col <cmp> const` over an integer lane, straight into a bitmap. Returns
/// `None` (caller falls back to the oracle) unless the lane is a dense I64/U64
/// lane and the constant is an exact `Value::Int/UInt`. The typed kernel sets
/// a bit when the comparison is *true*; NULL rows are then masked out with the
/// lane's validity bitmap, so the result equals `truthy(col <cmp> const)` per
/// row — TRUE only, FALSE and NULL both unset.
///
/// Restricted to integer lanes on purpose: they materialise as `Value::Int`
/// or `Value::UInt` (see `ColumnVector::value_at`), and the mixed signed/
/// unsigned branches below reproduce `compare_values` without a lossy f64
/// conversion. The `F64` lane is **not**
/// fast-pathed here — legacy `compare_values` gives doubles a total order
/// (NaN sorts greatest, `-0.0 < 0.0`) that diverges from the IEEE semantics of
/// the SIMD float kernels, so `NaN > x` / `col = 0.0` against a stored `-0.0`
/// would disagree. The float kernels stay reserved for callers (block-NLJ)
/// that have already accepted IEEE semantics.
#[cfg(test)]
fn try_column_const_cmp_bitmap(program: &ExprProgram, batch: &Batch) -> Option<Bitmap> {
    let (col, op, konst, collation_id) = column_const_cmp_for_batch(program, batch)?;
    try_column_const_cmp_bitmap_parts(col, op, konst.as_ref(), collation_id, batch)
}

#[inline]
fn text_value_str(value: &Value) -> Option<&str> {
    match value {
        Value::Bytes(bytes) => std::str::from_utf8(bytes).ok(),
        _ => crate::types::value_str(value),
    }
}

pub(crate) fn try_column_const_cmp_bitmap_parts(
    col: usize,
    op: BinaryOp,
    konst: &Value,
    collation_id: Option<u16>,
    batch: &Batch,
) -> Option<Bitmap> {
    let n = batch.num_rows();
    // `col <cmp> NULL` is NULL for every row -> nothing is truthy.
    if matches!(konst, Value::Null) {
        return Some(Bitmap::new_all_invalid(n));
    }
    let mut out = Bitmap::new_all_invalid(n);
    // Integer column vs integer constant, plus the order-preserving temporal
    // lanes: DATE (i32 days), TIME / DATETIME (i64 micros). SQL text literals
    // intentionally remain strings in ExprProgram so warning provenance is
    // retained. Parse a valid constant once per batch here instead of sending
    // every row through compare_temporal_string_values/chrono. Invalid values
    // still return `None` and use the scalar oracle, preserving sql_mode,
    // prepared-parameter warning and evaluation-order semantics.
    let column = batch.columns().get(col)?;
    let nulls = match (column, konst) {
        (ColumnVector::I64 { data, nulls }, Value::Int(rhs)) => {
            if data.len() != n {
                return None;
            }
            dispatch_i64_const_cmp(op, data, *rhs, &mut out)?;
            nulls.as_ref()
        }
        (ColumnVector::I64 { data, nulls }, Value::UInt(rhs)) => {
            if data.len() != n {
                return None;
            }
            if let Ok(rhs) = i64::try_from(*rhs) {
                dispatch_i64_const_cmp(op, data, rhs, &mut out)?;
            } else {
                // Every i64 is less than an unsigned value above i64::MAX.
                fill_constant_order_cmp(op, std::cmp::Ordering::Less, n, &mut out)?;
            }
            nulls.as_ref()
        }
        (ColumnVector::U64 { data, nulls }, Value::UInt(rhs)) => {
            if data.len() != n {
                return None;
            }
            dispatch_u64_const_cmp(op, data, *rhs, &mut out)?;
            nulls.as_ref()
        }
        (ColumnVector::U64 { data, nulls }, Value::Int(rhs)) => {
            if data.len() != n {
                return None;
            }
            if let Ok(rhs) = u64::try_from(*rhs) {
                dispatch_u64_const_cmp(op, data, rhs, &mut out)?;
            } else {
                // Every u64 is greater than a negative signed value.
                fill_constant_order_cmp(op, std::cmp::Ordering::Greater, n, &mut out)?;
            }
            nulls.as_ref()
        }
        (ColumnVector::DateTime64 { data, nulls }, Value::DateTime(rhs))
        | (ColumnVector::Time64 { data, nulls }, Value::Time(rhs)) => {
            if data.len() != n {
                return None;
            }
            dispatch_i64_const_cmp(op, data, *rhs, &mut out)?;
            nulls.as_ref()
        }
        (ColumnVector::Timestamp64 { data, nulls }, rhs) => {
            if data.len() != n {
                return None;
            }
            let Value::Timestamp(rhs) = crate::engine::expr::coerce_comparison_constant_for_type(
                rhs,
                &crate::storage::DataTypeKind::Timestamp { fsp: 6 },
            )?
            else {
                return None;
            };
            dispatch_i64_const_cmp(op, data, rhs, &mut out)?;
            nulls.as_ref()
        }
        (ColumnVector::Date32 { data, nulls }, Value::Date(rhs)) => {
            if data.len() != n {
                return None;
            }
            dispatch_i32_const_cmp(op, data, *rhs, &mut out)?;
            nulls.as_ref()
        }
        (ColumnVector::Date32 { data, nulls }, rhs)
            if matches!(
                rhs,
                Value::String(_) | Value::SmallString(_) | Value::Bytes(_)
            ) =>
        {
            if data.len() != n {
                return None;
            }
            let Value::Date(rhs) = crate::engine::expr::coerce_comparison_constant_for_type(
                rhs,
                &crate::storage::DataTypeKind::Date,
            )?
            else {
                return None;
            };
            dispatch_i32_const_cmp(op, data, rhs, &mut out)?;
            nulls.as_ref()
        }
        (ColumnVector::DateTime64 { data, nulls }, rhs)
            if matches!(
                rhs,
                Value::String(_) | Value::SmallString(_) | Value::Bytes(_)
            ) =>
        {
            if data.len() != n {
                return None;
            }
            let rhs =
                crate::storage::parse_datetime_to_micros(crate::types::value_str(rhs)?).ok()?;
            dispatch_i64_const_cmp(op, data, rhs, &mut out)?;
            nulls.as_ref()
        }
        (ColumnVector::Time64 { data, nulls }, rhs)
            if matches!(
                rhs,
                Value::String(_) | Value::SmallString(_) | Value::Bytes(_)
            ) =>
        {
            if data.len() != n {
                return None;
            }
            let rhs = crate::storage::parse_time_to_micros(crate::types::value_str(rhs)?).ok()?;
            dispatch_i64_const_cmp(op, data, rhs, &mut out)?;
            nulls.as_ref()
        }
        (ColumnVector::Decimal32 { data, scale, nulls }, rhs) => {
            let (rhs, rhs_scale) = exact_decimal_const_parts(rhs)?;
            if let Some(rhs) = decimal_const_at_column_scale(rhs, rhs_scale, *scale) {
                if let Ok(rhs) = i32::try_from(rhs) {
                    dispatch_i32_const_cmp(op, data, rhs, &mut out)?;
                } else {
                    for (row, lhs) in data.iter().enumerate() {
                        if ordered_cmp_matches(op, &i128::from(*lhs), &rhs)? {
                            out.set_true(row);
                        }
                    }
                }
            } else {
                for (row, lhs) in data.iter().enumerate() {
                    if decimal_order_matches(op, i128::from(*lhs), *scale, rhs, rhs_scale)? {
                        out.set_true(row);
                    }
                }
            }
            nulls.as_ref()
        }
        (ColumnVector::Decimal64 { data, scale, nulls }, rhs) => {
            let (rhs, rhs_scale) = exact_decimal_const_parts(rhs)?;
            if let Some(rhs) = decimal_const_at_column_scale(rhs, rhs_scale, *scale) {
                if let Ok(rhs) = i64::try_from(rhs) {
                    dispatch_i64_const_cmp(op, data, rhs, &mut out)?;
                } else {
                    for (row, lhs) in data.iter().enumerate() {
                        if ordered_cmp_matches(op, &i128::from(*lhs), &rhs)? {
                            out.set_true(row);
                        }
                    }
                }
            } else {
                for (row, lhs) in data.iter().enumerate() {
                    if decimal_order_matches(op, i128::from(*lhs), *scale, rhs, rhs_scale)? {
                        out.set_true(row);
                    }
                }
            }
            nulls.as_ref()
        }
        (ColumnVector::Decimal128 { data, scale, nulls }, rhs) => {
            let (rhs, rhs_scale) = exact_decimal_const_parts(rhs)?;
            if let Some(rhs) = decimal_const_at_column_scale(rhs, rhs_scale, *scale) {
                for (row, lhs) in data.iter().enumerate() {
                    if ordered_cmp_matches(op, lhs, &rhs)? {
                        out.set_true(row);
                    }
                }
            } else {
                for (row, lhs) in data.iter().enumerate() {
                    if decimal_order_matches(op, *lhs, *scale, rhs, rhs_scale)? {
                        out.set_true(row);
                    }
                }
            }
            nulls.as_ref()
        }
        (
            column @ (ColumnVector::Utf8 {
                offsets,
                data,
                nulls,
            }
            | ColumnVector::Bytes {
                offsets,
                data,
                nulls,
            }),
            konst,
        ) if matches!(
            konst,
            Value::String(_) | Value::SmallString(_) | Value::Bytes(_)
        ) =>
        {
            if offsets.len() != n + 1 {
                return None;
            }
            let rhs = text_value_str(konst)?;
            let collation = collation_id.and_then(crate::charset::resolve_collation_id);
            let empty_equality = rhs.is_empty() && matches!(op, BinaryOp::Eq | BinaryOp::NotEq);
            for row in 0..n {
                if nulls.as_ref().is_some_and(|valid| !valid.get(row)) {
                    continue;
                }
                let start = offsets[row] as usize;
                let end = offsets[row + 1] as usize;
                if start > end || end > data.len() {
                    return None;
                }
                let bytes = &data[start..end];
                let value = if matches!(column, ColumnVector::Utf8 { .. }) {
                    // SAFETY: ColumnVector::Utf8 is validated at every storage,
                    // expression and materialization construction boundary.
                    unsafe { std::str::from_utf8_unchecked(bytes) }
                } else if bytes.is_ascii() {
                    // SAFETY: ASCII is valid UTF-8 even for a Bytes lane.
                    unsafe { std::str::from_utf8_unchecked(bytes) }
                } else {
                    simdutf8::basic::from_utf8(bytes).ok()?
                };
                let matches = if empty_equality {
                    let equal = crate::charset::string_equals_empty_for_info(
                        collation.unwrap_or_else(crate::charset::default_collation_info),
                        value,
                    );
                    if op == BinaryOp::Eq {
                        equal
                    } else {
                        !equal
                    }
                } else {
                    let ordering = match collation {
                        Some(info) => crate::charset::compare_strings_for_info(info, value, rhs),
                        None => crate::charset::default_compare_strings(value, rhs),
                    };
                    match op {
                        BinaryOp::Eq => ordering == std::cmp::Ordering::Equal,
                        BinaryOp::NotEq => ordering != std::cmp::Ordering::Equal,
                        BinaryOp::Lt => ordering == std::cmp::Ordering::Less,
                        BinaryOp::LtEq => ordering != std::cmp::Ordering::Greater,
                        BinaryOp::Gt => ordering == std::cmp::Ordering::Greater,
                        BinaryOp::GtEq => ordering != std::cmp::Ordering::Less,
                        _ => return None,
                    }
                };
                if matches {
                    out.set_true(row);
                }
            }
            VEC_STRING_CMP_ROWS.fetch_add(n as u64, std::sync::atomic::Ordering::Relaxed);
            nulls.as_ref()
        }
        (ColumnVector::Generic { values }, konst)
            if matches!(
                konst,
                Value::String(_) | Value::SmallString(_) | Value::Bytes(_)
            ) =>
        {
            if values.len() != n {
                return None;
            }
            let rhs = text_value_str(konst)?;
            let collation = collation_id.and_then(crate::charset::resolve_collation_id);
            for (row, lhs) in values.iter().enumerate() {
                if matches!(lhs, Value::Null) {
                    continue;
                }
                if !matches!(
                    lhs,
                    Value::String(_) | Value::SmallString(_) | Value::Bytes(_)
                ) {
                    return None;
                }
                let lhs = text_value_str(lhs)?;
                let ordering = match collation {
                    Some(info) => crate::charset::compare_strings_for_info(info, &lhs, &rhs),
                    None => crate::charset::default_compare_strings(&lhs, &rhs),
                };
                if match op {
                    BinaryOp::Eq => ordering == std::cmp::Ordering::Equal,
                    BinaryOp::NotEq => ordering != std::cmp::Ordering::Equal,
                    BinaryOp::Lt => ordering == std::cmp::Ordering::Less,
                    BinaryOp::LtEq => ordering != std::cmp::Ordering::Greater,
                    BinaryOp::Gt => ordering == std::cmp::Ordering::Greater,
                    BinaryOp::GtEq => ordering != std::cmp::Ordering::Less,
                    _ => return None,
                } {
                    out.set_true(row);
                }
            }
            VEC_STRING_CMP_ROWS.fetch_add(n as u64, std::sync::atomic::Ordering::Relaxed);
            None
        }
        _ => return None,
    };
    if let Some(valid) = nulls {
        out.and_assign(valid);
    }
    Some(out)
}

fn exact_decimal_const_parts(value: &Value) -> Option<(i128, u8)> {
    match value {
        Value::Int(value) => Some((i128::from(*value), 0)),
        Value::UInt(value) => i128::try_from(*value).ok().map(|value| (value, 0)),
        Value::DecimalCompact(value, scale) => Some((i128::from(*value), *scale)),
        Value::Decimal128(value, scale) => Some((*value, *scale)),
        Value::Decimal(value) | Value::String(value) => {
            crate::util::decimal::parse_exact_decimal_str(value)
        }
        Value::SmallString(value) => crate::util::decimal::parse_exact_decimal_str(unsafe {
            std::str::from_utf8_unchecked(value)
        }),
        Value::Bytes(value) => std::str::from_utf8(value)
            .ok()
            .and_then(crate::util::decimal::parse_exact_decimal_str),
        _ => None,
    }
}

/// Convert an exact constant once per batch to the scale shared by a typed
/// decimal column. The common column-scale >= constant-scale case can then
/// use the integer comparison kernels. When downscaling would be necessary
/// (and may discard a fractional remainder), callers keep the exact legacy
/// comparator.
#[inline]
fn decimal_const_at_column_scale(value: i128, value_scale: u8, column_scale: u8) -> Option<i128> {
    let shift = column_scale.checked_sub(value_scale)?;
    value.checked_mul(crate::util::decimal::pow10_i128(u32::from(shift))?)
}

#[inline]
fn decimal_order_matches(
    op: BinaryOp,
    mut left: i128,
    left_scale: u8,
    mut right: i128,
    right_scale: u8,
) -> Option<bool> {
    if left_scale < right_scale {
        left = left.checked_mul(crate::util::decimal::pow10_i128(u32::from(
            right_scale - left_scale,
        ))?)?;
    } else if right_scale < left_scale {
        right = right.checked_mul(crate::util::decimal::pow10_i128(u32::from(
            left_scale - right_scale,
        ))?)?;
    }
    let ordering = left.cmp(&right);
    Some(match op {
        BinaryOp::Eq => ordering == std::cmp::Ordering::Equal,
        BinaryOp::NotEq => ordering != std::cmp::Ordering::Equal,
        BinaryOp::Lt => ordering == std::cmp::Ordering::Less,
        BinaryOp::LtEq => ordering != std::cmp::Ordering::Greater,
        BinaryOp::Gt => ordering == std::cmp::Ordering::Greater,
        BinaryOp::GtEq => ordering != std::cmp::Ordering::Less,
        _ => return None,
    })
}

static VEC_STRING_CMP_ROWS: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

#[cfg(test)]
pub(crate) fn vec_string_cmp_rows_processed() -> u64 {
    VEC_STRING_CMP_ROWS.load(std::sync::atomic::Ordering::Relaxed)
}

/// B14 — `Column(idx) [NOT] LIKE <const-pattern>` straight over a Utf8 lane
/// into a row bitmap. Returns `None` (the caller falls back to the row-by-row
/// oracle) when either the program root isn't a `Column LIKE Const` shape, or
/// the lane isn't a `Utf8` lane whose offsets align to `batch.num_rows()`.
///
/// # Semantics parity
/// * NULL rows: `LIKE` returns NULL, which the row oracle treats as falsy
///   (never sets the bit). We skip NULL rows here for the same result.
/// * Non-NULL rows: `LikePlan::matches_str` is the single kernel the row path
///   uses; XOR with `negated` handles `NOT LIKE`. So every row gets exactly
///   the same bit the oracle would set — zero semantic drift by construction.
/// # Safety
/// The Utf8 lane's push path (`ColumnAccumulator::Utf8`, batch.rs:1629) only
/// accepts `Value::String` / `Value::SmallString` / `Value::Decimal(String)`,
/// all Rust-side UTF-8. We still validate with `str::from_utf8`: this is one
/// SIMD-accelerated pass per row (typically much cheaper than the
/// `String::from_utf8_lossy(...).into_owned()` clone that `value_at` does)
/// and keeps the fast path free of `unsafe`.
/// Counter of rows served by `try_column_like_const_bitmap` (successful fast
/// path). Bench-only observability: unit tests read it via
/// `vec_like_const_rows_processed()` to confirm the vec kernel is actually
/// wired (rather than the row-oracle silently taking every batch).
static VEC_LIKE_CONST_ROWS: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

#[cfg(test)]
pub(crate) fn vec_like_const_rows_processed() -> u64 {
    VEC_LIKE_CONST_ROWS.load(std::sync::atomic::Ordering::Relaxed)
}

fn try_column_like_bitmap(program: &ExprProgram, batch: &Batch) -> Option<(usize, Bitmap)> {
    if let Some((col, plan, negated)) = program.as_column_like_const() {
        return try_utf8_like_bitmap(batch, col, plan, negated).map(|bits| (col, bits));
    }
    let (col, param, negated, escape_char, collation_id) = program.as_column_like_param()?;
    let pattern = crate::engine::params::current_param(param)?;
    // The row program's cached-plan path is intentionally restricted to
    // string parameters. Keep NULL, numeric and binary patterns on that
    // oracle because their coercion/binary-string semantics differ.
    let pattern = match &pattern {
        Value::String(value) => value.as_str(),
        Value::SmallString(value) => std::str::from_utf8(value).ok()?,
        _ => return None,
    };
    let plan = crate::engine::func_cache::cached_like_plan(pattern, escape_char, collation_id);
    try_utf8_like_bitmap(batch, col, &plan, negated).map(|bits| (col, bits))
}

fn try_utf8_like_bitmap(
    batch: &Batch,
    col: usize,
    plan: &LikePlan,
    negated: bool,
) -> Option<Bitmap> {
    let column = batch.columns().get(col)?;
    let n = batch.num_rows();
    let (offsets, data, nulls) = match column {
        ColumnVector::Utf8 {
            offsets,
            data,
            nulls,
        } => (offsets, data, nulls),
        _ => return None,
    };
    if offsets.len() != n + 1 {
        return None;
    }
    let mut out = Bitmap::new_all_invalid(n);
    // Const-pattern `%anything%` matches every non-NULL row. Skip the
    // per-row `matches_str` call entirely; only NULL rows stay unset.
    if matches!(plan, LikePlan::WildcardAny) && !negated {
        for i in 0..n {
            if nulls.as_ref().is_some_and(|b| !b.get(i)) {
                continue;
            }
            out.set_true(i);
        }
        return Some(out);
    }
    for i in 0..n {
        if nulls.as_ref().is_some_and(|b| !b.get(i)) {
            continue;
        }
        let start = offsets[i] as usize;
        let end = offsets[i + 1] as usize;
        // Bounds guard: the accumulator's monotonic offsets should always
        // satisfy this, but a corrupt lane would otherwise panic on slice.
        if start > end || end > data.len() {
            return None;
        }
        let bytes = &data[start..end];
        let Ok(s) = std::str::from_utf8(bytes) else {
            // Should never fire (Utf8 lane push path is String-typed) but
            // if it does, fall back to the row oracle by giving up the
            // fast path entirely — safer than guessing at bytes.
            return None;
        };
        let matched = plan.matches_str(s);
        if matched != negated {
            out.set_true(i);
        }
    }
    VEC_LIKE_CONST_ROWS.fetch_add(n as u64, std::sync::atomic::Ordering::Relaxed);
    Some(out)
}

fn dispatch_i64_const_cmp(op: BinaryOp, data: &[i64], rhs: i64, out: &mut Bitmap) -> Option<()> {
    match op {
        BinaryOp::Eq => cmp_i64_const_eq_simd(data, rhs, out),
        BinaryOp::NotEq => cmp_i64_const_ne_simd(data, rhs, out),
        BinaryOp::Lt => cmp_i64_const_lt_simd(data, rhs, out),
        BinaryOp::LtEq => cmp_i64_const_le_simd(data, rhs, out),
        BinaryOp::Gt => cmp_i64_const_gt_simd(data, rhs, out),
        BinaryOp::GtEq => cmp_i64_const_ge_simd(data, rhs, out),
        _ => return None,
    }
    Some(())
}

fn dispatch_u64_const_cmp(op: BinaryOp, data: &[u64], rhs: u64, out: &mut Bitmap) -> Option<()> {
    if !matches!(
        op,
        BinaryOp::Eq
            | BinaryOp::NotEq
            | BinaryOp::Lt
            | BinaryOp::LtEq
            | BinaryOp::Gt
            | BinaryOp::GtEq
    ) {
        return None;
    }
    let matches = |lhs: u64| match op {
        BinaryOp::Eq => lhs == rhs,
        BinaryOp::NotEq => lhs != rhs,
        BinaryOp::Lt => lhs < rhs,
        BinaryOp::LtEq => lhs <= rhs,
        BinaryOp::Gt => lhs > rhs,
        BinaryOp::GtEq => lhs >= rhs,
        _ => unreachable!("comparison op checked above"),
    };
    for (row, lhs) in data.iter().copied().enumerate() {
        if matches(lhs) {
            out.set_true(row);
        }
    }
    Some(())
}

fn fill_constant_order_cmp(
    op: BinaryOp,
    ordering: std::cmp::Ordering,
    rows: usize,
    out: &mut Bitmap,
) -> Option<()> {
    use std::cmp::Ordering;
    let matches = match op {
        BinaryOp::Eq => ordering == Ordering::Equal,
        BinaryOp::NotEq => ordering != Ordering::Equal,
        BinaryOp::Lt => ordering == Ordering::Less,
        BinaryOp::LtEq => ordering != Ordering::Greater,
        BinaryOp::Gt => ordering == Ordering::Greater,
        BinaryOp::GtEq => ordering != Ordering::Less,
        _ => return None,
    };
    if matches {
        for row in 0..rows {
            out.set_true(row);
        }
    }
    Some(())
}

fn dispatch_i32_const_cmp(op: BinaryOp, data: &[i32], rhs: i32, out: &mut Bitmap) -> Option<()> {
    match op {
        BinaryOp::Eq => cmp_i32_const_eq_simd(data, rhs, out),
        BinaryOp::NotEq => cmp_i32_const_ne_simd(data, rhs, out),
        BinaryOp::Lt => cmp_i32_const_lt_simd(data, rhs, out),
        BinaryOp::LtEq => cmp_i32_const_le_simd(data, rhs, out),
        BinaryOp::Gt => cmp_i32_const_gt_simd(data, rhs, out),
        BinaryOp::GtEq => cmp_i32_const_ge_simd(data, rhs, out),
        _ => return None,
    }
    Some(())
}

// ---------------------------------------------------------------------
// Phase 2.3 — SIMD-style fast paths.
//
// These work directly on `&[i64]` / `&[f64]` slices extracted from an
// I64 / F64 `ColumnVector`. They're intentionally agnostic of
// `ExprProgram` so callers can use them in any vectorized operator
// that builds its own AST visitor (HashAgg, HashJoin probe filter,
// future predicate-pushdown into SeqScan).
//
// Integer comparisons use runtime-dispatched AVX2 and packed scalar masks
// on other CPUs. Floating-point kernels below preserve their scalar NaN
// semantics. CPU features affect only the kernel, never the physical plan.
// ---------------------------------------------------------------------

pub fn cmp_i64_const_eq_simd(a: &[i64], rhs: i64, out: &mut Bitmap) {
    super::integer_cmp::i64_cmp::<{ super::integer_cmp::EQ }>(a, rhs, out);
}

pub fn cmp_i64_const_gt_simd(a: &[i64], rhs: i64, out: &mut Bitmap) {
    super::integer_cmp::i64_cmp::<{ super::integer_cmp::GT }>(a, rhs, out);
}

pub fn cmp_i64_const_lt_simd(a: &[i64], rhs: i64, out: &mut Bitmap) {
    super::integer_cmp::i64_cmp::<{ super::integer_cmp::LT }>(a, rhs, out);
}

pub fn cmp_i64_const_ge_simd(a: &[i64], rhs: i64, out: &mut Bitmap) {
    super::integer_cmp::i64_cmp::<{ super::integer_cmp::GE }>(a, rhs, out);
}

pub fn cmp_i64_const_le_simd(a: &[i64], rhs: i64, out: &mut Bitmap) {
    super::integer_cmp::i64_cmp::<{ super::integer_cmp::LE }>(a, rhs, out);
}

pub fn cmp_i64_const_ne_simd(a: &[i64], rhs: i64, out: &mut Bitmap) {
    super::integer_cmp::i64_cmp::<{ super::integer_cmp::NE }>(a, rhs, out);
}

/// Element-wise `a + b` into `out`. All three slices must have equal
/// length. Wrapping addition matches MySQL's BIGINT semantics.
pub fn add_i64_simd(a: &[i64], b: &[i64], out: &mut [i64]) {
    debug_assert_eq!(a.len(), b.len());
    debug_assert_eq!(a.len(), out.len());
    for i in 0..a.len() {
        out[i] = a[i].wrapping_add(b[i]);
    }
}

pub fn sum_i64_simd(a: &[i64]) -> i128 {
    // 8-way accumulator unrolling — the auto-vectorizer turns this
    // into AVX2 horizontal adds on x86_64 / wide vector adds on M-series.
    let mut acc0: i128 = 0;
    let mut acc1: i128 = 0;
    let mut acc2: i128 = 0;
    let mut acc3: i128 = 0;
    let chunks = a.chunks_exact(4);
    let tail = chunks.remainder();
    for c in chunks {
        acc0 += c[0] as i128;
        acc1 += c[1] as i128;
        acc2 += c[2] as i128;
        acc3 += c[3] as i128;
    }
    for v in tail {
        acc0 += *v as i128;
    }
    acc0 + acc1 + acc2 + acc3
}

// ---------------------------------------------------------------------
// f64 const-comparison SIMD kernels — `a[i] OP rhs` into an all-invalid
// `out` bitmap. Used by the block-major NLJ to compare a block of outer
// `F64` lane values against one broadcast inner-row scalar. NaN compares
// false under every operator (Rust's float ops match MySQL's DOUBLE
// semantics), so NaN rows simply never set their bit — no special case.
// NULL handling is the caller's job (AND in the lane's null bitmap).
// ---------------------------------------------------------------------

pub fn cmp_f64_const_eq_simd(a: &[f64], rhs: f64, out: &mut Bitmap) {
    debug_assert!(out.len() >= a.len());
    for i in 0..a.len() {
        if a[i] == rhs {
            out.set_true(i);
        }
    }
}

pub fn cmp_f64_const_ne_simd(a: &[f64], rhs: f64, out: &mut Bitmap) {
    debug_assert!(out.len() >= a.len());
    for i in 0..a.len() {
        // NaN != rhs is true in Rust; but MySQL treats `x <> NULL` etc via
        // the null-bitmap path, and a stored NaN double `<>` a value is
        // true — matches Rust. Keep the direct operator.
        if a[i] != rhs {
            out.set_true(i);
        }
    }
}

pub fn cmp_f64_const_lt_simd(a: &[f64], rhs: f64, out: &mut Bitmap) {
    debug_assert!(out.len() >= a.len());
    for i in 0..a.len() {
        if a[i] < rhs {
            out.set_true(i);
        }
    }
}

pub fn cmp_f64_const_le_simd(a: &[f64], rhs: f64, out: &mut Bitmap) {
    debug_assert!(out.len() >= a.len());
    for i in 0..a.len() {
        if a[i] <= rhs {
            out.set_true(i);
        }
    }
}

pub fn cmp_f64_const_gt_simd(a: &[f64], rhs: f64, out: &mut Bitmap) {
    debug_assert!(out.len() >= a.len());
    for i in 0..a.len() {
        if a[i] > rhs {
            out.set_true(i);
        }
    }
}

pub fn cmp_f64_const_ge_simd(a: &[f64], rhs: f64, out: &mut Bitmap) {
    debug_assert!(out.len() >= a.len());
    for i in 0..a.len() {
        if a[i] >= rhs {
            out.set_true(i);
        }
    }
}

// ---------------------------------------------------------------------
// i32 SIMD kernels — used by `ColumnVector::Decimal32` lane and any
// future I32 lane. AVX2 compares eight i32 values per register; other
// targets use the same packed scalar fallback as the i64 kernels.
// ---------------------------------------------------------------------

pub fn cmp_i32_const_eq_simd(a: &[i32], rhs: i32, out: &mut Bitmap) {
    super::integer_cmp::i32_cmp::<{ super::integer_cmp::EQ }>(a, rhs, out);
}

pub fn cmp_i32_const_gt_simd(a: &[i32], rhs: i32, out: &mut Bitmap) {
    super::integer_cmp::i32_cmp::<{ super::integer_cmp::GT }>(a, rhs, out);
}

pub fn cmp_i32_const_ne_simd(a: &[i32], rhs: i32, out: &mut Bitmap) {
    super::integer_cmp::i32_cmp::<{ super::integer_cmp::NE }>(a, rhs, out);
}

pub fn cmp_i32_const_lt_simd(a: &[i32], rhs: i32, out: &mut Bitmap) {
    super::integer_cmp::i32_cmp::<{ super::integer_cmp::LT }>(a, rhs, out);
}

pub fn cmp_i32_const_le_simd(a: &[i32], rhs: i32, out: &mut Bitmap) {
    super::integer_cmp::i32_cmp::<{ super::integer_cmp::LE }>(a, rhs, out);
}

pub fn cmp_i32_const_ge_simd(a: &[i32], rhs: i32, out: &mut Bitmap) {
    super::integer_cmp::i32_cmp::<{ super::integer_cmp::GE }>(a, rhs, out);
}

/// Sum an i32 slice, widening straight into i128 to avoid intermediate
/// i64 overflow on very long columns. Same 4-way unrolling pattern as
/// `sum_i64_simd` — compiler auto-vectorises into AVX2 / NEON wide loads
/// followed by horizontal adds.
pub fn sum_i32_simd(a: &[i32]) -> i128 {
    let mut acc0: i128 = 0;
    let mut acc1: i128 = 0;
    let mut acc2: i128 = 0;
    let mut acc3: i128 = 0;
    let chunks = a.chunks_exact(4);
    let tail = chunks.remainder();
    for c in chunks {
        acc0 += c[0] as i128;
        acc1 += c[1] as i128;
        acc2 += c[2] as i128;
        acc3 += c[3] as i128;
    }
    for v in tail {
        acc0 += *v as i128;
    }
    acc0 + acc1 + acc2 + acc3
}

// ---------------------------------------------------------------------
// i128 SIMD-style kernel — fallback for DECIMAL(P>18) magnitudes that
// don't fit in i64. AVX2/NEON do not have native i128 lanes, so the
// "SIMD" here is just 8-way scalar accumulator unrolling. Still wins
// vs the state-machine `ExactDecimalAcc` path because we skip the
// dynamic value dispatch per row. Overflow retries the exact wide accumulator.
// ---------------------------------------------------------------------

pub fn sum_i128_simd(a: &[i128]) -> Option<i128> {
    let mut acc = [0i128; 8];
    let chunks = a.chunks_exact(8);
    let tail = chunks.remainder();
    for c in chunks {
        acc[0] = acc[0].checked_add(c[0])?;
        acc[1] = acc[1].checked_add(c[1])?;
        acc[2] = acc[2].checked_add(c[2])?;
        acc[3] = acc[3].checked_add(c[3])?;
        acc[4] = acc[4].checked_add(c[4])?;
        acc[5] = acc[5].checked_add(c[5])?;
        acc[6] = acc[6].checked_add(c[6])?;
        acc[7] = acc[7].checked_add(c[7])?;
    }
    let mut tot: i128 = 0;
    for a in acc {
        tot = tot.checked_add(a)?;
    }
    for v in tail {
        tot = tot.checked_add(*v)?;
    }
    Some(tot)
}

// ---------------------------------------------------------------------
// Decimal binary-arithmetic SIMD kernels.
//
// These power the `CompiledNumericExpr` batch path used by TPC-H Q1's
// `extprice * (1 - disc) * (1 + tax)` family of expressions. Inputs and
// outputs are raw scaled-integer magnitudes — scale alignment is the
// caller's responsibility. NULL handling is also out-of-band: kernels
// always write *some* value at every index (using `wrapping_*` ops to
// avoid panics on uninitialised lanes), and the caller AND-combines the
// inputs' validity bitmaps to mark the output NULLs.
//
// All loops are written in the unrolled style that LLVM auto-vectorises
// into AVX2 / NEON wide ops — same pattern as `sum_i64_simd` above.
// ---------------------------------------------------------------------

/// `out[i] = const_lhs - a[i]`, wrapping (so an arbitrary NULL slot
/// can't panic). Models `1 - disc` in TPC-H Q1 once the constant has
/// been scale-aligned to match `disc`.
#[cfg(test)]
pub fn sub_const_i64_lhs(const_lhs: i64, a: &[i64], out: &mut [i64]) {
    debug_assert_eq!(a.len(), out.len());
    for i in 0..a.len() {
        out[i] = const_lhs.wrapping_sub(a[i]);
    }
}

/// `out[i] = const_rhs + a[i]`, wrapping. Models `1 + tax`. Addition
/// is commutative so a single helper covers both `const + col` and
/// `col + const`.
#[cfg(test)]
pub fn add_const_i64(const_rhs: i64, a: &[i64], out: &mut [i64]) {
    debug_assert_eq!(a.len(), out.len());
    for i in 0..a.len() {
        out[i] = const_rhs.wrapping_add(a[i]);
    }
}

/// `out[i] = a[i] - b[i]`, wrapping i64. Used when both operands are
/// already on the same scale and lane width.
pub fn sub_i64_simd(a: &[i64], b: &[i64], out: &mut [i64]) {
    debug_assert_eq!(a.len(), b.len());
    debug_assert_eq!(a.len(), out.len());
    for i in 0..a.len() {
        out[i] = a[i].wrapping_sub(b[i]);
    }
}

/// `out[i] = (a[i] as i128) * (b[i] as i128)`. The widening step is
/// mandatory: two DECIMAL(15,2) values can each be ~9e15, so their
/// product overflows i64. Output scale = `l.scale + r.scale`.
pub fn mul_i64_widen_to_i128(a: &[i64], b: &[i64], out: &mut [i128]) {
    debug_assert_eq!(a.len(), b.len());
    debug_assert_eq!(a.len(), out.len());
    for i in 0..a.len() {
        out[i] = (a[i] as i128).wrapping_mul(b[i] as i128);
    }
}

/// `out[i] = a[i].wrapping_mul(b[i] as i128)`. Used when the left
/// operand is already a widened intermediate (e.g. the i128 result of
/// `extprice * (1 - disc)`) and the right operand is an i64 lane.
#[cfg(test)]
pub fn mul_i128_i64(a: &[i128], b: &[i64], out: &mut [i128]) {
    debug_assert_eq!(a.len(), b.len());
    debug_assert_eq!(a.len(), out.len());
    for i in 0..a.len() {
        out[i] = a[i].wrapping_mul(b[i] as i128);
    }
}

/// AND-merge of two optional validity bitmaps. Result is `Some` iff at
/// least one input has explicit nulls; otherwise both sides were
/// all-valid and so is the output. Each input is expected to have
/// length `n_rows`; this is debug-asserted but not enforced in release
/// builds — callers must keep validity bitmaps in lock-step with the
/// column they belong to.
pub fn merge_nulls(left: Option<&Bitmap>, right: Option<&Bitmap>, n_rows: usize) -> Option<Bitmap> {
    match (left, right) {
        (None, None) => None,
        (Some(b), None) | (None, Some(b)) => {
            debug_assert_eq!(b.len(), n_rows);
            Some(b.clone())
        }
        (Some(a), Some(b)) => {
            debug_assert_eq!(a.len(), n_rows);
            debug_assert_eq!(b.len(), n_rows);
            Some(a.and(b))
        }
    }
}

#[cfg(any(test, feature = "tools"))]
/// SUM kernel that dispatches on the actual `ColumnVector` decimal lane.
/// Returns `(sum_magnitude_i128, scale)`. The caller decides whether to
/// expose the result as `DecimalCompact` (fits i64) or `Decimal(String)`
/// (overflow). NULL handling is the caller's responsibility — pass the
/// slice + nulls bitmap separately to a NULL-skipping variant if needed.
///
/// Returns `None` for non-decimal column vectors, letting the caller fall
/// back to the row-at-a-time state-machine path in `aggregate.rs`.
#[cfg(any(test, feature = "tools"))]
pub fn sum_decimal_column(col: &ColumnVector) -> Option<(i128, u8)> {
    if let Some((data, scale)) = col.as_decimal_i32_slice() {
        return Some((sum_i32_simd(data), scale));
    }
    if let Some((data, scale)) = col.as_decimal_i64_slice() {
        return Some((sum_i64_simd(data), scale));
    }
    if let Some((data, scale)) = col.as_decimal_i128_slice() {
        return Some((sum_i128_simd(data)?, scale));
    }
    None
}

/// NULL-aware SUM kernel. Skips rows where `nulls.get(i) == false`.
/// Used by HashAggregate when the column has a `nulls` bitmap.
pub fn sum_decimal_column_with_nulls(col: &ColumnVector) -> Option<(i128, u8)> {
    let nulls = col.nulls();
    if let Some((data, scale)) = col.as_decimal_i32_slice() {
        let total = match nulls {
            Some(b) => sum_i32_skip_nulls(data, b),
            None => sum_i32_simd(data),
        };
        return Some((total, scale));
    }
    if let Some((data, scale)) = col.as_decimal_i64_slice() {
        let total = match nulls {
            Some(b) => sum_i64_skip_nulls(data, b),
            None => sum_i64_simd(data),
        };
        return Some((total, scale));
    }
    if let Some((data, scale)) = col.as_decimal_i128_slice() {
        let total = match nulls {
            Some(b) => sum_i128_skip_nulls(data, b)?,
            None => sum_i128_simd(data)?,
        };
        return Some((total, scale));
    }
    None
}

fn sum_i32_skip_nulls(a: &[i32], nulls: &Bitmap) -> i128 {
    let mut acc: i128 = 0;
    for (i, v) in a.iter().enumerate() {
        if nulls.get(i) {
            acc += *v as i128;
        }
    }
    acc
}

fn sum_i64_skip_nulls(a: &[i64], nulls: &Bitmap) -> i128 {
    let mut acc: i128 = 0;
    for (i, v) in a.iter().enumerate() {
        if nulls.get(i) {
            acc += *v as i128;
        }
    }
    acc
}

fn sum_i128_skip_nulls(a: &[i128], nulls: &Bitmap) -> Option<i128> {
    let mut acc: i128 = 0;
    for (i, v) in a.iter().enumerate() {
        if nulls.get(i) {
            acc = acc.checked_add(*v)?;
        }
    }
    Some(acc)
}

/// Selection-aware SUM kernel. Walks the columnar lane through a
/// caller-supplied list of row indices, summing values with NULL skip.
/// Used by `HashAggregateCore::consume_batch`'s GROUP BY branch to fold each
/// bucket's slice through the typed lane instead of the per-row
/// `Value`-dispatching `update_state` path. For TPC-H Q1 the inner loop
/// becomes a tight `acc += data[r] as i128` over a contiguous i64 lane
/// — vastly cheaper than the legacy match-on-`AggregateKind`-then-match-on-lane
/// dance the per-row path walks per (row, slot) pair.
pub fn sum_decimal_column_with_selection(
    col: &ColumnVector,
    selection: &[u32],
) -> Option<(i128, u8)> {
    let nulls = col.nulls();
    if let Some((data, scale)) = col.as_decimal_i32_slice() {
        let mut acc: i128 = 0;
        match nulls {
            Some(b) => {
                for &r in selection {
                    let r = r as usize;
                    if b.get(r) {
                        acc += data[r] as i128;
                    }
                }
            }
            None => {
                for &r in selection {
                    acc += data[r as usize] as i128;
                }
            }
        }
        return Some((acc, scale));
    }
    if let Some((data, scale)) = col.as_decimal_i64_slice() {
        let mut acc: i128 = 0;
        match nulls {
            Some(b) => {
                for &r in selection {
                    let r = r as usize;
                    if b.get(r) {
                        acc += data[r] as i128;
                    }
                }
            }
            None => {
                for &r in selection {
                    acc += data[r as usize] as i128;
                }
            }
        }
        return Some((acc, scale));
    }
    if let Some((data, scale)) = col.as_decimal_i128_slice() {
        let mut acc: i128 = 0;
        match nulls {
            Some(b) => {
                for &r in selection {
                    let r = r as usize;
                    if b.get(r) {
                        acc = acc.checked_add(data[r])?;
                    }
                }
            }
            None => {
                for &r in selection {
                    acc = acc.checked_add(data[r as usize])?;
                }
            }
        }
        return Some((acc, scale));
    }
    None
}

/// Count NULL-free rows in a selection. Cheap helper kept beside
/// `sum_decimal_column_with_selection` so the same caller can update
/// `AggState::count` without re-walking the bitmap. Routes through
/// `ColumnVector::count_non_null_selected` because the `Null`/`Generic`
/// lanes hold NULLs without a bitmap.
pub fn count_non_null_in_selection(col: &ColumnVector, selection: &[u32]) -> u64 {
    col.count_non_null_selected(selection) as u64
}

/// Translate a `BatchSchema` into the `InputSchema` shape that the
/// existing `ExprCompiler` understands.
fn batch_schema_input(schema: &BatchSchema) -> Cow<'_, InputSchema> {
    match schema.cached_input() {
        Some(input) => Cow::Borrowed(input),
        None => Cow::Owned(batch_schema_to_input(schema)),
    }
}

pub fn batch_schema_to_input(schema: &BatchSchema) -> InputSchema {
    let cols: Vec<InputColumn> = schema
        .fields()
        .iter()
        .map(|f| InputColumn {
            table: f.table.clone(),
            alias: f.alias.clone(),
            name: f.name.clone(),
            column_type: f.column_type,
            unsigned: f.source.flags & crate::mysql::protocol::UNSIGNED_FLAG != 0
                || matches!(
                    f.column_type,
                    crate::types::ColumnType::Year | crate::types::ColumnType::Bit { .. }
                ),
            collation_id: f.source.collation_id,
        })
        .collect();
    let bound_types = schema
        .fields()
        .iter()
        .map(|field| {
            crate::engine::expr_type::ExprType::from_column_type(field.column_type, &field.source)
        })
        .collect();
    let mut input = InputSchema::new_with_bound_types(cols, bound_types);
    input.mark_silent_string_columns(schema.fields().iter().enumerate().filter_map(
        |(index, field)| match field.column_type {
            crate::types::ColumnType::Blob
                if field.source.flags & crate::mysql::protocol::BLOB_FLAG != 0 =>
            {
                Some(index)
            }
            crate::types::ColumnType::VarString
                if field.source.flags & crate::mysql::protocol::BLOB_FLAG != 0 =>
            {
                Some(index)
            }
            _ => None,
        },
    ));
    input
}

/// Evaluate `program` over the batch's rows and pack the results into a
/// single `ColumnVector`. Picks a typed lane when all results agree on a
/// kind, otherwise emits `Generic`.
pub fn eval_to_column(program: &ExprProgram, batch: &Batch) -> Result<ColumnVector, QueryError> {
    if batch.selection().is_some()
        && !matches!(
            crate::engine::executor::vec::config::effective_mode(),
            crate::engine::executor::vec::config::VecMode::Off
        )
    {
        if let Some(column) = selected_utf8_length(program, batch) {
            crate::engine::expr_program::record_column_eval_rows(batch.visible_rows() as u64);
            return Ok(column);
        }
    }
    // Keep temporal projections in an integer lane. A Value buffer followed
    // by from_values wrote and redispatched an envelope for every input row.
    if batch.selection().is_none() {
        if let Some((kind, index)) = program.as_temporal_extract() {
            if let Some(column) = try_temporal_extract_column(kind, batch, index) {
                crate::engine::expr_program::record_column_eval_rows(batch.visible_rows() as u64);
                return Ok(column);
            }
        }
    }
    // REGEXP_REPLACE can preserve physical alignment across a selection by
    // writing empty/null entries for invisible rows. Keep this before the
    // selection-free typed kernels: filtered analytical scans are the common
    // consumer, and the proven-infallible constant-regex shape must not fall
    // back to per-row Value materialisation merely because a bitmap exists.
    if let Some((index, regex, replacement)) = program.as_regexp_replace_const_column() {
        if let Some(column) = batch.columns().get(index) {
            let typed = match column {
                ColumnVector::Null { len } => Some(ColumnVector::Null { len: *len }),
                ColumnVector::Utf8 {
                    offsets,
                    data,
                    nulls,
                } => regexp_replace_utf8_column(
                    &regex,
                    replacement,
                    offsets,
                    data,
                    nulls.clone(),
                    batch.selection(),
                ),
                _ => None,
            };
            if let Some(typed) = typed {
                crate::engine::expr_program::record_column_eval_rows(batch.visible_rows() as u64);
                return Ok(typed);
            }
        }
    }
    if batch.selection().is_none() {
        if let Some(index) = program.as_direct_column() {
            return batch.columns().get(index).cloned().ok_or_else(|| {
                crate::error::catalog::ER_BAD_FIELD_ERROR
                    .error("row value is missing for compiled expression")
            });
        }
        if let Some((kind, index)) = program.as_string_length_column() {
            if let Some(column) = batch.columns().get(index) {
                let typed = match column {
                    ColumnVector::Null { len } => Some(ColumnVector::Null { len: *len }),
                    ColumnVector::Bytes { offsets, nulls, .. } => Some(ColumnVector::I64 {
                        data: offsets
                            .windows(2)
                            .map(|pair| i64::from(pair[1] - pair[0]))
                            .collect(),
                        nulls: nulls.clone(),
                    }),
                    ColumnVector::Utf8 {
                        offsets,
                        data,
                        nulls,
                    } => Some(ColumnVector::I64 {
                        data: offsets
                            .windows(2)
                            .map(|pair| {
                                let bytes = &data[pair[0] as usize..pair[1] as usize];
                                match kind {
                                    crate::engine::expr_program::StringLengthKind::Octets => {
                                        bytes.len() as i64
                                    }
                                    crate::engine::expr_program::StringLengthKind::Characters => {
                                        String::from_utf8_lossy(bytes).chars().count() as i64
                                    }
                                }
                            })
                            .collect(),
                        nulls: nulls.clone(),
                    }),
                    _ => None,
                };
                if let Some(typed) = typed {
                    crate::engine::expr_program::record_column_eval_rows(
                        batch.visible_rows() as u64
                    );
                    return Ok(typed);
                }
            }
        }
        if let Some((kind, index)) = program.as_string_transform_column() {
            if let Some(column) = batch.columns().get(index) {
                let typed = match column {
                    ColumnVector::Null { len } => Some(ColumnVector::Null { len: *len }),
                    ColumnVector::Utf8 {
                        offsets,
                        data,
                        nulls,
                    } => transform_utf8_column(kind, offsets, data, nulls.clone()),
                    _ => None,
                };
                if let Some(typed) = typed {
                    crate::engine::expr_program::record_column_eval_rows(
                        batch.visible_rows() as u64
                    );
                    return Ok(typed);
                }
            }
        }
        if let Some((index, collation_id)) = program.as_distinct_collation_key_column() {
            if let Some(column) = batch.columns().get(index) {
                let typed = match column {
                    ColumnVector::Null { len } => Some(ColumnVector::Null { len: *len }),
                    // Binary strings already are their canonical DISTINCT
                    // key. Keep the shared contiguous lane and skip all
                    // character-collation work.
                    ColumnVector::Bytes { .. } => Some(column.clone()),
                    ColumnVector::Utf8 {
                        offsets,
                        data,
                        nulls,
                    } => distinct_key_utf8_column(collation_id, offsets, data, nulls.clone()),
                    _ => None,
                };
                if let Some(typed) = typed {
                    crate::engine::expr_program::record_column_eval_rows(
                        batch.visible_rows() as u64
                    );
                    return Ok(typed);
                }
            }
        }
        if let Some(numeric) = program.as_batch_numeric_expr() {
            let columns = batch.columns();
            let rows = batch.num_rows();
            if let Some(column) =
                crate::engine::executor::select::plan::numeric_batch::try_eval_integer_column(
                    &numeric, columns, rows,
                )
            {
                crate::engine::expr_program::record_column_eval_rows(batch.visible_rows() as u64);
                return Ok(column);
            }
            if let Some(lane) = crate::engine::executor::select::plan::numeric_batch::try_eval_lane(
                &numeric, columns, rows,
            ) {
                crate::engine::expr_program::record_column_eval_rows(batch.visible_rows() as u64);
                return Ok(
                    crate::engine::executor::select::plan::numeric_batch::lane_into_column(
                        lane, rows,
                    ),
                );
            }
        }
    }
    // Preserve established direct-column kernels before lowering composition.
    // This expression-major boundary owns the complete evaluation order.
    if !matches!(super::config::effective_mode(), super::config::VecMode::Off) {
        if let Some(column) = program.try_eval_column(batch) {
            return Ok(column);
        }
    }
    let values = eval_value_per_row(program, batch)?;
    Ok(ColumnVector::from_values(values))
}

/// A selection does not require a row interpreter for pure string lengths.
/// Validate once over the contiguous buffer and check each selected boundary;
/// malformed binary fallbacks still use the Value oracle.
fn selected_utf8_length(program: &ExprProgram, batch: &Batch) -> Option<ColumnVector> {
    let (kind, column) = program.as_string_length_column()?;
    let column = batch.columns().get(column)?;
    let ColumnVector::Utf8 {
        offsets,
        data,
        nulls,
    } = column
    else {
        return None;
    };
    let text = simdutf8::basic::from_utf8(data).ok()?;
    let mut output = vec![0i64; batch.num_rows()];
    let mut valid = Bitmap::new_all_invalid(batch.num_rows());
    for row in batch.visible_indices() {
        if nulls.as_ref().is_some_and(|nulls| !nulls.get(row)) {
            continue;
        }
        let value = text.get(*offsets.get(row)? as usize..*offsets.get(row + 1)? as usize)?;
        output[row] = match kind {
            crate::engine::expr_program::StringLengthKind::Octets => value.len(),
            crate::engine::expr_program::StringLengthKind::Characters => value.chars().count(),
        } as i64;
        valid.set_true(row);
    }
    let nulls = (valid.count_ones() != batch.num_rows()).then_some(valid);
    Some(ColumnVector::I64 {
        data: output,
        nulls,
    })
}

fn distinct_key_utf8_column(
    collation_id: u16,
    offsets: &[u32],
    input: &[u8],
    nulls: Option<Bitmap>,
) -> Option<ColumnVector> {
    let info = crate::charset::resolve_collation_id(collation_id)?;
    // Validate once per contiguous batch, then check individual row boundaries
    // against the validated str. Binary fallback lanes can contain bad UTF-8.
    let text = simdutf8::basic::from_utf8(input).ok()?;
    let mut output = Vec::with_capacity(input.len());
    let mut output_offsets = Vec::with_capacity(offsets.len());
    let mut scratch = Vec::new();
    // Fixed stack metadata only: references are offsets into the input and
    // already-owned output buffers. No additional retained keys or payloads.
    let mut recent: [Option<(u32, u32, u32, u32)>; 16] = [None; 16];
    output_offsets.push(0);
    for pair in offsets.windows(2) {
        let start = pair[0] as usize;
        let end = pair[1] as usize;
        let value = text.get(start..end)?;
        let slot = super::key_encode::fxhash(value.as_bytes()) as usize & (recent.len() - 1);
        let cached = recent[slot]
            .filter(|&(lo, hi, _, _)| input[lo as usize..hi as usize] == *value.as_bytes());
        if let Some((_, _, lo, hi)) = cached {
            output.extend_from_within(lo as usize..hi as usize);
        } else {
            let lo = u32::try_from(output.len()).ok()?;
            crate::charset::collation_key_for_info_into(info, value, &mut scratch);
            output.extend_from_slice(&scratch);
            let hi = u32::try_from(output.len()).ok()?;
            recent[slot] = Some((pair[0], pair[1], lo, hi));
        }
        output_offsets.push(u32::try_from(output.len()).ok()?);
    }
    Some(ColumnVector::Bytes {
        offsets: output_offsets,
        data: output,
        nulls,
    })
}

pub(crate) fn transform_utf8_column(
    kind: StringTransformKind,
    offsets: &[u32],
    input: &[u8],
    nulls: Option<Bitmap>,
) -> Option<ColumnVector> {
    let mut output = Vec::with_capacity(input.len());
    let mut output_offsets = Vec::with_capacity(offsets.len());
    output_offsets.push(0);
    for pair in offsets.windows(2) {
        let start = pair[0] as usize;
        let end = pair[1] as usize;
        let value = std::str::from_utf8(input.get(start..end)?).ok()?;
        match kind {
            StringTransformKind::Lower | StringTransformKind::Upper => {
                let upper = matches!(kind, StringTransformKind::Upper);
                if value.is_ascii() {
                    output.extend(value.bytes().map(|byte| {
                        if upper {
                            byte.to_ascii_uppercase()
                        } else {
                            byte.to_ascii_lowercase()
                        }
                    }));
                } else {
                    for ch in value.chars() {
                        let mut bytes = [0u8; 4];
                        output.extend_from_slice(
                            crate::engine::expr::mysql_case_char(ch, upper)
                                .encode_utf8(&mut bytes)
                                .as_bytes(),
                        );
                    }
                }
            }
            StringTransformKind::Ltrim => {
                output.extend_from_slice(value.trim_start_matches(' ').as_bytes())
            }
            StringTransformKind::Rtrim => {
                output.extend_from_slice(value.trim_end_matches(' ').as_bytes())
            }
            StringTransformKind::Trim => {
                output.extend_from_slice(value.trim_matches(' ').as_bytes())
            }
            StringTransformKind::Reverse => {
                for ch in value.chars().rev() {
                    let mut bytes = [0_u8; 4];
                    output.extend_from_slice(ch.encode_utf8(&mut bytes).as_bytes());
                }
            }
        }
        output_offsets.push(u32::try_from(output.len()).ok()?);
    }
    Some(ColumnVector::Utf8 {
        offsets: output_offsets,
        data: output,
        nulls,
    })
}

fn regexp_replace_utf8_column(
    regex: &regex::Regex,
    replacement: &str,
    offsets: &[u32],
    input: &[u8],
    nulls: Option<Bitmap>,
    selection: Option<&Selection>,
) -> Option<ColumnVector> {
    let rows = offsets.len().checked_sub(1)?;
    // ColumnVector::Utf8 construction is the validation boundary (storage
    // decode validates every borrowed payload before the batch sink appends
    // it). Revalidating the concatenated lane here scans the full string input
    // a second time before the regex engine sees it.
    // SAFETY: the Utf8 lane invariant guarantees valid UTF-8 and every stored
    // offset is produced at an original value boundary.
    let input = unsafe { std::str::from_utf8_unchecked(input) };
    let mut validity = nulls.clone().unwrap_or_else(|| Bitmap::new_all_valid(rows));
    let mut retain_nulls = nulls.is_some();
    if let Some(selection) = selection {
        let mut visible = Bitmap::new_all_invalid(rows);
        for row in selection.iter() {
            visible.set_true(row);
        }
        validity = validity.and(&visible);
        retain_nulls = true;
    }
    let mut output = Vec::with_capacity(input.len());
    let mut output_offsets = Vec::with_capacity(offsets.len());
    let single_match =
        !replacement.contains('$') && crate::engine::expr::regexp_replace_has_single_match(regex);
    output_offsets.push(0);
    let mut previous: Option<(usize, usize, usize, usize)> = None;
    for (row, pair) in offsets.windows(2).enumerate() {
        if !validity.get(row) {
            output_offsets.push(u32::try_from(output.len()).ok()?);
            continue;
        }
        let start = pair[0] as usize;
        let end = pair[1] as usize;
        let value = input.get(start..end)?;
        let output_start = output.len();
        if let Some((old_start, old_end, result_start, result_end)) = previous {
            if input.get(old_start..old_end)? == value {
                output.extend_from_within(result_start..result_end);
                output_offsets.push(u32::try_from(output.len()).ok()?);
                previous = Some((start, end, output_start, output.len()));
                continue;
            }
        }
        if single_match {
            crate::engine::expr::regexp_replace_single_into(regex, value, replacement, &mut output);
        } else {
            crate::engine::expr::regexp_replace_all_into(regex, value, replacement, &mut output);
        }
        output_offsets.push(u32::try_from(output.len()).ok()?);
        previous = Some((start, end, output_start, output.len()));
    }
    Some(ColumnVector::Utf8 {
        offsets: output_offsets,
        data: output,
        nulls: retain_nulls.then_some(validity),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::executor::vec::batch::{ColumnVector, Selection};
    use crate::engine::executor::vec::schema::BatchField;
    use crate::parser::Expr;
    use crate::types::ColumnType;

    #[test]
    fn selected_utf8_lengths_match_row_oracle_and_reject_split_characters() {
        let schema = Arc::new(BatchSchema::new(vec![BatchField {
            table: "t".into(),
            alias: None,
            name: "s".into(),
            column_type: ColumnType::VarString,
            source: Default::default(),
        }]));
        let batch = Batch::new(
            schema.clone(),
            vec![ColumnVector::from_values(vec![
                Value::String("ÅЯ".into()),
                Value::String("abc".into()),
                Value::Null,
                Value::String("𝄞".into()),
            ])],
        )
        .with_selection(Selection::Indices(vec![0, 2, 3]));
        for name in ["LENGTH", "CHAR_LENGTH"] {
            let expr = Expr::Function {
                name: name.into(),
                args: vec![Expr::Identifier("s".into())],
                distinct: false,
            };
            let program = compile_for_batch(&expr, &schema).unwrap().unwrap();
            let oracle = eval_value_per_row(&program, &batch).unwrap();
            let column = selected_utf8_length(&program, &batch).unwrap();
            assert_eq!(
                (0..batch.num_rows())
                    .map(|row| column.value_at(row))
                    .collect::<Vec<_>>(),
                oracle
            );
            let split = Batch::new(
                schema.clone(),
                vec![ColumnVector::Utf8 {
                    offsets: vec![0, 1, 2],
                    data: vec![0xc3, 0xa9],
                    nulls: None,
                }],
            )
            .with_selection(Selection::Range { start: 0, len: 2 });
            assert!(selected_utf8_length(&program, &split).is_none());
            let actual = eval_to_column(&program, &split).unwrap();
            assert_eq!(
                (0..2).map(|row| actual.value_at(row)).collect::<Vec<_>>(),
                eval_value_per_row(&program, &split).unwrap()
            );
        }
    }

    fn schema() -> Arc<BatchSchema> {
        Arc::new(BatchSchema::new(vec![BatchField {
            table: "t".into(),
            alias: None,
            name: "x".into(),
            column_type: ColumnType::LongLong,
            source: Default::default(),
        }]))
    }

    #[test]
    fn distinct_collation_reuse_matches_unicode_oracle_and_rejects_split_rows() {
        let mut values = vec![
            "".to_string(),
            "Русский текст".repeat(8),
            "A\0 ".into(),
            "á".into(),
        ];
        values.extend((0..80).map(|i| format!("collision-{i}-中文")));
        values.extend(values[..4].to_vec());
        values.extend(std::iter::repeat_n("Русский текст".repeat(8), 30));
        let mut input = Vec::new();
        let mut offsets = vec![0];
        for value in &values {
            input.extend_from_slice(value.as_bytes());
            offsets.push(input.len() as u32);
        }
        let mut nulls = Bitmap::new_all_valid(values.len());
        nulls.set(2, false);
        for name in ["utf8mb4_0900_ai_ci", "utf8mb4_0900_as_cs", "utf8mb4_bin"] {
            let info = crate::charset::resolve_collation(name).unwrap();
            let actual =
                distinct_key_utf8_column(info.id, &offsets, &input, Some(nulls.clone())).unwrap();
            let mut expected = Vec::new();
            for (row, value) in values.iter().enumerate() {
                crate::charset::collation_key_for_info_into(info, value, &mut expected);
                if row == 2 {
                    assert!(actual.is_null(row));
                } else {
                    assert_eq!(
                        actual.value_at(row),
                        Value::Bytes(expected.clone()),
                        "{name} row {row}"
                    );
                }
            }
            assert!(distinct_key_utf8_column(info.id, &[0, 1, 2], &[0xc3, 0xa9], None).is_none());
            assert!(distinct_key_utf8_column(info.id, &[0, 1], &[0xff], None).is_none());
        }
    }

    #[test]
    fn distinct_collation_key_projects_one_typed_bytes_lane() {
        let collation_id = crate::charset::resolve_collation("utf8mb4_0900_ai_ci")
            .unwrap()
            .id;
        let string_schema = Arc::new(BatchSchema::new(vec![BatchField {
            table: "t".into(),
            alias: None,
            name: "v".into(),
            column_type: ColumnType::VarString,
            source: crate::types::ColumnSource {
                collation_id: Some(collation_id),
                ..Default::default()
            },
        }]));
        let batch = Batch::new(
            Arc::clone(&string_schema),
            vec![ColumnVector::from_values(vec![
                Value::String("A".into()),
                Value::String("á".into()),
                Value::String("B".into()),
            ])],
        );
        let expr = Expr::Function {
            name: "__DISTINCT_COLLATION_KEY".into(),
            args: vec![Expr::Identifier("v".into())],
            distinct: false,
        };
        let program = compile_for_batch(&expr, &string_schema)
            .unwrap()
            .expect("distinct key compiles");
        let projected = eval_to_column(&program, &batch).unwrap();
        let ColumnVector::Bytes { offsets, data, .. } = projected else {
            panic!("distinct key must stay in a typed Bytes lane");
        };
        let a = crate::charset::default_string_key("a");
        let b = crate::charset::default_string_key("b");
        assert_eq!(&data[offsets[0] as usize..offsets[1] as usize], a);
        assert_eq!(&data[offsets[1] as usize..offsets[2] as usize], a);
        assert_eq!(&data[offsets[2] as usize..offsets[3] as usize], b);
    }

    #[test]
    fn coalesce_numeric_comparisons_use_native_three_valued_masks() {
        use crate::parser::{BinaryOp, Literal};

        let schema = Arc::new(BatchSchema::new(
            ["a", "b"]
                .into_iter()
                .map(|name| BatchField {
                    table: "t".into(),
                    alias: None,
                    name: name.into(),
                    column_type: ColumnType::Decimal {
                        precision: 15,
                        scale: 2,
                    },
                    source: Default::default(),
                })
                .collect(),
        ));
        let mut a_valid = Bitmap::new_all_valid(6);
        for row in [2, 4, 5] {
            a_valid.set(row, false);
        }
        let mut b_valid = Bitmap::new_all_valid(6);
        for row in [0, 2, 5] {
            b_valid.set(row, false);
        }
        let batch = Batch::new(
            Arc::clone(&schema),
            vec![
                ColumnVector::Decimal64 {
                    data: vec![100, 0, 0, -100, 0, 0],
                    scale: 2,
                    nulls: Some(a_valid),
                },
                ColumnVector::Decimal64 {
                    data: vec![0, 100, 0, 0, -100, 0],
                    scale: 2,
                    nulls: Some(b_valid),
                },
            ],
        );
        let positive = |name: &str, function: &str| Expr::Binary {
            left: Box::new(Expr::Function {
                name: function.into(),
                args: vec![
                    Expr::Identifier(name.into()),
                    Expr::Literal(Literal::Int(0)),
                ],
                distinct: false,
            }),
            op: BinaryOp::Gt,
            right: Box::new(Expr::Literal(Literal::Int(0))),
        };
        let expr = Expr::Binary {
            left: Box::new(positive("a", "COALESCE")),
            op: BinaryOp::Or,
            right: Box::new(positive("b", "IFNULL")),
        };
        let vector = BatchExprProgram::compile(&expr, &schema)
            .unwrap()
            .expect("numeric COALESCE predicate compiles");
        let actual = vector.eval_predicate_bitmap(&batch).unwrap();
        let oracle_program = compile_for_batch(&expr, &schema).unwrap().unwrap();
        let oracle = eval_value_per_row(&oracle_program, &batch).unwrap();
        for (row, value) in oracle.iter().enumerate() {
            assert_eq!(actual.get(row), crate::engine::expr::truthy(value));
        }
        let (native, fallback) = vector.expression_rows();
        assert_eq!(fallback, 0);
        assert!(native >= batch.num_rows() as u64);

        // A NULL fallback preserves UNKNOWN for source-NULL rows. Exercise
        // the truth/null masks directly so NOT/AND/OR composition remains an
        // exact SQL three-valued-logic match, not merely a WHERE truthy match.
        let null_fallback = Expr::Binary {
            left: Box::new(Expr::Function {
                name: "COALESCE".into(),
                args: vec![Expr::Identifier("a".into()), Expr::Literal(Literal::Null)],
                distinct: false,
            }),
            op: BinaryOp::Gt,
            right: Box::new(Expr::Literal(Literal::Int(0))),
        };
        let leaf = compile_for_batch(&null_fallback, &schema).unwrap().unwrap();
        let mask = try_native_truth_mask(PredicateKernel::select(&leaf), &leaf, &batch)
            .expect("typed COALESCE mask");
        assert_eq!(mask.truth.iter_true().collect::<Vec<_>>(), vec![0]);
        assert_eq!(mask.null.iter_true().collect::<Vec<_>>(), vec![2, 4, 5]);
    }

    #[test]
    fn decimal_between_uses_two_native_bitmap_leaves() {
        use crate::parser::Literal;

        let schema = Arc::new(BatchSchema::new(vec![BatchField {
            table: "t".into(),
            alias: None,
            name: "discount".into(),
            column_type: ColumnType::Decimal {
                precision: 15,
                scale: 2,
            },
            source: Default::default(),
        }]));
        let batch = Batch::new(
            Arc::clone(&schema),
            vec![ColumnVector::Decimal64 {
                data: vec![4, 5, 7, 8],
                scale: 2,
                nulls: None,
            }],
        );
        let expr = Expr::Between {
            expr: Box::new(Expr::Identifier("discount".into())),
            low: Box::new(Expr::Literal(Literal::Decimal("0.05".into()))),
            high: Box::new(Expr::Literal(Literal::Decimal("0.07".into()))),
            negated: false,
        };
        let program = BatchExprProgram::compile(&expr, &schema)
            .unwrap()
            .expect("compile batch BETWEEN");
        let selected = program.eval_predicate_bitmap(&batch).unwrap();
        assert_eq!(
            (0..4).map(|row| selected.get(row)).collect::<Vec<_>>(),
            vec![false, true, true, false]
        );
        let (native, fallback) = program.expression_rows();
        assert_eq!(fallback, 0);
        assert!(native >= 4);
    }

    #[test]
    fn typed_decimal_constant_comparisons_match_exact_scale_oracle() {
        let schema = Arc::new(BatchSchema::new(vec![BatchField {
            table: "t".into(),
            alias: None,
            name: "d".into(),
            column_type: ColumnType::Decimal {
                precision: 9,
                scale: 2,
            },
            source: Default::default(),
        }]));
        let magnitudes = [-250i32, 100, 155, 200, 10_000];
        let batch = Batch::new(
            schema,
            vec![ColumnVector::Decimal32 {
                data: magnitudes.to_vec(),
                scale: 2,
                nulls: None,
            }],
        );
        // Integer constants exercise the once-per-batch upscale fast path;
        // the scale-3 value exercises the exact downscale fallback.
        for konst in [
            Value::Int(1),
            Value::DecimalCompact(155, 2),
            Value::DecimalCompact(1_555, 3),
        ] {
            let (rhs, rhs_scale) = exact_decimal_const_parts(&konst).unwrap();
            for op in [
                BinaryOp::Eq,
                BinaryOp::NotEq,
                BinaryOp::Lt,
                BinaryOp::LtEq,
                BinaryOp::Gt,
                BinaryOp::GtEq,
            ] {
                let actual =
                    try_column_const_cmp_bitmap_parts(0, op, &konst, None, &batch).unwrap();
                for (row, lhs) in magnitudes.iter().enumerate() {
                    assert_eq!(
                        actual.get(row),
                        decimal_order_matches(op, i128::from(*lhs), 2, rhs, rhs_scale).unwrap(),
                        "op={op:?} konst={konst:?} row={row}"
                    );
                }
            }
        }
    }

    #[test]
    fn bitmap_restriction_preserves_each_selection_representation() {
        let schema = schema();
        let base = Batch::new(
            schema,
            vec![ColumnVector::I64 {
                data: (0..130).collect(),
                nulls: None,
            }],
        );
        let mut bits = Bitmap::new_all_invalid(130);
        for row in [0usize, 1, 2, 63, 64, 65, 100, 129] {
            bits.set_true(row);
        }
        let selections = vec![
            (
                Selection::Range { start: 2, len: 99 },
                vec![2usize, 63, 64, 65, 100],
            ),
            (Selection::Indices(vec![2, 64, 100]), vec![2, 64, 100]),
            (
                {
                    let mut mask = Bitmap::new_all_invalid(130);
                    for row in [2usize, 64, 100] {
                        mask.set_true(row);
                    }
                    Selection::Mask(mask)
                },
                vec![2, 64, 100],
            ),
        ];
        for (selection, expected) in selections {
            let selected =
                restrict_bitmap_to_selection(&base.clone().with_selection(selection), bits.clone());
            assert_eq!(selected.iter_true().collect::<Vec<_>>(), expected,);
        }
    }

    #[test]
    fn integer_predicate_lanes_match_oracle_across_signedness() {
        use crate::parser::{BinaryOp, Literal};

        fn assert_comparison(
            schema: &Arc<BatchSchema>,
            batch: &Batch,
            op: BinaryOp,
            literal: Literal,
        ) {
            let expr = Expr::Binary {
                left: Box::new(Expr::Identifier("x".into())),
                op,
                right: Box::new(Expr::Literal(literal)),
            };
            let program = compile_for_batch(&expr, schema)
                .unwrap()
                .expect("integer comparison compiles");
            let typed = try_column_const_cmp_bitmap(&program, batch)
                .expect("integer comparison selects typed lane");
            let oracle = eval_value_per_row(&program, batch).unwrap();
            for (row, value) in oracle.iter().enumerate() {
                assert_eq!(typed.get(row), crate::engine::expr::truthy(value));
            }
        }

        let signed_schema = schema();
        let signed_batch = Batch::new(
            signed_schema.clone(),
            vec![ColumnVector::I64 {
                data: vec![i64::MIN, -1, 0, i64::MAX],
                nulls: None,
            }],
        );
        for op in [
            BinaryOp::Eq,
            BinaryOp::NotEq,
            BinaryOp::Lt,
            BinaryOp::LtEq,
            BinaryOp::Gt,
            BinaryOp::GtEq,
        ] {
            assert_comparison(&signed_schema, &signed_batch, op, Literal::UInt(100));
            assert_comparison(&signed_schema, &signed_batch, op, Literal::UInt(u64::MAX));
        }

        let mut unsigned_source = crate::types::ColumnSource::default();
        unsigned_source.flags = crate::mysql::protocol::UNSIGNED_FLAG;
        let unsigned_schema = Arc::new(BatchSchema::new(vec![BatchField {
            table: "t".into(),
            alias: None,
            name: "x".into(),
            column_type: ColumnType::LongLong,
            source: unsigned_source,
        }]));
        let unsigned_batch = Batch::new(
            unsigned_schema.clone(),
            vec![ColumnVector::U64 {
                data: vec![0, 1, i64::MAX as u64, u64::MAX],
                nulls: None,
            }],
        );
        for op in [
            BinaryOp::Eq,
            BinaryOp::NotEq,
            BinaryOp::Lt,
            BinaryOp::LtEq,
            BinaryOp::Gt,
            BinaryOp::GtEq,
        ] {
            assert_comparison(&unsigned_schema, &unsigned_batch, op, Literal::Int(-1));
            assert_comparison(&unsigned_schema, &unsigned_batch, op, Literal::Int(100));
        }
    }

    #[test]
    fn signed_bigint_between_unsigned_literals_stays_native() {
        use crate::parser::{BinaryOp, Literal};

        let schema = schema();
        let batch = Batch::new(
            schema.clone(),
            vec![ColumnVector::I64 {
                data: vec![-1, 99, 100, 900_000, 900_001],
                nulls: None,
            }],
        );
        let expr = Expr::Between {
            expr: Box::new(Expr::Identifier("x".into())),
            low: Box::new(Expr::Literal(Literal::UInt(100))),
            high: Box::new(Expr::Literal(Literal::UInt(900_000))),
            negated: false,
        };
        let program = BatchExprProgram::compile(&expr, &schema)
            .unwrap()
            .expect("compile integer BETWEEN");
        let selected = program.eval_predicate_bitmap(&batch).unwrap();
        assert_eq!(
            (0..batch.num_rows())
                .map(|row| selected.get(row))
                .collect::<Vec<_>>(),
            vec![false, false, true, true, false]
        );
        let (native, fallback) = program.expression_rows();
        assert_eq!(fallback, 0);
        assert!(native >= batch.num_rows() as u64);

        // Literal auto-parameterization reaches workers as placeholders; the
        // concrete bound integer must retain the same typed lane.
        let parameterized = Expr::Binary {
            left: Box::new(Expr::Identifier("x".into())),
            op: BinaryOp::GtEq,
            right: Box::new(Expr::Placeholder(0)),
        };
        let parameterized = compile_for_batch(&parameterized, &schema)
            .unwrap()
            .expect("compile integer placeholder");
        crate::engine::params::with_auto_params(vec![Value::UInt(100)], || {
            let selected = try_column_const_cmp_bitmap(&parameterized, &batch)
                .expect("bound unsigned integer selects signed typed lane");
            assert_eq!(selected.count_ones(), 3);
        });

        let parameterized_between = Expr::Between {
            expr: Box::new(Expr::Identifier("x".into())),
            low: Box::new(Expr::Placeholder(0)),
            high: Box::new(Expr::Placeholder(1)),
            negated: false,
        };
        let parameterized_between = BatchExprProgram::compile(&parameterized_between, &schema)
            .unwrap()
            .expect("compile parameterized integer BETWEEN");
        crate::engine::params::with_auto_params(
            vec![Value::UInt(100), Value::UInt(900_000)],
            || {
                let selected = parameterized_between.eval_predicate_bitmap(&batch).unwrap();
                assert_eq!(selected.count_ones(), 2);
            },
        );
        let (_, fallback) = parameterized_between.expression_rows();
        assert_eq!(fallback, 0);
    }

    /// B14 release performance baseline: vector LIKE is at least 1.5x faster than eval_value_per_row,
    /// observed at 3-5x. Debug narrows the gap, so it runs correctness differential tests only;
    /// performance thresholds apply only to release builds.
    #[test]
    #[cfg(not(debug_assertions))]
    fn b14_vec_like_kernel_beats_row_oracle_release() {
        use crate::parser::Expr as PExpr;

        let s = Arc::new(BatchSchema::new(vec![BatchField {
            table: "t".into(),
            alias: None,
            name: "c".into(),
            column_type: ColumnType::VarString,
            source: Default::default(),
        }]));
        let n = 100_000usize;
        let mut values = Vec::with_capacity(n);
        for i in 0..n {
            let flag = if i % 3 == 0 { "COMPLETED" } else { "PENDING" };
            values.push(Value::String(format!("order-{i}-{flag}-2026")));
        }
        let col = ColumnVector::from_values(values);
        let batch = Batch::new(s.clone(), vec![col]);

        let expr = PExpr::Like {
            expr: Box::new(PExpr::Identifier("c".into())),
            pattern: Box::new(PExpr::Literal(crate::parser::Literal::String(
                "%COMPLETED%".into(),
            ))),
            negated: false,
            escape_char: None,
        };
        let program = compile_for_batch(&expr, &s).unwrap().expect("compiles");

        // Warm up.
        let _ = eval_predicate_bitmap(&program, &batch).unwrap();
        let _ = eval_value_per_row(&program, &batch).unwrap();

        // Run each path five times and take the minimum wall-clock duration.
        let vec_min = (0..5)
            .map(|_| {
                let t = std::time::Instant::now();
                let b = eval_predicate_bitmap(&program, &batch).unwrap();
                std::hint::black_box(b);
                t.elapsed()
            })
            .min()
            .unwrap();
        let row_min = (0..5)
            .map(|_| {
                let t = std::time::Instant::now();
                let mut bits = Bitmap::new_all_invalid(batch.num_rows());
                for (i, v) in eval_value_per_row(&program, &batch)
                    .unwrap()
                    .iter()
                    .enumerate()
                {
                    if crate::engine::expr::truthy(v) {
                        bits.set_true(i);
                    }
                }
                std::hint::black_box(bits);
                t.elapsed()
            })
            .min()
            .unwrap();

        let vec_us = vec_min.as_micros();
        let row_us = row_min.as_micros();
        eprintln!(
            "B14 vec LIKE kernel: row_oracle={}µs vec_kernel={}µs speedup={:.2}×",
            row_us,
            vec_us,
            row_us as f64 / vec_us.max(1) as f64
        );
        // Require at least 1.5x speedup allowing CI noise; stable machines measured 3-5x.
        assert!(
            (vec_us as f64) * 1.5 <= (row_us as f64),
            "B14 vector LIKE must be at least 1.5x faster than the row oracle: row={row_us}µs, vec={vec_us}µs"
        );
    }

    /// B14 — Direct kernel test: build a Utf8 batch + `Column LIKE <const>`
    /// program, call `eval_predicate_bitmap`, assert (a) the counter grew
    /// (fast path fired, not the row oracle) and (b) the bitmap is
    /// bit-for-bit identical to the row-by-row `truthy(eval_row)` oracle
    /// for every plan shape.
    #[test]
    fn b14_vec_like_const_kernel_fires_and_matches_oracle() {
        use crate::parser::Expr as PExpr;

        let s = Arc::new(BatchSchema::new(vec![BatchField {
            table: "t".into(),
            alias: None,
            name: "c".into(),
            column_type: ColumnType::VarString,
            source: Default::default(),
        }]));
        // Rows: prefix / suffix / substring / cjk / empty / null.
        let values: Vec<Value> = vec![
            Value::String("order-1-COMPLETED".into()),
            Value::String("cart-2-pending".into()),
            Value::String("汉字-3-测试".into()),
            Value::String("plain".into()),
            Value::String("".into()),
            Value::Null,
            Value::String("prefix-only-COMPLETED-suffix".into()),
        ];
        let col = ColumnVector::from_values(values);
        let batch = Batch::new(s.clone(), vec![col]);

        // Pattern shapes to exercise every LikePlan variant that's meaningful
        // to fold into a byte-level fast path.
        let patterns: [(&str, bool); 8] = [
            ("plain", false),             // Literal
            ("order-%", false),           // Prefix
            ("%pending", false),          // Suffix
            ("%COMPLETED%", false),       // Contains
            ("%order%COMPLETED%", false), // MultiSegment
            ("%", false),                 // WildcardAny
            ("汉字%", false),             // CiAscii / non-ASCII
            ("plain", true),              // Literal negated
        ];

        for (pat, negated) in patterns {
            let expr = PExpr::Like {
                expr: Box::new(PExpr::Identifier("c".into())),
                pattern: Box::new(PExpr::Literal(crate::parser::Literal::String(pat.into()))),
                negated,
                escape_char: None,
            };
            let program = compile_for_batch(&expr, &s).unwrap().expect("compiles");
            assert!(
                program.as_column_like_const().is_some(),
                "program must be Column LIKE Const shape for pat={pat:?}"
            );
            let before = vec_like_const_rows_processed();
            let bits = eval_predicate_bitmap(&program, &batch).unwrap();
            let after = vec_like_const_rows_processed();
            assert!(
                after > before,
                "vec kernel must fire for pat={pat:?} (counter didn't grow)"
            );
            // Row oracle: materialise Values then apply truthy.
            let mut oracle = Bitmap::new_all_invalid(batch.num_rows());
            for (i, v) in eval_value_per_row(&program, &batch)
                .unwrap()
                .iter()
                .enumerate()
            {
                if crate::engine::expr::truthy(v) {
                    oracle.set_true(i);
                }
            }
            for i in 0..batch.num_rows() {
                assert_eq!(
                    bits.get(i),
                    oracle.get(i),
                    "vec/oracle bitmap disagree at row {i} for pat={pat:?} negated={negated}"
                );
            }
        }

        // Auto/prepared parameters keep the same native lane. The bound
        // pattern is classified once for the batch, including the resolved
        // column collation, rather than dispatched once per row.
        let parameterized = PExpr::Like {
            expr: Box::new(PExpr::Identifier("c".into())),
            pattern: Box::new(PExpr::Placeholder(0)),
            negated: true,
            escape_char: None,
        };
        let program = compile_for_batch(&parameterized, &s)
            .unwrap()
            .expect("parameterized LIKE compiles");
        assert!(program.as_column_like_param().is_some());
        crate::engine::params::with_auto_params(
            vec![Value::String("%special%requests%".into())],
            || {
                let before = vec_like_const_rows_processed();
                let native = eval_predicate_bitmap(&program, &batch).unwrap();
                assert!(vec_like_const_rows_processed() >= before + batch.num_rows() as u64);
                let oracle = eval_value_per_row(&program, &batch).unwrap();
                for (row, value) in oracle.iter().enumerate() {
                    assert_eq!(native.get(row), crate::engine::expr::truthy(value));
                }
            },
        );
        crate::engine::params::with_auto_params(vec![Value::Int(1)], || {
            assert!(
                try_column_like_bitmap(&program, &batch).is_none(),
                "numeric LIKE parameter must retain its scalar coercion oracle"
            );
        });
    }

    #[test]
    fn eval_column_reference_per_row() {
        let s = schema();
        let col = ColumnVector::from_values(vec![Value::Int(10), Value::Int(20), Value::Int(30)]);
        let b = Batch::new(s.clone(), vec![col]);
        let expr = Expr::Identifier("x".into());
        let program = compile_for_batch(&expr, &s).unwrap().expect("compiles");
        let v = eval_value_per_row(&program, &b).unwrap();
        assert_eq!(v, vec![Value::Int(10), Value::Int(20), Value::Int(30)]);
    }

    #[test]
    fn string_length_projection_stays_in_typed_column_lane() {
        let s = Arc::new(BatchSchema::new(vec![BatchField {
            table: "t".into(),
            alias: None,
            name: "s".into(),
            column_type: ColumnType::VarString,
            source: Default::default(),
        }]));
        let batch = Batch::new(
            s.clone(),
            vec![ColumnVector::from_values(vec![
                Value::String("é".into()),
                Value::String("abc".into()),
                Value::Null,
            ])],
        );
        for (name, expected) in [
            ("LENGTH", vec![Value::Int(2), Value::Int(3), Value::Null]),
            (
                "CHAR_LENGTH",
                vec![Value::Int(1), Value::Int(3), Value::Null],
            ),
        ] {
            let expr = Expr::Function {
                name: name.into(),
                args: vec![Expr::Identifier("s".into())],
                distinct: false,
            };
            let program = compile_for_batch(&expr, &s).unwrap().unwrap();
            let result = eval_to_column(&program, &batch).unwrap();
            assert!(matches!(&result, ColumnVector::I64 { .. }));
            assert_eq!(
                (0..batch.num_rows())
                    .map(|row| result.value_at(row))
                    .collect::<Vec<_>>(),
                expected
            );
        }
    }

    #[test]
    fn string_transform_projection_stays_in_utf8_lane() {
        let schema = Arc::new(BatchSchema::new(vec![BatchField {
            table: "t".into(),
            alias: None,
            name: "s".into(),
            column_type: ColumnType::VarString,
            source: Default::default(),
        }]));
        let batch = Batch::new(
            schema.clone(),
            vec![ColumnVector::from_values(vec![
                Value::String(" Abé ".into()),
                Value::String("汉字".into()),
                Value::Null,
            ])],
        );
        for name in ["LOWER", "UPPER", "LTRIM", "RTRIM", "REVERSE"] {
            let expr = Expr::Function {
                name: name.into(),
                args: vec![Expr::Identifier("s".into())],
                distinct: false,
            };
            let program = compile_for_batch(&expr, &schema).unwrap().unwrap();
            let oracle = eval_value_per_row(&program, &batch).unwrap();
            let result = eval_to_column(&program, &batch).unwrap();
            assert!(matches!(result, ColumnVector::Utf8 { .. }), "{name}");
            assert_eq!(
                (0..batch.num_rows())
                    .map(|row| result.value_at(row))
                    .collect::<Vec<_>>(),
                oracle,
                "{name}"
            );
        }
    }

    #[test]
    fn regexp_replace_const_projection_stays_in_utf8_lane() {
        let schema = Arc::new(BatchSchema::new(vec![BatchField {
            table: "t".into(),
            alias: None,
            name: "s".into(),
            column_type: ColumnType::VarString,
            source: Default::default(),
        }]));
        let batch = Batch::new(
            schema.clone(),
            vec![ColumnVector::from_values(vec![
                Value::String("https://www.example.com/path".into()),
                Value::String("http://other.test/a".into()),
                Value::String("plain".into()),
                Value::Null,
                Value::String("https://www.example.com/path".into()),
                Value::String("https://www.example.com/path".into()),
            ])],
        );
        let selected = batch
            .clone()
            .with_selection(Selection::Indices(vec![0, 2, 3, 4, 5]));
        for (pattern, replacement) in [
            (r"^https?://(?:www\.)?([^/]+)/.*$", "$1"),
            (r"^https?://(?:www\.)?([^/]+)/.*$", "url"),
        ] {
            let expr = Expr::Function {
                name: "REGEXP_REPLACE".into(),
                args: vec![
                    Expr::Identifier("s".into()),
                    Expr::Literal(crate::parser::Literal::String(pattern.into())),
                    Expr::Literal(crate::parser::Literal::String(replacement.into())),
                ],
                distinct: false,
            };
            let program = compile_for_batch(&expr, &schema).unwrap().unwrap();
            assert!(!program.capability().legacy_escape);
            assert!(!program.capability().may_error);
            for input in [&batch, &selected] {
                let oracle = eval_value_per_row(&program, input).unwrap();
                let result = eval_to_column(&program, input).unwrap();
                assert!(matches!(result, ColumnVector::Utf8 { .. }));
                assert_eq!(
                    (0..input.num_rows())
                        .map(|row| result.value_at(row))
                        .collect::<Vec<_>>(),
                    oracle,
                    "pattern={pattern:?} replacement={replacement:?}"
                );
            }
        }
    }

    #[test]
    fn general_projection_reuses_exact_numeric_batch_ir() {
        let schema = Arc::new(BatchSchema::new(
            ["a", "b"]
                .into_iter()
                .map(|name| BatchField {
                    table: "t".into(),
                    alias: None,
                    name: name.into(),
                    column_type: ColumnType::Decimal {
                        precision: 12,
                        scale: 2,
                    },
                    source: Default::default(),
                })
                .collect(),
        ));
        let batch = Batch::new(
            schema.clone(),
            vec![
                ColumnVector::Decimal64 {
                    data: vec![100, 250, -300],
                    scale: 2,
                    nulls: None,
                },
                ColumnVector::Decimal64 {
                    data: vec![20, -50, 400],
                    scale: 2,
                    nulls: None,
                },
            ],
        );
        let expr = Expr::Binary {
            left: Box::new(Expr::Identifier("a".into())),
            op: BinaryOp::Mul,
            right: Box::new(Expr::Identifier("b".into())),
        };
        let program = compile_for_batch(&expr, &schema).unwrap().unwrap();
        let oracle = eval_value_per_row(&program, &batch).unwrap();
        let result = eval_to_column(&program, &batch).unwrap();
        assert!(matches!(result, ColumnVector::Decimal128 { .. }));
        assert_eq!(
            (0..batch.num_rows())
                .map(|row| result.value_at(row))
                .collect::<Vec<_>>(),
            oracle
        );
    }

    #[test]
    fn eval_respects_selection_and_preserves_physical_alignment() {
        let s = schema();
        let col = ColumnVector::from_values(vec![Value::Int(10), Value::Int(20), Value::Int(30)]);
        let b = Batch::new(s.clone(), vec![col]).with_selection(Selection::Indices(vec![1]));
        let expr = Expr::Identifier("x".into());
        let program = compile_for_batch(&expr, &s).unwrap().expect("compiles");

        assert_eq!(
            eval_value_per_row(&program, &b).unwrap(),
            vec![Value::Null, Value::Int(20), Value::Null]
        );
    }

    #[test]
    fn predicate_fast_path_respects_selection() {
        use crate::parser::{BinaryOp, Literal};

        let s = schema();
        let col = ColumnVector::from_values(vec![Value::Int(1), Value::Int(5), Value::Int(10)]);
        let b = Batch::new(s.clone(), vec![col]).with_selection(Selection::Indices(vec![0]));
        let expr = Expr::Binary {
            left: Box::new(Expr::Identifier("x".into())),
            op: BinaryOp::Gt,
            right: Box::new(Expr::Literal(Literal::Int(2))),
        };
        let program = compile_for_batch(&expr, &s).unwrap().expect("compiles");
        let bits = eval_predicate_bitmap(&program, &b).unwrap();

        assert_eq!(bits.count_ones(), 0);
        assert!(!bits.get(0));
        assert!(!bits.get(1));
        assert!(!bits.get(2));
    }

    #[test]
    fn bytes_text_column_constant_uses_typed_predicate_lane() {
        use crate::parser::{BinaryOp, Literal};

        let schema = Arc::new(BatchSchema::new(vec![BatchField {
            table: "t".into(),
            alias: None,
            name: "day_name".into(),
            column_type: ColumnType::Blob,
            source: Default::default(),
        }]));
        let batch = Batch::new(
            schema.clone(),
            vec![ColumnVector::Bytes {
                offsets: vec![0, 6, 12, 18],
                data: b"SundayMondayFriday".to_vec(),
                nulls: None,
            }],
        );
        let expr = Expr::Binary {
            left: Box::new(Expr::Identifier("day_name".into())),
            op: BinaryOp::Eq,
            right: Box::new(Expr::Literal(Literal::String("Monday".into()))),
        };
        let program = compile_for_batch(&expr, &schema)
            .unwrap()
            .expect("comparison compiles");
        let typed = try_column_const_cmp_bitmap(&program, &batch)
            .expect("text stored in a Bytes lane must keep the typed comparison");
        let oracle = eval_value_per_row(&program, &batch).unwrap();
        for (row, value) in oracle.iter().enumerate() {
            assert_eq!(typed.get(row), crate::engine::expr::truthy(value));
        }
        assert!(!typed.get(0));
        assert!(typed.get(1));
        assert!(!typed.get(2));

        let generic = Batch::new(
            schema,
            vec![ColumnVector::Generic {
                values: vec![
                    Value::Bytes(b"Sunday".to_vec()),
                    Value::String("Monday".into()),
                    Value::Null,
                ],
            }],
        );
        let typed = try_column_const_cmp_bitmap_parts(
            0,
            BinaryOp::Eq,
            &Value::String("Monday".into()),
            None,
            &generic,
        )
        .expect("mixed text representations in Generic must keep the typed comparison");
        assert!(!typed.get(0));
        assert!(typed.get(1));
        assert!(!typed.get(2));
    }

    #[test]
    fn empty_string_typed_predicate_matches_collation_oracle() {
        use crate::parser::{BinaryOp, Literal};

        for collation_name in [
            "utf8mb4_0900_ai_ci",
            "utf8mb4_general_ci",
            "utf8mb4_0900_bin",
        ] {
            let collation_id = crate::charset::resolve_collation(collation_name)
                .unwrap()
                .id;
            let schema = Arc::new(BatchSchema::new(vec![BatchField {
                table: "t".into(),
                alias: None,
                name: "v".into(),
                column_type: ColumnType::VarString,
                source: crate::types::ColumnSource {
                    collation_id: Some(collation_id),
                    ..Default::default()
                },
            }]));
            let batch = Batch::new(
                schema.clone(),
                vec![ColumnVector::from_values(vec![
                    Value::String("".into()),
                    Value::String(" ".into()),
                    Value::String("\0".into()),
                    Value::String("\u{0301}".into()),
                    Value::String("é".into()),
                    Value::String("中文".into()),
                    Value::Null,
                ])],
            );
            for op in [BinaryOp::Eq, BinaryOp::NotEq] {
                let expr = Expr::Binary {
                    left: Box::new(Expr::Identifier("v".into())),
                    op,
                    right: Box::new(Expr::Literal(Literal::String(String::new()))),
                };
                let program = compile_for_batch(&expr, &schema)
                    .unwrap()
                    .expect("empty string comparison compiles");
                let typed = try_column_const_cmp_bitmap(&program, &batch)
                    .expect("empty string comparison keeps the typed lane");
                let oracle = eval_value_per_row(&program, &batch).unwrap();
                for (row, value) in oracle.iter().enumerate() {
                    assert_eq!(
                        typed.get(row),
                        crate::engine::expr::truthy(value),
                        "empty string predicate drift for {collation_name}, {op:?}, row {row}"
                    );
                }
            }
        }
    }

    #[test]
    fn temporal_column_string_constant_uses_typed_predicate_lane() {
        use crate::parser::{BinaryOp, Literal};

        fn assert_typed_matches_oracle(schema: &Arc<BatchSchema>, batch: &Batch, literal: &str) {
            let expr = Expr::Binary {
                left: Box::new(Expr::Identifier("v".into())),
                op: BinaryOp::Gt,
                right: Box::new(Expr::Literal(Literal::String(literal.into()))),
            };
            let program = compile_for_batch(&expr, schema)
                .unwrap()
                .expect("temporal comparison compiles");
            let typed = try_column_const_cmp_bitmap(&program, batch)
                .expect("valid temporal string must select typed lane");
            let oracle_values = eval_value_per_row(&program, batch).unwrap();
            for (row, value) in oracle_values.iter().enumerate() {
                assert_eq!(
                    typed.get(row),
                    crate::engine::expr::truthy(value),
                    "temporal predicate mismatch at row {row}"
                );
            }
        }

        let date_schema = Arc::new(BatchSchema::new(vec![BatchField {
            table: "t".into(),
            alias: None,
            name: "v".into(),
            column_type: ColumnType::Date,
            source: Default::default(),
        }]));
        let cutoff = crate::storage::parse_date_to_days_bytes(b"1995-03-15").unwrap();
        let mut date_valid = Bitmap::new_all_valid(4);
        date_valid.set(3, false);
        let date_batch = Batch::new(
            date_schema.clone(),
            vec![ColumnVector::Date32 {
                data: vec![cutoff - 1, cutoff, cutoff + 1, cutoff + 2],
                nulls: Some(date_valid),
            }],
        );
        assert_typed_matches_oracle(&date_schema, &date_batch, "1995-03-15");
        let parameterized = Expr::Binary {
            left: Box::new(Expr::Identifier("v".into())),
            op: BinaryOp::Gt,
            right: Box::new(Expr::Placeholder(0)),
        };
        let parameterized = compile_for_batch(&parameterized, &date_schema)
            .unwrap()
            .expect("parameterized temporal comparison compiles");
        crate::engine::params::with_auto_params(vec![Value::String("1995-03-15".into())], || {
            let typed = try_column_const_cmp_bitmap(&parameterized, &date_batch)
                .expect("bound temporal parameter must select typed lane");
            let oracle = eval_value_per_row(&parameterized, &date_batch).unwrap();
            for (row, value) in oracle.iter().enumerate() {
                assert_eq!(typed.get(row), crate::engine::expr::truthy(value));
            }
        });

        let datetime_schema = Arc::new(BatchSchema::new(vec![BatchField {
            table: "t".into(),
            alias: None,
            name: "v".into(),
            column_type: ColumnType::DateTime,
            source: Default::default(),
        }]));
        let cutoff = crate::storage::parse_datetime_to_micros("1995-03-15 12:30:00").unwrap();
        let datetime_batch = Batch::new(
            datetime_schema.clone(),
            vec![ColumnVector::DateTime64 {
                data: vec![cutoff - 1, cutoff, cutoff + 1],
                nulls: None,
            }],
        );
        assert_typed_matches_oracle(&datetime_schema, &datetime_batch, "1995-03-15 12:30:00");

        let timestamp_schema = Arc::new(BatchSchema::new(vec![BatchField {
            table: "t".into(),
            alias: None,
            name: "v".into(),
            column_type: ColumnType::Timestamp,
            source: Default::default(),
        }]));
        let Value::Timestamp(timestamp_cutoff) =
            crate::engine::expr::coerce_comparison_constant_for_type(
                &Value::String("1995-03-15 12:30:00".into()),
                &crate::storage::DataTypeKind::Timestamp { fsp: 0 },
            )
            .expect("valid local timestamp literal")
        else {
            panic!("timestamp coercion returned the wrong lane")
        };
        let timestamp_batch = Batch::new(
            timestamp_schema.clone(),
            vec![ColumnVector::Timestamp64 {
                data: vec![timestamp_cutoff - 1, timestamp_cutoff, timestamp_cutoff + 1],
                nulls: None,
            }],
        );
        assert_typed_matches_oracle(&timestamp_schema, &timestamp_batch, "1995-03-15 12:30:00");

        // STR_TO_DATE folds to a DATE Value. A TIMESTAMP column comparison
        // must convert that local midnight to UTC once and retain the typed
        // i64 lane instead of evaluating the predicate row by row.
        let date_days = crate::storage::parse_date_to_days_bytes(b"1995-03-15").unwrap();
        let date_bound = Expr::Binary {
            left: Box::new(Expr::Identifier("v".into())),
            op: BinaryOp::Gt,
            right: Box::new(Expr::BoundValue(Value::Date(date_days))),
        };
        let date_bound = compile_for_batch(&date_bound, &timestamp_schema)
            .unwrap()
            .expect("timestamp/date comparison compiles");
        let typed = try_column_const_cmp_bitmap(&date_bound, &timestamp_batch)
            .expect("folded DATE bound must select the timestamp lane");
        let oracle = eval_value_per_row(&date_bound, &timestamp_batch).unwrap();
        for (row, value) in oracle.iter().enumerate() {
            assert_eq!(typed.get(row), crate::engine::expr::truthy(value));
        }

        let time_schema = Arc::new(BatchSchema::new(vec![BatchField {
            table: "t".into(),
            alias: None,
            name: "v".into(),
            column_type: ColumnType::Time,
            source: Default::default(),
        }]));
        let cutoff = crate::storage::parse_time_to_micros("12:30:00").unwrap();
        let time_batch = Batch::new(
            time_schema.clone(),
            vec![ColumnVector::Time64 {
                data: vec![cutoff - 1, cutoff, cutoff + 1],
                nulls: None,
            }],
        );
        assert_typed_matches_oracle(&time_schema, &time_batch, "12:30:00");
    }

    #[test]
    fn predicate_bitmap_fast_path_matches_row_oracle() {
        use crate::parser::{BinaryOp, Literal};

        // Row-by-row oracle: materialise Values then apply `truthy`, exactly
        // the slow path `eval_predicate_bitmap` falls back to. The fast path
        // must produce an identical bitmap.
        fn oracle(program: &ExprProgram, batch: &Batch) -> Bitmap {
            let mut bits = Bitmap::new_all_invalid(batch.num_rows());
            for (i, v) in eval_value_per_row(program, batch)
                .unwrap()
                .iter()
                .enumerate()
            {
                if crate::engine::expr::truthy(v) {
                    bits.set_true(i);
                }
            }
            bits
        }
        fn assert_same(a: &Bitmap, b: &Bitmap) {
            assert_eq!(a.len(), b.len());
            for i in 0..a.len() {
                assert_eq!(a.get(i), b.get(i), "bitmap mismatch at row {i}");
            }
        }

        // I64 lane with a NULL at row 2.
        let i64_schema = Arc::new(BatchSchema::new(vec![BatchField {
            table: "t".into(),
            alias: None,
            name: "x".into(),
            column_type: ColumnType::LongLong,
            source: Default::default(),
        }]));
        let mut valid = Bitmap::new_all_valid(5);
        valid.set(2, false);
        let i64_col = ColumnVector::I64 {
            data: vec![1, 5, 7, 4, 10],
            nulls: Some(valid),
        };
        let i64_batch = Batch::new(i64_schema.clone(), vec![i64_col]);

        let col = || Box::new(Expr::Identifier("x".into()));
        let int = |v: i64| Box::new(Expr::Literal(Literal::Int(v)));
        let cases = [
            BinaryOp::Eq,
            BinaryOp::NotEq,
            BinaryOp::Lt,
            BinaryOp::LtEq,
            BinaryOp::Gt,
            BinaryOp::GtEq,
        ];
        for op in cases {
            // col <op> const
            let e = Expr::Binary {
                left: col(),
                op,
                right: int(5),
            };
            let p = compile_for_batch(&e, &i64_schema)
                .unwrap()
                .expect("compiles");
            assert_same(
                &eval_predicate_bitmap(&p, &i64_batch).unwrap(),
                &oracle(&p, &i64_batch),
            );
            // flipped: const <op> col
            let e = Expr::Binary {
                left: int(5),
                op,
                right: col(),
            };
            let p = compile_for_batch(&e, &i64_schema)
                .unwrap()
                .expect("compiles");
            assert_same(
                &eval_predicate_bitmap(&p, &i64_batch).unwrap(),
                &oracle(&p, &i64_batch),
            );
        }
        // col <cmp> NULL -> no row truthy (NULL row stays unset too).
        let e = Expr::Binary {
            left: col(),
            op: BinaryOp::Gt,
            right: Box::new(Expr::Literal(Literal::Null)),
        };
        let p = compile_for_batch(&e, &i64_schema)
            .unwrap()
            .expect("compiles");
        assert_eq!(
            eval_predicate_bitmap(&p, &i64_batch).unwrap().count_ones(),
            0
        );

        // Boundary i64 values (MIN/MAX/0/negative) against several constants
        // still match the oracle exactly — the integer lane is fast-pathed.
        let edge = ColumnVector::I64 {
            data: vec![i64::MIN, -1, 0, 1, i64::MAX],
            nulls: None,
        };
        let edge_batch = Batch::new(i64_schema.clone(), vec![edge]);
        for rhs in [i64::MIN, 0, i64::MAX] {
            for op in cases {
                let e = Expr::Binary {
                    left: col(),
                    op,
                    right: int(rhs),
                };
                let p = compile_for_batch(&e, &i64_schema)
                    .unwrap()
                    .expect("compiles");
                assert_same(
                    &eval_predicate_bitmap(&p, &edge_batch).unwrap(),
                    &oracle(&p, &edge_batch),
                );
            }
        }
    }

    #[test]
    fn utf8_comparison_batch_kernel_matches_row_oracle_for_all_operators() {
        use crate::parser::{BinaryOp, Literal};

        let s = Arc::new(BatchSchema::new(vec![BatchField {
            table: "t".into(),
            alias: None,
            name: "pad".into(),
            column_type: ColumnType::VarString,
            source: Default::default(),
        }]));
        let mut valid = Bitmap::new_all_valid(6);
        valid.set(5, false);
        let col = ColumnVector::from_values(vec![
            Value::String("Alpha".into()),
            Value::String("ALPHA ".into()),
            Value::String("beta".into()),
            Value::String("98996621624-36689827414".into()),
            Value::String("zeta".into()),
            Value::Null,
        ]);
        let col = match col {
            ColumnVector::Utf8 { offsets, data, .. } => ColumnVector::Utf8 {
                offsets,
                data,
                nulls: Some(valid),
            },
            other => panic!("expected Utf8 lane, got {other:?}"),
        };
        let batch = Batch::new(s.clone(), vec![col]);

        for op in [
            BinaryOp::Eq,
            BinaryOp::NotEq,
            BinaryOp::Lt,
            BinaryOp::LtEq,
            BinaryOp::Gt,
            BinaryOp::GtEq,
        ] {
            let expr = Expr::Binary {
                left: Box::new(Expr::Identifier("pad".into())),
                op,
                right: Box::new(Expr::Literal(Literal::String("alpha".into()))),
            };
            let program = compile_for_batch(&expr, &s).unwrap().expect("compiles");
            let before = vec_string_cmp_rows_processed();
            let native = eval_predicate_bitmap(&program, &batch).unwrap();
            assert!(vec_string_cmp_rows_processed() >= before + batch.num_rows() as u64);
            let values = eval_value_per_row(&program, &batch).unwrap();
            for (row, value) in values.iter().enumerate() {
                assert_eq!(
                    native.get(row),
                    crate::engine::expr::truthy(value),
                    "operator {op:?}, row {row}"
                );
            }
        }

        let parameterized = Expr::Binary {
            left: Box::new(Expr::Identifier("pad".into())),
            op: BinaryOp::Eq,
            right: Box::new(Expr::Placeholder(0)),
        };
        let program = compile_for_batch(&parameterized, &s)
            .unwrap()
            .expect("parameterized string comparison compiles");
        crate::engine::params::with_auto_params(vec![Value::String("alpha".into())], || {
            let before = vec_string_cmp_rows_processed();
            let native = eval_predicate_bitmap(&program, &batch).unwrap();
            assert!(vec_string_cmp_rows_processed() >= before + batch.num_rows() as u64);
            let oracle = eval_value_per_row(&program, &batch).unwrap();
            for (row, value) in oracle.iter().enumerate() {
                assert_eq!(native.get(row), crate::engine::expr::truthy(value));
            }
        });
        crate::engine::params::with_auto_params(vec![Value::Int(0)], || {
            assert!(
                try_column_const_cmp_bitmap(&program, &batch).is_none(),
                "string/numeric dynamic comparison must retain scalar coercion"
            );
        });
    }

    #[test]
    fn utf8_in_const_set_batch_kernel_matches_scalar_null_and_pad_space_semantics() {
        use crate::parser::Literal;

        let s = Arc::new(BatchSchema::new(vec![BatchField {
            table: "t".into(),
            alias: None,
            name: "container".into(),
            column_type: ColumnType::VarString,
            source: Default::default(),
        }]));
        let batch = Batch::new(
            s.clone(),
            vec![ColumnVector::from_values(vec![
                Value::String("SM CASE".into()),
                Value::String("SM CASE   ".into()),
                Value::String("sm box".into()),
                Value::String("OTHER".into()),
                Value::Null,
            ])],
        );
        for negated in [false, true] {
            let expr = Expr::InList {
                expr: Box::new(Expr::Identifier("container".into())),
                list: ["SM CASE", "SM BOX", "SM PACK", "SM PKG"]
                    .into_iter()
                    .map(|value| Expr::Literal(Literal::String(value.into())))
                    .chain(std::iter::once(Expr::Literal(Literal::Null)))
                    .collect(),
                negated,
            };
            let program = compile_for_batch(&expr, &s)
                .unwrap()
                .expect("constant string IN compiles");
            let native = try_column_string_in_const_set_truth_mask(&program, &batch)
                .expect("UTF-8 InConstSet uses the native lane");
            let oracle = eval_value_per_row(&program, &batch).unwrap();
            for (row, value) in oracle.iter().enumerate() {
                match value {
                    Value::Null => {
                        assert!(!native.truth.get(row));
                        assert!(native.null.get(row));
                    }
                    value => {
                        assert_eq!(native.truth.get(row), crate::engine::expr::truthy(value));
                        assert!(!native.null.get(row));
                    }
                }
            }

            let batch_program = BatchExprProgram::compile(&expr, &s)
                .unwrap()
                .expect("batch IN compiles");
            let selected = batch_program.eval_predicate_bitmap(&batch).unwrap();
            for (row, value) in oracle.iter().enumerate() {
                assert_eq!(selected.get(row), crate::engine::expr::truthy(value));
            }
            if !matches!(
                crate::engine::executor::vec::config::effective_mode(),
                crate::engine::executor::vec::config::VecMode::Off
            ) {
                assert_eq!(
                    batch_program.expression_rows(),
                    (batch.num_rows() as u64, 0)
                );
            }
        }
    }

    #[test]
    fn substring_utf8_in_const_set_batch_kernel_matches_scalar_oracle() {
        use crate::parser::Literal;

        let schema = Arc::new(BatchSchema::new(vec![BatchField {
            table: "t".into(),
            alias: None,
            name: "phone".into(),
            column_type: ColumnType::VarString,
            source: Default::default(),
        }]));
        let mut visible = Bitmap::new_all_invalid(7);
        for row in [0, 1, 2, 4, 5, 6] {
            visible.set_true(row);
        }
        let batch = Batch::new(
            schema.clone(),
            vec![ColumnVector::from_values(vec![
                Value::String("13-555".into()),
                Value::String("汉字-alpha".into()),
                Value::String("A ".into()),
                Value::String("31-hidden".into()),
                Value::String("zz-tail".into()),
                Value::String("".into()),
                Value::Null,
            ])],
        )
        .with_selection(Selection::Mask(visible));

        for (start, len, constants) in [
            (1, Some(2), vec!["13", "汉字", "a", "ZZ", "unused"]),
            (-4, Some(2), vec!["55", "ph", "A ", "ta", "unused"]),
        ] {
            for negated in [false, true] {
                let expr = Expr::InList {
                    expr: Box::new(Expr::Function {
                        name: "SUBSTRING".into(),
                        args: vec![
                            Expr::Identifier("phone".into()),
                            Expr::Literal(Literal::Int(start)),
                            Expr::Literal(Literal::Int(len.expect("test length"))),
                        ],
                        distinct: false,
                    }),
                    list: constants
                        .iter()
                        .map(|value| Expr::Literal(Literal::String((*value).into())))
                        .chain(std::iter::once(Expr::Literal(Literal::Null)))
                        .collect(),
                    negated,
                };
                let program = compile_for_batch(&expr, &schema)
                    .unwrap()
                    .expect("SUBSTRING IN compiles");
                let native = try_substring_column_string_in_const_set_truth_mask(&program, &batch)
                    .expect("SUBSTRING over UTF-8 uses the native lane");
                let oracle = eval_value_per_row(&program, &batch).unwrap();
                for (row, value) in oracle.iter().enumerate() {
                    match value {
                        Value::Null => {
                            assert!(!native.truth.get(row));
                            if batch.visible_indices().any(|visible| visible == row) {
                                assert!(native.null.get(row));
                            }
                        }
                        value => {
                            assert_eq!(
                                native.truth.get(row),
                                crate::engine::expr::truthy(value),
                                "row {row}, start {start}, negated {negated}"
                            );
                            assert!(!native.null.get(row));
                        }
                    }
                }

                let batch_program = BatchExprProgram::compile(&expr, &schema)
                    .unwrap()
                    .expect("batch SUBSTRING IN compiles");
                let selected = batch_program.eval_predicate_bitmap(&batch).unwrap();
                for (row, value) in oracle.iter().enumerate() {
                    assert_eq!(selected.get(row), crate::engine::expr::truthy(value));
                }
                if !matches!(
                    crate::engine::executor::vec::config::effective_mode(),
                    crate::engine::executor::vec::config::VecMode::Off
                ) {
                    assert_eq!(
                        batch_program.expression_rows(),
                        (batch.visible_rows() as u64, 0)
                    );
                }
            }
        }
    }

    #[test]
    fn integer_in_const_set_batch_kernel_matches_scalar_and_selection_semantics() {
        let schema = Arc::new(BatchSchema::new(vec![BatchField {
            table: "t".into(),
            alias: None,
            name: "k".into(),
            column_type: ColumnType::LongLong,
            source: Default::default(),
        }]));
        let mut selection = Bitmap::new_all_invalid(6);
        for row in [0, 1, 3, 5] {
            selection.set_true(row);
        }
        let batch = Batch::new(
            schema.clone(),
            vec![ColumnVector::from_values(vec![
                Value::Int(-1),
                Value::Int(2),
                Value::Int(3),
                Value::Int(4),
                Value::Int(99),
                Value::Null,
            ])],
        )
        .with_selection(Selection::Mask(selection));
        let expr = Expr::InList {
            expr: Box::new(Expr::Identifier("k".into())),
            list: vec![
                Expr::BoundValue(Value::Int(-1)),
                Expr::BoundValue(Value::UInt(2)),
                Expr::BoundValue(Value::Int(7)),
                Expr::BoundValue(Value::Null),
            ],
            negated: false,
        };
        let program = BatchExprProgram::compile(&expr, &schema)
            .unwrap()
            .expect("constant integer IN compiles");
        let selected = program.eval_predicate_bitmap(&batch).unwrap();
        let oracle_program = ExprCompiler::compile(&expr, &batch_schema_to_input(&schema)).unwrap();
        let oracle = eval_value_per_row(&oracle_program, &batch).unwrap();
        for (row, value) in oracle.iter().enumerate() {
            assert_eq!(selected.get(row), crate::engine::expr::truthy(value));
        }
        if !matches!(
            crate::engine::executor::vec::config::effective_mode(),
            crate::engine::executor::vec::config::VecMode::Off
        ) {
            assert_eq!(program.expression_rows(), (4, 0));
        }
    }

    #[test]
    fn hybrid_batch_program_keeps_three_value_logic_with_local_fallback() {
        use crate::parser::{BinaryOp, Literal, UnaryOp};

        let s = Arc::new(BatchSchema::new(vec![BatchField {
            table: "t".into(),
            alias: None,
            name: "pad".into(),
            column_type: ColumnType::VarString,
            source: Default::default(),
        }]));
        let batch = Batch::new(
            s.clone(),
            vec![ColumnVector::from_values(vec![
                Value::String("alpha".into()),
                Value::String("beta".into()),
                Value::String("toolong".into()),
                Value::Null,
            ])],
        );
        let column = || Box::new(Expr::Identifier("pad".into()));
        let eq_alpha = Expr::Binary {
            left: column(),
            op: BinaryOp::Eq,
            right: Box::new(Expr::Literal(Literal::String("alpha".into()))),
        };
        let expr = Expr::Binary {
            left: Box::new(eq_alpha),
            op: BinaryOp::Or,
            right: Box::new(Expr::Unary {
                op: UnaryOp::Not,
                // IS NULL is deliberately not a typed leaf yet, so it proves
                // that one safe long-tail leaf can fall back locally.
                expr: Box::new(Expr::IsNull(column())),
            }),
        };
        let hybrid = BatchExprProgram::compile(&expr, &s)
            .unwrap()
            .expect("hybrid predicate compiles");
        let actual = hybrid.eval_predicate_bitmap(&batch).unwrap();
        let oracle_program = compile_for_batch(&expr, &s).unwrap().unwrap();
        let oracle = eval_value_per_row(&oracle_program, &batch).unwrap();
        for (row, value) in oracle.iter().enumerate() {
            assert_eq!(actual.get(row), crate::engine::expr::truthy(value));
        }
        let (native, fallback) = hybrid.expression_rows();
        assert_eq!(native, batch.num_rows() as u64);
        // The first row is decided by the left side of OR. The fallback must
        // observe the right-side short circuit and evaluate only three rows.
        assert_eq!(fallback, batch.num_rows() as u64 - 1);
    }

    #[test]
    fn hybrid_batch_program_keeps_infallible_length_on_native_path() {
        use crate::parser::{BinaryOp, Literal};

        let s = Arc::new(BatchSchema::new(vec![BatchField {
            table: "t".into(),
            alias: None,
            name: "pad".into(),
            column_type: ColumnType::VarString,
            source: Default::default(),
        }]));
        let batch = Batch::new(
            s.clone(),
            vec![ColumnVector::from_values(vec![
                Value::String("alpha".into()),
                Value::String("beta".into()),
                Value::Null,
            ])],
        );
        let column = || Box::new(Expr::Identifier("pad".into()));
        let expr = Expr::Binary {
            left: Box::new(Expr::Binary {
                left: column(),
                op: BinaryOp::Eq,
                right: Box::new(Expr::Literal(Literal::String("alpha".into()))),
            }),
            op: BinaryOp::Or,
            right: Box::new(Expr::Binary {
                left: Box::new(Expr::Function {
                    name: "LENGTH".into(),
                    args: vec![*column()],
                    distinct: false,
                }),
                op: BinaryOp::Gt,
                right: Box::new(Expr::Literal(Literal::Int(4))),
            }),
        };
        let hybrid = BatchExprProgram::compile(&expr, &s)
            .unwrap()
            .expect("predicate compiles through the native scalar lane");
        let actual = hybrid.eval_predicate_bitmap(&batch).unwrap();
        let oracle_program = compile_for_batch(&expr, &s).unwrap().unwrap();
        let oracle = eval_value_per_row(&oracle_program, &batch).unwrap();
        for (row, value) in oracle.iter().enumerate() {
            assert_eq!(actual.get(row), crate::engine::expr::truthy(value));
        }
        // LENGTH with valid arity is intrinsically infallible. It no longer
        // forces the whole predicate into the legacy row oracle; the right
        // branch still observes OR short-circuiting for the first row, and
        // its remaining two rows now use the composable column kernel.
        assert_eq!(
            hybrid.expression_rows(),
            (2 * batch.num_rows() as u64 - 1, 0)
        );
    }

    #[test]
    fn cmp_i64_const_gt_marks_only_matching() {
        let a = vec![1i64, 5, 7, 2, 10];
        let mut b = Bitmap::new_all_invalid(a.len());
        cmp_i64_const_gt_simd(&a, 4, &mut b);
        assert!(!b.get(0));
        assert!(b.get(1));
        assert!(b.get(2));
        assert!(!b.get(3));
        assert!(b.get(4));
        assert_eq!(b.count_ones(), 3);
    }

    #[test]
    fn i64_const_kernels_match_scalar() {
        let a = vec![1i64, 5, 7, 2, 10, -3, 5, i64::MIN, i64::MAX, 0];
        let rhs = 5i64;
        let run = |f: &dyn Fn(&[i64], i64, &mut Bitmap)| {
            let mut b = Bitmap::new_all_invalid(a.len());
            f(&a, rhs, &mut b);
            (0..a.len()).map(|i| b.get(i)).collect::<Vec<_>>()
        };
        assert_eq!(
            run(&cmp_i64_const_lt_simd),
            a.iter().map(|x| *x < rhs).collect::<Vec<_>>()
        );
        assert_eq!(
            run(&cmp_i64_const_le_simd),
            a.iter().map(|x| *x <= rhs).collect::<Vec<_>>()
        );
        assert_eq!(
            run(&cmp_i64_const_ge_simd),
            a.iter().map(|x| *x >= rhs).collect::<Vec<_>>()
        );
        assert_eq!(
            run(&cmp_i64_const_ne_simd),
            a.iter().map(|x| *x != rhs).collect::<Vec<_>>()
        );
        // Empty slice: no bits set, no panic.
        let mut empty = Bitmap::new_all_invalid(0);
        cmp_i64_const_lt_simd(&[], 0, &mut empty);
        assert_eq!(empty.count_ones(), 0);
    }

    #[test]
    fn f64_const_kernels_match_scalar() {
        let a = vec![1.0f64, 5.0, 7.5, 2.25, -3.0, 5.0, f64::NAN, f64::INFINITY];
        let rhs = 5.0f64;
        let run = |f: &dyn Fn(&[f64], f64, &mut Bitmap)| {
            let mut b = Bitmap::new_all_invalid(a.len());
            f(&a, rhs, &mut b);
            (0..a.len()).map(|i| b.get(i)).collect::<Vec<_>>()
        };
        assert_eq!(
            run(&cmp_f64_const_eq_simd),
            a.iter().map(|x| *x == rhs).collect::<Vec<_>>()
        );
        assert_eq!(
            run(&cmp_f64_const_ne_simd),
            a.iter().map(|x| *x != rhs).collect::<Vec<_>>()
        );
        assert_eq!(
            run(&cmp_f64_const_lt_simd),
            a.iter().map(|x| *x < rhs).collect::<Vec<_>>()
        );
        assert_eq!(
            run(&cmp_f64_const_le_simd),
            a.iter().map(|x| *x <= rhs).collect::<Vec<_>>()
        );
        assert_eq!(
            run(&cmp_f64_const_gt_simd),
            a.iter().map(|x| *x > rhs).collect::<Vec<_>>()
        );
        assert_eq!(
            run(&cmp_f64_const_ge_simd),
            a.iter().map(|x| *x >= rhs).collect::<Vec<_>>()
        );
        // NaN compares false under every ordered operator (index 6).
        let mut lt = Bitmap::new_all_invalid(a.len());
        cmp_f64_const_lt_simd(&a, rhs, &mut lt);
        assert!(!lt.get(6));
    }

    #[test]
    fn add_i64_handles_overflow_via_wrapping() {
        let a = vec![i64::MAX, 1, 2];
        let b = vec![1i64, 1, 3];
        let mut out = vec![0i64; 3];
        add_i64_simd(&a, &b, &mut out);
        assert_eq!(out[0], i64::MAX.wrapping_add(1));
        assert_eq!(out[1], 2);
        assert_eq!(out[2], 5);
    }

    #[test]
    fn sum_i64_unrolled_matches_scalar() {
        let a: Vec<i64> = (1..=1000).collect();
        let want: i128 = a.iter().map(|x| *x as i128).sum();
        assert_eq!(sum_i64_simd(&a), want);
        assert_eq!(want, 500_500);
    }

    #[test]
    fn sub_const_lhs_q1_disc_kernel() {
        // Models `1 - l_discount` once the constant has been
        // scale-aligned to scale=2 (so const = 100).
        let disc = vec![5i64, 0, 10, 7, 100];
        let mut out = vec![0i64; 5];
        sub_const_i64_lhs(100, &disc, &mut out);
        assert_eq!(out, vec![95, 100, 90, 93, 0]);
    }

    #[test]
    fn add_const_q1_tax_kernel() {
        let tax = vec![8i64, 0, 100];
        let mut out = vec![0i64; 3];
        add_const_i64(100, &tax, &mut out);
        assert_eq!(out, vec![108, 100, 200]);
    }

    #[test]
    fn mul_i64_widen_overflows_safely_via_i128() {
        // i64::MAX * 2 overflows i64; in i128 it's well-defined.
        let a = vec![i64::MAX, 1_000_000_000_000_000i64];
        let b = vec![2i64, 1_000_000_000_000_000i64];
        let mut out = vec![0i128; 2];
        mul_i64_widen_to_i128(&a, &b, &mut out);
        assert_eq!(out[0], (i64::MAX as i128) * 2);
        assert_eq!(
            out[1],
            1_000_000_000_000_000i128 * 1_000_000_000_000_000i128
        );
    }

    #[test]
    fn mul_i128_i64_chained() {
        // Models `extprice * (1-disc) * (1+tax)`: i128 lane carries the
        // scale-4 product, multiplied with the scale-2 (1+tax) i64 lane.
        let prev = vec![10_000i128, 50_000i128, 0];
        let b = vec![108i64, 108, 108];
        let mut out = vec![0i128; 3];
        mul_i128_i64(&prev, &b, &mut out);
        assert_eq!(out, vec![1_080_000i128, 5_400_000i128, 0]);
    }

    #[test]
    fn merge_nulls_and_combines() {
        let mut left = Bitmap::new_all_valid(4);
        left.set(1, false);
        let mut right = Bitmap::new_all_valid(4);
        right.set(2, false);
        let merged = merge_nulls(Some(&left), Some(&right), 4).unwrap();
        assert!(merged.get(0));
        assert!(!merged.get(1));
        assert!(!merged.get(2));
        assert!(merged.get(3));
        // One-sided NULL clones the present bitmap.
        let only_left = merge_nulls(Some(&left), None, 4).unwrap();
        assert!(only_left.get(0));
        assert!(!only_left.get(1));
        assert!(only_left.get(2));
        // No NULLs anywhere keeps the lane lean.
        assert!(merge_nulls(None, None, 4).is_none());
    }

    #[test]
    fn temporal_extract_typed_matches_row_oracle() {
        use crate::engine::executor::vec::bitmap::Bitmap;

        let dt_schema = Arc::new(BatchSchema::new(vec![BatchField {
            table: "t".into(),
            alias: None,
            name: "dt".into(),
            column_type: ColumnType::DateTime,
            source: Default::default(),
        }]));
        // 2024-05-31 12:34:56 UTC = 1717158896 epoch secs; plus a NULL
        // slot and the zero-datetime sentinel.
        let micros: Vec<i64> = vec![
            1_717_158_896_000_000,
            0, // 1970-01-01 00:00:00
            crate::types::ZERO_DATETIME,
            1_717_158_896_000_000, // NULL slot (bitmap below)
        ];
        let mut nulls = Bitmap::new_all_valid(4);
        nulls.set(3, false);
        let batch = Batch::new(
            dt_schema.clone(),
            vec![ColumnVector::DateTime64 {
                data: micros,
                nulls: Some(nulls),
            }],
        );

        for func in &[
            "YEAR", "MONTH", "DAY", "HOUR", "MINUTE", "SECOND", "QUARTER",
        ] {
            let expr = Expr::Function {
                name: (*func).to_string(),
                args: vec![Expr::Identifier("dt".into())],
                distinct: false,
            };
            let program = compile_for_batch(&expr, &dt_schema)
                .unwrap()
                .expect("temporal extraction compiles");
            assert!(
                program.as_temporal_extract().is_some(),
                "{func}(dt) should be recognised as a typed extraction"
            );
            // Typed lane output…
            let typed = eval_value_per_row(&program, &batch).unwrap();
            // …must equal the row-by-row oracle (forced by evaluating
            // each row through the program directly).
            for r in 0..batch.num_rows() {
                let row_values: Vec<Value> =
                    batch.columns().iter().map(|c| c.value_at(r)).collect();
                let view = RowView::from_values(&row_values);
                let oracle = with_eval_scratch(|s| program.eval_row(&view, s)).unwrap();
                assert_eq!(typed[r], oracle, "{func} row {r}");
            }
        }

        // TIMESTAMP stores UTC micros and extracts in the session time zone.
        // The batch kernel must resolve that zone once without drifting from
        // the scalar Value::Timestamp oracle (including NULL and zero).
        let ts_schema = Arc::new(BatchSchema::new(vec![BatchField {
            table: "t".into(),
            alias: None,
            name: "ts".into(),
            column_type: ColumnType::Timestamp,
            source: Default::default(),
        }]));
        let ts_batch = Batch::new(
            ts_schema.clone(),
            vec![ColumnVector::Timestamp64 {
                data: vec![
                    1_717_158_896_000_000,
                    crate::types::ZERO_DATETIME,
                    1_717_158_896_000_000,
                ],
                nulls: Some({
                    let mut valid = Bitmap::new_all_valid(3);
                    valid.set(2, false);
                    valid
                }),
            }],
        );
        let extract_minute = Expr::Function {
            name: "EXTRACT".to_string(),
            args: vec![
                Expr::Literal(crate::parser::Literal::String("MINUTE".into())),
                Expr::Identifier("ts".into()),
            ],
            distinct: false,
        };
        let ts_program = compile_for_batch(&extract_minute, &ts_schema)
            .unwrap()
            .expect("timestamp EXTRACT compiles");
        let typed = eval_value_per_row(&ts_program, &ts_batch).unwrap();
        for row in 0..ts_batch.num_rows() {
            let values = vec![ts_batch.columns()[0].value_at(row)];
            let view = RowView::from_values(&values);
            let oracle = with_eval_scratch(|scratch| ts_program.eval_row(&view, scratch)).unwrap();
            assert_eq!(typed[row], oracle, "timestamp minute row {row}");
        }

        // Spot-check absolute values: zero sentinel → 0, NULL stays NULL.
        let expr = Expr::Function {
            name: "YEAR".to_string(),
            args: vec![Expr::Identifier("dt".into())],
            distinct: false,
        };
        let program = compile_for_batch(&expr, &dt_schema).unwrap().unwrap();
        let typed = eval_value_per_row(&program, &batch).unwrap();
        assert_eq!(typed[0], Value::Int(2024));
        assert_eq!(typed[1], Value::Int(1970));
        assert_eq!(typed[2], Value::Int(0));
        assert_eq!(typed[3], Value::Null);
    }

    #[test]
    fn temporal_extract_typed_on_date32_lane() {
        let d_schema = Arc::new(BatchSchema::new(vec![BatchField {
            table: "t".into(),
            alias: None,
            name: "d".into(),
            column_type: ColumnType::Date,
            source: Default::default(),
        }]));
        // 19874 days = 2024-05-31; ZERO_DATE sentinel extracts 0.
        let batch = Batch::new(
            d_schema.clone(),
            vec![ColumnVector::Date32 {
                data: vec![19_874, 0, crate::types::ZERO_DATE],
                nulls: None,
            }],
        );
        let expr = Expr::Function {
            name: "MONTH".to_string(),
            args: vec![Expr::Identifier("d".into())],
            distinct: false,
        };
        let program = compile_for_batch(&expr, &d_schema).unwrap().unwrap();
        let typed = eval_value_per_row(&program, &batch).unwrap();
        assert_eq!(typed, vec![Value::Int(5), Value::Int(1), Value::Int(0)]);
        let ColumnVector::I64 { data, .. } =
            try_eval_infallible_typed_column(&program, &batch).unwrap()
        else {
            panic!("typed temporal precompute must produce an integer lane")
        };
        assert_eq!(data, [5, 1, 0]);

        let extract = Expr::Function {
            name: "EXTRACT".to_string(),
            args: vec![
                Expr::Literal(crate::parser::Literal::String("YEAR".into())),
                Expr::Identifier("d".into()),
            ],
            distinct: false,
        };
        let extract_program = compile_for_batch(&extract, &d_schema).unwrap().unwrap();
        assert_eq!(
            extract_program.as_temporal_extract(),
            Some((TemporalExtractKind::Year, 0))
        );
        let ColumnVector::I64 { data, .. } =
            try_eval_infallible_typed_column(&extract_program, &batch).unwrap()
        else {
            panic!("EXTRACT(YEAR FROM date) must use the typed temporal lane")
        };
        assert_eq!(data, [2024, 1970, 0]);

        // HOUR over a DATE lane is 0 (date has no time-of-day).
        let expr = Expr::Function {
            name: "HOUR".to_string(),
            args: vec![Expr::Identifier("d".into())],
            distinct: false,
        };
        let program = compile_for_batch(&expr, &d_schema).unwrap().unwrap();
        let typed = eval_value_per_row(&program, &batch).unwrap();
        assert_eq!(typed[0], Value::Int(0));
    }

    #[test]
    fn predicate_bitmap_basic() {
        let s = schema();
        let col = ColumnVector::from_values(vec![
            Value::Int(1),
            Value::Int(0),
            Value::Int(5),
            Value::Null,
        ]);
        let b = Batch::new(s.clone(), vec![col]);
        // `x` truthy ↔ non-zero non-null
        let expr = Expr::Identifier("x".into());
        let program = compile_for_batch(&expr, &s).unwrap().expect("compiles");
        let bits = eval_predicate_bitmap(&program, &b).unwrap();
        assert!(bits.get(0));
        assert!(!bits.get(1));
        assert!(bits.get(2));
        assert!(!bits.get(3));
    }
}

#[cfg(test)]
mod refactor_tests;
