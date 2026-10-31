//! compute execution and preparation.

use super::*;

/// A `BatchOperator` wrapper that evaluates pre-compiled expressions on
/// each batch and appends the results as new columns. Used to
/// pre-compute aggregate input expressions (e.g. `price * discount`)
/// so the downstream `HashAggregateCore` sees them as plain column references.
pub(super) struct ComputeColumnsBatchStream<'a> {
    pub(super) inner: Box<dyn BatchOperator + 'a>,
    pub(super) programs: Vec<Arc<ExprProgram>>,
    pub(super) numeric_programs: Vec<Option<ComputedNumericProgram>>,
    pub(super) direct_expressions: Vec<Expr>,
    pub(super) schema: Arc<BatchSchema>,
    pub(super) row_major: bool,
    pub(super) pending_chunks:
        Option<crate::engine::executor::vec::evaluated_batch::ComputedRowCursor>,
}

/// Exact numeric computed inputs that can stay in typed lanes for a whole
/// batch.  Searched CASE is kept here, beside the arithmetic program it
/// selects, so a conditional DECIMAL aggregate does not have to escape to the
/// row evaluator merely because its result is conditional.
#[derive(Clone)]
pub(super) enum ComputedNumericProgram {
    Arithmetic(crate::engine::executor::select::plan::CompiledNumericExpr),
    SearchedEqualityCaseI64 {
        condition: ComputedEqualityCondition,
        result: i64,
    },
    SearchedDecimalCase {
        conditions: Vec<Arc<crate::engine::executor::vec::expr_batch::BatchExprProgram>>,
        simple_equalities: Vec<Option<ComputedEqualityCondition>>,
        results: Vec<crate::engine::executor::select::plan::CompiledNumericExpr>,
        else_result: Option<crate::engine::executor::select::plan::CompiledNumericExpr>,
        scale: u8,
    },
}

#[derive(Clone)]
pub(super) struct ComputedEqualityCondition {
    pub(super) column: usize,
    pub(super) value: Value,
    pub(super) collation_id: Option<u16>,
}

pub(super) fn compile_computed_equality_condition(
    expr: &Expr,
    template: &[PlanField],
    schema: &BatchSchema,
) -> Option<ComputedEqualityCondition> {
    let Expr::Binary {
        left,
        op: BinaryOp::Eq,
        right,
    } = expr
    else {
        return None;
    };
    let (column, constant) = if let Some(column) = expr_column_index_in_template(left, template) {
        (column, right.as_ref())
    } else {
        (
            expr_column_index_in_template(right, template)?,
            left.as_ref(),
        )
    };
    let value = crate::engine::expr::eval_const(constant).ok()?;
    Some(ComputedEqualityCondition {
        column,
        value,
        collation_id: schema.fields().get(column)?.source.collation_id,
    })
}

impl ComputedNumericProgram {
    pub(super) fn compile(
        expr: &Expr,
        template: &[PlanField],
        schema: &BatchSchema,
    ) -> Option<Self> {
        let resolve_column = |expr: &Expr| expr_column_index_in_template(expr, template);
        if let Expr::Case {
            operand: None,
            conditions,
            results,
            else_result,
        } = expr
        {
            if conditions.is_empty() || conditions.len() != results.len() {
                return None;
            }
            // COUNT(CASE WHEN status='x' THEN 1 ELSE NULL END) and the
            // equivalent SUM/COUNT integer forms are common over joins. Emit
            // a typed I64 lane plus a validity bitmap directly from the
            // equality predicate instead of invoking ExprProgram once per
            // joined row.
            if let ([condition], [result]) = (conditions.as_ref(), results.as_slice()) {
                let else_is_null = else_result.as_deref().is_none_or(|expr| {
                    matches!(crate::engine::expr::eval_const(expr), Ok(Value::Null))
                });
                let result = crate::engine::expr::eval_const(result)
                    .ok()
                    .and_then(|value| match value {
                        Value::Int(value) => Some(value),
                        Value::UInt(value) => i64::try_from(value).ok(),
                        _ => None,
                    });
                if else_is_null {
                    if let (Some(condition), Some(result)) = (
                        compile_computed_equality_condition(condition, template, schema),
                        result,
                    ) {
                        return Some(Self::SearchedEqualityCaseI64 { condition, result });
                    }
                }
            }
            let result_program = expr_batch::compile_for_batch(expr, schema).ok()??;
            let scale = match result_program.result_type().kind {
                crate::engine::expr_type::ExprTypeKind::Decimal { scale, .. } => scale,
                _ => return None,
            };
            let mut compiled_conditions = Vec::with_capacity(conditions.len());
            let mut simple_equalities = Vec::with_capacity(conditions.len());
            for condition in conditions {
                let scalar = expr_batch::compile_for_batch(condition, schema).ok()??;
                let capability = scalar.capability();
                // A batch condition may also inspect rows already matched by
                // an earlier arm. That is observable for volatile,
                // session-dependent, or erroring expressions, so those retain
                // the ordered row evaluator as their oracle.
                if capability.may_error
                    || !capability.deterministic
                    || capability.depends_on_session
                {
                    return None;
                }
                compiled_conditions.push(
                    crate::engine::executor::vec::expr_batch::BatchExprProgram::compile(
                        condition, schema,
                    )
                    .ok()??,
                );
                simple_equalities.push(compile_computed_equality_condition(
                    condition, template, schema,
                ));
            }
            let results = results
                .iter()
                .map(|result| {
                    crate::engine::executor::select::plan::CompiledNumericExpr::compile_with_column_resolver(
                        result,
                        &resolve_column,
                    )
                })
                .collect::<Option<Vec<_>>>()?;
            let else_result = match else_result.as_deref() {
                Some(result)
                    if matches!(crate::engine::expr::eval_const(result), Ok(Value::Null)) =>
                {
                    None
                }
                Some(result) => Some(
                    crate::engine::executor::select::plan::CompiledNumericExpr::compile_with_column_resolver(
                        result,
                        &resolve_column,
                    )?,
                ),
                None => None,
            };
            return Some(Self::SearchedDecimalCase {
                conditions: compiled_conditions,
                simple_equalities,
                results,
                else_result,
                scale,
            });
        }
        crate::engine::executor::select::plan::CompiledNumericExpr::compile_with_column_resolver(
            expr,
            &resolve_column,
        )
        .map(Self::Arithmetic)
    }

    pub(super) fn exact_magnitude_at(
        column: &ColumnVector,
        row: usize,
        scale: u8,
    ) -> Option<Option<i128>> {
        if column.is_null(row) {
            return Some(None);
        }
        let rescale = |magnitude: i128, source_scale: u8| {
            crate::util::decimal::rescale_round_i128(magnitude, source_scale, scale)
        };
        Some(Some(match column {
            ColumnVector::Decimal32 {
                data,
                scale: source_scale,
                ..
            } => rescale(i128::from(data[row]), *source_scale)?,
            ColumnVector::Decimal64 {
                data,
                scale: source_scale,
                ..
            } => rescale(i128::from(data[row]), *source_scale)?,
            ColumnVector::Decimal128 {
                data,
                scale: source_scale,
                ..
            } => rescale(data[row], *source_scale)?,
            ColumnVector::I64 { data, .. } => rescale(i128::from(data[row]), 0)?,
            ColumnVector::U64 { data, .. } => rescale(i128::from(data[row]), 0)?,
            _ => return None,
        }))
    }

    pub(super) fn try_eval_batch_decimal(&self, batch: &Batch) -> Option<ColumnVector> {
        match self {
            Self::Arithmetic(program) => {
                program.try_eval_batch_decimal(batch.columns(), batch.num_rows())
            }
            Self::SearchedEqualityCaseI64 { condition, result } => {
                let valid = expr_batch::try_column_const_cmp_bitmap_parts(
                    condition.column,
                    BinaryOp::Eq,
                    &condition.value,
                    condition.collation_id,
                    batch,
                )?;
                Some(ColumnVector::I64 {
                    data: vec![*result; batch.num_rows()],
                    nulls: (valid.count_ones() != batch.num_rows()).then_some(valid),
                })
            }
            Self::SearchedDecimalCase {
                conditions,
                simple_equalities,
                results,
                else_result,
                scale,
            } => {
                let n_rows = batch.num_rows();
                let mut data = vec![0; n_rows];
                let mut valid =
                    crate::engine::executor::vec::bitmap::Bitmap::new_all_invalid(n_rows);
                let mut matched =
                    crate::engine::executor::vec::bitmap::Bitmap::new_all_invalid(n_rows);

                // Merge branches in SQL CASE order and discard each mask and
                // result column before advancing. Transient memory is thus
                // O(batch rows), independent of the number of WHEN arms.
                // An unsupported/overflowing selected result declines the
                // whole batch so ExprProgram remains the correctness oracle.
                for ((condition, simple_equality), result) in
                    conditions.iter().zip(simple_equalities).zip(results)
                {
                    if matched.count_ones() == n_rows {
                        break;
                    }
                    let mask = match simple_equality {
                        Some(simple) => expr_batch::try_column_const_cmp_bitmap_parts(
                            simple.column,
                            BinaryOp::Eq,
                            &simple.value,
                            simple.collation_id,
                            batch,
                        )?,
                        None => condition.eval_predicate_bitmap(batch).ok()?,
                    };
                    let has_new_match = (0..n_rows).any(|row| !matched.get(row) && mask.get(row));
                    if !has_new_match {
                        continue;
                    }
                    // A CASE arm that returns a plain DECIMAL column can be
                    // gathered directly from the input lane.  Evaluating it
                    // as a standalone numeric program clones the complete
                    // source vector even though only the arm's selected rows
                    // are used; a family of conditional SUMs then repeats
                    // that full clone once per arm.
                    let materialized_result = match result {
                        crate::engine::executor::select::plan::CompiledNumericExpr::Column(_) => {
                            None
                        }
                        _ => Some(result.try_eval_batch_decimal(batch.columns(), n_rows)?),
                    };
                    for row in 0..n_rows {
                        if matched.get(row) || !mask.get(row) {
                            continue;
                        }
                        matched.set_true(row);
                        let magnitude = match result {
                            crate::engine::executor::select::plan::CompiledNumericExpr::Column(
                                column,
                            ) => Self::exact_magnitude_at(
                                batch.columns().get(*column)?,
                                row,
                                *scale,
                            )?,
                            _ => Self::exact_magnitude_at(
                                materialized_result.as_ref()?,
                                row,
                                *scale,
                            )?,
                        };
                        if let Some(magnitude) = magnitude {
                            data[row] = magnitude;
                            valid.set_true(row);
                        }
                    }
                }
                if matched.count_ones() != n_rows {
                    if let Some(else_result) = else_result {
                        let else_column = match else_result {
                            crate::engine::executor::select::plan::CompiledNumericExpr::Column(
                                _,
                            ) => None,
                            _ => Some(else_result.try_eval_batch_decimal(batch.columns(), n_rows)?),
                        };
                        for row in 0..n_rows {
                            if matched.get(row) {
                                continue;
                            }
                            matched.set_true(row);
                            let magnitude = match else_result {
                                crate::engine::executor::select::plan::CompiledNumericExpr::Column(column) => {
                                    Self::exact_magnitude_at(
                                        batch.columns().get(*column)?,
                                        row,
                                        *scale,
                                    )?
                                }
                                _ => Self::exact_magnitude_at(else_column.as_ref()?, row, *scale)?,
                            };
                            if let Some(magnitude) = magnitude {
                                data[row] = magnitude;
                                valid.set_true(row);
                            }
                        }
                    }
                }
                Some(ColumnVector::Decimal128 {
                    data,
                    scale: *scale,
                    nulls: (valid.count_ones() != n_rows).then_some(valid),
                })
            }
        }
    }
}

pub(super) fn try_eval_numeric_computed_column(
    programs: &[Option<ComputedNumericProgram>],
    index: usize,
    batch: &Batch,
) -> Option<ColumnVector> {
    programs
        .get(index)
        .and_then(Option::as_ref)
        .and_then(|numeric| numeric.try_eval_batch_decimal(batch))
}

/// Pre-compute every available fixed-point program as one atomic routing
/// decision. A failed numeric kernel leaves the caller free to execute the
/// complete program set in legacy row-major order, so error precedence never
/// depends on which expression happened to decline vectorization first.
pub(super) fn try_eval_row_major_numeric_columns(
    programs: &[Option<ComputedNumericProgram>],
    batch: &Batch,
) -> Option<Vec<Option<ColumnVector>>> {
    programs
        .iter()
        .map(|program| match program {
            Some(numeric) => Some(Some(numeric.try_eval_batch_decimal(batch)?)),
            None => Some(None),
        })
        .collect()
}

pub(super) fn try_eval_equality_case_family(
    programs: &[Option<ComputedNumericProgram>],
    batch: &Batch,
) -> Option<Vec<ColumnVector>> {
    if programs.len() < 2 {
        return None;
    }
    let mut conditions = Vec::with_capacity(programs.len());
    let mut selector_column = None;
    let mut result_column = None;
    let mut family_scale = None;
    let mut family_collation = None;
    for program in programs {
        let ComputedNumericProgram::SearchedDecimalCase {
            conditions: batch_conditions,
            simple_equalities,
            results,
            else_result: None,
            scale,
        } = program.as_ref()?
        else {
            return None;
        };
        if batch_conditions.len() != 1 || simple_equalities.len() != 1 || results.len() != 1 {
            return None;
        }
        let condition = simple_equalities[0].as_ref()?;
        let crate::engine::executor::select::plan::CompiledNumericExpr::Column(result) = results[0]
        else {
            return None;
        };
        if selector_column
            .replace(condition.column)
            .is_some_and(|column| column != condition.column)
            || result_column
                .replace(result)
                .is_some_and(|column| column != result)
            || family_scale
                .replace(*scale)
                .is_some_and(|existing| existing != *scale)
            || family_collation
                .replace(condition.collation_id)
                .is_some_and(|existing| existing != condition.collation_id)
        {
            return None;
        }
        if !matches!(
            condition.value,
            Value::String(_) | Value::SmallString(_) | Value::Bytes(_)
        ) {
            return None;
        }
        conditions.push(condition);
    }

    let selector = batch.columns().get(selector_column?)?;
    let result = batch.columns().get(result_column?)?;
    let scale = family_scale?;
    let collation_id = family_collation?;
    let constants = conditions
        .iter()
        .map(|condition| crate::types::value_str(&condition.value))
        .collect::<Option<Vec<_>>>()?;
    let constant_keys = constants
        .iter()
        .map(|constant| match collation_id {
            Some(collation_id) => crate::charset::collation_key_by_id(collation_id, constant),
            None => crate::charset::default_string_key(constant),
        })
        .collect::<Vec<_>>();
    // Breaking after the first match is valid only when constants are
    // mutually distinct under the column's actual collation (including PAD
    // SPACE and case folding). Duplicate-equivalent arms retain the complete
    // per-program path so every aggregate receives its input.
    for left in 0..constant_keys.len() {
        for right in left + 1..constant_keys.len() {
            if constant_keys[left] == constant_keys[right] {
                return None;
            }
        }
    }

    let rows = batch.num_rows();
    let mut data = (0..programs.len())
        .map(|_| vec![0i128; rows])
        .collect::<Vec<_>>();
    let mut valid = (0..programs.len())
        .map(|_| crate::engine::executor::vec::bitmap::Bitmap::new_all_invalid(rows))
        .collect::<Vec<_>>();
    let mut selector_key = Vec::new();
    let mut selector_cache: hashbrown::HashMap<Vec<u8>, Option<usize>, ahash::RandomState> =
        hashbrown::HashMap::with_capacity_and_hasher(
            constant_keys.len().saturating_mul(2),
            ahash::RandomState::new(),
        );
    for row in batch.visible_indices() {
        let value = match selector {
            ColumnVector::Utf8 {
                offsets,
                data,
                nulls,
            }
            | ColumnVector::Bytes {
                offsets,
                data,
                nulls,
            } => {
                if nulls.as_ref().is_some_and(|bitmap| !bitmap.get(row)) {
                    continue;
                }
                let start = *offsets.get(row)? as usize;
                let end = *offsets.get(row + 1)? as usize;
                std::borrow::Cow::Borrowed(std::str::from_utf8(data.get(start..end)?).ok()?)
            }
            ColumnVector::Generic { values } => {
                let value = values.get(row)?;
                if matches!(value, Value::Null) {
                    continue;
                }
                std::borrow::Cow::Borrowed(crate::types::value_str(value)?)
            }
            _ => return None,
        };
        let slot = if let Some(slot) = selector_cache.get(value.as_bytes()) {
            *slot
        } else {
            match collation_id {
                Some(collation_id) => crate::charset::collation_key_by_id_into(
                    collation_id,
                    &value,
                    &mut selector_key,
                ),
                None => crate::charset::default_string_key_into(&value, &mut selector_key),
            }
            let slot = constant_keys
                .iter()
                .position(|constant| selector_key == *constant);
            selector_cache.insert(value.as_bytes().to_vec(), slot);
            slot
        };
        let Some(slot) = slot else {
            continue;
        };
        if let Some(magnitude) = ComputedNumericProgram::exact_magnitude_at(result, row, scale)? {
            data[slot][row] = magnitude;
            valid[slot].set_true(row);
        }
    }
    Some(
        data.into_iter()
            .zip(valid)
            .map(|(data, valid)| ColumnVector::Decimal128 {
                data,
                scale,
                nulls: (valid.count_ones() != rows).then_some(valid),
            })
            .collect(),
    )
}

pub(super) struct TemporalScanWorker {
    pub(super) source: Box<dyn crate::engine::executor::vec::stream::ParallelBatchWorker>,
    pub(super) programs: Vec<Arc<ExprProgram>>,
    pub(super) schema: Arc<BatchSchema>,
    pub(super) _charge: Arc<mem_budget::ControlledMemoryCharge>,
}

impl crate::engine::executor::vec::stream::ParallelBatchWorker for TemporalScanWorker {
    fn run(
        self: Box<Self>,
        consume: &mut dyn FnMut(Batch) -> Result<(), QueryError>,
        abort: &std::sync::atomic::AtomicBool,
    ) -> Result<(), QueryError> {
        let Self {
            source,
            programs,
            schema,
            _charge,
        } = *self;
        source.run(
            &mut |batch| {
                compute_columns_and_consume(&programs, &[None], &schema, false, batch, consume)
            },
            abort,
        )
    }
}

pub(super) fn native_temporal_worker_eligible(
    programs: &[Arc<ExprProgram>],
    input: &dyn BatchOperator,
) -> bool {
    programs.len() == 1
        && !matches!(
            crate::engine::executor::vec::config::effective_mode(),
            crate::engine::executor::vec::config::VecMode::Off
        )
        && input.is_native_parallel_scan()
        && matches!(input.properties().ordering, BatchOrdering::Unordered)
        && (programs[0].column_parallel_safe(input.schema())
            || programs[0]
                .as_temporal_extract()
                .is_some_and(|(_, column)| {
                    input.schema().fields().get(column).is_some_and(|field| {
                        matches!(
                            field.column_type,
                            ColumnType::Date | ColumnType::DateTime | ColumnType::Timestamp
                        )
                    })
                }))
}

impl<'a> BatchOperator for ComputeColumnsBatchStream<'a> {
    fn take_parallel_workers(
        &mut self,
        degree: usize,
    ) -> Result<
        Option<Vec<Box<dyn crate::engine::executor::vec::stream::ParallelBatchWorker>>>,
        QueryError,
    > {
        if !native_temporal_worker_eligible(&self.programs, self.inner.as_ref())
            || self.numeric_programs.iter().any(Option::is_some)
        {
            return Ok(None);
        }
        let bytes = degree.saturating_mul(
            std::mem::size_of::<TemporalScanWorker>()
                + std::mem::size_of::<Arc<ExprProgram>>()
                + 2 * std::mem::size_of::<
                    Box<dyn crate::engine::executor::vec::stream::ParallelBatchWorker>,
                >(),
        );
        let charge = Arc::new(mem_budget::ControlledMemoryCharge::try_new(
            mem_budget::ControlledMemoryCategory::StorageScratch,
            bytes,
        )?);
        let Some(workers) = self.inner.take_parallel_workers(degree)? else {
            return Ok(None);
        };
        Ok(Some(
            workers
                .into_iter()
                .map(|source| {
                    Box::new(TemporalScanWorker {
                        source,
                        programs: self.programs.clone(),
                        schema: self.schema.clone(),
                        _charge: charge.clone(),
                    })
                        as Box<dyn crate::engine::executor::vec::stream::ParallelBatchWorker>
                })
                .collect(),
        ))
    }
    fn schema(&self) -> &Arc<BatchSchema> {
        &self.schema
    }

    fn properties(&self) -> BatchProperties {
        self.inner.properties()
    }

    fn install_runtime_filter(&mut self, filter: crate::planner::RuntimeFilter) -> bool {
        self.inner.install_runtime_filter(filter)
    }

    fn consume_into_hash_aggregate(
        &mut self,
        aggregate: &mut vec_hash_agg::HashAggregateCore,
    ) -> Result<bool, QueryError> {
        // With no computed columns this wrapper is schema-transparent.  Keep
        // the Join -> Aggregate sink visible instead of forcing the join to
        // produce pair batches merely because aggregate lowering installed
        // an empty expression wrapper.
        //
        // Computed aggregate inputs still consume bounded pair batches: the
        // join cannot evaluate those programs without also receiving their
        // expression metadata, so forwarding in that case would resolve the
        // appended column indexes against the wrong input schema.
        if self.programs.is_empty() {
            self.inner.consume_into_hash_aggregate(aggregate)
        } else if self.direct_expressions.len() == self.programs.len()
            && !crate::engine::executor::vec::evaluated_batch::needs_chunks(
                &self.programs,
                DEFAULT_BATCH_ROWS,
            )
        {
            self.inner
                .consume_computed_into_hash_aggregate(aggregate, &self.direct_expressions)
        } else {
            Ok(false)
        }
    }

    fn consume_computed_into_hash_aggregate(
        &mut self,
        aggregate: &mut vec_hash_agg::HashAggregateCore,
        expressions: &[Expr],
    ) -> Result<bool, QueryError> {
        self.inner
            .consume_computed_into_hash_aggregate(aggregate, expressions)
    }

    fn next_batch(&mut self) -> Result<Option<Batch>, QueryError> {
        use crate::engine::executor::vec::evaluated_batch::{self, ComputedRowCursor};
        loop {
            crate::engine::cancel::check_cancelled()?;
            if let Some(cursor) = self.pending_chunks.as_mut() {
                if let Some(batch) = next_computed_chunk(
                    cursor,
                    &self.programs,
                    &self.numeric_programs,
                    &self.schema,
                    self.row_major,
                )? {
                    return Ok(Some(batch));
                }
                self.pending_chunks = None;
            }
            let Some(batch) = self.inner.next_batch()? else {
                return Ok(None);
            };
            if !self.programs.is_empty()
                && evaluated_batch::needs_chunks(&self.programs, batch.num_rows())
            {
                if let Some((columns, charge)) =
                    try_bounded_numeric_columns(&self.numeric_programs, &batch)?
                {
                    return append_computed_columns(batch, &self.schema, charge, columns).map(Some);
                }
                self.pending_chunks = Some(ComputedRowCursor::new(batch)?);
                continue;
            }
            return compute_columns_batch(
                &self.programs,
                &self.numeric_programs,
                &self.schema,
                self.row_major,
                batch,
            )
            .map(Some);
        }
    }
}

pub(super) fn compute_columns_batch(
    programs: &[Arc<ExprProgram>],
    numeric_programs: &[Option<ComputedNumericProgram>],
    schema: &Arc<BatchSchema>,
    row_major: bool,
    batch: Batch,
) -> Result<Batch, QueryError> {
    if programs.is_empty() {
        return Ok(batch);
    }
    // A single typed-kernel call is admitted before allocating any result
    // lane. The two-buffer allowance covers Value-to-lane conversion; large
    // or unbounded domains arrive through ComputedRowCursor instead.
    let bound =
        programs
            .iter()
            .try_fold(0usize, |sum, program| {
                Some(sum.saturating_add(
                    crate::engine::executor::vec::evaluated_batch::result_bound(program)?,
                ))
            })
            .ok_or_else(|| {
                crate::error::catalog::ER_INTERNAL_ERROR
                    .error("unbounded computed input requires byte chunks")
            })?;
    let mut output_charge = batch.memory_charge();
    output_charge.add_force(
        bound
            .saturating_mul(batch.num_rows())
            .saturating_mul(2)
            .saturating_add(
                programs
                    .len()
                    .saturating_mul(std::mem::size_of::<ColumnVector>()),
            )
            .saturating_add(128),
    )?;
    // Independent, deterministic expressions use the faster
    // expression-major kernels. If multiple expressions can warn, error,
    // or observe session state, preserve SQL's row-major precedence:
    // row 0 expression 0..N, then row 1. Invisible rows are not evaluated.
    let scalar_mode = matches!(
        crate::engine::executor::vec::config::effective_mode(),
        crate::engine::executor::vec::config::VecMode::Off
    );
    let extra: Vec<ColumnVector> = if !row_major && !scalar_mode {
        if let Some(columns) = try_eval_equality_case_family(numeric_programs, &batch) {
            columns
        } else {
            programs
                .iter()
                .enumerate()
                .map(|(index, program)| {
                    if let Some(column) =
                        try_eval_numeric_computed_column(numeric_programs, index, &batch)
                    {
                        return Ok(column);
                    }
                    expr_batch::eval_to_column(program, &batch)
                })
                .collect::<Result<_, _>>()?
        }
    } else if row_major || (scalar_mode && programs.len() > 1) {
        // The scalar oracle still executes every legacy ExprProgram once
        // per visible row. Bind the batch columns to RowView only once
        // and run all independent programs against that view; evaluating
        // expression-major would rebuild and clone the complete input row
        // once per program (TPC-H Q1 has six computed aggregate inputs).
        // `row_major` remains mandatory for observable evaluation order;
        // the scalar-only extension is safe because the constructor sets
        // it for every program set that may warn, fail, observe session
        // state, or be non-deterministic.
        // A successful fixed-point batch kernel is non-observable: it
        // allocates no session state and cannot warn or fail. We can
        // therefore pre-compute those columns while keeping every
        // remaining program in the original row-major order. If any
        // numeric kernel declines the batch (including overflow), use no
        // pre-computed results and let the legacy evaluator remain the
        // complete correctness oracle.
        let mut numeric_columns = if scalar_mode {
            vec![None; programs.len()]
        } else {
            try_eval_row_major_numeric_columns(numeric_programs, &batch)
                .unwrap_or_else(|| vec![None; programs.len()])
        };
        if !scalar_mode {
            let read_only = programs
                .iter()
                .all(|program| program.has_column_contract(batch.schema()));
            for (index, program) in programs.iter().enumerate() {
                if numeric_columns[index].is_none() {
                    numeric_columns[index] = if read_only {
                        program.try_eval_column(&batch)
                    } else {
                        expr_batch::try_eval_infallible_typed_column(program, &batch)
                    };
                }
            }
        }
        if numeric_columns.iter().all(Option::is_some) {
            numeric_columns.into_iter().map(Option::unwrap).collect()
        } else {
            let mut values = vec![vec![Value::Null; batch.num_rows()]; programs.len()];
            let mut row_values = Vec::with_capacity(batch.columns().len());
            for (ordinal, row) in batch.visible_indices().enumerate() {
                if ordinal & crate::engine::cancel::SCALAR_ROW_CANCEL_MASK == 0 {
                    crate::engine::cancel::check_cancelled()?;
                }
                row_values.clear();
                row_values.extend(
                    batch
                        .columns()
                        .iter()
                        .zip(batch.schema().fields())
                        .map(|(column, field)| column.typed_value_at(row, field.column_type)),
                );
                let view = crate::engine::expr_program::RowView::from_values(&row_values);
                for (program_index, program) in programs.iter().enumerate() {
                    if numeric_columns[program_index].is_some() {
                        continue;
                    }
                    values[program_index][row] =
                        crate::engine::expr_program::with_eval_scratch(|scratch| {
                            program.eval_row(&view, scratch)
                        })?;
                }
            }
            values
                .into_iter()
                .enumerate()
                .map(|(index, values)| {
                    numeric_columns[index]
                        .take()
                        .unwrap_or_else(|| ColumnVector::from_values(values))
                })
                .collect()
        }
    } else {
        programs
            .iter()
            .enumerate()
            .map(|(index, program)| {
                // Independent deterministic programs choose their kernel
                // independently. A temporal group expression must not
                // force an adjacent exact-decimal aggregate expression
                // out of its fixed-point batch lane (or vice versa).
                // Row-major programs stay in the branch above so error /
                // warning precedence remains unchanged.
                if !scalar_mode {
                    if let Some(column) =
                        try_eval_numeric_computed_column(numeric_programs, index, &batch)
                    {
                        return Ok(column);
                    }
                }
                expr_batch::eval_to_column(program, &batch)
            })
            .collect::<Result<_, _>>()?
    };
    append_computed_columns(batch, schema, output_charge, extra)
}

fn append_computed_columns(
    mut batch: Batch,
    schema: &Arc<BatchSchema>,
    mut output_charge: mem_budget::ScopedCharge,
    extra: Vec<ColumnVector>,
) -> Result<Batch, QueryError> {
    let actual = extra
        .iter()
        .map(ColumnVector::memory_bytes)
        .sum::<usize>()
        .saturating_add(
            extra
                .capacity()
                .saturating_mul(std::mem::size_of::<ColumnVector>()),
        )
        .saturating_add(128);
    if actual > output_charge.charged() {
        output_charge.add_force(actual - output_charge.charged())?;
    } else {
        output_charge.release_bytes(output_charge.charged() - actual);
    }
    let transient_columns = extra
        .capacity()
        .saturating_mul(std::mem::size_of::<ColumnVector>());
    let sel = batch.selection.take();
    let mut cols = batch.take_columns()?;
    let mut metadata_charge = output_charge.sibling();
    metadata_charge.add_force(
        cols.capacity()
            .saturating_mul(std::mem::size_of::<ColumnVector>()),
    )?;
    mem_budget::try_reserve_vec_hard(
        &mut cols,
        extra.len(),
        &mut metadata_charge,
        "computed column metadata",
    )?;
    cols.extend(extra);
    output_charge.release_bytes(transient_columns);
    output_charge.absorb(&mut metadata_charge);
    cols.attach_charge(output_charge);
    let mut new_batch = Batch::from_owned_columns(schema.clone(), cols);
    if let Some(sel) = sel {
        new_batch = new_batch.with_selection(sel);
    }
    Ok(new_batch)
}

/// Admit the physical lane outputs and their expression-tree scratch, rather
/// than sizing native decimal columns as arrays of heap-capable Values. This
/// keeps an already bounded morsel intact and transfers its input owners.
fn try_bounded_numeric_columns(
    numeric_programs: &[Option<ComputedNumericProgram>],
    batch: &Batch,
) -> Result<Option<(Vec<ColumnVector>, mem_budget::ScopedCharge)>, QueryError> {
    if Arc::strong_count(&batch.columns) != 1
        || matches!(
            crate::engine::executor::vec::config::effective_mode(),
            crate::engine::executor::vec::config::VecMode::Off
        )
    {
        return Ok(None);
    }
    let mut retained = numeric_programs
        .len()
        .saturating_mul(std::mem::size_of::<ColumnVector>())
        .saturating_add(128);
    let mut peak = retained;
    for numeric in numeric_programs {
        let Some(ComputedNumericProgram::Arithmetic(program)) = numeric else {
            return Ok(None);
        };
        let Some((output, scratch)) =
            crate::engine::executor::select::plan::numeric_batch::decimal_workspace_bound(
                program,
                batch.columns(),
                batch.num_rows(),
            )
        else {
            return Ok(None);
        };
        peak = peak.max(retained.saturating_add(scratch));
        retained = retained.saturating_add(output);
    }
    if retained > crate::engine::executor::vec::evaluated_batch::byte_target() {
        return Ok(None);
    }
    let mut charge = batch.memory_charge();
    if !charge.add_within(peak, charge.remaining()) {
        return Ok(None);
    }
    let mut columns =
        mem_budget::try_vec_with_capacity(numeric_programs.len(), "bounded numeric columns")?;
    for numeric in numeric_programs {
        let Some(column) = numeric
            .as_ref()
            .and_then(|numeric| numeric.try_eval_batch_decimal(batch))
        else {
            // Native probes cannot warn or mutate SQL state. Drop every
            // partial result and retain the original ordered scalar fallback.
            return Ok(None);
        };
        columns.push(column);
    }
    Ok(Some((columns, charge)))
}

fn next_computed_chunk(
    cursor: &mut crate::engine::executor::vec::evaluated_batch::ComputedRowCursor,
    programs: &[Arc<ExprProgram>],
    numeric_programs: &[Option<ComputedNumericProgram>],
    schema: &Arc<BatchSchema>,
    row_major: bool,
) -> Result<Option<Batch>, QueryError> {
    let width = programs.iter().try_fold(0usize, |sum, program| {
        Some(
            sum.saturating_add(crate::engine::executor::vec::evaluated_batch::result_bound(
                program,
            )?),
        )
    });
    // An oversized static bound can be much larger than the actual value.
    // Keep the single-row path for these expressions instead of reserving
    // their entire declared maximum before observing one result.
    if let Some(width) = width.filter(|width| {
        width.saturating_mul(2) <= crate::engine::executor::vec::evaluated_batch::byte_target()
    }) {
        cursor
            .next_input_chunk(width)?
            .map(|batch| {
                compute_columns_batch(programs, numeric_programs, schema, row_major, batch)
            })
            .transpose()
    } else {
        cursor.next(programs, schema)
    }
}

fn compute_columns_and_consume(
    programs: &[Arc<ExprProgram>],
    numeric_programs: &[Option<ComputedNumericProgram>],
    schema: &Arc<BatchSchema>,
    row_major: bool,
    batch: Batch,
    consume: &mut dyn FnMut(Batch) -> Result<(), QueryError>,
) -> Result<(), QueryError> {
    use crate::engine::executor::vec::evaluated_batch::{self, ComputedRowCursor};
    if !programs.is_empty() && evaluated_batch::needs_chunks(programs, batch.num_rows()) {
        if let Some((columns, charge)) = try_bounded_numeric_columns(numeric_programs, &batch)? {
            return consume(append_computed_columns(batch, schema, charge, columns)?);
        }
        let mut cursor = ComputedRowCursor::new(batch)?;
        while let Some(batch) =
            next_computed_chunk(&mut cursor, programs, numeric_programs, schema, row_major)?
        {
            consume(batch)?;
        }
        Ok(())
    } else {
        consume(compute_columns_batch(
            programs,
            numeric_programs,
            schema,
            row_major,
            batch,
        )?)
    }
}

#[derive(Clone)]
pub(super) struct ComputeColumnsWorkerTransform {
    pub(super) programs: Vec<Arc<ExprProgram>>,
    pub(super) numeric_programs: Vec<Option<ComputedNumericProgram>>,
    pub(super) schema: Arc<BatchSchema>,
    pub(super) row_major: bool,
}

impl crate::engine::executor::vec::scheduler::hash_agg_driver::HashAggBatchTransform
    for ComputeColumnsWorkerTransform
{
    fn transform(&self, batch: Batch) -> Result<Batch, QueryError> {
        compute_columns_batch(
            &self.programs,
            &self.numeric_programs,
            &self.schema,
            self.row_major,
            batch,
        )
    }
    fn prepare_and_consume(
        &self,
        batch: Batch,
        consume: &mut dyn FnMut(Batch) -> Result<(), QueryError>,
    ) -> Result<(), QueryError> {
        compute_columns_and_consume(
            &self.programs,
            &self.numeric_programs,
            &self.schema,
            self.row_major,
            batch,
            consume,
        )
    }
}

// ---------------------------------------------------------------------
// HashAggregate metrics stash
// ---------------------------------------------------------------------
//
// `try_vec_hash_aggregate` doesn't see the matching `HashAggregate`
// `PhysicalPlan` node — that's the caller's frame. We stash the per-call
// metrics in a thread-local slot so the caller can pull them and forward
// to `analyze_stats` against the right pointer. Reads consume the slot
// so stale metrics from a previous call never leak into the next one.

#[cfg(test)]
mod bounded_numeric_tests {
    use super::*;
    use crate::engine::executor::select::plan::CompiledNumericExpr;

    fn fixture(rows: usize, value: i64) -> (Batch, Vec<Option<ComputedNumericProgram>>) {
        let schema = Arc::new(BatchSchema::new(vec![BatchField {
            table: String::new(),
            alias: None,
            name: "v".into(),
            column_type: ColumnType::Decimal {
                precision: 18,
                scale: 2,
            },
            source: Default::default(),
        }]));
        let batch = Batch::new(
            schema,
            vec![ColumnVector::Decimal64 {
                data: vec![value; rows],
                scale: 2,
                nulls: None,
            }],
        );
        let program = ComputedNumericProgram::Arithmetic(CompiledNumericExpr::Binary {
            left: Box::new(CompiledNumericExpr::Column(0)),
            op: BinaryOp::Add,
            right: Box::new(CompiledNumericExpr::Column(0)),
        });
        (batch, vec![Some(program)])
    }

    #[test]
    fn bounded_numeric_admission_releases_failed_probes_and_preserves_shared_input() {
        let budget = Arc::new(mem_budget::MemBudget::new(4 * 1024 * 1024));
        mem_budget::with_query_budget(budget.clone(), || {
            let (batch, programs) = fixture(8192, 100);
            let shared = batch.clone();
            assert!(try_bounded_numeric_columns(&programs, &batch)
                .unwrap()
                .is_none());
            assert_eq!(budget.used(), 0);
            drop(shared);
            let hold = mem_budget::force_charge_scoped(budget.limit() - 256 * 1024).unwrap();
            assert!(try_bounded_numeric_columns(&programs, &batch)
                .unwrap()
                .is_none());
            assert_eq!(budget.used(), hold.charged());
            drop(hold);
            let (columns, charge) = try_bounded_numeric_columns(&programs, &batch)
                .unwrap()
                .unwrap();
            assert!(
                budget.used()
                    >= columns
                        .iter()
                        .map(ColumnVector::memory_bytes)
                        .sum::<usize>()
            );
            drop(columns);
            drop(charge);
            assert_eq!(budget.used(), 0);
            let (overflow, programs) = fixture(8192, i64::MAX);
            assert!(try_bounded_numeric_columns(&programs, &overflow)
                .unwrap()
                .is_none());
            assert_eq!(budget.used(), 0);
            let _scalar = crate::engine::executor::vec::config::enter_mode(
                crate::engine::executor::vec::config::VecMode::Off,
            );
            assert!(try_bounded_numeric_columns(&programs, &batch)
                .unwrap()
                .is_none());
            assert_eq!(budget.used(), 0);
        });
    }

    #[test]
    fn bounded_numeric_output_preserves_nulls_duplicate_selection_and_mixed_lane_widths() {
        use crate::engine::executor::vec::{batch::Selection, bitmap::Bitmap};
        let (mut batch, programs) = fixture(4, 100);
        for wide in [false, true] {
            let mut valid = Bitmap::new_all_valid(4);
            valid.set(1, false);
            batch.try_columns_mut().unwrap()[0] = if wide {
                ColumnVector::Decimal128 {
                    data: vec![100, 0, 300, 400],
                    scale: 2,
                    nulls: Some(valid),
                }
            } else {
                ColumnVector::Decimal32 {
                    data: vec![100, 0, 300, 400],
                    scale: 2,
                    nulls: Some(valid),
                }
            };
            batch.selection = Some(Selection::Indices(vec![3, 1, 0, 3]));
            let (columns, _charge) = try_bounded_numeric_columns(&programs, &batch)
                .unwrap()
                .unwrap();
            let output = batch
                .visible_indices()
                .map(|row| columns[0].value_at(row))
                .collect::<Vec<_>>();
            let expected = vec![
                Value::DecimalCompact(800, 2),
                Value::Null,
                Value::DecimalCompact(200, 2),
                Value::DecimalCompact(800, 2),
            ];
            assert_eq!(output, expected);
        }
    }
}
