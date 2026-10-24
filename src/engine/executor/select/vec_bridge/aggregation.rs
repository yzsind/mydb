//! aggregate execution and preparation.

use super::*;
pub(super) use scheduler_hash_join_workers as scheduler_topn_workers;

/// Resolve GROUP BY inputs to physical batch columns. Plain columns remain
/// zero-copy; every other compilable scalar expression is appended once by
/// ComputeColumnsBatchStream and then treated exactly like a native column by
/// HashAgg. This is a layout choice inside the generic operator, not a query
/// shape fast path.
pub(super) fn compile_group_columns(
    group_by: &[Expr],
    template: &[PlanField],
    schema: &BatchSchema,
) -> Option<(
    Vec<usize>,
    Vec<Arc<ExprProgram>>,
    Vec<Option<ComputedNumericProgram>>,
    Vec<BatchField>,
)> {
    let mut columns = Vec::with_capacity(group_by.len());
    let mut programs = Vec::new();
    let mut numeric_programs = Vec::new();
    let mut fields = Vec::new();
    for (group_index, expr) in group_by.iter().enumerate() {
        if let Some(column) = expr_column_index_in_template(expr, template) {
            if template
                .get(column)
                .is_some_and(|field| field.source.flags & crate::mysql::protocol::ENUM_FLAG != 0)
            {
                return None;
            }
            columns.push(column);
            continue;
        }
        let program = expr_batch::compile_for_batch(expr, schema).ok()??;
        let field = computed_batch_field(&program, "__group", format!("g{group_index}"))?;
        columns.push(template.len() + programs.len());
        programs.push(program);
        numeric_programs.push(ComputedNumericProgram::compile(expr, template, schema));
        fields.push(field);
    }
    Some((columns, programs, numeric_programs, fields))
}

/// Rewrite an expression evaluated *after* HashAggregate onto the aggregate
/// operator's physical output schema. GroupKeyRef nodes and exact group
/// expression subtrees become the corresponding physical group column, while
/// every aggregate call becomes the synthetic `__agg.aN` output column. The
/// walk deliberately stops at aggregate calls: their arguments belong to the
/// pre-aggregate input schema, not the output schema.
///
/// This single rewrite is shared by HAVING, final projection and ORDER BY, so
/// expressions such as `SUM(v) / COUNT(*)`, rather than only a bare aggregate,
/// stay in the same BatchOperator pipeline.
pub(super) fn rewrite_hash_agg_output_expr(
    expr: &Expr,
    output_schema: &BatchSchema,
    group_count: usize,
    group_exprs: &[Expr],
    aggregate_keys: &[u64],
) -> Option<Expr> {
    fn output_column_expr(field: &BatchField) -> Expr {
        if field.table.is_empty() {
            Expr::Identifier(field.name.clone())
        } else {
            Expr::CompoundIdentifier(vec![field.table.clone(), field.name.clone()])
        }
    }
    crate::parser::visit::try_map_value_expr(expr, &mut |expr| {
        if let Expr::Function { name, .. } = expr {
            if crate::planner::is_aggregate_function(name) {
                let key = crate::engine::executor::select::aggregate::aggregate_key(expr);
                let slot = aggregate_keys
                    .iter()
                    .position(|candidate| *candidate == key)
                    .ok_or(())?;
                return Ok(Some(Expr::CompoundIdentifier(vec![
                    "__agg".to_string(),
                    format!("a{slot}"),
                ])));
            }
        }
        if let Some(index) = group_exprs
            .iter()
            .position(|group| expr_semantically_eq(group, expr))
        {
            return Ok(Some(output_column_expr(
                output_schema.fields().get(index).ok_or(())?,
            )));
        }
        if let Some(key) = crate::engine::executor::select::aggregate::hidden_group_column_key(expr)
        {
            if let Some(slot) = aggregate_keys
                .iter()
                .position(|candidate| *candidate == key)
            {
                return Ok(Some(Expr::CompoundIdentifier(vec![
                    "__agg".to_string(),
                    format!("a{slot}"),
                ])));
            }
        }

        Ok(Some(match expr {
            Expr::GroupKeyRef { index, .. } => {
                if *index >= group_count {
                    return Err(());
                }
                output_column_expr(output_schema.fields().get(*index).ok_or(())?)
            }
            Expr::Grouping(indices) => {
                let mask = output_schema
                    .fields()
                    .iter()
                    .find(|field| field.table == "__agg" && field.name == "grouping_mask")
                    .ok_or(())?;
                let mask = output_column_expr(mask);
                let mut value = Expr::Literal(Literal::Int(0));
                for index in indices {
                    let bit = Expr::Binary {
                        left: Box::new(Expr::Binary {
                            left: Box::new(mask.clone()),
                            op: BinaryOp::ShiftRight,
                            right: Box::new(Expr::Literal(Literal::UInt(*index as u64))),
                        }),
                        op: BinaryOp::BitAnd,
                        right: Box::new(Expr::Literal(Literal::UInt(1))),
                    };
                    value = Expr::Binary {
                        left: Box::new(Expr::Binary {
                            left: Box::new(value),
                            op: BinaryOp::Mul,
                            right: Box::new(Expr::Literal(Literal::Int(2))),
                        }),
                        op: BinaryOp::Add,
                        right: Box::new(bit),
                    };
                }
                Expr::Function {
                    name: "CAST".into(),
                    args: vec![value, Expr::Literal(Literal::String("SIGNED".into()))],
                    distinct: false,
                }
            }

            // These require a separate post-aggregate semantic context.
            Expr::Subquery(_)
            | Expr::InSubquery { .. }
            | Expr::QuantifiedComparison { .. }
            | Expr::Exists { .. }
            | Expr::WindowFunction { .. } => return Err(()),
            _ => return Ok(None),
        }))
    })
    .ok()
}

pub(super) fn compile_hash_agg_having(
    having: Option<&Expr>,
    output_schema: &BatchSchema,
    group_count: usize,
    group_exprs: &[Expr],
    aggregate_keys: &[u64],
    runtime_charge: Option<crate::engine::executor::mem_budget::OwnedMemCharge>,
) -> Option<Option<Arc<crate::engine::executor::vec::expr_batch::BatchExprProgram>>> {
    let Some(having) = having else {
        return Some(None);
    };
    let rewritten = rewrite_hash_agg_output_expr(
        having,
        output_schema,
        group_count,
        group_exprs,
        aggregate_keys,
    )?;
    let program = if runtime_charge.is_some() {
        crate::engine::executor::vec::expr_batch::BatchExprProgram::compile_post_aggregate_runtime_constants(
            &rewritten,
            output_schema,
            runtime_charge,
        )
        .ok()??
    } else {
        crate::engine::executor::vec::expr_batch::BatchExprProgram::compile_post_aggregate(
            &rewritten,
            output_schema,
        )
        .ok()??
    };
    Some(Some(program))
}

pub(super) fn compile_aggregate_final_filter(
    having: Option<&Expr>,
    aggregates: &[Expr],
) -> Option<vec_hash_agg::AggregateFinalFilter> {
    let Expr::Binary { left, op, right } = having? else {
        return None;
    };
    let (aggregate, literal_expr, op) = if let Some(index) = aggregates
        .iter()
        .position(|aggregate| aggregate == left.as_ref())
    {
        (index, right.as_ref(), *op)
    } else if let Some(index) = aggregates
        .iter()
        .position(|aggregate| aggregate == right.as_ref())
    {
        (index, left.as_ref(), crate::engine::expr::flip_cmp(*op))
    } else {
        return None;
    };
    let literal = crate::engine::expr::expr_literal_value(literal_expr)?;
    if matches!(literal, Value::Null) {
        return None;
    }
    let comparison = match op {
        BinaryOp::Eq | BinaryOp::NullSafeEq => vec_hash_agg::AggregateFinalComparison::Eq,
        BinaryOp::NotEq => vec_hash_agg::AggregateFinalComparison::NotEq,
        BinaryOp::Lt => vec_hash_agg::AggregateFinalComparison::Lt,
        BinaryOp::LtEq => vec_hash_agg::AggregateFinalComparison::LtEq,
        BinaryOp::Gt => vec_hash_agg::AggregateFinalComparison::Gt,
        BinaryOp::GtEq => vec_hash_agg::AggregateFinalComparison::GtEq,
        _ => return None,
    };
    Some(vec_hash_agg::AggregateFinalFilter {
        aggregate_index: aggregate,
        comparison,
        decimal_literal: literal.exact_decimal().and_then(|decimal| match decimal {
            crate::util::decimal::ExactDecimal::I64(value, scale) => {
                Some((i128::from(value), scale))
            }
            crate::util::decimal::ExactDecimal::I128(value, scale) => Some((value, scale)),
            crate::util::decimal::ExactDecimal::Big(_, _) => None,
        }),
        literal,
    })
}

pub(super) fn hash_agg_output_preflight(
    upstream: &PhysicalPlan,
    group_by: &[Expr],
    having: Option<&Expr>,
    aggregates: &[Expr],
) -> Option<HashAggPreflight> {
    if aggregates.is_empty() && group_by.is_empty() {
        return None;
    }
    let template = plan_output_template(upstream);
    hash_agg_output_preflight_for_template(template, group_by, having, aggregates, None)
}

/// Bind the output relation once, then attach HAVING after resolving its
/// statement constants. Input operators must not be opened until this succeeds.
fn prepare_hash_agg_output(
    db: &Arc<Database>,
    txn: TxnView,
    input: &PhysicalPlan,
    group_by: &[Expr],
    having: Option<&Expr>,
    aggregates: &[Expr],
) -> Result<Option<(HashAggPreflight, Option<vec_hash_agg::AggregateFinalFilter>)>, QueryError> {
    let Some(mut preflight) = hash_agg_output_preflight(input, group_by, None, aggregates) else {
        return Ok(None);
    };
    let (resolved_having, runtime_charge) =
        resolve_hash_agg_having_constants(db, txn, input, having)?;
    let effective_having = resolved_having.as_ref().or(having);
    let Some(having_program) = compile_hash_agg_having(
        effective_having,
        &preflight.output_schema,
        preflight.group_count,
        &preflight.group_exprs,
        &preflight.aggregate_keys,
        runtime_charge,
    ) else {
        return Ok(None);
    };
    preflight.having_program = having_program;
    let final_filter = compile_aggregate_final_filter(effective_having, aggregates);
    Ok(Some((preflight, final_filter)))
}

/// Resolve deterministic, uncorrelated subqueries in HAVING before opening
/// the aggregate input.  A subquery is a statement constant in this case, so
/// lowering it to `BoundValue` both preserves semantics and lets the ordinary
/// post-aggregate batch compiler keep the whole HashAggregate vectorized.
/// Correlated or volatile subqueries return `None` and retain the row oracle.
pub(super) fn resolve_hash_agg_having_constants(
    db: &Arc<Database>,
    txn: TxnView,
    upstream: &PhysicalPlan,
    having: Option<&Expr>,
) -> Result<
    (
        Option<Expr>,
        Option<crate::engine::executor::mem_budget::OwnedMemCharge>,
    ),
    QueryError,
> {
    let Some(having) =
        having.filter(|expr| crate::engine::executor::select::simple::expr_has_subquery(expr))
    else {
        return Ok((None, None));
    };
    let mut metas = Vec::new();
    crate::engine::executor::select::plan::collect_plan_fields(upstream, &mut metas);
    let template = PlanRow::from_fields(
        metas
            .into_iter()
            .map(|meta| PlanField {
                meta: crate::engine::executor::select::plan::intern_col_meta(meta),
                value: Value::Null,
            })
            .collect(),
    );
    let Some(resolved) =
        crate::engine::executor::select::eval::resolve_batch_constant_scalar_subqueries(
            db, txn, having, &template,
        )?
    else {
        return Ok((None, None));
    };
    Ok((Some(resolved.expr), resolved.charge))
}

pub(super) fn hash_agg_output_preflight_for_template(
    template: Vec<PlanField>,
    group_by: &[Expr],
    having: Option<&Expr>,
    aggregates: &[Expr],
    runtime_charge: Option<crate::engine::executor::mem_budget::OwnedMemCharge>,
) -> Option<HashAggPreflight> {
    if template.is_empty()
        && aggregates.iter().any(|aggregate| {
            !matches!(
                aggregate,
                Expr::Function { name, args, distinct: false }
                    if name.eq_ignore_ascii_case("COUNT")
                        && matches!(args.as_slice(), [Expr::Wildcard])
            )
        })
    {
        return None;
    }
    let schema = template_to_batch_schema(&template);
    let (group_columns, _group_programs, _group_numeric_programs, expression_fields) =
        compile_group_columns(group_by, &template, &schema)?;

    let mut aggregate_fields = Vec::with_capacity(aggregates.len());
    let mut aggregate_keys = Vec::with_capacity(aggregates.len());
    for (slot_index, aggregate) in aggregates.iter().enumerate() {
        let (column_type, source) = if let Some((function, input, result_type)) =
            translate_aggregate_extended(aggregate, &template, &schema)
        {
            let mut source = match input {
                AggInputSpec::Column(column)
                    if matches!(
                        function,
                        vec_hash_agg::AggregateKind::Min | vec_hash_agg::AggregateKind::Max
                    ) =>
                {
                    schema.fields()[column].source.clone().nullable_aggregate()
                }
                _ => crate::types::ColumnSource::default(),
            };
            if matches!(
                function,
                vec_hash_agg::AggregateKind::BitAnd
                    | vec_hash_agg::AggregateKind::BitOr
                    | vec_hash_agg::AggregateKind::BitXor
            ) {
                source.flags |= crate::mysql::protocol::UNSIGNED_FLAG;
            }
            (result_type, source)
        } else {
            // Ordered/holistic/spatial and binary BIT aggregates use the
            // exact same row accumulator implementation, but consume values
            // from the current BatchOperator pipeline. Capability discovery
            // therefore remains pre-I/O without forcing a second executor.
            let slot =
                crate::engine::executor::select::aggregate::AggregateSlot::from_expr(aggregate)?;
            let (arg_type, arg_source) = if slot.is_count_star {
                (None, crate::types::ColumnSource::default())
            } else if let Some(argument) = slot.args.first() {
                if let Some(column) = expr_column_index_in_template(argument, &template) {
                    (
                        Some(template[column].column_type),
                        template[column].source.clone(),
                    )
                } else {
                    let program = expr_batch::compile_for_batch(argument, &schema).ok()??;
                    let field =
                        computed_batch_field(&program, "__agg_input", format!("v{slot_index}"))?;
                    (Some(field.column_type), field.source)
                }
            } else {
                (None, crate::types::ColumnSource::default())
            };
            let bit_aggregate = matches!(
                slot.name.to_ascii_uppercase().as_str(),
                "BIT_AND" | "BIT_OR" | "BIT_XOR"
            );
            let descriptor = bit_aggregate.then(|| {
                let argument = arg_type.map(|kind| {
                    crate::engine::expr_type::ExprType::from_column_type(kind, &arg_source)
                });
                let no_column = |_: &Expr| None;
                let no_param = |_: usize| None;
                let context =
                    crate::engine::expr_type::TypeContext::context_free(&no_column, &no_param);
                crate::engine::expr_type::aggregate_result_type(&slot.name, argument, &context)
            });
            let result_type = descriptor.map_or_else(
                || {
                    crate::engine::executor::select::eval::aggregate_function_result_type(
                        &slot.name, arg_type,
                    )
                },
                |descriptor| descriptor.column_type(),
            );
            let mut source = if matches!(slot.name.to_ascii_uppercase().as_str(), "MIN" | "MAX") {
                arg_source.nullable_aggregate()
            } else if slot.name.eq_ignore_ascii_case("GROUP_CONCAT") {
                arg_source
            } else {
                crate::types::ColumnSource::default()
            };
            if let Some(descriptor) = descriptor {
                descriptor.apply_to_source(&mut source);
            }
            (result_type, source)
        };
        aggregate_fields.push(BatchField {
            table: "__agg".into(),
            alias: None,
            name: format!("a{slot_index}"),
            column_type,
            source,
        });
        aggregate_keys.push(crate::engine::executor::select::aggregate::aggregate_key(
            aggregate,
        ));
    }
    let mut fields = schema.fields().to_vec();
    fields.extend(expression_fields);
    let extended_schema = Arc::new(BatchSchema::new(fields));
    let mut output_fields = Vec::with_capacity(group_columns.len() + aggregate_fields.len());
    output_fields.extend(
        group_columns
            .iter()
            .map(|&column| extended_schema.fields()[column].clone()),
    );
    output_fields.extend(aggregate_fields);
    let output_schema = Arc::new(BatchSchema::new(output_fields));
    let output_template = output_schema
        .fields()
        .iter()
        .map(batch_field_template)
        .collect();
    let having_program = compile_hash_agg_having(
        having,
        &output_schema,
        group_columns.len(),
        group_by,
        &aggregate_keys,
        runtime_charge,
    )?;
    Some(HashAggPreflight {
        output_schema,
        output_template,
        group_count: group_columns.len(),
        group_exprs: group_by.to_vec(),
        aggregate_keys,
        having_program,
    })
}

pub(super) struct DeferredHashAggBatchOperator<'a> {
    pub(super) db: &'a Arc<Database>,
    pub(super) txn: TxnView,
    pub(super) plan: &'a PhysicalPlan,
    pub(super) input_plan: &'a PhysicalPlan,
    pub(super) group_by: Vec<Expr>,
    pub(super) aggregates: Vec<Expr>,
    pub(super) schema: Arc<BatchSchema>,
    pub(super) having_program:
        Option<Arc<crate::engine::executor::vec::expr_batch::BatchExprProgram>>,
    pub(super) output: Option<Box<dyn BatchOperator>>,
    pub(super) started: Option<std::time::Instant>,
    pub(super) io_start: Option<(u64, u64)>,
    pub(super) output_rows: u64,
    pub(super) stats_recorded: bool,
    pub(super) force_scalar: bool,
    pub(super) worker_count: usize,
    pub(super) final_filter: Option<vec_hash_agg::AggregateFinalFilter>,
    pub(super) count_topk: Option<vec_hash_agg::AggregateCountTopKHint>,
}

impl DeferredHashAggBatchOperator<'_> {
    pub(super) fn initialize(&mut self) -> Result<(), QueryError> {
        if self.output.is_some() {
            return Ok(());
        }
        self.started = analyze_stats::is_recording().then(std::time::Instant::now);
        self.io_start = self.started.map(|_| self.db.buffer_stats_counts());

        let input = match try_native_vec_input_stream(self.db, self.txn, self.input_plan)? {
            Some(input) => input,
            None => row_batch_input(self.db, self.txn, self.input_plan)?,
        };
        let having_program = self.having_program.take();
        let count_topk = self.count_topk.take();
        let count_topk_active = count_topk.is_some();
        let Some(mut built) = build_vec_hash_agg_stream_from_input(
            self.db,
            input,
            &self.group_by,
            &self.aggregates,
            self.force_scalar,
            self.worker_count,
            self.final_filter.take(),
            count_topk,
        )?
        else {
            return Err(crate::storage::error(
                crate::error::catalog::ER_UNKNOWN_ERROR,
                "batch HashAggregate capability changed after preflight",
            ));
        };
        if built.schema.len() != self.schema.len() {
            return Err(crate::storage::error(
                crate::error::catalog::ER_UNKNOWN_ERROR,
                "batch HashAggregate schema diverged from its preflight metadata",
            ));
        }
        built.metrics.conversion_rows = 0;
        if count_topk_active {
            AGG_COUNT_TOPK_PUSHDOWN_HITS.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        }
        record_pending_agg_metrics(built.metrics);
        forward_vec_hash_agg_metrics(self.plan);
        self.output = Some(if built.having_applied {
            built.stream
        } else if let Some(program) = having_program {
            let properties = built.stream.properties();
            Box::new(FilterBatchStream {
                upstream: built.stream,
                program,
                schema: self.schema.clone(),
                properties,
                // The deferred aggregate owns a `'static` output iterator;
                // its aggregate envelope publishes the node metrics.
                analyze_plan: None,
                analyze_recorded: false,
            })
        } else {
            built.stream
        });
        Ok(())
    }

    pub(super) fn record_stats(&mut self) {
        if self.stats_recorded {
            return;
        }
        self.stats_recorded = true;
        if let (Some(started), Some(io_start)) = (self.started, self.io_start) {
            analyze_stats::record_with_io_snapshot(
                self.db,
                self.plan,
                started.elapsed().as_nanos(),
                self.output_rows,
                io_start,
            );
        }
    }
}

impl BatchOperator for DeferredHashAggBatchOperator<'_> {
    fn schema(&self) -> &Arc<BatchSchema> {
        &self.schema
    }

    fn properties(&self) -> BatchProperties {
        BatchProperties::UNORDERED_SINGLE
    }

    fn next_batch(&mut self) -> Result<Option<Batch>, QueryError> {
        self.initialize()?;
        let batch = self
            .output
            .as_mut()
            .expect("HashAggregate output initialized")
            .next_batch()?;
        if let Some(batch) = batch.as_ref() {
            self.output_rows = self.output_rows.saturating_add(batch.visible_rows() as u64);
        } else {
            self.record_stats();
        }
        Ok(batch)
    }
}

impl Drop for DeferredHashAggBatchOperator<'_> {
    fn drop(&mut self) {
        if self.output.is_some() {
            self.record_stats();
        }
    }
}

pub(super) fn try_build_vec_hash_agg_input<'a>(
    db: &'a Arc<Database>,
    txn: TxnView,
    plan: &'a PhysicalPlan,
) -> Result<Option<VecBatchInput<'a>>, QueryError> {
    try_build_vec_hash_agg_input_with_count_topk(db, txn, plan, None)
}

pub(super) fn try_build_vec_hash_agg_input_with_count_topk<'a>(
    db: &'a Arc<Database>,
    txn: TxnView,
    plan: &'a PhysicalPlan,
    count_topk: Option<vec_hash_agg::AggregateCountTopKHint>,
) -> Result<Option<VecBatchInput<'a>>, QueryError> {
    let Some(aggregate) = plan.ordinary_aggregate_input() else {
        return Ok(None);
    };
    let (input, group_by, having, aggregates, worker_count) = (
        aggregate.input,
        aggregate.group_by,
        aggregate.having,
        aggregate.aggregates,
        aggregate.workers,
    );
    let Some((preflight, final_filter)) =
        prepare_hash_agg_output(db, txn, input, group_by, having, aggregates)?
    else {
        return Ok(None);
    };
    // A key-only ordered aggregate is adjacent-key DISTINCT. Running the
    // hash core here used to pay for both the upstream external sort and a
    // second, cardinality-sized hash table (and its spills). Keep the ordered
    // contract, including duplicate groups that cross batch boundaries.
    if matches!(plan, PhysicalPlan::StreamingAggregate { .. })
        && aggregates.is_empty()
        && !group_by.is_empty()
        && count_topk.is_none()
    {
        let template = plan_output_template(input);
        if let Some(columns) = group_by
            .iter()
            .map(|expr| expr_column_index_in_template(expr, &template))
            .collect::<Option<Vec<_>>>()
        {
            let upstream = match try_native_vec_input_stream(db, txn, input)? {
                Some(upstream) => upstream,
                None => row_batch_input(db, txn, input)?,
            };
            let schema = preflight.output_schema.clone();
            let projected = ProjectColumnsBatchStream {
                upstream: upstream.stream,
                columns,
                schema: schema.clone(),
            };
            let properties = projected.properties();
            let mut stream: Box<dyn BatchOperator + 'a> =
                Box::new(StreamingDistinctBatchOperator {
                    upstream: Box::new(projected),
                    schema: schema.clone(),
                    properties: properties.clone(),
                    key_columns: (0..group_by.len()).collect(),
                    last_key: Vec::new(),
                    has_last_key: false,
                    key_scratch: Vec::new(),
                    aggregate_plan: Some(plan),
                });
            if let Some(program) = preflight.having_program {
                stream = Box::new(FilterBatchStream {
                    upstream: stream,
                    program,
                    schema,
                    properties,
                    analyze_plan: None,
                    analyze_recorded: false,
                });
            }
            // Ordinary aggregates normally own a deferred hash envelope, so
            // the common compiler does not wrap them a second time.
            if analyze_stats::is_recording() {
                stream = Box::new(AnalyzedBatchOperator {
                    plan,
                    inner: stream,
                    active_ns: 0,
                    first_row_ns: None,
                    activated: false,
                    operator_io: Arc::new(crate::storage::QueryBufferStats::default()),
                    started: std::time::Instant::now(),
                    output_rows: 0,
                    batches: 0,
                    recorded: false,
                    runtime_filter_key: None,
                    range_plan_key: None,
                    zone_before: None,
                    partition_visits: None,
                });
            }
            return Ok(Some(VecBatchInput {
                stream,
                template: preflight.output_template,
            }));
        }
    }
    Ok(Some(VecBatchInput {
        stream: Box::new(DeferredHashAggBatchOperator {
            db,
            txn,
            plan,
            input_plan: input,
            group_by: group_by.to_vec(),
            aggregates: aggregates.to_vec(),
            schema: preflight.output_schema,
            having_program: preflight.having_program,
            output: None,
            started: None,
            io_start: None,
            output_rows: 0,
            stats_recorded: false,
            force_scalar: fixed_scale_approximate_aggregate(aggregates, input),
            worker_count,
            final_filter,
            count_topk,
        }),
        template: preflight.output_template,
    }))
}

/// Try to execute a GROUP BY / aggregate plan via the vectorized
/// `HashAggregateCore`. Returns `Ok(None)` on any unsupported shape:
/// - DISTINCT variants without a partitionable native state
/// - GROUP BY expressions outside the shared batch/legacy expression compiler
/// - aggregate functions without a native `AggregateKind` state
/// HAVING is compiled against the finalized aggregate schema and applied as
/// a selection mask on the output batches.
///
/// The output schema is `[group_columns..., agg0, agg1, ...]` with
/// PlanField metadata reconstructed from the upstream template plus
/// synthetic agg-call labels.
/// Output bundle from `build_vec_hash_agg_stream` — everything a
/// downstream consumer needs to either re-materialise the result back
/// into the legacy `PlanRow` pipeline or feed the `BatchOperator`
/// directly into the next vectorized operator.
pub(in crate::engine::executor::select) struct HashAggregateStream {
    pub stream: Box<dyn BatchOperator>,
    pub group_template: Vec<PlanField>,
    pub agg_keys: Vec<u64>,
    pub aggregates: Vec<Expr>,
    /// Output BatchSchema. Currently only used by downstream wiring; kept
    /// public so future fused paths (e.g. HashAgg → HashJoin) can resolve
    /// columns without re-deriving the layout.
    #[allow(dead_code)]
    pub schema: Arc<BatchSchema>,
    pub metrics: vec_hash_agg::AggMetrics,
    pub having_applied: bool,
}

/// One output of the exact-integer affine SUM kernel. `source` addresses a
/// pair of physical partials (`SUM(column)`, `COUNT(column)`) in the reduced
/// HashAggregate output. Finalization applies the affine transform once per
/// group instead of materialising the same source lane once per SQL aggregate.
#[derive(Debug, Clone, Copy)]
pub(super) struct AffineSumProjection {
    pub(super) source: usize,
    pub(super) coefficient: i8,
    pub(super) offset: i64,
}

pub(super) struct AffineSumFinalizeBatchOperator {
    pub(super) inner: Box<dyn BatchOperator>,
    pub(super) schema: Arc<BatchSchema>,
    pub(super) projections: Vec<AffineSumProjection>,
    pub(super) done: bool,
}

impl AffineSumFinalizeBatchOperator {
    pub(super) fn derive(
        sum: Value,
        count: Value,
        projection: AffineSumProjection,
    ) -> Result<Value, QueryError> {
        if matches!(sum, Value::Null) {
            return Ok(Value::Null);
        }
        let count = match count {
            Value::Int(count) if count >= 0 => count,
            _ => {
                return Err(crate::storage::error(
                    crate::error::catalog::ER_INTERNAL_ERROR,
                    "affine SUM received an invalid COUNT partial",
                ));
            }
        };
        let mut value = sum;
        if projection.coefficient == -1 {
            value = crate::engine::expr::eval_arithmetic(
                &Value::DecimalCompact(-1, 0),
                BinaryOp::Mul,
                &value,
            )?;
        }
        if projection.offset != 0 {
            // Shape COUNT as DECIMAL before multiplication. A valid table may
            // contain enough rows for count*offset to exceed BIGINT even when
            // every row-level affine expression is a valid BIGINT; SUM is an
            // exact DECIMAL aggregate and must not inherit that intermediate
            // BIGINT limit during the once-per-group derivation.
            let contribution = crate::engine::expr::eval_arithmetic(
                &Value::DecimalCompact(count, 0),
                BinaryOp::Mul,
                &Value::Int(projection.offset),
            )?;
            value = crate::engine::expr::eval_arithmetic(&value, BinaryOp::Add, &contribution)?;
        }
        Ok(value)
    }
}

impl BatchOperator for AffineSumFinalizeBatchOperator {
    fn schema(&self) -> &Arc<BatchSchema> {
        &self.schema
    }

    fn properties(&self) -> BatchProperties {
        BatchProperties::UNORDERED_SINGLE
    }

    fn next_batch(&mut self) -> Result<Option<Batch>, QueryError> {
        if self.done {
            return Ok(None);
        }
        let Some(batch) = self.inner.next_batch()? else {
            return Err(crate::storage::error(
                crate::error::catalog::ER_INTERNAL_ERROR,
                "implicit affine SUM produced no aggregate row",
            ));
        };
        let mut visible = batch.visible_indices();
        let row = visible.next().ok_or_else(|| {
            crate::storage::error(
                crate::error::catalog::ER_INTERNAL_ERROR,
                "implicit affine SUM produced an invisible aggregate row",
            )
        })?;
        if visible.next().is_some() {
            return Err(crate::storage::error(
                crate::error::catalog::ER_INTERNAL_ERROR,
                "implicit affine SUM produced multiple aggregate rows",
            ));
        }
        let mut columns = Vec::with_capacity(self.projections.len());
        for projection in &self.projections {
            let sum_column = projection.source.saturating_mul(2);
            let count_column = sum_column.saturating_add(1);
            let sum = batch
                .columns()
                .get(sum_column)
                .ok_or_else(|| {
                    crate::storage::error(
                        crate::error::catalog::ER_INTERNAL_ERROR,
                        "affine SUM lost its SUM partial",
                    )
                })?
                .value_at(row);
            let count = batch
                .columns()
                .get(count_column)
                .ok_or_else(|| {
                    crate::storage::error(
                        crate::error::catalog::ER_INTERNAL_ERROR,
                        "affine SUM lost its COUNT partial",
                    )
                })?
                .value_at(row);
            columns.push(ColumnVector::from_values(vec![Self::derive(
                sum,
                count,
                *projection,
            )?]));
        }
        self.done = true;
        Ok(Some(Batch::with_row_count(self.schema.clone(), columns, 1)))
    }
}

pub(super) fn signed_integer_bounds(field: &BatchField) -> Option<(i64, i64)> {
    if field.source.flags & crate::mysql::protocol::UNSIGNED_FLAG != 0 {
        return None;
    }
    Some(match field.column_type {
        ColumnType::Tiny => (i8::MIN.into(), i8::MAX.into()),
        ColumnType::Short => (i16::MIN.into(), i16::MAX.into()),
        ColumnType::MediumInt => (-8_388_608, 8_388_607),
        ColumnType::Long => (i32::MIN.into(), i32::MAX.into()),
        ColumnType::LongLong => (i64::MIN, i64::MAX),
        _ => return None,
    })
}

pub(super) fn affine_integer_constant(expr: &Expr) -> Option<i64> {
    if !matches!(expr, Expr::Literal(_) | Expr::BoundValue(_)) {
        return None;
    }
    match crate::engine::expr::eval_const(expr).ok()? {
        Value::Int(value) => Some(value),
        Value::UInt(value) => i64::try_from(value).ok(),
        _ => None,
    }
}

/// Recognise exact signed-integer `SUM(a*column + b)` expressions for which
/// the declared column range proves that row-level BIGINT arithmetic cannot
/// overflow. The accepted coefficient is ±1 and the SQL shapes are the common
/// `column`, `column +/- constant`, and `constant - column` forms.
///
/// This is a generic aggregate-state factorisation, not a workload rewrite:
/// every distinct source lane is summarized once as SUM+COUNT, and all SQL
/// outputs are reconstructed from those canonical partials. Approximate,
/// unsigned, DECIMAL, volatile and error-capable expressions retain the normal
/// expression evaluator as their correctness oracle.
pub(super) fn compile_affine_sum_family(
    aggregates: &[Expr],
    template: &[PlanField],
    schema: &BatchSchema,
) -> Option<(Vec<vec_hash_agg::AggregateSpec>, Vec<AffineSumProjection>)> {
    if aggregates.is_empty() {
        return None;
    }
    let mut sources = Vec::<usize>::new();
    let mut projections = Vec::with_capacity(aggregates.len());
    let mut needs_factorisation = false;
    for aggregate in aggregates {
        let slot = crate::engine::executor::select::aggregate::AggregateSlot::from_expr(aggregate)?;
        if !slot.name.eq_ignore_ascii_case("SUM") || slot.distinct || slot.args.len() != 1 {
            return None;
        }
        let argument = &slot.args[0];
        let (column, coefficient, offset) =
            if let Some(column) = expr_column_index_in_template(argument, template) {
                (column, 1i8, 0i64)
            } else if let Expr::Binary { left, op, right } = argument {
                match op {
                    BinaryOp::Add => {
                        if let (Some(column), Some(offset)) = (
                            expr_column_index_in_template(left, template),
                            affine_integer_constant(right),
                        ) {
                            (column, 1, offset)
                        } else {
                            (
                                expr_column_index_in_template(right, template)?,
                                1,
                                affine_integer_constant(left)?,
                            )
                        }
                    }
                    BinaryOp::Sub => {
                        if let Some(column) = expr_column_index_in_template(left, template) {
                            (column, 1, affine_integer_constant(right)?.checked_neg()?)
                        } else {
                            (
                                expr_column_index_in_template(right, template)?,
                                -1,
                                affine_integer_constant(left)?,
                            )
                        }
                    }
                    _ => return None,
                }
            } else {
                return None;
            };
        let field = schema.fields().get(column)?;
        let (minimum, maximum) = signed_integer_bounds(field)?;
        let endpoint = |value: i64| {
            i128::from(coefficient)
                .checked_mul(i128::from(value))?
                .checked_add(i128::from(offset))
        };
        let low = endpoint(minimum)?;
        let high = endpoint(maximum)?;
        if low.min(high) < i128::from(i64::MIN) || low.max(high) > i128::from(i64::MAX) {
            return None;
        }
        let source = match sources.iter().position(|candidate| *candidate == column) {
            Some(source) => {
                needs_factorisation = true;
                source
            }
            None => {
                sources.push(column);
                sources.len() - 1
            }
        };
        needs_factorisation |= coefficient != 1 || offset != 0;
        projections.push(AffineSumProjection {
            source,
            coefficient,
            offset,
        });
    }
    if !needs_factorisation {
        return None;
    }
    let mut specs = Vec::with_capacity(sources.len().saturating_mul(2));
    for input_column in sources {
        specs.push(vec_hash_agg::AggregateSpec {
            comparison_collation: None,
            func: vec_hash_agg::AggregateKind::Sum,
            input_column,
        });
        specs.push(vec_hash_agg::AggregateSpec {
            comparison_collation: None,
            func: vec_hash_agg::AggregateKind::Count,
            input_column,
        });
    }
    Some((specs, projections))
}

pub(super) struct AggregateOutputCoercionBatchOperator {
    pub(super) inner: Box<dyn BatchOperator>,
    pub(super) schema: Arc<BatchSchema>,
    pub(super) group_count: usize,
}

pub(in crate::engine::executor::select) fn decimal_value_at_scale(
    value: Value,
    target_scale: u8,
) -> Value {
    fn from_magnitude(magnitude: i128, scale: u8) -> Value {
        match i64::try_from(magnitude) {
            Ok(value) => Value::DecimalCompact(value, scale),
            Err(_) => Value::Decimal128(magnitude, scale),
        }
    }
    match value {
        Value::Null => Value::Null,
        Value::DecimalCompact(magnitude, scale) => {
            crate::util::decimal::rescale_round_i128(i128::from(magnitude), scale, target_scale)
                .map(|magnitude| from_magnitude(magnitude, target_scale))
                .unwrap_or(Value::DecimalCompact(magnitude, scale))
        }
        Value::Decimal128(magnitude, scale) => {
            crate::util::decimal::rescale_round_i128(magnitude, scale, target_scale)
                .map(|magnitude| from_magnitude(magnitude, target_scale))
                .unwrap_or(Value::Decimal128(magnitude, scale))
        }
        Value::DecimalWide(value) => crate::util::decimal::rescale_decimal_big_str(
            &Value::DecimalWide(value.clone()).to_display_string(),
            target_scale,
        )
        .map(Value::Decimal)
        .unwrap_or(Value::DecimalWide(value)),
        Value::Decimal(value) => {
            crate::util::decimal::rescale_decimal_big_str(&value, target_scale)
                .map(Value::Decimal)
                .unwrap_or(Value::Decimal(value))
        }
        Value::Int(value) => crate::util::decimal::pow10_i128(u32::from(target_scale))
            .and_then(|factor| i128::from(value).checked_mul(factor))
            .map(|magnitude| from_magnitude(magnitude, target_scale))
            .unwrap_or(Value::Int(value)),
        Value::UInt(value) => crate::util::decimal::pow10_i128(u32::from(target_scale))
            .and_then(|factor| i128::from(value).checked_mul(factor))
            .map(|magnitude| from_magnitude(magnitude, target_scale))
            .unwrap_or(Value::UInt(value)),
        value => Value::Decimal(format!(
            "{:.scale$}",
            crate::engine::expr::numeric(&value).unwrap_or(0.0),
            scale = usize::from(target_scale)
        )),
    }
}

impl BatchOperator for AggregateOutputCoercionBatchOperator {
    fn schema(&self) -> &Arc<BatchSchema> {
        &self.schema
    }

    fn properties(&self) -> BatchProperties {
        self.inner.properties()
    }

    fn next_batch(&mut self) -> Result<Option<Batch>, QueryError> {
        let Some(mut batch) = self.inner.next_batch()? else {
            return Ok(None);
        };
        for column_index in self.group_count..batch.columns.len() {
            let Some(ColumnType::Decimal { scale, .. }) = self
                .schema
                .fields()
                .get(column_index)
                .map(|field| field.column_type)
            else {
                continue;
            };
            let values = (0..batch.num_rows)
                .map(|row| decimal_value_at_scale(batch.columns[column_index].value_at(row), scale))
                .collect();
            batch.try_columns_mut()?[column_index] = ColumnVector::from_values(values);
        }
        batch.schema = self.schema.clone();
        Ok(Some(batch))
    }
}

pub(super) fn coerce_aggregate_output_stream(
    stream: Box<dyn BatchOperator>,
    schema: Arc<BatchSchema>,
    group_count: usize,
) -> Box<dyn BatchOperator> {
    Box::new(AggregateOutputCoercionBatchOperator {
        inner: stream,
        schema,
        group_count,
    })
}

pub(super) fn semantic_aggregate_argument_type(
    argument: &Expr,
    template: &[PlanField],
    fallback: Option<ColumnType>,
) -> Option<ColumnType> {
    let sample = PlanRow {
        fields: template.to_vec(),
        group: None,
        agg_values: None,
        schema: PlanRow::empty_schema(),
    };
    let resolved = crate::engine::executor::select::eval::plan_expr_result_type(argument, &sample)
        .ok()
        .or(fallback);
    crate::engine::executor::select::eval::aggregate_argument_semantic_type(
        Some(argument),
        resolved,
    )
}

pub(super) fn aggregate_requires_semantic_input_coercion(
    aggregate: &Expr,
    template: &[PlanField],
    schema: &BatchSchema,
) -> bool {
    let Expr::Function { args, .. } = aggregate else {
        return false;
    };
    let Some(argument) = args
        .first()
        .filter(|argument| !matches!(argument, Expr::Wildcard))
    else {
        return false;
    };
    let physical = expr_column_index_in_template(argument, template)
        .map(|column| template[column].column_type)
        .or_else(|| {
            expr_batch::compile_for_batch(argument, schema)
                .ok()
                .flatten()
                .map(|program| program.result_type().column_type())
        });
    let semantic = semantic_aggregate_argument_type(argument, template, physical);
    matches!(semantic, Some(ColumnType::Float | ColumnType::Double))
        && !matches!(physical, Some(ColumnType::Float | ColumnType::Double))
}

/// Build a streaming HashAggregate operator over `agg_input`. Returns
/// `Ok(None)` for any shape the vec engine can't handle, so the caller
/// can fall back to the legacy path. On success the caller gets a
/// `BatchOperator` of `[group_cols..., finalized_aggs...]` plus enough
/// metadata to reconstruct `PlanRow`s or to resolve ORDER BY references
/// in a fused downstream operator (TopN / Sort / further HashAgg).
///
/// The metrics this function records are *not* forwarded to
/// `analyze_stats` here — the caller knows which `PhysicalPlan` node the
/// metrics should land against, so the caller calls
/// `record_pending_agg_metrics` or `forward_vec_hash_agg_metrics`
/// directly.
pub(in crate::engine::executor::select) fn build_vec_hash_agg_stream(
    db: &Arc<Database>,
    txn: TxnView,
    agg_input: &PhysicalPlan,
    group_by: &[Expr],
    having: Option<&Expr>,
    aggregates: &[Expr],
) -> Result<Option<HashAggregateStream>, QueryError> {
    build_vec_hash_agg_stream_with_count_topk(
        db, txn, agg_input, group_by, having, aggregates, None,
    )
}

pub(super) fn build_vec_hash_agg_stream_with_count_topk(
    db: &Arc<Database>,
    txn: TxnView,
    agg_input: &PhysicalPlan,
    group_by: &[Expr],
    having: Option<&Expr>,
    aggregates: &[Expr],
    count_topk: Option<vec_hash_agg::AggregateCountTopKHint>,
) -> Result<Option<HashAggregateStream>, QueryError> {
    if !vectorize_enabled_for_plan(agg_input) {
        return Ok(None);
    }
    let Some((preflight, final_filter)) =
        prepare_hash_agg_output(db, txn, agg_input, group_by, having, aggregates)?
    else {
        return Ok(None);
    };

    // Build the input BatchOperator. For HashJoin / similar vec-eligible
    // inputs this skips the PlanRow round-trip; otherwise we wrap the
    // legacy row-based stream in a row→batch adapter so the rest of the
    // code is shape-agnostic.
    let input = match try_native_vec_input_stream(db, txn, agg_input)? {
        Some(input) => input,
        None => row_batch_input(db, txn, agg_input)?,
    };

    let Some(mut built) = build_vec_hash_agg_stream_from_input(
        db,
        input,
        group_by,
        aggregates,
        fixed_scale_approximate_aggregate(aggregates, agg_input),
        scheduler_hash_agg_workers(),
        final_filter,
        count_topk,
    )?
    else {
        return Ok(None);
    };
    if built.schema.len() != preflight.output_schema.len() {
        return Err(crate::storage::error(
            crate::error::catalog::ER_UNKNOWN_ERROR,
            "batch HashAggregate schema diverged from its preflight metadata",
        ));
    }
    if !built.having_applied {
        if let Some(program) = preflight.having_program {
            let properties = built.stream.properties();
            built.stream = Box::new(FilterBatchStream {
                upstream: built.stream,
                program,
                schema: built.schema.clone(),
                properties,
                analyze_plan: None,
                analyze_recorded: false,
            });
        }
    }
    Ok(Some(built))
}

pub(super) fn build_vec_hash_agg_stream_from_input(
    db: &Arc<Database>,
    input: VecBatchInput<'_>,
    group_by: &[Expr],
    aggregates: &[Expr],
    force_scalar: bool,
    worker_count: usize,
    final_filter: Option<vec_hash_agg::AggregateFinalFilter>,
    count_topk: Option<vec_hash_agg::AggregateCountTopKHint>,
) -> Result<Option<HashAggregateStream>, QueryError> {
    let base_schema = template_to_batch_schema(&input.template);
    if force_scalar
        || aggregates.iter().any(|aggregate| {
            translate_aggregate_extended(aggregate, &input.template, &base_schema).is_none()
                || aggregate_requires_semantic_input_coercion(
                    aggregate,
                    &input.template,
                    &base_schema,
                )
        })
    {
        return build_scalar_aggregate_kernel_stream(input, group_by, aggregates);
    }
    let VecBatchInput {
        stream: mut input_stream,
        template,
    } = input;

    let schema = template_to_batch_schema(&template);
    let base_col_count = template.len();

    // An implicit aggregate whose exact-integer SUM expressions are affine in
    // source columns needs only one SUM+COUNT partial per distinct source.
    // This avoids O(rows * output expressions) temporary lanes while keeping
    // the physical Aggregate node, parallel scheduler and memory accounting
    // unchanged. HAVING is applied to the reconstructed original output by
    // the enclosing preflight operator.
    if group_by.is_empty() && final_filter.is_none() {
        if let Some((reduced_specs, projections)) =
            compile_affine_sum_family(aggregates, &template, &schema)
        {
            let Some(original_preflight) = hash_agg_output_preflight_for_template(
                template.clone(),
                group_by,
                None,
                aggregates,
                None,
            ) else {
                return Ok(None);
            };
            let original_schema = original_preflight.output_schema;
            let reduced_schema = build_hash_agg_output_schema(&schema, &[], &reduced_specs);
            let (stream, mut metrics, _having_applied) =
                crate::engine::executor::vec::scheduler::hash_agg_driver::run_hash_agg_parallel_stream(
                    input_stream.as_mut(),
                    Vec::new(),
                    reduced_specs,
                    reduced_schema.clone(),
                    schema.clone(),
                    worker_count,
                    mem_budget::current_budget(),
                    db.data_dir().to_path_buf(),
                    None,
                    None,
                )?;
            metrics.expression_mode = Some("affine");
            metrics.native_expression_rows = metrics.native_aggregate_rows;
            let stream = coerce_aggregate_output_stream(stream, reduced_schema, 0);
            let aggregate_keys = aggregates
                .iter()
                .map(crate::engine::executor::select::aggregate::aggregate_key)
                .collect();
            return Ok(Some(HashAggregateStream {
                stream: Box::new(AffineSumFinalizeBatchOperator {
                    inner: stream,
                    schema: original_schema.clone(),
                    projections,
                    done: false,
                }),
                group_template: Vec::new(),
                agg_keys: aggregate_keys,
                aggregates: aggregates.to_vec(),
                schema: original_schema,
                metrics,
                having_applied: false,
            }));
        }
    }

    let Some((group_cols, mut expr_programs, mut numeric_programs, mut extended_fields)) =
        compile_group_columns(group_by, &template, &schema)
    else {
        return Ok(None);
    };

    // Translate aggregates, potentially with expression-based inputs.
    // Expression arguments (e.g. `SUM(price * discount)`) are compiled
    // and pre-computed per-batch via `ComputeColumnsBatchStream`.
    let mut agg_specs: Vec<vec_hash_agg::AggregateSpec> = Vec::new();
    let mut agg_keys: Vec<u64> = Vec::new();
    let group_expression_count = expr_programs.len();
    let mut computed_aggregate_exprs = Vec::new();

    for agg in aggregates {
        let Some((func, input_spec, _result_type)) =
            translate_aggregate_extended(agg, &template, &schema)
        else {
            return Ok(None);
        };
        agg_keys.push(crate::engine::executor::select::aggregate::aggregate_key(
            agg,
        ));
        match input_spec {
            AggInputSpec::Column(col) => {
                agg_specs.push(vec_hash_agg::AggregateSpec {
                    comparison_collation: schema.fields()[col].source.collation_id,
                    func,
                    input_column: col,
                });
            }
            AggInputSpec::Expr(program) => {
                let virtual_idx = base_col_count + expr_programs.len();
                let Some(field) = computed_batch_field(
                    &program,
                    "__agg_input",
                    format!("v{}", expr_programs.len()),
                ) else {
                    return Ok(None);
                };
                let numeric =
                    crate::engine::executor::select::aggregate::AggregateSlot::from_expr(agg)
                        .and_then(|slot| slot.args.first().cloned())
                        .and_then(|expr| {
                            ComputedNumericProgram::compile(&expr, &template, &schema)
                        });
                if group_expression_count == 0 {
                    let Some(expression) =
                        crate::engine::executor::select::aggregate::AggregateSlot::from_expr(agg)
                            .and_then(|slot| slot.args.first().cloned())
                    else {
                        return Ok(None);
                    };
                    computed_aggregate_exprs.push(expression);
                }
                expr_programs.push(program);
                numeric_programs.push(numeric);
                extended_fields.push(field);
                agg_specs.push(vec_hash_agg::AggregateSpec {
                    comparison_collation: extended_fields
                        .last()
                        .and_then(|field| field.source.collation_id),
                    func,
                    input_column: virtual_idx,
                });
            }
        }
    }

    // Prefixes are safe only if every aggregate/group consumer agrees.
    // Raw aggregate inputs and grouping keys always require complete values.
    // Filters and other intermediate operators enforce their own needs while
    // forwarding this optional, pre-I/O request to the physical scan.
    let mut prefix_limits = std::collections::HashMap::<usize, usize>::new();
    let mut full_columns: std::collections::HashSet<usize> = group_cols
        .iter()
        .copied()
        .chain(agg_specs.iter().map(|spec| spec.input_column))
        .filter(|column| *column < base_col_count)
        .collect();
    for program in &expr_programs {
        let limits = program.column_prefix_limits();
        full_columns.extend(
            program
                .referenced_columns()
                .iter()
                .copied()
                .filter(|column| !limits.iter().any(|(limited, _)| limited == column)),
        );
        for (column, limit) in limits {
            prefix_limits
                .entry(column)
                .and_modify(|old| *old = (*old).max(limit))
                .or_insert(limit);
        }
    }
    let limits: Vec<_> = prefix_limits
        .into_iter()
        .filter(|(column, _)| !full_columns.contains(column))
        .collect();
    if !limits.is_empty() {
        input_stream.limit_column_prefixes(&limits);
    }

    // If there are expression-based aggregates, extend the schema and
    // wrap the input stream with a ComputeColumnsBatchStream that
    // evaluates the expressions per-batch and appends result columns.
    let has_computed_expressions = !expr_programs.is_empty();
    let computed_expressions_native = expr_programs
        .iter()
        .all(|program| !program.capability().legacy_escape);
    let mut worker_transform: Option<
        Arc<dyn crate::engine::executor::vec::scheduler::hash_agg_driver::HashAggBatchTransform>,
    > = None;
    let (final_schema, mut input_stream): (Arc<BatchSchema>, Box<dyn BatchOperator + '_>) =
        if expr_programs.is_empty() {
            (schema, input_stream)
        } else {
            let mut all_fields: Vec<BatchField> = schema.fields().to_vec();
            all_fields.extend(extended_fields);
            let extended_schema = Arc::new(BatchSchema::new(all_fields));
            let row_major = expr_programs.len() > 1
                && expr_programs
                    .iter()
                    .any(|program| program.capability().requires_ordered_evaluation());
            let direct_computed = group_expression_count == 0
                && computed_aggregate_exprs.len() == expr_programs.len()
                && agg_specs.iter().all(|spec| {
                    matches!(
                        spec.func,
                        vec_hash_agg::AggregateKind::Count | vec_hash_agg::AggregateKind::CountStar
                    )
                });
            // One temporal extraction over a typed, unordered input observes
            // only the captured read-only session (not row sequencing). Its
            // generic capability includes string-coercion errors, but those
            // operands cannot occur on these physical temporal lanes. Keep
            // multi-expression error/warning precedence on the coordinator.
            let typed_temporal =
                native_temporal_worker_eligible(&expr_programs, input_stream.as_ref());
            let parallel_safe = worker_count > 1
                && !direct_computed
                && (typed_temporal
                    || expr_programs.iter().all(|program| {
                        // Existing temporal leaves retain their ordering-aware
                        // admission above; only new compositions use this proof.
                        program.as_temporal_extract().is_none()
                            && program.column_parallel_safe(&schema)
                    })
                    || expr_programs.iter().all(|program| {
                        let capability = program.capability();
                        !capability.may_error
                            && capability.deterministic
                            && !capability.depends_on_session
                    }));
            if parallel_safe {
                worker_transform = Some(Arc::new(ComputeColumnsWorkerTransform {
                    programs: expr_programs,
                    numeric_programs,
                    schema: extended_schema.clone(),
                    row_major,
                }));
                (extended_schema, input_stream)
            } else {
                let compute_stream = ComputeColumnsBatchStream {
                    pending_chunks: None,
                    inner: input_stream,
                    programs: expr_programs,
                    numeric_programs,
                    direct_expressions: computed_aggregate_exprs,
                    schema: extended_schema.clone(),
                    row_major,
                };
                (extended_schema, Box::new(compute_stream))
            }
        };

    // Choose parallel scheduler vs. serial path. After a small startup
    // sample, the scheduler feeds a worker pool through a bounded
    // 2×worker queue, each worker running its own HashAggregateCore; partition
    // partials are then merged on the main thread. A query budget makes
    // every worker spill-capable and the producer observes worker aborts,
    // so neither input collection nor a failed worker can grow/block the
    // pipeline without bound.
    // Worker count comes from the public parallel resource configuration.
    // An adjacent HashJoin may expose match-index batches directly to this
    // aggregate. Admission is checked without pulling either child; once
    // accepted, both operators keep their ordinary semantics and state/spill
    // implementations while skipping joined payload materialization.
    let mut direct_aggregate = if let Some(budget) = mem_budget::current_budget() {
        vec_hash_agg::HashAggregateCore::with_budget(
            group_cols.clone(),
            agg_specs.clone(),
            final_schema.clone(),
            budget,
            db.data_dir().to_path_buf(),
        )
    } else {
        vec_hash_agg::HashAggregateCore::new(group_cols.clone(), agg_specs.clone())
    };
    if worker_transform.is_none()
        && input_stream.consume_into_hash_aggregate(&mut direct_aggregate)?
    {
        let metrics = direct_aggregate.metrics();
        let out_schema = direct_aggregate.output_schema();
        let group_template = group_cols
            .iter()
            .map(|&index| batch_field_template(&final_schema.fields()[index]))
            .collect();
        let out_stream = direct_aggregate.finalize_stream()?;
        return Ok(Some(HashAggregateStream {
            stream: coerce_aggregate_output_stream(
                out_stream,
                out_schema.clone(),
                group_cols.len(),
            ),
            group_template,
            agg_keys,
            aggregates: aggregates.to_vec(),
            schema: out_schema,
            metrics,
            having_applied: false,
        }));
    }

    let use_scheduler = worker_count > 1;
    if use_scheduler {
        // Build the output schema *before* moving `agg_specs` into the
        // scheduler, so we don't need to round-trip through a real
        // HashAggregateCore::output_schema for the same answer.
        let out_schema = build_hash_agg_output_schema(&final_schema, &group_cols, &agg_specs);
        let group_template: Vec<PlanField> = group_cols
            .iter()
            .map(|&index| batch_field_template(&final_schema.fields()[index]))
            .collect();
        let (out_stream, mut metrics, having_applied) =
            crate::engine::executor::vec::scheduler::hash_agg_driver::run_hash_agg_parallel_stream_with_count_topk(
                input_stream.as_mut(),
                group_cols.clone(),
                agg_specs.clone(),
                out_schema.clone(),
                final_schema.clone(),
                worker_count,
                mem_budget::current_budget(),
                db.data_dir().to_path_buf(),
                worker_transform,
                final_filter,
                count_topk,
            )?;
        if has_computed_expressions {
            metrics.expression_mode = Some(if metrics.fallback_aggregate_rows > 0 {
                "scalar"
            } else if !computed_expressions_native {
                "legacy_escape"
            } else {
                "compiled"
            });
            if computed_expressions_native {
                metrics.native_expression_rows = metrics.native_aggregate_rows;
                metrics.fallback_expression_rows = metrics.fallback_aggregate_rows;
            } else {
                metrics.native_expression_rows = 0;
                metrics.fallback_expression_rows = metrics
                    .native_aggregate_rows
                    .saturating_add(metrics.fallback_aggregate_rows);
            }
        }
        return Ok(Some(HashAggregateStream {
            stream: coerce_aggregate_output_stream(
                out_stream,
                out_schema.clone(),
                group_cols.len(),
            ),
            group_template,
            agg_keys,
            aggregates: aggregates.to_vec(),
            schema: out_schema,
            metrics,
            having_applied,
        }));
    }

    // PGA wiring: when a per-query budget is active (e.g. CTE-wrapped
    // or test-driven query), go through `with_budget` so the operator
    // gets a spill path. Otherwise stay on `new()` — it still
    // registers against the WorkAreaManager (for advisor counters)
    // but won't try to spill, matching the historical no-budget
    // contract for COM_QUERY paths.
    let (agg, mut metrics) = if let Some(budget) = mem_budget::current_budget() {
        let data_dir = db.data_dir().to_path_buf();
        let mut agg = vec_hash_agg::HashAggregateCore::with_budget(
            group_cols.clone(),
            agg_specs.clone(),
            final_schema.clone(),
            budget,
            data_dir,
        );
        agg.consume_stream(input_stream.as_mut())?;
        let m = agg.metrics();
        (agg, m)
    } else {
        let mut agg = vec_hash_agg::HashAggregateCore::new(group_cols.clone(), agg_specs.clone());
        agg.consume_stream(input_stream.as_mut())?;
        let m = agg.metrics();
        (agg, m)
    };
    if has_computed_expressions {
        metrics.expression_mode = Some(if metrics.fallback_aggregate_rows > 0 {
            "scalar"
        } else if !computed_expressions_native {
            "legacy_escape"
        } else {
            "compiled"
        });
        if computed_expressions_native {
            metrics.native_expression_rows = metrics.native_aggregate_rows;
            metrics.fallback_expression_rows = metrics.fallback_aggregate_rows;
        } else {
            metrics.native_expression_rows = 0;
            metrics.fallback_expression_rows = metrics
                .native_aggregate_rows
                .saturating_add(metrics.fallback_aggregate_rows);
        }
    }

    let out_schema = agg.output_schema();
    let group_template: Vec<PlanField> = group_cols
        .iter()
        .map(|&index| batch_field_template(&final_schema.fields()[index]))
        .collect();
    let out_stream = agg.finalize_stream()?;

    Ok(Some(HashAggregateStream {
        stream: coerce_aggregate_output_stream(out_stream, out_schema.clone(), group_cols.len()),
        group_template,
        agg_keys,
        aggregates: aggregates.to_vec(),
        schema: out_schema,
        metrics,
        having_applied: false,
    }))
}

pub(super) fn append_scalar_aggregate_expression(
    expr: &Expr,
    template: &[PlanField],
    base_schema: &BatchSchema,
    programs: &mut Vec<Arc<ExprProgram>>,
    fields: &mut Vec<BatchField>,
    label: String,
) -> Option<usize> {
    if let Some(column) = expr_column_index_in_template(expr, template) {
        return Some(column);
    }
    let program = expr_batch::compile_for_batch(expr, base_schema).ok()??;
    let field = computed_batch_field(&program, "__agg_scalar", label).or_else(|| {
        // The legacy evaluator can intentionally return a dynamic type. Its
        // ColumnVector is still self-describing; Blob here is only conservative
        // metadata for this private lane and never reaches the client schema.
        Some(BatchField {
            table: "__agg_scalar".into(),
            alias: None,
            name: format!("v{}", fields.len()),
            column_type: ColumnType::Blob,
            source: crate::types::ColumnSource::default(),
        })
    })?;
    let column = template.len() + programs.len();
    programs.push(program);
    fields.push(field);
    Some(column)
}

pub(super) fn build_scalar_aggregate_kernel_stream<'a>(
    input: VecBatchInput<'a>,
    group_by: &[Expr],
    aggregates: &[Expr],
) -> Result<Option<HashAggregateStream>, QueryError> {
    let VecBatchInput {
        stream: input_stream,
        template,
    } = input;
    let base_schema = template_to_batch_schema(&template);
    let Some((group_columns, mut programs, _group_numeric_programs, mut private_fields)) =
        compile_group_columns(group_by, &template, &base_schema)
    else {
        return Ok(None);
    };

    let mut runtime_slots = Vec::with_capacity(aggregates.len());
    let mut aggregate_keys = Vec::with_capacity(aggregates.len());
    let mut output_aggregate_fields = Vec::with_capacity(aggregates.len());
    for (slot_index, aggregate) in aggregates.iter().enumerate() {
        let Some(slot) =
            crate::engine::executor::select::aggregate::AggregateSlot::from_expr(aggregate)
        else {
            return Ok(None);
        };
        let input_column = if slot.is_count_star {
            None
        } else {
            let Some(argument) = slot.args.first() else {
                return Ok(None);
            };
            append_scalar_aggregate_expression(
                argument,
                &template,
                &base_schema,
                &mut programs,
                &mut private_fields,
                format!("a{slot_index}"),
            )
        };
        if !slot.is_count_star && input_column.is_none() {
            return Ok(None);
        }
        let mut order_columns = Vec::with_capacity(slot.order_by.len());
        for (order_index, order) in slot.order_by.iter().enumerate() {
            let Some(column) = append_scalar_aggregate_expression(
                &order.expr,
                &template,
                &base_schema,
                &mut programs,
                &mut private_fields,
                format!("a{slot_index}_o{order_index}"),
            ) else {
                return Ok(None);
            };
            order_columns.push(column);
        }

        let physical_arg_type = input_column.and_then(|column| {
            if column < template.len() {
                Some(template[column].column_type)
            } else {
                private_fields
                    .get(column - template.len())
                    .map(|field| field.column_type)
            }
        });
        let arg_type = slot
            .args
            .first()
            .and_then(|argument| {
                semantic_aggregate_argument_type(argument, &template, physical_arg_type)
            })
            .or(physical_arg_type);
        let input_field = input_column.and_then(|column| {
            if column < base_schema.len() {
                base_schema.fields().get(column)
            } else {
                private_fields.get(column - template.len())
            }
        });
        let fixed_output_scale = input_field.and_then(|field| {
            if !matches!(field.column_type, ColumnType::Float | ColumnType::Double) {
                return None;
            }
            let source_scale = field.source.decimals.filter(|scale| *scale != 0x1f)?;
            match slot.name.to_ascii_uppercase().as_str() {
                "SUM" => Some(source_scale),
                "AVG" => Some(
                    (u32::from(source_scale)
                        + u32::from(crate::engine::expr::session_div_precision_increment()))
                    .min(u32::from(crate::util::decimal::MAX_DECIMAL_SCALE))
                        as u8,
                ),
                _ => None,
            }
        });
        output_aggregate_fields.push(BatchField {
            table: "__agg".into(),
            alias: None,
            name: format!("a{slot_index}"),
            column_type: crate::engine::executor::select::eval::aggregate_function_result_type(
                &slot.name, arg_type,
            ),
            source: if slot.name.eq_ignore_ascii_case("GROUP_CONCAT")
                || matches!(
                    slot.name.to_ascii_uppercase().as_str(),
                    "BIT_AND" | "BIT_OR" | "BIT_XOR"
                ) {
                let argument = input_field.map(|field| {
                    crate::engine::expr_type::ExprType::from_column_type(
                        field.column_type,
                        &field.source,
                    )
                });
                let no_column = |_: &Expr| None;
                let no_param = |_: usize| None;
                let context =
                    crate::engine::expr_type::TypeContext::context_free(&no_column, &no_param);
                let descriptor =
                    crate::engine::expr_type::aggregate_result_type(&slot.name, argument, &context);
                let mut source = crate::types::ColumnSource::default();
                descriptor.apply_to_source(&mut source);
                source
            } else if matches!(
                slot.name.to_ascii_uppercase().as_str(),
                "MIN" | "MAX" | "__HIDDEN_FIRST_VALUE"
            ) {
                input_field
                    .map(|field| field.source.clone().nullable_aggregate())
                    .unwrap_or_default()
            } else {
                crate::types::ColumnSource::default()
            },
        });
        aggregate_keys.push(crate::engine::executor::select::aggregate::aggregate_key(
            aggregate,
        ));
        let approximate_input = matches!(arg_type, Some(ColumnType::Double | ColumnType::Float))
            && !input_field.is_some_and(|field| {
                matches!(field.column_type, ColumnType::Double | ColumnType::Float)
            });
        runtime_slots.push(ScalarAggregateRuntimeSlot {
            slot,
            input_column,
            order_columns,
            fixed_output_scale,
            approximate_input,
        });
    }

    let mut extended_fields = base_schema.fields().to_vec();
    extended_fields.extend(private_fields);
    let extended_schema = Arc::new(BatchSchema::new(extended_fields));
    let row_major = programs.len() > 1
        && programs
            .iter()
            .any(|program| program.capability().requires_ordered_evaluation());
    let input_stream: Box<dyn BatchOperator + 'a> = if programs.is_empty() {
        input_stream
    } else {
        Box::new(ComputeColumnsBatchStream {
            pending_chunks: None,
            inner: input_stream,
            numeric_programs: vec![None; programs.len()],
            direct_expressions: Vec::new(),
            programs,
            schema: extended_schema.clone(),
            row_major,
        })
    };

    let mut output_fields = group_columns
        .iter()
        .map(|&column| extended_schema.fields()[column].clone())
        .collect::<Vec<_>>();
    output_fields.extend(output_aggregate_fields);
    let output_schema = Arc::new(BatchSchema::new(output_fields));
    let group_template = group_columns
        .iter()
        .map(|&column| batch_field_template(&extended_schema.fields()[column]))
        .collect();
    let mut scalar_kernel = ScalarAggregateKernelBatchOperator {
        input: input_stream,
        input_schema: extended_schema,
        output_schema: output_schema.clone(),
        group_columns,
        slots: runtime_slots,
        output: None,
        rows_consumed: 0,
        batches_consumed: 0,
        memory_charge: crate::engine::executor::mem_budget::ScopedCharge::new(0),
    };
    scalar_kernel.initialize()?;
    let scalar_rows = scalar_kernel.rows_consumed;
    let input_batches = scalar_kernel.batches_consumed;
    let batches = scalar_kernel
        .output
        .take()
        .map(Iterator::collect)
        .unwrap_or_default();
    let memory_charge = std::mem::replace(
        &mut scalar_kernel.memory_charge,
        crate::engine::executor::mem_budget::ScopedCharge::new(0),
    );
    Ok(Some(HashAggregateStream {
        stream: Box::new(ChargedMaterializedBatchOperator {
            inner: MaterializedBatchOperator::with_properties(
                output_schema.clone(),
                batches,
                BatchProperties::UNORDERED_SINGLE,
            ),
            _charge: memory_charge,
        }),
        group_template,
        agg_keys: aggregate_keys,
        aggregates: aggregates.to_vec(),
        schema: output_schema,
        metrics: vec_hash_agg::AggMetrics {
            aggregate_mode: Some("scalar_batch"),
            fallback_aggregate_rows: scalar_rows,
            batches: input_batches,
            ..Default::default()
        },
        having_applied: false,
    }))
}

// -----------------------------------------------------------------------
// Vec ROLLUP / GROUPING SETS finest-grouping fast path
// -----------------------------------------------------------------------

/// The finest grouping set of a ROLLUP/GROUPING SETS query, computed by
/// the vectorized hash aggregate. The caller seeds row accumulators from
/// these snapshots (`aggregate::seed_rollup_accumulator`) and runs the
/// existing two-phase derive + emit unchanged — so only phase 1 is
/// vectorized while all the fragile super-aggregate / GROUPING / NULL-key
/// semantics keep running on the proven row path.
pub(in crate::engine::executor::select) struct VecRollupFinest {
    pub collations: Vec<Option<u16>>,
    _direct_charge: Option<mem_budget::ScopedCharge>,
    pub groups: Vec<VecRollupFinestGroup>,
}

pub(in crate::engine::executor::select) struct VecRollupFinestGroup {
    pub key_values: Vec<Value>,
    /// Aligned 1:1 with the caller's `slots`.
    pub agg_states: Vec<VecRollupAggState>,
}

#[derive(Clone)]
pub(in crate::engine::executor::select) struct VecRollupAggState {
    pub sum: crate::engine::executor::select::aggregate::NumericAggState,
    pub count: u64,
    pub min: Option<Value>,
    pub max: Option<Value>,
}

/// Compute the finest grouping set vectorized, returning per-group
/// accumulator snapshots aligned to `slots`. Capability discovery (including
/// expression compilation) happens before the input is opened. `Ok(None)` is
/// therefore only a pre-open route decision; after execution starts, an empty
/// input returns `Some` with zero groups so the caller can synthesize ROLLUP's
/// grand total without rescanning. Budget pressure is handled inside
/// HashAggregateCore: spilled finest partitions are merged back as raw aggregate
/// states.
pub(in crate::engine::executor::select) fn vec_rollup_finest_groups(
    db: &Arc<Database>,
    txn: TxnView,
    input: &PhysicalPlan,
    group_by: &[Expr],
    slots: &[crate::engine::executor::select::aggregate::AggregateSlot],
) -> Result<Option<VecRollupFinest>, QueryError> {
    use crate::engine::executor::vec::hash_agg::{
        self as vec_hash_agg, AggregateKind, AggregateSpec,
    };

    if !vectorize_enabled_for_plan(input) || group_by.is_empty() {
        return Ok(None);
    }
    // The finest-state adapter returns an entire vector of owned snapshots.
    // When a downstream rank collector bounds ROLLUP output, large finest
    // populations must instead use the existing ordered spill derivation;
    // spilling the hash build alone does not bound this final snapshot vector.
    // Decide before opening the input so fallback never replays consumed rows.
    if crate::engine::executor::select::plan::current_rollup_window_topk_hint()
        .is_some_and(|hint| hint.k > 0 && !hint.order_by.is_empty())
    {
        if let Some(budget) = mem_budget::current_budget() {
            let groups = crate::planner::estimate_group_by_rows_for_plan(
                input,
                group_by,
                crate::planner::estimated_rows(input),
            );
            let snapshot_bytes = 128usize
                .saturating_add(
                    group_by
                        .len()
                        .saturating_mul(std::mem::size_of::<Value>() + 64),
                )
                .saturating_add(
                    slots
                        .len()
                        .saturating_mul(std::mem::size_of::<VecRollupAggState>()),
                );
            if groups * snapshot_bytes as f64 > budget.limit() as f64 {
                return Ok(None);
            }
        }
    }
    for slot in slots {
        if !crate::engine::executor::select::aggregate::rollup_vec_seedable_slot(slot) {
            return Ok(None);
        }
    }

    // Only engage over a genuinely columnar input; the rows→batch→rows
    // fallback adapter would erase the phase-1 vectorization win.
    let Some(VecBatchInput {
        mut stream,
        template,
    }) = try_native_vec_input_stream(db, txn, input)?
    else {
        return Ok(None);
    };
    if template.is_empty() {
        return Ok(None);
    }
    let schema = template_to_batch_schema(&template);
    let Some((group_cols, mut programs, _group_numeric_programs, mut extended_fields)) =
        compile_group_columns(group_by, &template, &schema)
    else {
        return Ok(None);
    };

    let mut agg_specs: Vec<AggregateSpec> = Vec::with_capacity(slots.len());
    for slot in slots {
        let aggregate = Expr::Function {
            name: slot.name.clone(),
            args: if slot.is_count_star {
                vec![Expr::Wildcard]
            } else {
                slot.args.clone()
            },
            distinct: slot.distinct,
        };
        let Some((func, input, _)) = translate_aggregate_extended(&aggregate, &template, &schema)
        else {
            return Ok(None);
        };
        debug_assert!(matches!(
            func,
            AggregateKind::CountStar
                | AggregateKind::Count
                | AggregateKind::Sum
                | AggregateKind::Avg
                | AggregateKind::Min
                | AggregateKind::Max
        ));
        let input_column = match input {
            AggInputSpec::Column(column) => column,
            AggInputSpec::Expr(program) => {
                let column = template.len() + programs.len();
                let Some(field) = computed_batch_field(
                    &program,
                    "__rollup_input",
                    format!("v{}", programs.len()),
                ) else {
                    return Ok(None);
                };
                programs.push(program);
                extended_fields.push(field);
                column
            }
        };
        agg_specs.push(AggregateSpec {
            comparison_collation: schema
                .fields()
                .get(input_column)
                .and_then(|field| field.source.collation_id),
            func,
            input_column,
        });
    }

    let mut fields = schema.fields().to_vec();
    fields.extend(extended_fields);
    let extended_schema = Arc::new(BatchSchema::new(fields));
    let group_cols_for_collation = group_cols
        .iter()
        .map(|&col| {
            extended_schema
                .fields()
                .get(col)
                .and_then(|f| f.source.collation_id)
        })
        .collect();
    if !programs.is_empty() {
        let row_major = programs.len() > 1
            && programs
                .iter()
                .any(|program| program.capability().requires_ordered_evaluation());
        stream = Box::new(ComputeColumnsBatchStream {
            pending_chunks: None,
            inner: stream,
            numeric_programs: vec![None; programs.len()],
            direct_expressions: Vec::new(),
            programs,
            schema: extended_schema,
            row_major,
        });
    }

    let nslots = slots.len();
    let map_bucket = |bucket: vec_hash_agg::GroupBucketSnapshot| {
        debug_assert_eq!(bucket.aggs.len(), nslots, "vec rollup agg-state width");
        let agg_states = bucket
            .aggs
            .into_iter()
            .map(|st| VecRollupAggState {
                sum: st.sum,
                count: st.count,
                min: st.min,
                max: st.max,
            })
            .collect();
        VecRollupFinestGroup {
            key_values: bucket.key_values,
            agg_states,
        }
    };

    let mut direct = if let Some(budget) = mem_budget::current_budget() {
        vec_hash_agg::HashAggregateCore::with_budget(
            group_cols.clone(),
            agg_specs.clone(),
            stream.schema().clone(),
            budget,
            db.data_dir().to_path_buf(),
        )
    } else {
        vec_hash_agg::HashAggregateCore::new(group_cols.clone(), agg_specs.clone())
    };
    direct.direct_build_rollup = true;
    if stream.consume_into_hash_aggregate(&mut direct)? {
        #[cfg(test)]
        super::DIRECT_ROLLUP_HITS.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let metrics = direct.metrics();
        let (buckets, charge) = direct
            .take_direct_rollup_groups()
            .expect("direct build ROLLUP retains raw groups");
        record_pending_agg_metrics(metrics);
        return Ok(Some(VecRollupFinest {
            groups: buckets.into_iter().map(map_bucket).collect(),
            collations: group_cols_for_collation,
            _direct_charge: charge,
        }));
    }
    drop(direct);

    // Fan the finest grouping across N worker
    // aggregators (the plain GROUP BY path already does this — the ROLLUP
    // path historically stayed serial here, which is why ROLLUP queries did
    // not scale with `MYDB_PARALLEL_DEGREE`). The parallel driver internally
    // falls back to a single in-process aggregator below the scheduler row
    // threshold, so small inputs keep the original serial cost profile.
    let worker_count = scheduler_hash_agg_workers();
    let groups: Vec<VecRollupFinestGroup> =
        match crate::engine::executor::vec::scheduler::hash_agg_driver::run_rollup_finest_parallel(
            stream.as_mut(),
            group_cols,
            agg_specs,
            worker_count,
            mem_budget::current_budget(),
            db.data_dir().to_path_buf(),
        )? {
            Some(result) => {
                record_pending_agg_metrics(result.metrics);
                result.buckets.into_iter().map(&map_bucket).collect()
            }
            // Capability was established before opening `stream`; `None`
            // here means it was empty, not that execution should restart.
            None => Vec::new(),
        };
    let collations = group_cols_for_collation;
    Ok(Some(VecRollupFinest {
        groups,
        collations,
        _direct_charge: None,
    }))
}

/// Pick worker count for the scheduler hash_agg path. Mirrors
/// `default_parallel_degree`, capped by `MYDB_PARALLEL_DEGREE` /
/// disabled via `MYDB_PARALLEL_DISABLE` / `MYDB_PARALLEL_AGGREGATE_DISABLE`.
pub(super) fn scheduler_hash_agg_workers() -> usize {
    if crate::util::env::var("MYDB_PARALLEL_DISABLE")
        .ok()
        .map(|v| v == "1" || v.eq_ignore_ascii_case("true") || v.eq_ignore_ascii_case("on"))
        .unwrap_or(false)
    {
        return 1;
    }
    if crate::util::env::var("MYDB_PARALLEL_AGGREGATE_DISABLE")
        .ok()
        .map(|v| v == "1" || v.eq_ignore_ascii_case("true") || v.eq_ignore_ascii_case("on"))
        .unwrap_or(false)
    {
        return 1;
    }
    crate::util::env::var("MYDB_PARALLEL_DEGREE")
        .ok()
        .and_then(|s| s.parse::<usize>().ok())
        .filter(|&n| n >= 1)
        .unwrap_or_else(|| crate::planner::default_parallel_degree() as usize)
}

/// C10 - worker count for the scheduler hash_join probe driver.
/// Mirrors `scheduler_hash_agg_workers` but governed by its own
/// disable knob so the two paths can be toggled independently.
/// `MYDB_PARALLEL_DISABLE=1` still kills both (global escape hatch).
pub(super) fn scheduler_hash_join_workers() -> usize {
    if crate::util::env::var("MYDB_PARALLEL_DISABLE")
        .ok()
        .map(|v| v == "1" || v.eq_ignore_ascii_case("true") || v.eq_ignore_ascii_case("on"))
        .unwrap_or(false)
    {
        return 1;
    }
    crate::util::env::var("MYDB_PARALLEL_DEGREE")
        .ok()
        .and_then(|s| s.parse::<usize>().ok())
        .filter(|&n| n >= 1)
        .unwrap_or_else(|| crate::planner::default_parallel_degree() as usize)
}

/// Build the same `BatchSchema` `HashAggregateCore::output_schema()` would
/// emit, without instantiating the operator. Mirrors the layout
/// (group cols first, then one column per agg) and the type rules:
/// COUNT → LongLong, SUM/AVG → Double, MIN/MAX → the source column's
/// type.
pub(super) fn build_hash_agg_output_schema(
    input_schema: &Arc<BatchSchema>,
    group_columns: &[usize],
    aggs: &[vec_hash_agg::AggregateSpec],
) -> Arc<BatchSchema> {
    let mut fields: Vec<BatchField> = Vec::with_capacity(group_columns.len() + aggs.len());
    for &g in group_columns {
        fields.push(input_schema.fields()[g].clone());
    }
    for (i, spec) in aggs.iter().enumerate() {
        use crate::engine::executor::vec::hash_agg::AggregateKind;
        let input_field = input_schema.fields().get(spec.input_column);
        let column_type = spec
            .func
            .result_type(input_field.map(|field| field.column_type));
        let mut source = if matches!(spec.func, AggregateKind::Min | AggregateKind::Max) {
            input_field
                .map(|field| field.source.clone().nullable_aggregate())
                .unwrap_or_default()
        } else {
            crate::types::ColumnSource::default()
        };
        if matches!(
            spec.func,
            AggregateKind::BitAnd | AggregateKind::BitOr | AggregateKind::BitXor
        ) {
            source.flags |= crate::mysql::protocol::UNSIGNED_FLAG;
        }
        fields.push(BatchField {
            table: "__agg".into(),
            alias: None,
            name: format!("a{i}"),
            column_type,
            source,
        });
    }
    Arc::new(BatchSchema::new(fields))
}

pub(in crate::engine::executor::select) fn try_vec_hash_aggregate(
    db: &Arc<Database>,
    txn: TxnView,
    upstream: &PhysicalPlan,
    group_by: &[Expr],
    having: Option<&Expr>,
    aggregates: &[Expr],
) -> Result<Option<Vec<PlanRow>>, QueryError> {
    // Drop any stale metrics from a previous (now-completed) call so
    // a `forward_vec_hash_agg_metrics` on the new plan doesn't pick up
    // numbers from an unrelated execution.
    LAST_AGG_METRICS.with(|cell| cell.set(None));
    if !vectorize_enabled_for_plan(upstream) {
        return Ok(None);
    }
    if aggregates.is_empty() && group_by.is_empty() {
        return Ok(None);
    }
    let Some(HashAggregateStream {
        mut stream,
        group_template,
        agg_keys,
        aggregates: _,
        schema: _,
        mut metrics,
        having_applied: _,
    }) = build_vec_hash_agg_stream(db, txn, upstream, group_by, having, aggregates)?
    else {
        return Ok(None);
    };

    // Output template: only the GROUP BY columns. Aggregate results
    // land in `PlanRow::agg_values` keyed by the canonical aggregate
    // key the legacy projection layer looks up via
    // `eval::eval_aggregate_expr` → `aggregate_key(expr)`.
    let n_group = group_template.len();
    let n_agg = agg_keys.len();

    let mut out = Vec::new();
    while let Some(batch) = stream.next_batch()? {
        debug_assert_eq!(
            batch.columns().len(),
            n_group + n_agg,
            "vec hash-agg batch column layout mismatch",
        );
        let visible_rows = batch.visible_rows();
        out.reserve(visible_rows);
        let cols = batch.columns();
        for r in batch.visible_indices() {
            let fields = vec_hash_agg_group_fields(&group_template, cols, r);
            let mut cache: std::collections::HashMap<u64, Value> =
                std::collections::HashMap::with_capacity(n_agg);
            for (slot, key) in agg_keys.iter().enumerate() {
                cache.insert(*key, cols[n_group + slot].value_at(r));
            }
            out.push(PlanRow {
                fields,
                // Tag this row as aggregate output (`group: Some(_)`) so
                // downstream operators that read aggregate values via
                // `eval_row_or_aggregate_expr` route to
                // `eval_aggregate_expr` — which honours the
                // `agg_values` cache below. Without this tag, an outer
                // `ORDER BY <alias>` referring to a SUM/AVG/MIN/MAX
                // result silently falls back to plain row-field
                // lookup, finds nothing, and turns into a stable-order
                // sort (TPC-H Q5: `ORDER BY revenue DESC` produced the
                // raw group-iteration order instead).
                group: Some(Vec::new()),
                agg_values: Some(Arc::new(cache)),
                schema: PlanRow::empty_schema(),
            });
        }
    }
    // The legacy protocol boundary still consumes PlanRows. Count this final
    // materialization explicitly; intermediate native scans/filters/compute
    // operators remain at zero conversion rows.
    metrics.conversion_rows = out.len() as u64;
    record_pending_agg_metrics(metrics);
    Ok(Some(out))
}

pub(super) fn fixed_scale_approximate_aggregate(
    aggregates: &[Expr],
    upstream: &PhysicalPlan,
) -> bool {
    let mut fields = Vec::new();
    crate::engine::executor::select::plan::collect_plan_fields(upstream, &mut fields);
    aggregates.iter().any(|expr| {
        let Expr::Function { name, args, .. } = expr else {
            return false;
        };
        if !matches!(name.to_ascii_uppercase().as_str(), "SUM" | "AVG") {
            return false;
        }
        let Some(arg) = args.first() else {
            return false;
        };
        let (qualifier, column) = match arg {
            Expr::Identifier(column) => (None, column.as_str()),
            Expr::CompoundIdentifier(parts) if parts.len() >= 2 => (
                Some(parts[parts.len() - 2].as_str()),
                parts[parts.len() - 1].as_str(),
            ),
            _ => return false,
        };
        let mut matches = fields.iter().filter(|field| {
            field.column.eq_ignore_ascii_case(column)
                && qualifier.is_none_or(|qualifier| {
                    field
                        .alias
                        .as_deref()
                        .is_some_and(|alias| alias.eq_ignore_ascii_case(qualifier))
                        || field.table.eq_ignore_ascii_case(qualifier)
                })
        });
        let Some(field) = matches.next() else {
            return false;
        };
        matches.next().is_none()
            && matches!(field.column_type, ColumnType::Float | ColumnType::Double)
            && field
                .source
                .decimals
                .is_some_and(|decimals| decimals != 0x1f)
    })
}

// ---------------------------------------------------------------------
// Expression-based aggregate support
// ---------------------------------------------------------------------

/// Describes how an aggregate's input value is obtained from the batch.
pub(super) enum AggInputSpec {
    /// Direct column reference — use `batch.columns()[idx]` directly.
    Column(usize),
    /// Compiled expression — must be evaluated per-batch and appended as
    /// a virtual column by `ComputeColumnsBatchStream`.
    Expr(Arc<ExprProgram>),
}

/// Extended version of `translate_aggregate` that also accepts
/// expression-based aggregate inputs (e.g. `SUM(price * discount)`).
/// For simple column refs the behaviour is identical to
/// `translate_aggregate`. For expressions the caller must arrange
/// pre-computation via `ComputeColumnsBatchStream`.
pub(super) fn translate_aggregate_extended(
    expr: &Expr,
    template: &[PlanField],
    batch_schema: &BatchSchema,
) -> Option<(vec_hash_agg::AggregateKind, AggInputSpec, ColumnType)> {
    let (name, args, distinct) = match expr {
        Expr::Function {
            name,
            args,
            distinct,
        } => (name.to_ascii_uppercase(), args, *distinct),
        _ => return None,
    };
    if distinct {
        // COUNT/SUM/AVG(DISTINCT) → the vec dedup variants. Anything else
        // was already filtered by `aggregate_static_ok`, but guard here
        // too so this fn is safe in isolation. Single non-wildcard arg.
        if args.len() != 1 || matches!(args.as_slice(), [Expr::Wildcard]) {
            return None;
        }
        let func = vec_hash_agg::AggregateKind::from_sql(&name, true, false)?;
        if matches!(func, vec_hash_agg::AggregateKind::CountDistinct) {
            if let Some(col) = expr_column_index_in_template(&args[0], template) {
                let field = &template[col];
                // Numeric/temporal values already have canonical ValueKey
                // equality. Blob and VarString are the two string/binary wire
                // lanes, so keep both on the private collation-key expression;
                // true binary values pass through that function unchanged.
                if !matches!(field.column_type, ColumnType::Blob | ColumnType::VarString) {
                    return Some((func, AggInputSpec::Column(col), ColumnType::LongLong));
                }
            }
            // COUNT(DISTINCT) equality is collation-aware for character
            // inputs (including PAD SPACE). Feed the native spillable set
            // the exact same private comparison key used by AggregateSlot;
            // the function leaves numeric/binary values unchanged.
            let keyed = Expr::Function {
                name: "__DISTINCT_COLLATION_KEY".to_string(),
                args: vec![args[0].clone()],
                distinct: false,
            };
            let program = expr_batch::compile_for_batch(&keyed, batch_schema).ok()??;
            return Some((func, AggInputSpec::Expr(program), ColumnType::LongLong));
        }
        if let Some(col) = expr_column_index_in_template(&args[0], template) {
            let argument_type = semantic_aggregate_argument_type(
                &args[0],
                template,
                Some(template[col].column_type),
            );
            return Some((
                func,
                AggInputSpec::Column(col),
                func.result_type(argument_type),
            ));
        }
        let program = expr_batch::compile_for_batch(&args[0], batch_schema).ok()??;
        let descriptor = program.result_type();
        let argument_type = (!matches!(
            descriptor.kind,
            crate::engine::expr_type::ExprTypeKind::Unknown
        ))
        .then(|| descriptor.column_type());
        let argument_type = semantic_aggregate_argument_type(&args[0], template, argument_type);
        return Some((
            func,
            AggInputSpec::Expr(program),
            func.result_type(argument_type),
        ));
    }
    let count_star = name == "COUNT" && matches!(args.as_slice(), [Expr::Wildcard]);
    let func = vec_hash_agg::AggregateKind::from_sql(&name, false, count_star)?;
    if count_star {
        return Some((func, AggInputSpec::Column(0), ColumnType::LongLong));
    }
    if args.len() != 1 {
        return None;
    }

    // Fast path: simple column reference.
    if let Some(col) = expr_column_index_in_template(&args[0], template) {
        if matches!(
            func,
            vec_hash_agg::AggregateKind::BitAnd
                | vec_hash_agg::AggregateKind::BitOr
                | vec_hash_agg::AggregateKind::BitXor
        ) && template[col].column_type == ColumnType::Blob
        {
            // The native bit state is numeric-only. Binary bit aggregates
            // use the shared row accumulator until the batch state carries
            // a bounded byte lane as well.
            return None;
        }
        let semantic_type =
            semantic_aggregate_argument_type(&args[0], template, Some(template[col].column_type));
        let result_type = match func {
            vec_hash_agg::AggregateKind::BitAnd
            | vec_hash_agg::AggregateKind::BitOr
            | vec_hash_agg::AggregateKind::BitXor => ColumnType::LongLong,
            vec_hash_agg::AggregateKind::VarPop
            | vec_hash_agg::AggregateKind::VarSamp
            | vec_hash_agg::AggregateKind::StddevPop
            | vec_hash_agg::AggregateKind::StddevSamp => ColumnType::Double,
            _ => func.result_type(semantic_type),
        };
        return Some((func, AggInputSpec::Column(col), result_type));
    }

    // Expression path: try to compile the argument expression against
    // the batch schema. If compilation fails, bail to legacy.
    if matches!(
        func,
        vec_hash_agg::AggregateKind::BitAnd
            | vec_hash_agg::AggregateKind::BitOr
            | vec_hash_agg::AggregateKind::BitXor
    ) {
        // CAST(... AS BINARY), binary literals and conditional expressions
        // can produce a byte lane whose width is not represented by
        // ExprProgram's scalar result metadata. Preserve row-oracle semantics.
        return None;
    }
    let program = expr_batch::compile_for_batch(&args[0], batch_schema).ok()??;
    let result_type = match func {
        vec_hash_agg::AggregateKind::BitAnd
        | vec_hash_agg::AggregateKind::BitOr
        | vec_hash_agg::AggregateKind::BitXor => ColumnType::LongLong,
        vec_hash_agg::AggregateKind::VarPop
        | vec_hash_agg::AggregateKind::VarSamp
        | vec_hash_agg::AggregateKind::StddevPop
        | vec_hash_agg::AggregateKind::StddevSamp => ColumnType::Double,
        _ => {
            let descriptor = program.result_type();
            let physical_type = (!matches!(
                descriptor.kind,
                crate::engine::expr_type::ExprTypeKind::Unknown
            ))
            .then(|| descriptor.column_type());
            func.result_type(semantic_aggregate_argument_type(
                &args[0],
                template,
                physical_type,
            ))
        }
    };
    Some((func, AggInputSpec::Expr(program), result_type))
}
