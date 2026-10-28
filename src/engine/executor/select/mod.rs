pub(crate) mod aggregate;
pub(super) mod analyze_stats;
pub(crate) mod bloom;
pub(crate) mod eval;
pub(in crate::engine::executor) mod execution_context;
pub(crate) use eval::aggregate_function_result_type;
mod explain;
pub(super) mod external_sort;
pub(in crate::engine::executor) mod fulltext_context;
pub(crate) mod grace;
pub(crate) mod hybrid;
pub(crate) mod join_stream;
pub(crate) mod loser_tree;
pub(crate) mod mrr_lru;
pub(crate) mod parallel;
pub(crate) mod plan;
mod row_schema;
mod shared_aggregate;

#[cfg(test)]
pub(crate) static SHARED_DERIVED_AGGREGATE_HITS: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);
pub(crate) mod simple;
pub(crate) mod stream;
pub(crate) mod vec_bridge;

use self::eval::*;
use self::plan::*;
use self::simple::*;
use super::qualification::qualify_table_name;
use super::*;
use crate::engine::expr_program::expr_semantically_eq;
use crate::engine::session_registry::SessionRegistry;
use crate::util::fast_hash::{ah_hash_set, AhHashSet};
use std::collections::hash_map::Entry;

thread_local! {
    static FORCE_STREAMING_RESPONSE_DEPTH: std::cell::Cell<u32> = const { std::cell::Cell::new(0) };
    static CURRENT_SESSION_REGISTRY: std::cell::RefCell<Option<SessionRegistry>> = const { std::cell::RefCell::new(None) };
}

pub(in crate::engine::executor) fn current_session_registry() -> Option<SessionRegistry> {
    CURRENT_SESSION_REGISTRY.with(|registry| registry.borrow().clone())
}

pub(in crate::engine::executor) fn with_session_registry<R>(
    registry: SessionRegistry,
    f: impl FnOnce() -> R,
) -> R {
    CURRENT_SESSION_REGISTRY.with(|slot| {
        let previous = slot.replace(Some(registry));
        struct Restore<'a>(
            &'a std::cell::RefCell<Option<SessionRegistry>>,
            Option<SessionRegistry>,
        );
        impl Drop for Restore<'_> {
            fn drop(&mut self) {
                self.0.replace(self.1.take());
            }
        }
        let _restore = Restore(slot, previous);
        f()
    })
}

pub(in crate::engine::executor) struct ForceStreamingResponseScope;

impl Drop for ForceStreamingResponseScope {
    fn drop(&mut self) {
        FORCE_STREAMING_RESPONSE_DEPTH.with(|depth| depth.set(depth.get().saturating_sub(1)));
    }
}

pub(in crate::engine::executor) fn force_streaming_response_scope() -> ForceStreamingResponseScope {
    FORCE_STREAMING_RESPONSE_DEPTH.with(|depth| depth.set(depth.get().saturating_add(1)));
    ForceStreamingResponseScope
}

pub(super) fn force_streaming_response_active() -> bool {
    FORCE_STREAMING_RESPONSE_DEPTH.with(|depth| depth.get() > 0)
}

pub(super) use self::explain::execute_explain;
pub(crate) use self::plan::{execute_compiled_select, execute_physical_select};
pub(in crate::engine::executor) use self::simple::TableProjectionProgram;
pub(super) use self::simple::{
    expr_has_subquery, index_order_covers_order_by, project_table_row, select_item_has_subquery,
    simple_projection_indexes, single_table_result_columns,
};

/// Metadata for an expression result with no catalog origin. MySQL's
/// `UNSIGNED_FLAG` describes the result value, not just base-table columns,
/// so keep it even when `org_table` / `org_name` are empty.
fn expression_column_source(unsigned: bool) -> crate::types::ColumnSource {
    crate::types::ColumnSource {
        flags: if unsigned {
            crate::mysql::protocol::UNSIGNED_FLAG
        } else {
            0
        },
        ..Default::default()
    }
}

/// `descriptor` is the caller's bound type of `expr`. Callers always derive it
/// first; re-inferring here would describe every nested scalar subquery a
/// second time per nesting level.
fn expression_column_source_for_expr(
    unsigned: bool,
    expr: &Expr,
    mut descriptor: crate::engine::expr_type::ExprType,
) -> crate::types::ColumnSource {
    let mut source = expression_column_source(unsigned);
    source.dynamic_parameter = matches!(expr, Expr::Placeholder(_));
    // A standalone computed field has no original catalog column. Set
    // operations fill the first branch's expression label later, where MySQL
    // exposes it as aggregate UNION metadata; doing so here would incorrectly
    // advertise `org_name` for ordinary SELECT expressions such as SHA2(...).
    if crate::engine::expr::expr_result_guaranteed_nonnull(expr) {
        source.flags |= crate::mysql::protocol::NOT_NULL_FLAG;
    }
    source.collation_id = crate::engine::expr::expression_metadata_collation_id(expr);
    source.length = crate::engine::expr::expression_metadata_length(expr);
    source.decimals = crate::engine::expr::expression_metadata_decimals(expr);
    if let Expr::Function { name, .. } = expr {
        if let Some(mut spec) = crate::engine::expr_type::builtin_spec(name) {
            if matches!(
                spec.result.kind,
                crate::engine::expr_type::ExprTypeKind::String {
                    kind: crate::engine::expr_type::StringKind::Character
                }
            ) {
                spec.result.collation_id = source.collation_id;
            }
            spec.result.apply_to_source(&mut source);
        }
    }
    if matches!(
        crate::engine::expr::expr_result_type(expr),
        ColumnType::VarString | ColumnType::Blob
    ) {
        source.decimals.get_or_insert(31);
    }
    if source.collation_id == Some(crate::charset::BINARY_COLLATION_ID)
        && matches!(
            crate::engine::expr::expr_result_type(expr),
            ColumnType::VarString | ColumnType::Blob
        )
    {
        source.flags |= crate::mysql::protocol::BINARY_FLAG;
    }
    if matches!(
        expr,
        Expr::Function { name, .. }
            if name.eq_ignore_ascii_case("WEIGHT_STRING")
                || name.eq_ignore_ascii_case("WEIGHT_STRING_TYPED")
    ) {
        source.collation_id = Some(crate::charset::BINARY_COLLATION_ID);
        source.flags |= crate::mysql::protocol::BINARY_FLAG;
    }
    if matches!(
        expr,
        Expr::Function { name, .. } if name.eq_ignore_ascii_case("UNHEX")
    ) {
        source.collation_id = Some(crate::charset::BINARY_COLLATION_ID);
        source.flags |= crate::mysql::protocol::BINARY_FLAG;
    }
    if matches!(
        expr,
        Expr::Function { name, .. }
            if matches!(
                name.to_ascii_uppercase().as_str(),
                "MD5" | "SHA" | "SHA1" | "SHA2"
            )
    ) {
        source.decimals = Some(31);
    }
    if matches!(expr, Expr::Function { name, .. } if name.eq_ignore_ascii_case("GROUP_CONCAT")) {
        let max_len = self::aggregate::group_concat_max_len().min(u32::MAX as usize) as u32;
        source.length = Some(if max_len <= 512 {
            let max_bytes_per_character = source
                .collation_id
                .and_then(crate::charset::resolve_collation_id)
                .and_then(|collation| crate::charset::resolve_charset(collation.charset).ok())
                .map(|charset| u32::from(charset.max_len))
                .unwrap_or(1);
            max_len.saturating_mul(max_bytes_per_character)
        } else {
            max_len
        });
        source.decimals = Some(31);
    }
    source.dynamic_parameter = descriptor.dynamic_parameter;
    if !matches!(
        descriptor.kind,
        crate::engine::expr_type::ExprTypeKind::Unknown
    ) {
        descriptor.max_octets = descriptor.max_octets.or(source.length);
        descriptor.collation_id = descriptor.collation_id.or(source.collation_id);
        descriptor.apply_to_source(&mut source);
    }
    source
}

fn context_free_expr_descriptor(expr: &Expr) -> crate::engine::expr_type::ExprType {
    let no_column = |_: &Expr| None;
    let no_placeholder = |_: usize| None;
    let context = crate::engine::expr_type::TypeContext::context_free(&no_column, &no_placeholder);
    crate::engine::expr_type::infer_expr_type(expr, &context)
}

fn expr_needs_bound_result_coercion(
    expr: &Expr,
    descriptor: crate::engine::expr_type::ExprType,
) -> bool {
    if matches!(expr, Expr::Function { name, .. } if name.eq_ignore_ascii_case("GREATEST") || name.eq_ignore_ascii_case("LEAST"))
    {
        return matches!(
            descriptor.kind,
            crate::engine::expr_type::ExprTypeKind::String { .. }
        );
    }
    matches!(expr, Expr::Case { .. })
        || matches!(
            expr,
            Expr::Function { name, .. }
                if matches!(
                    crate::engine::sys_schema::NormalizedFuncName::new(name).as_str(),
                    "IF" | "IFNULL" | "COALESCE" | "NULLIF"
                )
        )
}

/// Run SELECT with one or more CTE references appearing as join targets.
/// Each side is materialised into an in-memory rowset (CTE results read from
/// the registry, real tables fetched with a recursive `SELECT *`), then the
/// joins are evaluated as nested-loop joins. WHERE / projection / ORDER BY /
/// LIMIT / OFFSET / DISTINCT are applied on the joined PlanRows.
fn execute_select_with_cte_joins(
    db: &Arc<Database>,
    txn: TxnView,
    stmt: Select,
    registry: &SessionRegistry,
) -> Result<QueryResponse, QueryError> {
    let _tuning_scope = crate::engine::executor::tuning::enter_if_absent();
    // A nested SELECT is a new statement scope, even when it reads the same
    // physical table as its caller.
    let _caller_partition_scope = super::partition_router::suspend_pruned_partitions();
    self::eval::with_subquery_context(db, txn, || {
        execute_select_with_cte_joins_inner(db, txn, stmt, registry)
    })
}

fn execute_select_with_cte_joins_inner(
    db: &Arc<Database>,
    txn: TxnView,
    stmt: Select,
    registry: &SessionRegistry,
) -> Result<QueryResponse, QueryError> {
    use crate::engine::expr::expr_name;
    // Materialise FROM[0] and each join target, then evaluate the joins.
    // When every join is a plain comma/CROSS join (no ON, not lateral) and a
    // WHERE clause is present, route through the equi-join-aware greedy path:
    // it pushes WHERE equi-predicates into the joins and orders them so a
    // connecting hash join fires at each step. Without this, a query like
    // `FROM ctr1, customer_address, customer
    //   WHERE ca_address_sk = c_current_addr_sk
    //     AND ctr1.ctr_customer_sk = c_customer_sk` (the TPC-DS q30 family)
    // would materialise the full cartesian product of the comma-joined tables
    // and exhaust memory. Every other shape keeps the sequential NLJ path.
    let (mut current_rows, all_sources) = match try_greedy_equi_join(db, txn, &stmt, registry)? {
        Some(joined) => joined,
        None => {
            let primary = stmt.from.first().ok_or_else(|| {
                error(
                    crate::error::catalog::ER_NOT_SUPPORTED_YET,
                    "SELECT with no FROM",
                )
            })?;
            let primary_source = if stmt.joins.is_empty()
                && is_system_table_query(&primary.name)
                && stmt.selection.is_some()
            {
                materialize_filtered_system_source(db, primary, stmt.selection.clone(), registry)?
            } else {
                materialize_join_source_with_derived(
                    db,
                    txn,
                    primary,
                    stmt.derived.as_ref(),
                    registry,
                )?
            };
            let mut current_rows = source_to_plan_rows(&primary_source);
            let mut all_sources: Vec<MaterializedSource> = vec![primary_source];
            for join in &stmt.joins {
                if join.lateral {
                    let derived = match join.derived.as_ref() {
                        Some(derived) => derived,
                        None => {
                            return Err(error(
                                crate::error::catalog::ER_NOT_SUPPORTED_YET,
                                "LATERAL is currently supported only for derived table subqueries",
                            ));
                        }
                    };
                    let (joined_rows, right_source) = nested_loop_lateral_join(
                        db,
                        txn,
                        current_rows,
                        &join.table,
                        derived,
                        join.join_type,
                        join.condition.as_ref(),
                        registry,
                        &all_sources,
                    )?;
                    current_rows = joined_rows;
                    all_sources.push(right_source);
                    continue;
                }
                let right_source = materialize_join_source_with_derived(
                    db,
                    txn,
                    &join.table,
                    join.derived.as_ref(),
                    registry,
                )?;
                current_rows = nested_loop_join(
                    current_rows,
                    &all_sources,
                    &right_source,
                    join.join_type,
                    join.condition.as_ref(),
                )?;
                all_sources.push(right_source);
            }
            (current_rows, all_sources)
        }
    };
    // WHERE.
    if let Some(filter) = &stmt.selection {
        let mut kept = Vec::with_capacity(current_rows.len());
        for row in current_rows {
            if self::eval::eval_plan_bool(filter, &row)? {
                kept.push(row);
            }
        }
        current_rows = kept;
    }
    // Compute output columns from projection.
    let mut out_columns: Vec<Column> = Vec::new();
    if let Some(sample) = current_rows.first() {
        for item in &stmt.projection {
            if let SelectItem::Expr {
                expr: Expr::Function { name, args, .. },
                ..
            } = item
            {
                if name.eq_ignore_ascii_case("GREATEST") || name.eq_ignore_ascii_case("LEAST") {
                    self::eval::validate_plan_string_combinator(args, sample)?;
                }
            }
        }
    }
    for item in &stmt.projection {
        match item {
            SelectItem::Wildcard => {
                for src in &all_sources {
                    for col in &src.columns {
                        out_columns.push(col.clone());
                    }
                }
            }
            SelectItem::QualifiedWildcard(qualifier) => {
                let src = all_sources
                    .iter()
                    .find(|s| s.alias.eq_ignore_ascii_case(qualifier))
                    .ok_or_else(|| {
                        error(
                            crate::error::catalog::ER_BAD_FIELD_ERROR,
                            format!("unknown table qualifier '{qualifier}'"),
                        )
                    })?;
                for col in &src.columns {
                    out_columns.push(col.clone());
                }
            }
            SelectItem::Expr { expr, alias } => {
                let name = alias.clone().unwrap_or_else(|| expr_name(expr));
                let descriptor = infer_expr_descriptor(expr, &all_sources);
                let origin = source_column_for_expr(expr, &all_sources);
                let column_type = if matches!(
                    descriptor.kind,
                    crate::engine::expr_type::ExprTypeKind::Unknown
                ) {
                    infer_expr_type(expr, &all_sources)
                } else {
                    descriptor.column_type()
                };
                let mut source = origin
                    .map(|column| column.source.clone())
                    .unwrap_or_else(|| {
                        expression_column_source_for_expr(
                            infer_expr_unsigned(expr, &all_sources),
                            expr,
                            descriptor,
                        )
                    });
                if origin.is_none()
                    && !matches!(
                        descriptor.kind,
                        crate::engine::expr_type::ExprTypeKind::Unknown
                    )
                {
                    descriptor.apply_to_source(&mut source);
                }
                if let Expr::Function { name, args, .. } = expr {
                    if matches!(name.to_ascii_uppercase().as_str(), "SUM" | "AVG") {
                        if let Some(column) = args
                            .first()
                            .and_then(|arg| source_column_for_expr(arg, &all_sources))
                            .filter(|column| {
                                matches!(column.column_type, ColumnType::Float | ColumnType::Double)
                            })
                        {
                            source.decimals = column.source.decimals.map(|scale| {
                                if name.eq_ignore_ascii_case("AVG") && scale != 0x1f {
                                    scale.saturating_add(
                                        crate::engine::expr::session_div_precision_increment(),
                                    )
                                } else {
                                    scale
                                }
                            });
                        }
                    }
                }
                out_columns.push(Column {
                    source,
                    name,
                    column_type,
                });
            }
        }
    }

    // GROUP BY / HAVING / aggregation. Required for unmodified TPC-H Q15.
    // When GROUP BY is non-empty, partition the joined rows by the
    // GROUP BY key tuple and evaluate projection expressions with
    // `eval_aggregate_expr` against each partition. When GROUP BY is
    // empty but the projection contains aggregates, treat all rows as a
    // single group (implicit aggregation). HAVING is evaluated after
    // grouping using the same aggregate-aware evaluator.
    let projection_has_aggregate = select_projection_has_toplevel_aggregate(&stmt.projection);
    let aggregating = !stmt.group_by.is_empty()
        || projection_has_aggregate
        || stmt
            .having
            .as_ref()
            .is_some_and(expr_has_toplevel_aggregate);
    let _grouping = (!stmt.group_by.is_empty()).then(|| {
        crate::engine::cancel::enter_process_stage(
            crate::engine::session_registry::ProcessState::SortingForGroup,
        )
    });

    let mut out_rows: Vec<Vec<Value>> = Vec::new();
    if aggregating {
        // Phase A: accumulator-based grouping. Stream the joined rows
        // through per-group accumulators; never retain the input rows
        // beyond their currently-iterated borrow.
        use self::aggregate::{collect_aggregate_slots, Accumulator, AggregateSlot};
        let mut slot_exprs: Vec<&Expr> = Vec::new();
        for item in &stmt.projection {
            if let SelectItem::Expr { expr, .. } = item {
                slot_exprs.push(expr);
            }
        }
        if let Some(h) = &stmt.having {
            slot_exprs.push(h);
        }
        for ob in &stmt.order_by {
            slot_exprs.push(&ob.expr);
        }
        let slots: Vec<AggregateSlot> = collect_aggregate_slots(&slot_exprs);

        struct GroupBucket {
            keys: Vec<Value>,
            sample: self::plan::PlanRow,
            accumulators: Vec<Box<dyn Accumulator>>,
            insert_order: usize,
        }
        // O3: ValueKey vector instead of NUL-joined string of value_to_string,
        // sharing the canonical hash/eq impl with hash join. ahash::RandomState
        // (not std SipHash) for the same near-distinct reason as the join above.
        use crate::engine::expr::ValueKey;
        let mut buckets: hashbrown::HashMap<Vec<ValueKey>, GroupBucket, ahash::RandomState> =
            hashbrown::HashMap::with_hasher(ahash::RandomState::new());
        let mut next_order = 0usize;

        // Compile the aggregate slot input expressions and GROUP BY keys once
        // against the joined-row shape (all rows in this CTE-join result are
        // homogeneous). Per-row evaluation then resolves columns by direct
        // field index / compiled `ExprProgram` rather than the legacy by-name
        // `resolve_plan_field` walk. On TPC-DS q97 the implicit-group
        // `SUM(CASE ssci.customer_sk IS [NOT] NULL …)` was evaluated ~830k×3
        // times entirely on the legacy interpreter (the dominant cost center).
        // Compilation falls back to legacy for any unlowerable shape.
        let compiled_slot_args: Vec<Option<self::eval::CompiledPlanExpr>> = current_rows
            .first()
            .map(|first| {
                let schema = self::eval::input_schema_for_plan_row(first);
                slots
                    .iter()
                    .map(|slot| {
                        if slot.is_count_star || slot.args.is_empty() {
                            None
                        } else {
                            Some(self::eval::CompiledPlanExpr::compile_once(
                                &slot.args[0],
                                &schema,
                            ))
                        }
                    })
                    .collect()
            })
            .unwrap_or_default();
        let group_collations: Vec<_> = stmt
            .group_by
            .iter()
            .map(|expr| infer_expr_descriptor(expr, &all_sources).collation_id)
            .collect();
        let compiled_group_by: Vec<self::eval::CompiledPlanExpr> = current_rows
            .first()
            .map(|first| {
                let schema = self::eval::input_schema_for_plan_row(first);
                stmt.group_by
                    .iter()
                    .map(|k| self::eval::CompiledPlanExpr::compile_once(k, &schema))
                    .collect()
            })
            .unwrap_or_default();

        let feed = |slots: &[AggregateSlot],
                    accs: &mut [Box<dyn Accumulator>],
                    row: &self::plan::PlanRow|
         -> Result<(), QueryError> {
            for (idx, (slot, acc)) in slots.iter().zip(accs.iter_mut()).enumerate() {
                if slot.is_count_star {
                    acc.update_count_star();
                } else if slot.args.is_empty() {
                    acc.update(Value::Null)?;
                } else {
                    let v = match compiled_slot_args.get(idx).and_then(|c| c.as_ref()) {
                        Some(compiled) => compiled.eval(row)?,
                        None => self::eval::eval_plan_expr(&slot.args[0], row)?,
                    };
                    if slot.has_order_by() {
                        let keys = slot.eval_order_keys(|e| self::eval::eval_plan_expr(e, row))?;
                        acc.update_ordered(v, keys)?;
                    } else {
                        acc.update(v)?;
                    }
                }
            }
            Ok(())
        };

        if stmt.group_by.is_empty() {
            // Implicit single group.
            let mut sample: Option<self::plan::PlanRow> = None;
            let mut accs: Vec<Box<dyn Accumulator>> = slots
                .iter()
                .map(|s| {
                    current_rows.first().map_or_else(
                        || s.make_accumulator(),
                        |row| s.make_accumulator_for_row(row),
                    )
                })
                .collect();
            for row in &current_rows {
                if sample.is_none() {
                    sample = Some(row.clone());
                }
                feed(&slots, &mut accs, row)?;
            }
            let sample = sample.unwrap_or_else(|| self::plan::PlanRow {
                fields: Vec::new(),
                group: None,
                agg_values: None,
                schema: PlanRow::empty_schema(),
            });
            buckets.insert(
                Vec::new(),
                GroupBucket {
                    keys: Vec::new(),
                    sample,
                    accumulators: accs,
                    insert_order: 0,
                },
            );
        } else {
            for row in &current_rows {
                let key_values: Vec<Value> = compiled_group_by
                    .iter()
                    .map(|key| key.eval(row))
                    .collect::<Result<Vec<_>, _>>()?;
                let key: Vec<ValueKey> = key_values
                    .iter()
                    .zip(&group_collations)
                    .map(|(value, collation)| ValueKey::from_with_collation(value, *collation))
                    .collect();
                let entry = buckets.entry(key).or_insert_with(|| {
                    let bucket = GroupBucket {
                        keys: key_values,
                        sample: row.clone(),
                        accumulators: slots
                            .iter()
                            .map(|s| s.make_accumulator_for_row(row))
                            .collect(),
                        insert_order: next_order,
                    };
                    next_order += 1;
                    bucket
                });
                feed(&slots, &mut entry.accumulators, row)?;
            }
        }

        // Finalise each bucket and emit projected rows.
        let mut buckets_vec: Vec<GroupBucket> = buckets.into_values().collect();
        buckets_vec.sort_by_key(|b| b.insert_order);
        let mut group_rows: Vec<(Vec<Value>, Vec<Value>)> = Vec::new();
        for bucket in buckets_vec {
            let GroupBucket {
                keys,
                mut sample,
                accumulators,
                ..
            } = bucket;
            // Bake aggregate results into the sample row's cache.
            let mut cache: std::collections::HashMap<u64, Value> =
                std::collections::HashMap::with_capacity(slots.len());
            for (slot, acc) in slots.iter().zip(accumulators.into_iter()) {
                cache.insert(slot.key, acc.finalize()?);
            }
            for (idx, value) in keys.into_iter().enumerate() {
                // `sample` is cloned from an input row whose wide-table
                // lookup may already have been populated by WHERE / JOIN
                // evaluation. Appending a synthetic group key must invalidate
                // that lookup, otherwise GroupKeyRef resolves against the
                // pre-aggregate field set and reports unknown `group_N`.
                sample.push_field(self::aggregate::group_marker_field(idx, value));
            }
            sample.group = Some(Vec::new());
            sample.agg_values = Some(std::sync::Arc::new(cache));
            // HAVING (uses cache).
            if let Some(having) = &stmt.having {
                if !self::eval::eval_aggregate_bool(having, &[], &sample)? {
                    continue;
                }
            }
            // Projection.
            let mut output: Vec<Value> = Vec::new();
            for item in &stmt.projection {
                match item {
                    SelectItem::Wildcard => {
                        for f in &sample.fields {
                            output.push((f.value).clone());
                        }
                    }
                    SelectItem::QualifiedWildcard(qualifier) => {
                        for f in &sample.fields {
                            if f.alias
                                .as_deref()
                                .map(|a| a.eq_ignore_ascii_case(qualifier))
                                .unwrap_or(false)
                            {
                                output.push((f.value).clone());
                            }
                        }
                    }
                    SelectItem::Expr { expr, .. } => {
                        output.push(self::eval::eval_aggregate_expr(expr, &[], &sample)?);
                    }
                }
            }
            let order_values = stmt
                .order_by
                .iter()
                .map(|order| {
                    if let Some(index) = projection_index_for_order(&stmt.projection, &order.expr) {
                        Ok(output.get(index).cloned().unwrap_or(Value::Null))
                    } else {
                        self::eval::eval_aggregate_expr(&order.expr, &[], &sample)
                    }
                })
                .collect::<Result<Vec<_>, QueryError>>()?;
            group_rows.push((output, order_values));
        }
        if !stmt.order_by.is_empty() {
            let fields: Vec<_> = stmt
                .order_by
                .iter()
                .map(|order| {
                    let mut field = crate::engine::executor::sort::SortField::from_order_by(order);
                    field.collation_id =
                        infer_expr_descriptor(&order.expr, &all_sources).collation_id;
                    field
                })
                .collect();
            group_rows.sort_by(|a, b| {
                crate::engine::executor::sort::compare_sort_keys(&fields, &a.1, &b.1)
            });
        }
        out_rows = group_rows.into_iter().map(|(row, _)| row).collect();
    } else {
        // Non-aggregating path: ORDER BY / LIMIT / projection over rows.
        if !stmt.order_by.is_empty() {
            self::eval::sort_plan_rows_by_order(&mut current_rows, &stmt.order_by)?;
        }
        let output_offset = limit_value_or_zero(stmt.offset.as_ref())?;
        let output_bound = if stmt.distinct {
            None
        } else {
            optional_limit_value(stmt.limit.as_ref())?
                .map(|limit| limit.saturating_add(output_offset))
        };
        for row in &current_rows {
            if output_bound.is_some_and(|bound| out_rows.len() >= bound) {
                break;
            }
            if let Some(having) = &stmt.having {
                if !self::eval::eval_plan_bool(having, row)? {
                    continue;
                }
            }
            let mut output: Vec<Value> = Vec::new();
            for item in &stmt.projection {
                match item {
                    SelectItem::Wildcard => {
                        for f in &row.fields {
                            output.push((f.value).clone());
                        }
                    }
                    SelectItem::QualifiedWildcard(qualifier) => {
                        for f in &row.fields {
                            if f.alias
                                .as_deref()
                                .map(|a| a.eq_ignore_ascii_case(qualifier))
                                .unwrap_or(false)
                            {
                                output.push((f.value).clone());
                            }
                        }
                    }
                    SelectItem::Expr { expr, .. } => {
                        let v = self::eval::eval_plan_expr(expr, row)?;
                        output.push(v);
                    }
                }
            }
            out_rows.push(output);
        }
    }
    // DISTINCT.
    if stmt.distinct {
        // Preserve each projected expression's collation in its typed key.
        let mut seen: AhHashSet<Vec<u8>> = ah_hash_set();
        let mut deduped: Vec<Vec<Value>> = Vec::new();
        let mut key: Vec<u8> = Vec::new();
        for row in out_rows {
            key.clear();
            for (value, column) in row.iter().zip(&out_columns) {
                crate::engine::executor::vec::key_encode::encode_with_collation(
                    value,
                    column.source.collation_id,
                    &mut key,
                );
            }
            if seen.insert(key.clone()) {
                deduped.push(row);
            }
        }
        out_rows = deduped;
    }
    let off = limit_value_or_zero(stmt.offset.as_ref())?;
    out_rows.drain(..off.min(out_rows.len()));
    if let Some(limit) = optional_limit_value(stmt.limit.as_ref())? {
        out_rows.truncate(limit);
    }
    let _ = _snapshot_guard_marker(); // silence unused-import lints if any
    Ok(QueryResponse::ResultSet(ResultSet {
        columns: out_columns,
        rows: out_rows,
    }))
}

fn materialize_filtered_system_source(
    db: &Arc<Database>,
    table_ref: &TableRef,
    selection: Option<Expr>,
    registry: &SessionRegistry,
) -> Result<MaterializedSource, QueryError> {
    let alias = table_ref.alias.clone().unwrap_or_else(|| {
        table_ref
            .name
            .rsplit_once('.')
            .map(|(_, table)| table.to_string())
            .unwrap_or_else(|| table_ref.name.clone())
    });
    let star = Select {
        projection: vec![SelectItem::Wildcard],
        table: Some(table_ref.name.clone()),
        from: vec![table_ref.clone()],
        selection,
        ..Select::default()
    };
    let QueryResponse::ResultSet(result) = execute_system_select(db, star, registry)? else {
        return Err(error(
            crate::error::catalog::ER_GET_ERRNO,
            "system table materialization failed",
        ));
    };
    Ok(MaterializedSource {
        alias,
        table_name: table_ref.name.clone(),
        columns: result.columns,
        rows: SourceRows::Owned(result.rows),
    })
}

fn _snapshot_guard_marker() {}

/// When ORDER BY references a select-list expression by alias or by
/// repeating the expression, return the projection index. Letting ORDER
/// BY look up the already-computed value avoids re-evaluating aggregates
/// against possibly-stale group state.
fn projection_index_for_order(projection: &[SelectItem], order: &Expr) -> Option<usize> {
    for (i, item) in projection.iter().enumerate() {
        if let SelectItem::Expr { expr, alias } = item {
            // ORDER BY <alias>
            if let Some(a) = alias {
                if let Expr::Identifier(id) = order {
                    if id.eq_ignore_ascii_case(a) {
                        return Some(i);
                    }
                }
            }
            // ORDER BY repeats the projection expression verbatim.
            if expr_semantically_eq(expr, order) {
                return Some(i);
            }
        }
    }
    None
}

/// Backing rows for a JOIN source. An in-memory CTE source borrows the
/// shared `Arc<CteResult>` rows instead of cloning the whole `Vec<Vec<Value>>`:
/// a CTE joined N times (TPC-DS year_total self-join shape) would otherwise
/// pay N full deep copies of the materialised set on top of the per-row copy
/// `source_to_plan_rows` already does.
///
/// **P0-A2**: single-source predicate pushdown over a `SharedCte` no longer
/// rewrites to `Owned(kept)` (which deep-copied the survivors per reference);
/// instead it produces `SharedCteSelected { result, indices }` — an `Arc<Vec<u32>>`
/// of row indices into the shared store. N references with different filters
/// keep the same underlying `Arc<CteResult>` and only pay an O(kept) `u32`
/// index vector each, plus a one-time `source_to_plan_rows` gather over the
/// selected rows. `Owned` slots (non-CTE derived) keep the original deep-copy
/// path since they cannot be shared.
#[derive(Clone)]
enum SourceRows {
    Owned(Vec<Vec<Value>>),
    SpilledCteCollected {
        rows: Arc<Vec<Vec<Value>>>,
        _charge: Option<Arc<crate::engine::executor::mem_budget::OwnedMemCharge>>,
    },
    SharedCte(Arc<crate::engine::api::CteResult>),
    SharedCteSelected {
        result: Arc<crate::engine::api::CteResult>,
        indices: Arc<Vec<u32>>,
    },
}

/// Iterator over a `SourceRows`. `Owned`/`SharedCte` walk the underlying
/// slice directly; `SharedCteSelected` dereferences the selection indices
/// into the shared store. Returned by `SourceRows::iter_rows`; the per-row
/// `&Vec<Value>` borrow remains valid for the lifetime of the source.
enum SourceRowsIter<'a> {
    Slice(std::slice::Iter<'a, Vec<Value>>),
    Selected {
        store_rows: &'a [Vec<Value>],
        indices: std::slice::Iter<'a, u32>,
    },
}

impl<'a> Iterator for SourceRowsIter<'a> {
    type Item = &'a Vec<Value>;
    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        match self {
            SourceRowsIter::Slice(it) => it.next(),
            SourceRowsIter::Selected {
                store_rows,
                indices,
            } => indices.next().map(|&i| &store_rows[i as usize]),
        }
    }
    #[inline]
    fn size_hint(&self) -> (usize, Option<usize>) {
        match self {
            SourceRowsIter::Slice(it) => it.size_hint(),
            SourceRowsIter::Selected { indices, .. } => indices.size_hint(),
        }
    }
}

impl ExactSizeIterator for SourceRowsIter<'_> {}

impl SourceRows {
    /// Iterate the backing rows. Respects `SharedCteSelected`'s selection.
    #[inline]
    fn iter_rows(&self) -> SourceRowsIter<'_> {
        match self {
            SourceRows::Owned(v) => SourceRowsIter::Slice(v.iter()),
            SourceRows::SpilledCteCollected { rows, .. } => SourceRowsIter::Slice(rows.iter()),
            SourceRows::SharedCte(c) => SourceRowsIter::Slice(c.rows().iter()),
            SourceRows::SharedCteSelected { result, indices } => SourceRowsIter::Selected {
                store_rows: result.rows(),
                indices: indices.iter(),
            },
        }
    }
    #[inline]
    fn is_empty(&self) -> bool {
        self.len() == 0
    }
    #[inline]
    fn len(&self) -> usize {
        match self {
            SourceRows::Owned(v) => v.len(),
            SourceRows::SpilledCteCollected { rows, .. } => rows.len(),
            SourceRows::SharedCte(c) => c.nrows(),
            SourceRows::SharedCteSelected { indices, .. } => indices.len(),
        }
    }
    /// When this source is backed by a CTE materialization result, return the
    /// shared `CteHandle`. P0-B (cross-reference hash build sharing) uses this
    /// entry to grab the `CteStore` and attach it directly onto `CteStore.builds`,
    /// bypassing the `CteResult` wrapper. `Owned` and spill forms return `None`.
    #[allow(dead_code)]
    #[inline]
    fn cte_handle(&self) -> Option<crate::engine::executor::cte_store::CteHandle> {
        match self {
            SourceRows::Owned(_) | SourceRows::SpilledCteCollected { .. } => None,
            SourceRows::SharedCte(c) => Some(c.store.clone()),
            SourceRows::SharedCteSelected { result, .. } => Some(result.store.clone()),
        }
    }
    /// Underlying CTE result for `SharedCte`/`SharedCteSelected`. Used by
    /// P0-A2 pushdown to share the result across reference rewrites and by
    /// P0-B hash-build sharing to key the build cache.
    #[inline]
    fn cte_result(&self) -> Option<&Arc<crate::engine::api::CteResult>> {
        match self {
            SourceRows::Owned(_) | SourceRows::SpilledCteCollected { .. } => None,
            SourceRows::SharedCte(c) => Some(c),
            SourceRows::SharedCteSelected { result, .. } => Some(result),
        }
    }
    /// Selection indices when present. `None` means "all rows" (or `Owned`).
    #[inline]
    fn cte_selection(&self) -> Option<&Arc<Vec<u32>>> {
        match self {
            SourceRows::SharedCteSelected { indices, .. } => Some(indices),
            _ => None,
        }
    }
}

#[derive(Clone)]
struct MaterializedSource {
    /// Identifier used for `qualified.*` and `qualifier.col` resolution.
    alias: String,
    /// Original FROM/JOIN name (CTE name or qualified table name).
    table_name: String,
    columns: Vec<Column>,
    rows: SourceRows,
}

fn materialize_join_source_with_derived(
    db: &Arc<Database>,
    txn: TxnView,
    table_ref: &TableRef,
    derived: Option<&SubQuery>,
    registry: &SessionRegistry,
) -> Result<MaterializedSource, QueryError> {
    // The `table_ref.name` carries the `database.table` qualifier that
    // `qualify_select_tables` stamped on (e.g. `test.t`). Use only the
    // bare table portion as the default alias when the user did not write
    // an explicit `AS x`, so `t.col` in user SQL resolves through the
    // alias-shadows-name match (a qualified `test.t` would otherwise fail
    // to match the bare `t` qualifier the user typed).
    let alias = table_ref.alias.clone().unwrap_or_else(|| {
        table_ref
            .name
            .rsplit_once('.')
            .map(|(_, t)| t.to_string())
            .unwrap_or_else(|| table_ref.name.clone())
    });
    // 1) JOIN (SELECT ...) AS sub  /  JOIN (SELECT ... UNION ...) AS sub
    if let Some(sub) = derived {
        let mut rs = crate::engine::executor::dispatch::execute_subquery(db, txn, sub, registry)?;
        // Some constant-select adapters expose only value/type metadata.  A
        // derived column must additionally retain the collation derivation of
        // its defining expression; the outer query block no longer has access
        // to that AST and otherwise cannot apply MySQL's coercibility rules.
        // Set only missing metadata so catalog-backed and explicitly supplied
        // protocol sources remain authoritative.
        annotate_derived_column_collations(sub, &mut rs.columns);
        crate::planner::apply_derived_column_aliases(
            table_ref,
            rs.columns.iter_mut().map(|column| &mut column.name),
        )?;
        return Ok(MaterializedSource {
            alias: alias.clone(),
            // Keep the internal nested-join marker.  `source_to_plan_rows`
            // uses it to retain the leaf aliases of an unaliased
            // parenthesised join group; ordinary derived tables still have
            // `table_ref.name == alias`.
            table_name: table_ref.name.clone(),
            columns: rs.columns,
            rows: SourceRows::Owned(rs.rows),
        });
    }
    // 2) JOIN cte_name
    if let Some(cte) = super::cte::lookup_cte(&table_ref.name) {
        // A large CTE may have spilled its rows to disk; read them back
        // (transient, flows into the JOIN which has its own spill path).
        // An in-memory CTE borrows its shared `Arc<CteResult>` rows instead
        // of deep-copying the whole set (P0-1).
        let columns = cte.columns.clone();
        let rows = match &cte.spill {
            Some(spill) => {
                let collected = super::cte::spilled_cte_read_all(spill)?;
                SourceRows::SpilledCteCollected {
                    rows: collected.rows,
                    _charge: collected.charge,
                }
            }
            None => SourceRows::SharedCte(cte),
        };
        return Ok(MaterializedSource {
            alias,
            table_name: table_ref.name.clone(),
            columns,
            rows,
        });
    }
    // 2.5) JOIN generate_series(...): materialise via the single-source path
    // by selecting the series on its own (the dispatch detects the
    // `generate_series` spec on FROM[0] and emits a `GenerateSeries` leaf).
    if table_ref.generate_series.is_some() {
        let star = Select {
            projection: vec![SelectItem::Wildcard],
            table: Some(table_ref.name.clone()),
            from: vec![table_ref.clone()],
            ..Select::default()
        };
        let response = execute_select(db, txn, star, registry)?.materialize()?;
        let QueryResponse::ResultSet(rs) = response else {
            return Err(error(
                crate::error::catalog::ER_NOT_SUPPORTED_YET,
                format!("could not materialise generate_series JOIN '{}'", alias),
            ));
        };
        return Ok(MaterializedSource {
            alias: alias.clone(),
            table_name: alias,
            columns: rs.columns,
            rows: SourceRows::Owned(rs.rows),
        });
    }
    // 3) JOIN <catalog table>
    let star = Select {
        projection: vec![SelectItem::Wildcard],
        table: Some(table_ref.name.clone()),
        from: vec![TableRef {
            name: table_ref.name.clone(),
            alias: None,
            system_time: None,
            index_hints: vec![],
            partitions: vec![],
            column_aliases: Vec::new(),
            generate_series: None,
        }],
        ..Select::default()
    };
    let response = execute_select(db, txn, star, registry)?.materialize()?;
    let QueryResponse::ResultSet(rs) = response else {
        return Err(error(
            crate::error::catalog::ER_NOT_SUPPORTED_YET,
            format!("could not materialise FROM target '{}'", table_ref.name),
        ));
    };
    Ok(MaterializedSource {
        alias,
        table_name: table_ref.name.clone(),
        columns: rs.columns,
        rows: SourceRows::Owned(rs.rows),
    })
}

fn materialize_lateral_join_source_for_row(
    db: &Arc<Database>,
    txn: TxnView,
    table_ref: &TableRef,
    derived: &SubQuery,
    left_row: &self::plan::PlanRow,
    registry: &SessionRegistry,
) -> Result<MaterializedSource, QueryError> {
    let alias = table_ref.alias.clone().unwrap_or_else(|| {
        table_ref
            .name
            .rsplit_once('.')
            .map(|(_, t)| t.to_string())
            .unwrap_or_else(|| table_ref.name.clone())
    });
    let mut bound = self::eval::bind_correlated_subquery_tree(db, derived, left_row);
    // JSON_TABLE is implicitly lateral. Bind its document expression directly
    // against the current joined row as a correctness backstop: unlike normal
    // derived projections, the expression lives in `Select::json_table` and
    // older generic correlated-subquery walkers did not visit that field.
    if let (SubQuery::Select(original), SubQuery::Select(bound_select)) = (derived, &mut bound) {
        if let (Some(original), Some(bound_source)) = (
            original.json_table.as_ref(),
            bound_select.json_table.as_mut(),
        ) {
            bound_source.expr =
                Expr::BoundValue(self::eval::eval_plan_expr(&original.expr, left_row)?);
        }
    }
    let namespace = derived as *const SubQuery as usize;
    let response = crate::engine::expr::with_rand_occurrence_namespace(namespace, || {
        crate::engine::executor::dispatch::execute_subquery_response(db, txn, &bound, registry)
    })?
    .materialize()?;
    let QueryResponse::ResultSet(mut rs) = response else {
        return Err(error(
            crate::error::catalog::ER_NOT_SUPPORTED_YET,
            format!("could not materialise lateral derived JOIN '{alias}'"),
        ));
    };
    crate::planner::apply_derived_column_aliases(
        table_ref,
        rs.columns.iter_mut().map(|column| &mut column.name),
    )?;
    Ok(MaterializedSource {
        alias: alias.clone(),
        table_name: alias,
        columns: rs.columns,
        rows: SourceRows::Owned(rs.rows),
    })
}

fn null_plan_row_for_sources(sources: &[MaterializedSource]) -> self::plan::PlanRow {
    let mut fields = Vec::new();
    for source in sources {
        let bare_table = source
            .table_name
            .rsplit_once('.')
            .map(|(_, t)| t.to_string())
            .unwrap_or_else(|| source.table_name.clone());
        for column in &source.columns {
            fields.push(self::plan::PlanField {
                meta: crate::engine::executor::select::plan::intern_col_meta(ColMeta {
                    invisible: false,
                    table: bare_table.clone(),
                    alias: Some(source.alias.clone()),
                    column: column.name.clone(),
                    column_type: column.column_type,
                    source: column.source.clone(),
                }),
                value: Value::Null,
            });
        }
    }
    self::plan::PlanRow {
        fields,
        group: None,
        agg_values: None,
        schema: PlanRow::empty_schema(),
    }
}

fn nested_loop_lateral_join(
    db: &Arc<Database>,
    txn: TxnView,
    left_rows: Vec<self::plan::PlanRow>,
    table_ref: &TableRef,
    derived: &SubQuery,
    join_type: crate::parser::JoinType,
    condition: Option<&Expr>,
    registry: &SessionRegistry,
    left_sources: &[MaterializedSource],
) -> Result<(Vec<self::plan::PlanRow>, MaterializedSource), QueryError> {
    use crate::parser::JoinType;
    if matches!(join_type, JoinType::Right) {
        return Err(error(
            crate::error::catalog::ER_NOT_SUPPORTED_YET,
            "RIGHT LATERAL join cannot preserve a row that depends on its NULL-extended left side",
        ));
    }
    let alias = table_ref
        .alias
        .clone()
        .unwrap_or_else(|| table_ref.name.clone());
    if left_rows.is_empty() {
        let null_left = null_plan_row_for_sources(left_sources);
        let source = materialize_lateral_join_source_for_row(
            db, txn, table_ref, derived, &null_left, registry,
        )?;
        return Ok((
            Vec::new(),
            MaterializedSource {
                alias: alias.clone(),
                table_name: alias,
                columns: source.columns,
                rows: SourceRows::Owned(Vec::new()),
            },
        ));
    }

    let mut out = Vec::new();
    let mut right_columns: Option<Vec<Column>> = None;
    for left in &left_rows {
        let right_source =
            materialize_lateral_join_source_for_row(db, txn, table_ref, derived, left, registry)?;
        if let Some(columns) = &right_columns {
            if columns.len() != right_source.columns.len() {
                return Err(error(
                    crate::error::catalog::ER_WRONG_NUMBER_OF_COLUMNS_IN_SELECT,
                    "lateral derived table returned a different column count",
                ));
            }
        } else {
            right_columns = Some(right_source.columns.clone());
        }
        let right_rows = source_to_plan_rows(&right_source);
        let mut left_matched = false;
        for right_row in &right_rows {
            let mut fields = left.fields.clone();
            fields.extend(right_row.fields.iter().cloned());
            let joined = self::plan::PlanRow {
                fields,
                group: None,
                agg_values: None,
                schema: PlanRow::empty_schema(),
            };
            let pass = match condition {
                Some(cond) => self::eval::eval_plan_bool(cond, &joined)?,
                None => true,
            };
            if pass {
                left_matched = true;
                match join_type {
                    JoinType::Semi => {
                        out.push(left.clone());
                        break;
                    }
                    JoinType::Anti => break,
                    _ => out.push(joined),
                }
            }
        }
        if matches!(join_type, JoinType::Anti) && !left_matched {
            out.push(left.clone());
        }
        if matches!(join_type, JoinType::Left) && !left_matched {
            let right_null_source = MaterializedSource {
                alias: alias.clone(),
                table_name: alias.clone(),
                columns: right_columns.clone().unwrap_or_default(),
                rows: SourceRows::Owned(Vec::new()),
            };
            let mut fields = left.fields.clone();
            fields.extend(null_right_fields(&right_null_source));
            out.push(self::plan::PlanRow {
                fields,
                group: None,
                agg_values: None,
                schema: PlanRow::empty_schema(),
            });
        }
    }

    Ok((
        out,
        MaterializedSource {
            alias: alias.clone(),
            table_name: alias,
            columns: right_columns.unwrap_or_default(),
            rows: SourceRows::Owned(Vec::new()),
        },
    ))
}

/// Intern the per-column `ColMeta` for a source *once*. A source's
/// alias/table/column shape is fixed, so interning per row (or per join
/// match) repeats the same `intern_col_meta` hashmap lookup for every cell
/// — millions of redundant lookups on a wide CTE self-join. Callers
/// materialising many rows from one source pre-compute this `&'static`
/// pointer array and copy (pointer copy) it per cell instead.
fn intern_source_metas(source: &MaterializedSource, bare_table: &str) -> Vec<plan::ColMetaRef> {
    source
        .columns
        .iter()
        .map(|col| {
            let preserve_nested_qualifier =
                source.table_name == "$nested_join_preserve" && !col.source.table.is_empty();
            let table = if preserve_nested_qualifier {
                if col.source.org_table.is_empty() {
                    col.source.table.clone()
                } else {
                    col.source.org_table.clone()
                }
            } else {
                bare_table.to_string()
            };
            let alias = if preserve_nested_qualifier {
                Some(col.source.table.clone())
            } else {
                Some(source.alias.clone())
            };
            crate::engine::executor::select::plan::intern_col_meta(ColMeta {
                invisible: false,
                table,
                alias,
                column: col.name.clone(),
                column_type: col.column_type,
                source: col.source.clone(),
            })
        })
        .collect()
}

fn source_to_plan_rows(source: &MaterializedSource) -> Vec<self::plan::PlanRow> {
    // Strip a leading database qualifier so the in-memory join can resolve
    // both `t.col` (user-written) and `test.t.col` if needed; PlanField.table
    // for real tables in mydb stores the bare table name.
    let bare_table = source
        .table_name
        .rsplit_once('.')
        .map(|(_, t)| t.to_string())
        .unwrap_or_else(|| source.table_name.clone());
    let metas = intern_source_metas(source, &bare_table);
    source
        .rows
        .iter_rows()
        .map(|row| self::plan::PlanRow {
            fields: metas
                .iter()
                .zip(row.iter())
                .map(|(meta, val)| self::plan::PlanField {
                    meta: meta.clone(),
                    value: val.clone(),
                })
                .collect(),
            group: None,
            agg_values: None,
            schema: PlanRow::empty_schema(),
        })
        .collect()
}

fn null_right_fields(right: &MaterializedSource) -> Vec<self::plan::PlanField> {
    let bare = right
        .table_name
        .rsplit_once('.')
        .map(|(_, t)| t.to_string())
        .unwrap_or_else(|| right.table_name.clone());
    right
        .columns
        .iter()
        .map(|col| self::plan::PlanField {
            meta: {
                let preserve_nested_qualifier =
                    right.table_name == "$nested_join_preserve" && !col.source.table.is_empty();
                crate::engine::executor::select::plan::intern_col_meta(ColMeta {
                    invisible: false,
                    table: if preserve_nested_qualifier {
                        if col.source.org_table.is_empty() {
                            col.source.table.clone()
                        } else {
                            col.source.org_table.clone()
                        }
                    } else {
                        bare.clone()
                    },
                    alias: if preserve_nested_qualifier {
                        Some(col.source.table.clone())
                    } else {
                        Some(right.alias.clone())
                    },
                    source: col.source.clone(),
                    column: col.name.clone(),
                    column_type: col.column_type,
                })
            },
            value: Value::Null,
        })
        .collect()
}

/// P0-B helper: build a single right-side `PlanRow` from a raw CTE store
/// row using a pre-interned `ColMeta` pointer array (see
/// `intern_source_metas`). The cached hash build holds only `Vec<u32>`
/// store indices (alias-agnostic) so it can be reused across references;
/// the current reference's alias/table metadata is interned once by the
/// caller and copied (pointer copy) per cell here.
fn build_right_plan_row_from_store(
    metas: &[plan::ColMetaRef],
    store_row: &[Value],
) -> self::plan::PlanRow {
    let fields: Vec<self::plan::PlanField> = metas
        .iter()
        .zip(store_row.iter())
        .map(|(meta, val)| self::plan::PlanField {
            meta: meta.clone(),
            value: val.clone(),
        })
        .collect();
    self::plan::PlanRow {
        fields,
        group: None,
        agg_values: None,
        schema: PlanRow::empty_schema(),
    }
}

/// Attempt a hash-based equi-join when the condition decomposes into one
/// or more equi-conjuncts whose two sides cleanly partition between the
/// left and right inputs. Uses a sample row from each side to decide
/// sidedness — if `eval_plan_expr` succeeds against only the left sample
/// row, the expression resolves entirely in left fields, and likewise
/// for right. Conjuncts that fail this purity check (or non-equi
/// conjuncts) are evaluated as a residual on every matched pair.
///
/// Supports INNER plus the LEFT / RIGHT / FULL outer flavours: the hash is
/// always built on the right input and probed by the left, with the outer
/// side(s) padded with NULLs for rows that find no passing match. A NULL
/// equi-key never matches (SQL semantics), so on an outer side it preserves
/// the row NULL-extended. This is exactly the shape TPC-DS q78 needs — three
/// column-list-free CTEs chained by multi-column `LEFT JOIN`s, which otherwise
/// degrade to an O(N×M) nested loop over the materialised CTE row sets.
///
/// Returns `Ok(Some(rows))` if at least one equi-conjunct could be
/// extracted (so the hash path is profitable); `Ok(None)` otherwise so
/// the caller falls through to the original NLJ.
fn try_hash_join(
    left_rows: &[self::plan::PlanRow],
    right_rows: &[self::plan::PlanRow],
    right_source: &MaterializedSource,
    cond: &Expr,
    join_type: crate::parser::JoinType,
    right_sample: &self::plan::PlanRow,
    left_sample: &self::plan::PlanRow,
) -> Result<Option<Vec<self::plan::PlanRow>>, QueryError> {
    use crate::engine::expr::ValueKey;
    use crate::parser::{BinaryOp, JoinType};
    // Only INNER and the outer flavours have a meaningful build-right /
    // probe-left hash shape here; semi/anti/cross keep the NLJ path.
    if !matches!(
        join_type,
        JoinType::Inner | JoinType::Left | JoinType::Right | JoinType::Full
    ) {
        return Ok(None);
    }
    let mut conjuncts: Vec<&Expr> = Vec::new();
    collect_top_level_and(cond, &mut conjuncts);
    let mut left_keys: Vec<Expr> = Vec::new();
    let mut right_keys: Vec<Expr> = Vec::new();
    let mut residuals: Vec<&Expr> = Vec::new();
    for c in &conjuncts {
        if let Expr::Binary { left, op, right } = c {
            if matches!(*op, BinaryOp::Eq) {
                let l_in_left = self::eval::eval_plan_expr(left, left_sample).is_ok();
                let l_in_right = self::eval::eval_plan_expr(left, right_sample).is_ok();
                let r_in_left = self::eval::eval_plan_expr(right, left_sample).is_ok();
                let r_in_right = self::eval::eval_plan_expr(right, right_sample).is_ok();
                if l_in_left && !l_in_right && r_in_right && !r_in_left {
                    left_keys.push((**left).clone());
                    right_keys.push((**right).clone());
                    continue;
                }
                if l_in_right && !l_in_left && r_in_left && !r_in_right {
                    left_keys.push((**right).clone());
                    right_keys.push((**left).clone());
                    continue;
                }
            }
        }
        residuals.push(c);
    }
    if left_keys.is_empty() {
        return Ok(None);
    }
    let descriptor = |expr: &Expr, sample: &PlanRow| {
        let resolve = |expr: &Expr| {
            let field = match expr {
                Expr::Identifier(name) => {
                    self::eval::resolve_plan_field(sample, None, name).ok()?
                }
                Expr::CompoundIdentifier(parts) => self::eval::resolve_plan_field(
                    sample,
                    (parts.len() >= 2).then(|| parts[parts.len() - 2].as_str()),
                    parts.last()?,
                )
                .ok()?,
                _ => return None,
            };
            Some(crate::engine::expr_type::ExprType::from_column_type(
                field.column_type,
                &field.source,
            ))
        };
        crate::engine::expr_type::infer_expr_type(
            expr,
            &crate::engine::expr_type::TypeContext::context_free(&resolve, &|_| None),
        )
    };
    let mut comparison_rechecks = Vec::new();
    for (left, right) in left_keys.iter_mut().zip(&mut right_keys) {
        let left_type = descriptor(left, left_sample);
        let right_type = descriptor(right, right_sample);
        if crate::engine::expr_type::equality_key_needs_recheck(left_type.kind, right_type.kind) {
            comparison_rechecks.push(Expr::Binary {
                left: Box::new(left.clone()),
                op: BinaryOp::Eq,
                right: Box::new(right.clone()),
            });
        }
        crate::engine::expr_type::normalize_equality_key_exprs(
            left,
            right,
            left_type.kind,
            right_type.kind,
        );
    }
    residuals.extend(comparison_rechecks.iter());
    let preserve_left = matches!(join_type, JoinType::Left | JoinType::Full);
    let preserve_right = matches!(join_type, JoinType::Right | JoinType::Full);
    // Compile the equi-join key expressions once against each side's row
    // shape. The per-row key extraction below then resolves column refs by
    // direct field index (or a compiled `ExprProgram` for compound keys)
    // instead of the legacy by-name `resolve_plan_field` walk — the #1
    // hotspot on TPC-DS q97's `ssci FULL JOIN csci` (830k build+probe key
    // evals, each a 2-column surrogate-int lookup). Falls back to the legacy
    // interpreter for any shape the compiler can't lower (same contract as
    // every other `CompiledPlanExpr` call site).
    let right_key_compiled: Vec<self::eval::CompiledPlanExpr> = {
        let schema = self::eval::input_schema_for_plan_row(right_sample);
        right_keys
            .iter()
            .map(|k| self::eval::CompiledPlanExpr::compile_once(k, &schema))
            .collect()
    };
    let left_key_compiled: Vec<self::eval::CompiledPlanExpr> = {
        let schema = self::eval::input_schema_for_plan_row(left_sample);
        left_keys
            .iter()
            .map(|k| self::eval::CompiledPlanExpr::compile_once(k, &schema))
            .collect()
    };
    let key_collations: Vec<_> = left_keys
        .iter()
        .zip(&right_keys)
        .map(|(left, right)| {
            self::eval::plan_expr_collation(left, left_sample)
                .map(|c| c.id)
                .or_else(|| self::eval::plan_expr_collation(right, right_sample).map(|c| c.id))
        })
        .collect();
    // **P0-B**: try cross-reference hash-build sharing when the right side
    // is a CTE source and every right key is a simple column reference.
    // Cache lives on `CteStore.builds`, keyed by the column positions in the
    // CTE's column list (alias-agnostic — `year_total t1/t2/...` 4-way self
    // joins on `customer_id` share one bucket map). Restricted to residual-free
    // Inner/Left joins so the shared bucket is not polluted with rows that an
    // alias-local predicate would immediately discard.
    let cache_attempt: Option<(crate::engine::executor::cte_store::CteHandle, Vec<usize>)> = if matches!(join_type, JoinType::Inner | JoinType::Left)
            && residuals.is_empty()
            // Only when this reference sees the *full* store (no per-reference
            // selection). A filtered reference would have to mask its surviving
            // rows out of a build that also mixes in every *other* reference's
            // rows, and that bucket pollution costs more than the shared build
            // saves: TPC-DS q74/q4 self-join on `customer_id` but each alias
            // filters a different `sale_type`/`year`, so a shared `customer_id`
            // bucket is mostly rows this reference discards.
            && right_source.rows.cte_selection().is_none()
            // ...and only when the CTE is genuinely consumed more than once, so
            // the one-time full-store build amortises across references.
            && right_source
                .rows
                .cte_result()
                .is_some_and(|r| r.consumer_count > 1)
    {
        right_source.rows.cte_handle().and_then(|handle| {
            let positions: Option<Vec<usize>> = right_keys
                .iter()
                .map(|e| {
                    let name = match e {
                        Expr::Identifier(name) => Some(name.as_str()),
                        Expr::CompoundIdentifier(parts) => parts.last().map(|s| s.as_str()),
                        _ => None,
                    }?;
                    right_source
                        .columns
                        .iter()
                        .position(|c| c.name.eq_ignore_ascii_case(name))
                })
                .collect();
            positions
                .filter(|positions| {
                    positions.iter().zip(&key_collations).all(|(p, collation)| {
                        right_source.columns[*p].source.collation_id == *collation
                    })
                })
                .map(|p| (handle, p))
        })
    } else {
        None
    };

    // Cached bucket map + per-reference selection bitset (None → all rows).
    let cached_build: Option<std::sync::Arc<crate::engine::executor::cte_store::CachedHashBuild>>;
    let selection_bitset: Option<Vec<bool>>;
    // Local build path (used when cache is not applicable).
    let mut buckets: hashbrown::HashMap<Vec<ValueKey>, Vec<usize>, ahash::RandomState> =
        hashbrown::HashMap::with_hasher(ahash::RandomState::new());

    if let Some((handle, positions)) = cache_attempt {
        let store_rows_len = handle.dense_rows().len();
        let handle_for_build = handle.clone();
        let positions_for_build = positions.clone();
        let cached = handle.builds.get_or_init(
            positions,
            || -> Result<crate::engine::executor::cte_store::CachedHashBuild, QueryError> {
                let store_rows = handle_for_build.dense_rows();
                let mut b: hashbrown::HashMap<Vec<ValueKey>, Vec<u32>, ahash::RandomState> =
                    hashbrown::HashMap::with_hasher(ahash::RandomState::new());
                for (i, row) in store_rows.iter().enumerate() {
                    let mut key = Vec::with_capacity(positions_for_build.len());
                    let mut has_null = false;
                    for (&p, collation) in positions_for_build.iter().zip(&key_collations) {
                        let v = row.get(p).cloned().unwrap_or(crate::types::Value::Null);
                        if matches!(v, crate::types::Value::Null) {
                            has_null = true;
                            break;
                        }
                        key.push(ValueKey::from_with_collation(&v, *collation));
                    }
                    if !has_null {
                        b.entry(key).or_default().push(i as u32);
                    }
                }
                Ok(crate::engine::executor::cte_store::CachedHashBuild::with_buckets(b))
            },
        )?;
        let bitset = right_source.rows.cte_selection().map(|idx_vec| {
            let mut bs = vec![false; store_rows_len];
            for &i in idx_vec.iter() {
                if (i as usize) < bs.len() {
                    bs[i as usize] = true;
                }
            }
            bs
        });
        cached_build = Some(cached);
        selection_bitset = bitset;
    } else {
        // Build hash on right. ahash::RandomState (not std SipHash) — the
        // composite `Vec<ValueKey>` key hashes far cheaper, and a near-distinct
        // join (q97) probes the map once per build+probe row.
        for (i, row) in right_rows.iter().enumerate() {
            let mut key = Vec::with_capacity(right_key_compiled.len());
            let mut has_null = false;
            for (k, collation) in right_key_compiled.iter().zip(&key_collations) {
                let v = k.eval(row)?;
                if matches!(v, crate::types::Value::Null) {
                    has_null = true;
                    break;
                }
                key.push(ValueKey::from_with_collation(&v, *collation));
            }
            if !has_null {
                buckets.entry(key).or_default().push(i);
            }
            // A right row whose key is NULL can never match, so for RIGHT/FULL it
            // stays unmatched and is emitted NULL-left below (left in `right_matched`
            // as false).
        }
        cached_build = None;
        selection_bitset = None;
    }
    // NULL-padding template for the right side, built once (LEFT/FULL only).
    let null_right: Vec<self::plan::PlanField> = if preserve_left {
        null_right_fields(right_source)
    } else {
        Vec::new()
    };
    let mut right_matched = if preserve_right {
        vec![false; right_rows.len()]
    } else {
        Vec::new()
    };
    let mut out = Vec::new();
    // P0-B: when the cached build is in play, gather store rows by index
    // and lazily wrap each match in a `PlanRow` carrying this reference's
    // alias. Pre-bind the bare table + store-row slice so the inner loop
    // stays branch-free.
    let bare_table_for_cache: String = right_source
        .table_name
        .rsplit_once('.')
        .map(|(_, t)| t.to_string())
        .unwrap_or_else(|| right_source.table_name.clone());
    let right_store_rows: Option<&[Vec<Value>]> = if cached_build.is_some() {
        right_source.rows.cte_result().map(|r| r.rows())
    } else {
        None
    };
    // T2-2: intern this reference's column metadata once, outside the probe
    // loop. The cached build re-wraps matched store rows into `PlanRow`s on
    // every probe hit, and a high-selectivity self-join produces hundreds of
    // thousands of matches — interning per cell there was a measurable hotspot.
    let right_metas: Vec<plan::ColMetaRef> = if cached_build.is_some() {
        intern_source_metas(right_source, &bare_table_for_cache)
    } else {
        Vec::new()
    };
    for left in left_rows {
        let mut key = Vec::with_capacity(left_key_compiled.len());
        let mut has_null = false;
        for (k, collation) in left_key_compiled.iter().zip(&key_collations) {
            let v = k.eval(left)?;
            if matches!(v, crate::types::Value::Null) {
                has_null = true;
                break;
            }
            key.push(ValueKey::from_with_collation(&v, *collation));
        }
        let mut left_matched = false;
        if !has_null {
            if let Some(cached) = &cached_build {
                if let Some(matches) = cached.buckets.get(&key) {
                    let store_rows = right_store_rows.expect("cached path implies CTE store");
                    for &store_idx in matches {
                        if let Some(bs) = &selection_bitset {
                            if !bs.get(store_idx as usize).copied().unwrap_or(false) {
                                continue;
                            }
                        }
                        let raw = match store_rows.get(store_idx as usize) {
                            Some(r) => r,
                            None => continue,
                        };
                        let right_row = build_right_plan_row_from_store(&right_metas, raw);
                        let mut fields = left.fields.clone();
                        fields.extend(right_row.fields.into_iter());
                        let joined = self::plan::PlanRow {
                            fields,
                            group: None,
                            agg_values: None,
                            schema: PlanRow::empty_schema(),
                        };
                        if residuals.is_empty()
                            || residuals.iter().try_fold(
                                true,
                                |acc, r| -> Result<bool, QueryError> {
                                    Ok(acc && self::eval::eval_plan_bool(r, &joined)?)
                                },
                            )?
                        {
                            out.push(joined);
                            left_matched = true;
                        }
                    }
                }
            } else if let Some(matches) = buckets.get(&key) {
                for &ri in matches {
                    let right_row = &right_rows[ri];
                    let mut fields = left.fields.clone();
                    fields.extend(right_row.fields.iter().cloned());
                    let joined = self::plan::PlanRow {
                        fields,
                        group: None,
                        agg_values: None,
                        schema: PlanRow::empty_schema(),
                    };
                    if residuals.is_empty()
                        || residuals.iter().try_fold(
                            true,
                            |acc, r| -> Result<bool, QueryError> {
                                Ok(acc && self::eval::eval_plan_bool(r, &joined)?)
                            },
                        )?
                    {
                        out.push(joined);
                        left_matched = true;
                        if preserve_right {
                            right_matched[ri] = true;
                        }
                    }
                }
            }
        }
        if preserve_left && !left_matched {
            let mut fields = left.fields.clone();
            fields.extend(null_right.iter().cloned());
            out.push(self::plan::PlanRow {
                fields,
                group: None,
                agg_values: None,
                schema: PlanRow::empty_schema(),
            });
        }
    }
    if preserve_right {
        // Emit unmatched right rows with a NULL-left side, reconstructing the
        // left field shape from the first left row (left_rows is non-empty:
        // the caller only takes this path with both inputs populated).
        let null_left_template: Vec<self::plan::PlanField> = left_rows
            .first()
            .map(|first_left| {
                first_left
                    .fields
                    .iter()
                    .map(|f| self::plan::PlanField {
                        meta: crate::engine::executor::select::plan::intern_col_meta(ColMeta {
                            invisible: false,
                            table: f.table.clone(),
                            alias: f.alias.clone(),
                            column: f.column.clone(),
                            column_type: f.column_type,
                            source: f.source.clone(),
                        }),
                        value: Value::Null,
                    })
                    .collect()
            })
            .unwrap_or_default();
        for (j, matched) in right_matched.iter().enumerate() {
            if !*matched {
                let mut fields = null_left_template.clone();
                fields.extend(right_rows[j].fields.iter().cloned());
                out.push(self::plan::PlanRow {
                    fields,
                    group: None,
                    agg_values: None,
                    schema: PlanRow::empty_schema(),
                });
            }
        }
    }
    Ok(Some(out))
}

fn collect_top_level_and<'a>(expr: &'a Expr, out: &mut Vec<&'a Expr>) {
    match expr {
        Expr::Binary { left, op, right } if matches!(*op, crate::parser::BinaryOp::And) => {
            collect_top_level_and(left, out);
            collect_top_level_and(right, out);
        }
        other => out.push(other),
    }
}

fn nested_loop_join(
    left_rows: Vec<self::plan::PlanRow>,
    left_sources: &[MaterializedSource],
    right: &MaterializedSource,
    join_type: crate::parser::JoinType,
    condition: Option<&Expr>,
) -> Result<Vec<self::plan::PlanRow>, QueryError> {
    use crate::parser::JoinType;
    let right_rows = source_to_plan_rows(right);
    // INNER/LEFT/RIGHT/FULL + decomposable equi-condition: switch to a hash
    // join so we don't pay an O(N×M) PlanField clone per row pair. TPC-H Q15
    // joins supplier (10k rows) to the materialised `revenue` CTE (10k rows)
    // on `s_suppkey = supplier_no`; TPC-DS q78 chains three CTEs by
    // multi-column `LEFT JOIN`s. The NLJ path below would build the full
    // cartesian product of joined PlanRows just to throw most of them away.
    // Both inputs must be non-empty: the hash path needs a sample row per side
    // to classify key sidedness, and an empty side is cheap in the NLJ anyway
    // (it still applies the correct outer NULL-extension).
    if matches!(
        join_type,
        JoinType::Inner | JoinType::Left | JoinType::Right | JoinType::Full
    ) && !left_rows.is_empty()
        && !right_rows.is_empty()
    {
        if let Some(cond) = condition {
            if let Some(rows) = try_hash_join(
                &left_rows,
                &right_rows,
                right,
                cond,
                join_type,
                &right_rows[0],
                &left_rows[0],
            )? {
                return Ok(rows);
            }
        }
    }
    let mut out: Vec<self::plan::PlanRow> = Vec::new();
    let mut right_matched = vec![false; right_rows.len()];
    let semi_or_anti = matches!(join_type, JoinType::Semi | JoinType::Anti);
    for left in &left_rows {
        let mut left_matched = false;
        for (j, right_row) in right_rows.iter().enumerate() {
            let mut fields = left.fields.clone();
            fields.extend(right_row.fields.iter().cloned());
            let joined = self::plan::PlanRow {
                fields,
                group: None,
                agg_values: None,
                schema: PlanRow::empty_schema(),
            };
            let pass = match condition {
                Some(cond) => self::eval::eval_plan_bool(cond, &joined)?,
                None => true,
            };
            if pass {
                left_matched = true;
                right_matched[j] = true;
                if !semi_or_anti {
                    out.push(joined);
                } else if matches!(join_type, JoinType::Semi) {
                    // Semi-join: emit the LEFT row exactly once, then
                    // stop probing right.
                    out.push(self::plan::PlanRow {
                        fields: left.fields.clone(),
                        group: None,
                        agg_values: None,
                        schema: PlanRow::empty_schema(),
                    });
                    break;
                }
            }
        }
        match join_type {
            // FULL emits an unmatched left row exactly like LEFT (and adds the
            // unmatched-right pass below, exactly like RIGHT).
            JoinType::Left | JoinType::Full => {
                if !left_matched {
                    let mut fields = left.fields.clone();
                    fields.extend(null_right_fields(right));
                    out.push(self::plan::PlanRow {
                        fields,
                        group: None,
                        agg_values: None,
                        schema: PlanRow::empty_schema(),
                    });
                }
            }
            JoinType::Anti => {
                if !left_matched {
                    out.push(self::plan::PlanRow {
                        fields: left.fields.clone(),
                        group: None,
                        agg_values: None,
                        schema: PlanRow::empty_schema(),
                    });
                }
            }
            JoinType::Inner | JoinType::Cross | JoinType::Right | JoinType::Semi => {}
        }
    }
    if matches!(join_type, JoinType::Right | JoinType::Full) {
        // Emit unmatched right rows with null left side. We need null-left
        // schema; reconstruct from the first left row's field shape if any.
        let null_left_template: Vec<self::plan::PlanField> =
            if let Some(first_left) = left_rows.first() {
                first_left
                    .fields
                    .iter()
                    .map(|f| self::plan::PlanField {
                        meta: crate::engine::executor::select::plan::intern_col_meta(ColMeta {
                            invisible: false,
                            table: f.table.clone(),
                            alias: f.alias.clone(),
                            column: f.column.clone(),
                            column_type: f.column_type,
                            source: f.source.clone(),
                        }),
                        value: Value::Null,
                    })
                    .collect()
            } else {
                null_plan_row_for_sources(left_sources).fields
            };
        for (j, matched) in right_matched.iter().enumerate() {
            if !*matched {
                let mut fields = null_left_template.clone();
                fields.extend(right_rows[j].fields.iter().cloned());
                out.push(self::plan::PlanRow {
                    fields,
                    group: None,
                    agg_values: None,
                    schema: PlanRow::empty_schema(),
                });
            }
        }
    }
    Ok(out)
}

/// Equi-join-aware greedy evaluation of an all-comma/CROSS-join SELECT whose
/// equi-predicates live in the WHERE clause (e.g. the TPC-DS q30 family).
///
/// Returns `Some((joined_rows, sources))` with `sources` in FROM order, or
/// `None` when the statement isn't eligible (any ON condition, outer/lateral
/// join, or no WHERE) so the caller falls back to the sequential NLJ path.
///
/// The greedy order picks, at each step, a not-yet-joined source connected to
/// the joined set by an equi-predicate and pushes every now-resolvable WHERE
/// conjunct involving it into the join condition — so `nested_loop_join` takes
/// its hash-join fast path and the cartesian product is never materialised.
/// Predicates containing a subquery (e.g. q30's correlated `> (SELECT avg…)`)
/// are never pushed; the unchanged WHERE pass at the call site still applies
/// the full `stmt.selection`, so the pushed copies only ever pre-filter and
/// the final result set is identical to the cartesian-then-filter evaluation.
fn try_greedy_equi_join(
    db: &Arc<Database>,
    txn: TxnView,
    stmt: &Select,
    registry: &SessionRegistry,
) -> Result<Option<(Vec<self::plan::PlanRow>, Vec<MaterializedSource>)>, QueryError> {
    use crate::parser::JoinType;
    if stmt.straight_join || stmt.joins.iter().any(|join| join.straight) {
        return Ok(None);
    }
    let Some(where_expr) = stmt.selection.as_ref() else {
        return Ok(None);
    };
    if stmt.joins.is_empty() {
        return Ok(None);
    }
    let n = stmt.joins.len() + 1;
    if n > 64 {
        // Source mask is a u64; bail to the sequential path past 64 sources.
        return Ok(None);
    }
    for j in &stmt.joins {
        if j.lateral || j.condition.is_some() || !matches!(j.join_type, JoinType::Cross) {
            return Ok(None);
        }
    }

    // Materialise all sources in FROM order.
    let primary = stmt.from.first().ok_or_else(|| {
        error(
            crate::error::catalog::ER_NOT_SUPPORTED_YET,
            "SELECT with no FROM",
        )
    })?;
    let mut sources: Vec<MaterializedSource> = Vec::with_capacity(n);
    sources.push(materialize_join_source_with_derived(
        db,
        txn,
        primary,
        stmt.derived.as_ref(),
        registry,
    )?);
    for j in &stmt.joins {
        sources.push(materialize_join_source_with_derived(
            db,
            txn,
            &j.table,
            j.derived.as_ref(),
            registry,
        )?);
    }

    // Push-down candidates: top-level WHERE conjuncts without a subquery, each
    // tagged with the set of sources it references.
    let mut conjuncts: Vec<&Expr> = Vec::new();
    collect_top_level_and(where_expr, &mut conjuncts);
    let mut pushable: Vec<(&Expr, u64)> = Vec::new();
    for e in &conjuncts {
        if !expr_has_subquery(e) {
            pushable.push((*e, expr_source_mask(e, &sources)));
        }
    }

    // Pre-filter each source by its single-source (pure filter) predicates so
    // the hash-join build sides stay small — e.g. q11 pins each `year_total`
    // copy to one (sale_type, dyear) before its 4-way self-join on customer_id,
    // turning a many-to-many blow-up into a near 1:1 join. Single-source
    // filters commute with inner/cross joins, and the unchanged final WHERE
    // re-applies the full `stmt.selection`, so this only shrinks intermediates.
    // A row whose predicate errors is kept (the final WHERE stays the
    // authority for both rows and error propagation).
    for (e, m) in &pushable {
        if m.count_ones() != 1 {
            continue;
        }
        let i = m.trailing_zeros() as usize;
        if sources[i].rows.is_empty() {
            continue;
        }
        // P0-A2: when the source is a `SharedCte` or already-filtered
        // `SharedCteSelected`, keep the underlying `Arc<CteResult>` and
        // record an `Arc<Vec<u32>>` of surviving row indices instead of
        // deep-copying the kept rows per reference. N references with
        // distinct filters end up holding N tiny index vectors atop the
        // same store.
        if let Some(result) = sources[i].rows.cte_result().cloned() {
            let bare_table = sources[i]
                .table_name
                .rsplit_once('.')
                .map(|(_, t)| t.to_string())
                .unwrap_or_else(|| sources[i].table_name.clone());
            let metas = intern_source_metas(&sources[i], &bare_table);
            let sample = sources[i]
                .rows
                .iter_rows()
                .next()
                .map(|row| build_right_plan_row_from_store(&metas, row));
            let compiled = sample
                .as_ref()
                .map(|row| self::eval::CompiledPlanPredicate::compile_once(e, row));
            // `plan_rows[j]` corresponds to the j-th *currently-visible* row
            // of this source (i.e. already respects any existing selection).
            // We therefore need to map j back to the underlying store row id.
            let prev_indices = sources[i].rows.cte_selection().cloned();
            let mut kept_idx: Vec<u32> = Vec::with_capacity(sources[i].rows.len());
            for (j, raw) in sources[i].rows.iter_rows().enumerate() {
                let pr = build_right_plan_row_from_store(&metas, raw);
                let keep = match &compiled {
                    Some(compiled) => compiled.eval_bool(&pr),
                    None => self::eval::eval_plan_bool(e, &pr),
                };
                if matches!(keep, Ok(false)) {
                    continue;
                }
                let store_idx = match &prev_indices {
                    Some(prev) => prev[j],
                    None => j as u32,
                };
                kept_idx.push(store_idx);
            }
            sources[i].rows = SourceRows::SharedCteSelected {
                result,
                indices: Arc::new(kept_idx),
            };
            continue;
        }

        // Non-CTE source (`Owned`) — the deep-copy path is unavoidable
        // since there is nothing shared to index into.
        let plan_rows = source_to_plan_rows(&sources[i]);
        let compiled = plan_rows
            .first()
            .map(|row| self::eval::CompiledPlanPredicate::compile_once(e, row));
        let mut kept: Vec<Vec<Value>> = Vec::with_capacity(sources[i].rows.len());
        for (raw, pr) in sources[i].rows.iter_rows().zip(plan_rows.iter()) {
            let keep = match &compiled {
                Some(compiled) => compiled.eval_bool(pr),
                None => self::eval::eval_plan_bool(e, pr),
            };
            if !matches!(keep, Ok(false)) {
                kept.push(raw.clone());
            }
        }
        sources[i].rows = SourceRows::Owned(kept);
    }

    let full_mask: u64 = if n == 64 { u64::MAX } else { (1u64 << n) - 1 };
    let mut joined_mask: u64 = 1; // primary = index 0
    let mut order: Vec<usize> = vec![0];
    let mut current_rows = source_to_plan_rows(&sources[0]);

    while joined_mask != full_mask {
        // Prefer a source connected to the joined set by an equi-predicate (so
        // the hash-join fast path fires); else any source with a resolvable
        // predicate involving it; else (disconnected graph) the lowest-indexed
        // remaining source as a genuine cross product.
        let mut best: Option<(usize, bool)> = None;
        for k in 0..n {
            let kb = 1u64 << k;
            if joined_mask & kb != 0 {
                continue;
            }
            let target = joined_mask | kb;
            let mut involves_resolvable = false;
            let mut has_equi = false;
            for (e, m) in &pushable {
                // Skip conjuncts that don't involve k or still reference a
                // not-yet-joined source.
                if *m & kb == 0 || *m & !target != 0 {
                    continue;
                }
                involves_resolvable = true;
                if is_equi_connector(e, joined_mask, k, &sources) {
                    has_equi = true;
                }
            }
            if !involves_resolvable {
                continue;
            }
            best = match best {
                None => Some((k, has_equi)),
                Some((_, be)) if has_equi && !be => Some((k, has_equi)),
                Some(prev) => Some(prev),
            };
        }
        let chosen = match best {
            Some((k, _)) => k,
            None => (0..n).find(|k| joined_mask & (1u64 << k) == 0).unwrap(),
        };
        let cb = 1u64 << chosen;
        let target = joined_mask | cb;
        let mut cond_parts: Vec<&Expr> = Vec::new();
        for (e, m) in &pushable {
            if *m & cb != 0 && *m & !target == 0 {
                cond_parts.push(*e);
            }
        }
        let cond = and_combine(&cond_parts);
        current_rows = nested_loop_join(
            current_rows,
            &[], // This path only performs INNER/CROSS joins.
            &sources[chosen],
            if cond.is_some() {
                JoinType::Inner
            } else {
                JoinType::Cross
            },
            cond.as_ref(),
        )?;
        joined_mask = target;
        order.push(chosen);
    }

    // Restore FROM-order field layout after the connectivity-aware join order.
    // Wildcards can already have been expanded to explicit projection items by
    // the time this executor sees the statement, so inspecting `projection`
    // here is not a reliable way to decide whether the physical field order is
    // externally observable.  Keeping every row in the same order as the
    // returned `sources` also makes the row and batch boundaries share one
    // positional schema; named expression lookup remains unchanged because the
    // PlanField metadata moves together with each value.
    if !order.iter().copied().eq(0..n) {
        let mut block: Vec<(usize, usize)> = vec![(0, 0); n];
        let mut off = 0usize;
        for &si in &order {
            let len = sources[si].columns.len();
            block[si] = (off, len);
            off += len;
        }
        for row in &mut current_rows {
            let mut nf: Vec<self::plan::PlanField> = Vec::with_capacity(row.fields.len());
            for (start, len) in block.iter().copied() {
                nf.extend(row.fields[start..start + len].iter().cloned());
            }
            row.fields = nf;
        }
    }

    Ok(Some((current_rows, sources)))
}

/// Bitmask of indices into `sources` that `expr` references by column.
/// Unqualified identifiers match by column name across every source (an
/// ambiguous name sets every candidate bit — conservative, so the dependency
/// is never under-approximated). `q.col` matches the source whose alias or
/// bare table name equals `q`. Subquery bodies are not traversed; callers drop
/// subquery-bearing predicates before calling this.
fn expr_source_mask(expr: &Expr, sources: &[MaterializedSource]) -> u64 {
    let mut mask = 0u64;
    expr_source_mask_walk(expr, sources, &mut mask);
    mask
}

fn mark_sources_by_column(col: &str, sources: &[MaterializedSource], mask: &mut u64) {
    for (i, s) in sources.iter().enumerate() {
        if s.columns.iter().any(|c| c.name.eq_ignore_ascii_case(col)) {
            *mask |= 1u64 << i;
        }
    }
}

fn expr_source_mask_walk(expr: &Expr, sources: &[MaterializedSource], mask: &mut u64) {
    match expr {
        Expr::Identifier(name) => mark_sources_by_column(name, sources, mask),
        Expr::CompoundIdentifier(parts) => {
            let col = parts.last().map(|s| s.as_str()).unwrap_or("");
            if parts.len() >= 2 {
                let q = parts[parts.len() - 2].as_str();
                let mut matched = false;
                for (i, s) in sources.iter().enumerate() {
                    let bare = s
                        .table_name
                        .rsplit_once('.')
                        .map(|(_, t)| t)
                        .unwrap_or(s.table_name.as_str());
                    if s.alias.eq_ignore_ascii_case(q) || bare.eq_ignore_ascii_case(q) {
                        *mask |= 1u64 << i;
                        matched = true;
                    }
                }
                if !matched {
                    mark_sources_by_column(col, sources, mask);
                }
            } else {
                mark_sources_by_column(col, sources, mask);
            }
        }
        Expr::Unary { expr, .. }
        | Expr::IsNull(expr)
        | Expr::IsNotNull(expr)
        | Expr::GroupKeyRef { expr, .. } => expr_source_mask_walk(expr, sources, mask),
        Expr::Binary { left, right, .. } => {
            expr_source_mask_walk(left, sources, mask);
            expr_source_mask_walk(right, sources, mask);
        }
        Expr::Between {
            expr, low, high, ..
        } => {
            expr_source_mask_walk(expr, sources, mask);
            expr_source_mask_walk(low, sources, mask);
            expr_source_mask_walk(high, sources, mask);
        }
        Expr::InList { expr, list, .. } => {
            expr_source_mask_walk(expr, sources, mask);
            for e in list {
                expr_source_mask_walk(e, sources, mask);
            }
        }
        Expr::Like { expr, pattern, .. } => {
            expr_source_mask_walk(expr, sources, mask);
            expr_source_mask_walk(pattern, sources, mask);
        }
        Expr::Case {
            operand,
            conditions,
            results,
            else_result,
        } => {
            if let Some(o) = operand {
                expr_source_mask_walk(o, sources, mask);
            }
            for c in conditions {
                expr_source_mask_walk(c, sources, mask);
            }
            for r in results {
                expr_source_mask_walk(r, sources, mask);
            }
            if let Some(e) = else_result {
                expr_source_mask_walk(e, sources, mask);
            }
        }
        Expr::Function { args, .. } | Expr::Tuple(args) => {
            for a in args {
                expr_source_mask_walk(a, sources, mask);
            }
        }
        Expr::WindowFunction { args, spec, .. } => {
            let crate::parser::WindowSpec {
                partition_by,
                order_by,
                ..
            } = spec.as_ref();

            for a in args {
                expr_source_mask_walk(a, sources, mask);
            }
            for p in partition_by {
                expr_source_mask_walk(p, sources, mask);
            }
            for o in order_by {
                expr_source_mask_walk(&o.expr, sources, mask);
            }
        }
        // Literals, params, user variables, and subquery-bearing nodes
        // contribute no source dependency here (subquery predicates are
        // excluded from push-down by the caller).
        _ => {}
    }
}

/// True when `e` is an `=` predicate that cleanly connects the already-joined
/// set to the candidate source `k`: one operand resolves entirely within the
/// joined set (non-empty) and the other resolves entirely within `{k}`. This
/// is exactly the partition `try_hash_join_inner` needs to build a hash key,
/// guaranteeing the join avoids a cartesian product.
fn is_equi_connector(e: &Expr, joined_mask: u64, k: usize, sources: &[MaterializedSource]) -> bool {
    use crate::parser::BinaryOp;
    if let Expr::Binary { left, op, right } = e {
        if matches!(op, BinaryOp::Eq) {
            let lm = expr_source_mask(left, sources);
            let rm = expr_source_mask(right, sources);
            let kb = 1u64 << k;
            let left_in_joined = lm != 0 && lm & !joined_mask == 0;
            let right_in_joined = rm != 0 && rm & !joined_mask == 0;
            return (left_in_joined && rm == kb) || (right_in_joined && lm == kb);
        }
    }
    false
}

/// Fold a slice of predicates into a single `AND` chain, cloning each so the
/// result is owned (and can be borrowed as `nested_loop_join`'s condition).
fn and_combine(parts: &[&Expr]) -> Option<Expr> {
    use crate::parser::BinaryOp;
    let mut iter = parts.iter();
    let first = *iter.next()?;
    let mut acc = first.clone();
    for p in iter {
        acc = Expr::Binary {
            left: Box::new(acc),
            op: BinaryOp::And,
            right: Box::new((*p).clone()),
        };
    }
    Some(acc)
}

fn infer_expr_type(expr: &Expr, sources: &[MaterializedSource]) -> ColumnType {
    let descriptor = infer_expr_descriptor(expr, sources);
    if !matches!(
        descriptor.kind,
        crate::engine::expr_type::ExprTypeKind::Unknown
    ) {
        return descriptor.column_type();
    }
    match expr {
        Expr::Identifier(name) => {
            for src in sources {
                if let Some(col) = src
                    .columns
                    .iter()
                    .find(|c| c.name.eq_ignore_ascii_case(name))
                {
                    return col.column_type;
                }
            }
            ColumnType::VarString
        }
        Expr::CompoundIdentifier(parts) => {
            if parts.len() == 2 {
                if let Some(src) = sources
                    .iter()
                    .find(|s| s.alias.eq_ignore_ascii_case(&parts[0]))
                {
                    if let Some(col) = src
                        .columns
                        .iter()
                        .find(|c| c.name.eq_ignore_ascii_case(&parts[1]))
                    {
                        return col.column_type;
                    }
                }
            }
            ColumnType::VarString
        }
        // Conditional functions type from their branches; resolve column-ref
        // branches against these sources so `COALESCE(int_col, ..)` → BIGINT.
        Expr::Function { name, args, .. } => {
            crate::engine::expr::conditional_function_result_type(name, args, |e| {
                infer_expr_type(e, sources)
            })
            .unwrap_or_else(|| crate::engine::expr::expr_result_type(expr))
        }
        Expr::Binary { left, op, right } => crate::engine::expr::binary_expression_result_type(
            *op,
            infer_expr_type(left, sources),
            infer_expr_type(right, sources),
        ),
        // Non-column expressions (literals, comparisons, NOT, IN/BETWEEN/LIKE,
        // ...) share the canonical classifier so this path advertises the same
        // wire type as the const/single-table paths (e.g. `a=b`, `NOT c` →
        // BIGINT, not VAR_STRING).
        other => crate::engine::expr::expr_result_type(other),
    }
}

fn infer_expr_descriptor(
    expr: &Expr,
    sources: &[MaterializedSource],
) -> crate::engine::expr_type::ExprType {
    let resolve_column = |column_expr: &Expr| {
        let column = source_column_for_expr(column_expr, sources)?;
        Some(crate::engine::expr_type::ExprType::from_column_type(
            column.column_type,
            &column.source,
        ))
    };
    let resolve_placeholder = |_: usize| None;
    let context =
        crate::engine::expr_type::TypeContext::context_free(&resolve_column, &resolve_placeholder);
    crate::engine::expr_type::infer_expr_type(expr, &context)
}

fn source_column_for_expr<'a>(
    expr: &Expr,
    sources: &'a [MaterializedSource],
) -> Option<&'a Column> {
    match expr {
        Expr::Identifier(name) => {
            let mut matches = sources.iter().flat_map(|source| {
                source
                    .columns
                    .iter()
                    .filter(move |column| column.name.eq_ignore_ascii_case(name))
            });
            let first = matches.next()?;
            matches.next().is_none().then_some(first)
        }
        Expr::CompoundIdentifier(parts) if parts.len() >= 2 => {
            let qualifier = &parts[parts.len() - 2];
            let column_name = parts.last()?;
            sources
                .iter()
                .find(|source| source.alias.eq_ignore_ascii_case(qualifier))
                .and_then(|source| {
                    source
                        .columns
                        .iter()
                        .find(|column| column.name.eq_ignore_ascii_case(column_name))
                })
        }
        _ => None,
    }
}

fn infer_expr_unsigned(expr: &Expr, sources: &[MaterializedSource]) -> bool {
    crate::engine::expr::expr_result_unsigned(expr, &|column_expr| match column_expr {
        Expr::Identifier(name) => sources.iter().find_map(|source| {
            source
                .columns
                .iter()
                .find(|column| column.name.eq_ignore_ascii_case(name))
                .map(|column| column.source.flags & crate::mysql::protocol::UNSIGNED_FLAG != 0)
        }),
        Expr::CompoundIdentifier(parts) if parts.len() == 2 => sources
            .iter()
            .find(|source| source.alias.eq_ignore_ascii_case(&parts[0]))
            .and_then(|source| {
                source
                    .columns
                    .iter()
                    .find(|column| column.name.eq_ignore_ascii_case(&parts[1]))
            })
            .map(|column| column.source.flags & crate::mysql::protocol::UNSIGNED_FLAG != 0),
        _ => None,
    })
}

pub(super) fn execute_analyze_table(
    db: &Arc<Database>,
    session: &SessionContext,
    table_name: &str,
    histogram: Option<&crate::parser::HistogramAction>,
) -> Result<QueryResponse, QueryError> {
    use crate::parser::HistogramAction;
    let visible_name = super::qualification::qualify_persistent_table_name(session, table_name);
    let temporary = session.has_temp_table(table_name);
    if temporary && histogram.is_some() {
        return Err(error(
            crate::error::catalog::ER_NOT_SUPPORTED_YET,
            "Histogram operations on temporary tables are not supported",
        ));
    }
    let table_name = qualify_table_name(session, table_name);
    let columns = vec![
        col("Table", ColumnType::VarString),
        col("Op", ColumnType::VarString),
        col("Msg_type", ColumnType::VarString),
        col("Msg_text", ColumnType::VarString),
    ];
    let analyze_row = |op: &str, msg_type: &str, msg: String| {
        vec![
            Value::String(visible_name.clone()),
            Value::String(op.to_string()),
            Value::String(msg_type.to_string()),
            Value::String(msg),
        ]
    };

    match histogram {
        None => {
            db.analyze_table(&table_name)?;
            Ok(QueryResponse::ResultSet(ResultSet {
                columns,
                rows: vec![analyze_row("analyze", "status", "OK".to_string())],
            }))
        }
        Some(HistogramAction::Update {
            columns: cols,
            buckets,
        }) => {
            let updated = db.update_table_histograms(&table_name, cols, buckets.unwrap_or(100))?;
            let rows = updated
                .into_iter()
                .map(|c| {
                    analyze_row(
                        "histogram",
                        "status",
                        format!("Histogram statistics created for column '{c}'."),
                    )
                })
                .collect();
            Ok(QueryResponse::ResultSet(ResultSet { columns, rows }))
        }
        Some(HistogramAction::Drop { columns: cols }) => {
            let dropped = db.drop_table_histograms(&table_name, cols)?;
            let rows = dropped
                .into_iter()
                .map(|c| {
                    analyze_row(
                        "histogram",
                        "status",
                        format!("Histogram statistics removed for column '{c}'."),
                    )
                })
                .collect();
            Ok(QueryResponse::ResultSet(ResultSet { columns, rows }))
        }
    }
}

/// Simplify only in predicate context: `TRUE AND x` has x's truth value,
/// but replacing it with x in a projection would change its numeric value.
fn simplify_folded_predicate(expr: Expr) -> Expr {
    use crate::parser::BinaryOp;
    fn boolean(expr: &Expr) -> Option<bool> {
        match expr {
            Expr::BoundValue(
                value @ (Value::Int(_)
                | Value::UInt(_)
                | Value::Float(_)
                | Value::Double(_)
                | Value::Decimal(_)
                | Value::DecimalCompact(..)
                | Value::Decimal128(..)
                | Value::DecimalWide(_)),
            ) => Some(crate::engine::expr::truthy(value)),
            Expr::Literal(crate::parser::Literal::Bool(value)) => Some(*value),
            Expr::Literal(crate::parser::Literal::Int(value)) => Some(*value != 0),
            Expr::Literal(crate::parser::Literal::UInt(value)) => Some(*value != 0),
            Expr::Literal(crate::parser::Literal::Float(value)) => Some(*value != 0.0),
            _ => None,
        }
    }
    match expr {
        Expr::Binary {
            left,
            op: op @ (BinaryOp::And | BinaryOp::Or),
            right,
        } => {
            let left = simplify_folded_predicate(*left);
            let right = simplify_folded_predicate(*right);
            let neutral = op == BinaryOp::And;
            if boolean(&left) == Some(neutral) {
                return right;
            }
            if boolean(&right) == Some(neutral) {
                return left;
            }
            Expr::Binary {
                left: Box::new(left),
                op,
                right: Box::new(right),
            }
        }
        expr if boolean(&expr) == Some(true) => Expr::Literal(crate::parser::Literal::Bool(true)),
        expr => expr,
    }
}

pub(super) fn fold_select_constants(stmt: &mut Select) {
    use crate::engine::expr::{fold_constant_subexprs, fold_predicate_temporal_constants};
    if let Some(sel) = &mut stmt.selection {
        fold_predicate_temporal_constants(sel);
        fold_constant_subexprs(sel);
    }
    if let Some(having) = &mut stmt.having {
        fold_predicate_temporal_constants(having);
        fold_constant_subexprs(having);
    }
    for join in &mut stmt.joins {
        if let Some(cond) = &mut join.condition {
            fold_predicate_temporal_constants(cond);
            fold_constant_subexprs(cond);
        }
    }
    for predicate in std::iter::once(&mut stmt.selection)
        .chain(std::iter::once(&mut stmt.having))
        .chain(stmt.joins.iter_mut().map(|join| &mut join.condition))
    {
        if let Some(expr) = predicate.take() {
            let expr = simplify_folded_predicate(expr);
            if !matches!(expr, Expr::Literal(crate::parser::Literal::Bool(true))) {
                *predicate = Some(expr);
            }
        }
    }
    for item in &mut stmt.projection {
        if let SelectItem::Expr { expr, .. } = item {
            fold_constant_subexprs(expr);
        }
    }
    if let Some(values) = &mut stmt.values {
        for row in &mut values.rows {
            for expr in row {
                fold_constant_subexprs(expr);
            }
        }
    }
}

pub(super) fn validate_select_static_collations(stmt: &Select) -> Result<(), QueryError> {
    validate_select_collations_with(stmt, |expr| {
        crate::engine::expr::validate_static_expr_numeric_operands(expr)?;
        crate::engine::expr::validate_static_expr_collations(expr)
    })
}

/// Resolve the temporal constant diagnostics that belong to SQL PREPARE.
/// The ordinary execution binder must not publish these repeatedly on every
/// EXECUTE, so the named-statement path calls this once against a qualified
/// validation copy.
pub(super) fn warn_prepared_select_temporal_constants(db: &Database, stmt: &Select) {
    if !stmt.joins.is_empty() || stmt.derived.is_some() {
        return;
    }
    let Some(base) = stmt
        .from
        .first()
        .filter(|base| base.generate_series.is_none())
    else {
        return;
    };
    let Ok(table) = db.table(&base.name) else {
        return;
    };
    let warn = |expr: &Expr| {
        crate::engine::expr::warn_prepared_temporal_in_list_constants(expr, &table);
        Ok(())
    };
    let _: Result<(), QueryError> = validate_select_collations_with(stmt, warn);
}

fn validate_select_collations_with(
    stmt: &Select,
    validate: impl Fn(&Expr) -> Result<(), QueryError>,
) -> Result<(), QueryError> {
    if let Some(selection) = &stmt.selection {
        validate(selection)?;
    }
    if let Some(having) = &stmt.having {
        validate(having)?;
    }
    for join in &stmt.joins {
        if let Some(condition) = &join.condition {
            validate(condition)?;
        }
    }
    for item in &stmt.projection {
        if let SelectItem::Expr { expr, .. } = item {
            validate(expr)?;
        }
    }
    for expr in &stmt.group_by {
        validate(expr)?;
    }
    for order in &stmt.order_by {
        validate(&order.expr)?;
    }
    if let Some(values) = &stmt.values {
        for row in &values.rows {
            for expr in row {
                validate(expr)?;
            }
        }
    }
    if let Some(json_table) = &stmt.json_table {
        validate(&json_table.expr)?;
    }
    for window in &stmt.named_windows {
        for expr in &window.partition_by {
            validate(expr)?;
        }
        for order in &window.order_by {
            validate(&order.expr)?;
        }
    }
    Ok(())
}

fn validate_bit_aggregate_operand_widths(
    expr: &Expr,
    table: &TableSchema,
) -> Result<(), QueryError> {
    fn declared_binary_width(expr: &Expr, table: &TableSchema) -> Option<usize> {
        let column_name = match expr {
            Expr::Identifier(name) => Some(name.as_str()),
            Expr::CompoundIdentifier(parts) => parts.last().map(String::as_str),
            _ => None,
        };
        if let Some(column_name) = column_name {
            let column = table
                .columns
                .iter()
                .find(|column| column.name.eq_ignore_ascii_case(column_name))?;
            return match column.data_type {
                crate::storage::schema::DataTypeKind::Binary { len }
                | crate::storage::schema::DataTypeKind::VarBinary { len } => Some(len.unwrap_or(1)),
                crate::storage::schema::DataTypeKind::Blob { max_len } => Some(max_len as usize),
                _ => None,
            };
        }
        if let Some(width) = crate::engine::expr::expression_metadata_length(expr) {
            return usize::try_from(width).ok();
        }
        let Expr::Function { name, args, .. } = expr else {
            return None;
        };
        if matches!(
            name.to_ascii_uppercase().as_str(),
            "SUBSTRING" | "SUBSTR" | "MID"
        ) {
            let source_width = args
                .first()
                .and_then(|source| declared_binary_width(source, table))?;
            let requested = match args.get(2) {
                Some(Expr::Literal(Literal::Int(length))) => usize::try_from(*length).ok(),
                Some(Expr::Literal(Literal::UInt(length))) => usize::try_from(*length).ok(),
                _ => None,
            }?;
            return Some(source_width.min(requested));
        }
        None
    }

    let slots = self::aggregate::collect_aggregate_slots(&[expr]);
    for slot in slots {
        if !matches!(
            slot.name.to_ascii_uppercase().as_str(),
            "BIT_AND" | "BIT_OR" | "BIT_XOR"
        ) {
            continue;
        }
        let Some(declared_max) = slot
            .args
            .first()
            .and_then(|arg| declared_binary_width(arg, table))
        else {
            continue;
        };
        if declared_max > 511 {
            return Err(error(
                crate::error::catalog::ER_INVALID_BITWISE_AGGREGATE_OPERANDS_SIZE,
                "Binary operands of bitwise aggregate functions must not exceed 511 bytes",
            ));
        }
        self::aggregate::register_bit_aggregate_width_hint(slot.key, declared_max);
    }
    Ok(())
}

pub(super) fn prepare_select_for_planning(
    db: &Arc<Database>,
    stmt: &mut Select,
) -> Result<(), QueryError> {
    crate::engine::params::validate_limit(stmt.limit.as_ref())?;
    crate::engine::params::validate_limit(stmt.offset.as_ref())?;
    let _preparing = crate::engine::cancel::enter_process_stage(
        crate::engine::session_registry::ProcessState::Preparing,
    );
    // Capture MySQL's display name before any normalisation can simplify an
    // expression. In particular sqlparser/planner lowering turns TRIM forms
    // into their literal operands; waiting until after those passes made
    // several distinct CTAS columns all inherit the name `' '`.
    for item in &mut stmt.projection {
        if let SelectItem::Expr { expr, alias } = item {
            if alias.is_none()
                && !matches!(
                    expr,
                    Expr::Identifier(_)
                        | Expr::CompoundIdentifier(_)
                        // GROUP BY binding can wrap an originally bare
                        // identifier before a prepared CTE/derived query is
                        // prepared again for materialisation.  It must keep
                        // the identifier's normal output name; rendering the
                        // internal wrapper as SQL produces a synthetic alias
                        // such as "`s_store_id`" and leaks the quoting syntax
                        // into the materialised schema.
                        | Expr::GroupKeyRef { .. }
                        | Expr::Literal(_)
                        | Expr::BoundValue(_)
                )
            {
                *alias = Some(crate::parser::expr_to_sql(expr));
            }
        }
    }
    // Resolve the process/session-visible NULL placement while binding on the
    // connection thread. Streaming workers must consume an explicit physical
    // property instead of consulting thread-local configuration again.
    for order in &mut stmt.order_by {
        if order.nulls_first.is_none() {
            order.nulls_first = Some(order.nulls_first_effective());
        }
    }
    planner::resolve_wildcard_order_ordinals(db, stmt)?;
    planner::resolve_join_using_natural(db, stmt)?;
    // GROUP BY alias references must be substituted on the executed
    // statement (not just `plan_select`'s clone) and before the column
    // validation below — otherwise a legal `GROUP BY <alias>` is
    // rejected as an unknown column, and the row-pipeline paths that
    // evaluate `stmt.group_by` directly would never see the resolution.
    planner::resolve_group_by_refs(db, stmt);
    // Positional references may resolve to a constant select item (for
    // example ClickBench's `SELECT 1, url ... GROUP BY 1, url`). Prune that
    // redundant lane before GROUP BY references are bound to physical slot
    // indexes; doing it later would require remapping GroupKeyRef nodes.
    planner::prune_redundant_constant_group_keys(stmt);
    planner::prune_redundant_functional_group_keys(db, stmt);
    // ORDER BY referencing a SELECT output column by name (e.g. `ORDER BY
    // d_week_seq` over `SELECT d1.d_week_seq ...`) must bind to that output
    // column before validation and before the row pipeline reads `order_by`,
    // so a name that is ambiguous across self-joined sources resolves to the
    // single projected column instead of raising 1052.
    planner::resolve_order_by_output_refs(db, stmt);
    planner::resolve_having_aliases(stmt);
    if stmt.group_by.is_empty()
        && stmt.grouping_sets.is_empty()
        && !select_projection_has_toplevel_aggregate(&stmt.projection)
        && stmt
            .order_by
            .iter()
            .any(|order| expr_has_toplevel_aggregate(&order.expr))
    {
        return Err(error(
            crate::error::catalog::ER_AGGREGATE_ORDER_NON_AGG_QUERY,
            "Expression in ORDER BY contains aggregate function and applies to a non-aggregated query",
        ));
    }
    if stmt.group_by.is_empty()
        && stmt.grouping_sets.is_empty()
        && select_projection_has_toplevel_aggregate(&stmt.projection)
    {
        // The query produces exactly one aggregate row. ORDER BY cannot alter
        // that row's position, and retaining a base-column sort key until
        // after aggregation incorrectly tries to resolve a column that is no
        // longer present (`SELECT BIT_AND(c.pk) ... ORDER BY c.pk`).
        stmt.order_by.clear();
    }
    planner::bind_rollup_select_for_execution(db, stmt)?;
    // Enforce grouping legality before generic column validation. MySQL gives
    // ER_WRONG_FIELD_WITH_GROUP priority when a correlated/non-grouped
    // reference is both illegal under ONLY_FULL_GROUP_BY and not resolvable in
    // the local query block; validating names first incorrectly surfaced 1054.
    if let Some(expr) = stmt
        .group_by
        .iter()
        .find(|expr| expr_has_toplevel_aggregate(expr))
    {
        return Err(crate::error::catalog::ER_WRONG_GROUP_FIELD.error(format!(
            "Can't group on '{}'",
            crate::parser::expr_to_sql(expr)
        )));
    }
    if crate::engine::expr::session_only_full_group_by() {
        planner::check_only_full_group_by(db, stmt)?;
    }
    if !select_references_system_table(stmt)
        && stmt.json_table.is_none()
        && (stmt.table.is_some() || stmt.derived.is_some())
    {
        planner::validate_select_column_references(db, stmt)?;
    }
    warn_group_by_resolution_diagnostics(db, stmt);
    // A single base table can be validated completely here, including
    // column-derived collations. Multi-source queries keep using the
    // qualified planner/row schemas until their binding scope is available.
    validate_single_base_select_exprs(db, stmt)?;
    // ENUM/SET numeric semantics (ordinal ORDER BY / numeric-literal
    // comparisons) rewrite on the executed statement — `plan_select`
    // clones its input, so a planner-only pass would never reach the
    // row pipeline. Idempotent: rewritten nodes no longer match.
    crate::planner::rewrite_enum_set_numeric_semantics(db, stmt);
    crate::planner::normalize_select_predicates(db, stmt);
    if crate::planner::decorrelate_enabled_external() {
        crate::planner_decorrelate::scalar::decorrelate_scalar_subqueries(db, stmt);
        crate::planner_decorrelate::derived::pushdown_derived_predicates_with_db(db, stmt);
        crate::planner_decorrelate::anti::rewrite_not_in_to_anti(db, stmt);
    }
    // Validate combinations whose collation metadata is carried by the AST
    // before constant folding can replace value-equivalent subtrees with
    // untyped BoundValue nodes.
    validate_select_static_collations(stmt)?;
    // WHERE aggregate legality is query-block scoped, so it cannot be decided
    // in the syntax converter. Perform it here after binding and deterministic
    // argument/type validation: this preserves MySQL's error precedence while
    // still rejecting a genuine local aggregate instead of falling through to
    // stored-function lookup (1305).
    if stmt
        .selection
        .as_ref()
        .is_some_and(expr_has_toplevel_aggregate)
    {
        return Err(error(
            crate::error::catalog::ER_INVALID_GROUP_FUNC_USE,
            "Invalid use of group function",
        ));
    }
    fold_select_constants(stmt);
    if let Some(mut flattened) = try_flatten_locking_derived_select(db, stmt)
        .or_else(|| try_flatten_simple_derived_select(stmt))
    {
        crate::planner::normalize_select_predicates(db, &mut flattened);
        // Flattening replaces derived-column identifiers with their defining
        // expressions. Run the same typed validation used for an ordinary
        // base-table SELECT before folding, otherwise an empty derived/CTE
        // input can hide deterministic conversion errors indefinitely.
        validate_single_base_select_exprs(db, &flattened)?;
        validate_select_static_collations(&flattened)?;
        fold_select_constants(&mut flattened);
        *stmt = flattened;
    }
    Ok(())
}

fn warn_group_by_resolution_diagnostics(db: &Database, stmt: &Select) {
    if stmt.group_by.is_empty() || !stmt.joins.is_empty() || stmt.derived.is_some() {
        return;
    }
    let Some(base) = stmt
        .from
        .first()
        .filter(|base| base.generate_series.is_none())
    else {
        return;
    };
    let Ok(table) = db.table(&base.name) else {
        return;
    };
    for group in &stmt.group_by {
        let Expr::Function { name, args, .. } = group else {
            continue;
        };
        if !name.eq_ignore_ascii_case("INET_NTOA") {
            continue;
        }
        let Some(column_name) = args.first().and_then(|arg| match arg {
            Expr::Identifier(name) => Some(name.as_str()),
            Expr::CompoundIdentifier(parts) => parts.last().map(String::as_str),
            _ => None,
        }) else {
            continue;
        };
        let Some(column) = table
            .column_index(column_name)
            .ok()
            .and_then(|index| table.columns.get(index))
        else {
            continue;
        };
        if column.unsigned
            || !matches!(
                column.data_type,
                DataTypeKind::TinyInt
                    | DataTypeKind::SmallInt
                    | DataTypeKind::MediumInt
                    | DataTypeKind::Int
                    | DataTypeKind::BigInt
            )
        {
            continue;
        }
        // MySQL resolves the signed-column domain once while constructing
        // the grouping item, in addition to per-row conversion diagnostics.
        crate::engine::api::push_statement_warning(
            crate::error::ConditionLevel::Warning,
            crate::error::catalog::ER_WRONG_VALUE_FOR_TYPE,
            format!("Incorrect integer value: '{column_name}' for function INET_NTOA"),
        );
    }
}

pub(super) fn validate_cached_temporal_literals(
    db: &Database,
    stmt: &Select,
) -> Result<(), QueryError> {
    if !stmt.joins.is_empty() || stmt.derived.is_some() {
        return Ok(());
    }
    let Some(base) = stmt
        .from
        .first()
        .filter(|base| base.generate_series.is_none())
    else {
        return Ok(());
    };
    let Ok(table) = db.table(&base.name) else {
        return Ok(());
    };
    if !table.columns.iter().any(|column| {
        matches!(
            column.data_type,
            DataTypeKind::Date | DataTypeKind::DateTime { .. } | DataTypeKind::Timestamp { .. }
        )
    }) {
        return Ok(());
    }
    validate_select_collations_with(stmt, |expr| {
        crate::engine::expr::validate_expr_temporal_literals(expr, &table)
    })
}

fn validate_single_base_select_exprs(db: &Database, stmt: &Select) -> Result<(), QueryError> {
    self::aggregate::clear_bit_aggregate_width_hints();
    if !stmt.joins.is_empty() || stmt.derived.is_some() {
        return Ok(());
    }
    let Some(base) = stmt
        .from
        .first()
        .filter(|base| base.generate_series.is_none())
    else {
        return Ok(());
    };
    let Ok(table) = db.table(&base.name) else {
        return Ok(());
    };
    validate_select_collations_with(stmt, |expr| {
        validate_bit_aggregate_operand_widths(expr, &table)?;
        crate::engine::expr::validate_expr_numeric_operands(expr, &table)?;
        crate::engine::expr::validate_expr_collations(expr, &table)?;
        crate::engine::expr::validate_expr_temporal_literals(expr, &table)
    })
}

/// Drive an already-obtained SELECT [`QueryResponse`] to `sink` in row chunks
/// of at most `chunk_size`, whether the plan materialised (`ResultSet`) or
/// streams (`StreamingResultSet`). Shared by the INSERT…SELECT loader and the
/// streaming CTAS loader so both get the same bounded-batch behaviour without
/// re-running the SELECT. A materialised result is still chunked on the way
/// out so the downstream storage batch stays bounded.
pub(crate) fn drive_response_chunks<F>(
    response: QueryResponse,
    chunk_size: usize,
    mut sink: F,
) -> Result<(), QueryError>
where
    F: FnMut(Vec<Vec<Value>>) -> Result<(), QueryError>,
{
    match response {
        QueryResponse::ResultSet(result) => {
            let mut iter = result.rows.into_iter();
            loop {
                let mut chunk = Vec::with_capacity(chunk_size);
                while chunk.len() < chunk_size {
                    match iter.next() {
                        Some(row) => chunk.push(row),
                        None => break,
                    }
                }
                if chunk.is_empty() {
                    break;
                }
                sink(chunk)?;
            }
            Ok(())
        }
        QueryResponse::SharedResultSet(result) => {
            let mut iter = result.rows.into_iter();
            loop {
                let mut chunk = Vec::with_capacity(chunk_size);
                while chunk.len() < chunk_size {
                    match iter.next() {
                        Some(row) => chunk.push(row),
                        None => break,
                    }
                }
                if chunk.is_empty() {
                    break;
                }
                sink(chunk)?;
            }
            Ok(())
        }
        QueryResponse::StreamingResultSet(mut stream) => {
            loop {
                let mut chunk = Vec::with_capacity(chunk_size);
                while chunk.len() < chunk_size {
                    match stream.cursor.next_row() {
                        Ok(Some(row)) => chunk.push(row),
                        Ok(None) => break,
                        Err(err) => {
                            for warning in stream.cursor.take_warnings() {
                                crate::engine::api::push_sql_warning_local(warning);
                            }
                            return Err(err);
                        }
                    }
                }
                for warning in stream.cursor.take_warnings() {
                    crate::engine::api::push_sql_warning_local(warning);
                }
                if chunk.is_empty() {
                    break;
                }
                sink(chunk)?;
            }
            Ok(())
        }
        QueryResponse::Ok | QueryResponse::OkAffected { .. } => Err(error(
            crate::error::catalog::ER_NOT_SUPPORTED_YET,
            "SELECT source must return rows",
        )),
        QueryResponse::MultiResult(_) => Err(error(
            crate::error::catalog::ER_NOT_SUPPORTED_YET,
            "SELECT source must be a single SELECT",
        )),
    }
}

pub(crate) fn check_primary_definition_for_select(
    db: &Arc<Database>,
    txn: TxnView,
    stmt: &Select,
) -> Result<(), QueryError> {
    if db.primary_definition_changed_since(txn) {
        if stmt.derived.is_none() && stmt.from.is_empty() {
            if let Some(name) = stmt.table.as_deref() {
                if super::cte::lookup_cte(name).is_none() {
                    if let Ok(table) = db.table(name) {
                        db.check_primary_read_view(&table, txn)?;
                    }
                }
            }
        }
        for source in stmt
            .from
            .iter()
            .take(if stmt.derived.is_some() {
                0
            } else {
                stmt.from.len()
            })
            .chain(
                stmt.joins
                    .iter()
                    .filter(|join| join.derived.is_none())
                    .map(|join| &join.table),
            )
        {
            if source.generate_series.is_some() || super::cte::lookup_cte(&source.name).is_some() {
                continue;
            }
            if let Ok(table) = db.table(&source.name) {
                db.check_primary_read_view(&table, txn)?;
            }
        }
    }
    Ok(())
}

pub(super) fn execute_select(
    db: &Arc<Database>,
    txn: TxnView,
    stmt: Select,
    registry: &SessionRegistry,
) -> Result<QueryResponse, QueryError> {
    let _read_schema = crate::storage::read_schema::ReadSchemaScope::enter(
        (!txn.dirty_read).then_some(txn.snapshot_scn),
    );
    check_primary_definition_for_select(db, txn, &stmt)?;
    let _distinct = stmt.distinct.then(|| {
        crate::engine::cancel::enter_process_stage(
            crate::engine::session_registry::ProcessState::RemovingDuplicates,
        )
    });
    with_session_registry(registry.clone(), || {
        self::eval::with_subquery_context(db, txn, || execute_select_inner(db, txn, stmt, registry))
    })
}

fn execute_select_inner(
    db: &Arc<Database>,
    txn: TxnView,
    mut stmt: Select,
    registry: &SessionRegistry,
) -> Result<QueryResponse, QueryError> {
    // Capture this algebraic derived-table shape before alias binding rewrites
    // GROUP/ORDER expressions. The later preparation pass is intentionally
    // free to normalize those expressions for the ordinary planner, while
    // this proof depends on the original output-reference relationship.
    let derived_frequency_spec = stmt
        .from
        .iter()
        .all(|table| table.column_aliases.is_empty())
        .then_some(&stmt)
        .and_then(|stmt| stmt.derived.as_ref().map(|derived| (stmt, derived)))
        .and_then(|(stmt, derived)| derived_count_frequency_spec(stmt, derived));

    // Freeze executor topology/tuning inputs before planning. Nested SELECTs
    // inherit this scope, and worker contexts clone the same snapshot.
    let _tuning_scope = crate::engine::executor::tuning::enter_if_absent();
    // Single-table fast paths use a scan scope; joins carry each reference's
    // selection in PlanTable so another participant is never restricted.
    let _partition_scope = if stmt.from.len() == 1 && stmt.joins.is_empty() {
        crate::engine::executor::partition_clause::resolve_partition_clause(
            db,
            &stmt.from[0].name,
            &stmt.from[0].partitions,
        )?
        .map(crate::engine::executor::partition_router::push_pruned_partitions)
    } else {
        None
    };
    let temporal_context = super::temporal::preflight(db, &stmt)?;
    let _temporal_scope = temporal_context.map(|context| super::temporal::enter(Some(context)));
    // Normalize WHERE / HAVING / JOIN-ON before planning and execution so
    // every physical executor observes the same rewritten predicate tree.
    prepare_select_for_planning(db, &mut stmt)?;
    bind_single_table_metadata_inspection_collations(db, &mut stmt);
    let _snapshot_guard =
        (!txn.snapshot_registered).then(|| db.register_statement_snapshot(txn.snapshot_scn));
    if !txn.dirty_read {
        if let Some(shared) = shared_aggregate::factor(db, &stmt) {
            // Use the caller's exact TxnView for both producer and consumers;
            // factoring a SELECT must not start a second RC read snapshot.
            let body_hint = crate::engine::cache::take_query_hint();
            let mut entries = std::collections::HashMap::new();
            for (definition, consumer_count) in &shared.definitions {
                #[cfg(test)]
                SHARED_DERIVED_AGGREGATE_HITS.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                let response = super::cte::with_cte_materialization_active(true, || {
                    crate::engine::executor::dispatch::execute_subquery_response(
                        db,
                        txn,
                        &definition.query,
                        registry,
                    )
                })?;
                let mut result =
                    materialize_response_with_spill(response, db.data_dir().to_path_buf())?;
                result.consumer_count = *consumer_count;
                entries.insert(definition.name.clone(), Arc::new(result));
            }
            return super::cte::with_cte_registry_extended(entries, || match body_hint {
                Some(hint) => crate::engine::cache::with_query_hint(hint, || {
                    execute_select(db, txn, shared.body, registry)
                }),
                None => execute_select(db, txn, shared.body, registry),
            });
        }
    }

    // Window functions reached here live in a derived table / subquery: the
    // top-level window path runs in the session-ful dispatcher and never calls
    // `execute_select` with a windowed statement. Route to the session-free
    // window executor, which materializes any derived FROM into transient CTEs
    // and runs the window over the current `txn`.
    if crate::engine::executor::dispatch::select_has_window_functions(&stmt) {
        return crate::engine::executor::dispatch::execute_windowed_select(db, txn, stmt, registry);
    }
    if stmt.values.is_some() || stmt.json_table.is_some() {
        let physical = planner::plan_prepared_physical_select(db, &stmt)?;
        return execute_physical_select(db, txn, &stmt, &physical);
    }
    if let Some(response) = try_bridge_scalar_derived_parallel(db, txn, &mut stmt, registry)? {
        return Ok(response);
    }
    if stmt
        .from
        .first()
        .is_some_and(|t| t.name == "$nested_join_preserve")
    {
        return execute_select_with_cte_joins(db, txn, stmt, registry);
    }
    if stmt.derived.is_some() {
        return execute_derived_select(db, txn, stmt, derived_frequency_spec, registry);
    }
    // `FROM generate_series(...)` as the sole source (no JOIN): route through
    // the planner so it emits a `PhysicalPlan::GenerateSeries` leaf;
    // `execute_const_select` assumes no FROM. JOINs against a series are
    // handled in a later milestone.
    if stmt.joins.is_empty()
        && stmt
            .from
            .first()
            .map(|t| t.generate_series.is_some())
            .unwrap_or(false)
    {
        let physical = planner::plan_prepared_physical_select(db, &stmt)?;
        return execute_physical_select(db, txn, &stmt, &physical);
    }
    if stmt.from.iter().any(|table| table.system_time.is_some())
        || stmt
            .joins
            .iter()
            .any(|join| join.table.system_time.is_some())
    {
        let physical = planner::plan_prepared_physical_select(db, &stmt)?;
        return execute_physical_select(db, txn, &stmt, &physical);
    }
    // Correlated LATERAL / anonymous parenthesised derived JOIN targets still
    // route to the scope-preserving path. Ordinary table/CTE/GenerateSeries
    // joins, including FULL joins, are regular
    // PhysicalPlan::HashJoin nodes and use the common batch match bitmap.
    // Ordinary CTE references are
    // planned as `PhysicalPlan::CteScan` leaves, so they can participate in
    // hash/semi joins instead of materialising a cartesian nested-loop spine.
    //
    // Inline derived tables in JOIN positions (`FROM a, (SELECT ...) b` or
    // `JOIN (SELECT ...) b ON ...`) are first bridged to CTE registry
    // entries so they take the *same* `CteScan` + CBO path. Without this,
    // the in-memory `execute_select_with_cte_joins` fallback below fully
    // materialises every base table and joins with index-blind nested
    // loops — pathological when a derived table is comma-joined against a
    // large fact table (TPC-DS Q8: 3.7 s vs ~0.12 s for the equivalent
    // `WITH` CTE). The bridge is skipped (leaving the in-memory path) when
    // the query needs it for another reason — a LATERAL derived (references
    // outer rows, cannot be pre-materialised) or a `generate_series` source.
    // FULL joins are ordinary batch HashJoin nodes, including when their
    // derived inputs have first been exposed as CteScan leaves. GenerateSeries
    // is likewise a native leaf and needs no table-function dispatch branch.
    if !stmt.joins.is_empty()
        && super::cte::select_has_join_derived(&stmt)
        && !stmt.joins.iter().any(|j| j.derived.is_some() && j.lateral)
        // An unaliased parenthesised join deliberately exposes its leaf
        // qualifiers to the enclosing ON clause.  A CteScan has one relation
        // alias and would erase that scope, so keep this recursive join group
        // on the in-memory derived path where per-column leaf metadata is
        // retained.
        && !stmt
            .joins
            .iter()
            .any(|j| j.table.name == "$nested_join_preserve")
    {
        return bridge_join_derived_to_ctes(db, txn, stmt, registry);
    }
    if !stmt.joins.is_empty() && super::cte::select_has_join_derived(&stmt) {
        return execute_select_with_cte_joins(db, txn, stmt, registry);
    }
    // COUNT(*) over a single system table can be answered from the resident
    // catalog headers. Route it through the dedicated aggregate pushdown
    // before the physical SystemScan path materialises every metadata row.
    // This is essential for 100k-table catalogs: the generic batch scan both
    // loads every descriptor and builds a multi-hundred-MiB row vector.
    if stmt.joins.is_empty()
        && stmt.from.len() == 1
        && is_system_table_query(&stmt.from[0].name)
        && system_count_pushdown_supported(&stmt.from[0].name)
        && system_count_star_fast_path(&stmt)
        && stmt
            .selection
            .as_ref()
            .is_none_or(system_count_filter_is_exact)
    {
        return execute_system_select(db, stmt, registry);
    }
    if select_references_unregistered_system_table(&stmt) {
        let physical = planner::plan_prepared_physical_select(db, &stmt)?;
        return execute_physical_select(db, txn, &stmt, &physical);
    }
    // CTE reference in plain FROM: let the planner emit a
    // `PhysicalPlan::CteScan` instead of resolving a catalog table.
    let from_is_cte = stmt
        .from
        .first()
        .map(|t| super::cte::has_cte(&t.name))
        .unwrap_or(false);
    // `table` is the legacy single-source shortcut.  Rewrites that preserve
    // the canonical `from` relation (notably implicit single-group aggregate
    // binding) may deliberately clear it, so source classification must use
    // the real relation list as the authority.  Treating `table == None` as a
    // constant SELECT routed a sourceful query through `execute_const_select`
    // and rejected its hidden-group column before the physical pipeline was
    // even planned.
    let Some(table_name) = stmt
        .table
        .clone()
        .or_else(|| stmt.from.first().map(|source| source.name.clone()))
    else {
        return execute_const_select(db, txn, &stmt);
    };
    if !is_system_table_query(&table_name) {
        inject_order_limit_lower_bound(db, &mut stmt, &table_name)?;
    }
    if from_is_cte {
        let physical = planner::plan_prepared_physical_select(db, &stmt)?;
        return execute_physical_select(db, txn, &stmt, &physical);
    }
    // Take the hint exactly once per SELECT, so nested subqueries don't
    // mistakenly reuse the outer SQL's cache key against their own AST.
    //
    // Phase 2.5: plans containing `Expr::Placeholder` are now safely
    // cacheable. The planner produces deferred `KeyPart::Param` parts
    // inside `IndexKey`s, and the storage layer resolves them against
    // the active thread-local parameter context at every index access
    // entry point. A single cached plan therefore applies to every
    // EXECUTE on the same prepared statement template (or every
    // auto-parameterised COM_QUERY shape). Cardinality tiers below separate
    // bindings that need materially different access/join strategies.
    // Snapshot-filtered schemas share the catalog version with the current
    // schema. Do not publish their fallback plan under a snapshot-independent
    // cache key: it would hide the newly built index from later fresh reads.
    if let Some(hint) =
        crate::engine::cache::take_query_hint().filter(|_| !db.read_definition_changed_since(txn))
    {
        if let Some(cache) = hint.plan_cache.upgrade() {
            // SQL EXECUTE installs bindings even when its outer COM_QUERY
            // hint is not auto-parameterized. Include those domains as well;
            // otherwise EXECUTE reuses a plan compiled for an earlier type.
            let plan_params = crate::engine::params::snapshot_current_params().unwrap_or_default();
            let explicit_bindings =
                !plan_params.is_empty() && !crate::engine::params::auto_parameterized_active();
            let cache_sql = if hint.parameterized || explicit_bindings {
                let mut fingerprint =
                    crate::engine::auto_param::typed_plan_fingerprint(&stmt, &plan_params);
                fingerprint.push_str(&planner::parameter_selectivity_signature(
                    db,
                    &stmt,
                    &plan_params,
                ));
                Arc::<str>::from(fingerprint)
            } else {
                Arc::clone(&hint.normalized_sql)
            };
            let key = crate::engine::cache::StatementKey {
                sql: cache_sql,
                database: Arc::clone(&hint.database),
                semantics: hint.semantics,
            };
            if let Some(cached) = cache.get_select(&key, &stmt, |deps, obj| {
                plan_deps_match_catalog(db, deps) && plan_object_deps_match_catalog(db, obj)
            }) {
                db.record_select_compiled_plan_hit();
                return execute_compiled_select(db, txn, &stmt, &cached);
            }
            let plan_arc = Arc::new(planner::with_planning_params(&plan_params, || {
                planner::plan_prepared_physical_select(db, &stmt)
            })?);
            let deps = collect_plan_deps(&plan_arc);
            // Views expanded into this plan (recorded during expand_views);
            // validated on lookup so ALTER/DROP VIEW invalidates only the
            // plans that used the view, without a global cache flush.
            let object_deps = crate::engine::cache::current_plan_object_deps();
            // Plans whose freshness the dependency snapshot can't fully
            // describe must not enter the cache: a CteScan reads rows
            // materialised outside this plan (its base tables never
            // appear in `deps`), and an empty dependency set has nothing
            // to validate against. Table-scoped DDL no longer bumps the
            // global cache epoch, so an undescribed dependency would
            // otherwise serve a stale plan after e.g. ALTER TABLE on a
            // CTE base table. A view-only dependency (empty table deps but
            // non-empty object deps) is still describable, so allow it.
            let describable = !deps.is_empty() || !object_deps.is_empty();
            if describable && !plan_contains_cte_scan(&plan_arc) {
                db.record_select_operator_compile();
                let compiled = cache.insert(
                    key,
                    Arc::new(stmt.clone()),
                    Arc::clone(&plan_arc),
                    deps,
                    object_deps,
                );
                return execute_compiled_select(db, txn, &stmt, &compiled);
            }
            let compiled = crate::engine::cache::CompiledSelectPlan::new(
                Arc::new(stmt.clone()),
                plan_arc,
                smallvec::SmallVec::new(),
                smallvec::SmallVec::new(),
                cache.current_epoch(),
            );
            db.record_select_operator_compile();
            return execute_compiled_select(db, txn, &stmt, &compiled);
        }
    }

    let compiled = crate::engine::cache::CompiledSelectPlan::new(
        Arc::new(stmt.clone()),
        Arc::new(planner::plan_prepared_physical_select(db, &stmt)?),
        smallvec::SmallVec::new(),
        smallvec::SmallVec::new(),
        0,
    );
    db.record_select_operator_compile();
    execute_compiled_select(db, txn, &stmt, &compiled)
}

fn bind_single_table_metadata_inspection_collations(db: &Database, stmt: &mut Select) {
    if !stmt.joins.is_empty() || stmt.from.len() != 1 {
        return;
    }
    let Some(table_name) = stmt.table.as_deref() else {
        return;
    };
    let Ok(table) = db.table(table_name) else {
        return;
    };
    for item in &mut stmt.projection {
        let SelectItem::Expr {
            expr: Expr::Function { name, args, .. },
            ..
        } = item
        else {
            continue;
        };
        if args.len() != 1
            || !(name.eq_ignore_ascii_case("CHARSET")
                || name.eq_ignore_ascii_case("COLLATION")
                || name.eq_ignore_ascii_case("COERCIBILITY"))
            || matches!(
                args.first(),
                Some(Expr::Function { name, .. })
                    if name.eq_ignore_ascii_case("__COLLATED_CONST")
            )
        {
            continue;
        }
        let descriptor = crate::engine::expr::expr_type_descriptor(&args[0], &table);
        let Some(collation) = descriptor.collation else {
            continue;
        };
        let Some(info) = crate::charset::resolve_collation_id(collation.collation_id) else {
            continue;
        };
        let original = std::mem::replace(&mut args[0], Expr::Literal(Literal::Null));
        args[0] = Expr::Function {
            name: "__COLLATED_CONST".to_string(),
            args: vec![
                original,
                Expr::Literal(Literal::String(info.name.to_string())),
                Expr::Literal(Literal::Int(collation.coercibility as i64)),
            ],
            distinct: false,
        };
    }
}

fn inject_order_limit_lower_bound(
    db: &Arc<Database>,
    stmt: &mut Select,
    table_name: &str,
) -> Result<(), QueryError> {
    if stmt.selection.is_some()
        || stmt.order_by.is_empty()
        || (stmt.limit.is_none() && stmt.offset.is_none())
    {
        return Ok(());
    }
    let Some(first_order) = stmt.order_by.first() else {
        return Ok(());
    };
    let Some(column_name) = simple_order_expr_column_name(&first_order.expr) else {
        return Ok(());
    };
    // This is a plan-shape optimization, not a resolution step: the primary
    // source may be a CTE (or any non-catalog reference) whose name has no
    // base-table row in the dictionary. The injected lower bound only helps an
    // index-backed ORDER BY ... LIMIT, which a CTE never has, so a failed
    // catalog lookup means "optimization not applicable" — bail quietly and let
    // the planner (which is CTE-aware) resolve the source. Propagating the 1146
    // here would spuriously fail a CTE driving a JOIN with ORDER BY ... LIMIT.
    let Ok(table) = db.table(table_name) else {
        return Ok(());
    };
    let Some(column) = table
        .columns
        .iter()
        .find(|col| col.name.eq_ignore_ascii_case(column_name))
    else {
        return Ok(());
    };
    if column.nullable {
        return Ok(());
    }
    let has_order_index = table.iter_queryable_indexes().any(|index| {
        index
            .columns
            .first()
            .is_some_and(|name| name.eq_ignore_ascii_case(column_name))
    });
    if !has_order_index {
        return Ok(());
    }
    let literal = match (&column.data_type, column.unsigned) {
        (
            crate::storage::DataTypeKind::TinyInt
            | crate::storage::DataTypeKind::SmallInt
            | crate::storage::DataTypeKind::MediumInt
            | crate::storage::DataTypeKind::Int
            | crate::storage::DataTypeKind::BigInt
            | crate::storage::DataTypeKind::Bool,
            false,
        ) => crate::parser::Literal::Int(i64::MIN),
        (
            crate::storage::DataTypeKind::TinyInt
            | crate::storage::DataTypeKind::SmallInt
            | crate::storage::DataTypeKind::MediumInt
            | crate::storage::DataTypeKind::Int
            | crate::storage::DataTypeKind::BigInt
            | crate::storage::DataTypeKind::Bool,
            true,
        ) => crate::parser::Literal::UInt(0),
        _ => return Ok(()),
    };
    stmt.selection = Some(Expr::Binary {
        left: Box::new(first_order.expr.clone()),
        op: BinaryOp::GtEq,
        right: Box::new(Expr::Literal(literal)),
    });
    Ok(())
}

fn simple_order_expr_column_name(expr: &Expr) -> Option<&str> {
    match expr {
        Expr::Identifier(name) => Some(name.as_str()),
        Expr::CompoundIdentifier(parts) => parts.last().map(String::as_str),
        _ => None,
    }
}

/// Preserve row-lock identity when a mergeable derived query (including an
/// inlined view or CTE) is a projection/filter over base tables.
pub(super) fn try_flatten_locking_derived_select(db: &Database, select: &Select) -> Option<Select> {
    if !select.for_update
        || !select.group_by.is_empty()
        || select.having.is_some()
        || select_projection_has_toplevel_aggregate(&select.projection)
    {
        return None;
    }
    let mut candidate = select.clone();
    candidate.for_update = false;
    candidate.lock_clause = None;
    let SubQuery::Select(inner) = candidate.derived.as_mut()? else {
        return None;
    };
    if inner.from.len() != 1 || inner.derived.is_some() {
        return None;
    }
    // A view-level OF target cannot be copied verbatim to flattened base
    // aliases. Keep such queries on the explicit unsupported path.
    if !inner.joins.is_empty()
        && select
            .lock_clause
            .as_ref()
            .is_some_and(|clause| !clause.targets.is_empty())
    {
        return None;
    }
    let table_ref = &inner.from[0];
    if super::cte::lookup_cte(&table_ref.name).is_some() {
        return None;
    }
    let table = db.table(&table_ref.name).ok()?;
    let qualifier = table_ref.alias.as_ref().unwrap_or(&table_ref.name);
    let mut projection = Vec::new();
    for item in &inner.projection {
        match item {
            SelectItem::Wildcard | SelectItem::QualifiedWildcard(_) => {
                if !inner.joins.is_empty() {
                    return None;
                }
                for (_, column) in table.iter_select_columns() {
                    projection.push(SelectItem::Expr {
                        expr: Expr::CompoundIdentifier(vec![
                            qualifier.clone(),
                            column.name.clone(),
                        ]),
                        alias: Some(column.name.clone()),
                    });
                }
            }
            other => projection.push(other.clone()),
        }
    }
    inner.projection = projection;
    let names = inner
        .projection
        .iter()
        .map(|item| {
            let SelectItem::Expr { expr, alias } = item else {
                return None;
            };
            alias
                .clone()
                .or_else(|| expr_column_name(expr).map(str::to_string))
        })
        .collect::<Option<Vec<_>>>()?;
    let outer_ref = candidate.from.first()?;
    let outer_qualifier = outer_ref.alias.as_ref().unwrap_or(&outer_ref.name);
    let mut projection = Vec::new();
    for item in &candidate.projection {
        match item {
            SelectItem::Wildcard => {
                projection.extend(names.iter().map(|name| SelectItem::Expr {
                    expr: Expr::Identifier(name.clone()),
                    alias: Some(name.clone()),
                }));
            }
            SelectItem::QualifiedWildcard(qualifier)
                if qualifier.eq_ignore_ascii_case(outer_qualifier) =>
            {
                projection.extend(names.iter().map(|name| SelectItem::Expr {
                    expr: Expr::Identifier(name.clone()),
                    alias: Some(name.clone()),
                }));
            }
            other => projection.push(other.clone()),
        }
    }
    candidate.projection = projection;
    let limit = candidate.limit.take();
    let offset = candidate.offset.take();
    let mut flat = try_flatten_simple_derived_select(&candidate)?;
    flat.limit = limit;
    flat.offset = offset;
    flat.for_update = select.for_update;
    flat.lock_clause = select.lock_clause.clone();
    Some(flat)
}

pub(super) fn try_flatten_simple_derived_select(select: &Select) -> Option<Select> {
    if select.derived.is_none()
        || select.straight_join
        || select.from.len() != 1
        || !select.joins.is_empty()
        || select.distinct
        || select.limit.is_some()
        || select.offset.is_some()
        || select.for_update
        || select.lock_clause.is_some()
        || !select.named_windows.is_empty()
        // GROUP BY ... WITH ROLLUP / GROUPING SETS over the derived table:
        // `flat` is built from the inner SELECT and would not carry the
        // outer super-aggregate modifier, silently dropping it. Leave these
        // to the general planner path (`execute_derived_select` re-routes
        // them through a real `HashRollupAggregate`).
        || select.group_by_modifier != crate::parser::GroupByModifier::None
        || !select.grouping_sets.is_empty()
        || select
            .projection
            .iter()
            .any(|item| select_item_has_subquery(item) || select_item_has_window(item))
        || select.group_by.iter().any(derived_rewrite_expr_unsafe)
        || select
            .order_by
            .iter()
            .any(|item| derived_rewrite_expr_unsafe(&item.expr))
        || select
            .selection
            .as_ref()
            .is_some_and(derived_rewrite_expr_unsafe)
        || select
            .having
            .as_ref()
            .is_some_and(derived_rewrite_expr_unsafe)
    {
        return None;
    }

    // Only an ordinary SELECT can be flattened into the outer query; a
    // set-operation derived table falls through to the general path
    // (`as_select()` returns None → no flattening).
    let inner = select.derived.as_ref()?.as_select()?;
    if inner.no_merge
        || inner.derived.is_some()
        || inner.distinct
        || inner.limit.is_some()
        || inner.offset.is_some()
        || !inner.group_by.is_empty()
        || inner.having.is_some()
        || !inner.order_by.is_empty()
        || inner.for_update
        || inner.lock_clause.is_some()
        || !inner.named_windows.is_empty()
        || inner
            .selection
            .as_ref()
            .is_some_and(derived_inner_expr_unsafe)
        || inner.joins.iter().any(|join| {
            join.derived.is_some()
                || join
                    .condition
                    .as_ref()
                    .is_some_and(derived_inner_expr_unsafe)
        })
        || inner
            .projection
            .iter()
            .any(|item| !matches!(item, SelectItem::Expr { .. }))
        || inner.projection.iter().any(|item| match item {
            SelectItem::Expr { expr, .. } => derived_inner_expr_unsafe(expr),
            _ => true,
        })
    {
        return None;
    }

    let alias_map = derived_projection_alias_map(&inner.projection)?;
    let derived_ref = select.from.first()?;
    let derived_qualifier = derived_ref.alias.as_deref().unwrap_or(&derived_ref.name);
    let mut flat = inner.clone();
    flat.projection = select
        .projection
        .iter()
        .map(|item| rewrite_select_item_for_derived(item, &alias_map, derived_qualifier))
        .collect::<Option<Vec<_>>>()?;
    flat.selection = combine_optional_filters(
        inner.selection.clone(),
        rewrite_optional_expr_for_derived(
            select.selection.as_ref(),
            &alias_map,
            derived_qualifier,
        )?,
    );
    flat.group_by = select
        .group_by
        .iter()
        .map(|expr| rewrite_expr_for_derived(expr, &alias_map, derived_qualifier))
        .collect::<Option<Vec<_>>>()?;
    flat.having =
        rewrite_optional_expr_for_derived(select.having.as_ref(), &alias_map, derived_qualifier)?;
    flat.order_by = select
        .order_by
        .iter()
        .map(|item| {
            Some(OrderBy {
                expr: rewrite_expr_for_derived(&item.expr, &alias_map, derived_qualifier)?,
                asc: item.asc,
                nulls_first: item.nulls_first,
            })
        })
        .collect::<Option<Vec<_>>>()?;
    flat.limit = None;
    flat.offset = None;
    flat.distinct = false;
    flat.for_update = false;
    flat.lock_clause = None;
    flat.named_windows.clear();
    flat.query_hints.extend(select.query_hints.clone());
    Some(flat)
}

fn derived_projection_alias_map(projection: &[SelectItem]) -> Option<HashMap<String, Expr>> {
    let mut map = HashMap::new();
    for item in projection {
        let SelectItem::Expr { expr, alias } = item else {
            return None;
        };
        let name = alias.clone().unwrap_or_else(|| expr_name(expr));
        let key = name.to_ascii_lowercase();
        match map.entry(key) {
            Entry::Vacant(slot) => {
                slot.insert(expr.clone());
            }
            Entry::Occupied(existing) if expr_semantically_eq(existing.get(), expr) => {}
            Entry::Occupied(_) => return None,
        }
    }
    if map.is_empty() {
        None
    } else {
        Some(map)
    }
}

fn rewrite_select_item_for_derived(
    item: &SelectItem,
    alias_map: &HashMap<String, Expr>,
    derived_qualifier: &str,
) -> Option<SelectItem> {
    let SelectItem::Expr { expr, alias } = item else {
        return None;
    };
    let rewritten = rewrite_expr_for_derived(expr, alias_map, derived_qualifier)?;
    let alias = alias
        .clone()
        .or_else(|| derived_column_reference_name(expr, alias_map, derived_qualifier));
    Some(SelectItem::Expr {
        expr: rewritten,
        alias,
    })
}

fn derived_column_reference_name(
    expr: &Expr,
    alias_map: &HashMap<String, Expr>,
    derived_qualifier: &str,
) -> Option<String> {
    match expr {
        Expr::Identifier(name) if alias_map.contains_key(&name.to_ascii_lowercase()) => {
            Some(name.clone())
        }
        Expr::CompoundIdentifier(parts)
            if parts.len() == 2
                && parts[0].eq_ignore_ascii_case(derived_qualifier)
                && alias_map.contains_key(&parts[1].to_ascii_lowercase()) =>
        {
            Some(parts[1].clone())
        }
        _ => None,
    }
}

fn combine_optional_filters(left: Option<Expr>, right: Option<Expr>) -> Option<Expr> {
    match (left, right) {
        (Some(left), Some(right)) => Some(Expr::Binary {
            left: Box::new(left),
            op: BinaryOp::And,
            right: Box::new(right),
        }),
        (left, None) => left,
        (None, right) => right,
    }
}

fn rewrite_optional_expr_for_derived(
    expr: Option<&Expr>,
    alias_map: &HashMap<String, Expr>,
    derived_qualifier: &str,
) -> Option<Option<Expr>> {
    match expr {
        Some(expr) => Some(Some(rewrite_expr_for_derived(
            expr,
            alias_map,
            derived_qualifier,
        )?)),
        None => Some(None),
    }
}

fn rewrite_expr_for_derived(
    expr: &Expr,
    alias_map: &HashMap<String, Expr>,
    derived_qualifier: &str,
) -> Option<Expr> {
    crate::parser::visit::try_map_value_expr(expr, &mut |node| match node {
        Expr::Identifier(name) => alias_map
            .get(&name.to_ascii_lowercase())
            .cloned()
            .map(Some)
            .ok_or(()),
        Expr::CompoundIdentifier(parts)
            if parts.len() == 2 && parts[0].eq_ignore_ascii_case(derived_qualifier) =>
        {
            alias_map
                .get(&parts[1].to_ascii_lowercase())
                .cloned()
                .map(Some)
                .ok_or(())
        }
        Expr::CompoundIdentifier(_) => Err(()),
        _ => Ok(None),
    })
    .ok()
}

fn derived_rewrite_expr_unsafe(expr: &Expr) -> bool {
    expr_has_subquery(expr) || expr_has_window(expr)
}

fn derived_inner_expr_unsafe(expr: &Expr) -> bool {
    expr_has_subquery(expr)
        || expr_has_aggregate(expr)
        || expr_has_window(expr)
        || expr_has_observable_side_effect(expr)
}

/// Projection expressions that cannot be discarded or have their evaluation
/// count changed while flattening a derived table. In particular,
/// `SELECT COUNT(*) FROM (SELECT @n := @n + 1 FROM t) d` must execute the
/// assignment once per input row even though the outer aggregate does not
/// consume the projected value.
fn expr_has_observable_side_effect(expr: &Expr) -> bool {
    match expr {
        Expr::Function { name, args, .. } => {
            crate::engine::http::is_function(name)
                || crate::engine::expr::function_has_external_effect(name)
                || matches!(
                    name.to_ascii_uppercase().as_str(),
                    "__SET_USER_VAR"
                        | "NEXTVAL"
                        | "SETVAL"
                        | "SLEEP"
                        | "GET_LOCK"
                        | "RELEASE_LOCK"
                        | "RELEASE_ALL_LOCKS"
                        | "LOAD_FILE"
                )
                || args.iter().any(expr_has_observable_side_effect)
        }
        Expr::Unary { expr, .. }
        | Expr::IsNull(expr)
        | Expr::IsNotNull(expr)
        | Expr::GroupKeyRef { expr, .. } => expr_has_observable_side_effect(expr),
        Expr::Binary { left, right, .. }
        | Expr::Like {
            expr: left,
            pattern: right,
            ..
        } => expr_has_observable_side_effect(left) || expr_has_observable_side_effect(right),
        Expr::InList { expr, list, .. } => {
            expr_has_observable_side_effect(expr)
                || list.iter().any(expr_has_observable_side_effect)
        }
        Expr::Between {
            expr, low, high, ..
        } => {
            expr_has_observable_side_effect(expr)
                || expr_has_observable_side_effect(low)
                || expr_has_observable_side_effect(high)
        }
        Expr::Case {
            operand,
            conditions,
            results,
            else_result,
        } => {
            operand
                .as_deref()
                .is_some_and(expr_has_observable_side_effect)
                || conditions.iter().any(expr_has_observable_side_effect)
                || results.iter().any(expr_has_observable_side_effect)
                || else_result
                    .as_deref()
                    .is_some_and(expr_has_observable_side_effect)
        }
        Expr::Tuple(items) => items.iter().any(expr_has_observable_side_effect),
        Expr::QuantifiedComparison { left, right, .. } => {
            expr_has_observable_side_effect(left)
                || match right {
                    QuantifiedComparisonRight::List(items) => {
                        items.iter().any(expr_has_observable_side_effect)
                    }
                    QuantifiedComparisonRight::Subquery(_) => true,
                }
        }
        Expr::WindowFunction { args, spec, .. } => {
            let crate::parser::WindowSpec {
                partition_by,
                order_by,
                frame,
                ..
            } = spec.as_ref();

            args.iter()
                .chain(partition_by)
                .any(expr_has_observable_side_effect)
                || order_by
                    .iter()
                    .any(|item| expr_has_observable_side_effect(&item.expr))
                || frame.as_ref().is_some_and(|frame| {
                    [&frame.start, &frame.end]
                        .into_iter()
                        .any(|bound| match bound {
                            crate::parser::WindowFrameBound::Preceding(expr)
                            | crate::parser::WindowFrameBound::Following(expr) => {
                                expr_has_observable_side_effect(expr)
                            }
                            _ => false,
                        })
                })
        }
        Expr::Subquery(_) | Expr::Exists { .. } | Expr::InSubquery { .. } => true,
        _ => false,
    }
}

fn select_item_has_window(item: &SelectItem) -> bool {
    matches!(item, SelectItem::Expr { expr, .. } if expr_has_window(expr))
}

pub(super) fn expr_has_window(expr: &Expr) -> bool {
    match expr {
        Expr::WindowFunction { .. } => true,
        Expr::Unary { expr, .. } | Expr::IsNull(expr) | Expr::IsNotNull(expr) => {
            expr_has_window(expr)
        }
        Expr::Binary { left, right, .. } => expr_has_window(left) || expr_has_window(right),
        Expr::InList { expr, list, .. } => {
            expr_has_window(expr) || list.iter().any(expr_has_window)
        }
        Expr::QuantifiedComparison { left, right, .. } => {
            expr_has_window(left)
                || match right {
                    QuantifiedComparisonRight::Subquery(subquery) => select_has_window(subquery),
                    QuantifiedComparisonRight::List(list) => list.iter().any(expr_has_window),
                }
        }
        Expr::Between {
            expr, low, high, ..
        } => expr_has_window(expr) || expr_has_window(low) || expr_has_window(high),
        Expr::Function { args, .. } => args.iter().any(expr_has_window),
        Expr::Like { expr, pattern, .. } => expr_has_window(expr) || expr_has_window(pattern),
        Expr::Case {
            operand,
            conditions,
            results,
            else_result,
        } => {
            operand.as_ref().is_some_and(|expr| expr_has_window(expr))
                || conditions.iter().any(expr_has_window)
                || results.iter().any(expr_has_window)
                || else_result
                    .as_ref()
                    .is_some_and(|expr| expr_has_window(expr))
        }
        Expr::InSubquery { expr, subquery, .. } => {
            expr_has_window(expr) || select_has_window(subquery)
        }
        Expr::Subquery(subquery) | Expr::Exists { subquery, .. } => select_has_window(subquery),
        _ => false,
    }
}

fn select_has_window(select: &Select) -> bool {
    !select.named_windows.is_empty()
        || select.projection.iter().any(select_item_has_window)
        || select.selection.as_ref().is_some_and(expr_has_window)
        || select.group_by.iter().any(expr_has_window)
        || select.having.as_ref().is_some_and(expr_has_window)
        || select
            .order_by
            .iter()
            .any(|item| expr_has_window(&item.expr))
        || select.joins.iter().any(|join| {
            join.condition.as_ref().is_some_and(expr_has_window)
                || join
                    .derived
                    .as_ref()
                    .and_then(SubQuery::as_select)
                    .is_some_and(select_has_window)
        })
        // A set-operation derived table's branches are executed independently
        // (each through `execute_select`, which detects its own windows), so
        // only an ordinary-SELECT derived table affects the outer routing.
        || select
            .derived
            .as_ref()
            .and_then(SubQuery::as_select)
            .is_some_and(select_has_window)
}

// Phase 2.5 removed `select_contains_placeholder` /
// `expr_contains_placeholder`: plans with placeholders are first-class
// cache citizens now that storage entry points resolve `KeyPart::Param`
// against the live thread-local parameter context, so a single cached
// plan applies correctly to every parameter set.

/// Return the (table_id, schema_version) pairs every `PlanTable` in the
/// tree references. Used at insert time to capture the catalog snapshot
/// the plan was built against and at lookup time to detect stale entries.
pub(super) fn collect_plan_deps(
    plan: &PhysicalPlan,
) -> smallvec::SmallVec<[crate::engine::cache::PlanDep; 4]> {
    let mut deps = smallvec::SmallVec::<[crate::engine::cache::PlanDep; 4]>::new();
    walk_plan_tables(plan, &mut |table| {
        if table.table_id == 0 {
            return;
        }
        // Capture the `db.name` key so cache validation resolves the table
        // with one BTreeMap lookup (O(deps)) rather than scanning by id.
        if deps
            .iter()
            .any(|(_, id, ver)| *id == table.table_id && *ver == table.schema.schema_version)
        {
            return;
        }
        let key: std::sync::Arc<str> = std::sync::Arc::from(
            crate::storage::naming::table_key(&table.schema.database, &table.schema.name).as_str(),
        );
        deps.push((key, table.table_id, table.schema.schema_version));
    });
    deps
}

/// Base-table `(table_id, schema_version)` pairs a CTE body reads, for the
/// session CTE result cache's per-table invalidation. Returns `None` when
/// the body isn't a plain `Select` or references another CTE (a `CteScan`'s
/// base tables don't surface in `collect_plan_deps`), telling the caller to
/// fall back to conservative snapshot-SCN invalidation.
pub(super) fn cte_body_base_table_versions(
    db: &Database,
    select: &Select,
) -> Option<smallvec::SmallVec<[crate::engine::cache::PlanDep; 4]>> {
    // `plan_select` can't plan a derived-table FROM. A CTE body like q39's
    // `inv` (`SELECT ... FROM (inner) foo WHERE ...`) is a pure wrapper whose
    // only table source is the derived subquery: peel it and recurse on the
    // inner SELECT, whose base-table FROM `plan_select` *can* plan. The peel
    // fires only when the wrapper introduces no table of its own (single
    // derived FROM, no joins, no subquery in any clause that could hide an
    // uncaptured table), so the inner's base tables are exactly the wrapper's
    // dependencies — no table is missed.
    if let Some(inner) = derived_wrapper_inner(select) {
        return cte_body_base_table_versions(db, inner);
    }
    let planned = crate::planner::plan_select(db, select).ok()?;
    if plan_contains_cte_scan(&planned.physical) {
        return None;
    }
    Some(collect_plan_deps(&planned.physical))
}

/// A CTE body that is *only* a wrapper around a single derived-table FROM
/// (no extra FROM/JOIN tables, no subqueries in any clause). Returns the
/// inner SELECT so the caller recurses to its base tables. `None` for any
/// other shape, including a set-op subquery or any clause subquery that
/// might reference an uncaptured table.
fn derived_wrapper_inner(select: &Select) -> Option<&Select> {
    let inner = select.derived.as_ref()?.as_select()?;
    if select.from.len() != 1 || !select.joins.is_empty() {
        return None;
    }
    if select_clauses_have_subquery(select) {
        return None;
    }
    Some(inner)
}

/// True if any clause of `select` contains a subquery (which could reference
/// a base table not visible in FROM/JOIN). Conservative gate for the
/// derived-wrapper peel.
fn select_clauses_have_subquery(select: &Select) -> bool {
    use crate::engine::executor::select::simple::expr_has_subquery;
    select.projection.iter().any(|item| match item {
        SelectItem::Expr { expr, .. } => expr_has_subquery(expr),
        _ => false,
    }) || select.selection.as_ref().is_some_and(expr_has_subquery)
        || select.having.as_ref().is_some_and(expr_has_subquery)
        || select.group_by.iter().any(expr_has_subquery)
        || select.order_by.iter().any(|o| expr_has_subquery(&o.expr))
}

fn walk_plan_tables<F: FnMut(&PlanTable)>(plan: &PhysicalPlan, f: &mut F) {
    plan.walk(|node| match node {
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
        | PhysicalPlan::ParallelSeqScan { table, .. }
        | PhysicalPlan::ParallelZoneMapScan { table, .. }
        | PhysicalPlan::ParallelIndexFastScan { table, .. }
        | PhysicalPlan::ParallelIndexRangeScan { table, .. }
        | PhysicalPlan::IndexAggregate { table, .. }
        | PhysicalPlan::CoveringIndexAggregate { table, .. }
        | PhysicalPlan::ParallelIndexAggregate { table, .. }
        | PhysicalPlan::IndexInLookup { table, .. } => f(table),
        PhysicalPlan::IndexNestedLoopJoin { right_table, .. } => f(right_table),
        _ => {}
    });
}

pub(super) fn plan_deps_match_catalog(
    db: &Database,
    deps: &[crate::engine::cache::PlanDep],
) -> bool {
    db.table_versions_match(deps)
}

pub(super) fn plan_object_deps_match_catalog(
    db: &Database,
    object_deps: &[crate::engine::cache::ObjectDep],
) -> bool {
    db.view_versions_match(object_deps)
}

/// Resolve an EXPLAIN format after parsing. Plain EXPLAIN continues to obey
/// the session default, while ANALYZE defaults to TREE so its per-operator
/// runtime metrics are visible without an explicit FORMAT clause.
pub(in crate::engine::executor) fn resolve_explain_format(
    explicit: Option<crate::parser::ExplainFormat>,
    analyze: bool,
    session_default: crate::parser::ExplainFormat,
) -> crate::parser::ExplainFormat {
    explicit.unwrap_or(if analyze {
        crate::parser::ExplainFormat::Tree
    } else {
        session_default
    })
}

pub(super) fn describe_statement_columns<'q>(
    db: &Database,
    session: &SessionContext,
    statement: impl Into<crate::parser::QueryRef<'q>>,
) -> Result<Vec<Column>, QueryError> {
    let statement = statement.into();
    match statement {
        crate::parser::QueryRef::Select(stmt) => describe_select_columns(db, stmt),
        crate::parser::QueryRef::Rrf(rrf) => hybrid::describe_columns(db, rrf),
        crate::parser::QueryRef::SetOperation(set) => {
            let left = describe_statement_columns(db, session, &set.left)?;
            let right = describe_statement_columns(db, session, &set.right)?;
            describe_setop_columns(left, right)
        }
        crate::parser::QueryRef::Other(Statement::Insert(stmt)) if stmt.returning.is_some() => {
            let table = db.table(&stmt.table)?;
            super::returning::describe_returning(
                stmt.returning.as_ref(),
                &table,
                super::returning::DefaultImage::New,
                stmt.target_alias.as_deref(),
            )
        }
        crate::parser::QueryRef::Other(Statement::Update(stmt)) if stmt.returning.is_some() => {
            let table = db.table(&stmt.table)?;
            super::returning::describe_returning(
                stmt.returning.as_ref(),
                &table,
                super::returning::DefaultImage::New,
                stmt.primary_alias.as_deref(),
            )
        }
        crate::parser::QueryRef::Other(Statement::Delete(stmt)) if stmt.returning.is_some() => {
            let table = db.table(&stmt.table)?;
            super::returning::describe_returning(
                stmt.returning.as_ref(),
                &table,
                super::returning::DefaultImage::Old,
                stmt.primary_alias.as_deref(),
            )
        }
        crate::parser::QueryRef::WithCte(with) => describe_with_cte_statement(db, session, with),
        crate::parser::QueryRef::Other(Statement::Explain {
            target,
            format,
            analyze,
        }) => {
            let supported = matches!(
                crate::parser::QueryRef::from(target.as_ref()),
                crate::parser::QueryRef::Select(_)
                    | crate::parser::QueryRef::SetOperation(_)
                    | crate::parser::QueryRef::WithCte(_)
                    | crate::parser::QueryRef::Other(Statement::Insert(_))
                    | crate::parser::QueryRef::Other(Statement::Update(_))
                    | crate::parser::QueryRef::Other(Statement::Delete(_))
            );
            if !supported {
                return Ok(Vec::new());
            }
            let resolved = resolve_explain_format(*format, *analyze, session.explain_format);
            if matches!(
                resolved,
                crate::parser::ExplainFormat::Tree | crate::parser::ExplainFormat::Json
            ) {
                Ok(vec![Column {
                    source: Default::default(),
                    name: "EXPLAIN".to_string(),
                    column_type: ColumnType::VarString,
                }])
            } else if *analyze {
                Ok(explain::explain_analyze_result_columns())
            } else {
                Ok(planner::explain_result_columns())
            }
        }
        crate::parser::QueryRef::Other(Statement::Describe { table }) => {
            describe_show_columns(db, session, table, false)
        }
        crate::parser::QueryRef::Other(Statement::ShowDatabases { .. }) => {
            Ok(show_databases(db, session, None)?.columns())
        }
        crate::parser::QueryRef::Other(Statement::ShowTables { database, full, .. }) => {
            Ok(show_tables_columns(
                database.as_deref().unwrap_or(&session.current_database),
                *full,
            ))
        }
        crate::parser::QueryRef::Other(Statement::ShowTableStatus { .. }) => {
            Ok(show_table_status_columns())
        }
        crate::parser::QueryRef::Other(Statement::ShowColumns { table, full, .. }) => {
            describe_show_columns(db, session, table, *full)
        }
        crate::parser::QueryRef::Other(Statement::ShowIndex { table, .. }) => {
            describe_show_index(db, session, table)
        }
        crate::parser::QueryRef::Other(Statement::ShowCreateDatabase { .. }) => Ok(vec![
            Column {
                source: Default::default(),
                name: "Database".to_string(),
                column_type: ColumnType::VarString,
            },
            Column {
                source: Default::default(),
                name: "Create Database".to_string(),
                column_type: ColumnType::VarString,
            },
        ]),
        crate::parser::QueryRef::Other(Statement::ShowCreateTable { table }) => {
            describe_show_create_table(db, session, table)
        }
        crate::parser::QueryRef::Other(Statement::ShowCreateRoutine { kind, name }) => {
            Ok(crate::engine::show::show_create_routine(db, session, *kind, name)?.columns())
        }
        crate::parser::QueryRef::Other(Statement::ShowRoutineStatus { kind, filter }) => Ok(
            crate::engine::show::show_routine_status(db, session, *kind, filter.as_ref())?
                .columns(),
        ),
        crate::parser::QueryRef::Other(Statement::ShowCreateTrigger { name }) => {
            Ok(crate::engine::show::show_create_trigger(db, session, name)?.columns())
        }
        crate::parser::QueryRef::Other(Statement::ShowTriggers { database, filter }) => Ok(
            crate::engine::show::show_triggers(db, session, database.as_deref(), filter.as_ref())?
                .columns(),
        ),
        crate::parser::QueryRef::Other(Statement::ShowEvents { database, filter }) => Ok(
            crate::engine::show::show_events(db, session, database.as_deref(), filter.as_ref())?
                .columns(),
        ),
        crate::parser::QueryRef::Other(Statement::ShowCharset { .. }) => {
            Ok(show_charset_result(None)?.columns())
        }
        crate::parser::QueryRef::Other(Statement::ShowCollation { .. }) => {
            Ok(crate::engine::system::show_collation_result(None)?.columns())
        }
        crate::parser::QueryRef::Other(Statement::ShowVariables { .. }) => {
            Ok(crate::engine::system::show_variables_result(
                session,
                Some(db),
                crate::parser::ShowScope::Session,
                None,
            )?
            .columns())
        }
        crate::parser::QueryRef::Other(Statement::ShowStatus { .. }) => {
            Ok(crate::engine::system::status_result_columns())
        }
        crate::parser::QueryRef::Other(Statement::ShowDiagnostics { .. }) => {
            Ok(crate::engine::system::show_diagnostics_result(session, false, None).columns())
        }
        crate::parser::QueryRef::Other(Statement::ShowCountDiagnostics { errors_only }) => {
            Ok(crate::engine::system::show_count_result(
                if *errors_only {
                    "@@session.error_count"
                } else {
                    "@@session.warning_count"
                },
                0,
            )
            .columns())
        }
        crate::parser::QueryRef::Other(Statement::ShowEngines) => {
            Ok(show_engines_result().columns())
        }
        crate::parser::QueryRef::Other(Statement::ShowEngineMydbStatus) => {
            Ok(show_engine_mydb_status_columns())
        }
        crate::parser::QueryRef::Other(Statement::ShowMydbMemory) => {
            Ok(crate::engine::memory_status::summary_columns())
        }
        crate::parser::QueryRef::Other(Statement::ShowMydbMemorySessions) => {
            Ok(crate::engine::memory_status::session_columns())
        }
        crate::parser::QueryRef::Other(Statement::ShowEngine { .. }) => {
            Ok(show_engine_mydb_status_columns())
        }
        crate::parser::QueryRef::Other(Statement::ShowPlugins) => {
            Ok(show_plugins_result().columns())
        }
        crate::parser::QueryRef::Other(Statement::ShowOpenTables { database, filter }) => {
            Ok(crate::engine::show::show_open_tables(
                db,
                session,
                database.as_deref(),
                filter.as_ref(),
            )?
            .columns())
        }
        crate::parser::QueryRef::Other(Statement::ShowProcesslist { .. }) => {
            Ok(processlist_columns())
        }
        crate::parser::QueryRef::Other(Statement::ShowGrants)
        | crate::parser::QueryRef::Other(Statement::ShowGrantsFor { .. }) => Ok(vec![Column {
            source: Default::default(),
            name: "Grants".to_string(),
            column_type: ColumnType::VarString,
        }]),
        crate::parser::QueryRef::Other(Statement::ReplicationAdmin { sql }) => {
            Ok(crate::replication_sql::admin_result_columns(sql))
        }
        crate::parser::QueryRef::Other(Statement::BinlogAdmin { sql }) => {
            crate::binlog::admin_result_columns(sql)
        }
        crate::parser::QueryRef::Other(Statement::ShowPrivileges) => {
            Ok(show_privileges_result().columns())
        }
        _ => Ok(Vec::new()),
    }
}

fn describe_setop_columns(
    mut left: Vec<Column>,
    mut right: Vec<Column>,
) -> Result<Vec<Column>, QueryError> {
    if left.len() != right.len() {
        return Err(error(
            crate::error::catalog::ER_WRONG_NUMBER_OF_COLUMNS_IN_SELECT,
            "The used SELECT statements have a different number of columns",
        ));
    }
    super::dispatch::unify_setop_declared_column_metadata(&mut left, &mut right)?;
    Ok(left)
}

fn describe_with_cte_statement(
    db: &Database,
    session: &SessionContext,
    with: crate::parser::WithRef<'_>,
) -> Result<Vec<Column>, QueryError> {
    let stubs = describe_cte_definitions(db, session, &with.ctes)?;
    super::cte::with_cte_registry_extended(stubs, || {
        describe_statement_columns(db, session, &with.body)
    })
}

pub(super) fn describe_cte_definitions(
    db: &Database,
    session: &SessionContext,
    ctes: &[crate::parser::CteDef],
) -> Result<std::collections::HashMap<String, Arc<crate::engine::api::CteResult>>, QueryError> {
    super::cte::describe_definitions(ctes, |query| describe_statement_columns(db, session, query))
}

/// Describe the output columns of a FROM-clause derived-table subquery
/// without executing it. A set operation's result columns are taken from its
/// leftmost SELECT (MySQL/SQL semantics), so descend the left spine to the
/// first ordinary SELECT. Session-free, mirroring `describe_select_columns`.
pub(crate) fn describe_subquery_columns(
    db: &Database,
    sub: &SubQuery,
) -> Result<Vec<Column>, QueryError> {
    fn describe_query_statement<'q>(
        db: &Database,
        statement: impl Into<crate::parser::QueryRef<'q>>,
    ) -> Result<Vec<Column>, QueryError> {
        let statement = statement.into();
        match statement {
            crate::parser::QueryRef::Select(select) => describe_select_columns(db, select),
            crate::parser::QueryRef::Rrf(rrf) => hybrid::describe_columns(db, rrf),
            crate::parser::QueryRef::SetOperation(set) => {
                let left = describe_query_statement(db, &set.left)?;
                let right = describe_query_statement(db, &set.right)?;
                describe_setop_columns(left, right)
            }
            crate::parser::QueryRef::WithCte(with) => {
                let stubs = super::cte::describe_definitions(&with.ctes, |query| {
                    describe_query_statement(db, query)
                })?;
                super::cte::with_cte_registry_extended(stubs, || {
                    describe_query_statement(db, &with.body)
                })
            }
            _ => Err(error(
                crate::error::catalog::ER_NOT_SUPPORTED_YET,
                "cannot describe columns of derived query",
            )),
        }
    }

    match sub {
        SubQuery::Select(select) => describe_select_columns(db, select),
        stmt @ (SubQuery::SetOp(_) | SubQuery::Rrf(_) | SubQuery::WithCte(_)) => {
            describe_query_statement(db, stmt)
        }
    }
}

pub(super) fn describe_select_columns(
    db: &Database,
    stmt: &Select,
) -> Result<Vec<Column>, QueryError> {
    if stmt.values.is_some() {
        return describe_values_select_columns(stmt);
    }
    if let Some(source) = &stmt.json_table {
        return Ok(json_table_columns(source));
    }
    // Describing only the base derived relation loses every joined column.
    // Conversely, sending a JOIN-side derived relation straight to the planner
    // treats its placeholder name as a catalog table. Expose all independent
    // derived sources as metadata-only CTEs, then describe the complete join.
    // Internal identities keep sibling/nested SQL aliases from shadowing CTEs.
    if !stmt.joins.is_empty()
        && (stmt.derived.is_some() || stmt.joins.iter().any(|join| join.derived.is_some()))
        && !stmt
            .from
            .iter()
            .any(|table| table.name == "$nested_join_preserve")
        && !stmt.joins.iter().any(|join| {
            (join.lateral && join.derived.is_some()) || join.table.name == "$nested_join_preserve"
        })
    {
        let mut described = stmt.clone();
        let mut stubs = std::collections::HashMap::new();
        let mut describe_source = |table: &mut TableRef, subquery: &SubQuery| {
            let mut columns = describe_subquery_columns(db, subquery)?;
            crate::planner::apply_derived_column_aliases(
                table,
                columns.iter_mut().map(|column| &mut column.name),
            )?;
            let key = super::cte::next_inline_derived_name();
            table.alias.get_or_insert_with(|| table.name.clone());
            table.name = key.clone();
            stubs.insert(
                key,
                Arc::new(crate::engine::api::CteResult::materialized(
                    columns,
                    Vec::new(),
                )),
            );
            Ok::<(), QueryError>(())
        };
        if let Some(subquery) = described.derived.take() {
            if let Some(table) = described.from.first_mut() {
                describe_source(table, &subquery)?;
                described.table = Some(table.name.clone());
            }
        }
        for join in &mut described.joins {
            if let Some(subquery) = join.derived.take() {
                describe_source(&mut join.table, &subquery)?;
            }
        }
        return super::cte::with_cte_registry_extended(stubs, || {
            describe_select_columns(db, &described)
        });
    }
    if let Some(derived) = &stmt.derived {
        let mut inner_columns = describe_subquery_columns(db, derived)?;
        let table_ref = stmt.from.first().cloned().unwrap_or(TableRef {
            name: "derived".to_string(),
            alias: Some("derived".to_string()),
            system_time: None,
            index_hints: vec![],
            partitions: vec![],
            column_aliases: Vec::new(),
            generate_series: None,
        });
        crate::planner::apply_derived_column_aliases(
            &table_ref,
            inner_columns.iter_mut().map(|column| &mut column.name),
        )?;
        let plan = PhysicalPlan::CteScan {
            name: table_ref.name.clone(),
            alias: table_ref.alias.clone(),
            columns: inner_columns,
            filter: None,
            estimated_rows: 0.0,
            cost: crate::engine::cbo::Cost::ZERO,
            projected_columns: None,
        };
        return empty_result_columns(stmt, &plan);
    }
    if stmt.joins.is_empty() && stmt.from.len() == 1 {
        if let Some(table_ref) = stmt.from.first() {
            if let Some(plan) = crate::planner::try_plan_cte_scan(table_ref) {
                return empty_result_columns(stmt, &plan);
            }
        }
    }

    let Some(table_name) = stmt.table.as_deref() else {
        return describe_const_select_columns_with_db(Some(db), stmt);
    };
    if is_system_table_query(table_name) {
        let fields = system_table_fields(db, table_name)?;
        return system_result_columns(stmt, &fields);
    }
    if stmt.joins.is_empty() && stmt.from.len() == 1 {
        let table = db.table(table_name)?;
        return single_table_result_columns(stmt, &table, None);
    }
    // PREPARE needs binding and wire types, but no join-order search, index
    // costing or runtime topology. Plain inner joins expose the concatenated
    // base-table fields regardless of their eventual physical order.
    if stmt.group_by_modifier == crate::parser::GroupByModifier::None
        && stmt.grouping_sets.is_empty()
        && stmt.named_windows.is_empty()
        && stmt.joins.iter().all(|join| {
            matches!(
                join.join_type,
                crate::parser::JoinType::Inner | crate::parser::JoinType::Cross
            ) && join.derived.is_none()
                && !join.lateral
                && !join.natural
                && join.using_columns.is_empty()
        })
    {
        let sources = stmt
            .from
            .iter()
            .take(1)
            .chain(stmt.joins.iter().map(|join| &join.table));
        let mut fields = Vec::new();
        let mut supported = true;
        for source in sources {
            if source.system_time.is_some()
                || source.generate_series.is_some()
                || crate::engine::executor::cte::lookup_cte(&source.name).is_some()
                || is_system_table_query(&source.name)
            {
                supported = false;
                break;
            }
            let schema = db.table(&source.name)?;
            let table = crate::planner::PlanTable::new(schema.id, schema, source.alias.clone());
            for position in 0..table.schema.columns.len() {
                plan::push_table_fields(&mut fields, &table, Some(std::slice::from_ref(&position)));
            }
        }
        if supported {
            planner::validate_select_column_references(db, stmt)?;
            return plan::empty_result_columns_from_fields(stmt, fields);
        }
    }
    let planned = planner::plan_select(db, stmt)?;
    empty_result_columns(stmt, &planned.physical)
}

#[cfg(test)]
pub(super) fn describe_const_select_columns(stmt: &Select) -> Result<Vec<Column>, QueryError> {
    describe_const_select_columns_with_db(None, stmt)
}

pub(super) fn describe_const_select_columns_with_db(
    db: Option<&Database>,
    stmt: &Select,
) -> Result<Vec<Column>, QueryError> {
    stmt.projection
        .iter()
        .map(|item| {
            match item {
                SelectItem::Expr { expr, alias } => {
                    let no_column = |_: &Expr| None;
                    let no_placeholder = |_: usize| None;
                    let resolve_subquery = |select: &Select| {
                        let columns = describe_select_columns(db?, select).ok()?;
                        let column = columns.first()?;
                        Some(crate::engine::expr_type::ExprType::from_column_type(
                            column.column_type,
                            &column.source,
                        ))
                    };
                    let mut context = crate::engine::expr_type::TypeContext::context_free(
                        &no_column,
                        &no_placeholder,
                    );
                    context.resolve_subquery = &resolve_subquery;
                    let descriptor = crate::engine::expr_type::infer_expr_type(expr, &context);
                    let mut source = expression_column_source_for_expr(
                        crate::engine::expr::expr_result_unsigned(expr, &|_| None),
                        expr,
                        descriptor,
                    );
                    source.dynamic_parameter = descriptor.dynamic_parameter;
                    if !matches!(
                        descriptor.kind,
                        crate::engine::expr_type::ExprTypeKind::Unknown
                    ) {
                        descriptor.apply_to_source(&mut source);
                    }
                    Ok(Column {
                        source,
                        name: alias.clone().unwrap_or_else(|| expr_name(expr)),
                        column_type: if matches!(
                            descriptor.kind,
                            crate::engine::expr_type::ExprTypeKind::Unknown
                        ) {
                            const_projection_type(expr)
                        } else {
                            descriptor.column_type()
                        },
                    })
                }
                // A table-less wildcard can be an outer wildcard in a
                // correlated EXISTS. Its concrete width is bound per outer
                // row; one placeholder column is sufficient for describe.
                SelectItem::Wildcard => Ok(Column {
                    source: Default::default(),
                    name: "*".to_string(),
                    column_type: ColumnType::LongLong,
                }),
                SelectItem::QualifiedWildcard(qualifier) => Err(error(
                    crate::error::catalog::ER_BAD_FIELD_ERROR,
                    format!("Unknown table '{qualifier}'"),
                )),
            }
        })
        .collect()
}

fn const_projection_type(expr: &Expr) -> ColumnType {
    match expr {
        Expr::Function { name, args, .. } => {
            let normalized = crate::engine::sys_schema::NormalizedFuncName::new(name);
            match normalized.as_str() {
                // Keep the wire-level binary-string contract explicit at the
                // final projection boundary. These functions evaluate to
                // Value::String/Bytes internally, so value-shape inference
                // alone would incorrectly advertise VAR_STRING.
                "ST_ASTEXT" | "ST_ASWKT" | "ST_ASBINARY" | "ST_ASWKB" => ColumnType::Blob,
                _ => crate::engine::geo::sql::result_type(normalized.as_str(), args.len())
                    .unwrap_or_else(|| const_expr_type(expr)),
            }
        }
        _ => const_expr_type(expr),
    }
}

fn describe_values_select_columns(stmt: &Select) -> Result<Vec<Column>, QueryError> {
    let Some(values) = &stmt.values else {
        return Err(error(
            crate::error::catalog::ER_NOT_SUPPORTED_YET,
            "expected VALUES source",
        ));
    };
    Ok(values
        .columns
        .iter()
        .enumerate()
        .map(|(idx, name)| Column {
            source: Default::default(),
            name: name.clone(),
            column_type: values
                .rows
                .iter()
                .find_map(|row| row.get(idx))
                .map(const_expr_type)
                .unwrap_or_default(),
        })
        .collect())
}

pub(crate) fn json_table_columns(source: &JsonTableSource) -> Vec<Column> {
    let mut columns = Vec::new();
    push_json_table_columns(&source.columns, &mut columns);
    columns
}

fn push_json_table_columns(columns: &[JsonTableColumnDef], out: &mut Vec<Column>) {
    for column in columns {
        match column {
            JsonTableColumnDef::Ordinality { name } => out.push(Column {
                source: crate::types::ColumnSource {
                    flags: crate::mysql::protocol::UNSIGNED_FLAG,
                    ..Default::default()
                },
                name: name.clone(),
                column_type: ColumnType::Long,
            }),
            JsonTableColumnDef::Named {
                name,
                data_type,
                unsigned,
                exists,
                ..
            } => {
                let column_type = if *exists {
                    ColumnType::LongLong
                } else {
                    crate::storage::schema::DataTypeKind::from(data_type.clone()).column_type()
                };
                out.push(Column {
                    source: crate::types::ColumnSource {
                        flags: if *unsigned {
                            crate::mysql::protocol::UNSIGNED_FLAG
                        } else {
                            0
                        },
                        ..Default::default()
                    },
                    name: name.clone(),
                    column_type,
                });
            }
            JsonTableColumnDef::Nested { columns, .. } => push_json_table_columns(columns, out),
        }
    }
}

pub(super) fn json_table_doc_arg(value: &Value) -> Result<Vec<u8>, QueryError> {
    match value {
        Value::Json(bytes) => Ok(bytes.clone()),
        Value::Bytes(bytes) => {
            let text = std::str::from_utf8(bytes).map_err(|_| {
                crate::error::catalog::ER_INVALID_JSON_CHARSET_IN_FUNCTION
                    .error("Invalid encoding in argument 1 to function json_table.")
            })?;
            crate::engine::json::parse_text(text, 1, "json_table")
        }
        _ => crate::engine::json::parse_text(&value_to_string(value), 1, "json_table").map_err(
            |_| {
                error(
                    crate::error::catalog::ER_INVALID_JSON_TEXT_IN_PARAM,
                    "invalid JSON text in argument 1 to function json_table",
                )
            },
        ),
    }
}

pub(super) fn json_table_eval_path(doc: &[u8], path: &str) -> Result<Vec<Vec<u8>>, QueryError> {
    use crate::engine::json::JsonbRef;
    let root = JsonbRef::new(doc)?;
    let path = crate::engine::func_cache::cached_json_path(path)?;
    Ok(path
        .eval(root)
        .into_iter()
        .map(|node| node.to_owned_bytes())
        .collect())
}

pub(super) fn json_table_rows_for_columns(
    context: &[u8],
    columns: &[JsonTableColumnDef],
    ordinality: u64,
) -> Result<Vec<Vec<Value>>, QueryError> {
    let total_width = columns.iter().map(json_table_column_width).sum();
    let mut base = Vec::with_capacity(total_width);
    let mut nested = Vec::new();
    for column in columns {
        match column {
            JsonTableColumnDef::Ordinality { .. } => {
                base.push(Value::UInt(ordinality));
            }
            JsonTableColumnDef::Named {
                data_type,
                unsigned,
                path,
                exists,
                on_empty,
                on_error,
                ..
            } => {
                let value = json_table_column_value(
                    context, data_type, *unsigned, path, *exists, on_empty, on_error,
                )?;
                base.push(value);
            }
            JsonTableColumnDef::Nested {
                path,
                columns: nested_columns,
            } => {
                let offset = base.len();
                let width: usize = nested_columns.iter().map(json_table_column_width).sum();
                base.resize(offset + width, Value::Null);
                nested.push((offset, width, path, nested_columns));
            }
        }
    }
    if nested.is_empty() {
        return Ok(vec![base]);
    }

    // MySQL treats sibling NESTED PATH clauses as a union (sum of rows),
    // with every inactive sibling's columns NULL-complemented. Sequentially
    // multiplying child row sets produces an incorrect Cartesian product.
    let mut rows = Vec::new();
    for (offset, width, path, nested_columns) in nested {
        let matches = json_table_eval_path(context, path)?;
        let child_rows = if matches.is_empty() {
            Vec::new()
        } else {
            let mut out = Vec::new();
            for (idx, child) in matches.iter().enumerate() {
                out.extend(json_table_rows_for_columns(
                    child,
                    nested_columns,
                    (idx + 1) as u64,
                )?);
            }
            out
        };
        for child in child_rows {
            debug_assert_eq!(child.len(), width);
            let mut row = base.clone();
            row[offset..offset + width].clone_from_slice(&child);
            rows.push(row);
        }
    }
    if rows.is_empty() {
        rows.push(base);
    }
    Ok(rows)
}

fn json_table_column_width(column: &JsonTableColumnDef) -> usize {
    match column {
        JsonTableColumnDef::Ordinality { .. } | JsonTableColumnDef::Named { .. } => 1,
        JsonTableColumnDef::Nested { columns, .. } => {
            columns.iter().map(json_table_column_width).sum()
        }
    }
}

fn json_table_column_value(
    context: &[u8],
    data_type: &crate::parser::DataType,
    unsigned: bool,
    path: &str,
    exists: bool,
    on_empty: &JsonTableOnResponse,
    on_error: &JsonTableOnResponse,
) -> Result<Value, QueryError> {
    let matches = json_table_eval_path(context, path)?;
    if exists {
        return Ok(Value::Int((!matches.is_empty()) as i64));
    }
    let Some(first) = matches.first() else {
        return json_table_on_response(on_empty, data_type, unsigned, true, None);
    };
    if matches.len() != 1 {
        return json_table_on_response(on_error, data_type, unsigned, false, None);
    }
    match json_table_jsonb_to_value(first, data_type, unsigned) {
        Ok(value) => Ok(value),
        Err(conversion_error) => {
            json_table_on_response(on_error, data_type, unsigned, false, Some(conversion_error))
        }
    }
}

fn json_table_on_response(
    response: &JsonTableOnResponse,
    data_type: &crate::parser::DataType,
    unsigned: bool,
    empty: bool,
    conversion_error: Option<QueryError>,
) -> Result<Value, QueryError> {
    match response {
        JsonTableOnResponse::Null => Ok(Value::Null),
        JsonTableOnResponse::Default(expr) => {
            let value = crate::engine::expr::eval_const(expr)?;
            // DEFAULT is a JSON string literal in MySQL's grammar, not an SQL
            // scalar to be wrapped. Parse it before applying column coercion.
            let jsonb = match &value {
                Value::String(s) | Value::Decimal(s) => {
                    crate::engine::json::parse_text(s, 1, "json_table").unwrap_or_else(|_| {
                        crate::engine::expr::value_to_jsonb_typed(&value, false)
                    })
                }
                _ => crate::engine::expr::value_to_jsonb_typed(&value, false),
            };
            json_table_jsonb_to_value(&jsonb, data_type, unsigned)
        }
        JsonTableOnResponse::Error => Err(if let Some(err) = conversion_error {
            err
        } else if empty {
            crate::error::catalog::ER_MISSING_JSON_TABLE_VALUE
                .error("Missing value for JSON_TABLE column")
        } else {
            crate::error::catalog::ER_WRONG_JSON_TABLE_VALUE
                .error("Can't store an array or an object in the scalar JSON_TABLE column")
        }),
    }
}

fn json_table_jsonb_to_value(
    jsonb: &[u8],
    data_type: &crate::parser::DataType,
    unsigned: bool,
) -> Result<Value, QueryError> {
    use crate::engine::json::{JsonType, JsonbRef};
    use crate::parser::DataType;
    let node = JsonbRef::new(jsonb)?;
    if matches!(data_type, DataType::Json) {
        return Ok(Value::Json(jsonb.to_vec()));
    }
    let scalar = match node.json_type() {
        JsonType::Null => return Ok(Value::Null),
        JsonType::Boolean => Value::Int(i64::from(node.as_bool().unwrap_or(false))),
        JsonType::Integer => Value::Int(node.as_i64().unwrap_or_default()),
        JsonType::UnsignedInteger => Value::UInt(
            node.number_text()
                .and_then(|value| value.parse().ok())
                .unwrap_or_default(),
        ),
        JsonType::Double => Value::Double(node.as_f64().unwrap_or_default()),
        JsonType::Decimal => Value::Decimal(node.number_text().unwrap_or_default()),
        JsonType::String
        | JsonType::Date
        | JsonType::Time
        | JsonType::Datetime
        | JsonType::Timestamp => Value::String(node.as_str().unwrap_or_default().to_string()),
        JsonType::Opaque => Value::Bytes(
            node.opaque_value()
                .map(|(_, data)| data.to_vec())
                .unwrap_or_default(),
        ),
        JsonType::Array | JsonType::Object => {
            return Err(crate::error::catalog::ER_WRONG_JSON_TABLE_VALUE
                .error("Can't store an array or an object in the scalar JSON_TABLE column"));
        }
    };
    if let Value::String(text) = &scalar {
        if matches!(
            data_type,
            DataType::TinyInt
                | DataType::SmallInt
                | DataType::MediumInt
                | DataType::Int
                | DataType::BigInt
                | DataType::Bool
                | DataType::Year
                | DataType::Float(_)
                | DataType::Double(_)
                | DataType::Decimal(_)
        ) && crate::util::decimal::parse_exact_decimal_big(text.trim()).is_none()
            && text
                .trim()
                .parse::<f64>()
                .ok()
                .filter(|v| v.is_finite())
                .is_none()
        {
            return Err(json_table_out_of_range());
        }
    }
    match data_type {
        DataType::TinyInt
        | DataType::SmallInt
        | DataType::MediumInt
        | DataType::Int
        | DataType::BigInt => json_table_integer_to_value(&scalar, data_type, unsigned),
        DataType::Bool => crate::engine::expr::cast_to_integer(&scalar, false),
        DataType::Year => {
            let year = match crate::engine::expr::cast_to_integer(&scalar, false)? {
                Value::Int(year) => year,
                _ => unreachable!("signed integer cast returned a non-integer"),
            };
            let year = match year {
                0 => 0,
                1..=69 => year + 2000,
                70..=99 => year + 1900,
                year => year,
            };
            if year != 0 && !(1901..=2155).contains(&year) {
                return Err(json_table_out_of_range());
            }
            Ok(Value::Year(if year == 0 { 0 } else { (year - 1900) as u8 }))
        }
        DataType::Float(_) | DataType::Double(_) => {
            crate::engine::expr::numeric(&scalar).map(Value::Double)
        }
        DataType::Decimal(spec) => {
            let (precision, scale) = spec.unwrap_or((10, 0));
            json_table_decimal_to_value(&scalar, precision, scale)
        }
        DataType::Date => {
            json_table_require_conversion(crate::engine::expr::cast_value_to_date(&scalar))
        }
        DataType::Time { fsp } => {
            json_table_require_conversion(crate::engine::expr::value_to_time_string(&scalar, *fsp))
        }
        DataType::DateTime { fsp } | DataType::Timestamp { fsp } => json_table_require_conversion(
            crate::engine::expr::cast_value_to_datetime(&scalar, *fsp),
        ),
        DataType::Char(_)
        | DataType::VarChar(_)
        | DataType::Text(_)
        | DataType::Enum(_)
        | DataType::Set(_) => Ok(Value::String(crate::engine::expr::string_value(&scalar))),
        DataType::Binary(_) | DataType::VarBinary(_) | DataType::Blob(_) => Ok(Value::Bytes(
            crate::engine::expr::string_value(&scalar).into_bytes(),
        )),
        _ => {
            let mut text = String::new();
            node.write_text(&mut text);
            Ok(Value::String(text))
        }
    }
}

fn json_table_integer_to_value(
    scalar: &Value,
    data_type: &crate::parser::DataType,
    unsigned: bool,
) -> Result<Value, QueryError> {
    use crate::parser::DataType;
    let (signed_min, unsigned_max) = match data_type {
        DataType::TinyInt => (-128_i64, 255_u64),
        DataType::SmallInt => (-32_768, 65_535),
        DataType::MediumInt => (-8_388_608, 16_777_215),
        DataType::Int => (i64::from(i32::MIN), u64::from(u32::MAX)),
        DataType::BigInt => (i64::MIN, u64::MAX),
        _ => unreachable!("non-integer JSON_TABLE type"),
    };
    if let Some((magnitude, scale)) =
        crate::util::decimal::parse_exact_decimal_big(&crate::engine::expr::string_value(scalar))
    {
        let rounded = crate::util::decimal::rescale_exact(
            crate::util::decimal::ExactDecimal::from_big(magnitude, scale),
            0,
            true,
        );
        let (rounded, _) = rounded.into_big_parts();
        let in_range = if unsigned {
            rounded >= num_bigint::BigInt::from(0_u8)
                && rounded <= num_bigint::BigInt::from(unsigned_max)
        } else {
            let signed_max = if matches!(data_type, DataType::BigInt) {
                i64::MAX
            } else {
                i64::try_from(unsigned_max / 2).unwrap_or(i64::MAX)
            };
            rounded >= num_bigint::BigInt::from(signed_min)
                && rounded <= num_bigint::BigInt::from(signed_max)
        };
        if !in_range {
            return Err(json_table_out_of_range());
        }
    }
    crate::engine::expr::cast_to_integer(scalar, unsigned).map_err(|_| json_table_out_of_range())
}

fn json_table_decimal_to_value(
    scalar: &Value,
    precision: u8,
    scale: u8,
) -> Result<Value, QueryError> {
    let text = crate::engine::expr::string_value(scalar);
    if let Some((magnitude, source_scale)) = crate::util::decimal::parse_exact_decimal_big(&text) {
        let rounded = crate::util::decimal::rescale_exact(
            crate::util::decimal::ExactDecimal::from_big(magnitude, source_scale),
            scale,
            true,
        );
        let (magnitude, _) = rounded.into_big_parts();
        let digits = magnitude
            .to_str_radix(10)
            .trim_start_matches('-')
            .trim_start_matches('0')
            .len();
        if digits > usize::from(precision) {
            return Err(json_table_out_of_range());
        }
    }
    crate::engine::expr::cast_to_decimal(scalar, precision, scale)
}

fn json_table_require_conversion(result: Result<Value, QueryError>) -> Result<Value, QueryError> {
    match result? {
        Value::Null => Err(crate::error::catalog::ER_WRONG_JSON_TABLE_VALUE
            .error("Invalid value for JSON_TABLE column conversion")),
        value => Ok(value),
    }
}

fn json_table_out_of_range() -> QueryError {
    crate::error::catalog::ER_JT_VALUE_OUT_OF_RANGE.error("Out of range JSON_TABLE value")
}

/// Bridge inline derived tables sitting in JOIN positions into CTE registry
/// entries, then re-dispatch through the standard planner.
///
/// [`execute_derived_select`] already does this for a derived table in
/// `FROM[0]`; this is the mirror for the comma / JOIN positions (`FROM[1..]`).
/// Without it, any query whose join list contains an inline `(SELECT ...)`
/// routes to the in-memory nested-loop path ([`execute_select_with_cte_joins`]),
/// which fully materialises every base table and cannot use indexes or CBO
/// join ordering — pathological when the derived table is comma-joined against
/// a large fact table (TPC-DS Q8: 3.7 s vs ~0.12 s for the same query written
/// with a `WITH` CTE, which already takes the planner path).
///
/// Each derived body is materialised once via `execute_subquery_response`
/// (handling both ordinary SELECTs and set operations such as Q8's INTERSECT,
/// and spilling while its cursor is consumed), registered under its FROM alias,
/// and the join target rewritten to reference that alias. The re-dispatched
/// SELECT then sees CTE references, which the planner lowers to `CteScan`
/// leaves that join via hash / index-nested-loop operators under CBO.
///
/// The caller guarantees no derived join is LATERAL (and no anonymous nested
/// join scope must be preserved), so every derived join here is safe to
/// pre-materialise independently. FULL and GenerateSeries inputs remain
/// ordinary physical join leaves after this bridge.
fn bridge_join_derived_to_ctes(
    db: &Arc<Database>,
    txn: TxnView,
    mut stmt: Select,
    registry: &SessionRegistry,
) -> Result<QueryResponse, QueryError> {
    let mut additional = std::collections::HashMap::new();
    for join in stmt.joins.iter_mut() {
        let Some(derived) = join.derived.take() else {
            continue;
        };
        let mut inner = crate::engine::executor::dispatch::execute_subquery_response(
            db, txn, &derived, registry,
        )?;
        rename_derived_response(&join.table, &mut inner)?;
        let alias = join
            .table
            .alias
            .clone()
            .unwrap_or_else(|| join.table.name.clone());
        let cte_result = std::sync::Arc::new(materialize_response_with_spill(
            inner,
            db.data_dir().to_path_buf(),
        )?);
        // Rewrite the join target so the planner's case-insensitive CTE lookup
        // (`try_plan_cte_scan`) resolves it to the registered rows. The alias
        // is preserved so `alias.col` references in the outer query still bind.
        join.table.name = alias.clone();
        join.table.alias = Some(alias.clone());
        additional.insert(alias.to_ascii_lowercase(), cte_result);
    }
    // Extend (not replace) the active registry so an enclosing `WITH` stays
    // visible to the re-dispatched body and to sibling join sources.
    super::cte::with_cte_registry_extended(additional, || execute_select(db, txn, stmt, registry))
}

/// Materialise independent scalar derived sources concurrently, bounded by
/// the statement's configured parallel degree. A comma product of scalar
/// aggregates has no data dependency between its inputs; running them one by
/// one wastes the same worker budget eight times over. The completed one-row
/// spools are registered in source order and the owner is re-planned over
/// ordinary CteScan leaves.
///
/// Eligibility is deliberately structural and general: at least two
/// non-LATERAL ordinary SELECT sources, each an implicit aggregate guaranteed
/// to emit at most one row, with no volatile/uncacheable expression. Large
/// grouped/set-operation derived relations retain the serial spill-aware path.
fn try_bridge_scalar_derived_parallel(
    db: &Arc<Database>,
    txn: TxnView,
    stmt: &mut Select,
    registry: &SessionRegistry,
) -> Result<Option<QueryResponse>, QueryError> {
    if stmt
        .from
        .iter()
        .chain(stmt.joins.iter().map(|join| &join.table))
        .any(|table| !table.column_aliases.is_empty())
    {
        return Ok(None);
    }
    fn enabled() -> bool {
        crate::util::env::var("MYDB_SCALAR_DERIVED_PARALLEL")
            .map(|value| {
                !matches!(
                    value.trim().to_ascii_lowercase().as_str(),
                    "0" | "off" | "false" | "no"
                )
            })
            .unwrap_or(true)
    }

    fn scalar_aggregate(subquery: &crate::parser::SubQuery) -> bool {
        let Some(select) = subquery.as_select() else {
            return false;
        };
        select.group_by.is_empty()
            && select.grouping_sets.is_empty()
            && select.group_by_modifier == crate::parser::GroupByModifier::None
            && select_projection_has_toplevel_aggregate(&select.projection)
            && !crate::engine::executor::dispatch::select_has_window_functions(select)
            && !self::explain::select_is_uncacheable(select)
            && !select.for_update
            && select.lock_clause.is_none()
    }

    // Observability must not change the execution topology. ExecutionContext
    // already propagates the active collector into scoped workers, so ANALYZE
    // uses the same scalar-derived parallel bridge as an ordinary SELECT.
    if !enabled() {
        return Ok(None);
    }
    let primary_eligible = stmt.derived.as_ref().is_some_and(scalar_aggregate);
    let eligible_join_count = stmt
        .joins
        .iter()
        .filter(|join| join.derived.as_ref().is_some_and(scalar_aggregate))
        .count();
    let total = usize::from(primary_eligible) + eligible_join_count;
    if total < 2
        || stmt
            .derived
            .as_ref()
            .is_some_and(|derived| !scalar_aggregate(derived))
        || stmt.joins.iter().any(|join| {
            join.derived
                .as_ref()
                .is_some_and(|derived| join.lateral || !scalar_aggregate(derived))
                || join.table.name == "$nested_join_preserve"
        })
    {
        return Ok(None);
    }
    let parallelism = crate::engine::executor::tuning::current_or_capture()
        .scheduler_parallelism
        .min(total);
    if parallelism < 2 {
        return Ok(None);
    }

    #[derive(Clone)]
    struct Job {
        index: usize,
        alias: String,
        derived: crate::parser::SubQuery,
    }

    let mut jobs = Vec::with_capacity(total);
    if let Some(derived) = stmt.derived.take() {
        let alias = stmt
            .from
            .first()
            .map(|table| table.alias.clone().unwrap_or_else(|| table.name.clone()))
            .unwrap_or_else(|| "derived".to_string());
        if let Some(table) = stmt.from.first_mut() {
            table.name = alias.clone();
            table.alias = Some(alias.clone());
        }
        jobs.push(Job {
            index: jobs.len(),
            alias,
            derived,
        });
    }
    for join in &mut stmt.joins {
        let Some(derived) = join.derived.take() else {
            continue;
        };
        let alias = join
            .table
            .alias
            .clone()
            .unwrap_or_else(|| join.table.name.clone());
        join.table.name = alias.clone();
        join.table.alias = Some(alias.clone());
        jobs.push(Job {
            index: jobs.len(),
            alias,
            derived,
        });
    }

    // The outer pool already consumes the statement's full parallel budget.
    // Force each independent scalar job to one inner scheduler worker so the
    // two levels never multiply MYDB_PARALLEL_DEGREE.
    let context =
        self::execution_context::ExecutionContext::capture(txn).with_scheduler_parallelism(1);
    let queue = std::sync::Mutex::new(jobs.into_iter().collect::<std::collections::VecDeque<_>>());
    let slots = std::sync::Mutex::new(
        (0..total)
            .map(|_| None)
            .collect::<Vec<Option<(String, std::sync::Arc<crate::engine::api::CteResult>)>>>(),
    );
    let abort = std::sync::atomic::AtomicBool::new(false);
    std::thread::scope(|scope| -> Result<(), QueryError> {
        let mut handles = Vec::with_capacity(parallelism);
        for _ in 0..parallelism {
            let db = Arc::clone(db);
            let context = context.clone();
            let queue = &queue;
            let slots = &slots;
            let abort = &abort;
            handles.push(scope.spawn(move || -> Result<(), QueryError> {
                loop {
                    if abort.load(std::sync::atomic::Ordering::Acquire) {
                        return Ok(());
                    }
                    let job = queue.lock().expect("scalar derived queue lock").pop_front();
                    let Some(job) = job else {
                        return Ok(());
                    };
                    let result = context.run(&db, || {
                        let response =
                            crate::engine::executor::dispatch::execute_subquery_response(
                                &db,
                                txn,
                                &job.derived,
                                registry,
                            )?;
                        materialize_response_with_spill(response, db.data_dir().to_path_buf())
                    });
                    match result {
                        Ok(result) => {
                            slots.lock().expect("scalar derived slots lock")[job.index] =
                                Some((job.alias, std::sync::Arc::new(result)));
                        }
                        Err(error) => {
                            abort.store(true, std::sync::atomic::Ordering::Release);
                            return Err(error);
                        }
                    }
                }
            }));
        }
        let mut first_error = None;
        for handle in handles {
            match handle.join() {
                Ok(Ok(())) => {}
                Ok(Err(worker_error)) => {
                    first_error.get_or_insert(worker_error);
                }
                Err(_) => {
                    first_error.get_or_insert_with(|| {
                        error(
                            crate::error::catalog::ER_UNKNOWN_ERROR,
                            "parallel scalar derived worker panicked",
                        )
                    });
                }
            };
        }
        match first_error {
            Some(error) => Err(error),
            None => Ok(()),
        }
    })?;

    let mut additional = std::collections::HashMap::with_capacity(total);
    for slot in slots.into_inner().expect("scalar derived slots poisoned") {
        let (alias, result) = slot.expect("every scalar derived slot filled on success");
        additional.insert(alias.to_ascii_lowercase(), result);
    }
    SCALAR_DERIVED_PARALLEL_HITS.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let owner = stmt.clone();
    Ok(Some(super::cte::with_cte_registry_extended(
        additional,
        || execute_select(db, txn, owner, registry),
    )?))
}

pub(crate) static SCALAR_DERIVED_PARALLEL_HITS: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);

fn annotate_derived_column_collations(derived: &SubQuery, columns: &mut [Column]) {
    let Some(select) = derived.leftmost_select() else {
        return;
    };
    for (column, item) in columns.iter_mut().zip(select.projection.iter()) {
        if column.source.collation_id.is_none() {
            if let SelectItem::Expr { expr, .. } = item {
                column.source.collation_id =
                    crate::engine::expr::expression_metadata_collation_id(expr);
            }
        }
    }
}

/// Materialise a (possibly streaming) derived / set-operation response into
/// a `CteResult`, spilling past the in-memory threshold. A streaming
/// `UNION ALL` body feeds its chained branch cursors straight into the
/// builder, so the full intermediate row set is never built — the win for
/// TPC-DS Q2's `UNION ALL` of fact tables that then joins `date_dim`.
pub(super) fn materialize_response_with_spill(
    resp: QueryResponse,
    data_dir: std::path::PathBuf,
) -> Result<crate::engine::api::CteResult, QueryError> {
    let (columns, mut cursor) = crate::engine::executor::dispatch::response_into_cursor(resp)?;
    let mut builder = super::cte::CteSpillBuilder::new(columns, data_dir);
    builder.consume_cursor(cursor.as_mut())?;
    builder.finish()
}

/// Detect an enclosing window Top-N filter `rk <= k` / `rk < k` (or the
/// reversed `k >= rk` / `k > rk`) among the top-level `AND` conjuncts of a
/// derived-table WHERE clause. Returns `(column, k)` where `k` is the inclusive
/// rank bound. Only descends through `AND` (an `OR` could require rows beyond
/// the bound, so it is not pushed). The returned column is validated against
/// the actual window alias downstream, so a false match is harmless.
pub(super) fn extract_window_topk_filter(filter: &Expr) -> Option<(String, usize)> {
    use crate::parser::{BinaryOp, Literal};
    fn ident_name(e: &Expr) -> Option<String> {
        match e {
            Expr::Identifier(n) => Some(n.clone()),
            Expr::CompoundIdentifier(parts) => parts.last().cloned(),
            _ => None,
        }
    }
    fn value_to_i64(v: &crate::types::Value) -> Option<i64> {
        match v {
            crate::types::Value::Int(x) => Some(*x),
            crate::types::Value::UInt(x) => i64::try_from(*x).ok(),
            _ => None,
        }
    }
    fn int_lit(e: &Expr) -> Option<i64> {
        match e {
            Expr::Literal(Literal::Int(v)) => Some(*v),
            Expr::Literal(Literal::UInt(v)) => i64::try_from(*v).ok(),
            // Auto-parameterization (MYDB_AUTO_PARAMETERIZE) folds the WHERE
            // literal into a placeholder; resolve it against the params bound
            // for this execution.
            Expr::Placeholder(idx) => crate::engine::params::current_param(*idx)
                .as_ref()
                .and_then(value_to_i64),
            Expr::BoundValue(v) => value_to_i64(v),
            _ => None,
        }
    }
    match filter {
        Expr::Binary {
            left,
            op: BinaryOp::And,
            right,
        } => extract_window_topk_filter(left).or_else(|| extract_window_topk_filter(right)),
        Expr::Binary { left, op, right } => {
            if let (Some(col), Some(k)) = (ident_name(left), int_lit(right)) {
                match op {
                    BinaryOp::LtEq if k >= 0 => return Some((col, k as usize)),
                    BinaryOp::Lt => return Some((col, k.saturating_sub(1).max(0) as usize)),
                    _ => {}
                }
            }
            if let (Some(k), Some(col)) = (int_lit(left), ident_name(right)) {
                match op {
                    BinaryOp::GtEq if k >= 0 => return Some((col, k as usize)),
                    BinaryOp::Gt => return Some((col, k.saturating_sub(1).max(0) as usize)),
                    _ => {}
                }
            }
            None
        }
        _ => None,
    }
}

fn rename_derived_response(
    table: &TableRef,
    response: &mut QueryResponse,
) -> Result<(), QueryError> {
    if table.column_aliases.is_empty() {
        return Ok(());
    }
    let columns = match response {
        QueryResponse::ResultSet(result) => result.columns.as_mut_slice(),
        QueryResponse::SharedResultSet(result) => Arc::make_mut(&mut result.columns),
        QueryResponse::StreamingResultSet(result) => result.columns.as_mut_slice(),
        _ => {
            return Err(crate::error::catalog::ER_INTERNAL_ERROR
                .error("derived query did not return columns"))
        }
    };
    crate::planner::apply_derived_column_aliases(
        table,
        columns.iter_mut().map(|column| &mut column.name),
    )
}

fn execute_derived_select(
    db: &Arc<Database>,
    txn: TxnView,
    mut stmt: Select,
    derived_frequency_spec: Option<DerivedCountFrequencySpec>,
    registry: &SessionRegistry,
) -> Result<QueryResponse, QueryError> {
    let _rrf_window = hybrid::window_for_select(db, &stmt)?;
    let Some(derived) = stmt.derived.take() else {
        unreachable!();
    };
    let alias = stmt
        .from
        .first()
        .map(|table| table.alias.clone().unwrap_or_else(|| table.name.clone()))
        .unwrap_or_else(|| "derived".to_string());

    // A derived relation is an explicit pipeline boundary. Consume its batch
    // cursor directly into the bounded CTE spool, then expose the spool as a
    // regular CteScan leaf and let the common planner execute every outer
    // Filter/Aggregate/Sort/Limit/Projection shape. The former implementation
    // rebuilt all of those operators over a fully materialised Vec<PlanRow>.
    let mut inner_response =
        crate::engine::executor::cte::with_cte_materialization_active(true, || {
            let _streaming_window = matches!(&derived, crate::parser::SubQuery::Select(select)
            if crate::engine::executor::dispatch::select_has_window_functions(select))
            .then(force_streaming_response_scope);
            // Keep rank Top-K pushdown local to the derived window body. The outer
            // predicate is still evaluated by the common pipeline, so a hint that
            // does not match the inner window alias cannot change results.
            let rank_source = matches!(&derived, crate::parser::SubQuery::Select(inner)
            if inner.limit.is_none() && inner.offset.is_none()
                && crate::engine::executor::dispatch::select_has_window_functions(inner));
            let _topk_guard = (rank_source && stmt.joins.is_empty() && stmt.from.len() == 1)
                .then(|| stmt.selection.as_ref().and_then(extract_window_topk_filter))
                .flatten()
                .map(|hint| crate::engine::executor::dispatch::set_window_topk_hint(Some(hint)));
            crate::engine::executor::dispatch::execute_subquery_response(
                db, txn, &derived, registry,
            )
        })?;
    if let Some(table) = stmt.from.first() {
        rename_derived_response(table, &mut inner_response)?;
    }
    if let Some(spec) = derived_frequency_spec {
        let (columns, mut cursor) =
            crate::engine::executor::dispatch::response_into_cursor(inner_response)?;
        let group_column = columns
            .iter()
            .position(|column| column.name.eq_ignore_ascii_case(&spec.inner_count_name))
            .ok_or_else(|| {
                crate::storage::error(
                    crate::error::catalog::ER_BAD_FIELD_ERROR,
                    format!("unknown derived COUNT column '{}'", spec.inner_count_name),
                )
            })?;
        let mut frequencies = std::collections::BTreeMap::<i64, u64>::new();
        // `BTreeMap` does not expose node capacity. Charge a conservative
        // whole-node share before every distinct entry is retained; the
        // reservation lives until the frequency table has been converted to
        // the bounded derived result below.
        const FREQUENCY_ENTRY_RESERVATION: usize = 128;
        let mut frequency_charge = crate::engine::executor::mem_budget::ScopedCharge::new(0);
        while let Some(row) = cursor.next_row()? {
            let count = match row.get(group_column) {
                Some(Value::Int(value)) if *value >= 0 => *value,
                Some(Value::UInt(value)) => i64::try_from(*value).map_err(|_| {
                    crate::storage::error(
                        crate::error::catalog::ER_DATA_OUT_OF_RANGE,
                        "derived COUNT value exceeds signed BIGINT",
                    )
                })?,
                _ => {
                    return Err(crate::storage::error(
                        crate::error::catalog::ER_INTERNAL_ERROR,
                        "derived COUNT frequency input is not a non-negative integer",
                    ));
                }
            };
            let frequency = match frequencies.entry(count) {
                std::collections::btree_map::Entry::Vacant(entry) => {
                    frequency_charge.add(FREQUENCY_ENTRY_RESERVATION)?;
                    entry.insert(0)
                }
                std::collections::btree_map::Entry::Occupied(entry) => entry.into_mut(),
            };
            *frequency = frequency.checked_add(1).ok_or_else(|| {
                crate::storage::error(
                    crate::error::catalog::ER_DATA_OUT_OF_RANGE,
                    "COUNT result exceeds BIGINT",
                )
            })?;
        }

        let mut rows = Vec::with_capacity(frequencies.len());
        for (count, frequency) in frequencies {
            let frequency = i64::try_from(frequency).map_err(|_| {
                crate::storage::error(
                    crate::error::catalog::ER_DATA_OUT_OF_RANGE,
                    "COUNT result exceeds signed BIGINT",
                )
            })?;
            let values = spec
                .outputs
                .iter()
                .map(|output| match output.kind {
                    DerivedFrequencyOutputKind::InnerCount => Value::Int(count),
                    DerivedFrequencyOutputKind::Frequency => Value::Int(frequency),
                })
                .collect();
            rows.push(values);
        }
        let result_columns = spec
            .outputs
            .iter()
            .map(|output| crate::types::Column {
                name: output.name.clone(),
                column_type: ColumnType::LongLong,
                source: expression_column_source(true),
            })
            .collect();
        let cte_result =
            std::sync::Arc::new(crate::engine::executor::cte::materialize_rows_with_spill(
                result_columns,
                rows,
                db.data_dir().to_path_buf(),
            )?);
        for order in &mut stmt.order_by {
            if let Some(position) = stmt.projection.iter().position(
                |item| matches!(item, SelectItem::Expr { expr, .. } if expr_semantically_eq(expr, &order.expr)),
            ) {
                order.expr = Expr::Identifier(spec.outputs[position].name.clone());
            }
        }
        for (item, output) in stmt.projection.iter_mut().zip(&spec.outputs) {
            let SelectItem::Expr { expr, .. } = item else {
                unreachable!("derived frequency shape checked");
            };
            *expr = Expr::Identifier(output.name.clone());
        }
        stmt.group_by.clear();
        if let Some(table) = stmt.from.first_mut() {
            table.name = alias.clone();
            table.alias = Some(alias.clone());
        }
        stmt.table = Some(alias.clone());
        let mut additional = std::collections::HashMap::new();
        additional.insert(alias.to_ascii_lowercase(), cte_result);
        return super::cte::with_cte_registry_extended(additional, || {
            execute_select(db, txn, stmt, registry)
        });
    }
    let cte_result = std::sync::Arc::new(materialize_response_with_spill(
        inner_response,
        db.data_dir().to_path_buf(),
    )?);

    if let Some(table) = stmt.from.first_mut() {
        table.name = alias.clone();
        table.alias = Some(alias.clone());
    } else {
        // Expression-subquery set operations are normalised by the parser to
        // a synthetic `SELECT * FROM (<setop>)` whose derived payload has no
        // concrete TableRef. Give the materialised relation an explicit leaf
        // so the re-dispatch cannot mistake the wildcard query for FROM-less.
        stmt.from.push(TableRef {
            name: alias.clone(),
            alias: Some(alias.clone()),
            system_time: None,
            index_hints: Vec::new(),
            partitions: Vec::new(),
            column_aliases: Vec::new(),
            generate_series: None,
        });
    }
    stmt.table = Some(alias.clone());
    let mut additional = std::collections::HashMap::new();
    additional.insert(alias.to_ascii_lowercase(), cte_result);
    super::cte::with_cte_registry_extended(additional, || execute_select(db, txn, stmt, registry))
}

#[derive(Clone, Copy)]
enum DerivedFrequencyOutputKind {
    InnerCount,
    Frequency,
}

struct DerivedFrequencyOutput {
    name: String,
    kind: DerivedFrequencyOutputKind,
}

struct DerivedCountFrequencySpec {
    inner_count_name: String,
    outputs: Vec<DerivedFrequencyOutput>,
}

fn unqualified_expr_name(expr: &Expr) -> Option<&str> {
    match expr {
        Expr::Identifier(name) => Some(name),
        Expr::CompoundIdentifier(parts) => parts.last().map(String::as_str),
        _ => None,
    }
}

/// Algebraic collapse for `GROUP BY <inner COUNT>, COUNT(*)` over one derived
/// relation. Inner groups are disjoint, so the number of distinct count values
/// is bounded by O(sqrt(input rows)); retaining only their frequency table is
/// both exact and much smaller than materializing every inner group row.
fn derived_count_frequency_spec(
    outer: &Select,
    derived: &crate::parser::SubQuery,
) -> Option<DerivedCountFrequencySpec> {
    if outer.selection.is_some()
        || outer.having.is_some()
        || !outer.joins.is_empty()
        || outer.distinct
        || outer.group_by.len() != 1
        || !outer.grouping_sets.is_empty()
        || !matches!(
            outer.group_by_modifier,
            crate::parser::GroupByModifier::None
        )
        || !outer.named_windows.is_empty()
        || outer.projection.len() != 2
        || outer.for_update
        || outer.sql_calc_found_rows
    {
        return None;
    }
    let group_name = unqualified_expr_name(&outer.group_by[0])?;
    let inner = match derived {
        crate::parser::SubQuery::Select(select) => select.as_ref(),
        crate::parser::SubQuery::SetOp(_)
        | crate::parser::SubQuery::Rrf(_)
        | crate::parser::SubQuery::WithCte(_) => return None,
    };
    let inner_count = inner.projection.iter().find_map(|item| {
        let SelectItem::Expr {
            expr:
                Expr::Function {
                    name,
                    distinct: _,
                    args: _,
                },
            alias: Some(alias),
        } = item
        else {
            return None;
        };
        (name.eq_ignore_ascii_case("COUNT") && alias.eq_ignore_ascii_case(group_name))
            .then(|| alias.clone())
    })?;

    let mut outputs = Vec::with_capacity(2);
    let mut saw_group = false;
    let mut saw_frequency = false;
    for item in &outer.projection {
        let SelectItem::Expr { expr, alias } = item else {
            return None;
        };
        if unqualified_expr_name(expr).is_some_and(|name| name.eq_ignore_ascii_case(group_name)) {
            let name = alias.clone().unwrap_or_else(|| group_name.to_string());
            outputs.push(DerivedFrequencyOutput {
                name,
                kind: DerivedFrequencyOutputKind::InnerCount,
            });
            saw_group = true;
            continue;
        }
        let Expr::Function {
            name,
            args,
            distinct,
        } = expr
        else {
            return None;
        };
        if !name.eq_ignore_ascii_case("COUNT")
            || *distinct
            || !matches!(args.as_slice(), [Expr::Wildcard])
        {
            return None;
        }
        let output_name = alias.clone()?;
        outputs.push(DerivedFrequencyOutput {
            name: output_name,
            kind: DerivedFrequencyOutputKind::Frequency,
        });
        saw_frequency = true;
    }
    if !saw_group || !saw_frequency {
        return None;
    }
    let order_names = outputs
        .iter()
        .map(|output| output.name.as_str())
        .collect::<Vec<_>>();
    if outer.order_by.iter().any(|item| match &item.expr {
        Expr::Literal(crate::parser::Literal::Int(value)) => {
            *value < 1 || *value as usize > outputs.len()
        }
        Expr::Literal(crate::parser::Literal::UInt(value)) => {
            *value < 1 || *value as usize > outputs.len()
        }
        expr => {
            let references_output_alias = unqualified_expr_name(expr).is_some_and(|name| {
                order_names
                    .iter()
                    .any(|output| output.eq_ignore_ascii_case(name))
            });
            // Alias resolution normalizes `ORDER BY aggregate_alias` back to
            // the projection expression before this phase (for example,
            // `ORDER BY custdist` becomes `ORDER BY COUNT(*)`). Preserve that
            // already-resolved expression as another safe output reference.
            let matches_projection = outer.projection.iter().any(|projection| {
                matches!(projection, SelectItem::Expr { expr: projected, .. } if expr_semantically_eq(projected, expr))
            });
            !references_output_alias && !matches_projection
        }
    }) {
        return None;
    }
    Some(DerivedCountFrequencySpec {
        inner_count_name: inner_count,
        outputs,
    })
}
fn cap_group_concat_warnings_for_output(stmt: &Select, output_rows: usize) {
    let expressions = stmt
        .projection
        .iter()
        .filter_map(|item| match item {
            SelectItem::Expr { expr, .. } => Some(expr),
            _ => None,
        })
        .collect::<Vec<_>>();
    let group_concat_slots = self::aggregate::collect_aggregate_slots(&expressions)
        .into_iter()
        .filter(|slot| slot.name.eq_ignore_ascii_case("GROUP_CONCAT"))
        .count();
    if group_concat_slots > 0 {
        crate::engine::api::cap_statement_warnings_since(
            0,
            crate::error::catalog::ER_CUT_VALUE_GROUP_CONCAT
                .code()
                .get(),
            output_rows.saturating_mul(group_concat_slots),
        );
    }
}

pub(in crate::engine::executor) fn cap_prepared_group_concat_limit_warnings<'q>(
    statement: impl Into<crate::parser::QueryRef<'q>>,
    response: &QueryResponse,
) {
    let statement = statement.into();
    let crate::parser::QueryRef::Select(select) = statement else {
        return;
    };
    if select.limit.is_none() {
        return;
    }
    let output_rows = match response {
        QueryResponse::ResultSet(result) => result.rows.len(),
        _ => return,
    };
    cap_group_concat_warnings_for_output(select, output_rows);
}

pub(super) fn refresh_select_stats(db: &Arc<Database>, stmt: &Select) -> Result<(), QueryError> {
    if !refresh_stats_on_select() {
        return Ok(());
    }
    refresh_stale_stats_for_select(db, stmt)
}

pub(super) fn refresh_stale_stats_for_select(
    db: &Arc<Database>,
    stmt: &Select,
) -> Result<(), QueryError> {
    let mut table_names = stmt
        .from
        .iter()
        .map(|table| table.name.as_str())
        .collect::<Vec<_>>();
    table_names.extend(stmt.joins.iter().map(|join| join.table.name.as_str()));
    db.refresh_stale_stats_for_tables(table_names)
}

pub(super) fn refresh_stats_on_select() -> bool {
    static REFRESH: OnceLock<bool> = OnceLock::new();
    *REFRESH.get_or_init(|| {
        crate::util::env::var("MYDB_REFRESH_STATS_ON_SELECT")
            .map(|value| {
                matches!(
                    value.to_ascii_lowercase().as_str(),
                    "1" | "true" | "yes" | "on"
                )
            })
            .unwrap_or(false)
    })
}

pub(super) fn select_projection_has_aggregate(projection: &[SelectItem]) -> bool {
    projection.iter().any(|item| match item {
        SelectItem::Expr { expr, .. } => expr_has_aggregate(expr),
        _ => false,
    })
}

/// Whether a projection contains a *block-level* aggregate — i.e. one that
/// makes THIS query block an implicit/explicit aggregation. Unlike
/// [`select_projection_has_aggregate`], this does NOT descend into nested
/// subqueries: `SELECT id, (SELECT MAX(id) FROM t) FROM t` has an aggregate in
/// the scalar subquery, but that aggregate belongs to the subquery, so the
/// outer block is NOT an aggregate query and must return one row per input.
/// Use this for the "should I plan this block as an aggregation?" decision;
/// using the descending variant collapses such queries to a single NULL row
/// (issues #56 / #64).
pub(super) fn select_projection_has_toplevel_aggregate(projection: &[SelectItem]) -> bool {
    projection.iter().any(|item| match item {
        SelectItem::Expr { expr, .. } => expr_has_toplevel_aggregate(expr),
        _ => false,
    })
}

/// Descends into nested subqueries — see the doc on
/// [`select_projection_has_toplevel_aggregate`] for why that is WRONG for the
/// block-aggregation decision. Kept for complex-expression detection sites that
/// already test for subqueries separately (so the descent is a no-op there).
pub(super) fn expr_has_aggregate(expr: &Expr) -> bool {
    crate::parser::visit::any_expr_matching(
        expr,
        |node| match node {
            Expr::Function { name, .. } => planner::is_aggregate_function(name),
            Expr::Subquery(query)
            | Expr::Exists {
                subquery: query, ..
            }
            | Expr::InSubquery {
                subquery: query, ..
            } => select_projection_has_aggregate(&query.projection),
            Expr::QuantifiedComparison {
                right: QuantifiedComparisonRight::Subquery(query),
                ..
            } => select_projection_has_aggregate(&query.projection),
            _ => false,
        },
        |node| {
            crate::parser::visit::scalar_query_operand_children(node)
                || matches!(node, Expr::Tuple(_))
        },
    )
}

pub(super) fn data_type_kind_for_column_type(column_type: ColumnType) -> DataTypeKind {
    match column_type {
        ColumnType::Tiny => DataTypeKind::TinyInt,
        ColumnType::Short => DataTypeKind::SmallInt,
        ColumnType::MediumInt => DataTypeKind::MediumInt,
        ColumnType::Long => DataTypeKind::Int,
        ColumnType::LongLong => DataTypeKind::BigInt,
        ColumnType::Year => DataTypeKind::Year,
        ColumnType::Float => DataTypeKind::Float {
            precision: None,
            scale: None,
        },
        ColumnType::Double => DataTypeKind::Double {
            precision: None,
            scale: None,
        },
        ColumnType::Decimal { precision, scale } => DataTypeKind::Decimal {
            precision: Some(precision),
            scale: Some(scale),
        },
        ColumnType::Date => DataTypeKind::Date,
        ColumnType::Time => DataTypeKind::Time { fsp: 0 },
        ColumnType::DateTime => DataTypeKind::DateTime { fsp: 0 },
        ColumnType::Timestamp => DataTypeKind::Timestamp { fsp: 0 },
        ColumnType::Blob => DataTypeKind::Blob {
            max_len: crate::storage::schema::DEFAULT_LOB_MAX,
        },
        ColumnType::Json => DataTypeKind::Json,
        ColumnType::Geometry => DataTypeKind::Geometry {
            subtype: crate::storage::schema::GeometrySubtype::Geometry,
            srid: None,
        },
        ColumnType::Vector => DataTypeKind::Vector {
            dim: None,
            datatype: crate::vector::VectorDataType::Float32,
        },
        ColumnType::Bit { bits } => DataTypeKind::Bit {
            len: Some(bits as usize),
        },
        ColumnType::VarString => DataTypeKind::Text {
            max_len: crate::storage::schema::DEFAULT_LOB_MAX,
        },
    }
}

/// Resolve a `LimitValue::Placeholder(idx)` against the thread-local
/// parameter context (Phase 2 late binding). Returns the bound value as
/// `usize`, or an `ER_NO_DATA_FOR_BIND_PARAMETER (1210)` error if no
/// parameter is bound. The conversion accepts MySQL-style integer-shaped
/// values; anything else is rejected as a range error.
fn resolve_limit_placeholder(idx: usize) -> Result<usize, QueryError> {
    let value = crate::engine::params::current_param(idx).ok_or_else(|| {
        error(
            crate::error::catalog::ER_WRONG_ARGUMENTS,
            "parameter is not bound",
        )
    })?;
    match value {
        crate::types::Value::Int(n) if n >= 0 => Ok(n as usize),
        crate::types::Value::UInt(n) => Ok(n as usize),
        _ => Err(error(
            crate::error::catalog::ER_WRONG_ARGUMENTS,
            "LIMIT/OFFSET parameter must be a non-negative integer",
        )),
    }
}

pub(super) fn limit_value_or_zero(value: Option<&LimitValue>) -> Result<usize, QueryError> {
    match value {
        Some(LimitValue::Rows(value)) => Ok(*value),
        Some(LimitValue::Placeholder(idx) | LimitValue::OffsetCommaPlaceholder(idx)) => {
            resolve_limit_placeholder(*idx)
        }
        None => Ok(0),
    }
}

pub(super) fn limit_value_or_max(value: Option<&LimitValue>) -> Result<usize, QueryError> {
    match value {
        Some(LimitValue::Rows(value)) => Ok(*value),
        Some(LimitValue::Placeholder(idx) | LimitValue::OffsetCommaPlaceholder(idx)) => {
            resolve_limit_placeholder(*idx)
        }
        None => Ok(usize::MAX),
    }
}

pub(super) fn optional_limit_value(
    value: Option<&LimitValue>,
) -> Result<Option<usize>, QueryError> {
    match value {
        Some(LimitValue::Rows(value)) => Ok(Some(*value)),
        Some(LimitValue::Placeholder(idx) | LimitValue::OffsetCommaPlaceholder(idx)) => {
            resolve_limit_placeholder(*idx).map(Some)
        }
        None => Ok(None),
    }
}

pub(super) fn select_limit(stmt: &Select) -> Result<Option<usize>, QueryError> {
    optional_limit_value(stmt.limit.as_ref())
}

pub(super) fn select_offset(stmt: &Select) -> Result<usize, QueryError> {
    limit_value_or_zero(stmt.offset.as_ref())
}

/// Whether a parsed SELECT can be represented by the source-free constant
/// operator.  This is a semantic AST classification (never a SQL-text sniff):
/// nested queries, aggregates and source-producing constructs stay on the
/// general runner so their privilege, MDL and transaction dependencies are
/// collected normally.
pub(crate) fn const_operator_supported(stmt: &Select) -> bool {
    stmt.table.is_none()
        && stmt.from.is_empty()
        && stmt.joins.is_empty()
        && stmt.values.is_none()
        && stmt.json_table.is_none()
        && stmt.derived.is_none()
        && stmt.group_by.is_empty()
        && stmt.having.is_none()
        && stmt.selection.is_none()
        && stmt.order_by.is_empty()
        && !stmt.distinct
        && !stmt.for_update
        && stmt.lock_clause.is_none()
        && stmt.projection.iter().all(|item| match item {
            SelectItem::Expr { expr, .. } => {
                !expr_has_subquery(expr) && !expr_has_toplevel_aggregate(expr)
            }
            SelectItem::Wildcard | SelectItem::QualifiedWildcard(_) => false,
        })
}

pub(crate) fn execute_const_select(
    db: &Arc<Database>,
    txn: TxnView,
    stmt: &Select,
) -> Result<QueryResponse, QueryError> {
    validate_select_static_collations(stmt)?;
    if stmt
        .projection
        .iter()
        .any(|item| matches!(item, SelectItem::Wildcard))
    {
        return Err(error(
            crate::error::catalog::ER_BAD_FIELD_ERROR,
            "wildcard requires a table",
        ));
    }
    // Issue #56: a FROM-less / `FROM DUAL` / single-row-constant-derived-table
    // query whose projection or HAVING contains an aggregate is an implicit
    // single-group aggregate over one synthetic input row (MySQL semantics) —
    // `COUNT(*)` = 1, other aggregates evaluate over that row, and aggregates
    // nested in arithmetic (`COUNT(*) + 1`) work. Route it through the shared
    // aggregate machinery instead of the per-projection constant evaluator,
    // which would hit the row-level aggregate guard and reject it with 1111.
    if stmt.projection.iter().any(
        |item| matches!(item, SelectItem::Expr { expr, .. } if expr_has_toplevel_aggregate(expr)),
    ) || stmt
        .having
        .as_ref()
        .is_some_and(|h| expr_has_toplevel_aggregate(h))
    {
        return execute_const_aggregate_select(db, txn, stmt.clone());
    }
    let mut columns = Vec::new();
    let mut row = Vec::new();
    let plan_row = PlanRow::default();
    let mut eval_ctx = EvalQueryContext::default();
    let limit = select_limit(&stmt)?;
    let offset = select_offset(&stmt)?;
    if let Some(selection) = &stmt.selection {
        if let Some(column) = first_column_ref(selection) {
            return Err(error(
                crate::error::catalog::ER_BAD_FIELD_ERROR,
                format!("Unknown column '{column}' in 'where clause'"),
            ));
        }
    }
    let passes_selection = stmt
        .selection
        .as_ref()
        .map(|expr| {
            eval_plan_expr_with_db(db, txn, expr, &plan_row, &mut eval_ctx)
                .map(|value| truthy(&value))
        })
        .transpose()?
        .unwrap_or(true);
    for order in &stmt.order_by {
        if let Some(column) = first_column_ref(&order.expr) {
            return Err(error(
                crate::error::catalog::ER_BAD_FIELD_ERROR,
                format!("Unknown column '{column}' in 'order clause'"),
            ));
        }
        if passes_selection {
            let _ = eval_plan_expr_with_db(db, txn, &order.expr, &plan_row, &mut eval_ctx)?;
        }
    }
    for item in &stmt.projection {
        let SelectItem::Expr { expr, alias } = item else {
            unreachable!();
        };
        if let Some(column) = first_column_ref(expr) {
            return Err(error(
                crate::error::catalog::ER_BAD_FIELD_ERROR,
                format!("Unknown column '{column}' in 'field list'"),
            ));
        }
        let mut evaluate = || eval_plan_expr_with_db(db, txn, expr, &plan_row, &mut eval_ctx);
        let value = if !passes_selection {
            Value::Null
        } else if crate::engine::expr::expr_contains_assignment_int_div(expr) {
            crate::engine::expr::with_suppressed_expression_warning_code(
                crate::error::catalog::ER_TRUNCATED_WRONG_VALUE.code().get(),
                evaluate,
            )?
        } else {
            evaluate()?
        };
        let descriptor = context_free_expr_descriptor(expr);
        let value = if matches!(
            descriptor.kind,
            crate::engine::expr_type::ExprTypeKind::Unknown
        ) || !expr_needs_bound_result_coercion(expr, descriptor)
        {
            value
        } else {
            crate::engine::expr::coerce_result_to_expr_type(value, descriptor)?
        };
        let declared_type = if matches!(
            descriptor.kind,
            crate::engine::expr_type::ExprTypeKind::Unknown
        ) {
            const_projection_type(expr)
        } else {
            descriptor.column_type()
        };
        let column_type =
            if declared_type == ColumnType::VarString && matches!(value, Value::Geometry(_)) {
                // Defensive reconciliation for overloaded native functions:
                // the registry remains the metadata oracle, while a concrete
                // constant GEOMETRY result must never be exposed as VARCHAR.
                ColumnType::Geometry
            } else {
                declared_type
            };
        let mut source = expression_column_source_for_expr(
            crate::engine::expr::expr_result_unsigned(&expr, &|_| None),
            &expr,
            descriptor,
        );
        source.dynamic_parameter = descriptor.dynamic_parameter;
        if !matches!(
            descriptor.kind,
            crate::engine::expr_type::ExprTypeKind::Unknown
        ) {
            descriptor.apply_to_source(&mut source);
        }
        columns.push(Column {
            source,
            name: alias.clone().unwrap_or_else(|| expr_name(expr)),
            column_type,
        });
        row.push(value);
    }

    let passes_having = stmt
        .having
        .as_ref()
        .map(|expr| {
            let fields = columns
                .iter()
                .zip(&row)
                .map(|(column, value)| PlanField {
                    meta: self::plan::intern_col_meta(ColMeta {
                        invisible: false,
                        table: String::new(),
                        alias: None,
                        column: column.name.clone(),
                        column_type: column.column_type,
                        source: column.source.clone(),
                    }),
                    value: value.clone(),
                })
                .collect();
            let projected = PlanRow::from_fields(fields);
            eval_plan_expr_with_db(db, txn, expr, &projected, &mut eval_ctx)
                .map(|value| truthy(&value))
        })
        .transpose()?
        .unwrap_or(true);
    let rows = if limit == Some(0) || offset > 0 || !passes_selection || !passes_having {
        Vec::new()
    } else {
        vec![row]
    };
    Ok(QueryResponse::ResultSet(ResultSet { columns, rows }))
}

/// Like `expr_has_aggregate`, but does NOT descend into nested query blocks
/// (subqueries / EXISTS / IN-subquery): an aggregate inside a subquery belongs
/// to *that* block's grouping, not this one. This matches the scoping of
/// `collect_aggregate_slots` (`walk_expr_for_aggregates` also stops at
/// subqueries), so detection and slot collection agree — a FROM-less
/// `SELECT (SELECT SUM(x) FROM t)` is a plain scalar-subquery projection, not
/// an implicit aggregate over the outer empty row.
pub(super) fn expr_has_toplevel_aggregate(expr: &Expr) -> bool {
    match expr {
        Expr::Function { name, args, .. } => {
            planner::is_aggregate_function(name) || args.iter().any(expr_has_toplevel_aggregate)
        }
        Expr::Unary { expr, .. } | Expr::IsNull(expr) | Expr::IsNotNull(expr) => {
            expr_has_toplevel_aggregate(expr)
        }
        Expr::Binary { left, right, .. } => {
            expr_has_toplevel_aggregate(left) || expr_has_toplevel_aggregate(right)
        }
        Expr::InList { expr, list, .. } => {
            expr_has_toplevel_aggregate(expr) || list.iter().any(expr_has_toplevel_aggregate)
        }
        Expr::Tuple(items) => items.iter().any(expr_has_toplevel_aggregate),
        // Only the left operand belongs to this block; the subquery does not.
        Expr::Subquery(_) => planner::has_aggregate_expr(expr),
        Expr::InSubquery { expr, .. } => expr_has_toplevel_aggregate(expr),
        Expr::QuantifiedComparison { left, .. } => expr_has_toplevel_aggregate(left),
        Expr::Between {
            expr, low, high, ..
        } => {
            expr_has_toplevel_aggregate(expr)
                || expr_has_toplevel_aggregate(low)
                || expr_has_toplevel_aggregate(high)
        }
        Expr::Like { expr, pattern, .. } => {
            expr_has_toplevel_aggregate(expr) || expr_has_toplevel_aggregate(pattern)
        }
        Expr::Case {
            operand,
            conditions,
            results,
            else_result,
        } => {
            operand
                .as_ref()
                .is_some_and(|e| expr_has_toplevel_aggregate(e))
                || conditions.iter().any(expr_has_toplevel_aggregate)
                || results.iter().any(expr_has_toplevel_aggregate)
                || else_result
                    .as_ref()
                    .is_some_and(|e| expr_has_toplevel_aggregate(e))
        }
        // Subquery / Exists and everything else: do not descend into nested
        // query blocks.
        _ => false,
    }
}

/// Evaluate a FROM-less (or `FROM DUAL` / single-row-constant-derived-table)
/// SELECT whose projection or HAVING contains an aggregate. The input is a
/// single implicit row, so every accumulator is fed exactly once with an empty
/// `PlanRow`: `COUNT(*)` → 1, `COUNT(expr)` → 1/0 by nullness, and
/// `SUM`/`MIN`/`MAX`/`AVG`/`GROUP_CONCAT` reduce over that one row. Mirrors the
/// implicit-single-group finalise tail of the general aggregate path.
fn execute_const_aggregate_select(
    db: &Arc<Database>,
    txn: TxnView,
    stmt: Select,
) -> Result<QueryResponse, QueryError> {
    use self::aggregate::collect_aggregate_slots;

    let limit = select_limit(&stmt)?;
    let offset = select_offset(&stmt)?;

    // Collect aggregate slots from projection + HAVING + ORDER BY (ORDER BY is a
    // no-op over one row but may still carry an aggregate reference).
    let mut slot_exprs: Vec<&Expr> = Vec::new();
    for item in &stmt.projection {
        if let SelectItem::Expr { expr, .. } = item {
            slot_exprs.push(expr);
        }
    }
    if let Some(h) = &stmt.having {
        slot_exprs.push(h);
    }
    for ob in &stmt.order_by {
        slot_exprs.push(&ob.expr);
    }
    let slots = collect_aggregate_slots(&slot_exprs);

    // Feed the single implicit input row into every accumulator.
    let implicit_row = PlanRow::default();
    let mut accs: Vec<_> = slots.iter().map(|s| s.make_accumulator()).collect();
    let included = match &stmt.selection {
        Some(predicate) => {
            let value = eval_plan_expr_with_db(
                db,
                txn,
                predicate,
                &implicit_row,
                &mut EvalQueryContext::default(),
            )?;
            crate::engine::expr::truthy(&value)
        }
        None => true,
    };
    for (slot, acc) in slots.iter().zip(accs.iter_mut()).filter(|_| included) {
        if slot.is_count_star {
            acc.update_count_star();
        } else if slot.args.is_empty() {
            acc.update(Value::Null)?;
        } else {
            let mut ctx = EvalQueryContext::default();
            let v = eval_plan_expr_with_db(db, txn, &slot.args[0], &implicit_row, &mut ctx)?;
            if slot.has_order_by() {
                let keys = slot.eval_order_keys(|e| {
                    let mut ctx = EvalQueryContext::default();
                    eval_plan_expr_with_db(db, txn, e, &implicit_row, &mut ctx)
                })?;
                acc.update_ordered(v, keys)?;
            } else {
                acc.update(v)?;
            }
        }
    }

    // Finalise aggregate results into the sample row's cache, keyed identically
    // to the projection/HAVING lookups (`aggregate_key` == `slot.key`).
    let mut cache: std::collections::HashMap<u64, Value> =
        std::collections::HashMap::with_capacity(slots.len());
    for (slot, acc) in slots.iter().zip(accs.into_iter()) {
        cache.insert(slot.key, acc.finalize()?);
    }
    let mut sample = PlanRow::default();
    sample.group = Some(Vec::new());
    sample.agg_values = Some(std::sync::Arc::new(cache));

    // Output column metadata.
    let mut columns = Vec::new();
    for item in &stmt.projection {
        if let SelectItem::Expr { expr, alias } = item {
            let descriptor = context_free_expr_descriptor(expr);
            let mut source = expression_column_source_for_expr(
                crate::engine::expr::expr_result_unsigned(expr, &|_| None),
                expr,
                descriptor,
            );
            source.dynamic_parameter = descriptor.dynamic_parameter;
            if !matches!(
                descriptor.kind,
                crate::engine::expr_type::ExprTypeKind::Unknown
            ) {
                descriptor.apply_to_source(&mut source);
            }
            columns.push(Column {
                source,
                name: alias.clone().unwrap_or_else(|| expr_name(expr)),
                column_type: if matches!(
                    descriptor.kind,
                    crate::engine::expr_type::ExprTypeKind::Unknown
                ) {
                    const_projection_type(expr)
                } else {
                    descriptor.column_type()
                },
            });
        }
    }

    // HAVING over the single implicit group.
    let having_ok = match &stmt.having {
        Some(having) => self::eval::eval_aggregate_bool(having, &[], &sample)?,
        None => true,
    };

    let rows = if !having_ok || limit == Some(0) || offset > 0 {
        Vec::new()
    } else {
        let mut out = Vec::with_capacity(columns.len());
        for item in &stmt.projection {
            if let SelectItem::Expr { expr, .. } = item {
                let value = self::eval::eval_aggregate_expr(expr, &[], &sample)?;
                let descriptor = context_free_expr_descriptor(expr);
                out.push(
                    if matches!(
                        descriptor.kind,
                        crate::engine::expr_type::ExprTypeKind::Unknown
                    ) || !expr_needs_bound_result_coercion(expr, descriptor)
                    {
                        value
                    } else {
                        crate::engine::expr::coerce_result_to_expr_type(value, descriptor)?
                    },
                );
            }
        }
        vec![out]
    };
    Ok(QueryResponse::ResultSet(ResultSet { columns, rows }))
}

fn first_column_ref(expr: &Expr) -> Option<String> {
    match expr {
        Expr::Identifier(name) => Some(name.clone()),
        Expr::CompoundIdentifier(parts) => Some(parts.join(".")),
        Expr::Unary { expr, .. } | Expr::IsNull(expr) | Expr::IsNotNull(expr) => {
            first_column_ref(expr)
        }
        Expr::Subquery(_) => None,
        Expr::Binary { left, right, .. } => {
            first_column_ref(left).or_else(|| first_column_ref(right))
        }
        Expr::InList { expr, list, .. } => {
            first_column_ref(expr).or_else(|| list.iter().find_map(first_column_ref))
        }
        Expr::InSubquery { expr, .. } => first_column_ref(expr),
        Expr::QuantifiedComparison { left, right, .. } => {
            first_column_ref(left).or_else(|| match right {
                QuantifiedComparisonRight::Subquery(_) => None,
                QuantifiedComparisonRight::List(list) => list.iter().find_map(first_column_ref),
            })
        }
        Expr::Exists { .. } => None,
        Expr::Between {
            expr, low, high, ..
        } => first_column_ref(expr)
            .or_else(|| first_column_ref(low))
            .or_else(|| first_column_ref(high)),
        Expr::Like { expr, pattern, .. } => {
            first_column_ref(expr).or_else(|| first_column_ref(pattern))
        }
        Expr::Case {
            operand,
            conditions,
            results,
            else_result,
        } => operand
            .as_deref()
            .and_then(first_column_ref)
            .or_else(|| conditions.iter().find_map(first_column_ref))
            .or_else(|| results.iter().find_map(first_column_ref))
            .or_else(|| else_result.as_deref().and_then(first_column_ref)),
        Expr::Function { args, .. } => args.iter().find_map(first_column_ref),
        Expr::WindowFunction { args, spec, .. } => {
            let crate::parser::WindowSpec {
                partition_by,
                order_by,
                ..
            } = spec.as_ref();
            args.iter()
                .find_map(first_column_ref)
                .or_else(|| partition_by.iter().find_map(first_column_ref))
                .or_else(|| {
                    order_by
                        .iter()
                        .find_map(|order| first_column_ref(&order.expr))
                })
        }
        Expr::Tuple(items) => items.iter().find_map(first_column_ref),
        Expr::GroupKeyRef { expr, .. } => first_column_ref(expr),
        Expr::Wildcard
        | Expr::UserVariable(_)
        | Expr::SystemVariable { .. }
        | Expr::Literal(_)
        | Expr::BoundValue(_)
        | Expr::Placeholder(_)
        | Expr::Default(_)
        | Expr::DefaultQualified(_)
        | Expr::Grouping(_) => None,
    }
}

pub(super) fn execute_system_select(
    db: &Database,
    stmt: Select,
    registry: &SessionRegistry,
) -> Result<QueryResponse, QueryError> {
    if stmt.joins.len() > 0 || stmt.from.len() != 1 {
        return Err(error(
            crate::error::catalog::ER_NOT_SUPPORTED_YET,
            "system table SELECT supports one table",
        ));
    }
    let table_ref = stmt.from[0].clone();
    // Apply the table alias to the shared column metadata once, up front,
    // so the per-row loop only swaps in the row's value.
    let fields: Vec<self::plan::PlanField> = system_table_fields(db, &table_ref.name)?
        .into_iter()
        .map(|f| {
            let mut meta = (*f.meta).clone();
            meta.alias = table_ref.alias.clone();
            self::plan::PlanField {
                meta: self::plan::intern_col_meta(meta),
                value: f.value,
            }
        })
        .collect();
    let pushed = stmt.selection.as_ref().and_then(zone_map_diagnostic_filter);
    let scan_request = crate::engine::information_schema::SystemScanRequest {
        table_schema: pushed
            .as_ref()
            .and_then(|filter| filter.table_schema.clone()),
        table_name: pushed.as_ref().and_then(|filter| filter.table_name.clone()),
        column_name: pushed
            .as_ref()
            .and_then(|filter| filter.column_name.clone()),
        storage_id: pushed.as_ref().and_then(|filter| filter.storage_id),
        equalities: pushed
            .as_ref()
            .map(|filter| filter.equalities.clone())
            .unwrap_or_default(),
    };
    if system_count_star_fast_path(&stmt)
        && stmt
            .selection
            .as_ref()
            .is_none_or(system_count_filter_is_exact)
    {
        if let Some(count) =
            crate::engine::information_schema::fast_count_rows(db, &table_ref.name, &scan_request)?
        {
            let columns = system_result_columns(&stmt, &fields)?;
            let value = i64::try_from(count)
                .map(Value::Int)
                .unwrap_or(Value::UInt(count));
            return Ok(QueryResponse::ResultSet(ResultSet {
                columns,
                rows: vec![vec![value]],
            }));
        }
    }
    let governed = system_table_rows(db, &table_ref.name, registry, &scan_request)?;
    let (source_rows, _system_metadata_charge) = governed.into_parts();
    let mut rows = source_rows
        .into_iter()
        .map(|values| {
            let fields = fields
                .iter()
                .cloned()
                .zip(values)
                .map(|(mut field, value)| {
                    field.value = value;
                    field
                })
                .collect();
            PlanRow {
                fields,
                group: None,
                agg_values: None,
                schema: PlanRow::empty_schema(),
            }
        })
        .collect::<Vec<_>>();
    if let Some(filter) = &stmt.selection {
        let compiled = rows
            .first()
            .map(|row| eval::CompiledPlanPredicate::compile_once(filter, row));
        let mut filtered = Vec::with_capacity(rows.len());
        for row in rows {
            let keep = match &compiled {
                Some(compiled) => compiled.eval_bool(&row)?,
                None => eval_plan_bool(filter, &row)?,
            };
            if keep {
                filtered.push(row);
            }
        }
        rows = filtered;
    }
    let mut rows = external_sort::sort_plan_rows(db.data_dir(), rows, &stmt.order_by)?;
    let offset = select_offset(&stmt)?.min(rows.len());
    let end = match select_limit(&stmt)? {
        Some(limit) => offset.saturating_add(limit).min(rows.len()),
        None => rows.len(),
    };
    if offset > 0 {
        rows.drain(0..offset);
    }
    rows.truncate(end - offset);
    let columns = system_result_columns(&stmt, &fields)?;
    let projection_program = rows
        .first()
        .map(|row| PlanProjectionProgram::compile_once(row, &stmt.projection));
    let rows = rows
        .iter()
        .map(|row| {
            if let Some(program) = projection_program.as_ref() {
                program.project_row_values(row)
            } else {
                stmt.projection
                    .iter()
                    .flat_map(|item| project_item(row, item))
                    .collect::<Result<Vec<_>, _>>()
            }
        })
        .collect::<Result<Vec<_>, _>>()?;
    Ok(QueryResponse::ResultSet(ResultSet { columns, rows }))
}

fn system_count_star_fast_path(stmt: &Select) -> bool {
    if !stmt.group_by.is_empty()
        || !stmt.grouping_sets.is_empty()
        || stmt.having.is_some()
        || stmt.distinct
        || stmt.limit.is_some()
        || stmt.offset.is_some()
        || stmt.projection.len() != 1
    {
        return false;
    }
    matches!(
        &stmt.projection[0],
        SelectItem::Expr {
            expr: Expr::Function {
                name,
                args,
                distinct: false,
            },
            ..
        } if name.eq_ignore_ascii_case("count")
            && (args.is_empty() || matches!(args.as_slice(), [Expr::Wildcard]))
    )
}

fn system_count_pushdown_supported(name: &str) -> bool {
    let (schema, table) = crate::storage::split_table_name(crate::storage::DEFAULT_DATABASE, name);
    schema.eq_ignore_ascii_case("information_schema")
        && matches!(table.to_ascii_lowercase().as_str(), "tables" | "statistics")
}

/// True when the complete WHERE clause is a conjunction of literal equality
/// predicates understood by `SystemScanRequest`. Partial predicate pushdown
/// is safe for ordinary scans, but not for COUNT pushdown because omitted
/// residuals would change the aggregate.
fn system_count_filter_is_exact(expr: &Expr) -> bool {
    fn identifier(expr: &Expr) -> bool {
        matches!(expr, Expr::Identifier(_) | Expr::CompoundIdentifier(_))
    }
    fn literal(expr: &Expr) -> bool {
        matches!(
            expr,
            Expr::BoundValue(_)
                | Expr::Literal(crate::parser::Literal::String(_))
                | Expr::Literal(crate::parser::Literal::Int(_))
                | Expr::Literal(crate::parser::Literal::UInt(_))
                | Expr::Placeholder(_)
        )
    }
    match expr {
        Expr::Binary {
            left,
            op: crate::parser::BinaryOp::And,
            right,
        } => system_count_filter_is_exact(left) && system_count_filter_is_exact(right),
        Expr::Binary {
            left,
            op: crate::parser::BinaryOp::Eq,
            right,
        } => (identifier(left) && literal(right)) || (identifier(right) && literal(left)),
        _ => false,
    }
}

fn system_rows_capacity_bytes(rows: &Vec<Vec<Value>>) -> usize {
    let mut bytes = rows
        .capacity()
        .saturating_mul(std::mem::size_of::<Vec<Value>>());
    for row in rows {
        bytes = bytes.saturating_add(row.capacity().saturating_mul(std::mem::size_of::<Value>()));
        for value in row {
            let capacity = match value {
                Value::String(value) | Value::Decimal(value) => value.capacity(),
                Value::Bytes(value) | Value::Json(value) | Value::Geometry(value) => {
                    value.capacity()
                }
                Value::Vector(value) => value.capacity(),
                _ => 0,
            };
            bytes = bytes.saturating_add(capacity);
        }
    }
    bytes
}

#[derive(Debug, Default)]
struct ZoneMapDiagnosticFilter {
    table_schema: Option<String>,
    table_name: Option<String>,
    storage_id: Option<u64>,
    column_name: Option<String>,
    equalities: Vec<(String, Value)>,
}

fn zone_map_diagnostic_filter(expr: &Expr) -> Option<ZoneMapDiagnosticFilter> {
    fn identifier(expr: &Expr) -> Option<&str> {
        match expr {
            Expr::Identifier(name) => Some(name),
            Expr::CompoundIdentifier(parts) => parts.last().map(String::as_str),
            _ => None,
        }
    }
    fn literal(expr: &Expr) -> Option<Value> {
        match expr {
            Expr::BoundValue(value) => Some(value.clone()),
            Expr::Literal(crate::parser::Literal::String(value)) => {
                Some(Value::String(value.clone()))
            }
            Expr::Literal(crate::parser::Literal::Int(value)) => Some(Value::Int(*value)),
            Expr::Literal(crate::parser::Literal::UInt(value)) => Some(Value::UInt(*value)),
            Expr::Placeholder(index) => crate::engine::params::current_param(*index),
            _ => None,
        }
    }
    fn collect(expr: &Expr, filter: &mut ZoneMapDiagnosticFilter) -> bool {
        match expr {
            Expr::Binary {
                left,
                op: crate::parser::BinaryOp::And,
                right,
            } => collect(left, filter) && collect(right, filter),
            Expr::Binary {
                left,
                op: crate::parser::BinaryOp::Eq,
                right,
            } => {
                let pair = identifier(left)
                    .zip(literal(right))
                    .or_else(|| identifier(right).zip(literal(left)));
                let Some((name, value)) = pair else {
                    return true;
                };
                let normalized = name.trim_matches('`').to_ascii_lowercase();
                filter.equalities.push((normalized.clone(), value.clone()));
                match normalized.as_str() {
                    "table_schema" => filter.table_schema = Some(value.to_display_string()),
                    "table_name" => filter.table_name = Some(value.to_display_string()),
                    "column_name" => filter.column_name = Some(value.to_display_string()),
                    "storage_id" => {
                        filter.storage_id = match value {
                            Value::UInt(value) => Some(value),
                            Value::Int(value) if value >= 0 => Some(value as u64),
                            _ => None,
                        }
                    }
                    _ => {}
                }
                true
            }
            // Do not derive constraints through OR/NOT/expressions. The
            // ordinary predicate evaluator below remains the correctness
            // oracle for every shape.
            Expr::Binary {
                op: crate::parser::BinaryOp::Or,
                ..
            }
            | Expr::Unary {
                op: crate::parser::UnaryOp::Not,
                ..
            } => false,
            _ => true,
        }
    }

    let mut filter = ZoneMapDiagnosticFilter::default();
    collect(expr, &mut filter).then_some(filter)
}

fn system_result_columns(stmt: &Select, fields: &[PlanField]) -> Result<Vec<Column>, QueryError> {
    let sample = PlanRow {
        fields: fields.to_vec(),
        group: None,
        agg_values: None,
        schema: PlanRow::empty_schema(),
    };
    let mut columns = Vec::new();
    for item in &stmt.projection {
        match item {
            SelectItem::Wildcard | SelectItem::QualifiedWildcard(_) => {
                columns.extend(
                    fields
                        .iter()
                        .map(|field| col(&field.column, field.column_type)),
                );
            }
            SelectItem::Expr { expr, alias } => columns.push(col(
                &alias.clone().unwrap_or_else(|| expr_name(expr)),
                plan_expr_result_type(expr, &sample)?,
            )),
        }
    }
    Ok(columns)
}

pub(super) fn is_system_table_query(table_name: &str) -> bool {
    let (database, _) = split_table_name(DEFAULT_DATABASE, table_name);
    is_system_database(&database)
}

fn select_references_system_table(stmt: &Select) -> bool {
    stmt.table.as_deref().is_some_and(is_system_table_query)
        || stmt
            .joins
            .iter()
            .any(|join| join.derived.is_none() && is_system_table_query(&join.table.name))
}

fn select_references_unregistered_system_table(stmt: &Select) -> bool {
    stmt.from
        .iter()
        .chain(
            stmt.joins
                .iter()
                .filter(|join| join.derived.is_none())
                .map(|join| &join.table),
        )
        .any(|source| is_system_table_query(&source.name) && !super::cte::has_cte(&source.name))
}

fn system_table_fields(_db: &Database, table_name: &str) -> Result<Vec<PlanField>, QueryError> {
    let columns = crate::engine::information_schema::columns_for_table_name(table_name)?;
    let (_, table) = split_table_name(DEFAULT_DATABASE, table_name);
    return Ok(columns
        .into_iter()
        .map(|column| PlanField {
            meta: crate::engine::executor::select::plan::intern_col_meta(ColMeta {
                invisible: false,
                table: table.clone(),
                alias: None,
                column: column.name,
                column_type: column.column_type,
                source: column.source,
            }),
            value: Value::Null,
        })
        .collect());
}

pub(super) struct GovernedSystemRows {
    rows: Vec<Vec<Value>>,
    charge: crate::engine::executor::mem_budget::ControlledMemoryCharge,
}

impl GovernedSystemRows {
    pub(super) fn into_parts(
        self,
    ) -> (
        Vec<Vec<Value>>,
        crate::engine::executor::mem_budget::ControlledMemoryCharge,
    ) {
        (self.rows, self.charge)
    }
}

pub(super) fn system_table_rows(
    db: &Database,
    table_name: &str,
    registry: &SessionRegistry,
    request: &crate::engine::information_schema::SystemScanRequest,
) -> Result<GovernedSystemRows, QueryError> {
    use crate::engine::executor::mem_budget::{ControlledMemoryCategory, ControlledMemoryCharge};
    let estimate = estimate_system_rows_capacity_bytes(db, table_name, registry, request);
    let mut charge =
        ControlledMemoryCharge::try_new(ControlledMemoryCategory::SystemMetadata, estimate)?;
    let rows = crate::engine::information_schema::rows_for_table_name_with_request(
        db, table_name, registry, request,
    )?;
    let actual = system_rows_capacity_bytes(&rows);
    if actual > estimate {
        charge.try_grow(actual - estimate)?;
    } else {
        charge.shrink(estimate - actual);
    }
    Ok(GovernedSystemRows { rows, charge })
}

fn estimate_system_rows_capacity_bytes(
    db: &Database,
    table_name: &str,
    registry: &SessionRegistry,
    request: &crate::engine::information_schema::SystemScanRequest,
) -> usize {
    let (schema, table) = split_table_name(DEFAULT_DATABASE, table_name);
    let exact_object = request.table_schema.is_some() && request.table_name.is_some();
    let columns = crate::engine::information_schema::find_system_table_name(table_name)
        .map(|definition| definition.columns.len())
        .unwrap_or(8);
    let estimate = |row_count: usize| {
        row_count
            .saturating_mul(
                std::mem::size_of::<Vec<Value>>()
                    .saturating_add(columns.saturating_mul(std::mem::size_of::<Value>()))
                    .saturating_add(256),
            )
            .max(4 * 1024)
    };
    if schema.eq_ignore_ascii_case("performance_schema") {
        if !request.equalities.is_empty() {
            return estimate(4);
        }
        let exact_rows = match table.to_ascii_lowercase().as_str() {
            "setup_consumers" => Some(
                crate::engine::performance_schema_mysql80_generated::SETUP_CONSUMERS_DEFAULTS.len(),
            ),
            "setup_instruments" => Some(
                crate::engine::performance_schema_mysql80_generated::SETUP_INSTRUMENT_DEFAULTS
                    .len(),
            ),
            "setup_threads" => Some(
                crate::engine::performance_schema_mysql80_generated::SETUP_THREAD_DEFAULTS.len(),
            ),
            "events_errors_summary_global_by_error" => {
                Some(crate::engine::performance_schema_mysql80_generated::ERROR_CATALOG.len())
            }
            "events_statements_summary_by_digest" => Some(crate::engine::perf_digest::row_count()),
            "events_statements_histogram_by_digest" => {
                Some(crate::engine::perf_digest::histogram_row_count())
            }
            "events_statements_histogram_global" => Some(450),
            "threads" | "processlist" | "accounts" | "hosts" | "users" => {
                Some(registry.snapshot().len())
            }
            _ => None,
        };
        if let Some(rows) = exact_rows {
            return estimate(rows.max(1));
        }
        if let Some(rows) =
            crate::engine::performance_schema_mysql80_generated::table_metadata(&table)
                .and_then(|metadata| metadata.table_rows)
        {
            // A table hint is configured capacity, not necessarily current
            // cardinality. Bound speculative admission; producers with large
            // live cardinality have exact cases above and grow before output.
            return estimate(usize::try_from(rows).unwrap_or(usize::MAX).min(10_000));
        }
    }
    // Preserve the producer's exact-key fast path all the way through memory
    // admission. Cloning the complete catalog merely to estimate a 1-table
    // result made the supposedly pushed-down query O(total catalog size).
    if exact_object {
        let row_count = match table.to_ascii_lowercase().as_str() {
            "tables" => 2,
            "columns" => request
                .table_schema
                .as_deref()
                .zip(request.table_name.as_deref())
                .and_then(|(schema, table)| {
                    let qualified = format!("{schema}.{table}");
                    db.table(&qualified)
                        .ok()
                        .map(|table| table.iter_visible_columns().count())
                        .or_else(|| {
                            db.view_definition(schema, table)
                                .map(|view| view.columns.len())
                        })
                        .or_else(|| {
                            crate::engine::information_schema::find_system_table(schema, table)
                                .map(|table| table.columns.len())
                        })
                })
                .unwrap_or(1),
            "statistics" => 16,
            _ => 64,
        };
        return estimate(row_count);
    }
    let catalog = db.catalog();
    let row_count = match table.to_ascii_lowercase().as_str() {
        "tables" => {
            catalog.tables.len()
                + catalog.views.len()
                + crate::engine::information_schema::system_table_definitions().count()
        }
        "columns" => {
            catalog
                .tables
                .values()
                .map(|table| table.iter_visible_columns().count())
                .sum::<usize>()
                + crate::engine::information_schema::system_table_definitions()
                    .map(|table| table.columns.len())
                    .sum::<usize>()
        }
        "statistics" => catalog
            .tables
            .values()
            .map(|table| {
                table
                    .iter_public_indexes()
                    .map(|index| index.columns.len())
                    .sum::<usize>()
            })
            .sum::<usize>()
            .max(1),
        "innodb_buffer_page" | "innodb_buffer_page_lru" => {
            db.storage_status().buffer_pool_used_pages as usize
        }
        "st_spatial_reference_systems" => 5_152,
        "keywords" => 751,
        _ => (catalog.tables.len() + catalog.views.len()).max(64),
    };
    estimate(row_count)
}
fn processlist_columns() -> Vec<Column> {
    crate::engine::show::processlist_columns()
}

#[cfg(test)]
mod derived_frequency_tests {
    use super::*;

    #[test]
    fn derived_count_frequency_recognizes_count_of_counts_shape() {
        let statement = crate::parser::parse_statement(
            "SELECT c_count, COUNT(*) AS custdist \
             FROM (SELECT c_custkey, COUNT(o_orderkey) AS c_count \
                   FROM customer LEFT JOIN orders ON c_custkey = o_custkey \
                   GROUP BY c_custkey) AS c_orders \
             GROUP BY c_count ORDER BY custdist DESC, c_count DESC",
        )
        .unwrap();
        let Statement::Select(outer) = statement else {
            panic!("expected SELECT");
        };
        assert!(
            derived_count_frequency_spec(&outer, outer.derived.as_ref().unwrap()).is_some(),
            "AST shape was not recognized: {outer:#?}"
        );
    }
}
