//! output execution and preparation.

use super::*;

/// Concatenate `left` ++ `right` field vectors into a joined row.
///
/// O3 — `lookup` is an operator-level shared `Arc<RowSchema>`.
/// Every row a join emits has the same schema, so the caller builds one
/// `OnceLock` per join and the field-name → index map is built once on
/// first resolve instead of once per output row (TPC-H Q9 profile:
/// `FieldLookup::build` was the #1 malloc caller after Stage 1).
/// Projected variant of `join_rows`: concatenates only the columns kept by the
/// join-level projection (index space = the concatenation order of
/// left.fields ++ right.fields). Chained streaming joins use it on the emit hot path to produce narrow rows directly, skipping the "concat-fully-then-narrow" second copy.
pub(in crate::engine::executor::select) fn join_rows_projected(
    left: &PlanRow,
    right: &PlanRow,
    lookup: &std::sync::Arc<RowSchema>,
    projection: &[usize],
) -> PlanRow {
    let left_len = left.fields.len();
    let mut fields = Vec::with_capacity(projection.len());
    for &i in projection {
        let field = if i < left_len {
            left.fields.get(i)
        } else {
            right.fields.get(i - left_len)
        };
        if let Some(field) = field {
            fields.push(field.clone());
        }
    }
    PlanRow {
        fields,
        group: None,
        agg_values: None,
        schema: lookup.clone().into(),
    }
}

pub(in crate::engine::executor::select) fn join_rows(
    left: &PlanRow,
    right: &PlanRow,
    lookup: &std::sync::Arc<RowSchema>,
) -> PlanRow {
    let mut fields = Vec::with_capacity(left.fields.len() + right.fields.len());
    fields.extend(left.fields.iter().cloned());
    fields.extend(right.fields.iter().cloned());
    PlanRow {
        fields,
        group: None,
        agg_values: None,
        schema: lookup.clone().into(),
    }
}

/// SEMI/ANTI fused projection: the output row is the kept-column subset of the probe row.
pub(in crate::engine::executor::select) fn project_row_fields(
    row: &PlanRow,
    projection: &[usize],
) -> PlanRow {
    let mut fields = Vec::with_capacity(projection.len());
    for &i in projection {
        if let Some(field) = row.fields.get(i) {
            fields.push(field.clone());
        }
    }
    PlanRow {
        fields,
        group: None,
        agg_values: None,
        schema: PlanRow::empty_schema(),
    }
}

pub(in crate::engine::executor::select) fn join_rows_with_owned_right(
    left: &PlanRow,
    mut right: PlanRow,
    lookup: &std::sync::Arc<RowSchema>,
) -> PlanRow {
    let mut fields = Vec::with_capacity(left.fields.len() + right.fields.len());
    fields.extend(left.fields.iter().cloned());
    fields.append(&mut right.fields);
    PlanRow {
        fields,
        group: None,
        agg_values: None,
        schema: lookup.clone().into(),
    }
}

/// Lighter-weight join: the right side is raw `Vec<Value>` paired with
/// pre-resolved `&'static ColMeta` slots — no intermediate `PlanRow` is
/// built for the inner row. Used by batched INLJ so 377K-row probes don't
/// allocate a throwaway PlanRow + Vec<PlanField> per match.
#[inline]
pub(in crate::engine::executor::select) fn join_row_with_values(
    left: &PlanRow,
    right_metas: &[ColMetaRef],
    right_values: &[Value],
    lookup: &std::sync::Arc<RowSchema>,
) -> PlanRow {
    debug_assert_eq!(right_metas.len(), right_values.len());
    let mut fields = Vec::with_capacity(left.fields.len() + right_values.len());
    fields.extend(left.fields.iter().cloned());
    for (meta, value) in right_metas.iter().zip(right_values.iter()) {
        fields.push(PlanField {
            meta: meta.clone(),
            value: value.clone(),
        });
    }
    PlanRow {
        fields,
        group: None,
        agg_values: None,
        schema: lookup.clone().into(),
    }
}

/// Fresh per-operator join-output lookup slot. One per join executor; the
/// `FieldLookup` inside is built lazily once and shared by all emitted rows.
#[inline]
pub(in crate::engine::executor::select) fn new_join_lookup() -> std::sync::Arc<RowSchema> {
    RowSchema::empty()
}

/// Bind every row in a homogeneous eager result set to a single shared,
/// pre-built `FieldLookup`. Rows in one result set always share a schema, so
/// the name→index map only needs building once; without this each row that
/// arrives with its own empty `OnceLock` rebuilds the map on first
/// `resolve_plan_field` (the legacy projection / ORDER-BY interpreter path).
/// Only worthwhile for wide rows — narrow rows take the linear-scan path in
/// `resolve_plan_field` and never build a lookup at all.
pub(in crate::engine::executor::select) fn share_field_lookup_across_rows(rows: &mut [PlanRow]) {
    // Mirror `resolve_plan_field`'s `LOOKUP_THRESHOLD`: at or below it the
    // resolver never builds a `FieldLookup`, so sharing buys nothing.
    const LOOKUP_THRESHOLD: usize = 8;
    let Some(first) = rows.first() else {
        return;
    };
    if first.fields.len() <= LOOKUP_THRESHOLD || rows.len() < 2 {
        return;
    }
    let shared = RowSchema::from_fields(&rows[0].fields);
    // Pre-build so the very first projection call also skips the build.
    let _ = shared.lookup(&rows[0].fields);
    for row in rows.iter_mut() {
        row.schema = shared.clone().into();
    }
}

pub(in crate::engine::executor::select) fn dedup_projected_rows(
    rows: &mut Vec<PlanRow>,
    projection: &[SelectItem],
    data_dir: &std::path::Path,
) -> Result<(), QueryError> {
    // Keep projected equality separate from retained PlanRow payloads. A soft
    // miss externalizes the already-evaluated keys and first ordinals; neither
    // the input nor projection expressions are evaluated again on fallback.
    let collations: Vec<Option<u16>> = rows
        .first()
        .map(|row| {
            projection
                .iter()
                .flat_map(|item| match item {
                    SelectItem::Expr { expr, .. } => {
                        vec![crate::engine::executor::select::eval::plan_expr_collation(expr, row).map(|c| c.id)]
                    }
                    SelectItem::Wildcard => row
                        .fields
                        .iter()
                        .filter(|f| !f.invisible)
                        .map(|f| f.source.collation_id)
                        .collect(),
                    SelectItem::QualifiedWildcard(q) => row
                        .fields
                        .iter()
                        .filter(|f| {
                            !f.invisible && crate::engine::executor::select::row_schema::field_matches_qualifier(f, q)
                        })
                        .map(|f| f.source.collation_id)
                        .collect(),
                })
                .collect()
        })
        .unwrap_or_default();
    projected_distinct::dedup(rows, projection, &collations, data_dir)
}

pub(in crate::engine::executor::select) fn result_columns(
    db: &Arc<Database>,
    stmt: &Select,
    rows: &[PlanRow],
    plan: &PhysicalPlan,
) -> Result<Vec<Column>, QueryError> {
    let Some(sample) = rows.first() else {
        return empty_result_columns(stmt, plan);
    };
    // Vectorized/fused aggregate paths may carry only the value-bearing
    // runtime fields and omit catalog origin details. Build the immutable
    // plan schema once as a metadata fallback so a bare projected column
    // keeps its table, declared width and flags next to aggregate outputs.
    let mut plan_fields = Vec::new();
    collect_plan_fields(plan, &mut plan_fields);
    let plan_sample = PlanRow {
        fields: plan_fields
            .into_iter()
            .map(|meta| PlanField {
                meta: intern_col_meta(meta),
                value: Value::Null,
            })
            .collect(),
        group: None,
        agg_values: None,
        schema: PlanRow::empty_schema(),
    };
    projection_result_columns(stmt, sample, |expr, alias| {
        let planned = projected_expr_column(expr, alias, &plan_sample)?;
        // An implicit single-group aggregate has no GROUP BY output
        // fields.  Permissive MySQL hidden columns are represented by
        // `__hidden_first_value` state in `agg_values`, so the runtime
        // row can legitimately have an empty field schema even though
        // the projected column has catalog metadata.  Bind that
        // metadata from the immutable input plan; value projection
        // still reads the finalized hidden aggregate state.
        let hidden_aggregate_output =
            crate::engine::executor::select::aggregate::hidden_group_column_key(expr).is_some_and(
                |key| {
                    sample
                        .agg_values
                        .as_ref()
                        .is_some_and(|values| values.contains_key(&key))
                },
            );
        let mut column = if hidden_aggregate_output {
            planned.clone()
        } else {
            projected_expr_column(expr, alias, sample)?
        };
        // A vectorized aggregate row contains finalized group keys and
        // aggregate values, but not the aggregate's original argument
        // columns. Runtime-only type inference therefore cannot
        // resolve e.g. `SUM(decimal_col)` and conservatively reports
        // DOUBLE. The immutable plan sample still carries the source
        // column metadata, so use it as the type oracle for aggregate
        // projections before this result is materialized as a
        // CTE/derived table and consumed by schema-driven vector lanes.
        if crate::engine::executor::select::expr_has_aggregate(expr) {
            column.column_type = planned.column_type;
            column.source.decimals = planned.source.decimals;
        }
        if column.source.org_table.is_empty() {
            if !planned.source.org_table.is_empty() {
                column = planned;
            }
        }
        if column.source.org_table.is_empty() && stmt.joins.is_empty() && stmt.from.len() == 1 {
            use crate::engine::executor::select::simple::catalog_column_name;
            let catalog_name = catalog_column_name(expr);
            if let (Some(name), Some(table_name)) = (catalog_name, stmt.table.as_deref()) {
                if let Ok(table) = db.table(table_name) {
                    if let Ok(index) = table.column_index(name) {
                        column.source = table
                            .wire_column_source_with_alias(index, stmt.from[0].alias.as_deref());
                        column.column_type = table.result_type(index);
                    }
                }
            }
        }
        Ok(column)
    })
}

/// Drain a physical plan through the row streaming interface, applying the
/// SELECT projection and passing each output row to `sink` immediately.
/// EXPLAIN ANALYZE uses this for synthetic CTE/derived materialization so
/// a large derived body can spill incrementally instead of first building a
/// full in-memory `ResultSet`.
pub(in crate::engine::executor) fn execute_plan_projected_rows_streaming<F>(
    db: &Arc<Database>,
    txn: TxnView,
    stmt: &Select,
    plan: &PhysicalPlan,
    mut sink: F,
) -> Result<(), QueryError>
where
    F: FnMut(Vec<Value>) -> Result<(), QueryError>,
{
    let mut run = || -> Result<(), QueryError> {
        let mut stream = execute_plan_stream(db, txn, plan)?;
        let source_order = wildcard_source_order(stmt);
        let ordered_wildcard =
            projection_uses_unqualified_wildcard(&stmt.projection) && !source_order.is_empty();
        let mut projection_program: Option<PlanProjectionProgram> = None;
        while let Some(row) = stream.next()? {
            crate::engine::cancel::check_cancelled()?;
            let projected = if ordered_wildcard {
                stmt.projection
                    .iter()
                    .flat_map(|item| project_item_with_source_order(&row, item, &source_order))
                    .collect::<Result<Vec<_>, _>>()?
            } else {
                let program = projection_program.get_or_insert_with(|| {
                    PlanProjectionProgram::compile_once(&row, &stmt.projection)
                });
                program.project_row_owned(row)?
            };
            sink(projected)?;
        }
        Ok(())
    };
    if crate::engine::executor::mem_budget::current_budget().is_some() {
        run()
    } else {
        let budget =
            std::sync::Arc::new(crate::engine::executor::mem_budget::MemBudget::from_env());
        crate::engine::executor::mem_budget::with_query_budget(budget, run)
    }
}

pub(in crate::engine::executor) fn empty_result_columns(
    stmt: &Select,
    plan: &PhysicalPlan,
) -> Result<Vec<Column>, QueryError> {
    let mut cols: Vec<ColMeta> = Vec::new();
    collect_plan_fields(plan, &mut cols);
    empty_result_columns_from_fields(stmt, cols)
}

pub(in crate::engine::executor::select) fn empty_result_columns_from_fields(
    stmt: &Select,
    cols: Vec<ColMeta>,
) -> Result<Vec<Column>, QueryError> {
    let sample = PlanRow {
        fields: cols
            .into_iter()
            .map(|m| PlanField {
                meta: intern_col_meta(m),
                value: Value::Null,
            })
            .collect(),
        group: None,
        agg_values: None,
        schema: PlanRow::empty_schema(),
    };
    projection_result_columns(stmt, &sample, |expr, alias| {
        projected_expr_column(expr, alias, &sample)
    })
}
pub(super) fn projection_result_columns(
    stmt: &Select,
    sample: &PlanRow,
    mut project_expr: impl FnMut(&Expr, Option<&str>) -> Result<Column, QueryError>,
) -> Result<Vec<Column>, QueryError> {
    let mut columns = Vec::new();
    let source_order = wildcard_source_order(stmt);
    for item in &stmt.projection {
        match item {
            SelectItem::Wildcard => {
                columns.extend(
                    ordered_wildcard_fields(sample, &source_order)
                        .into_iter()
                        .filter(|field| !is_ft_score_column(&field.column) && !field.invisible)
                        .map(|field| Column {
                            // Plumb the catalog-derived wire metadata
                            // (NOT_NULL / PRI_KEY / UNSIGNED / ENUM_FLAG / …)
                            // captured on `ColMeta.source` through to the
                            // wire Column so clients see the real flags.
                            source: field.source.clone(),
                            name: field.column.clone(),
                            column_type: field.column_type,
                        }),
                );
            }
            SelectItem::QualifiedWildcard(qualifier) => {
                columns.extend(sample.fields.iter().filter_map(|field| {
                    if field_matches_qualifier(field, qualifier)
                        && !is_ft_score_column(&field.column)
                        && !field.invisible
                    {
                        Some(Column {
                            source: field.source.clone(),
                            name: field.column.clone(),
                            column_type: field.column_type,
                        })
                    } else {
                        None
                    }
                }));
            }
            SelectItem::Expr { expr, alias } => columns.push(project_expr(expr, alias.as_deref())?),
        }
    }
    Ok(columns)
}

pub(super) fn projection_uses_unqualified_wildcard(projection: &[SelectItem]) -> bool {
    projection
        .iter()
        .any(|item| matches!(item, SelectItem::Wildcard))
}

pub(in crate::engine::executor::select) fn wildcard_source_order(stmt: &Select) -> Vec<String> {
    let mut out = Vec::new();
    if let Some(source) = stmt.from.first() {
        out.push(source_visible_name(source));
    }
    out.extend(
        stmt.joins
            .iter()
            .map(|join| source_visible_name(&join.table)),
    );
    out
}

pub(super) fn source_visible_name(source: &crate::parser::TableRef) -> String {
    source.alias.clone().unwrap_or_else(|| {
        source
            .name
            .rsplit('.')
            .next()
            .unwrap_or(&source.name)
            .to_string()
    })
}

pub(super) fn ordered_wildcard_fields<'a>(
    row: &'a PlanRow,
    source_order: &[String],
) -> Vec<&'a PlanField> {
    if source_order.is_empty() {
        return row.fields.iter().collect();
    }
    let mut used = vec![false; row.fields.len()];
    let mut ordered = Vec::with_capacity(row.fields.len());
    for source in source_order {
        for (idx, field) in row.fields.iter().enumerate() {
            if !used[idx] && field_matches_qualifier(field, source) {
                used[idx] = true;
                ordered.push(field);
            }
        }
    }
    for (idx, field) in row.fields.iter().enumerate() {
        if !used[idx] {
            ordered.push(field);
        }
    }
    ordered
}

pub(super) fn project_item_with_source_order(
    row: &PlanRow,
    item: &SelectItem,
    source_order: &[String],
) -> Vec<Result<Value, QueryError>> {
    match item {
        SelectItem::Wildcard if !source_order.is_empty() => {
            ordered_wildcard_fields(row, source_order)
                .into_iter()
                .filter(|field| !is_ft_score_column(&field.column) && !field.invisible)
                .map(|field| Ok((field.value).clone()))
                .collect()
        }
        _ => project_item(row, item),
    }
}

pub(super) fn projected_expr_column(
    expr: &Expr,
    alias: Option<&str>,
    sample: &PlanRow,
) -> Result<Column, QueryError> {
    if let Some(field) = source_field_for_projection_expr(expr, sample) {
        return Ok(Column {
            source: field.source.clone(),
            name: alias.map(str::to_string).unwrap_or_else(|| expr_name(expr)),
            column_type: field.column_type,
        });
    }
    if let Expr::Function { name, args, .. } = expr {
        if matches!(
            name.to_ascii_uppercase().as_str(),
            "MIN" | "MAX" | "__HIDDEN_FIRST_VALUE"
        ) {
            if let Some(argument) = args.first() {
                // An index aggregate replaces its input schema with one
                // finalized field. Bind that field's catalog descriptor
                // instead of resolving an argument that no longer exists.
                let output_name = alias.map(str::to_string).unwrap_or_else(|| expr_name(expr));
                if source_field_for_projection_expr(argument, sample).is_none() {
                    if let Some(field) = sample.fields.iter().find(|field| {
                        field.table.is_empty()
                            && field.column.eq_ignore_ascii_case(&output_name)
                            && !field.source.org_name.is_empty()
                    }) {
                        return Ok(Column {
                            name: output_name,
                            column_type: field.column_type,
                            source: field.source.clone(),
                        });
                    }
                }
                if let Ok(mut column) = projected_expr_column(argument, alias, sample) {
                    column.name = alias.map(str::to_string).unwrap_or_else(|| expr_name(expr));
                    column.source = min_max_column_source(column.source);
                    return Ok(column);
                }
            }
        }
    }
    let descriptor = plan_bound_expr_type(expr, sample);
    let column_type = if matches!(
        descriptor.kind,
        crate::engine::expr_type::ExprTypeKind::Unknown
    ) {
        plan_expr_result_type(expr, sample)?
    } else {
        descriptor.column_type()
    };
    let mut source = crate::engine::executor::select::expression_column_source_for_expr(
        plan_expr_unsigned(expr, sample),
        expr,
        descriptor,
    );
    // Functions such as FORMAT() and UNHEX() derive their protocol width
    // from the declared width of an input column.  The cached physical plan
    // retains that catalog metadata in `ColMeta`, so compute the bound here
    // instead of asking the planner to rebuild an equivalent table schema.
    source.length = plan_expression_metadata_length(expr, sample).or(source.length);
    if plan_expr_nonnull(expr, sample) {
        source.flags |= crate::mysql::protocol::NOT_NULL_FLAG;
    }
    source.dynamic_parameter = descriptor.dynamic_parameter;
    if !matches!(
        descriptor.kind,
        crate::engine::expr_type::ExprTypeKind::Unknown
    ) {
        descriptor.apply_to_source(&mut source);
    }
    if let ColumnType::Decimal { scale, .. } = column_type {
        source.decimals = Some(scale);
    } else if matches!(column_type, ColumnType::Float | ColumnType::Double) {
        if let Expr::Function { name, args, .. } = expr {
            if matches!(name.to_ascii_uppercase().as_str(), "SUM" | "AVG") {
                source.decimals = args
                    .first()
                    .and_then(|arg| source_field_for_projection_expr(arg, sample))
                    .and_then(|field| field.source.decimals);
            }
        }
    }
    Ok(Column {
        source,
        name: alias.map(str::to_string).unwrap_or_else(|| expr_name(expr)),
        column_type,
    })
}

pub(super) fn index_aggregate_output_meta(
    table: &PlanTable,
    op: &IndexAggOp,
    output_alias: &str,
    output_type: ColumnType,
) -> ColMeta {
    let source = match op {
        IndexAggOp::Min(column) | IndexAggOp::Max(column) => table
            .schema
            .column_index(column)
            .ok()
            .map(|index| {
                table
                    .schema
                    .wire_column_source_with_alias(index, table.alias.as_deref())
            })
            .unwrap_or_default(),
        _ => Default::default(),
    };
    ColMeta {
        invisible: false,
        table: String::new(),
        alias: None,
        column: output_alias.to_string(),
        column_type: output_type,
        source: min_max_column_source(source),
    }
}

pub(in crate::engine::executor) fn min_max_column_source(
    source: crate::types::ColumnSource,
) -> crate::types::ColumnSource {
    source.nullable_aggregate()
}

pub(super) fn plan_expression_metadata_length(expr: &Expr, sample: &PlanRow) -> Option<u32> {
    let Expr::Function { name, args, .. } = expr else {
        return crate::engine::expr::expression_metadata_length(expr);
    };
    if name.eq_ignore_ascii_case("UNHEX") {
        let field = args
            .first()
            .and_then(|argument| source_field_for_projection_expr(argument, sample));
        let Some(input_bytes) = field.and_then(|field| field.source.length) else {
            return crate::engine::expr::expression_metadata_length(expr);
        };
        return Some(input_bytes / 2 + input_bytes % 2);
    }
    if name.eq_ignore_ascii_case("FORMAT") {
        let field = args
            .first()
            .and_then(|argument| source_field_for_projection_expr(argument, sample));
        let Some(field) = field else {
            return crate::engine::expr::expression_metadata_length(expr);
        };
        let unsigned = field.source.flags & crate::mysql::protocol::UNSIGNED_FLAG != 0;
        let input_characters = match field.column_type {
            ColumnType::Float => field.source.length.unwrap_or(12),
            ColumnType::Double => field.source.length.unwrap_or(22),
            ColumnType::Decimal { precision, scale } => {
                u32::from(precision) + u32::from(scale > 0) + u32::from(!unsigned)
            }
            ColumnType::Tiny => 4,
            ColumnType::Short => 6,
            ColumnType::MediumInt => 9,
            ColumnType::Long => 11,
            ColumnType::LongLong => {
                if unsigned {
                    20
                } else {
                    21
                }
            }
            _ => return crate::engine::expr::expression_metadata_length(expr),
        };
        let output_characters = input_characters
            .saturating_add(input_characters / 3)
            .saturating_add(32);
        let max_bytes_per_character = crate::engine::expr::connection_charset_max_len();
        return Some(output_characters.saturating_mul(u32::from(max_bytes_per_character)));
    }
    crate::engine::expr::expression_metadata_length(expr)
}

pub(super) fn plan_bound_expr_type(
    expr: &Expr,
    sample: &PlanRow,
) -> crate::engine::expr_type::ExprType {
    let resolve_column = |column_expr: &Expr| {
        let field = match column_expr {
            Expr::Identifier(name) => {
                crate::engine::executor::select::eval::resolve_plan_field(sample, None, name).ok()
            }
            Expr::CompoundIdentifier(parts) => {
                let column = parts.last()?;
                let qualifier = parts
                    .len()
                    .checked_sub(2)
                    .and_then(|index| parts.get(index))
                    .map(String::as_str);
                crate::engine::executor::select::eval::resolve_plan_field(sample, qualifier, column)
                    .ok()
            }
            _ => None,
        }?;
        Some(crate::engine::expr_type::ExprType::from_column_type(
            field.column_type,
            &field.source,
        ))
    };
    let resolve_placeholder = |_: usize| None;
    let context =
        crate::engine::expr_type::TypeContext::context_free(&resolve_column, &resolve_placeholder);
    crate::engine::expr_type::infer_expr_type(expr, &context)
}

pub(super) fn plan_expr_unsigned(expr: &Expr, sample: &PlanRow) -> bool {
    crate::engine::expr::expr_result_unsigned(expr, &|column_expr| {
        let field = match column_expr {
            Expr::Identifier(name) => {
                crate::engine::executor::select::eval::resolve_plan_field(sample, None, name).ok()
            }
            Expr::CompoundIdentifier(parts) => {
                let column = parts.last()?;
                let qualifier = parts
                    .len()
                    .checked_sub(2)
                    .and_then(|idx| parts.get(idx))
                    .map(String::as_str);
                crate::engine::executor::select::eval::resolve_plan_field(sample, qualifier, column)
                    .ok()
            }
            _ => None,
        }?;
        Some(field.source.flags & crate::mysql::protocol::UNSIGNED_FLAG != 0)
    })
}

pub(super) fn plan_expr_nonnull(expr: &Expr, sample: &PlanRow) -> bool {
    crate::engine::expr::expr_result_guaranteed_nonnull_with(expr, &|column_expr| {
        let field = match column_expr {
            Expr::Identifier(name) => {
                crate::engine::executor::select::eval::resolve_plan_field(sample, None, name).ok()
            }
            Expr::CompoundIdentifier(parts) => {
                let column = parts.last()?;
                let qualifier = parts
                    .len()
                    .checked_sub(2)
                    .and_then(|idx| parts.get(idx))
                    .map(String::as_str);
                crate::engine::executor::select::eval::resolve_plan_field(sample, qualifier, column)
                    .ok()
            }
            _ => None,
        }?;
        Some(field.source.flags & crate::mysql::protocol::NOT_NULL_FLAG != 0)
    })
}

pub(super) fn source_field_for_projection_expr<'a>(
    expr: &Expr,
    sample: &'a PlanRow,
) -> Option<&'a PlanField> {
    match expr {
        Expr::Identifier(name) => {
            crate::engine::executor::select::eval::resolve_plan_field(sample, None, name).ok()
        }
        Expr::CompoundIdentifier(parts) => {
            let column = parts.last()?;
            let qualifier = parts
                .len()
                .checked_sub(2)
                .and_then(|idx| parts.get(idx))
                .map(String::as_str);
            crate::engine::executor::select::eval::resolve_plan_field(sample, qualifier, column)
                .ok()
        }
        Expr::GroupKeyRef { expr, .. } => source_field_for_projection_expr(expr, sample),
        _ => None,
    }
}

pub(in crate::engine::executor::select) fn null_extended_table_fields(
    table: &PlanTable,
) -> Vec<ColMeta> {
    let mut fields = Vec::new();
    push_table_fields(&mut fields, table, None);
    for field in &mut fields {
        field.source.flags &= !crate::mysql::protocol::NOT_NULL_FLAG;
    }
    fields
}

/// Null-extension is part of the bound schema even if this execution is
/// empty or happens to match every row. Preserve it through derived/CTE scans
/// and expression binding rather than inferring it from observed values.
pub(super) fn mark_outer_join_fields(
    fields: &mut [ColMeta],
    right_start: usize,
    join_type: &crate::parser::JoinType,
) {
    use crate::parser::JoinType;
    let nullable = match join_type {
        JoinType::Left => &mut fields[right_start..],
        JoinType::Right => &mut fields[..right_start],
        JoinType::Full => fields,
        _ => return,
    };
    for field in nullable {
        field.source.flags &= !crate::mysql::protocol::NOT_NULL_FLAG;
    }
}

pub(crate) fn physical_expr_type(
    plan: &PhysicalPlan,
    expr: &Expr,
) -> crate::engine::expr_type::ExprType {
    use crate::engine::expr_type::{infer_expr_type, ExprType, TypeContext};
    let mut fields = Vec::new();
    collect_plan_fields(plan, &mut fields);
    let lookup = FieldLookup::build(&fields);
    let resolve = |expr: &Expr| {
        let index = resolve_hash_join_key_index(expr, &lookup)?;
        let field = fields.get(index)?;
        Some(ExprType::from_column_type(field.column_type, &field.source))
    };
    infer_expr_type(expr, &TypeContext::context_free(&resolve, &|_| None))
}

pub(in crate::engine::executor::select) fn collect_plan_fields(
    plan: &PhysicalPlan,
    out: &mut Vec<ColMeta>,
) {
    match plan {
        PhysicalPlan::NullExtend { input, table, .. } => {
            collect_plan_fields(input, out);
            out.extend(null_extended_table_fields(table));
        }

        PhysicalPlan::RrfFusion { columns, .. } => {
            out.extend(columns.iter().map(|column| ColMeta {
                table: "$rrf".into(),
                alias: None,
                column: column.name.clone(),
                column_type: column.column_type,
                source: column.source.clone(),
                invisible: false,
            }));
        }
        PhysicalPlan::SetOp { left, .. } => collect_plan_fields(left, out),
        PhysicalPlan::RecursiveUnion { anchor, .. } => collect_plan_fields(anchor, out),
        PhysicalPlan::ValuesScan { columns, .. } => {
            out.extend(columns.iter().map(|column| ColMeta {
                invisible: false,
                table: "values".to_string(),
                alias: None,
                column: column.name.clone(),
                column_type: column.column_type,
                source: column.source.clone(),
            }));
        }
        PhysicalPlan::JsonTableScan {
            relation, columns, ..
        } => {
            out.extend(columns.iter().map(|column| ColMeta {
                invisible: false,
                table: relation.clone(),
                alias: Some(relation.clone()),
                column: column.name.clone(),
                column_type: column.column_type,
                source: column.source.clone(),
            }));
        }
        PhysicalPlan::SystemScan {
            relation,
            columns,
            projected_columns,
            ..
        } => {
            let projected = projected_columns
                .as_deref()
                .map(|projection| projection.to_vec())
                .unwrap_or_else(|| (0..columns.len()).collect());
            out.extend(
                projected
                    .into_iter()
                    .filter_map(|index| columns.get(index))
                    .map(|column| ColMeta {
                        invisible: false,
                        table: relation.clone(),
                        alias: Some(relation.clone()),
                        column: column.name.clone(),
                        column_type: column.column_type,
                        source: column.source.clone(),
                    }),
            );
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
        } => {
            push_table_fields(
                out,
                table,
                projected_columns.as_deref().map(|v| v.as_slice()),
            );
        }
        PhysicalPlan::IndexFastScan {
            table,
            projected_columns,
            ..
        }
        | PhysicalPlan::ParallelIndexFastScan {
            table,
            projected_columns,
            ..
        } => {
            push_table_fields(
                out,
                table,
                projected_columns.as_deref().map(|v| v.as_slice()),
            );
        }
        PhysicalPlan::IndexRangeScan {
            table,
            projected_columns,
            ..
        }
        | PhysicalPlan::ParallelIndexRangeScan {
            table,
            projected_columns,
            ..
        } => {
            push_table_fields(
                out,
                table,
                projected_columns.as_deref().map(|v| v.as_slice()),
            );
        }
        PhysicalPlan::FulltextScan {
            table,
            projected_columns,
            match_fingerprint,
            needs_score,
            additional_matches,
            ..
        } => {
            push_table_fields(
                out,
                table,
                projected_columns.as_deref().map(|v| v.as_slice()),
            );
            // The score remains invisible at the SQL/protocol boundary, but
            // it is part of the operator's internal relation. Keeping it in
            // the batch schema lets SELECT/ORDER BY/HAVING consume the same
            // fingerprinted value without dropping it during PlanRow→Batch
            // conversion and recomputing or rescanning the FULLTEXT source.
            let mut push_score = |fingerprint: u64| {
                out.push(ColMeta {
                    invisible: true,
                    table: table.schema.name.clone(),
                    alias: table.alias.clone(),
                    column: format!("{FT_SCORE_PREFIX}{fingerprint:016x}"),
                    column_type: ColumnType::Double,
                    source: crate::types::ColumnSource::default(),
                });
            };
            if *needs_score {
                push_score(*match_fingerprint);
            }
            for additional in additional_matches {
                if additional.needs_score {
                    push_score(additional.match_fingerprint);
                }
            }
        }
        PhysicalPlan::SpatialScan {
            table,
            projected_columns,
            ..
        }
        | PhysicalPlan::VectorIndexScan {
            table,
            projected_columns,
            ..
        } => {
            push_table_fields(
                out,
                table,
                projected_columns.as_deref().map(|v| v.as_slice()),
            );
        }
        PhysicalPlan::VectorBruteScan { table, .. } => {
            push_table_fields(out, table, None);
        }
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
        } => {
            // Must mirror the executor's view of these nodes:
            // `read_plan_table_ids` decodes only the projected columns
            // when `projected_columns` is `Some`, so the emitted
            // `PlanRow.fields` is exactly that narrow set. If we let the
            // upper plan think the full table schema is visible here,
            // any HashJoin / lookup that compiles its key plan against
            // the wider schema indexes off the end of the runtime row
            // and the executor crashes with
            // "hash join key field index out of bounds".
            push_table_fields(
                out,
                table,
                projected_columns.as_deref().map(|v| v.as_slice()),
            );
        }
        PhysicalPlan::NestedLoopJoin {
            left,
            right,
            join_type,
            ..
        }
        | PhysicalPlan::AdaptiveJoin {
            left,
            right,
            join_type,
            ..
        } => {
            let start = out.len();
            collect_plan_fields(left, out);
            let right_start = out.len();
            if !matches!(
                join_type,
                crate::parser::JoinType::Semi | crate::parser::JoinType::Anti
            ) {
                collect_plan_fields(right, out);
            }
            mark_outer_join_fields(&mut out[start..], right_start - start, join_type);
        }
        PhysicalPlan::HashJoin {
            left,
            right,
            join_type,
            output_projection,
            ..
        } => {
            // S2: after join-level projection pruning, this node's output schema
            // is a subset of the concatenation order — it must be mirrored here,
            // else upper layers (outer-join key plans, residual templates) indexing by full width go out of bounds / hit wrong columns.
            let start = out.len();
            collect_plan_fields(left, out);
            let right_start = out.len();
            if !matches!(
                join_type,
                crate::parser::JoinType::Semi | crate::parser::JoinType::Anti
            ) {
                collect_plan_fields(right, out);
            }
            mark_outer_join_fields(&mut out[start..], right_start - start, join_type);
            if let Some(projection) = output_projection {
                let full: Vec<ColMeta> = out.drain(start..).collect();
                for &i in projection.iter() {
                    if let Some(meta) = full.get(i) {
                        out.push(meta.clone());
                    }
                }
            }
        }
        PhysicalPlan::IndexNestedLoopJoin {
            left,
            right_table,
            right_projected_columns,
            join_type,
            ..
        } => {
            let start = out.len();
            collect_plan_fields(left, out);
            let right_start = out.len();
            if !matches!(
                join_type,
                crate::parser::JoinType::Semi | crate::parser::JoinType::Anti
            ) {
                push_table_fields(
                    out,
                    right_table,
                    right_projected_columns.as_deref().map(|v| v.as_slice()),
                );
            }
            mark_outer_join_fields(&mut out[start..], right_start - start, join_type);
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
        | PhysicalPlan::Filter { input, .. }
        | PhysicalPlan::Apply { outer: input, .. }
        | PhysicalPlan::Materialize { input, .. }
        | PhysicalPlan::Limit { input, .. }
        | PhysicalPlan::StreamingAggregate { input, .. }
        | PhysicalPlan::StreamingRollupAggregate { input, .. }
        | PhysicalPlan::StreamingGroupingSetsAggregate { input, .. }
        | PhysicalPlan::StreamingDistinct { input, .. }
        | PhysicalPlan::Exchange { input, .. }
        | PhysicalPlan::ParallelHashAggregate { input, .. }
        | PhysicalPlan::ParallelRollupAggregate { input, .. }
        | PhysicalPlan::ParallelGroupingSetsAggregate { input, .. }
        | PhysicalPlan::ParallelSort { input, .. } => collect_plan_fields(input, out),
        PhysicalPlan::WindowAgg { input, windows, .. } => {
            collect_plan_fields(input, out);
            for w in windows {
                out.push(ColMeta {
                    invisible: false,
                    table: String::new(),
                    alias: None,
                    column: w.output_alias.clone(),
                    column_type: w.output_type,
                    source: Default::default(),
                });
            }
        }
        PhysicalPlan::CteScan {
            name,
            alias,
            columns,
            projected_columns,
            ..
        } => {
            let alias_str = alias.clone().unwrap_or_else(|| name.clone());
            // Same reasoning as IndexLookup above: the CteScan executor
            // emits a `PlanRow.fields` whose width matches
            // `projected_columns` when set, so this metadata view must
            // shrink in step.
            match projected_columns.as_deref() {
                Some(indices) => {
                    for &i in indices {
                        let col = &columns[i];
                        out.push(ColMeta {
                            invisible: false,
                            table: name.clone(),
                            alias: Some(alias_str.clone()),
                            column: col.name.clone(),
                            column_type: col.column_type,
                            source: col.source.clone(),
                        });
                    }
                }
                None => {
                    for col in columns {
                        out.push(ColMeta {
                            invisible: false,
                            table: name.clone(),
                            alias: Some(alias_str.clone()),
                            column: col.name.clone(),
                            column_type: col.column_type,
                            source: col.source.clone(),
                        });
                    }
                }
            }
        }
        PhysicalPlan::GenerateSeries {
            relation, columns, ..
        } => {
            for col in columns {
                out.push(ColMeta {
                    invisible: false,
                    table: relation.clone(),
                    alias: Some(relation.clone()),
                    column: col.name.clone(),
                    column_type: col.column_type,
                    source: Default::default(),
                });
            }
        }
        PhysicalPlan::IndexAggregate {
            table,
            op,
            output_alias,
            output_type,
            ..
        } => {
            out.push(index_aggregate_output_meta(
                table,
                op,
                output_alias,
                *output_type,
            ));
        }
        PhysicalPlan::CoveringIndexAggregate { table, .. }
        | PhysicalPlan::ParallelIndexAggregate { table, .. } => {
            push_table_fields(out, table, None);
        }
    }
}
