use super::super::*;
use super::*;
use crate::engine::expr_program::CompiledTableExpr;
use crate::storage::{ProjectedReadPlan, RowDecodePlan};
use crate::util::fast_hash::{ah_hash_set, AhHashMap};

fn sort_fields_with_schema(
    order_by: &[OrderBy],
    schema: &TableSchema,
) -> smallvec::SmallVec<[crate::engine::executor::sort::SortField; 8]> {
    let mut fields = smallvec::SmallVec::with_capacity(order_by.len());
    for item in order_by {
        let mut field = crate::engine::executor::sort::SortField::from_order_by(item);
        let column_name = match &item.expr {
            Expr::Identifier(name) => Some(name.as_str()),
            Expr::CompoundIdentifier(parts) => parts.last().map(String::as_str),
            _ => None,
        };
        field.collation_id = column_name
            .and_then(|name| {
                schema
                    .columns
                    .iter()
                    .find(|column| column.name.eq_ignore_ascii_case(name))
            })
            .and_then(|column| {
                crate::engine::expr_program::resolve_column_collation_id(schema, column)
            })
            .or_else(|| crate::engine::expr::expression_metadata_collation_id(&item.expr));
        fields.push(field);
    }
    fields
}

/// True when ORDER BY establishes exactly the equivalence classes required by
/// DISTINCT.  Keep this deliberately narrow: direct source columns have one
/// stable SQL type/collation, and an identical key tuple makes duplicate rows
/// adjacent.  Expressions, aliases, reordered/subset keys and wildcards keep
/// the hash-based oracle.
fn can_dedup_distinct_after_sort(stmt: &Select) -> bool {
    stmt.distinct
        && !stmt.order_by.is_empty()
        && stmt.projection.len() == stmt.order_by.len()
        && stmt
            .projection
            .iter()
            .zip(&stmt.order_by)
            .all(|(projection, order)| {
                let SelectItem::Expr { expr, alias: None } = projection else {
                    return false;
                };
                matches!(expr, Expr::Identifier(_) | Expr::CompoundIdentifier(_))
                    && expr == &order.expr
            })
}

/// Compile the complete projected unique-point recipe from the physical plan.
/// This is deliberately plan-driven: the helper never decides whether a SQL
/// statement should bypass planning, it only lowers an already chosen
/// `IndexLookup` node into an allocation-free execution operator.
pub(crate) fn compile_projected_point_lookup_operator(
    stmt: &Select,
    plan: &PhysicalPlan,
) -> Option<crate::engine::cache::ProjectedPointLookupOperator> {
    if !projected_lookup_shape_supported(stmt) {
        return None;
    }

    fn point_lookup(plan: &PhysicalPlan) -> Option<(&PlanTable, &IndexSchema, &IndexKey)> {
        match plan {
            PhysicalPlan::Limit { input, .. }
            | PhysicalPlan::Projection { input, .. }
            | PhysicalPlan::Sort { input, .. }
            | PhysicalPlan::StreamingDistinct { input, .. }
            | PhysicalPlan::HashAggregate { input, .. } => point_lookup(input),
            PhysicalPlan::IndexLookup {
                table,
                index,
                key,
                exact,
                filter,
                covering,
                ..
            } => {
                if !*exact
                    || *covering
                    || !index.included_columns.is_empty()
                    || !(index.primary || index.unique)
                    || table.schema.is_partitioned()
                {
                    return None;
                }
                let residual_filter = filter.as_ref().and_then(|expr| {
                    let access = EngineIndexAccess::Lookup {
                        key: key.clone(),
                        exact: true,
                    };
                    index_access_residual_filter(&table.schema, index, &access, expr)
                });
                residual_filter.is_none().then_some((table, index, key))
            }
            _ => None,
        }
    }

    let (table, index, key) = point_lookup(plan)?;
    // The planner has already validated qualifiers. Expand stars once using
    // catalog logical order (including INSTANT column reordering), excluding
    // dropped/invisible columns, just as the general projection does. A star
    // is a column projection too; rejecting it here rebuilt the whole generic
    // row/predicate/metadata pipeline for every otherwise compiled point read.
    let mut requested = Vec::new();
    for item in &stmt.projection {
        match item {
            SelectItem::Wildcard | SelectItem::QualifiedWildcard(_) => {
                requested.extend(table.schema.iter_select_columns().map(|(idx, _)| idx));
            }
            SelectItem::Expr { expr, .. } => {
                requested.push(expr_column_index(expr, &table.schema)?);
            }
        }
    }
    let mut unique = Vec::with_capacity(requested.len());
    let mut output_map = Vec::with_capacity(requested.len());
    for column in requested.iter().copied() {
        let decoded_slot = unique
            .iter()
            .position(|existing| *existing == column)
            .unwrap_or_else(|| {
                unique.push(column);
                unique.len() - 1
            });
        output_map.push(decoded_slot);
    }
    let columns: Arc<[usize]> = unique.clone().into();
    let output_map = (unique.len() != requested.len()).then(|| Arc::from(output_map));
    let key_columns = (0..index.columns.len())
        .map(|pos| match index.key_part(pos) {
            Some(crate::storage::IndexKeyPart::Column(name)) => table
                .schema
                .column_index(name)
                .ok()
                .filter(|column| table.schema.columns[*column].generated_expr.is_none()),
            _ => None,
        })
        .collect::<Option<Vec<_>>>();
    let key_slots = key_columns.map(|columns| {
        columns
            .into_iter()
            .map(|column| {
                unique
                    .iter()
                    .position(|existing| *existing == column)
                    .unwrap_or_else(|| {
                        unique.push(column);
                        unique.len() - 1
                    })
            })
            .collect::<Arc<[usize]>>()
    });
    let key_projection = key_slots.as_ref().map(|_| {
        let key_columns: Arc<[usize]> = unique.into();
        crate::engine::cache::CompiledProjectedRead {
            output_map: None,
            decode_plan: RowDecodePlan::compile_with_codec(
                &key_columns,
                table.schema.row_codec.as_ref(),
            ),
            projected_plan: ProjectedReadPlan::new(&key_columns, &table.schema),
            columns: key_columns,
        }
    });
    let table_alias = stmt.from.first().and_then(|from| from.alias.as_deref());
    let result_columns = simple_result_columns(stmt, &table.schema, table_alias)
        .ok()?
        .into();
    Some(crate::engine::cache::ProjectedPointLookupOperator {
        table: Arc::clone(&table.schema),
        index: index.clone(),
        key: key.clone(),
        key_slots,
        key_projection,
        result_columns,
        projection: crate::engine::cache::CompiledProjectedRead {
            output_map,
            decode_plan: RowDecodePlan::compile_with_codec(
                &columns,
                table.schema.row_codec.as_ref(),
            ),
            projected_plan: ProjectedReadPlan::new(&columns, &table.schema),
            columns,
        },
    })
}

/// Execute a compiled unique-point operator.  All structural work (plan-tree
/// walking, residual-filter proof, projection analysis and decode-plan
/// construction) happened at compile time; only parameter binding, the index
/// probe and MVCC row read remain in the hot loop.
pub(crate) fn execute_compiled_projected_point_lookup(
    db: &Arc<Database>,
    txn: TxnView,
    stmt: &Select,
    operator: &crate::engine::cache::ProjectedPointLookupOperator,
) -> Result<ResultSet, QueryError> {
    let rows = execute_compiled_projected_point_lookup_rows(db, txn, stmt, operator)?;
    Ok(ResultSet {
        columns: projected_point_result_columns(stmt, operator)?.to_vec(),
        rows,
    })
}

pub(crate) fn execute_compiled_projected_point_lookup_rows(
    db: &Arc<Database>,
    txn: TxnView,
    stmt: &Select,
    operator: &crate::engine::cache::ProjectedPointLookupOperator,
) -> Result<Vec<Vec<Value>>, QueryError> {
    let offset = select_offset(stmt)?;
    let limit = select_limit(stmt)?;
    let mut rows = Vec::with_capacity(1);
    if limit != Some(0) && offset == 0 {
        if let Some(key_slots) = &operator.key_slots {
            let row = db.read_unique_index_candidate(
                &operator.table,
                &operator.index,
                operator.key.clone(),
                txn,
                |row_id, key| {
                    let projection = if key.is_some() {
                        operator
                            .key_projection
                            .as_ref()
                            .expect("key slots have a decode recipe")
                    } else {
                        &operator.projection
                    };
                    let Some(mut row) = db.read_row_columns_at_with_plan(
                        operator.table.id,
                        &operator.table,
                        row_id,
                        txn,
                        &projection.columns,
                        Some(&projection.decode_plan),
                        Some(&projection.projected_plan),
                    )?
                    else {
                        return Ok(None);
                    };
                    if let Some(key) = key {
                        for (pos, &slot) in key_slots.iter().enumerate() {
                            let column = &operator.table.columns[projection.columns[slot]];
                            let part = crate::storage::KeyPart::from_column_value_with_collation(
                                &row[slot],
                                &column.data_type,
                                column.collation.as_deref(),
                            );
                            if key.0.get(pos)
                                != Some(&operator.index.apply_prefix_to_key_part(pos, part))
                            {
                                return Ok(None);
                            }
                        }
                    }
                    row.truncate(operator.projection.columns.len());
                    if let Some(output_map) = &operator.projection.output_map {
                        row = output_map.iter().map(|slot| row[*slot].clone()).collect();
                    }
                    Ok(Some(row))
                },
            )?;
            if let Some(row) = row {
                crate::engine::perf_events::record_rows_examined(1);
                rows.push(row);
            }
            return Ok(rows);
        }
        let row_id = db.lookup_unique_index_candidate_one(
            &operator.table,
            &operator.index,
            operator.key.clone(),
            txn,
        )?;
        if let Some(row_id) = row_id {
            if let Some(mut row) = db.read_row_columns_at_with_plan(
                operator.table.id,
                &operator.table,
                row_id,
                txn,
                &operator.projection.columns,
                Some(&operator.projection.decode_plan),
                Some(&operator.projection.projected_plan),
            )? {
                if let Some(output_map) = &operator.projection.output_map {
                    row = output_map
                        .iter()
                        .map(|slot| row.get(*slot).cloned().unwrap_or(Value::Null))
                        .collect();
                }
                rows.push(row);
            }
        }
    }
    Ok(rows)
}

pub(in crate::engine::executor) fn index_order_covers_order_by(
    table: &TableSchema,
    index: &IndexSchema,
    filter: &Expr,
    order_by: &[OrderBy],
) -> bool {
    let mut eq_values = HashMap::new();
    let mut ranges = HashMap::new();
    collect_index_constraints(table, filter, &mut eq_values, &mut ranges);
    let mut index_pos = 0usize;
    while index_pos < index.columns.len()
        && eq_values.contains_key(&normalize_expr_column(&index.columns[index_pos]))
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
            if eq_values.contains_key(&index_column) {
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

pub(crate) fn projected_lookup_shape_supported(stmt: &Select) -> bool {
    stmt.derived.is_none()
        && stmt.joins.is_empty()
        && stmt.from.len() == 1
        && stmt.table.is_some()
        && stmt.group_by.is_empty()
        && stmt.having.is_none()
        && stmt.order_by.is_empty()
        && !stmt.distinct
        && !stmt.for_update
        && !select_projection_has_aggregate(&stmt.projection)
        && stmt
            .selection
            .as_ref()
            .is_none_or(|expr| !expr_has_subquery(expr))
        && stmt
            .projection
            .iter()
            .all(|item| !select_item_has_subquery(item))
}

pub(crate) fn execute_fused_single_table_operator(
    db: &Arc<Database>,
    txn: TxnView,
    stmt: &Select,
    plan: &PhysicalPlan,
) -> Result<Option<ResultSet>, QueryError> {
    // A declined kernel must not consume its input: the caller will execute
    // the general plan afterwards. In particular, multiple/unsupported
    // aggregates must be rejected before even resolving index row ids.
    let aggregate_projection = if select_projection_has_aggregate(&stmt.projection) {
        let Some((projection, _)) = single_table_aggregate_projection(stmt) else {
            return Ok(None);
        };
        Some(projection)
    } else {
        None
    };
    let source = match simple_scan_source(db, txn, plan)? {
        Some(source) => source,
        None => return Ok(None),
    };
    execute_fused_single_table_from_source(db, txn, stmt, plan, source, aggregate_projection)
}

fn execute_fused_single_table_from_source(
    db: &Arc<Database>,
    txn: TxnView,
    stmt: &Select,
    plan: &PhysicalPlan,
    source: (PlanTable, Option<Vec<RowId>>, Option<Expr>),
    aggregate_projection: Option<&Expr>,
) -> Result<Option<ResultSet>, QueryError> {
    let (table, ids, filter) = source;
    if aggregate_projection.is_some_and(|projection| {
        wrapper_refs_table_columns_outside_aggregate(projection, &table.schema)
    }) {
        return Ok(None);
    }
    let columns = single_table_result_columns(stmt, &table.schema, table.alias.as_deref())?;

    if filter.is_none() {
        if let Some(rows) =
            execute_descriptor_lob_projection(db, txn, stmt, &table.schema, ids.as_deref())?
        {
            return Ok(Some(ResultSet { columns, rows }));
        }
    }

    // Aggregate path: `project_single_table_aggregate` walks
    // `&stmt.projection` directly rather than through
    // `TableProjectionProgram`, so narrowing the heap read needs its
    // own analysis over the aggregate's argument expressions (plus the
    // residual filter). When that analysis succeeds, e.g.
    // `SELECT SUM(k) FROM sbtest1 WHERE id BETWEEN ? AND ?` reads just
    // `k` instead of decoding every column of every qualifying row.
    if select_projection_has_aggregate(&stmt.projection) {
        // Large full-scan aggregate (no id-list narrowing, scan touches
        // ≥ `MYDB_PARALLEL_THRESHOLD_ROWS` rows): bail to the planner
        // path. `collect_single_table_rows` here materialises every
        // qualifying row through `eval_bool` then re-evaluates each
        // aggregate per row in `project_single_table_aggregate` — on
        // TPC-H Q6 (6M lineitem rows, single SUM of Decimal*Decimal)
        // that's ~2.7s. The planner-routed path engages the vec
        // parallel-fused entry that streams page → SIMD filter →
        // Decimal-lane agg in one fused loop, ~10× faster. Small
        // aggregates (e.g. `SELECT COUNT(*) FROM small_table`) keep
        // the row-based fast path — its overhead is microseconds and
        // the planner has no faster shape for them. id-list-narrowed
        // aggregates (IndexLookup feeding a SUM) also keep this path:
        // their scan is already bounded, the row-fast-path is fine.
        if ids.is_none() {
            let threshold = crate::util::env::var("MYDB_PARALLEL_THRESHOLD_ROWS")
                .ok()
                .and_then(|s| s.parse::<u64>().ok())
                .unwrap_or(100_000) as f64;
            if estimated_access_rows_for_scan(plan) >= threshold {
                return Ok(None);
            }
        }
        if let Some(ids_ref) = ids.as_ref() {
            if let Some(row) =
                stream_direct_index_sum(db, txn, &table.schema, ids_ref, filter.as_ref(), stmt)?
            {
                return Ok(Some(ResultSet {
                    columns,
                    rows: vec![row],
                }));
            }
        }
        let rows = match (
            ids.as_ref(),
            aggregate_fast_path_referenced_columns(stmt, &table.schema, filter.as_ref()),
        ) {
            (Some(ids_ref), Some(referenced)) => {
                let filter_program = filter
                    .as_ref()
                    .map(|f| CompiledTableExpr::compile_once(f, &table.schema));
                collect_single_table_rows_projected(
                    db,
                    txn,
                    &table.schema,
                    ids_ref,
                    filter_program.as_ref(),
                    &referenced,
                )?
            }
            _ => collect_single_table_rows(db, txn, &table.schema, ids, filter.as_ref())?,
        };
        return Ok(
            project_single_table_aggregate(stmt, &table.schema, &rows)?.map(|row| ResultSet {
                columns,
                rows: vec![row],
            }),
        );
    }

    let projection_program = TableProjectionProgram::compile_once(&stmt.projection, &table.schema);
    let order_program = TableOrderProgram::compile_once(&stmt.order_by, &table.schema);
    let filter_program = filter
        .as_ref()
        .map(|f| CompiledTableExpr::compile_once(f, &table.schema));

    // Try to narrow the heap read to just the columns that projection +
    // filter + ORDER BY actually touch. When every expression compiled
    // (and any wildcard is absent), this skips decoding the rest of the
    // row payload — a big win for narrow point-selects on wide tables
    // (e.g. `SELECT c FROM sbtest1 WHERE id=?` over `(id, k, c, pad)`
    // skips ~180 B of `c`/`pad` decode per missed column per row).
    let referenced_columns = compute_fast_path_referenced_columns(
        &table.schema,
        &projection_program,
        &order_program,
        filter_program.as_ref(),
    );

    // An unordered LIMIT needs at most OFFSET + LIMIT qualifying rows.  Keep
    // that bound inside the storage walk instead of taking the historical
    // `scan_rows` path, which decoded and materialised the entire table before
    // applying the window below.  Besides early stop, the helper consumes the
    // projected streaming-scan API, so narrow SELECT lists do not decode
    // unrelated payload columns and skipped OFFSET rows are never copied into
    // a table-sized intermediate Vec.
    if ids.is_none() && stmt.order_by.is_empty() {
        if let Some(limit) = select_limit(stmt)? {
            let rows = collect_single_table_rows_streaming_window(
                db,
                txn,
                &table.schema,
                filter.as_ref(),
                referenced_columns.as_deref(),
                stmt,
                select_offset(stmt)?,
                limit,
            )?;
            return Ok(Some(ResultSet { columns, rows }));
        }
    }

    let rows =
        if let (Some(ids_ref), Some(referenced)) = (ids.as_ref(), referenced_columns.as_ref()) {
            if let Some(rows) = collect_single_table_rows_projected_direct(
                db,
                txn,
                &table.schema,
                ids_ref,
                filter.as_ref(),
                referenced,
                stmt,
            )? {
                return Ok(Some(ResultSet { columns, rows }));
            }
            collect_single_table_rows_projected(
                db,
                txn,
                &table.schema,
                ids_ref,
                filter_program.as_ref(),
                referenced,
            )?
        } else {
            collect_single_table_rows(db, txn, &table.schema, ids, filter.as_ref())?
        };

    let mut rows = rows
        .into_iter()
        .map(|values| {
            // Order keys evaluate against the intact source row, then
            // the projection takes ownership and moves plain column
            // values out instead of cloning them.
            let order_values = order_program.eval_keys(&table.schema, &values)?;
            let projected = projection_program.project_row_owned(&table.schema, values)?;
            Ok((projected, order_values))
        })
        .collect::<Result<Vec<_>, QueryError>>()?;

    let distinct_columns = &columns;
    let dedup_after_sort = can_dedup_distinct_after_sort(stmt);
    if stmt.distinct && !dedup_after_sort {
        let mut seen = ah_hash_set();
        rows.retain(|(values, _)| {
            let key = values
                .iter()
                .zip(distinct_columns.iter())
                .map(|(value, column)| {
                    crate::engine::expr::ValueKey::from_with_collation(
                        value,
                        column.source.collation_id,
                    )
                })
                .collect::<Vec<_>>();
            seen.insert(key)
        });
    }

    if !stmt.order_by.is_empty() {
        let sort_fields = sort_fields_with_schema(&stmt.order_by, &table.schema);
        // This fused shape never handles ORDER BY with LIMIT/OFFSET, so SQL
        // does not expose a tie-selection boundary.  The input row order is
        // deterministic and Rust's in-place unstable sort is deterministic
        // for that input, while avoiding the stable sort's scratch allocation
        // and extra moves for blocking range sorts.
        rows.sort_unstable_by(|left, right| {
            crate::engine::executor::sort::compare_sort_keys(&sort_fields, &left.1, &right.1)
        });
        if dedup_after_sort {
            rows.dedup_by(|left, right| {
                crate::engine::executor::sort::compare_sort_keys(&sort_fields, &left.1, &right.1)
                    == std::cmp::Ordering::Equal
            });
        }
    }

    let offset = select_offset(stmt)?.min(rows.len());
    let end = match select_limit(stmt)? {
        Some(limit) => offset.saturating_add(limit).min(rows.len()),
        None => rows.len(),
    };
    // Move out of the intermediate Vec rather than cloning every row.
    // The previous `iter().map(|(v,_)| v.clone())` cloned every
    // `Vec<Value>` (and every `Value::String` inside it) into a fresh
    // allocation just to drop the order-key half of the tuple. For a
    // sysbench range scan that's ~25 Vec clones × ~50 String clones per
    // query and shows up clearly as `_xzm_free` self time in
    // `execute_fused_single_table_operator`. Truncating, then draining
    // the kept range, transfers each row by ownership in one step.
    rows.truncate(end);
    let rows: Vec<Vec<Value>> = rows.drain(offset..).map(|(values, _)| values).collect();

    Ok(Some(ResultSet { columns, rows }))
}

#[derive(Clone, Copy)]
enum DescriptorLobProjection {
    Length {
        column: usize,
    },
    Substring {
        column: usize,
        start: i64,
        len: Option<i64>,
    },
    Left {
        column: usize,
        count: i64,
    },
    Right {
        column: usize,
        count: i64,
    },
}

#[cfg(test)]
static DESCRIPTOR_LOB_PROJECTION_HITS: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);

#[cfg(test)]
pub(crate) fn descriptor_lob_projection_hits_for_test() -> u64 {
    DESCRIPTOR_LOB_PROJECTION_HITS.load(std::sync::atomic::Ordering::Relaxed)
}

fn const_i64(expr: &Expr) -> Option<i64> {
    match expr {
        Expr::Literal(Literal::Int(value)) | Expr::BoundValue(Value::Int(value)) => Some(*value),
        Expr::Literal(Literal::UInt(value)) | Expr::BoundValue(Value::UInt(value)) => {
            i64::try_from(*value).ok()
        }
        _ => None,
    }
}

pub(crate) fn descriptor_lob_projection_shape_supported(stmt: &Select) -> bool {
    !stmt.projection.is_empty()
        && stmt.selection.is_none()
        && stmt.group_by.is_empty()
        && stmt.having.is_none()
        && stmt.order_by.is_empty()
        && !stmt.distinct
        && !stmt.for_update
        && stmt.projection.iter().all(|item| {
            matches!(
                item,
                SelectItem::Expr {
                    expr: Expr::Function { name, distinct: false, .. },
                    ..
                } if matches!(
                    name.to_ascii_uppercase().as_str(),
                    "LENGTH" | "OCTET_LENGTH" | "SUBSTRING" | "SUBSTR" | "MID" | "LEFT" | "RIGHT"
                )
            )
        })
}

fn compile_descriptor_lob_projection(
    stmt: &Select,
    table: &TableSchema,
) -> Option<Vec<DescriptorLobProjection>> {
    if !descriptor_lob_projection_shape_supported(stmt) || table.is_partitioned() {
        return None;
    }
    let mut out = Vec::with_capacity(stmt.projection.len());
    for item in &stmt.projection {
        let SelectItem::Expr {
            expr:
                Expr::Function {
                    name,
                    args,
                    distinct: false,
                },
            ..
        } = item
        else {
            return None;
        };
        let upper = name.to_ascii_uppercase();
        let projection = match upper.as_str() {
            "LENGTH" | "OCTET_LENGTH" => {
                let [arg] = args.as_slice() else { return None };
                let column = expr_column_index(arg, table)?;
                if !table.row_codec.is_text_or_binary_lob_column(column) {
                    return None;
                }
                DescriptorLobProjection::Length { column }
            }
            "SUBSTRING" | "SUBSTR" | "MID" => {
                let (arg, start, len) = match args.as_slice() {
                    [arg, start] => (arg, const_i64(start)?, None),
                    [arg, start, len] => (arg, const_i64(start)?, Some(const_i64(len)?)),
                    _ => return None,
                };
                let column = expr_column_index(arg, table)?;
                if !table.row_codec.is_binary_lob_column(column) {
                    return None;
                }
                DescriptorLobProjection::Substring { column, start, len }
            }
            "LEFT" | "RIGHT" => {
                let [arg, count] = args.as_slice() else {
                    return None;
                };
                let column = expr_column_index(arg, table)?;
                if !table.row_codec.is_binary_lob_column(column) {
                    return None;
                }
                let count = const_i64(count)?;
                if upper == "LEFT" {
                    DescriptorLobProjection::Left { column, count }
                } else {
                    DescriptorLobProjection::Right { column, count }
                }
            }
            _ => return None,
        };
        out.push(projection);
    }
    (!out.is_empty()).then_some(out)
}

fn execute_descriptor_lob_projection(
    db: &Database,
    txn: TxnView,
    stmt: &Select,
    table: &TableSchema,
    ids: Option<&[RowId]>,
) -> Result<Option<Vec<Vec<Value>>>, QueryError> {
    let Some(projections) = compile_descriptor_lob_projection(stmt, table) else {
        return Ok(None);
    };
    #[cfg(test)]
    DESCRIPTOR_LOB_PROJECTION_HITS.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let owned_ids;
    let ids = match ids {
        Some(ids) => ids,
        None => {
            owned_ids = db.scan_visible_row_ids(table, txn)?;
            &owned_ids
        }
    };
    let offset = select_offset(stmt)?;
    let limit = select_limit(stmt)?.unwrap_or(usize::MAX);
    if limit == 0 {
        return Ok(Some(Vec::new()));
    }
    let mut rows = Vec::new();
    let mut visible_ordinal = 0usize;
    for row_id in ids.iter().copied() {
        crate::engine::cancel::check_cancelled()?;
        let mut row = Vec::with_capacity(projections.len());
        let mut visible = true;
        for projection in &projections {
            let value = match *projection {
                DescriptorLobProjection::Length { column } => {
                    db.lob_column_byte_length_visible(table, row_id, txn, column)?
                }
                DescriptorLobProjection::Substring { column, start, len } => {
                    let Some(total_value) =
                        db.lob_column_byte_length_visible(table, row_id, txn, column)?
                    else {
                        visible = false;
                        break;
                    };
                    if matches!(total_value, Value::Null) {
                        Some(Value::Null)
                    } else {
                        let total = match total_value {
                            Value::Int(value) => value.max(0) as usize,
                            _ => 0,
                        };
                        let (start, length) =
                            crate::engine::expr::mysql_substring_byte_bounds(total, start, len);
                        db.lob_column_binary_range_visible(
                            table,
                            row_id,
                            txn,
                            column,
                            start as u64,
                            length as u64,
                        )?
                    }
                }
                DescriptorLobProjection::Left { column, count }
                | DescriptorLobProjection::Right { column, count } => {
                    let Some(total_value) =
                        db.lob_column_byte_length_visible(table, row_id, txn, column)?
                    else {
                        visible = false;
                        break;
                    };
                    if matches!(total_value, Value::Null) {
                        Some(Value::Null)
                    } else {
                        let total = match total_value {
                            Value::Int(value) => value.max(0) as usize,
                            _ => 0,
                        };
                        let count = count.max(0) as usize;
                        let length = count.min(total);
                        let start = if matches!(projection, DescriptorLobProjection::Right { .. }) {
                            total - length
                        } else {
                            0
                        };
                        db.lob_column_binary_range_visible(
                            table,
                            row_id,
                            txn,
                            column,
                            start as u64,
                            length as u64,
                        )?
                    }
                }
            };
            let Some(value) = value else {
                visible = false;
                break;
            };
            row.push(value);
        }
        if !visible {
            continue;
        }
        if visible_ordinal < offset {
            visible_ordinal += 1;
            continue;
        }
        visible_ordinal += 1;
        rows.push(row);
        if rows.len() >= limit {
            break;
        }
    }
    Ok(Some(rows))
}

/// Accumulate a direct scalar aggregate while visiting an index-produced row
/// id batch.  The normal small-range path first materialises one owned
/// `Vec<Value>` per row and then immediately consumes those rows into the
/// aggregate.  This visitor instead reuses the heap decoder's projected-value
/// scratch buffer and feeds the shared aggregate state directly.
///
/// Keep the capability deliberately semantic rather than workload-specific:
/// one unpartitioned table, no residual predicate, a bare non-DISTINCT
/// SUM over one source column.  Everything else keeps
/// `project_single_table_aggregate` as the correctness oracle.
fn stream_direct_index_sum(
    db: &Database,
    txn: TxnView,
    table: &TableSchema,
    ids: &[RowId],
    filter: Option<&Expr>,
    stmt: &Select,
) -> Result<Option<Vec<Value>>, QueryError> {
    if table.is_partitioned() || !stmt.order_by.is_empty() {
        return Ok(None);
    }
    let [SelectItem::Expr { expr, .. }] = stmt.projection.as_slice() else {
        return Ok(None);
    };
    let Expr::Function {
        name,
        args,
        distinct,
    } = expr
    else {
        return Ok(None);
    };
    if *distinct {
        return Ok(None);
    }
    if !name.eq_ignore_ascii_case("SUM") {
        return Ok(None);
    }
    let [arg] = args.as_slice() else {
        return Ok(None);
    };
    let Some(column) = expr_column_index(arg, table) else {
        return Ok(None);
    };
    // Keep the MVCC residual predicate: another transaction can change an
    // indexed key while our visible row version still carries its old value.
    // Compile against the compact read schema and aggregate in the visitor,
    // rather than materialising and expanding every qualifying row.
    let mut columns = smallvec::smallvec![column];
    let filter_program = match filter {
        Some(filter) => {
            let mut referenced = std::collections::HashSet::from([column]);
            if !CompiledTableExpr::compile_once(filter, table)
                .try_collect_referenced_columns(&mut referenced)
            {
                return Ok(None);
            }
            columns = referenced
                .into_iter()
                .collect::<smallvec::SmallVec<[usize; 8]>>();
            columns.sort_unstable();
            let schema = super::plan::projected_schema_view(table, &columns);
            match super::plan::CompiledPredicate::compile_for_storage(filter, &schema) {
                Some(program) => Some(program),
                None => return Ok(None),
            }
        }
        None => None,
    };
    let sum_slot = columns
        .iter()
        .position(|candidate| *candidate == column)
        .unwrap();

    let mut sorted_ids = Vec::new();
    let ordered_ids = if ids.windows(2).all(|pair| pair[0] <= pair[1]) {
        ids
    } else {
        sorted_ids.extend_from_slice(ids);
        sorted_ids.sort_unstable();
        sorted_ids.as_slice()
    };
    let heap = db.cached_heap_handle(table.id)?;
    let mut sum = super::aggregate::NumericAggState::default();
    let mut sum_seen = false;
    let accepted = db.read_row_columns_many_predicate_first_with_handle(
        &heap,
        table,
        ordered_ids,
        txn,
        &columns,
        &[],
        |values| {
            if let Some(filter) = &filter_program {
                if !filter.eval(values)? {
                    return Ok(false);
                }
            }
            let value = values.get(sum_slot).unwrap_or(&Value::Null);
            if !matches!(value, Value::Null) {
                super::aggregate::accumulate_numeric_state(&mut sum, value)?;
                sum_seen = true;
            }
            // The predicate-first reader materialises output columns only for
            // accepted rows.  Returning false turns it into a pure visitor.
            Ok(false)
        },
    )?;
    debug_assert!(accepted.is_empty());

    let value = if sum_seen {
        super::aggregate::numeric_state_to_value(&sum)
    } else {
        Value::Null
    };
    Ok(Some(vec![value]))
}

pub(crate) fn fused_single_table_shape_supported(stmt: &Select) -> bool {
    stmt.derived.is_none()
        && stmt.joins.is_empty()
        && stmt.from.len() == 1
        && stmt.table.is_some()
        && stmt.group_by.is_empty()
        && stmt.having.is_none()
        && !stmt.for_update
        && stmt
            .selection
            .as_ref()
            .is_none_or(|expr| !expr_has_subquery(expr))
        && stmt
            .projection
            .iter()
            .all(|item| !select_item_has_subquery(item))
        // DISTINCT aggregate equality is collation-aware.  The general
        // aggregate implementation installs that comparison context; this
        // compact row kernel currently does not, so keep it as the oracle.
        && !crate::planner::collect_aggregate_calls(stmt)
            .iter()
            .any(|expr| matches!(expr, Expr::Function { distinct: true, .. }))
        && !(!stmt.order_by.is_empty() && (stmt.limit.is_some() || stmt.offset.is_some()))
        && stmt
            .order_by
            .iter()
            .all(|item| !expr_has_subquery(&item.expr))
        && (!select_projection_has_aggregate(&stmt.projection)
            || single_table_aggregate_projection(stmt).is_some())
}

/// Walk the compiled projection, ORDER BY, and filter programs and
/// return the sorted, deduplicated list of column indices they read,
/// or `None` if any sub-expression couldn't be analyzed (and therefore
/// the caller must read every column to stay correct). Returns `None`
/// also when the union already covers every visible column — the
/// narrowed-read path adds an expand-to-schema-aligned copy per row,
/// so it only pays off if we actually skip some columns.
fn compute_fast_path_referenced_columns(
    table: &TableSchema,
    projection: &TableProjectionProgram,
    order: &TableOrderProgram,
    filter: Option<&CompiledTableExpr>,
) -> Option<Vec<usize>> {
    let mut set = std::collections::HashSet::new();
    if !projection.try_collect_referenced_columns(table, &mut set) {
        return None;
    }
    if !order.try_collect_referenced_columns(&mut set) {
        return None;
    }
    if let Some(f) = filter {
        if !f.try_collect_referenced_columns(&mut set) {
            return None;
        }
    }
    let visible_count = table.iter_visible_columns().count();
    if set.len() >= visible_count {
        return None;
    }
    let mut cols: Vec<usize> = set.into_iter().collect();
    cols.sort_unstable();
    Some(cols)
}

/// Referenced-column analysis for the aggregate fast path: the union
/// of columns read by the lone aggregate's arguments and the residual
/// filter. Returns `None` (caller keeps the full-row read) when the
/// projection isn't the lone-aggregate shape
/// `project_single_table_aggregate` accepts, when any expression can't
/// be analyzed, or when narrowing wouldn't skip any column.
fn aggregate_fast_path_referenced_columns(
    stmt: &Select,
    table: &TableSchema,
    filter: Option<&Expr>,
) -> Option<Vec<usize>> {
    let [SelectItem::Expr {
        expr: projection_expr,
        ..
    }] = stmt.projection.as_slice()
    else {
        return None;
    };
    let aggregate = find_lone_aggregate_call(projection_expr)?;
    // A wrapper that reads table columns outside the aggregate makes
    // `project_single_table_aggregate` reject the statement later, so
    // the narrowed read would be wasted work — keep the legacy path.
    if wrapper_refs_table_columns_outside_aggregate(projection_expr, table) {
        return None;
    }
    let Expr::Function { args, .. } = &aggregate else {
        return None;
    };
    let mut set = std::collections::HashSet::new();
    for arg in args {
        if !CompiledTableExpr::compile_once(arg, table).try_collect_referenced_columns(&mut set) {
            return None;
        }
    }
    if let Some(f) = filter {
        if !CompiledTableExpr::compile_once(f, table).try_collect_referenced_columns(&mut set) {
            return None;
        }
    }
    let visible_count = table.iter_visible_columns().count();
    if set.is_empty() || set.len() >= visible_count {
        return None;
    }
    let mut cols: Vec<usize> = set.into_iter().collect();
    cols.sort_unstable();
    Some(cols)
}

/// Column-narrowed counterpart of `collect_single_table_rows` for the
/// `ids`-driven branch (i.e. index lookup produced a row id list).
/// Reads only the projected columns from each row's heap payload,
/// expands the compact result into a schema-aligned `Vec<Value>` so
/// the existing `projection_program.project_row` / filter / ORDER BY
/// code paths can index by schema column position, and applies the
/// (compiled) residual filter per row.
fn collect_single_table_rows_projected(
    db: &Database,
    txn: TxnView,
    table: &TableSchema,
    ids: &[RowId],
    filter: Option<&CompiledTableExpr>,
    referenced: &[usize],
) -> Result<Vec<Vec<Value>>, QueryError> {
    let schema_len = table.columns.len();
    let mut rows = Vec::with_capacity(ids.len());
    let mut push_row = |compact: Vec<Value>| -> Result<(), QueryError> {
        let mut schema_aligned = vec![Value::Null; schema_len];
        for (i, value) in compact.into_iter().enumerate() {
            let col_idx = referenced[i];
            if col_idx < schema_len {
                schema_aligned[col_idx] = value;
            }
        }
        if let Some(f) = filter {
            if !crate::engine::expr::truthy(&f.eval(table, &schema_aligned)?) {
                return Ok(());
            }
        }
        rows.push(schema_aligned);
        Ok(())
    };
    if !table.is_partitioned() {
        // Batched read: one page pin per page run instead of one per
        // row, and the column decode plan is compiled once for the
        // whole id list instead of per row.
        let heap = db.cached_heap_handle(table.id)?;
        let mut pairs = db.read_row_columns_many_with_handle(&heap, table, ids, txn, referenced)?;
        restore_row_id_order(&mut pairs, ids, |(row_id, _)| *row_id);
        for (_row_id, compact) in pairs {
            push_row(compact)?;
        }
        return Ok(rows);
    }
    for &row_id in ids {
        let Some(compact) = db.read_row_columns(table, row_id, txn, referenced)? else {
            continue;
        };
        push_row(compact)?;
    }
    Ok(rows)
}

fn collect_single_table_rows_projected_direct(
    db: &Database,
    txn: TxnView,
    table: &TableSchema,
    ids: &[RowId],
    filter: Option<&Expr>,
    referenced: &[usize],
    stmt: &Select,
) -> Result<Option<Vec<Vec<Value>>>, QueryError> {
    // Plain-column projections with no residual predicate already have all
    // values in `referenced` order.  Keep that compact schema as the execution
    // schema: constructing `projected_schema_view` clones catalog strings,
    // row-layout metadata and statistics once per execution, while evaluating
    // ORDER BY into a second `Vec<Value>` clones every string key once per
    // row.  Mapping source-column ordinals to compact slots lets us sort the
    // owned rows in place and move the selected values out afterwards.
    //
    // DISTINCT stays on this lane only when the ORDER BY tuple is exactly the
    // projected tuple.  In that proven shape adjacent-key dedup is the legacy
    // oracle; less direct DISTINCT forms retain the hash/projection path below.
    let compact_recipe = (|| -> Option<(
        smallvec::SmallVec<[usize; 8]>,
        smallvec::SmallVec<[usize; 8]>,
        bool,
    )> {
        if stmt.distinct && !can_dedup_distinct_after_sort(stmt) {
            return None;
        }
        let mut projection_slots = smallvec::SmallVec::<[usize; 8]>::new();
        for item in &stmt.projection {
            let SelectItem::Expr { expr, .. } = item else {
                return None;
            };
            let column = expr_column_index(expr, table)?;
            let slot = referenced.iter().position(|candidate| *candidate == column)?;
            if projection_slots.contains(&slot) {
                return None;
            }
            projection_slots.push(slot);
        }
        let mut order_slots = smallvec::SmallVec::<[usize; 8]>::new();
        for item in &stmt.order_by {
            let column = expr_column_index(&item.expr, table)?;
            order_slots.push(referenced.iter().position(|candidate| *candidate == column)?);
        }
        Some((
            projection_slots,
            order_slots,
            stmt.distinct && can_dedup_distinct_after_sort(stmt),
        ))
    })();

    if let Some((projection_slots, order_slots, dedup_after_sort)) = compact_recipe {
        let filter_program = match filter {
            Some(filter) => {
                let schema = super::plan::projected_schema_view(table, referenced);
                match super::plan::CompiledPredicate::compile(filter, &schema) {
                    Some(program) => Some(program),
                    None => return Ok(None),
                }
            }
            None => None,
        };
        let mut pairs = if !table.is_partitioned() {
            let heap = db.cached_heap_handle(table.id)?;
            db.read_row_columns_many_with_handle(&heap, table, ids, txn, referenced)?
        } else {
            let mut pairs = Vec::with_capacity(ids.len());
            for &row_id in ids {
                if let Some(row) = db.read_row_columns(table, row_id, txn, referenced)? {
                    pairs.push((row_id, row));
                }
            }
            pairs
        };
        restore_row_id_order(&mut pairs, ids, |(row_id, _)| *row_id);
        let mut rows = if let Some(filter) = &filter_program {
            let mut rows = Vec::with_capacity(pairs.len());
            for (_, values) in pairs {
                if filter.eval(&values)? {
                    rows.push(values);
                }
            }
            rows
        } else {
            pairs.into_iter().map(|(_, values)| values).collect()
        };

        if !order_slots.is_empty() {
            let sort_fields = sort_fields_with_schema(&stmt.order_by, table);
            rows.sort_unstable_by(|left, right| {
                for (field, slot) in sort_fields.iter().zip(&order_slots) {
                    let ord = crate::engine::executor::sort::compare_value_for_sort(
                        left.get(*slot).unwrap_or(&Value::Null),
                        right.get(*slot).unwrap_or(&Value::Null),
                        *field,
                    );
                    if ord != std::cmp::Ordering::Equal {
                        return ord;
                    }
                }
                std::cmp::Ordering::Equal
            });
            if dedup_after_sort {
                rows.dedup_by(|left, right| {
                    sort_fields.iter().zip(&order_slots).all(|(field, slot)| {
                        crate::engine::executor::sort::compare_value_for_sort(
                            left.get(*slot).unwrap_or(&Value::Null),
                            right.get(*slot).unwrap_or(&Value::Null),
                            *field,
                        ) == std::cmp::Ordering::Equal
                    })
                });
            }
        }

        let identity_projection = projection_slots.len() == referenced.len()
            && projection_slots
                .iter()
                .enumerate()
                .all(|(output, slot)| output == *slot);
        if !identity_projection {
            // Each source slot occurs once (checked above). Compile a
            // permutation once, then compact each owned row in place. Moving
            // into a new Vec paid an allocation per row even when projection
            // only dropped the residual-predicate column.
            let mut positions: smallvec::SmallVec<[usize; 8]> = (0..referenced.len()).collect();
            let mut swaps = smallvec::SmallVec::<[(usize, usize); 8]>::new();
            for (output, source) in projection_slots.iter().enumerate() {
                let current = positions.iter().position(|slot| slot == source).unwrap();
                if current != output {
                    positions.swap(output, current);
                    swaps.push((output, current));
                }
            }
            for values in &mut rows {
                if values.len() < referenced.len() {
                    values.resize(referenced.len(), Value::Null);
                }
                for &(output, current) in &swaps {
                    values.swap(output, current);
                }
                values.truncate(projection_slots.len());
            }
        }

        let offset = select_offset(stmt)?.min(rows.len());
        let end = match select_limit(stmt)? {
            Some(limit) => offset.saturating_add(limit).min(rows.len()),
            None => rows.len(),
        };
        rows.truncate(end);
        if offset != 0 {
            rows.drain(..offset);
        }
        return Ok(Some(rows));
    }

    let projected_schema = super::plan::projected_schema_view(table, referenced);
    let filter_program = match filter {
        Some(expr) => match super::plan::CompiledPredicate::compile(expr, &projected_schema) {
            Some(compiled) => Some(compiled),
            None => return Ok(None),
        },
        None => None,
    };
    let projection_program =
        TableProjectionProgram::compile_once(&stmt.projection, &projected_schema);
    let order_program = TableOrderProgram::compile_once(&stmt.order_by, &projected_schema);
    if projection_program.move_plan.is_none() {
        return Ok(None);
    }

    let mut pairs = if !table.is_partitioned() {
        let heap = db.cached_heap_handle(table.id)?;
        db.read_row_columns_many_with_handle(&heap, table, ids, txn, referenced)?
    } else {
        let mut pairs = Vec::with_capacity(ids.len());
        for &row_id in ids {
            if let Some(row) = db.read_row_columns(table, row_id, txn, referenced)? {
                pairs.push((row_id, row));
            }
        }
        pairs
    };
    restore_row_id_order(&mut pairs, ids, |(row_id, _)| *row_id);
    let mut rows = Vec::with_capacity(pairs.len());
    for (_row_id, values) in pairs {
        if let Some(filter) = &filter_program {
            if !filter.eval(&values)? {
                continue;
            }
        }
        let order_values = order_program.eval_keys(&projected_schema, &values)?;
        let projected = projection_program.project_row_owned(&projected_schema, values)?;
        rows.push((projected, order_values));
    }

    let distinct_columns = if stmt.distinct {
        single_table_result_columns(stmt, table, None)?
    } else {
        Vec::new()
    };
    let dedup_after_sort = can_dedup_distinct_after_sort(stmt);
    if stmt.distinct && !dedup_after_sort {
        let mut seen = ah_hash_set();
        rows.retain(|(values, _)| {
            let key = values
                .iter()
                .zip(distinct_columns.iter())
                .map(|(value, column)| {
                    crate::engine::expr::ValueKey::from_with_collation(
                        value,
                        column.source.collation_id,
                    )
                })
                .collect::<Vec<_>>();
            seen.insert(key)
        });
    }

    if !stmt.order_by.is_empty() {
        let sort_fields = sort_fields_with_schema(&stmt.order_by, &projected_schema);
        // See the fused caller above: this path also rejects an ordered
        // LIMIT/OFFSET, so equal-key stability is not semantically observable.
        rows.sort_unstable_by(|left, right| {
            crate::engine::executor::sort::compare_sort_keys(&sort_fields, &left.1, &right.1)
        });
        if dedup_after_sort {
            rows.dedup_by(|left, right| {
                crate::engine::executor::sort::compare_sort_keys(&sort_fields, &left.1, &right.1)
                    == std::cmp::Ordering::Equal
            });
        }
    }

    let offset = select_offset(stmt)?.min(rows.len());
    let end = match select_limit(stmt)? {
        Some(limit) => offset.saturating_add(limit).min(rows.len()),
        None => rows.len(),
    };
    rows.truncate(end);
    Ok(Some(
        rows.drain(offset..).map(|(values, _)| values).collect(),
    ))
}

/// Fused full-scan path for unordered `LIMIT [OFFSET]`.
///
/// The storage callback sees only the columns needed by WHERE + projection,
/// evaluates the filter before counting the LIMIT window, and returns `true`
/// as soon as the requested number of qualifying (and, when requested,
/// distinct) rows has been produced.  This keeps work proportional to the
/// requested window rather than table cardinality.
fn collect_single_table_rows_streaming_window(
    db: &Database,
    txn: TxnView,
    table: &TableSchema,
    filter: Option<&Expr>,
    referenced: Option<&[usize]>,
    stmt: &Select,
    offset: usize,
    limit: usize,
) -> Result<Vec<Vec<Value>>, QueryError> {
    if limit == 0 {
        return Ok(Vec::new());
    }

    // `None` means reference analysis could not safely narrow the row, or
    // narrowing would cover every logical column anyway.  Still go through
    // the projected streaming API: unlike `scan_rows`, it does not build a
    // Vec<StoredRow> for the complete heap and supports callback early-stop.
    let all_columns;
    let scan_columns = match referenced {
        Some(columns) => columns,
        None => {
            all_columns = super::plan::logical_visible_column_indices(table);
            &all_columns
        }
    };
    let projected_schema = super::plan::projected_schema_view(table, scan_columns);
    let filter_program =
        filter.map(|expr| CompiledTableExpr::compile_once(expr, &projected_schema));
    let projection_program =
        TableProjectionProgram::compile_once(&stmt.projection, &projected_schema);
    let qualified = db.storage_key_for_table(table);
    let mut rows = Vec::with_capacity(limit.min(64));
    let mut qualified_rows = 0usize;
    let distinct_columns = if stmt.distinct {
        single_table_result_columns(stmt, table, None)?
    } else {
        Vec::new()
    };
    let mut seen = stmt.distinct.then(ah_hash_set);

    db.scan_rows_columns_streaming_until(&qualified, txn, scan_columns, |values| {
        crate::engine::cancel::check_cancelled()?;
        if let Some(filter) = &filter_program {
            if !crate::engine::expr::truthy(&filter.eval(&projected_schema, values)?) {
                return Ok(false);
            }
        }

        // Without DISTINCT, OFFSET rows need neither projection nor an owned
        // copy.  DISTINCT must project first because duplicate semantics are
        // defined over the SELECT list.
        if seen.is_none() && qualified_rows < offset {
            qualified_rows = qualified_rows.saturating_add(1);
            return Ok(false);
        }

        let projected = projection_program.project_row(&projected_schema, values)?;
        if let Some(seen) = seen.as_mut() {
            let key = projected
                .iter()
                .zip(distinct_columns.iter())
                .map(|(value, column)| {
                    crate::engine::expr::ValueKey::from_with_collation(
                        value,
                        column.source.collation_id,
                    )
                })
                .collect::<Vec<_>>();
            if !seen.insert(key) {
                return Ok(false);
            }
            if qualified_rows < offset {
                qualified_rows = qualified_rows.saturating_add(1);
                return Ok(false);
            }
        }

        qualified_rows = qualified_rows.saturating_add(1);
        rows.push(projected);
        Ok(rows.len() >= limit)
    })?;
    Ok(rows)
}

/// `read_row_columns_many_with_handle` returns buffer-pool hits in
/// input order followed by any fallback (page-miss / version-chain)
/// rows, so the combined result can come back out of input order. The
/// fast path's result order is observable when the statement has no
/// ORDER BY, so restore it. The common all-hits case is verified with
/// an allocation-free subsequence walk; only the rare mixed case pays
/// for a position map and sort.
fn restore_row_id_order<T>(items: &mut [T], ids: &[RowId], key: impl Fn(&T) -> RowId) {
    if items.len() <= 1 {
        return;
    }
    let mut tail = ids.iter();
    if items.iter().all(|item| {
        let row_id = key(item);
        tail.any(|id| *id == row_id)
    }) {
        return;
    }
    let positions: AhHashMap<RowId, usize> =
        ids.iter().enumerate().map(|(pos, id)| (*id, pos)).collect();
    items.sort_by_key(|item| positions.get(&key(item)).copied().unwrap_or(usize::MAX));
}

pub(super) fn collect_single_table_rows(
    db: &Database,
    txn: TxnView,
    table: &TableSchema,
    ids: Option<Vec<RowId>>,
    filter: Option<&Expr>,
) -> Result<Vec<Vec<Value>>, QueryError> {
    let mut rows = Vec::new();
    if let Some(ids) = ids {
        if !table.is_partitioned() {
            // Same batched-read rationale as
            // `collect_single_table_rows_projected`: page pins and the
            // decode plan amortize over the whole id list.
            let heap = db.cached_heap_handle(table.id)?;
            let mut stored = db.read_rows_many_with_handle(&heap, table, &ids, txn)?;
            restore_row_id_order(&mut stored, &ids, |row| row.row_id);
            for row in stored {
                if filter
                    .map(|expr| eval_bool(expr, table, &row.values))
                    .transpose()?
                    .unwrap_or(true)
                {
                    rows.push(row.values);
                }
            }
            return Ok(rows);
        }
        for row_id in ids {
            let Some(row) = db.read_row(table, row_id, txn)? else {
                continue;
            };
            if filter
                .map(|expr| eval_bool(expr, table, &row.values))
                .transpose()?
                .unwrap_or(true)
            {
                rows.push(row.values);
            }
        }
        return Ok(rows);
    }

    // Partition pruning: when the filter exposes equality/range/IN on
    // the partition key, only visit the matching partition heaps.
    let pruned =
        crate::engine::executor::partition_router::pruned_storage_ids_from_filter(table, filter);
    let storage_key = db.storage_key_for_table(table);
    let scan_call = || db.scan_rows(&storage_key, txn);
    let stored = match pruned {
        Some(ids) => {
            crate::engine::executor::partition_router::with_pruned_partitions(ids, scan_call)
        }
        None => scan_call(),
    }?;
    for row in stored {
        if filter
            .map(|expr| eval_bool(expr, table, &row.values))
            .transpose()?
            .unwrap_or(true)
        {
            rows.push(row.values);
        }
    }
    Ok(rows)
}

/// Shared metadata-only preflight for the compact aggregate evaluator.
/// Keep this in sync with `compute_single_table_aggregate_value`: rejection
/// after reading rows can duplicate both storage work and SQL diagnostics.
fn single_table_aggregate_projection(stmt: &Select) -> Option<(&Expr, Expr)> {
    let [SelectItem::Expr {
        expr: projection_expr,
        ..
    }] = stmt.projection.as_slice()
    else {
        return None;
    };
    if !stmt.order_by.is_empty() {
        return None;
    }

    // Find a single non-nested aggregate node anywhere in the projection
    // expression. Bare `SUM(x)` and wrapped `0.5 * SUM(x)` / `MAX(y) + 1`
    // / `100 - COUNT(*)` all qualify; multiple aggregates, nested ones,
    // and embedded subqueries / window functions reject. The wrapper
    // outside the aggregate must not touch other table columns —
    // there's only one aggregate result available at the end, so a
    // per-row column reference outside the aggregate would silently
    // see only the last row.
    let aggregate = find_lone_aggregate_call(projection_expr)?;
    let Expr::Function {
        name,
        args,
        distinct,
    } = &aggregate
    else {
        return None;
    };
    if *distinct
        || args.len() != 1
        || !matches!(
            name.to_ascii_uppercase().as_str(),
            "COUNT" | "SUM" | "AVG" | "MIN" | "MAX" | "GROUP_CONCAT"
        )
        || args.iter().any(|arg| {
            matches!(arg, Expr::Function { name, .. }
            if name.eq_ignore_ascii_case(crate::parser::GROUP_CONCAT_ORDERED_FN))
        })
    {
        return None;
    }
    Some((projection_expr, aggregate))
}

pub(super) fn project_single_table_aggregate(
    stmt: &Select,
    table: &TableSchema,
    rows: &[Vec<Value>],
) -> Result<Option<Vec<Value>>, QueryError> {
    let Some((projection_expr, aggregate)) = single_table_aggregate_projection(stmt) else {
        return Ok(None);
    };
    if wrapper_refs_table_columns_outside_aggregate(projection_expr, table) {
        return Ok(None);
    }
    let Expr::Function {
        name,
        args,
        distinct,
    } = &aggregate
    else {
        return Ok(None);
    };

    let Some(agg_value) = compute_single_table_aggregate_value(name, args, *distinct, table, rows)?
    else {
        return Ok(None);
    };

    if let Expr::Function {
        name: inspector,
        args: inspector_args,
        ..
    } = projection_expr
    {
        if inspector_args.len() == 1
            && (inspector.eq_ignore_ascii_case("CHARSET")
                || inspector.eq_ignore_ascii_case("COLLATION"))
        {
            if let Some(collation) =
                crate::engine::expr::expr_type_descriptor(&aggregate, table).collation
            {
                if let Some(info) = crate::charset::resolve_collation_id(collation.collation_id) {
                    let metadata = if inspector.eq_ignore_ascii_case("CHARSET") {
                        info.charset
                    } else {
                        info.name
                    };
                    return Ok(Some(vec![Value::String(metadata.to_string())]));
                }
            }
        }
    }

    // Bare aggregate: skip the wrapper substitution path.
    if is_aggregate_node(projection_expr) {
        return Ok(Some(vec![agg_value]));
    }

    // Wrapped aggregate: substitute the aggregate node with its result
    // and evaluate the now-constant wrapper. The wrapper may reference
    // placeholders / bound values; `eval_const_runtime` resolves those
    // from the thread-local parameter context.
    let mut replacement = Expr::BoundValue(agg_value);
    if let Some(collation) = crate::engine::expr::expr_type_descriptor(&aggregate, table).collation
    {
        if let Some(info) = crate::charset::resolve_collation_id(collation.collation_id) {
            replacement = Expr::Function {
                name: "__COLLATED_CONST".to_string(),
                args: vec![
                    replacement,
                    Expr::Literal(crate::parser::Literal::String(info.name.to_string())),
                    Expr::Literal(crate::parser::Literal::Int(collation.coercibility as i64)),
                ],
                distinct: false,
            };
        }
    }
    let wrapped = substitute_first_aggregate_node(projection_expr, &replacement);
    let final_value = crate::engine::expr::eval_const_runtime(&wrapped)?;
    Ok(Some(vec![final_value]))
}

fn compute_single_table_aggregate_value(
    name: &str,
    args: &[Expr],
    distinct: bool,
    table: &TableSchema,
    rows: &[Vec<Value>],
) -> Result<Option<Value>, QueryError> {
    if !planner::is_aggregate_function(name) {
        return Ok(None);
    }
    // `GROUP_CONCAT(... ORDER BY ...)` is encoded by the parser as
    // `GROUP_CONCAT(__gc_ordered(value, ord1, "ASC"|"DESC", ...))` — this
    // fast path doesn't carry per-row sort-key buffering, so defer to the
    // slot-based aggregate pipeline which already understands the wrapper.
    if args.iter().any(|arg| {
        matches!(
            arg,
            Expr::Function { name, .. }
                if name.eq_ignore_ascii_case(crate::parser::GROUP_CONCAT_ORDERED_FN)
        )
    }) {
        return Ok(None);
    }
    let compiled_arg = if args.len() == 1 && !matches!(args[0], Expr::Wildcard) {
        Some(CompiledTableExpr::compile_once(&args[0], table))
    } else {
        None
    };
    let mut seen = if distinct {
        Some(ah_hash_set::<crate::engine::expr::ValueKey>())
    } else {
        None
    };
    let upper = name.to_ascii_uppercase();
    let comparison_collation = if matches!(upper.as_str(), "MIN" | "MAX") {
        args.first()
            .and_then(|arg| crate::engine::expr::expr_collation(arg, table))
            .map(|info| info.collation_id)
    } else {
        None
    };
    let mut result_count = 0usize;
    let mut sum_total = super::aggregate::NumericAggState::default();
    let mut sum_seen = false;
    let mut best: Option<Value> = None;
    let mut group_concat: Vec<String> = Vec::new();

    for row in rows {
        let value = if args.is_empty() {
            Value::Null
        } else if args.len() == 1 && matches!(args[0], Expr::Wildcard) {
            Value::Int(1)
        } else if let Some(compiled) = compiled_arg.as_ref() {
            compiled.eval(table, row)?
        } else {
            eval_expr(&args[0], table, row)?
        };

        if let Some(seen) = seen.as_mut() {
            if !seen.insert(crate::engine::expr::ValueKey::from(&value)) {
                continue;
            }
        }

        match upper.as_str() {
            "COUNT" if args.len() == 1 && matches!(args[0], Expr::Wildcard) => {
                result_count += 1;
            }
            "COUNT" => {
                if !matches!(value, Value::Null) {
                    result_count += 1;
                }
            }
            "SUM" => {
                if !matches!(value, Value::Null) {
                    super::aggregate::accumulate_numeric_state(&mut sum_total, &value)?;
                    sum_seen = true;
                }
            }
            "AVG" => {
                if !matches!(value, Value::Null) {
                    super::aggregate::accumulate_numeric_state(&mut sum_total, &value)?;
                    result_count += 1;
                }
            }
            "MIN" | "MAX" => {
                if matches!(value, Value::Null) {
                    continue;
                }
                let replace = match best.as_ref() {
                    None => true,
                    Some(current) => {
                        let ordering = crate::engine::expr::with_optional_compare_collation(
                            comparison_collation,
                            || compare_values(&value, current),
                        )?;
                        (upper == "MAX" && ordering == Ordering::Greater)
                            || (upper == "MIN" && ordering == Ordering::Less)
                    }
                };
                if replace {
                    best = Some(value);
                }
            }
            "GROUP_CONCAT" => {
                if !matches!(value, Value::Null) {
                    group_concat.push(value_to_string(&value));
                }
            }
            _ => return Ok(None),
        }
    }

    let value = match upper.as_str() {
        "COUNT" if args.len() == 1 && matches!(args[0], Expr::Wildcard) => {
            Value::Int(rows.len() as i64)
        }
        "COUNT" => Value::Int(result_count as i64),
        "SUM" => {
            if sum_seen {
                super::aggregate::numeric_state_to_value(&sum_total)
            } else {
                Value::Null
            }
        }
        "AVG" => super::aggregate::finalize_avg_value(&sum_total, result_count as u64),
        "MIN" | "MAX" => best.unwrap_or(Value::Null),
        "GROUP_CONCAT" => {
            if group_concat.is_empty() {
                Value::Null
            } else {
                let max_len = super::aggregate::group_concat_max_len();
                let mut out = String::new();
                let mut first = true;
                let mut truncated_row = None;
                for (index, part) in group_concat.iter().enumerate() {
                    if !first {
                        out.push(',');
                    }
                    first = false;
                    out.push_str(part);
                    if out.len() > max_len
                        || (out.len() == max_len && index + 1 < group_concat.len())
                    {
                        truncated_row = Some(if out.len() > max_len {
                            index + 1
                        } else {
                            index + 2
                        });
                        break;
                    }
                }
                if out.len() > max_len {
                    let mut cut = max_len;
                    while cut > 0 && !out.is_char_boundary(cut) {
                        cut -= 1;
                    }
                    out.truncate(cut);
                }
                if let Some(row) = truncated_row {
                    crate::engine::api::push_statement_warning(
                        crate::error::ConditionLevel::Warning,
                        crate::error::catalog::ER_CUT_VALUE_GROUP_CONCAT,
                        format!("Row {row} was cut by GROUP_CONCAT()"),
                    );
                }
                if crate::engine::expr::string_result_exceeds_packet(
                    "group_concat",
                    out.len() as u64,
                ) {
                    Value::Null
                } else {
                    Value::String(out)
                }
            }
        }
        _ => return Ok(None),
    };
    Ok(Some(value))
}

fn is_aggregate_node(expr: &Expr) -> bool {
    matches!(expr, Expr::Function { name, .. } if planner::is_aggregate_function(name))
}

/// Find a unique non-nested aggregate node anywhere in `expr`. Returns
/// `None` when there are zero aggregates, multiple aggregates, nested
/// aggregates (e.g. `SUM(MAX(x))`), or embedded subqueries / window
/// functions.
fn find_lone_aggregate_call(expr: &Expr) -> Option<Expr> {
    use crate::parser::visit::{any_expr_matching, scalar_expr_children};

    let mut count = 0;
    let rejected = any_expr_matching(
        expr,
        |node| {
            // A second aggregate also covers nested aggregates. Inspect the
            // arguments of the first one so unsupported nodes cannot hide there.
            if is_aggregate_node(node) {
                count += 1;
            }
            count > 1
                || matches!(
                    node,
                    Expr::Subquery(_)
                        | Expr::InSubquery { .. }
                        | Expr::QuantifiedComparison { .. }
                        | Expr::Exists { .. }
                        | Expr::WindowFunction { .. }
                )
        },
        |node| scalar_expr_children(node) || matches!(node, Expr::Tuple(_)),
    );
    if rejected || count != 1 {
        return None;
    }
    // Clone only after validation; rejected expressions allocate nothing.
    let mut found = None;
    any_expr_matching(
        expr,
        |node| {
            if is_aggregate_node(node) {
                found = Some(node.clone());
                true
            } else {
                false
            }
        },
        |node| scalar_expr_children(node) || matches!(node, Expr::Tuple(_)),
    );
    found
}

/// True when the wrapper expression mentions a column of `table` outside
/// of the aggregate node. Any such reference would need a per-row value
/// at evaluation time — the fast path collapses the aggregate to a
/// single value, so per-row references can't be reconstructed.
fn wrapper_refs_table_columns_outside_aggregate(expr: &Expr, table: &TableSchema) -> bool {
    let mut skipped = false;
    crate::parser::visit::try_inspect_value_expr(expr, |node| {
        if !skipped && is_aggregate_node(node) {
            skipped = true;
            return Ok(false);
        }
        let column = match node {
            Expr::Identifier(name) => Some(name.as_str()),
            Expr::CompoundIdentifier(parts) if parts.len() >= 2 => parts.last().map(String::as_str),
            _ => None,
        };
        if column.is_some_and(|name| table.column_index(name).is_ok()) {
            return Err(());
        }
        // Preserve this check's scalar-only scope (including its tuple boundary).
        Ok(crate::parser::visit::scalar_expr_children(node))
    })
    .is_err()
}

fn substitute_first_aggregate_node(expr: &Expr, replacement: &Expr) -> Expr {
    let mut done = false;
    walk_substitute_first_agg(expr, replacement, &mut done)
}

fn walk_substitute_first_agg(expr: &Expr, replacement: &Expr, done: &mut bool) -> Expr {
    if !*done && is_aggregate_node(expr) {
        *done = true;
        return replacement.clone();
    }
    match expr {
        Expr::Binary { left, op, right } => Expr::Binary {
            left: Box::new(walk_substitute_first_agg(left, replacement, done)),
            op: *op,
            right: Box::new(walk_substitute_first_agg(right, replacement, done)),
        },
        Expr::Unary { op, expr: inner } => Expr::Unary {
            op: *op,
            expr: Box::new(walk_substitute_first_agg(inner, replacement, done)),
        },
        Expr::IsNull(inner) => Expr::IsNull(Box::new(walk_substitute_first_agg(
            inner,
            replacement,
            done,
        ))),
        Expr::IsNotNull(inner) => Expr::IsNotNull(Box::new(walk_substitute_first_agg(
            inner,
            replacement,
            done,
        ))),
        Expr::InList {
            expr: inner,
            list,
            negated,
        } => Expr::InList {
            expr: Box::new(walk_substitute_first_agg(inner, replacement, done)),
            list: list
                .iter()
                .map(|e| walk_substitute_first_agg(e, replacement, done))
                .collect(),
            negated: *negated,
        },
        Expr::Between {
            expr: inner,
            low,
            high,
            negated,
        } => Expr::Between {
            expr: Box::new(walk_substitute_first_agg(inner, replacement, done)),
            low: Box::new(walk_substitute_first_agg(low, replacement, done)),
            high: Box::new(walk_substitute_first_agg(high, replacement, done)),
            negated: *negated,
        },
        Expr::Like {
            expr: inner,
            pattern,
            negated,
            escape_char,
        } => Expr::Like {
            expr: Box::new(walk_substitute_first_agg(inner, replacement, done)),
            pattern: Box::new(walk_substitute_first_agg(pattern, replacement, done)),
            negated: *negated,
            escape_char: *escape_char,
        },
        Expr::Function {
            name,
            args,
            distinct,
        } => Expr::Function {
            name: name.clone(),
            args: args
                .iter()
                .map(|a| walk_substitute_first_agg(a, replacement, done))
                .collect(),
            distinct: *distinct,
        },
        Expr::Case {
            operand,
            conditions,
            results,
            else_result,
        } => Expr::Case {
            operand: operand
                .as_ref()
                .map(|o| Box::new(walk_substitute_first_agg(o, replacement, done))),
            conditions: conditions
                .iter()
                .map(|c| walk_substitute_first_agg(c, replacement, done))
                .collect(),
            results: results
                .iter()
                .map(|r| walk_substitute_first_agg(r, replacement, done))
                .collect(),
            else_result: else_result
                .as_ref()
                .map(|e| Box::new(walk_substitute_first_agg(e, replacement, done))),
        },
        other => other.clone(),
    }
}

pub(in crate::engine::executor) fn single_table_result_columns(
    stmt: &Select,
    table: &TableSchema,
    table_alias: Option<&str>,
) -> Result<Vec<Column>, QueryError> {
    let mut columns = Vec::new();
    for item in &stmt.projection {
        match item {
            SelectItem::Wildcard | SelectItem::QualifiedWildcard(_) => {
                // SELECT * must skip Dropped tombstones — those columns
                // are no longer part of the table's logical schema.
                columns.extend(table.iter_select_columns().map(|(idx, column)| Column {
                    // Capture the catalog-derived wire metadata so a
                    // single-table `SELECT * FROM t` exposes the real
                    // NOT_NULL / PRI_KEY / UNSIGNED / ENUM_FLAG / SET_FLAG /
                    // declared length to JDBC's ResultSetMetaData.
                    source: table.wire_column_source_with_alias(idx, table_alias),
                    name: column.name.clone(),
                    column_type: table.result_type(idx),
                }));
            }
            // SELECT col_ref: when the projection is a bare column reference,
            // propagate the catalog source so aliased columns (`id AS pk`)
            // keep org_table/org_name pointing at the real catalog row.
            SelectItem::Expr { expr, alias }
                if catalog_column_name(expr)
                    .and_then(|name| table.column_index(name).ok())
                    .is_some() =>
            {
                let name = catalog_column_name(expr).expect("guarded catalog column");
                let idx = table.column_index(name)?;
                columns.push(Column {
                    source: table.wire_column_source_with_alias(idx, table_alias),
                    name: alias.clone().unwrap_or_else(|| name.to_string()),
                    column_type: table.result_type(idx),
                });
            }
            SelectItem::Expr { expr, alias } => {
                if let Expr::Function { name, args, .. } = expr {
                    if matches!(
                        name.to_ascii_uppercase().as_str(),
                        "MIN" | "MAX" | "__HIDDEN_FIRST_VALUE"
                    ) {
                        if let Some(index) = args
                            .first()
                            .and_then(catalog_column_name)
                            .and_then(|column| table.column_index(column).ok())
                        {
                            columns.push(Column {
                                name: alias.clone().unwrap_or_else(|| expr_name(expr)),
                                column_type: table.result_type(index),
                                source: super::plan::min_max_column_source(
                                    table.wire_column_source_with_alias(index, table_alias),
                                ),
                            });
                            continue;
                        }
                    }
                }
                let descriptor = single_table_bound_expr_type(expr, table);
                let mut source = super::expression_column_source_for_expr(
                    single_table_expr_unsigned(expr, table),
                    expr,
                    descriptor,
                );
                if crate::engine::expr::expr_result_guaranteed_nonnull_with(expr, &|column_expr| {
                    let name = match column_expr {
                        Expr::Identifier(name) => name.as_str(),
                        Expr::CompoundIdentifier(parts) => parts.last()?.as_str(),
                        _ => return None,
                    };
                    table
                        .column_index(name)
                        .ok()
                        .map(|idx| !table.columns[idx].nullable)
                }) {
                    source.flags |= crate::mysql::protocol::NOT_NULL_FLAG;
                }
                if let Some(collation) =
                    crate::engine::expr::expr_type_descriptor(expr, table).collation
                {
                    source.collation_id = Some(collation.collation_id);
                }
                source.length =
                    crate::engine::expr::expression_metadata_length_in_table(expr, table)
                        .or(source.length);
                source.dynamic_parameter = descriptor.dynamic_parameter;
                if !matches!(
                    descriptor.kind,
                    crate::engine::expr_type::ExprTypeKind::Unknown
                ) {
                    descriptor.apply_to_source(&mut source);
                }
                if let Expr::Function { name, args, .. } = expr {
                    if matches!(name.to_ascii_uppercase().as_str(), "SUM" | "AVG") {
                        if let Some(scale) = args
                            .first()
                            .and_then(catalog_column_name)
                            .and_then(|name| table.column_index(name).ok())
                            .and_then(|index| table.wire_column_source(index).decimals)
                            .filter(|scale| *scale != 0x1f)
                        {
                            if matches!(
                                args.first()
                                    .and_then(catalog_column_name)
                                    .and_then(|name| table.column_index(name).ok())
                                    .map(|index| table.result_type(index)),
                                Some(ColumnType::Float | ColumnType::Double)
                            ) {
                                source.decimals = Some(if name.eq_ignore_ascii_case("AVG") {
                                    scale.saturating_add(
                                        crate::engine::expr::session_div_precision_increment(),
                                    )
                                } else {
                                    scale
                                });
                            }
                        }
                    }
                }
                columns.push(Column {
                    source,
                    name: alias.clone().unwrap_or_else(|| expr_name(expr)),
                    column_type: if matches!(
                        descriptor.kind,
                        crate::engine::expr_type::ExprTypeKind::Unknown
                    ) {
                        expr_result_type(expr)
                    } else {
                        descriptor.column_type()
                    },
                });
            }
        }
    }
    Ok(columns)
}

fn single_table_bound_expr_type(
    expr: &Expr,
    table: &TableSchema,
) -> crate::engine::expr_type::ExprType {
    let resolve_column = |column_expr: &Expr| {
        let name = match column_expr {
            Expr::Identifier(name) => name.as_str(),
            Expr::CompoundIdentifier(parts) => parts.last()?.as_str(),
            _ => return None,
        };
        let index = table.column_index(name).ok()?;
        let column = &table.columns[index];
        let source = table.wire_column_source(index);
        Some(crate::engine::expr_type::ExprType::from_data_type(
            &column.data_type,
            column.unsigned,
            column.nullable,
            source.length,
            source.collation_id,
        ))
    };
    let resolve_placeholder = |_: usize| None;
    let context =
        crate::engine::expr_type::TypeContext::context_free(&resolve_column, &resolve_placeholder);
    if let Expr::Function { name, args, .. } = expr {
        if name.eq_ignore_ascii_case("GROUP_CONCAT") {
            return crate::engine::expr_type::infer_expr_type(expr, &context);
        }
        if crate::planner::is_aggregate_function(name) {
            let resolved = args.first().map(|argument| {
                crate::engine::expr_type::infer_expr_type(argument, &context).column_type()
            });
            let semantic = super::eval::aggregate_argument_semantic_type(args.first(), resolved);
            if matches!(
                name.to_ascii_uppercase().as_str(),
                "BIT_AND" | "BIT_OR" | "BIT_XOR"
            ) {
                return crate::engine::expr_type::infer_expr_type(expr, &context);
            }
            let result = super::eval::aggregate_function_result_type(name, semantic);
            return crate::engine::expr_type::ExprType::from_column_type(
                result,
                &crate::types::ColumnSource::default(),
            );
        }
    }
    crate::engine::expr_type::infer_expr_type(expr, &context)
}

fn single_table_expr_unsigned(expr: &Expr, table: &TableSchema) -> bool {
    crate::engine::expr::expr_result_unsigned(expr, &|column_expr| {
        let name = match column_expr {
            Expr::Identifier(name) => name.as_str(),
            Expr::CompoundIdentifier(parts) => parts.last()?.as_str(),
            _ => return None,
        };
        table
            .column_index(name)
            .ok()
            .map(|idx| table.columns[idx].unsigned)
    })
}

pub(in crate::engine::executor) fn select_item_has_subquery(item: &SelectItem) -> bool {
    matches!(item, SelectItem::Expr { expr, .. } if expr_has_subquery(expr))
}

pub(in crate::engine::executor) fn expr_has_subquery(expr: &Expr) -> bool {
    match expr {
        Expr::Subquery(_)
        | Expr::InSubquery { .. }
        | Expr::QuantifiedComparison {
            right: QuantifiedComparisonRight::Subquery(_),
            ..
        }
        | Expr::Exists { .. } => true,
        Expr::Unary { expr, .. } | Expr::IsNull(expr) | Expr::IsNotNull(expr) => {
            expr_has_subquery(expr)
        }
        Expr::Binary { left, right, .. } => expr_has_subquery(left) || expr_has_subquery(right),
        Expr::InList { expr, list, .. } => {
            expr_has_subquery(expr) || list.iter().any(expr_has_subquery)
        }
        Expr::QuantifiedComparison { left, right, .. } => {
            expr_has_subquery(left)
                || match right {
                    QuantifiedComparisonRight::Subquery(_) => true,
                    QuantifiedComparisonRight::List(list) => list.iter().any(expr_has_subquery),
                }
        }
        Expr::Between {
            expr, low, high, ..
        } => expr_has_subquery(expr) || expr_has_subquery(low) || expr_has_subquery(high),
        Expr::Function { args, .. } => args.iter().any(expr_has_subquery),
        Expr::Like { expr, pattern, .. } => expr_has_subquery(expr) || expr_has_subquery(pattern),
        Expr::Case {
            operand,
            conditions,
            results,
            else_result,
        } => {
            operand.as_ref().is_some_and(|e| expr_has_subquery(e))
                || conditions.iter().any(expr_has_subquery)
                || results.iter().any(expr_has_subquery)
                || else_result.as_ref().is_some_and(|e| expr_has_subquery(e))
        }
        _ => false,
    }
}

pub(super) fn simple_scan_source(
    db: &Arc<Database>,
    txn: TxnView,
    plan: &PhysicalPlan,
) -> Result<Option<(PlanTable, Option<Vec<RowId>>, Option<Expr>)>, QueryError> {
    match plan {
        PhysicalPlan::Limit { input, .. }
        | PhysicalPlan::Projection { input, .. }
        | PhysicalPlan::Sort { input, .. }
        | PhysicalPlan::StreamingDistinct { input, .. }
        | PhysicalPlan::HashAggregate { input, .. } => simple_scan_source(db, txn, input),
        PhysicalPlan::ParallelHashAggregate {
            input,
            group_by,
            having: None,
            ..
        } if group_by.is_empty()
            && runtime_unique_range_fits_batch(
                input,
                crate::engine::executor::vec::DEFAULT_BATCH_ROWS,
            )? =>
        {
            simple_scan_source(db, txn, input)
        }
        PhysicalPlan::SeqScan { table, filter, .. } => {
            Ok(Some((table.clone(), None, filter.clone())))
        }
        // Zone Map scans need page-range candidates rather than an optional
        // RowId list. The simple path's current source contract cannot carry
        // those ranges, so route through the planner executor where all
        // projected, limited and aggregate scan optimisations are range-aware.
        PhysicalPlan::ZoneMapScan { .. } | PhysicalPlan::ParallelZoneMapScan { .. } => Ok(None),
        PhysicalPlan::IndexLookup {
            table,
            index,
            key,
            exact,
            filter,
            covering,
            ..
        } => {
            // The simple fast path always reads rows back from the heap,
            // so covering / INCLUDE-bearing indexes must fall through to
            // the full plan executor — that's the only path that can
            // decode column values from the index entry instead.
            if *covering || !index.included_columns.is_empty() {
                return Ok(None);
            }
            let ids = if *exact {
                db.lookup_index_visible(&table.schema, index, key.clone(), txn)?
            } else {
                db.prefix_index_visible(&table.schema, index, key.clone(), txn)?
            };
            let residual_filter = filter.as_ref().and_then(|expr| {
                if db.has_active_write_txns() {
                    return Some(expr.clone());
                }
                let access = EngineIndexAccess::Lookup {
                    key: key.clone(),
                    exact: *exact,
                };
                index_access_residual_filter(&table.schema, index, &access, expr)
            });
            Ok(Some((table.clone(), Some(ids), residual_filter)))
        }
        PhysicalPlan::IndexMultiLookup {
            table,
            index,
            keys,
            exact,
            filter,
            covering,
            ..
        } => {
            if *covering || !index.included_columns.is_empty() {
                return Ok(None);
            }
            let ids = super::plan::execute_index_multi_lookup_ids(
                db,
                txn,
                &table.schema,
                index,
                keys,
                *exact,
            )?;
            Ok(Some((table.clone(), Some(ids), filter.clone())))
        }
        PhysicalPlan::IndexRangeScan {
            table,
            index,
            low,
            high,
            filter,
            covering,
            ..
        } => {
            if *covering || !index.included_columns.is_empty() {
                return Ok(None);
            }
            let ids = db.range_index_bounds_visible(
                &table.schema,
                index,
                low.clone(),
                high.clone(),
                txn,
            )?;
            if super::analyze_stats::is_recording() && !ids.is_empty() {
                let mut pages: Vec<u32> = ids.iter().map(|row_id| row_id.page_id).collect();
                pages.sort_unstable();
                pages.dedup();
                let rids = ids.len() as u64;
                let unique_pages = pages.len() as u64;
                super::analyze_stats::add_range_scan_work(rids, rids, unique_pages);
                super::analyze_stats::add_mrr_work(
                    1,
                    rids,
                    unique_pages,
                    0,
                    rids.saturating_mul(std::mem::size_of::<crate::storage::RowId>() as u64),
                );
            }
            let residual_filter = filter.as_ref().and_then(|expr| {
                if db.has_active_write_txns() {
                    return Some(expr.clone());
                }
                let access = EngineIndexAccess::Range {
                    low: low.clone(),
                    high: high.clone(),
                };
                index_access_residual_filter(&table.schema, index, &access, expr)
            });
            Ok(Some((table.clone(), Some(ids), residual_filter)))
        }
        PhysicalPlan::IndexInLookup { .. } => Ok(None),
        _ => Ok(None),
    }
}

pub(super) fn simple_result_columns(
    stmt: &Select,
    table: &TableSchema,
    table_alias: Option<&str>,
) -> Result<Vec<Column>, QueryError> {
    let mut columns = Vec::new();
    for item in &stmt.projection {
        match item {
            SelectItem::Wildcard => {
                columns.extend(table.iter_select_columns().map(|(idx, column)| Column {
                    source: table.wire_column_source_with_alias(idx, table_alias),
                    name: column.name.clone(),
                    column_type: table.result_type(idx),
                }));
            }
            SelectItem::QualifiedWildcard(_) => {
                columns.extend(table.iter_select_columns().map(|(idx, column)| Column {
                    source: table.wire_column_source_with_alias(idx, table_alias),
                    name: column.name.clone(),
                    column_type: table.result_type(idx),
                }));
            }
            // A bare column reference (with or without table qualifier)
            // resolves to a catalog row; carry its wire source so the
            // alias is observable through `org_name` even when the
            // select-list label is renamed.
            SelectItem::Expr {
                expr: Expr::Identifier(ident),
                alias,
            } if table.column_index(ident).is_ok() => {
                let idx = table.column_index(ident)?;
                columns.push(Column {
                    source: table.wire_column_source_with_alias(idx, table_alias),
                    name: alias.clone().unwrap_or_else(|| ident.clone()),
                    column_type: table.result_type(idx),
                });
            }
            SelectItem::Expr {
                expr: Expr::CompoundIdentifier(parts),
                alias,
            } if parts
                .last()
                .and_then(|n| table.column_index(n).ok())
                .is_some() =>
            {
                let name = parts.last().cloned().unwrap_or_default();
                let idx = table.column_index(&name)?;
                columns.push(Column {
                    source: table.wire_column_source_with_alias(idx, table_alias),
                    name: alias.clone().unwrap_or(name),
                    column_type: table.result_type(idx),
                });
            }
            SelectItem::Expr { expr, alias } => {
                let descriptor = single_table_bound_expr_type(expr, table);
                let mut source = super::expression_column_source_for_expr(
                    single_table_expr_unsigned(expr, table),
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
                        expr_result_type(expr)
                    } else {
                        descriptor.column_type()
                    },
                });
            }
        }
    }
    Ok(columns)
}

pub(in crate::engine::executor) fn simple_projection_indexes(
    stmt: &Select,
    table: &TableSchema,
) -> Option<Vec<usize>> {
    stmt.projection
        .iter()
        .map(|item| match item {
            SelectItem::Expr { expr, .. } => expr_column_index(expr, table),
            _ => None,
        })
        .collect()
}

#[derive(Debug, Clone)]
enum TableProjectionItem {
    Wildcard,
    QualifiedWildcard,
    Expr(CompiledTableExpr),
}

#[derive(Debug, Clone)]
pub(in crate::engine::executor) struct TableProjectionProgram {
    items: Vec<TableProjectionItem>,
    /// When every projection item resolves to a plain column reference
    /// (wildcards expand to the visible columns) and no column is read
    /// twice, this holds the flat output→input index list. Rows the
    /// caller owns can then be projected by *moving* each value out of
    /// the source row — a `Value::String` clone is a full heap copy,
    /// so for a 100-row range scan of a CHAR(120) column the cloning
    /// path costs 100 extra allocations per query.
    move_plan: Option<Vec<usize>>,
}

impl TableProjectionProgram {
    pub(in crate::engine::executor) fn compile_once(
        projection: &[SelectItem],
        table: &TableSchema,
    ) -> Self {
        let items: Vec<TableProjectionItem> = projection
            .iter()
            .map(|item| match item {
                SelectItem::Wildcard => TableProjectionItem::Wildcard,
                SelectItem::QualifiedWildcard(_) => TableProjectionItem::QualifiedWildcard,
                SelectItem::Expr { expr, .. } => {
                    TableProjectionItem::Expr(CompiledTableExpr::compile_once(expr, table))
                }
            })
            .collect();
        let mut indices = Vec::new();
        let mut movable = true;
        for item in &items {
            match item {
                TableProjectionItem::Wildcard | TableProjectionItem::QualifiedWildcard => {
                    for (idx, _) in table.iter_select_columns() {
                        indices.push(idx);
                    }
                }
                TableProjectionItem::Expr(CompiledTableExpr::SimpleColumn(idx)) => {
                    indices.push(*idx);
                }
                TableProjectionItem::Expr(_) => {
                    movable = false;
                    break;
                }
            }
        }
        if movable {
            let mut seen = indices.clone();
            seen.sort_unstable();
            movable = seen.windows(2).all(|w| w[0] != w[1]);
        }
        let move_plan = movable.then_some(indices);
        Self { items, move_plan }
    }

    /// Move-based counterpart of [`Self::project_row`] for callers that
    /// own the source row and drop it right after projecting. Falls
    /// back to the cloning path when the projection contains computed
    /// expressions or duplicate column reads.
    pub(super) fn project_row_owned(
        &self,
        table: &TableSchema,
        mut values: Vec<Value>,
    ) -> Result<Vec<Value>, QueryError> {
        let Some(plan) = &self.move_plan else {
            return self.project_row(table, &values);
        };
        // Identity projection: the source row already is the result.
        if plan.len() == values.len() && plan.iter().enumerate().all(|(out, idx)| out == *idx) {
            return Ok(values);
        }
        let mut out = Vec::with_capacity(plan.len());
        for &idx in plan {
            out.push(match values.get_mut(idx) {
                Some(slot) => std::mem::replace(slot, Value::Null),
                None => Value::Null,
            });
        }
        Ok(out)
    }

    pub(in crate::engine::executor) fn project_row(
        &self,
        table: &TableSchema,
        values: &[Value],
    ) -> Result<Vec<Value>, QueryError> {
        let mut out = Vec::new();
        for item in &self.items {
            match item {
                TableProjectionItem::Wildcard | TableProjectionItem::QualifiedWildcard => {
                    for (idx, _) in table.iter_select_columns() {
                        out.push(values.get(idx).cloned().unwrap_or(Value::Null));
                    }
                }
                TableProjectionItem::Expr(expr) => out.push(expr.eval(table, values)?),
            }
        }
        Ok(out)
    }

    /// Accumulate every input column this projection reads into `out`.
    /// Wildcards mark every visible column; legacy (uncompiled) item
    /// exprs return `false` so the caller falls back to a full-row
    /// read.
    pub(super) fn try_collect_referenced_columns(
        &self,
        table: &TableSchema,
        out: &mut std::collections::HashSet<usize>,
    ) -> bool {
        for item in &self.items {
            match item {
                TableProjectionItem::Wildcard | TableProjectionItem::QualifiedWildcard => {
                    for (idx, _) in table.iter_select_columns() {
                        out.insert(idx);
                    }
                }
                TableProjectionItem::Expr(expr) => {
                    if !expr.try_collect_referenced_columns(out) {
                        return false;
                    }
                }
            }
        }
        true
    }
}

#[derive(Debug, Clone)]
struct TableOrderProgram {
    keys: Vec<CompiledTableExpr>,
}

impl TableOrderProgram {
    fn compile_once(order_by: &[OrderBy], table: &TableSchema) -> Self {
        Self {
            keys: order_by
                .iter()
                .map(|item| CompiledTableExpr::compile_once(&item.expr, table))
                .collect(),
        }
    }

    fn eval_keys(&self, table: &TableSchema, values: &[Value]) -> Result<Vec<Value>, QueryError> {
        self.keys
            .iter()
            .map(|expr| expr.eval(table, values))
            .collect()
    }

    fn try_collect_referenced_columns(&self, out: &mut std::collections::HashSet<usize>) -> bool {
        for key in &self.keys {
            if !key.try_collect_referenced_columns(out) {
                return false;
            }
        }
        true
    }
}

pub(in crate::engine::executor) fn project_table_row(
    stmt: &Select,
    table: &TableSchema,
    values: &[Value],
) -> Result<Vec<Value>, QueryError> {
    let mut out = Vec::new();
    for item in &stmt.projection {
        match item {
            SelectItem::Wildcard | SelectItem::QualifiedWildcard(_) => {
                // SELECT * pulls out only logically-visible columns —
                // tombstones from INSTANT DROP COLUMN are skipped.
                // The full row carries a placeholder slot at the
                // dropped physical_pos which we deliberately ignore.
                for (idx, _) in table.iter_select_columns() {
                    out.push(values.get(idx).cloned().unwrap_or(Value::Null));
                }
            }
            SelectItem::Expr { expr, .. } => out.push(eval_expr(expr, table, values)?),
        }
    }
    Ok(out)
}

/// Drill through pipeline wrappers (Sort / Limit / Projection / Aggregate)
/// and return the `access_rows` recorded on the underlying scan node.
/// Returns `0.0` when the plan doesn't reach a scan we can read (e.g. a
/// join). Used both by the aggregate fast path's large-scan bail-out and by
/// the compiled dispatcher when deciding whether a bounded single-table
/// access is smaller than one vector batch.
fn estimated_access_rows_for_scan(plan: &PhysicalPlan) -> f64 {
    let mut cur = plan;
    loop {
        match cur {
            PhysicalPlan::Sort { input, .. }
            | PhysicalPlan::IncrementalSort { input, .. }
            | PhysicalPlan::ParallelSort { input, .. }
            | PhysicalPlan::Limit { input, .. }
            | PhysicalPlan::Projection { input, .. }
            | PhysicalPlan::HashAggregate { input, .. }
            | PhysicalPlan::StreamingDistinct { input, .. }
            | PhysicalPlan::TopK { input, .. }
            | PhysicalPlan::ParallelTopK { input, .. }
            | PhysicalPlan::Exchange { input, .. } => cur = input.as_ref(),
            PhysicalPlan::SeqScan { access_rows, .. }
            | PhysicalPlan::ZoneMapScan { access_rows, .. }
            | PhysicalPlan::ParallelSeqScan { access_rows, .. }
            | PhysicalPlan::IndexLookup { access_rows, .. }
            | PhysicalPlan::IndexMultiLookup { access_rows, .. }
            | PhysicalPlan::IndexInLookup { access_rows, .. }
            | PhysicalPlan::IndexRangeScan { access_rows, .. }
            | PhysicalPlan::IndexFastScan { access_rows, .. }
            | PhysicalPlan::ParallelIndexRangeScan { access_rows, .. }
            | PhysicalPlan::ParallelIndexFastScan { access_rows, .. } => return *access_rows,
            _ => return 0.0,
        }
    }
}

/// The fused row kernel has less fixed cost than constructing a complete
/// streaming/vector graph when the access fits in a single batch.  This is an
/// operator-internal choice: the CBO's scan, index and ordering decisions stay
/// unchanged, and larger inputs continue to use the spillable batch path.
pub(crate) fn prefer_fused_single_table_operator(plan: &PhysicalPlan) -> bool {
    fn is_bounded_range_spine(plan: &PhysicalPlan) -> bool {
        match plan {
            PhysicalPlan::Sort { input, .. }
            | PhysicalPlan::IncrementalSort { input, .. }
            | PhysicalPlan::ParallelSort { input, .. }
            | PhysicalPlan::Limit { input, .. }
            | PhysicalPlan::Projection { input, .. }
            | PhysicalPlan::HashAggregate { input, .. }
            | PhysicalPlan::StreamingDistinct { input, .. }
            | PhysicalPlan::TopK { input, .. }
            | PhysicalPlan::ParallelTopK { input, .. } => is_bounded_range_spine(input),
            PhysicalPlan::IndexRangeScan { low, high, .. } => low.is_some() || high.is_some(),
            _ => false,
        }
    }

    if !is_bounded_range_spine(plan) {
        return false;
    }
    let access_rows = estimated_access_rows_for_scan(plan);
    access_rows.is_finite()
        // `0.0` is also the sentinel for a shape that did not reach a
        // recognized single-table scan (joins, custom aggregate inputs).
        && access_rows > 0.0
        && access_rows <= crate::engine::executor::vec::DEFAULT_BATCH_ROWS as f64
}

/// Prove from the bound values that a prepared unique-index range can emit at
/// most `cap` rows even when its value-independent CBO estimate is larger.
///
/// Prepared plans deliberately do not sniff parameter values, so a stable
/// estimate for `unique_int_col BETWEEN ? AND ?` must cover wide executions.
/// At execution time the operator may still choose its bounded one-batch
/// implementation when the current integer interval itself is a semantic
/// upper bound: a UNIQUE index contributes at most one row per integer value.
/// This changes neither the access path nor its order and cannot depend on
/// memory, cache residency, RSS, concurrency, or observed row counts.
pub(crate) fn runtime_unique_range_fits_batch(
    plan: &PhysicalPlan,
    cap: usize,
) -> Result<bool, QueryError> {
    fn bound_count_at_most(
        low: &crate::storage::IndexBound,
        high: &crate::storage::IndexBound,
        key_arity: usize,
        cap: usize,
    ) -> Result<bool, QueryError> {
        // A unique prefix can match many suffixes, and UNIQUE permits
        // duplicate keys containing NULL. Only a complete, non-NULL key
        // contributes at most one row per value of its final integer part.
        if key_arity == 0 || low.key.0.len() != key_arity || high.key.0.len() != key_arity {
            return Ok(false);
        }
        let low = low.resolve_params()?;
        let high = high.resolve_params()?;
        let split = low.key.0.len() - 1;
        if low.key.0[..split] != high.key.0[..split]
            || low.key.0[..split]
                .iter()
                .any(|part| matches!(part, KeyPart::Null))
        {
            return Ok(false);
        }
        let cap = cap as u128;
        let count = match (&low.key.0[split], &high.key.0[split]) {
            (KeyPart::Int(low_value), KeyPart::Int(high_value)) => {
                let start = i128::from(*low_value) + i128::from(!low.inclusive);
                let end = i128::from(*high_value) - i128::from(!high.inclusive);
                if end < start {
                    0
                } else {
                    (end - start + 1) as u128
                }
            }
            (KeyPart::UInt(low_value), KeyPart::UInt(high_value)) => {
                let start = u128::from(*low_value) + u128::from(!low.inclusive);
                let end = u128::from(*high_value).checked_sub(u128::from(!high.inclusive));
                match end {
                    Some(end) if end >= start => end - start + 1,
                    _ => 0,
                }
            }
            _ => return Ok(false),
        };
        Ok(count <= cap)
    }

    match plan {
        PhysicalPlan::Limit { input, .. }
        | PhysicalPlan::Projection { input, .. }
        | PhysicalPlan::Sort { input, .. }
        | PhysicalPlan::StreamingDistinct { input, .. }
        | PhysicalPlan::HashAggregate { input, .. } => runtime_unique_range_fits_batch(input, cap),
        PhysicalPlan::ParallelHashAggregate {
            input,
            group_by,
            having: None,
            ..
        } if group_by.is_empty() => runtime_unique_range_fits_batch(input, cap),
        PhysicalPlan::IndexRangeScan {
            index,
            low: Some(low),
            high: Some(high),
            covering,
            ..
        } if (index.primary || index.unique) && !*covering && index.included_columns.is_empty() => {
            bound_count_at_most(low, high, index.columns.len(), cap)
        }
        _ => Ok(false),
    }
}

/// Resolve the catalog column behind a direct or grouped-key projection.
pub(super) fn catalog_column_name(expr: &Expr) -> Option<&str> {
    match expr {
        Expr::Identifier(name) => Some(name),
        Expr::CompoundIdentifier(parts) => parts.last().map(String::as_str),
        Expr::GroupKeyRef { expr, .. } => catalog_column_name(expr),
        _ => None,
    }
}

#[cfg(test)]
mod aggregate_walk_tests {
    use super::*;

    #[test]
    fn lone_aggregate_respects_scalar_edges_and_query_boundaries() {
        for sql in [
            "SUM(x)",
            "-SUM(x)",
            "SUM(x) IS NULL",
            "SUM(x) IS NOT NULL",
            "1 + SUM(x)",
            "SUM(x) IN (1, 2)",
            "1 IN (SUM(x), 2)",
            "SUM(x) BETWEEN 1 AND 2",
            "1 BETWEEN SUM(x) AND 2",
            "1 BETWEEN 2 AND SUM(x)",
            "SUM(x) LIKE 'a'",
            "'a' LIKE SUM(x)",
            "ABS(SUM(x))",
            "(1, SUM(x))",
            "CASE SUM(x) WHEN 1 THEN 2 END",
            "CASE WHEN SUM(x) > 0 THEN 1 ELSE 0 END",
            "CASE WHEN TRUE THEN SUM(x) ELSE 0 END",
            "CASE WHEN FALSE THEN 0 ELSE SUM(x) END",
        ] {
            let expr = crate::parser::parse_catalog_expr(sql).unwrap();
            let found = find_lone_aggregate_call(&expr).unwrap_or_else(|| panic!("{sql}"));
            assert!(
                matches!(found, Expr::Function { ref name, .. } if name.eq_ignore_ascii_case("SUM")),
                "{sql}"
            );
        }
        for sql in [
            "x + 1",
            "SUM(x) + MAX(x)",
            "SUM(MAX(x))",
            "(SUM(x), MAX(x))",
            "SUM(x) + (SELECT 1)",
            "SUM(x) + ROW_NUMBER() OVER ()",
            "SUM(x) IN (SELECT 1)",
            "SUM(x) + EXISTS (SELECT 1)",
        ] {
            let expr = crate::parser::parse_catalog_expr(sql).unwrap();
            assert!(find_lone_aggregate_call(&expr).is_none(), "{sql}");
        }
    }
}

#[derive(Debug)]
pub(crate) struct CompactRangeRecipe {
    projection: Vec<SelectItem>,
    order_by: Vec<OrderBy>,
    distinct: bool,
    filter: Option<Expr>,
    columns: Arc<[Column]>,
    without_filter: CompactRangeInput,
    with_filter: Option<CompactRangeInput>,
    _charge: super::super::mem_budget::ControlledMemoryCharge,
}

#[derive(Debug)]
struct CompactRangeInput {
    columns: Vec<usize>,
    schema: Option<TableSchema>,
    decode: Arc<(
        RowDecodePlan,
        super::super::mem_budget::ControlledMemoryCharge,
    )>,
    projected: ProjectedReadPlan,
    order_slots: Vec<usize>,
    sort_fields: smallvec::SmallVec<[crate::engine::executor::sort::SortField; 8]>,
    swaps: Vec<(usize, usize)>,
    output_len: usize,
    dedup: bool,
}

pub(crate) fn compile_compact_range_recipe(
    stmt: &Select,
    plan: &PhysicalPlan,
) -> Option<Arc<CompactRangeRecipe>> {
    if !fused_single_table_shape_supported(stmt)
        || select_projection_has_aggregate(&stmt.projection)
    {
        return None;
    }
    fn source(plan: &PhysicalPlan) -> Option<(&PlanTable, Option<&Expr>)> {
        match plan {
            PhysicalPlan::Limit { input, .. }
            | PhysicalPlan::Projection { input, .. }
            | PhysicalPlan::Sort { input, .. }
            | PhysicalPlan::StreamingDistinct { input, .. } => source(input),
            PhysicalPlan::IndexRangeScan {
                table,
                index,
                filter,
                covering: false,
                ..
            } if !table.schema.is_partitioned() && index.included_columns.is_empty() => {
                Some((table, filter.as_ref()))
            }
            _ => None,
        }
    }
    let (table, filter) = source(plan)?;
    if stmt.distinct && !can_dedup_distinct_after_sort(stmt) {
        return None;
    }
    fn input(
        stmt: &Select,
        table: &TableSchema,
        filter: Option<&Expr>,
    ) -> Option<CompactRangeInput> {
        let projection = stmt
            .projection
            .iter()
            .map(|item| match item {
                SelectItem::Expr { expr, .. } => expr_column_index(expr, table),
                _ => None,
            })
            .collect::<Option<Vec<_>>>()?;
        let order = stmt
            .order_by
            .iter()
            .map(|item| expr_column_index(&item.expr, table))
            .collect::<Option<Vec<_>>>()?;
        let mut columns = projection.clone();
        columns.extend(&order);
        if let Some(filter) = filter {
            let unresolved = crate::parser::visit::any_expr(filter, |expr| {
                if matches!(expr, Expr::Identifier(_) | Expr::CompoundIdentifier(_)) {
                    match expr_column_index(expr, table) {
                        Some(column) => columns.push(column),
                        None => return true,
                    }
                }
                false
            });
            if unresolved {
                return None;
            }
        }
        columns.sort_unstable();
        columns.dedup();
        // Generated ASTs own nested parser storage which this compact
        // optional metadata reservation deliberately does not duplicate.
        if columns
            .iter()
            .any(|&i| table.columns[i].generated_expr_ast.is_some())
        {
            return None;
        }
        let mut positions = columns.clone();
        let mut swaps = Vec::new();
        for (out, source) in projection.iter().enumerate() {
            if projection[..out].contains(source) {
                return None;
            }
            let at = positions.iter().position(|candidate| candidate == source)?;
            if at != out {
                positions.swap(at, out);
                swaps.push((out, at));
            }
        }
        Some(CompactRangeInput {
            schema: filter.map(|_| super::plan::projected_schema_view(table, &columns)),
            decode: RowDecodePlan::try_compile_cached(&columns, &table.row_codec)?,
            projected: ProjectedReadPlan::new(&columns, table),
            order_slots: order
                .iter()
                .map(|column| columns.iter().position(|c| c == column).unwrap())
                .collect(),
            sort_fields: sort_fields_with_schema(&stmt.order_by, table),
            columns,
            swaps,
            output_len: projection.len(),
            dedup: stmt.distinct,
        })
    }
    let without_filter = input(stmt, &table.schema, None)?;
    let with_filter = match filter {
        Some(filter) => Some(input(stmt, &table.schema, Some(filter))?),
        None => None,
    };
    let columns: Arc<[Column]> =
        single_table_result_columns(stmt, &table.schema, table.alias.as_deref())
            .ok()?
            .into();
    struct Count(usize);
    impl std::io::Write for Count {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            self.0 = self.0.saturating_add(bytes.len());
            Ok(bytes.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    let mut count = Count(0);
    serde_json::to_writer(
        &mut count,
        &(
            stmt.projection.as_slice(),
            stmt.order_by.as_slice(),
            filter,
            with_filter.as_ref().and_then(|input| input.schema.as_ref()),
        ),
    )
    .ok()?;
    let mut bytes =
        std::mem::size_of::<CompactRangeRecipe>().saturating_add(count.0.saturating_mul(8));
    for input in std::iter::once(&without_filter).chain(with_filter.iter()) {
        bytes = bytes
            .saturating_add(std::mem::size_of::<CompactRangeInput>())
            .saturating_add(input.columns.capacity() * std::mem::size_of::<usize>())
            .saturating_add(input.order_slots.capacity() * std::mem::size_of::<usize>())
            .saturating_add(input.swaps.capacity() * std::mem::size_of::<(usize, usize)>())
            .saturating_add(
                input.sort_fields.len()
                    * std::mem::size_of::<crate::engine::executor::sort::SortField>(),
            );
    }
    bytes = bytes.saturating_add(columns.len() * std::mem::size_of::<Column>());
    for column in columns.iter() {
        bytes = bytes
            .saturating_add(column.name.capacity())
            .saturating_add(column.source.schema.capacity())
            .saturating_add(column.source.table.capacity())
            .saturating_add(column.source.org_table.capacity())
            .saturating_add(column.source.org_name.capacity());
    }
    let charge = super::super::mem_budget::ControlledMemoryCharge::try_new_optional(
        super::super::mem_budget::ControlledMemoryCategory::DerivedCache,
        bytes,
    )?;
    Some(Arc::new(CompactRangeRecipe {
        projection: stmt.projection.clone(),
        order_by: stmt.order_by.clone(),
        distinct: stmt.distinct,
        filter: filter.cloned(),
        columns,
        without_filter,
        with_filter,
        _charge: charge,
    }))
}

pub(crate) fn execute_compact_range_recipe(
    db: &Arc<Database>,
    txn: TxnView,
    stmt: &Select,
    plan: &PhysicalPlan,
    recipe: &CompactRangeRecipe,
) -> Result<Option<QueryResponse>, QueryError> {
    // Presentation-bearing ASTs can differ while sharing a physical artifact.
    // Decline before enumerating candidates when their compact layouts differ.
    if stmt.projection != recipe.projection
        || stmt.order_by != recipe.order_by
        || stmt.distinct != recipe.distinct
    {
        return Ok(None);
    }
    let Some(source) = simple_scan_source(db, txn, plan)? else {
        return Ok(None);
    };
    let input = match &source.2 {
        None => Some(&recipe.without_filter),
        Some(filter) if Some(filter) == recipe.filter.as_ref() => recipe.with_filter.as_ref(),
        _ => None,
    };
    let Some(input) = input else {
        return execute_fused_single_table_from_source(db, txn, stmt, plan, source, None)
            .map(|result| result.map(QueryResponse::ResultSet));
    };
    let filter = match (&source.2, &input.schema) {
        (Some(expr), Some(schema)) => match super::plan::CompiledPredicate::compile(expr, schema) {
            Some(program) => Some(program),
            None => {
                return execute_fused_single_table_from_source(db, txn, stmt, plan, source, None)
                    .map(|result| result.map(QueryResponse::ResultSet))
            }
        },
        _ => None,
    };
    let (table, ids, _) = source;
    let ids = ids.expect("compiled non-covering range has row ids");
    let heap = db.cached_heap_handle(table.schema.id)?;
    let mut pairs = db.read_row_columns_many_with_handle_and_plan(
        &heap,
        &table.schema,
        &ids,
        txn,
        &input.columns,
        Some(&input.decode.0),
        &input.projected,
    )?;
    restore_row_id_order(&mut pairs, &ids, |(id, _)| *id);
    let mut rows = Vec::with_capacity(pairs.len());
    for (_, values) in pairs {
        if filter
            .as_ref()
            .map(|filter| filter.eval(&values))
            .transpose()?
            .unwrap_or(true)
        {
            rows.push(values);
        }
    }
    if !input.order_slots.is_empty() {
        let compare = |left: &[Value], right: &[Value]| {
            input
                .sort_fields
                .iter()
                .zip(&input.order_slots)
                .map(|(field, &slot)| {
                    crate::engine::executor::sort::compare_value_for_sort(
                        &left[slot],
                        &right[slot],
                        *field,
                    )
                })
                .find(|order| *order != std::cmp::Ordering::Equal)
                .unwrap_or(std::cmp::Ordering::Equal)
        };
        rows.sort_unstable_by(|a, b| compare(a, b));
        if input.dedup {
            rows.dedup_by(|a, b| compare(a, b) == std::cmp::Ordering::Equal);
        }
    }
    for row in &mut rows {
        for &(a, b) in &input.swaps {
            row.swap(a, b);
        }
        row.truncate(input.output_len);
    }
    let offset = select_offset(stmt)?.min(rows.len());
    let end = select_limit(stmt)?.map_or(rows.len(), |limit| {
        offset.saturating_add(limit).min(rows.len())
    });
    rows.truncate(end);
    if offset != 0 {
        rows.drain(..offset);
    }
    // A shared physical artifact can retain a differently-cased table alias.
    // Column labels were checked above; refresh only source presentation when
    // the current statement's alias differs from the retained wire metadata.
    let table_alias = stmt.from.first().and_then(|from| from.alias.as_deref());
    let wire_table = table_alias.unwrap_or(&table.schema.name);
    let columns = if recipe
        .columns
        .iter()
        .all(|column| column.source.table == wire_table)
    {
        Arc::clone(&recipe.columns)
    } else {
        simple_result_columns(stmt, &table.schema, table_alias)?.into()
    };
    Ok(Some(QueryResponse::SharedResultSet(
        crate::engine::api::SharedResultSet { columns, rows },
    )))
}

pub(crate) fn projected_point_result_columns(
    stmt: &Select,
    operator: &crate::engine::cache::ProjectedPointLookupOperator,
) -> Result<Arc<[Column]>, QueryError> {
    let table_alias = stmt.from.first().and_then(|from| from.alias.as_deref());
    let wire_table = table_alias.unwrap_or(&operator.table.name);
    let mut output = 0usize;
    let mut matches = true;
    let mut check = |name: &str| {
        matches &= operator
            .result_columns
            .get(output)
            .is_some_and(|column| column.name == name && column.source.table == wire_table);
        output += 1;
    };
    for item in &stmt.projection {
        match item {
            SelectItem::Wildcard | SelectItem::QualifiedWildcard(_) => {
                for (_, column) in operator.table.iter_select_columns() {
                    check(&column.name);
                }
            }
            SelectItem::Expr { expr, alias } => {
                let name = alias.as_deref().or_else(|| match expr {
                    Expr::Identifier(name) => Some(name.as_str()),
                    Expr::CompoundIdentifier(parts) => parts.last().map(String::as_str),
                    _ => None,
                });
                let Some(name) = name else {
                    return simple_result_columns(stmt, &operator.table, table_alias)
                        .map(Arc::from);
                };
                check(name);
            }
        }
    }
    if matches && output == operator.result_columns.len() {
        Ok(Arc::clone(&operator.result_columns))
    } else {
        simple_result_columns(stmt, &operator.table, table_alias).map(Arc::from)
    }
}
