//! Lock joined rows using private provenance columns. The ordinary join
//! planner evaluates predicates/order/limit; provenance never reaches clients.
use super::*;
use std::collections::{HashMap, HashSet};

struct Source {
    table: Arc<TableSchema>,
    rows: Vec<crate::storage::DmlRowRef>,
}

fn unsupported() -> QueryError {
    error(crate::error::catalog::ER_NOT_SUPPORTED_YET,
        "locking joins require base tables or simple views without aggregation, DISTINCT, NATURAL/USING or lateral sources")
}

fn source_query(reference: &TableRef, derived: Option<&SubQuery>) -> Select {
    static TEMPLATE: std::sync::LazyLock<Select> = std::sync::LazyLock::new(|| {
        let Statement::Select(select) =
            crate::parser::parse_statement("SELECT * FROM __lock_source").unwrap()
        else {
            unreachable!()
        };
        select
    });
    let mut select = TEMPLATE.clone();
    select.table = Some(reference.name.clone());
    select.from = vec![reference.clone()];
    select.derived = derived.cloned();
    select
}

fn visible_name(reference: &TableRef) -> String {
    reference
        .alias
        .clone()
        .unwrap_or_else(|| crate::storage::split_table_name("test", &reference.name).1)
}

fn expand_projection(db: &Arc<Database>, select: &mut Select) -> Result<(), QueryError> {
    let mut columns = Vec::new();
    for (reference, derived) in select
        .from
        .iter()
        .take(1)
        .map(|t| (t, select.derived.as_ref()))
        .chain(select.joins.iter().map(|j| (&j.table, j.derived.as_ref())))
    {
        columns.push((
            visible_name(reference),
            super::select::describe_select_columns(db, &source_query(reference, derived))?,
        ));
    }
    let mut projection = Vec::new();
    for item in std::mem::take(&mut select.projection) {
        match item {
            SelectItem::Wildcard => {
                for (alias, fields) in &columns {
                    projection.extend(fields.iter().map(|field| SelectItem::Expr {
                        expr: Expr::CompoundIdentifier(vec![alias.clone(), field.name.clone()]),
                        alias: Some(field.name.clone()),
                    }));
                }
            }
            SelectItem::QualifiedWildcard(ref qualifier) => {
                let Some((alias, fields)) = columns
                    .iter()
                    .find(|(alias, _)| alias.eq_ignore_ascii_case(qualifier))
                else {
                    return Err(crate::error::catalog::ER_BAD_FIELD_ERROR
                        .error(format!("Unknown table '{qualifier}'")));
                };
                projection.extend(fields.iter().map(|field| SelectItem::Expr {
                    expr: Expr::CompoundIdentifier(vec![alias.clone(), field.name.clone()]),
                    alias: Some(field.name.clone()),
                }));
            }
            item => projection.push(item),
        }
    }
    select.projection = projection;
    Ok(())
}

fn bind_source(
    db: &Arc<Database>,
    txn: TxnView,
    reference: &mut TableRef,
    derived: &mut Option<SubQuery>,
    sources: &mut Vec<Source>,
    stores: &mut HashMap<String, Arc<crate::engine::api::CteResult>>,
    skipped: &HashSet<(usize, crate::storage::DmlRowRef)>,
) -> Result<String, QueryError> {
    let alias = visible_name(reference);
    if let Some(derived) = derived {
        let SubQuery::Select(inner) = derived else {
            return Err(unsupported());
        };
        if inner.from.len() != 1
            || !inner.joins.is_empty()
            || inner.no_merge
            || inner.distinct
            || !inner.group_by.is_empty()
            || inner.having.is_some()
            || inner.limit.is_some()
            || inner.offset.is_some()
            || !inner.named_windows.is_empty()
            || inner.projection.iter().any(|item| {
                !matches!(
                    item,
                    SelectItem::Wildcard
                        | SelectItem::QualifiedWildcard(_)
                        | SelectItem::Expr {
                            expr: Expr::Identifier(_) | Expr::CompoundIdentifier(_),
                            ..
                        }
                )
            })
        {
            return Err(unsupported());
        }
        expand_projection(db, inner)?;
        let token = bind_source(
            db,
            txn,
            &mut inner.from[0],
            &mut inner.derived,
            sources,
            stores,
            skipped,
        )?;
        inner.table = Some(inner.from[0].name.clone());
        inner.projection.push(SelectItem::Expr {
            expr: Expr::CompoundIdentifier(vec![visible_name(&inner.from[0]), token.clone()]),
            alias: Some(token.clone()),
        });
        return Ok(token);
    }
    if reference.system_time.is_some()
        || reference.generate_series.is_some()
        || !reference.partitions.is_empty()
        || super::cte::lookup_cte(&reference.name).is_some()
    {
        return Err(unsupported());
    }
    let table = db.table(&reference.name)?;
    let mut query = source_query(reference, None);
    let indices = table
        .columns
        .iter()
        .enumerate()
        .filter(|(_, c)| !matches!(c.state, crate::storage::ColumnState::Dropped))
        .map(|(i, _)| i)
        .collect::<Vec<_>>();
    query.projection = indices
        .iter()
        .map(|&i| SelectItem::Expr {
            expr: Expr::Identifier(table.columns[i].name.clone()),
            alias: None,
        })
        .collect();
    let mut columns = super::select::single_table_result_columns(&query, &table, Some(&alias))?;
    let token = format!("\0mydb_lock_origin_{}", sources.len());
    columns.push(Column {
        name: token.clone(),
        column_type: ColumnType::LongLong,
        ..Default::default()
    });
    let mut builder = super::cte::CteSpillBuilder::new(columns, db.data_dir().to_path_buf());
    let mut origins = Vec::new();
    for (storage_id, row) in db.scan_rows_with_storage(&reference.name, txn)? {
        let origin = crate::storage::DmlRowRef {
            storage_id,
            row_id: row.row_id,
        };
        if skipped.contains(&(sources.len(), origin)) {
            continue;
        }
        let mut values = indices
            .iter()
            .map(|&i| row.values[i].clone())
            .collect::<Vec<_>>();
        values.push(Value::UInt(origins.len() as u64));
        origins.push(origin);
        builder.push(values)?;
    }
    let name = super::cte::next_inline_derived_name();
    stores.insert(name.clone(), Arc::new(builder.finish()?));
    reference.name = name;
    reference.alias = Some(alias);
    sources.push(Source {
        table,
        rows: origins,
    });
    Ok(token)
}

/// Returns a fully consumed result at the snapshot whose participating rows
/// are locked. Re-reading after waits prevents stale joins and unlocked rows
/// from appearing when a concurrent writer changes a join key.
pub(super) fn execute(
    db: &Arc<Database>,
    txn: TxnView,
    stmt: &Select,
    registry: &SessionRegistry,
) -> Result<Option<QueryResponse>, QueryError> {
    if !stmt.for_update {
        return Ok(None);
    }
    // The provenance path also handles single-table subqueries, including
    // correlated predicates, and re-evaluates them after waiting for locks.
    let has_subquery = stmt.selection.as_ref().is_some_and(expr_has_subquery)
        || stmt.projection.iter().any(select_item_has_subquery)
        || stmt.order_by.iter().any(|o| expr_has_subquery(&o.expr));
    if stmt.joins.is_empty() && !has_subquery {
        return Ok(None);
    }
    if stmt.from.len() != 1
        || stmt.distinct
        || select_has_window_functions(stmt)
        || stmt
            .joins
            .iter()
            .any(|j| j.natural || !j.using_columns.is_empty() || j.lateral)
        || stmt.projection.iter().any(|item| {
            matches!(
                item,
                SelectItem::Expr {
                    expr: Expr::WindowFunction { .. },
                    ..
                }
            )
        })
    {
        return Err(unsupported());
    }
    crate::planner::validate_select_column_references(db, stmt)?;
    let aggregate = select_projection_has_toplevel_aggregate(&stmt.projection)
        || !stmt.group_by.is_empty()
        || !stmt.grouping_sets.is_empty()
        || stmt
            .having
            .as_ref()
            .is_some_and(expr_has_toplevel_aggregate)
        || stmt
            .order_by
            .iter()
            .any(|order| expr_has_toplevel_aggregate(&order.expr));
    super::dml::validate_lock_targets(stmt)?;
    if aggregate && super::select::optional_limit_value(stmt.limit.as_ref())? == Some(0) {
        let mut query = stmt.clone();
        query.for_update = false;
        query.lock_clause = None;
        return execute_select(db, txn, query, registry)?
            .materialize()
            .map(Some);
    }
    let clause = stmt
        .lock_clause
        .clone()
        .unwrap_or(crate::parser::LockClause {
            mode: crate::parser::LockClauseMode::ForUpdate,
            wait: crate::parser::LockWaitMode::Default,
            targets: Vec::new(),
        });
    let clauses: Vec<_> = stmt
        .from
        .iter()
        .chain(stmt.joins.iter().map(|join| &join.table))
        .map(|reference| clause.for_reference(reference))
        .collect();
    let skip_locked = clauses
        .iter()
        .flatten()
        .any(|clause| clause.wait == crate::parser::LockWaitMode::SkipLocked);
    let output_limit = if skip_locked {
        super::select::optional_limit_value(stmt.limit.as_ref())?
    } else {
        None
    };
    let output_offset = if skip_locked {
        super::select::limit_value_or_zero(stmt.offset.as_ref())?
    } else {
        0
    };
    // LIMIT 0 must keep the original empty query, even with OFFSET: widening
    // it would acquire locks for a statement that cannot return any rows.
    let skip_locked = skip_locked && output_limit != Some(0);
    let mut held = HashSet::new();
    let mut skipped = HashSet::new();
    loop {
        crate::engine::cancel::check_cancelled()?;
        let current = db.session_statement_view(txn.txn_id, txn.isolation, None);
        let _snapshot = db.register_statement_snapshot(current.snapshot_scn);
        let mut query = stmt.clone();
        query.for_update = false;
        query.lock_clause = None;
        if stmt.joins.is_empty() && stmt.derived.is_none() && stmt.from.len() == 1 {
            // Materialize non-correlated subqueries in their original table
            // namespace before replacing the source with a private CTE. In
            // particular an inner unaliased reference to the same base table
            // must never be rebound to the outer CTE's alias by decorrelation.
            let source = &stmt.from[0];
            let table = db.table(&source.name)?;
            let table_name = crate::storage::split_table_name(DEFAULT_DATABASE, &source.name).1;
            let materialize = |expr: &mut Expr| -> Result<(), QueryError> {
                if expr_has_subquery(expr)
                    && !super::dml::expr_has_correlated_subquery(
                        db,
                        expr,
                        source.alias.as_deref(),
                        &table_name,
                        &table.columns,
                    )
                {
                    super::dml::materialize_subqueries(db, current, expr)?;
                }
                Ok(())
            };
            if let Some(selection) = &mut query.selection {
                materialize(selection)?;
            }
            for item in &mut query.projection {
                if let SelectItem::Expr { expr, .. } = item {
                    materialize(expr)?;
                }
            }
            for item in &mut query.order_by {
                materialize(&mut item.expr)?;
            }
        }
        if skip_locked && !aggregate {
            query.limit = None;
            query.offset = None;
        }
        expand_projection(db, &mut query)?;
        let visible_width = query.projection.len();
        let mut sources = Vec::new();
        let mut stores = HashMap::new();
        let alias = visible_name(&query.from[0]);
        let token = bind_source(
            db,
            current,
            &mut query.from[0],
            &mut query.derived,
            &mut sources,
            &mut stores,
            &skipped,
        )?;
        query.table = Some(query.from[0].name.clone());
        query.projection.push(SelectItem::Expr {
            expr: Expr::CompoundIdentifier(vec![alias, token.clone()]),
            alias: Some(token),
        });
        for join in &mut query.joins {
            let alias = visible_name(&join.table);
            let token = bind_source(
                db,
                current,
                &mut join.table,
                &mut join.derived,
                &mut sources,
                &mut stores,
                &skipped,
            )?;
            query.projection.push(SelectItem::Expr {
                expr: Expr::CompoundIdentifier(vec![alias, token.clone()]),
                alias: Some(token),
            });
        }
        let aggregate_query = aggregate.then(|| {
            let mut final_query = query.clone();
            final_query.projection.truncate(visible_width);
            final_query
        });
        let token_offset = if aggregate {
            query.projection.drain(..visible_width);
            query.group_by.clear();
            query.grouping_sets.clear();
            query.group_by_modifier = crate::parser::GroupByModifier::None;
            query.having = None;
            query.order_by.clear();
            query.limit = None;
            query.offset = None;
            0
        } else {
            visible_width
        };
        let QueryResponse::ResultSet(mut result) =
            super::cte::with_cte_registry_extended(stores.clone(), || {
                execute_select(db, current, query, registry)?.materialize()
            })?
        else {
            return Err(unsupported());
        };
        let mut candidates = vec![std::collections::BTreeSet::new(); sources.len()];
        result.rows.retain(|row| {
            !sources.iter().enumerate().any(|(i, source)| {
                let index = match &row[token_offset + i] {
                    Value::UInt(n) => Some(*n as usize),
                    Value::Int(n) => Some(*n as usize),
                    _ => None,
                };
                index.is_some_and(|index| skipped.contains(&(i, source.rows[index])))
            })
        });
        // Apply the window only after excluding locked source rows. Retries
        // can now refill it from later candidates without locking the tail.
        if let Some(limit) = output_limit.filter(|_| !aggregate) {
            result.rows.truncate(output_offset.saturating_add(limit));
        }
        for row in &result.rows {
            for (i, source) in sources.iter().enumerate() {
                if clauses[i].is_none() {
                    continue;
                }
                let index = match &row[token_offset + i] {
                    Value::UInt(n) => *n as usize,
                    Value::Int(n) => *n as usize,
                    Value::Null => continue,
                    _ => return Err(unsupported()),
                };
                let origin = source.rows[index];
                if !held.contains(&(i, origin)) {
                    candidates[i].insert(origin);
                }
            }
        }
        if candidates.iter().all(|rows| rows.is_empty()) {
            if let Some(query) = aggregate_query {
                return super::cte::with_cte_registry_extended(stores, || {
                    execute_select(db, current, query, registry)?
                        .materialize()
                        .map(Some)
                });
            }
            if skip_locked && output_offset > 0 {
                result.rows.drain(..output_offset.min(result.rows.len()));
            }
            result.columns.truncate(visible_width);
            for row in &mut result.rows {
                row.truncate(visible_width);
            }
            return Ok(Some(QueryResponse::ResultSet(result)));
        }
        let mut order = (0..sources.len()).collect::<Vec<_>>();
        order.sort_by_key(|&i| sources[i].table.id);
        for i in order {
            let Some(clause) = clauses[i].clone() else {
                continue;
            };
            let source = &sources[i];
            let rows = candidates[i].iter().copied().collect::<Vec<_>>();
            let acquired =
                super::dml::apply_lock_clause(db, current, &source.table, &rows, clause)?;
            for row in &acquired {
                held.insert((i, *row));
            }
            for row in rows {
                if !held.contains(&(i, row)) {
                    skipped.insert((i, row));
                }
            }
        }
    }
}
