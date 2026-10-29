use super::*;
use crate::parser::Literal;

pub(super) fn validate_lock_targets(stmt: &Select) -> Result<(), QueryError> {
    if let Some(clause) = &stmt.lock_clause {
        for target in &clause.targets {
            let probe = crate::parser::LockClause {
                mode: target.mode,
                wait: target.wait,
                targets: vec![target.clone()],
            };
            if !stmt
                .from
                .iter()
                .chain(stmt.joins.iter().map(|join| &join.table))
                .any(|reference| probe.for_reference(reference).is_some())
            {
                return Err(crate::error::catalog::ER_BAD_FIELD_ERROR
                    .error(format!("Unknown locking target '{}'", target.table)));
            }
        }
    }
    Ok(())
}

pub(super) fn lock_select_rows_for_update(
    db: &Arc<Database>,
    txn: TxnView,
    stmt: &Select,
) -> Result<Option<Vec<crate::storage::DmlRowRef>>, QueryError> {
    if stmt.for_update && stmt.derived.is_some() {
        return Err(error(
            crate::error::catalog::ER_NOT_SUPPORTED_YET,
            "locking a derived query requires mergeable base-table row provenance",
        ));
    }
    if !stmt.for_update || stmt.joins.len() > 0 || stmt.from.len() != 1 {
        return Ok(None);
    }
    validate_lock_targets(stmt)?;
    let table = db.table(stmt.table.as_deref().unwrap_or_default())?;
    let lock_clause = stmt
        .lock_clause
        .clone()
        .unwrap_or(crate::parser::LockClause {
            mode: crate::parser::LockClauseMode::ForUpdate,
            wait: crate::parser::LockWaitMode::Default,
            targets: Vec::new(),
        });
    let lock_clause = lock_clause.for_reference(&stmt.from[0]).ok_or_else(|| {
        crate::error::catalog::ER_BAD_FIELD_ERROR
            .error("locking target is not a source of this query")
    })?;
    // Fast path: unique-key exact match (the common SELECT pk = ? FOR UPDATE
    // shape used by TPC-C's district/customer/warehouse access). Skips the
    // candidate read+filter step entirely — the post-lock re-read in
    // `execute_locked_single_table_select` already reads the row at the fresh
    // snapshot, so reading it twice is wasteful.
    if stmt.order_by.is_empty()
        && stmt.limit.is_none()
        && !stmt.distinct
        && stmt.group_by.is_empty()
        && stmt.having.is_none()
    {
        if let Some(row_id) =
            unique_row_id_for_exact_write(db, txn, &table, stmt.selection.as_ref())?
        {
            let row_ref = crate::storage::DmlRowRef {
                storage_id: table.id,
                row_id,
            };
            let locked = apply_lock_clause(db, txn, &table, &[row_ref], lock_clause)?;
            return Ok(Some(locked));
        }
    }
    if let Some(row_ids) = ordered_select_for_update_candidates(db, txn, &table, stmt)? {
        let locked = apply_lock_clause(db, txn, &table, &row_ids, lock_clause)?;
        return Ok(Some(locked));
    }
    let candidates = candidate_row_refs_for_write(db, txn, &table, stmt.selection.as_ref())?;
    let rows = db.dml_candidate_rows_with_origin_for_write(&table, txn, candidates)?;
    let mut matched = Vec::new();
    for row in rows {
        crate::engine::cancel::check_cancelled()?;
        let matches = match &stmt.selection {
            Some(expr) => eval_bool(expr, &table, &row.values)?,
            None => true,
        };
        if matches {
            matched.push(row);
        }
    }
    if !stmt.order_by.is_empty() {
        let fields: Vec<_> = stmt
            .order_by
            .iter()
            .map(|item| {
                let mut field = crate::engine::executor::sort::SortField::from_order_by(item);
                field.collation_id = crate::engine::expr::expr_collation(&item.expr, &table)
                    .map(|info| info.collation_id);
                field
            })
            .collect();
        let mut ranked = Vec::with_capacity(matched.len());
        for row in matched.drain(..) {
            let keys = stmt
                .order_by
                .iter()
                .map(|item| eval_expr(&item.expr, &table, &row.values))
                .collect::<Result<Vec<_>, _>>()?;
            ranked.push((row, keys));
        }
        ranked.sort_by(|(left_row, left_keys), (right_row, right_keys)| {
            for ((left, right), field) in left_keys
                .iter()
                .zip(right_keys.iter())
                .zip(fields.iter().copied())
            {
                let ordering =
                    crate::engine::executor::sort::compare_value_for_sort(left, right, field);
                if ordering != Ordering::Equal {
                    return ordering;
                }
            }
            left_row.row_ref.cmp(&right_row.row_ref)
        });
        matched = ranked.into_iter().map(|(row, _)| row).collect();
    }
    let offset = select_offset(stmt)?.min(matched.len());
    let end = select_limit(stmt)?
        .map(|limit| offset.saturating_add(limit).min(matched.len()))
        .unwrap_or(matched.len());
    let matched = matched[offset..end].to_vec();
    let candidate_refs = matched.iter().map(|row| row.row_ref).collect::<Vec<_>>();
    let locked = apply_lock_clause(db, txn, &table, &candidate_refs, lock_clause)?;
    Ok(Some(locked))
}

/// M1.3 - Apply the SELECT lock clause: FOR UPDATE/FOR SHARE with NOWAIT/SKIP LOCKED semantics.
pub(super) fn apply_lock_clause(
    db: &Arc<Database>,
    txn: TxnView,
    table: &TableSchema,
    row_refs: &[crate::storage::DmlRowRef],
    clause: crate::parser::LockClause,
) -> Result<Vec<crate::storage::DmlRowRef>, QueryError> {
    use crate::parser::{LockClauseMode, LockWaitMode};
    use crate::storage::wait_events::WaitEvent;
    let mode = match clause.mode {
        LockClauseMode::ForUpdate => crate::storage::wait_events::WaitEvent::RowLockContention,
        LockClauseMode::ForShare => crate::storage::wait_events::WaitEvent::RowLockContention,
    };
    let _ = mode; // silence unused
    let lock_mode = match clause.mode {
        LockClauseMode::ForUpdate => crate::storage::LockMode::Exclusive,
        LockClauseMode::ForShare => crate::storage::LockMode::Shared,
    };
    match clause.wait {
        LockWaitMode::SkipLocked => {
            let acquired = db.try_lock_rows_skip_locked(txn, table, row_refs, lock_mode)?;
            if !acquired.is_empty() {
                let _ = WaitEvent::RowLockContention; // touch for clarity
            }
            Ok(acquired)
        }
        LockWaitMode::NoWait => {
            // Try once; if any row is held by another txn, return error 3572.
            db.try_acquire_row_locks_nowait(txn, table, row_refs, lock_mode)?;
            Ok(row_refs.to_vec())
        }
        LockWaitMode::Default => {
            match clause.mode {
                LockClauseMode::ForUpdate => {
                    db.lock_rows_for_update(txn, table, row_refs)?;
                }
                LockClauseMode::ForShare => {
                    db.lock_rows_for_share(txn, table, row_refs)?;
                }
            }
            Ok(row_refs.to_vec())
        }
    }
}

pub(super) fn ordered_select_for_update_candidates(
    db: &Arc<Database>,
    txn: TxnView,
    table: &TableSchema,
    stmt: &Select,
) -> Result<Option<Vec<crate::storage::DmlRowRef>>, QueryError> {
    if stmt.order_by.is_empty()
        || stmt.distinct
        || !stmt.group_by.is_empty()
        || stmt.having.is_some()
        || select_projection_has_aggregate(&stmt.projection)
        || stmt
            .order_by
            .iter()
            .any(|item| !item.asc || item.has_custom_nulls())
        || stmt
            .selection
            .as_ref()
            .is_some_and(|expr| expr_has_subquery(expr))
    {
        return Ok(None);
    }
    let Some(selection) = stmt.selection.as_ref() else {
        return Ok(None);
    };
    let offset = select_offset(stmt)?;
    let limit = select_limit(stmt)?;
    let fetch = limit
        .map(|limit| offset.saturating_add(limit))
        .unwrap_or(usize::MAX);
    let candidates = ordered_index_limited_dml_candidates(
        db,
        txn,
        table,
        Some(selection),
        &stmt.order_by,
        Some(fetch),
        None,
    )?;
    let Some(mut candidates) = candidates else {
        return Ok(None);
    };
    let start = offset.min(candidates.len());
    let end = limit
        .map(|limit| start.saturating_add(limit).min(candidates.len()))
        .unwrap_or(candidates.len());
    Ok(Some(candidates.drain(start..end).collect()))
}

pub(super) fn execute_locked_single_table_select(
    db: &Arc<Database>,
    txn: TxnView,
    stmt: &Select,
    row_refs: Vec<crate::storage::DmlRowRef>,
) -> Result<Option<QueryResponse>, QueryError> {
    if stmt.derived.is_some()
        || stmt.joins.len() > 0
        || stmt.from.len() != 1
        || stmt.table.is_none()
        || stmt.distinct
        || !stmt.group_by.is_empty()
        || stmt.having.is_some()
        || select_projection_has_aggregate(&stmt.projection)
        || stmt
            .projection
            .iter()
            .any(|item| select_item_has_subquery(item))
        || stmt
            .order_by
            .iter()
            .any(|item| expr_has_subquery(&item.expr))
    {
        return Ok(None);
    }

    let table = db.table(stmt.table.as_deref().unwrap_or_default())?;
    let columns = single_table_result_columns(stmt, &table, None)?;
    let mut rows = Vec::with_capacity(row_refs.len());
    for row_ref in row_refs {
        crate::engine::cancel::check_cancelled()?;
        let Some(row) = db.dml_candidate_row_at_for_write(&table, row_ref, txn)? else {
            continue;
        };
        if stmt
            .selection
            .as_ref()
            .map(|expr| eval_bool(expr, &table, &row.values))
            .transpose()?
            .unwrap_or(true)
        {
            let projected = project_table_row(stmt, &table, &row.values)?;
            let order_values = stmt
                .order_by
                .iter()
                .map(|item| eval_expr(&item.expr, &table, &row.values))
                .collect::<Result<Vec<_>, _>>()?;
            rows.push((projected, order_values));
        }
    }

    if !stmt.order_by.is_empty() {
        // Resolve NULL placement (which reads the MYDB_ORDER_BY_NULLS env
        // default) exactly once per sort, not once per comparison.
        let sort_fields = crate::engine::executor::sort::fields_from_order_by(&stmt.order_by);
        rows.sort_by(|left, right| {
            crate::engine::executor::sort::compare_sort_keys(&sort_fields, &left.1, &right.1)
        });
    }

    let offset = select_offset(stmt)?.min(rows.len());
    let end = stmt
        .limit
        .as_ref()
        .and_then(LimitValue::as_usize)
        .map(|limit| offset.saturating_add(limit).min(rows.len()))
        .unwrap_or(rows.len());
    let rows = rows
        .into_iter()
        .skip(offset)
        .take(end.saturating_sub(offset))
        .map(|(values, _)| values)
        .collect();
    Ok(Some(QueryResponse::ResultSet(ResultSet { columns, rows })))
}

pub(super) fn validate_assignment_qualifier(
    assignment: &crate::parser::Assignment,
    table: &str,
    alias: Option<&str>,
) -> Result<(), QueryError> {
    if let Some(qualifier) = &assignment.table_qualifier {
        let expected = alias.unwrap_or(table.rsplit('.').next().unwrap_or(table));
        if !qualifier.eq_ignore_ascii_case(expected) {
            return Err(error(
                crate::error::catalog::ER_BAD_FIELD_ERROR,
                format!("unknown column '{}.{}'", qualifier, assignment.column),
            ));
        }
    }
    Ok(())
}

pub(super) fn execute_update(
    db: &Arc<Database>,
    session: &mut SessionContext,
    txn: TxnView,
    mut stmt: Update,
    registry: &SessionRegistry,
) -> Result<QueryResponse, QueryError> {
    let _dml_expression_guard = crate::engine::expr::DmlExpressionGuard::new();
    let _ignore_coercion = crate::storage::DmlIgnoreCoercionGuard::new(stmt.ignore);
    crate::engine::cancel::check_cancelled()?;
    // `sql_safe_updates`: reject UPDATE that lacks both a WHERE and a
    // LIMIT clause. MySQL error 1175 ER_UPDATE_WITHOUT_KEY_IN_SAFE_MODE.
    // The check stays generous: any WHERE clause counts (MySQL's stricter
    // form additionally requires the WHERE to reference a KEY column);
    // matching that requires the optimiser, which already ran by this
    // point. We keep the practical guard MySQL ships by default.
    check_safe_updates(session, stmt.selection.as_ref(), stmt.limit.as_ref())?;
    // M5.C2 — `UPDATE t PARTITION (p0, p1) ...` restricts scans to the
    // named partitions; resolve to storage_ids and install via the
    // partition-router thread-local around the rest of the statement.
    if super::multi_table::is_multi_table_update(&stmt) {
        return super::multi_table::execute_multi_update(db, session, txn, stmt, registry);
    }
    if !stmt.partitions.is_empty() {
        let storage_ids =
            super::partition_clause::resolve_partition_clause(db, &stmt.table, &stmt.partitions)?
                .unwrap_or_default();
        let partitions = std::mem::take(&mut stmt.partitions);
        let result = super::partition_router::with_pruned_partitions(storage_ids, || {
            execute_update(db, session, txn, stmt.clone(), registry)
        });
        let _ = partitions;
        return result;
    }

    let outer_alias = stmt.primary_alias.clone();
    let outer_tname = stmt
        .table
        .rsplit('.')
        .next()
        .unwrap_or(stmt.table.as_str())
        .to_string();
    let table = db.table(&stmt.table)?.clone();
    for assignment in &stmt.assignments {
        validate_assignment_qualifier(assignment, &outer_tname, outer_alias.as_deref())?;
    }
    let selection_is_correlated = stmt.selection.as_ref().is_some_and(|selection| {
        expr_has_correlated_subquery(
            db,
            selection,
            outer_alias.as_deref(),
            &outer_tname,
            &table.columns,
        )
    });
    if !selection_is_correlated {
        if let Some(selection) = &mut stmt.selection {
            materialize_subqueries(db, txn, selection)?;
        }
    }
    if let Some(check_predicate) = &mut stmt.view_check_predicate {
        prepare_view_check(db, txn, check_predicate, &table, outer_alias.as_deref())?;
    }
    for order in &mut stmt.order_by {
        if super::prepared_dml::expr_contains_subquery(&order.expr) {
            materialize_subqueries(db, txn, &mut order.expr)?;
        }
    }
    // A SET expression whose subquery references the target table's alias /
    // name is *correlated* — it must be resolved per row against the current
    // row's outer columns (issue #70). Only materialise the non-correlated
    // subqueries once here; correlated ones are left intact for the per-row
    // `resolve_correlated_expr` below.
    for assignment in &mut stmt.assignments {
        if !expr_has_correlated_subquery(
            db,
            &assignment.value,
            outer_alias.as_deref(),
            &outer_tname,
            &table.columns,
        ) {
            materialize_subqueries(db, txn, &mut assignment.value)?;
        }
    }
    let selection_for_scan = (!selection_is_correlated)
        .then_some(stmt.selection.as_ref())
        .flatten();
    let eval_selection = |expr: &Expr, values: &[Value]| {
        eval_delete_selection(
            db,
            txn,
            expr,
            selection_is_correlated,
            outer_alias.as_deref(),
            &outer_tname,
            &table,
            values,
        )
    };
    if let Some(selection) = &stmt.selection {
        crate::engine::expr::validate_expr_temporal_literals(selection, &table)?;
        crate::engine::expr::validate_dml_temporal_functions(selection)?;
    }
    let view_check_predicate = stmt.view_check_predicate.clone();
    let mut updates: Vec<(usize, Expr, bool)> = Vec::with_capacity(stmt.assignments.len());
    let mut changed_column_indexes = Vec::new();
    for assignment in std::mem::take(&mut stmt.assignments) {
        let idx = table.column_index(&assignment.column)?;
        if table.columns[idx].generated_expr.is_some() {
            if !update_expr_is_generated_default(&assignment.value, &table.columns[idx].name) {
                return Err(error(
                    crate::error::catalog::ER_NON_DEFAULT_VALUE_FOR_GENERATED_COLUMN,
                    "value specified for generated column is not allowed; use DEFAULT",
                ));
            }
            continue;
        }
        if !changed_column_indexes.contains(&idx) {
            changed_column_indexes.push(idx);
        }
        let correlated = expr_has_correlated_subquery(
            db,
            &assignment.value,
            outer_alias.as_deref(),
            &outer_tname,
            &table.columns,
        );
        if let Some((_, existing, existing_corr)) = updates
            .iter_mut()
            .find(|(existing_idx, _, _)| *existing_idx == idx)
        {
            *existing = assignment.value;
            *existing_corr = correlated;
        } else {
            updates.push((idx, assignment.value, correlated));
        }
    }
    // No-op detection compares only the *user-set* columns: an
    // `ON UPDATE CURRENT_TIMESTAMP` column always differs (fresh NOW() vs
    // stored), so including it would make every matched row look changed
    // and defeat the point. Snapshot the user columns before appending the
    // auto on-update columns to the write set below.
    let user_changed_columns: Vec<usize> = changed_column_indexes.clone();
    let on_update_columns = on_update_timestamp_columns(&table, &changed_column_indexes);
    for &idx in &on_update_columns {
        changed_column_indexes.push(idx);
    }
    let has_update_triggers = table_has_update_triggers(db, &stmt.table);
    let apply_updates = |values: &[Value]| -> Result<Vec<Value>, QueryError> {
        let mut new_values = values.to_vec();
        for (idx, expr, correlated) in &updates {
            // MySQL evaluates SET assignments left-to-right: a later RHS sees
            // the already-updated value of an earlier column, so evaluate
            // against the progressively-updated `new_values`, not the
            // original `values` (issue #80).
            //
            // A correlated subquery in the RHS is resolved against this row's
            // outer columns first, then materialised (issue #70).
            let resolved;
            let expr = if *correlated {
                let outer = UpdateOuterRow {
                    alias: outer_alias.as_deref(),
                    table_name: &outer_tname,
                    table: &table,
                    values: &new_values,
                };
                resolved = resolve_correlated_expr(db, txn, expr, &outer)?;
                &resolved
            } else {
                expr
            };
            let expr = crate::engine::expr::bind_column_defaults(expr, &table, Some(*idx))?;
            let v = eval_expr(&expr, &table, &new_values)?;
            new_values[*idx] = crate::engine::expr::binary_assignment_value(
                v,
                &expr,
                &table,
                &table.columns[*idx].data_type,
            )?;
        }
        if has_update_triggers {
            // BEFORE UPDATE observes the typed assignment image. Automatic
            // timestamps change only for a real SET change; a trigger may
            // still explicitly change any column, including the timestamp.
            crate::storage::coerce_updated_row(
                &table,
                &mut new_values,
                Some(&user_changed_columns),
            )?;
        }
        if !has_update_triggers
            || user_changed_columns
                .iter()
                .any(|&idx| values[idx] != new_values[idx])
        {
            apply_on_update_timestamps(&table, &mut new_values, &on_update_columns);
        }
        Ok(new_values)
    };
    let validate_view_check = |values: &[Value]| -> Result<(), QueryError> {
        if let Some(predicate) = &view_check_predicate {
            if !eval_view_check(db, txn, predicate, &table, values, outer_alias.as_deref())? {
                return Err(error(
                    crate::error::catalog::ER_VIEW_CHECK_FAILED,
                    "CHECK OPTION failed for view",
                ));
            }
        }
        Ok(())
    };
    let ordered_limited_candidates = ordered_limited_dml_candidates(
        db,
        txn,
        &table,
        selection_for_scan,
        &stmt.order_by,
        stmt.limit.as_ref(),
        !selection_is_correlated,
    )?;
    if let Some(candidate_set) = ordered_limited_candidates {
        return execute_update_candidates(
            db,
            session,
            txn,
            &stmt,
            &table,
            registry,
            candidate_set,
            has_update_triggers,
            &changed_column_indexes,
            &user_changed_columns,
            |values| match &stmt.selection {
                Some(expr) => eval_selection(expr, values),
                None => Ok(true),
            },
            apply_updates,
            validate_view_check,
        );
    }
    if !has_update_triggers && stmt.returning.is_none() {
        if let Some(row_id) = unique_row_id_for_exact_write(db, txn, &table, selection_for_scan)? {
            let changed = match db.update_row_by_id_with_changed_columns_noop(
                txn,
                &table,
                row_id,
                Some(&changed_column_indexes),
                Some(&user_changed_columns),
                |values| {
                    let new_values = apply_updates(values)?;
                    validate_view_check(&new_values)?;
                    Ok(new_values)
                },
            ) {
                Ok(updated) => updated as u64,
                Err(err) if stmt.ignore && crate::storage::ignorable_update_error(&err) => {
                    crate::engine::api::push_statement_condition(
                        crate::error::ConditionLevel::Warning,
                        &err,
                    );
                    0
                }
                Err(err) => return Err(err),
            };
            // S2/Q2 — the unique lookup resolved exactly one live row,
            // so `matched == 1`; `changed` is 0 when the UPDATE is a
            // no-op (all user-set columns already hold the new values).
            // The protocol layer returns matched to CLIENT_FOUND_ROWS
            // clients and changed (affected_rows) to everyone else.
            crate::engine::api::set_statement_matched_rows(1);
            return Ok(QueryResponse::OkAffected {
                affected_rows: changed,
            });
        }
    }
    let candidates = if table.is_partitioned() {
        None
    } else {
        candidate_row_ids_for_write(db, txn, &table, selection_for_scan)?
    };
    let origin_candidates = if table.is_partitioned() {
        candidate_row_refs_for_write(db, txn, &table, selection_for_scan)?
    } else {
        candidates.map(|rows| {
            rows.into_iter()
                .map(|row_id| crate::storage::DmlRowRef {
                    storage_id: table.id,
                    row_id,
                })
                .collect()
        })
    };
    if has_update_triggers || stmt.returning.is_some() {
        let rows = db.dml_candidate_rows_with_origin_for_write(&table, txn, origin_candidates)?;
        let candidate_set = DmlCandidateSet {
            rows: rows.into_iter().map(|r| r.row_ref).collect(),
            limit: None,
        };
        return execute_update_candidates(
            db,
            session,
            txn,
            &stmt,
            &table,
            registry,
            candidate_set,
            has_update_triggers,
            &changed_column_indexes,
            &user_changed_columns,
            |values| match &stmt.selection {
                Some(expr) => eval_selection(expr, values),
                None => Ok(true),
            },
            apply_updates,
            validate_view_check,
        );
    }
    // `matched` counts lock-rechecked WHERE hits; `changed` is the storage return (rows that
    // actually differed after coercion). No-op rows raise matched but not
    // changed — MySQL's `Rows matched: N Changed: M` split.
    let matched_rows = std::cell::Cell::new(0u64);
    let update_rows = |ignore_checks| {
        let predicate = |values: &[Value]| -> Result<bool, QueryError> {
            crate::engine::cancel::check_cancelled()?;
            match &stmt.selection {
                Some(expr) => eval_selection(expr, values),
                None => Ok(true),
            }
        };
        let apply = |values: &[Value]| -> Result<Vec<Value>, QueryError> {
            // Storage invokes `apply` exactly once for a row whose predicate
            // still matches after the row lock is acquired. Counting here
            // avoids double-counting the snapshot prefilter and current-read
            // recheck while preserving matched-but-unchanged semantics.
            matched_rows.set(matched_rows.get() + 1);
            let new_values = apply_updates(values)?;
            validate_view_check(&new_values)?;
            Ok(new_values)
        };
        db.update_rows_with_changed_columns_noop_at(
            txn,
            &stmt.table,
            origin_candidates.clone(),
            Some(&changed_column_indexes),
            Some(&user_changed_columns),
            predicate,
            apply,
            ignore_checks,
        )
    };
    let affected_rows = update_rows(stmt.ignore)? as u64;
    crate::engine::api::set_statement_matched_rows(matched_rows.get());
    Ok(QueryResponse::OkAffected { affected_rows })
}

/// Consume physical candidates once; access planning remains outside this loop.
/// All expression/trigger work uses the locked current row. In particular, a
/// swallowed error is not a successful no-op and cannot produce RETURNING.
#[allow(clippy::too_many_arguments)]
fn execute_update_candidates<E, A, V>(
    db: &Arc<Database>,
    session: &mut SessionContext,
    txn: TxnView,
    stmt: &Update,
    table: &TableSchema,
    registry: &SessionRegistry,
    candidates: DmlCandidateSet,
    has_triggers: bool,
    changed_columns: &[usize],
    compare_columns: &[usize],
    selection: E,
    apply: A,
    validate: V,
) -> Result<QueryResponse, QueryError>
where
    E: Fn(&[Value]) -> Result<bool, QueryError>,
    A: Fn(&[Value]) -> Result<Vec<Value>, QueryError>,
    V: Fn(&[Value]) -> Result<(), QueryError>,
{
    let mut returning = super::returning::ReturningCollector::new(
        stmt.returning.as_ref(),
        table,
        super::returning::DefaultImage::New,
        stmt.primary_alias.as_deref(),
    )?;
    let mut matched_rows = 0u64;
    let mut affected_rows = 0u64;
    let needs_images = has_triggers || stmt.returning.is_some();
    let all_columns = if has_triggers {
        (0..table.columns.len()).collect::<Vec<_>>()
    } else {
        Vec::new()
    };
    for row_ref in candidates.rows {
        if matched_rows as usize >= candidates.limit.unwrap_or(usize::MAX) {
            break;
        }
        crate::engine::cancel::check_cancelled()?;
        let mut matched = false;
        let mut trigger_failed = false;
        let update = |old: &[Value]| {
            if !selection(old)? {
                return Ok(old.to_vec());
            }
            matched = true;
            let mut new = apply(old)?;
            if has_triggers {
                new = fire_before_update(db, session, table, old.to_vec(), new, registry).map_err(
                    |err| {
                        trigger_failed = true;
                        err
                    },
                )?;
            }
            validate(&new)?;
            Ok(new)
        };
        // BEFORE can write columns outside SET, including generated dependencies.
        let writes = if has_triggers {
            None
        } else {
            Some(changed_columns)
        };
        let compare = if has_triggers {
            Some(all_columns.as_slice())
        } else {
            Some(compare_columns)
        };
        let result = if needs_images {
            db.update_row_with_images_at(
                txn,
                table,
                Some(row_ref.storage_id),
                row_ref.row_id,
                writes,
                compare,
                update,
            )
        } else {
            db.update_row_by_id_with_changed_columns_noop_at(
                txn,
                table,
                Some(row_ref.storage_id),
                row_ref.row_id,
                writes,
                compare,
                update,
            )
            .map(|changed| (changed, None))
        };
        if matched {
            matched_rows += 1;
        }
        let (changed, images) = match result {
            Ok(result) => result,
            Err(err)
                if stmt.ignore
                    && !trigger_failed
                    && crate::storage::ignorable_update_error(&err) =>
            {
                crate::engine::api::push_statement_condition(
                    crate::error::ConditionLevel::Warning,
                    &err,
                );
                continue;
            }
            Err(err) => return Err(err),
        };
        if !matched {
            continue;
        }
        affected_rows += u64::from(changed);
        if let Some((old, new)) = images {
            if has_triggers {
                fire_after_update(db, session, table, old.clone(), new.clone(), registry)?;
            }
            returning.push(Some(&old), Some(&new))?;
        }
    }
    crate::engine::api::set_statement_matched_rows(matched_rows);
    Ok(returning.finish(QueryResponse::OkAffected { affected_rows }))
}

#[allow(clippy::too_many_arguments)]
fn execute_delete_candidates(
    db: &Arc<Database>,
    session: &mut SessionContext,
    txn: TxnView,
    stmt: &Delete,
    table: &TableSchema,
    registry: &SessionRegistry,
    candidates: DmlCandidateSet,
    has_triggers: bool,
    selection_is_correlated: bool,
    outer_alias: Option<&str>,
    outer_tname: &str,
) -> Result<QueryResponse, QueryError> {
    let mut returning = super::returning::ReturningCollector::new(
        stmt.returning.as_ref(),
        table,
        super::returning::DefaultImage::Old,
        stmt.primary_alias.as_deref(),
    )?;
    let needs_old = has_triggers || stmt.returning.is_some();
    let mut affected_rows = 0u64;
    for row_ref in candidates.rows {
        if affected_rows as usize >= candidates.limit.unwrap_or(usize::MAX) {
            break;
        }
        crate::engine::cancel::check_cancelled()?;
        let mut old_image = None;
        let mut trigger_failed = false;
        let result =
            db.delete_row_by_id_at_if(txn, table, row_ref.storage_id, row_ref.row_id, |values| {
                if let Some(expr) = &stmt.selection {
                    if !eval_delete_selection(
                        db,
                        txn,
                        expr,
                        selection_is_correlated,
                        outer_alias,
                        outer_tname,
                        table,
                        values,
                    )? {
                        return Ok(false);
                    }
                }
                if has_triggers {
                    fire_before_delete(db, session, table, values.to_vec(), registry).map_err(
                        |err| {
                            trigger_failed = true;
                            err
                        },
                    )?;
                }
                if needs_old {
                    old_image = Some(values.to_vec());
                }
                Ok(true)
            });
        let deleted = match result {
            Ok(deleted) => deleted,
            Err(err) if stmt.ignore && !trigger_failed && is_ignorable_delete_error(&err) => {
                crate::engine::api::push_statement_condition(
                    crate::error::ConditionLevel::Warning,
                    &err,
                );
                continue;
            }
            Err(err) => return Err(err),
        };
        if deleted {
            if let Some(old) = old_image {
                if has_triggers {
                    fire_after_delete(db, session, table, old.clone(), registry)?;
                }
                returning.push(Some(&old), None)?;
            }
            affected_rows += 1;
        }
    }
    Ok(returning.finish(QueryResponse::OkAffected { affected_rows }))
}

pub(super) fn execute_update_borrowed_simple(
    db: &Arc<Database>,
    session: &mut SessionContext,
    txn: TxnView,
    stmt: &Update,
) -> Result<Option<QueryResponse>, QueryError> {
    crate::engine::cancel::check_cancelled()?;
    check_safe_updates(session, stmt.selection.as_ref(), stmt.limit.as_ref())?;
    if !stmt.partitions.is_empty()
        || stmt.returning.is_some()
        || super::multi_table::is_multi_table_update(stmt)
        || stmt
            .selection
            .as_ref()
            .is_some_and(|expr| expr_has_subquery(expr))
        || stmt
            .view_check_predicate
            .as_ref()
            .is_some_and(|expr| expr_has_subquery(expr))
        || stmt
            .assignments
            .iter()
            .any(|assignment| expr_has_subquery(&assignment.value))
        || table_has_update_triggers(db, &stmt.table)
    {
        return Ok(None);
    }

    let table = db.table(&stmt.table)?.clone();
    if let Some(selection) = &stmt.selection {
        crate::engine::expr::validate_expr_temporal_literals(selection, &table)?;
        crate::engine::expr::validate_dml_temporal_functions(selection)?;
    }
    let mut updates: Vec<(usize, &Expr)> = Vec::with_capacity(stmt.assignments.len());
    let mut changed_column_indexes = Vec::new();
    for assignment in &stmt.assignments {
        let idx = table.column_index(&assignment.column)?;
        if table.columns[idx].generated_expr.is_some() {
            if !update_expr_is_generated_default(&assignment.value, &table.columns[idx].name) {
                return Err(error(
                    crate::error::catalog::ER_NON_DEFAULT_VALUE_FOR_GENERATED_COLUMN,
                    "value specified for generated column is not allowed; use DEFAULT",
                ));
            }
            continue;
        }
        if !changed_column_indexes.contains(&idx) {
            changed_column_indexes.push(idx);
        }
        if let Some((_, existing)) = updates
            .iter_mut()
            .find(|(existing_idx, _)| *existing_idx == idx)
        {
            *existing = &assignment.value;
        } else {
            updates.push((idx, &assignment.value));
        }
    }
    // B7 — snapshot the user-set columns BEFORE appending the auto
    // `ON UPDATE CURRENT_TIMESTAMP` columns. No-op detection must compare
    // only user-set columns: an on-update timestamp always differs, so
    // including it would make every matched row look changed. This mirrors
    // the text-dispatch `execute_update` path so a statement returns the
    // same matched/changed split whether or not it hit the plan cache.
    let user_changed_columns: Vec<usize> = changed_column_indexes.clone();
    let on_update_columns = on_update_timestamp_columns(&table, &changed_column_indexes);
    for &idx in &on_update_columns {
        changed_column_indexes.push(idx);
    }
    let apply_updates = |values: &[Value]| -> Result<Vec<Value>, QueryError> {
        let mut new_values = values.to_vec();
        for (idx, expr) in &updates {
            // Left-to-right SET semantics: evaluate against the
            // progressively-updated row so a later RHS sees earlier
            // columns' new values (issue #80).
            let v = if update_expr_contains_default(expr) {
                let bound = crate::engine::expr::bind_column_defaults(expr, &table, Some(*idx))?;
                eval_expr(&bound, &table, &new_values)?
            } else {
                eval_expr(expr, &table, &new_values)?
            };
            new_values[*idx] = crate::engine::expr::binary_assignment_value(
                v,
                expr,
                &table,
                &table.columns[*idx].data_type,
            )?;
        }
        apply_on_update_timestamps(&table, &mut new_values, &on_update_columns);
        Ok(new_values)
    };
    let validate_view_check = |values: &[Value]| -> Result<(), QueryError> {
        if let Some(predicate) = &stmt.view_check_predicate {
            if !eval_bool(predicate, &table, values)? {
                return Err(error(
                    crate::error::catalog::ER_VIEW_CHECK_FAILED,
                    "CHECK OPTION failed for view",
                ));
            }
        }
        Ok(())
    };

    let ordered_limited_candidates = ordered_limited_dml_candidates(
        db,
        txn,
        &table,
        stmt.selection.as_ref(),
        &stmt.order_by,
        stmt.limit.as_ref(),
        true,
    )?;
    if let Some(candidate_set) = ordered_limited_candidates {
        let mut affected_rows = 0u64;
        let mut matched_rows = 0u64;
        for row_ref in candidate_set.rows {
            if matched_rows as usize >= candidate_set.limit.unwrap_or(usize::MAX) {
                break;
            }
            crate::engine::cancel::check_cancelled()?;
            let Some(_) = db.dml_candidate_row_at_for_write(&table, row_ref, txn)? else {
                continue;
            };
            let still_matches = std::cell::Cell::new(false);
            let changed = match db.update_row_by_id_with_changed_columns_noop_at(
                txn,
                &table,
                Some(row_ref.storage_id),
                row_ref.row_id,
                Some(&changed_column_indexes),
                Some(&user_changed_columns),
                |values| {
                    if let Some(expr) = &stmt.selection {
                        if !eval_bool(expr, &table, values)? {
                            return Ok(values.to_vec());
                        }
                    }
                    still_matches.set(true);
                    let new_values = apply_updates(values)?;
                    validate_view_check(&new_values)?;
                    Ok(new_values)
                },
            ) {
                Ok(changed) => changed,
                Err(err) if stmt.ignore && crate::storage::ignorable_update_error(&err) => {
                    crate::engine::api::push_statement_condition(
                        crate::error::ConditionLevel::Warning,
                        &err,
                    );
                    false
                }
                Err(err) => return Err(err),
            };
            if !still_matches.get() {
                continue;
            }
            matched_rows += 1;
            affected_rows += changed as u64;
        }
        crate::engine::api::set_statement_matched_rows(matched_rows);
        return Ok(Some(QueryResponse::OkAffected { affected_rows }));
    }
    let ordered_limited_candidates: Option<Vec<RowId>> = None;
    if let Some(row_id) = unique_row_id_for_exact_write(db, txn, &table, stmt.selection.as_ref())? {
        if ordered_limited_candidates.is_none() {
            let changed = match db.update_row_by_id_with_changed_columns_noop(
                txn,
                &table,
                row_id,
                Some(&changed_column_indexes),
                Some(&user_changed_columns),
                |values| {
                    let new_values = apply_updates(values)?;
                    validate_view_check(&new_values)?;
                    Ok(new_values)
                },
            ) {
                Ok(updated) => updated as u64,
                Err(err) if stmt.ignore && crate::storage::ignorable_update_error(&err) => {
                    crate::engine::api::push_statement_condition(
                        crate::error::ConditionLevel::Warning,
                        &err,
                    );
                    0
                }
                Err(err) => return Err(err),
            };
            // matched == 1 (the unique lookup found one live row); changed
            // is 0 on a no-op. FOUND_ROWS clients get matched, others get
            // changed.
            crate::engine::api::set_statement_matched_rows(1);
            return Ok(Some(QueryResponse::OkAffected {
                affected_rows: changed,
            }));
        }
    }

    let candidates = candidate_row_refs_for_write(db, txn, &table, stmt.selection.as_ref())?;
    // B7 — track matched (WHERE hits) separately from changed (rows that
    // actually differed), with no-op detection on user-set columns, exactly
    // like the text path. `matched` feeds CLIENT_FOUND_ROWS.
    let matched_rows = std::cell::Cell::new(0u64);
    let predicate = |values: &[Value]| -> Result<bool, QueryError> {
        crate::engine::cancel::check_cancelled()?;
        match &stmt.selection {
            Some(expr) => eval_bool(expr, &table, values),
            None => Ok(true),
        }
    };
    let apply = |values: &[Value]| -> Result<Vec<Value>, QueryError> {
        matched_rows.set(matched_rows.get() + 1);
        let new_values = apply_updates(values)?;
        validate_view_check(&new_values)?;
        Ok(new_values)
    };
    let affected_rows = db.update_rows_with_changed_columns_noop_at(
        txn,
        &stmt.table,
        candidates,
        Some(&changed_column_indexes),
        Some(&user_changed_columns),
        predicate,
        apply,
        stmt.ignore,
    )? as u64;
    crate::engine::api::set_statement_matched_rows(matched_rows.get());
    Ok(Some(QueryResponse::OkAffected { affected_rows }))
}

/// Small indexed updates avoid candidate row materialization and decode only
/// assignment/predicate columns under the row lock. Large ranges retain bulk
/// heap/zone-map/index publication, using the same compiled expressions.
pub(super) fn execute_prepared_range_update(
    db: &Arc<Database>,
    session: &mut SessionContext,
    txn: TxnView,
    stmt: &Update,
    range: &crate::engine::api::PreparedRangeUpdate,
) -> Result<QueryResponse, QueryError> {
    crate::engine::cancel::check_cancelled()?;
    check_safe_updates(session, stmt.selection.as_ref(), stmt.limit.as_ref())?;
    let table = &range.table;
    let update = &range.update;
    if let Some(selection) = &stmt.selection {
        crate::engine::expr::validate_expr_temporal_literals(selection, table)?;
        crate::engine::expr::validate_dml_temporal_functions(selection)?;
    }
    let Some(candidates) = candidate_row_ids_for_write(db, txn, table, stmt.selection.as_ref())?
    else {
        return execute_update_borrowed_simple(db, session, txn, stmt)?.ok_or_else(|| {
            crate::error::catalog::ER_UNKNOWN_ERROR.error("invalid prepared range UPDATE binding")
        });
    };
    let apply = |values: &mut [Value]| -> Result<(), QueryError> {
        for assignment in update.assignments.iter() {
            values[assignment.column_index] = assignment.eval(table, values)?;
        }
        apply_on_update_timestamps(table, values, &update.on_update_columns);
        Ok(())
    };
    let mut matched = 0u64;
    let changed = if candidates.len() > 64 {
        db.update_rows_with_changed_columns_noop(
            txn,
            &table.qualified_name(),
            Some(candidates),
            Some(&update.changed_columns),
            Some(&update.user_changed_columns),
            |values| range.predicate.eval_bool(table, values),
            |values| {
                matched += 1;
                let mut values = values.to_vec();
                apply(&mut values)?;
                Ok(values)
            },
        )? as u64
    } else {
        let mut changed = 0u64;
        for row_id in candidates {
            crate::engine::cancel::check_cancelled()?;
            let row_matched = std::cell::Cell::new(false);
            let mut predicate = |values: &[Value]| {
                let matches = range.predicate.eval_bool(table, values)?;
                row_matched.set(matches);
                Ok(matches)
            };
            let result = db.try_update_prepared_row_by_id_in_place_delta_noop_if(
                txn,
                table,
                row_id,
                Some(&update.changed_columns),
                Some(&update.user_changed_columns),
                &update.affected_indexes,
                &update.required_columns,
                update.decode_recipe.as_deref().map(|(plan, _)| plan),
                Some(&mut predicate),
                &apply,
            )?;
            let updated = match result {
                Some(updated) => updated,
                None => db.update_prepared_row_by_id_with_changed_columns_noop(
                    txn,
                    table,
                    row_id,
                    Some(&update.changed_columns),
                    Some(&update.user_changed_columns),
                    &update.affected_indexes,
                    |values| {
                        let mut new_values = values.to_vec();
                        if predicate(values)? {
                            apply(&mut new_values)?;
                        }
                        Ok(new_values)
                    },
                )?,
            };
            matched += u64::from(row_matched.get());
            changed += u64::from(updated);
        }
        changed
    };
    crate::engine::api::set_statement_matched_rows(matched);
    Ok(QueryResponse::OkAffected {
        affected_rows: changed,
    })
}

/// Prepared exact-unique UPDATE fast path. All schema-dependent routing and
/// assignment-column resolution was validated when the prepared memo was
/// compiled; execution only binds the current key/values and enters storage.
pub(super) fn execute_prepared_point_update(
    db: &Arc<Database>,
    session: &mut SessionContext,
    txn: TxnView,
    stmt: &Update,
    point: &crate::engine::api::PreparedPointWrite,
) -> Result<QueryResponse, QueryError> {
    crate::engine::cancel::check_cancelled()?;
    check_safe_updates(session, stmt.selection.as_ref(), stmt.limit.as_ref())?;
    let crate::engine::api::PreparedPointWriteKind::Update(update) = &point.kind else {
        return Err(crate::error::catalog::ER_UNKNOWN_ERROR
            .error("internal error: UPDATE received non-update prepared point plan"));
    };
    if let Some(selection) = &stmt.selection {
        crate::engine::expr::validate_expr_temporal_literals(selection, &point.table)?;
        crate::engine::expr::validate_dml_temporal_functions(selection)?;
    }
    let row_id = db.lookup_unique_index_for_write_one(
        txn,
        &point.table,
        &point.index,
        point.key.resolve_params()?,
    )?;
    let Some(row_id) = row_id else {
        crate::engine::api::set_statement_matched_rows(0);
        return Ok(QueryResponse::OkAffected { affected_rows: 0 });
    };
    let update_result = match db.try_update_prepared_row_by_id_in_place_delta_noop(
        txn,
        &point.table,
        row_id,
        Some(&update.changed_columns),
        Some(&update.user_changed_columns),
        &update.affected_indexes,
        &update.required_columns,
        update.decode_recipe.as_deref().map(|(plan, _)| plan),
        |values| {
            for assignment in update.assignments.iter() {
                let value = assignment.eval(&point.table, values)?;
                values[assignment.column_index] = value;
            }
            apply_on_update_timestamps(&point.table, values, &update.on_update_columns);
            Ok(())
        },
    ) {
        Ok(Some(updated)) => Ok(updated),
        Ok(None) => db.update_prepared_row_by_id_with_changed_columns_noop(
            txn,
            &point.table,
            row_id,
            Some(&update.changed_columns),
            Some(&update.user_changed_columns),
            &update.affected_indexes,
            |values| {
                let mut new_values = values.to_vec();
                for assignment in update.assignments.iter() {
                    let value = assignment.eval(&point.table, &new_values)?;
                    new_values[assignment.column_index] = value;
                }
                apply_on_update_timestamps(
                    &point.table,
                    &mut new_values,
                    &update.on_update_columns,
                );
                Ok(new_values)
            },
        ),
        Err(err) => Err(err),
    };
    let changed = match update_result {
        Ok(updated) => updated as u64,
        Err(err) if stmt.ignore && crate::storage::ignorable_update_error(&err) => {
            crate::engine::api::push_statement_condition(
                crate::error::ConditionLevel::Warning,
                &err,
            );
            0
        }
        Err(err) => return Err(err),
    };
    crate::engine::api::set_statement_matched_rows(1);
    Ok(QueryResponse::OkAffected {
        affected_rows: changed,
    })
}

/// Single source of truth for `ON UPDATE CURRENT_TIMESTAMP` on an UPDATE:
/// the set of columns carrying an on-update expression that were NOT among
/// the user-set columns (`changed`). These are appended to the write set and
/// refreshed on every changed row. Shared by single-table and multi-table
/// UPDATE so the three paths cannot drift (B9).
pub(super) fn on_update_timestamp_columns(table: &TableSchema, changed: &[usize]) -> Vec<usize> {
    table
        .columns
        .iter()
        .enumerate()
        .filter(|(idx, col)| col.on_update_expr.is_some() && !changed.contains(idx))
        .map(|(idx, _)| idx)
        .collect()
}

/// Apply `ON UPDATE CURRENT_TIMESTAMP` to `new_values` for the given
/// columns. Companion to [`on_update_timestamp_columns`]. Only rows that
/// actually change should be passed here (the caller's no-op detection or
/// changed-column tracking decides that).
pub(super) fn apply_on_update_timestamps(
    table: &TableSchema,
    new_values: &mut [Value],
    on_update_columns: &[usize],
) {
    for &idx in on_update_columns {
        let column = &table.columns[idx];
        if let Some(ref expr_str) = column.on_update_expr {
            if expr_str
                .to_ascii_uppercase()
                .starts_with("CURRENT_TIMESTAMP")
            {
                let fsp = match &column.data_type {
                    crate::storage::schema::DataTypeKind::DateTime { fsp }
                    | crate::storage::schema::DataTypeKind::Timestamp { fsp } => *fsp,
                    _ => 0,
                };
                if let Some(slot) = new_values.get_mut(idx) {
                    *slot = match &column.data_type {
                        crate::storage::schema::DataTypeKind::Timestamp { .. } => Value::Timestamp(
                            crate::engine::expr::session_utc_now()
                                .and_utc()
                                .timestamp_micros(),
                        ),
                        _ => {
                            let now = crate::engine::expr::session_now();
                            let formatted = if fsp > 0 {
                                now.format("%Y-%m-%d %H:%M:%S%.6f").to_string()
                            } else {
                                now.format("%Y-%m-%d %H:%M:%S").to_string()
                            };
                            Value::String(formatted)
                        }
                    };
                }
            }
        }
    }
}

pub(super) fn update_expr_contains_default(expr: &Expr) -> bool {
    crate::parser::visit::try_inspect_value_expr(expr, |node| {
        if matches!(node, Expr::Default(_) | Expr::DefaultQualified(_)) {
            return Err(());
        }
        // UPDATE defaults do not bind across subquery expressions.
        Ok(!matches!(node, Expr::InSubquery { .. }))
    })
    .is_err()
}

pub(super) fn changed_columns_after_trigger(
    planned_changed_columns: &[usize],
    old_values: &[Value],
    new_values: &[Value],
) -> Vec<usize> {
    let mut changed_columns = planned_changed_columns.to_vec();
    for (idx, (old_value, new_value)) in old_values.iter().zip(new_values.iter()).enumerate() {
        if old_value != new_value && !changed_columns.contains(&idx) {
            changed_columns.push(idx);
        }
    }
    changed_columns
}

fn update_expr_is_generated_default(expr: &Expr, column: &str) -> bool {
    match expr {
        Expr::Default(None) => true,
        Expr::Default(Some(name)) => name.eq_ignore_ascii_case(column),
        _ => false,
    }
}

pub(super) fn execute_delete(
    db: &Arc<Database>,
    session: &mut SessionContext,
    txn: TxnView,
    mut stmt: Delete,
    registry: &SessionRegistry,
) -> Result<QueryResponse, QueryError> {
    let _dml_expression_guard = crate::engine::expr::DmlExpressionGuard::new();
    crate::engine::cancel::check_cancelled()?;
    // `sql_safe_updates` guard — same MySQL semantics as UPDATE.
    check_safe_updates(session, stmt.selection.as_ref(), stmt.limit.as_ref())?;
    if super::multi_table::is_multi_table_delete(&stmt) {
        return super::multi_table::execute_multi_delete(db, session, txn, stmt, registry);
    }
    if !stmt.partitions.is_empty() {
        let storage_ids =
            super::partition_clause::resolve_partition_clause(db, &stmt.table, &stmt.partitions)?
                .unwrap_or_default();
        let partitions = std::mem::take(&mut stmt.partitions);
        let result = super::partition_router::with_pruned_partitions(storage_ids, || {
            execute_delete(db, session, txn, stmt.clone(), registry)
        });
        let _ = partitions;
        return result;
    }

    let outer_alias = stmt.primary_alias.clone();
    let outer_tname = stmt
        .table
        .rsplit('.')
        .next()
        .unwrap_or(stmt.table.as_str())
        .to_string();
    let table = db.table(&stmt.table)?.clone();
    let selection_is_correlated = stmt.selection.as_ref().is_some_and(|selection| {
        expr_has_correlated_subquery(
            db,
            selection,
            outer_alias.as_deref(),
            &outer_tname,
            &table.columns,
        )
    });
    if !selection_is_correlated {
        if let Some(selection) = &mut stmt.selection {
            materialize_subqueries(db, txn, selection)?;
        }
    }
    if let Some(selection) = &stmt.selection {
        crate::engine::expr::validate_expr_temporal_literals(selection, &table)?;
        crate::engine::expr::validate_dml_temporal_functions(selection)?;
    }
    let ordered_limited_candidates = ordered_limited_dml_candidates(
        db,
        txn,
        &table,
        (!selection_is_correlated)
            .then_some(stmt.selection.as_ref())
            .flatten(),
        &stmt.order_by,
        stmt.limit.as_ref(),
        false,
    )?;
    let has_delete_triggers = table_has_delete_triggers(db, &stmt.table);
    if let Some(candidate_set) = ordered_limited_candidates {
        return execute_delete_candidates(
            db,
            session,
            txn,
            &stmt,
            &table,
            registry,
            candidate_set,
            has_delete_triggers,
            selection_is_correlated,
            outer_alias.as_deref(),
            &outer_tname,
        );
    }
    let scan_selection = (!selection_is_correlated)
        .then_some(stmt.selection.as_ref())
        .flatten();
    let exact_candidates = if !has_delete_triggers && stmt.returning.is_none() {
        unique_row_ids_for_exact_delete(db, txn, &table, scan_selection)?
    } else {
        None
    };
    let origin_candidates = if table.is_partitioned() {
        candidate_row_refs_for_write(db, txn, &table, scan_selection)?
    } else {
        let candidates = match exact_candidates {
            Some(ids) => Some(ids),
            None => candidate_row_ids_for_write(db, txn, &table, scan_selection)?,
        };
        candidates.map(|rows| {
            rows.into_iter()
                .map(|row_id| crate::storage::DmlRowRef {
                    storage_id: table.id,
                    row_id,
                })
                .collect()
        })
    };
    if has_delete_triggers || stmt.returning.is_some() || stmt.ignore {
        let rows = db.dml_candidate_rows_with_origin_for_write(&table, txn, origin_candidates)?;
        let candidate_set = DmlCandidateSet {
            rows: rows.into_iter().map(|r| r.row_ref).collect(),
            limit: None,
        };
        return execute_delete_candidates(
            db,
            session,
            txn,
            &stmt,
            &table,
            registry,
            candidate_set,
            has_delete_triggers,
            selection_is_correlated,
            outer_alias.as_deref(),
            &outer_tname,
        );
    }
    // Bind column slots and constant IN sets once per statement. The legacy
    // fallback preserves unsupported expressions/collations; correlated
    // subqueries still resolve against each current row below. Storage invokes
    // this same predicate again after locking, so compilation never removes
    // the current-read recheck.
    let compiled_selection = (!selection_is_correlated)
        .then(|| {
            stmt.selection.as_ref().map(|expr| {
                crate::engine::expr_program::CompiledTableExpr::compile_once(expr, &table)
            })
        })
        .flatten();
    let affected_rows =
        db.delete_rows_with_origin(txn, &stmt.table, origin_candidates, |values| {
            crate::engine::cancel::check_cancelled()?;
            if let Some(compiled) = &compiled_selection {
                return compiled.eval_bool(&table, values);
            }
            match &stmt.selection {
                Some(expr) => eval_delete_selection(
                    db,
                    txn,
                    expr,
                    selection_is_correlated,
                    outer_alias.as_deref(),
                    &outer_tname,
                    &table,
                    values,
                ),
                None => Ok(true),
            }
        })? as u64;
    Ok(QueryResponse::OkAffected { affected_rows })
}

#[allow(clippy::too_many_arguments)]
fn eval_delete_selection(
    db: &Arc<Database>,
    txn: TxnView,
    expr: &Expr,
    correlated: bool,
    outer_alias: Option<&str>,
    outer_table_name: &str,
    table: &TableSchema,
    values: &[Value],
) -> Result<bool, QueryError> {
    if !correlated {
        return eval_bool(expr, table, values);
    }
    let outer = UpdateOuterRow {
        alias: outer_alias,
        table_name: outer_table_name,
        table,
        values,
    };
    let resolved = resolve_correlated_expr(db, txn, expr, &outer)?;
    eval_bool(&resolved, table, values)
}

pub(super) fn execute_delete_borrowed_simple(
    db: &Arc<Database>,
    session: &mut SessionContext,
    txn: TxnView,
    stmt: &Delete,
) -> Result<Option<QueryResponse>, QueryError> {
    let _dml_expression_guard = crate::engine::expr::DmlExpressionGuard::new();
    crate::engine::cancel::check_cancelled()?;
    check_safe_updates(session, stmt.selection.as_ref(), stmt.limit.as_ref())?;
    if !stmt.partitions.is_empty()
        || stmt.returning.is_some()
        || super::multi_table::is_multi_table_delete(stmt)
        || stmt
            .selection
            .as_ref()
            .is_some_and(|expr| expr_has_subquery(expr))
        || table_has_delete_triggers(db, &stmt.table)
    {
        return Ok(None);
    }
    let table = db.table(&stmt.table)?.clone();
    if let Some(selection) = &stmt.selection {
        crate::engine::expr::validate_expr_temporal_literals(selection, &table)?;
        crate::engine::expr::validate_dml_temporal_functions(selection)?;
    }
    let ordered_limited_candidates = ordered_limited_dml_candidates(
        db,
        txn,
        &table,
        stmt.selection.as_ref(),
        &stmt.order_by,
        stmt.limit.as_ref(),
        false,
    )?;
    if let Some(candidate_set) = ordered_limited_candidates {
        let mut affected_rows = 0u64;
        for row_ref in candidate_set.rows {
            if affected_rows as usize >= candidate_set.limit.unwrap_or(usize::MAX) {
                break;
            }
            crate::engine::cancel::check_cancelled()?;
            let Some(row) = db.dml_candidate_row_at_for_write(&table, row_ref, txn)? else {
                continue;
            };
            if let Some(expr) = &stmt.selection {
                if !eval_bool(expr, &table, &row.values)? {
                    continue;
                }
            }
            match db.delete_row_by_id_at_if(
                txn,
                &table,
                row_ref.storage_id,
                row_ref.row_id,
                |values| match &stmt.selection {
                    Some(expr) => eval_bool(expr, &table, values),
                    None => Ok(true),
                },
            ) {
                Ok(true) => affected_rows += 1,
                Ok(false) => {}
                Err(err) if stmt.ignore && is_ignorable_delete_error(&err) => {
                    crate::engine::api::push_statement_condition(
                        crate::error::ConditionLevel::Warning,
                        &err,
                    );
                }
                Err(err) => return Err(err),
            }
        }
        return Ok(Some(QueryResponse::OkAffected { affected_rows }));
    }
    let candidates = candidate_row_refs_for_write(db, txn, &table, stmt.selection.as_ref())?;
    if stmt.ignore {
        let rows = db.dml_candidate_rows_with_origin_for_write(&table, txn, candidates.clone())?;
        let mut affected_rows = 0u64;
        for row in rows {
            crate::engine::cancel::check_cancelled()?;
            let matches = match &stmt.selection {
                Some(expr) => eval_bool(expr, &table, &row.values)?,
                None => true,
            };
            if !matches {
                continue;
            }
            match db.delete_row_by_id_at_if(
                txn,
                &table,
                row.row_ref.storage_id,
                row.row_ref.row_id,
                |values| match &stmt.selection {
                    Some(expr) => eval_bool(expr, &table, values),
                    None => Ok(true),
                },
            ) {
                Ok(true) => affected_rows += 1,
                Ok(false) => {}
                Err(err) if is_ignorable_delete_error(&err) => {
                    crate::engine::api::push_statement_condition(
                        crate::error::ConditionLevel::Warning,
                        &err,
                    );
                }
                Err(err) => return Err(err),
            }
        }
        return Ok(Some(QueryResponse::OkAffected { affected_rows }));
    }
    let affected_rows = db.delete_rows_with_origin(txn, &stmt.table, candidates, |values| {
        crate::engine::cancel::check_cancelled()?;
        match &stmt.selection {
            Some(expr) => eval_bool(expr, &table, values),
            None => Ok(true),
        }
    })? as u64;
    Ok(Some(QueryResponse::OkAffected { affected_rows }))
}

/// Prepared exact-unique DELETE counterpart to
/// [`execute_prepared_point_update`]. The predicate is rechecked after the
/// storage lock wait so current-read semantics remain identical to the
/// general DELETE path.
pub(super) fn execute_prepared_point_delete(
    db: &Arc<Database>,
    session: &mut SessionContext,
    txn: TxnView,
    stmt: &Delete,
    point: &crate::engine::api::PreparedPointWrite,
) -> Result<QueryResponse, QueryError> {
    let _dml_expression_guard = crate::engine::expr::DmlExpressionGuard::new();
    crate::engine::cancel::check_cancelled()?;
    check_safe_updates(session, stmt.selection.as_ref(), stmt.limit.as_ref())?;
    if !matches!(
        point.kind,
        crate::engine::api::PreparedPointWriteKind::Delete
    ) {
        return Err(crate::error::catalog::ER_UNKNOWN_ERROR
            .error("internal error: DELETE received non-delete prepared point plan"));
    }
    if let Some(selection) = &stmt.selection {
        crate::engine::expr::validate_expr_temporal_literals(selection, &point.table)?;
        crate::engine::expr::validate_dml_temporal_functions(selection)?;
    }
    let resolved_key = point.key.resolve_params()?;
    // DELETE is not a useful cache consumer in the common OLTP shape: its
    // preceding selector is usually a range/oldest-row read, not an exact
    // FOR UPDATE on the same unique key. Keep its replacement-row loop on the
    // authoritative B-tree path instead of paying a guaranteed cache miss.
    let Some(mut row_id) = db.lookup_unique_index_for_write_one(
        txn,
        &point.table,
        &point.index,
        resolved_key.clone(),
    )?
    else {
        return Ok(QueryResponse::OkAffected { affected_rows: 0 });
    };
    loop {
        let deleted =
            match db.delete_row_by_id_at_if(txn, &point.table, point.table.id, row_id, |values| {
                match &stmt.selection {
                    Some(expr) => eval_bool(expr, &point.table, values),
                    None => Ok(true),
                }
            }) {
                Ok(deleted) => deleted,
                Err(err) if stmt.ignore && is_ignorable_delete_error(&err) => {
                    crate::engine::api::push_statement_condition(
                        crate::error::ConditionLevel::Warning,
                        &err,
                    );
                    false
                }
                Err(err) => return Err(err),
            };
        if deleted {
            return Ok(QueryResponse::OkAffected { affected_rows: 1 });
        }

        // The candidate can become a tombstone while we wait for its row
        // lock if another transaction commits DELETE+INSERT of the same
        // unique key. Re-probe current index state and follow the replacement
        // RowId. If the key still resolves to the same row, the predicate did
        // not match; retrying it would spin forever.
        let Some(next_row_id) = db.lookup_unique_index_for_write_one(
            txn,
            &point.table,
            &point.index,
            resolved_key.clone(),
        )?
        else {
            return Ok(QueryResponse::OkAffected { affected_rows: 0 });
        };
        if next_row_id == row_id {
            return Ok(QueryResponse::OkAffected { affected_rows: 0 });
        }
        row_id = next_row_id;
    }
}

/// Error codes that DELETE IGNORE swallows on a per-row basis. Matches MySQL's
/// IGNORE behaviour for foreign-key class failures.
pub(super) fn is_ignorable_delete_error(err: &QueryError) -> bool {
    err.is_any(&[
        crate::error::catalog::ER_NO_REFERENCED_ROW,
        crate::error::catalog::ER_ROW_IS_REFERENCED,
        crate::error::catalog::ER_ROW_IS_REFERENCED_2,
        crate::error::catalog::ER_NO_REFERENCED_ROW_2,
    ])
}

pub(super) fn ordered_limited_dml_candidates(
    db: &Arc<Database>,
    txn: TxnView,
    table: &TableSchema,
    selection: Option<&Expr>,
    order_by: &[OrderBy],
    limit: Option<&LimitValue>,
    limit_counts_matches: bool,
) -> Result<Option<DmlCandidateSet>, QueryError> {
    if order_by.is_empty() && limit.is_none() {
        return Ok(None);
    }
    let _partition_scope = super::partition_router::push_filter_partition_scope(table, selection);
    let limit = optional_limit_value(limit)?;
    let retain_refill_candidates = db.has_active_write_txns();

    // Binding/evaluation errors in ORDER BY are statement errors even when
    // LIMIT is zero. Evaluate against a schema-shaped NULL row before any
    // early return so unknown columns and unsupported expressions cannot be
    // hidden by an empty window.
    if !order_by.is_empty() {
        let null_row = vec![Value::Null; table.columns.len()];
        for item in order_by {
            eval_expr(&item.expr, table, &null_row)?;
        }
    }
    if limit == Some(0) {
        return Ok(Some(DmlCandidateSet {
            rows: Vec::new(),
            limit,
        }));
    }
    if limit_counts_matches && !retain_refill_candidates {
        let index_candidates = if table.is_partitioned() {
            ordered_partition_index_limited_dml_candidates(
                db, txn, table, selection, order_by, limit,
            )?
        } else {
            ordered_index_limited_dml_candidates(db, txn, table, selection, order_by, limit, None)?
        };
        if let Some(candidates) = index_candidates {
            return Ok(Some(DmlCandidateSet {
                rows: candidates,
                limit,
            }));
        }
    }
    // Preserve the physical origin for every index candidate. RowId alone
    // is not globally unique across a partitioned table.
    let base_candidates = candidate_row_refs_for_write(db, txn, table, selection)?;
    if order_by.is_empty() && limit_counts_matches && !retain_refill_candidates {
        if let Some(limit) = limit {
            let mut matched_refs = Vec::with_capacity(limit.min(1024));
            let mut accept = |row: crate::storage::DmlStoredRow| -> Result<bool, QueryError> {
                let matches = match selection {
                    Some(expr) => eval_bool(expr, table, &row.values)?,
                    None => true,
                };
                if matches {
                    matched_refs.push(row.row_ref);
                }
                Ok(matched_refs.len() >= limit)
            };
            match &base_candidates {
                Some(candidates) => {
                    for &row_ref in candidates {
                        if let Some(row) = db.dml_candidate_row_at_for_write(table, row_ref, txn)? {
                            if accept(row)? {
                                break;
                            }
                        }
                    }
                }
                None => {
                    db.dml_candidate_rows_with_origin_streaming_until(table, txn, &mut accept)?
                }
            }
            return Ok(Some(DmlCandidateSet {
                rows: matched_refs,
                limit: Some(limit),
            }));
        }
    }
    let mut rows = db.dml_candidate_rows_with_origin_for_write(table, txn, base_candidates)?;

    // M3.B5 — When a positive LIMIT is given alongside ORDER BY, run
    // a TopN heap pass instead of materialising every candidate and
    // sorting. The candidate set itself still lives in `rows` (the
    // storage layer hasn't grown a streaming row reader yet), but the
    // heap holds only sort keys + row_id for the top K — far cheaper
    // than the full StoredRow when the row width is large — and the
    // CPU cost drops from O(N log N) to O(N log K). For wide LIMITs or
    // LIMIT-less queries we keep the original sort path.
    const DML_IN_MEMORY_SORT_ROWS: usize = 4096;
    if limit_counts_matches && !retain_refill_candidates {
        if let (Some(limit), false) = (limit, order_by.is_empty()) {
            if limit > 0 && limit <= DML_IN_MEMORY_SORT_ROWS {
                return Ok(Some(DmlCandidateSet {
                    rows: topn_dml_candidates(table, selection, order_by, limit, &mut rows)?,
                    limit: Some(limit),
                }));
            }
        }
    }

    let fields: Vec<_> = order_by
        .iter()
        .map(|item| {
            let mut field = crate::engine::executor::sort::SortField::from_order_by(item);
            field.collation_id = crate::engine::expr::expr_collation(&item.expr, &table)
                .map(|info| info.collation_id);
            field
        })
        .collect();
    let mut matched = Vec::new();
    for row in rows.drain(..) {
        let matches = match selection {
            Some(expr) => eval_bool(expr, table, &row.values)?,
            None => true,
        };
        if !matches {
            continue;
        }
        let mut keys = Vec::with_capacity(order_by.len());
        for item in order_by {
            keys.push(eval_expr(&item.expr, table, &row.values)?);
        }
        matched.push(RankedDmlRow {
            row_ref: row.row_ref,
            keys,
        });
        // LIMIT without ORDER BY is naturally streaming: once UPDATE has
        // matched K rows no later row can enter its window.
        if order_by.is_empty()
            && limit_counts_matches
            && !retain_refill_candidates
            && matched.len() == limit.unwrap_or(usize::MAX)
        {
            break;
        }
    }
    let mut matched_refs = if !order_by.is_empty() && matched.len() > DML_IN_MEMORY_SORT_ROWS {
        super::select::external_sort::sort_precomputed_dml_refs(
            db.data_dir(),
            matched
                .into_iter()
                .map(|row| (row.keys, row.row_ref))
                .collect(),
            &fields,
        )?
    } else {
        if !order_by.is_empty() {
            matched.sort_by(|left, right| compare_ranked_dml_rows(left, right, &fields));
        }
        matched.into_iter().map(|row| row.row_ref).collect()
    };
    if limit_counts_matches && !retain_refill_candidates {
        if let Some(limit) = limit {
            matched_refs.truncate(limit);
        }
    }
    Ok(Some(DmlCandidateSet {
        rows: matched_refs,
        limit,
    }))
}

#[derive(Debug)]
pub(super) struct DmlCandidateSet {
    pub rows: Vec<crate::storage::DmlRowRef>,
    pub limit: Option<usize>,
}

#[derive(Debug, Clone)]
struct RankedDmlRow {
    row_ref: crate::storage::DmlRowRef,
    keys: Vec<Value>,
}

fn compare_ranked_dml_rows(
    left: &RankedDmlRow,
    right: &RankedDmlRow,
    fields: &[crate::engine::executor::sort::SortField],
) -> Ordering {
    for ((left, right), field) in left
        .keys
        .iter()
        .zip(right.keys.iter())
        .zip(fields.iter().copied())
    {
        let ordering = crate::engine::executor::sort::compare_value_for_sort(left, right, field);
        if ordering != Ordering::Equal {
            return ordering;
        }
    }
    left.row_ref.cmp(&right.row_ref)
}

/// M3.B5 — TopN heap-only DML candidate selection.
///
/// Walks every candidate row, evaluates WHERE, and keeps the K rows
/// whose ORDER BY tuple is smallest (using a max-heap of size K).
/// Returns the row_ids in ascending ORDER BY order — exactly what
/// the previous sort+truncate path produced.
fn topn_dml_candidates(
    table: &TableSchema,
    selection: Option<&Expr>,
    order_by: &[OrderBy],
    limit: usize,
    rows: &mut Vec<crate::storage::DmlStoredRow>,
) -> Result<Vec<crate::storage::DmlRowRef>, QueryError> {
    use std::collections::BinaryHeap;
    let fields: std::sync::Arc<[crate::engine::executor::sort::SortField]> = order_by
        .iter()
        .map(|item| {
            let mut field = crate::engine::executor::sort::SortField::from_order_by(item);
            field.collation_id = crate::engine::expr::expr_collation(&item.expr, &table)
                .map(|info| info.collation_id);
            field
        })
        .collect::<Vec<_>>()
        .into();
    // Never reserve from a user-controlled LIMIT. The heap grows only as
    // real candidates arrive, preventing usize::MAX from panicking.
    let mut heap: BinaryHeap<TopnEntry> =
        BinaryHeap::with_capacity(limit.min(rows.len()).min(1024));
    for row in rows.drain(..) {
        if let Some(expr) = selection {
            if !eval_bool(expr, table, &row.values)? {
                continue;
            }
        }
        let mut keys = Vec::with_capacity(order_by.len());
        for item in order_by {
            keys.push(eval_expr(&item.expr, table, &row.values)?);
        }
        let entry = TopnEntry {
            keys,
            row_ref: row.row_ref,
            fields: fields.clone(),
        };
        if heap.len() < limit {
            heap.push(entry);
        } else if let Some(top) = heap.peek() {
            // `top` is the current max — if the new row is smaller in
            // ORDER BY order, evict the max and push the new row.
            if entry.cmp(top) == Ordering::Less {
                heap.pop();
                heap.push(entry);
            }
        }
    }
    // `into_sorted_vec` returns the heap entries in ascending order
    // (by `Ord`), which under the comparator below means ascending
    // ORDER BY — exactly the DML candidate order the caller expects.
    Ok(heap
        .into_sorted_vec()
        .into_iter()
        .map(|entry| entry.row_ref)
        .collect())
}

/// Heap entry for `topn_dml_candidates`. `keys` are the pre-computed
/// ORDER BY expression values for this row; `row_id` is the heap
/// row identifier we hand back to the UPDATE/DELETE executor. The
/// `order_by` clone carries ASC/DESC flags so the `Ord` impl can
/// honour them without holding a `TableSchema` reference (which
/// would tie the heap to a borrow scope it can't outlive).
#[derive(Debug, Clone)]
struct TopnEntry {
    keys: Vec<Value>,
    row_ref: crate::storage::DmlRowRef,
    fields: std::sync::Arc<[crate::engine::executor::sort::SortField]>,
}

impl Ord for TopnEntry {
    fn cmp(&self, other: &Self) -> Ordering {
        // Compare key-by-key in ORDER BY order, honouring ASC/DESC and
        // explicit NULLS FIRST/LAST. Equal tuples tie-break on row_id
        // so the heap order is total and `into_sorted_vec` is
        // deterministic.
        for (idx, (left, right)) in self.keys.iter().zip(other.keys.iter()).enumerate() {
            let field = self.fields[idx];
            let ord = crate::engine::executor::sort::compare_value_for_sort(left, right, field);
            if ord != Ordering::Equal {
                return ord;
            }
        }
        self.row_ref.cmp(&other.row_ref)
    }
}

impl PartialOrd for TopnEntry {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl PartialEq for TopnEntry {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == Ordering::Equal
    }
}

impl Eq for TopnEntry {}

fn ordered_partition_index_limited_dml_candidates(
    db: &Arc<Database>,
    txn: TxnView,
    table: &TableSchema,
    selection: Option<&Expr>,
    order_by: &[OrderBy],
    limit: Option<usize>,
) -> Result<Option<Vec<crate::storage::DmlRowRef>>, QueryError> {
    if !table.is_partitioned() || db.has_active_write_txns() {
        return Ok(None);
    }
    let mut candidates = Vec::new();
    for storage_id in super::partition_router::in_scope_storage_ids(table) {
        let _scope = super::partition_router::push_pruned_partitions(vec![storage_id]);
        let Some(mut local) = ordered_index_limited_dml_candidates(
            db,
            txn,
            table,
            selection,
            order_by,
            limit,
            Some(storage_id),
        )?
        else {
            return Ok(None);
        };
        candidates.append(&mut local);
    }

    let fields: Vec<_> = order_by
        .iter()
        .map(|item| {
            let mut field = crate::engine::executor::sort::SortField::from_order_by(item);
            field.collation_id = crate::engine::expr::expr_collation(&item.expr, &table)
                .map(|info| info.collation_id);
            field
        })
        .collect();
    let mut ranked = Vec::with_capacity(candidates.len());
    for row_ref in candidates {
        let Some(row) = db.dml_candidate_row_at_for_write(table, row_ref, txn)? else {
            continue;
        };
        let keys = order_by
            .iter()
            .map(|item| eval_expr(&item.expr, table, &row.values))
            .collect::<Result<Vec<_>, _>>()?;
        ranked.push(RankedDmlRow { row_ref, keys });
    }
    let mut merged = if ranked.len() > 4096 {
        super::select::external_sort::sort_precomputed_dml_refs(
            db.data_dir(),
            ranked
                .into_iter()
                .map(|row| (row.keys, row.row_ref))
                .collect(),
            &fields,
        )?
    } else {
        ranked.sort_by(|left, right| compare_ranked_dml_rows(left, right, &fields));
        ranked.into_iter().map(|row| row.row_ref).collect()
    };
    if let Some(limit) = limit {
        merged.truncate(limit);
    }
    Ok(Some(merged))
}

pub(super) fn plan_ordered_dml_index<'a>(
    table: &'a TableSchema,
    selection: Option<&Expr>,
    order_by: &[OrderBy],
) -> Option<(
    &'a IndexSchema,
    Option<IndexBound>,
    Option<IndexBound>,
    Option<Expr>,
)> {
    if order_by.is_empty() {
        return None;
    }
    let all_asc = order_by.iter().all(|item| item.asc);
    let all_desc = order_by.iter().all(|item| !item.asc);
    if (!all_asc && !all_desc) || order_by.iter().any(OrderBy::has_custom_nulls) {
        return None;
    };

    let access = if let Some(selection) = selection {
        let Some((index, access)) = best_index_access(table, selection) else {
            return None;
        };
        if !index_order_covers_order_by(table, index, selection, order_by) {
            return None;
        }
        let residual = index_access_residual_filter(table, index, &access, selection);
        let (low, high) = index_access_bounds(&access);
        (index, low, high, residual)
    } else {
        let Some(index) = table.iter_queryable_indexes().find(|index| {
            index.columns.len() >= order_by.len()
                && order_by.iter().enumerate().all(|(idx, item)| {
                    index.column_prefix_len(idx).is_none()
                        && expr_column_name(&item.expr)
                            .is_some_and(|column| index.columns[idx].eq_ignore_ascii_case(column))
                })
        }) else {
            return None;
        };
        (index, None, None, None)
    };

    Some(access)
}

pub(super) fn ordered_index_limited_dml_candidates(
    db: &Arc<Database>,
    txn: TxnView,
    table: &TableSchema,
    selection: Option<&Expr>,
    order_by: &[OrderBy],
    limit: Option<usize>,
    storage_id_override: Option<u64>,
) -> Result<Option<Vec<crate::storage::DmlRowRef>>, QueryError> {
    if order_by.is_empty()
        || (table.is_partitioned() && storage_id_override.is_none())
        || db.has_active_write_txns()
    {
        return Ok(None);
    }
    let Some((index, low, high, residual_filter)) =
        plan_ordered_dml_index(table, selection, order_by)
    else {
        return Ok(None);
    };
    let all_asc = order_by.iter().all(|item| item.asc);
    let all_desc = !all_asc;

    if limit == Some(0) {
        return Ok(Some(Vec::new()));
    }
    let max_rows = limit.unwrap_or(usize::MAX);
    let mut rows = Vec::new();
    let mut forward = all_asc
        .then(|| db.range_index_entries_cursor(table, index, low.clone(), high.clone()))
        .transpose()?;
    let mut reverse = all_desc
        .then(|| db.range_index_entries_reverse_cursor(table, index, low, high))
        .transpose()?;
    loop {
        let chunk = match (&mut forward, &mut reverse) {
            (Some(cursor), _) => cursor.next_chunk(64)?,
            (_, Some(cursor)) => cursor.next_chunk(64)?,
            _ => unreachable!("ordered DML index scan has one direction"),
        };
        if chunk.is_empty() {
            break;
        }
        for (_key, row_id) in chunk {
            let row_ref = crate::storage::DmlRowRef {
                storage_id: storage_id_override.unwrap_or(table.id),
                row_id,
            };
            let Some(row) = db.dml_candidate_row_at_for_write(table, row_ref, txn)? else {
                continue;
            };
            if let Some(filter) = residual_filter.as_ref() {
                if !eval_bool(filter, table, &row.values)? {
                    continue;
                }
            };
            rows.push(row.row_ref);
            if rows.len() >= max_rows {
                return Ok(Some(rows));
            }
        }
    }

    Ok(Some(rows))
}

pub(super) fn unique_row_id_for_exact_write(
    db: &Arc<Database>,
    txn: TxnView,
    table: &TableSchema,
    selection: Option<&Expr>,
) -> Result<Option<RowId>, QueryError> {
    if table.is_partitioned() {
        return Ok(None);
    }
    let Some(selection) = selection else {
        return Ok(None);
    };
    let Some((index, EngineIndexAccess::Lookup { key, exact: true })) =
        best_index_access(table, selection)
    else {
        return Ok(None);
    };
    if !index.unique
        || key.contains_null()
        || !index_access_covers_all_conjuncts(table, index, selection, &key)
    {
        return Ok(None);
    }
    let row_id = db.lookup_unique_index_for_write_one(txn, table, index, key)?;
    crate::engine::api::add_statement_dml_candidate_rows(u64::from(row_id.is_some()));
    Ok(row_id)
}

pub(super) fn unique_row_ids_for_exact_delete(
    db: &Arc<Database>,
    txn: TxnView,
    table: &TableSchema,
    selection: Option<&Expr>,
) -> Result<Option<Vec<RowId>>, QueryError> {
    if table.is_partitioned() {
        return Ok(None);
    }
    let Some(selection) = selection else {
        return Ok(None);
    };
    let Some((index, EngineIndexAccess::Lookup { key, exact: true })) =
        best_index_access(table, selection)
    else {
        return Ok(None);
    };
    if !index.unique
        || key.contains_null()
        || !index_access_covers_all_conjuncts(table, index, selection, &key)
    {
        return Ok(None);
    }
    let row_id = db.lookup_unique_index_for_write_one(txn, table, index, key)?;
    crate::engine::api::add_statement_dml_candidate_rows(u64::from(row_id.is_some()));
    Ok(row_id.map(|id| vec![id]))
}

// Private lexical name for outer columns captured from a stored view body.
// It cannot collide with a base-table alias introduced by a subquery.
pub(in crate::engine::executor) const VIEW_CHECK_ROW_ALIAS: &str = "\0mydb_view_check_row";

/// Materialize only row-independent subqueries once per statement. Correlated
/// checks are evaluated against each candidate's post-image instead.
pub(super) fn prepare_view_check(
    db: &Arc<Database>,
    txn: TxnView,
    predicate: &mut Expr,
    table: &TableSchema,
    alias: Option<&str>,
) -> Result<(), QueryError> {
    if super::prepared_dml::expr_contains_subquery(predicate)
        && !expr_has_correlated_subquery(db, predicate, alias, &table.name, &table.columns)
    {
        materialize_subqueries(db, txn, predicate)?;
    }
    Ok(())
}

pub(super) fn eval_view_check(
    db: &Arc<Database>,
    txn: TxnView,
    predicate: &Expr,
    table: &TableSchema,
    values: &[Value],
    alias: Option<&str>,
) -> Result<bool, QueryError> {
    if !super::prepared_dml::expr_contains_subquery(predicate) {
        return eval_bool(predicate, table, values);
    }
    let outer = UpdateOuterRow {
        alias,
        table_name: &table.name,
        table,
        values,
    };
    let resolved = resolve_correlated_expr(db, txn, predicate, &outer)?;
    eval_bool(&resolved, table, values)
}

pub(super) fn materialize_subqueries(
    db: &Arc<Database>,
    txn: TxnView,
    expr: &mut Expr,
) -> Result<(), QueryError> {
    match expr {
        Expr::Subquery(subquery) => {
            let value = execute_scalar_subquery(db, txn, subquery)?;
            *expr = literal_value_to_expr(value);
        }
        Expr::InSubquery {
            expr: inner,
            subquery,
            negated,
        } => {
            materialize_subqueries(db, txn, inner)?;
            let list = if let Expr::Tuple(items) = inner.as_ref() {
                let response = execute_expression_subquery(
                    db,
                    txn,
                    *subquery.clone(),
                    dml_subquery_registry(),
                )?
                .materialize()?;
                let QueryResponse::ResultSet(result) = response else {
                    return Err(error(
                        crate::error::catalog::ER_NOT_SUPPORTED_YET,
                        "IN subquery must return rows",
                    ));
                };
                if result.columns.len() != items.len() {
                    return Err(crate::engine::expr::operand_columns_error(items.len()));
                }
                result
                    .rows
                    .into_iter()
                    .map(|row| Expr::Tuple(row.into_iter().map(literal_value_to_expr).collect()))
                    .collect()
            } else {
                execute_in_subquery_values(db, txn, subquery)?
                    .into_iter()
                    .map(literal_value_to_expr)
                    .collect()
            };
            *expr = Expr::InList {
                expr: inner.clone(),
                list,
                negated: *negated,
            };
        }
        Expr::QuantifiedComparison { left, right, .. } => {
            materialize_subqueries(db, txn, left)?;
            match right {
                QuantifiedComparisonRight::Subquery(subquery) => {
                    let values = execute_quantified_subquery_values(db, txn, subquery)?;
                    *right = QuantifiedComparisonRight::List(
                        values.into_iter().map(literal_value_to_expr).collect(),
                    );
                }
                QuantifiedComparisonRight::List(list) => {
                    for item in list {
                        materialize_subqueries(db, txn, item)?;
                    }
                }
            }
        }
        Expr::Exists { subquery, negated } => {
            let exists = execute_exists_subquery(db, txn, subquery)?;
            let result = if *negated { !exists } else { exists };
            *expr = Expr::Literal(Literal::Int(result as i64));
        }
        Expr::Unary { expr: inner, .. } | Expr::IsNull(inner) | Expr::IsNotNull(inner) => {
            materialize_subqueries(db, txn, inner)?;
        }
        Expr::Binary { left, right, .. } => {
            materialize_subqueries(db, txn, left)?;
            materialize_subqueries(db, txn, right)?;
        }
        Expr::InList {
            expr: inner, list, ..
        } => {
            materialize_subqueries(db, txn, inner)?;
            for item in list {
                materialize_subqueries(db, txn, item)?;
            }
        }
        Expr::Between {
            expr: inner,
            low,
            high,
            ..
        } => {
            materialize_subqueries(db, txn, inner)?;
            materialize_subqueries(db, txn, low)?;
            materialize_subqueries(db, txn, high)?;
        }
        Expr::Function { args, .. } => {
            for arg in args {
                materialize_subqueries(db, txn, arg)?;
            }
        }
        Expr::GroupKeyRef { expr: inner, .. } => {
            materialize_subqueries(db, txn, inner)?;
        }
        Expr::Grouping(_) => {}
        Expr::Like {
            expr: inner,
            pattern,
            ..
        } => {
            materialize_subqueries(db, txn, inner)?;
            materialize_subqueries(db, txn, pattern)?;
        }
        Expr::Case {
            operand,
            conditions,
            results,
            else_result,
        } => {
            if let Some(operand) = operand {
                materialize_subqueries(db, txn, operand)?;
            }
            for cond in conditions {
                materialize_subqueries(db, txn, cond)?;
            }
            for result in results {
                materialize_subqueries(db, txn, result)?;
            }
            if let Some(else_result) = else_result {
                materialize_subqueries(db, txn, else_result)?;
            }
        }
        Expr::WindowFunction { args, spec, .. } => {
            let crate::parser::WindowSpec {
                partition_by,
                order_by,
                ..
            } = spec.as_mut();

            for arg in args {
                materialize_subqueries(db, txn, arg)?;
            }
            for expr in partition_by {
                materialize_subqueries(db, txn, expr)?;
            }
            for OrderBy { expr, .. } in order_by {
                materialize_subqueries(db, txn, expr)?;
            }
        }
        Expr::Tuple(items) => {
            for item in items {
                materialize_subqueries(db, txn, item)?;
            }
        }
        Expr::Wildcard
        | Expr::Identifier(_)
        | Expr::CompoundIdentifier(_)
        | Expr::Literal(_)
        | Expr::BoundValue(_)
        | Expr::Default(_)
        | Expr::DefaultQualified(_)
        | Expr::Placeholder(_)
        | Expr::UserVariable(_)
        | Expr::SystemVariable { .. } => {}
    }
    Ok(())
}

/// Outer-row binding for resolving a correlated subquery in an UPDATE SET
/// (or WHERE) expression: the target table's alias / base name plus the
/// current row's schema and values.
struct UpdateOuterRow<'a> {
    alias: Option<&'a str>,
    table_name: &'a str,
    table: &'a TableSchema,
    values: &'a [Value],
}

impl UpdateOuterRow<'_> {
    /// The current row's value for `column` as a bound expression, if it is
    /// an outer column. A string column keeps its collation (implicit
    /// derivation), so `t.s < 'ab'` on a `utf8mb4_bin` column stays binary.
    fn column_value(&self, column: &str) -> Option<Expr> {
        let idx = self
            .table
            .columns
            .iter()
            .position(|c| c.name.eq_ignore_ascii_case(column))?;
        let value = Expr::BoundValue(self.values.get(idx)?.clone());
        let collation = crate::engine::expr_program::resolve_column_collation_id(
            self.table,
            &self.table.columns[idx],
        )
        .filter(|id| *id != crate::charset::BINARY_COLLATION_ID)
        .and_then(crate::charset::resolve_collation_id);
        Some(match collation {
            Some(collation) => Expr::Function {
                name: "__COLLATED_CONST".into(),
                args: vec![
                    value,
                    Expr::Literal(crate::parser::Literal::String(collation.name.into())),
                    Expr::Literal(crate::parser::Literal::Int(2)),
                ],
                distinct: false,
            },
            None => value,
        })
    }
}

/// True when `qualifier` on a `CompoundIdentifier` names the outer table.
fn compound_qualifier_matches_outer(
    parts: &[String],
    alias: Option<&str>,
    table_name: &str,
) -> bool {
    if parts.len() < 2 {
        return false;
    }
    let qualifier = &parts[parts.len() - 2];
    qualifier == VIEW_CHECK_ROW_ALIAS
        || alias.is_some_and(|a| a.eq_ignore_ascii_case(qualifier))
        || table_name.eq_ignore_ascii_case(qualifier)
}

/// True when `expr` (recursing through child expressions and nested subquery
/// bodies) references the outer table's alias / name via a qualified column.
fn expr_refs_outer(expr: &Expr, alias: Option<&str>, table_name: &str) -> bool {
    if let Expr::CompoundIdentifier(parts) = expr {
        if compound_qualifier_matches_outer(parts, alias, table_name) {
            return true;
        }
    }
    let mut found = false;
    for_each_child_expr(expr, &mut |e| {
        found |= expr_refs_outer(e, alias, table_name)
    });
    for_each_immediate_select(expr, &mut |sel| {
        for_each_expr_in_select(sel, &mut |e| found |= expr_refs_outer(e, alias, table_name));
    });
    found
}

/// True when `expr` contains a subquery whose body references the outer
/// table's alias / name — i.e. it is a *correlated* subquery that must be
/// resolved per row rather than materialised once. The outer table's own
/// tables inside the subquery resolve by their own aliases, so a bare
/// outer-table qualifier there can only be an outer reference.
fn select_inner_columns(
    db: &Arc<Database>,
    sel: &Select,
) -> Option<std::collections::HashSet<String>> {
    if sel.derived.is_some() || sel.values.is_some() || sel.json_table.is_some() {
        return None;
    }
    let mut columns = std::collections::HashSet::new();
    for table_ref in sel
        .from
        .iter()
        .chain(sel.joins.iter().map(|join| &join.table))
    {
        let table = db.table(&table_ref.name).ok()?;
        columns.extend(
            table
                .columns
                .iter()
                .map(|column| column.name.to_ascii_lowercase()),
        );
    }
    Some(columns)
}

pub(super) fn expr_has_correlated_subquery(
    db: &Arc<Database>,
    expr: &Expr,
    alias: Option<&str>,
    table_name: &str,
    outer_columns: &[crate::storage::schema::ColumnSchema],
) -> bool {
    fn has_outer_unqualified_identifier(
        expr: &Expr,
        inner_columns: &std::collections::HashSet<String>,
        outer_columns: &[crate::storage::schema::ColumnSchema],
    ) -> bool {
        if let Expr::Identifier(name) = expr {
            return !inner_columns.contains(&name.to_ascii_lowercase())
                && outer_columns
                    .iter()
                    .any(|column| column.name.eq_ignore_ascii_case(name));
        }
        let mut found = false;
        for_each_child_expr(expr, &mut |child| {
            found |= has_outer_unqualified_identifier(child, inner_columns, outer_columns)
        });
        found
    }

    let mut correlated = false;
    for_each_immediate_select(expr, &mut |sel| {
        let inner_columns = select_inner_columns(db, sel);
        for_each_expr_in_select(sel, &mut |e| {
            correlated |= expr_refs_outer(e, alias, table_name)
                || inner_columns.as_ref().is_some_and(|columns| {
                    has_outer_unqualified_identifier(e, columns, outer_columns)
                });
        });
    });
    if correlated {
        return true;
    }
    for_each_child_expr(expr, &mut |e| {
        correlated |= expr_has_correlated_subquery(db, e, alias, table_name, outer_columns)
    });
    correlated
}

/// Replace, in place, every outer-qualified column reference (`alias.col` /
/// `table.col`) — at the top level *and* inside nested subqueries — with a
/// literal from the current row. After this, the subqueries are no longer
/// correlated and can be materialised with `materialize_subqueries`.
fn substitute_outer_in_expr(db: &Arc<Database>, expr: &mut Expr, outer: &UpdateOuterRow) {
    substitute_outer_in_expr_scoped(db, expr, outer, None);
}

fn substitute_outer_in_expr_scoped(
    db: &Arc<Database>,
    expr: &mut Expr,
    outer: &UpdateOuterRow,
    inner_columns: Option<&std::collections::HashSet<String>>,
) {
    if let Some(inner_columns) = inner_columns {
        if let Expr::Identifier(column) = expr {
            if !inner_columns.contains(&column.to_ascii_lowercase()) {
                if let Some(value) = outer.column_value(column) {
                    *expr = value;
                    return;
                }
            }
        }
    }
    // Substitute the node itself if it is an outer-qualified column ref.
    if let Expr::CompoundIdentifier(parts) = expr {
        if compound_qualifier_matches_outer(parts, outer.alias, outer.table_name) {
            let column = parts[parts.len() - 1].clone();
            if let Some(value) = outer.column_value(&column) {
                *expr = value;
                return;
            }
        }
    }
    // Recurse into child expressions and any nested subquery bodies.
    for_each_child_expr_mut(expr, &mut |child| {
        substitute_outer_in_expr_scoped(db, child, outer, inner_columns)
    });
    for_each_immediate_select_mut(expr, &mut |sel| substitute_outer_in_select(db, sel, outer));
}

fn substitute_outer_in_select(db: &Arc<Database>, sel: &mut Select, outer: &UpdateOuterRow) {
    let inner_columns = select_inner_columns(db, sel);
    let shadows = |name: &str| {
        sel.from
            .iter()
            .chain(sel.joins.iter().map(|join| &join.table))
            .any(|source| {
                source
                    .alias
                    .as_deref()
                    .unwrap_or_else(|| source.name.rsplit('.').next().unwrap_or(&source.name))
                    .eq_ignore_ascii_case(name)
            })
    };
    let scoped_outer = UpdateOuterRow {
        alias: outer.alias.filter(|name| !shadows(name)),
        table_name: if shadows(outer.table_name) {
            ""
        } else {
            outer.table_name
        },
        table: outer.table,
        values: outer.values,
    };
    for_each_expr_in_select_mut(sel, &mut |e| {
        substitute_outer_in_expr_scoped(db, e, &scoped_outer, inner_columns.as_ref())
    });
}

/// Apply `f` to each immediate operand expression of `expr` (not descending
/// into subquery Selects — see [`for_each_immediate_select`]).
fn for_each_child_expr(expr: &Expr, f: &mut dyn FnMut(&Expr)) {
    crate::parser::visit::for_each_expr_child(expr, f);
}

fn for_each_child_expr_mut(expr: &mut Expr, f: &mut dyn FnMut(&mut Expr)) {
    crate::parser::visit::for_each_expr_child_mut(expr, f);
}

/// Apply `f` to each Select that is an immediate subquery operand of `expr`.
fn for_each_immediate_select(expr: &Expr, f: &mut dyn FnMut(&Select)) {
    match expr {
        Expr::Subquery(sel) | Expr::Exists { subquery: sel, .. } => f(sel),
        Expr::InSubquery { subquery, .. } => f(subquery),
        Expr::QuantifiedComparison {
            right: QuantifiedComparisonRight::Subquery(sel),
            ..
        } => f(sel),
        _ => {}
    }
}

fn for_each_immediate_select_mut(expr: &mut Expr, f: &mut dyn FnMut(&mut Select)) {
    match expr {
        Expr::Subquery(sel) | Expr::Exists { subquery: sel, .. } => f(sel),
        Expr::InSubquery { subquery, .. } => f(subquery),
        Expr::QuantifiedComparison {
            right: QuantifiedComparisonRight::Subquery(sel),
            ..
        } => f(sel),
        _ => {}
    }
}

/// Apply `f` to each top-level expression carried by a Select body
/// (projection / WHERE / GROUP BY / HAVING / ORDER BY / JOIN conditions).
fn for_each_expr_in_select(sel: &Select, f: &mut dyn FnMut(&Expr)) {
    for item in &sel.projection {
        if let SelectItem::Expr { expr, .. } = item {
            f(expr);
        }
    }
    sel.selection.iter().for_each(|e| f(e));
    sel.having.iter().for_each(|e| f(e));
    sel.group_by.iter().for_each(|e| f(e));
    sel.order_by.iter().for_each(|o| f(&o.expr));
    for join in &sel.joins {
        join.condition.iter().for_each(|e| f(e));
    }
}

fn for_each_expr_in_select_mut(sel: &mut Select, f: &mut dyn FnMut(&mut Expr)) {
    for item in &mut sel.projection {
        if let SelectItem::Expr { expr, .. } = item {
            f(expr);
        }
    }
    sel.selection.iter_mut().for_each(|e| f(e));
    sel.having.iter_mut().for_each(|e| f(e));
    sel.group_by.iter_mut().for_each(|e| f(e));
    sel.order_by.iter_mut().for_each(|o| f(&mut o.expr));
    for join in &mut sel.joins {
        join.condition.iter_mut().for_each(|e| f(e));
    }
}

/// Resolve a SET/WHERE expression that carries a correlated subquery for the
/// current row: substitute the outer columns, then materialise the (now
/// uncorrelated) subqueries into literals so `eval_expr` can evaluate it.
fn resolve_correlated_expr(
    db: &Arc<Database>,
    txn: TxnView,
    expr: &Expr,
    outer: &UpdateOuterRow,
) -> Result<Expr, QueryError> {
    let mut resolved = expr.clone();
    substitute_outer_in_expr(db, &mut resolved, outer);
    materialize_subqueries(db, txn, &mut resolved)?;
    Ok(resolved)
}

fn execute_scalar_subquery(
    db: &Arc<Database>,
    txn: TxnView,
    subquery: &Select,
) -> Result<Value, QueryError> {
    // P1-1: the subquery may run on the streaming path; materialise
    // before destructuring so we can index `result.rows`.
    let response = execute_expression_subquery(db, txn, subquery.clone(), dml_subquery_registry())?
        .materialize()?;
    scalar_subquery_value(response)
}

/// Validate the materialized scalar result after qualification and locking.
fn scalar_subquery_value(response: QueryResponse) -> Result<Value, QueryError> {
    let QueryResponse::ResultSet(result) = response else {
        return Err(error(
            crate::error::catalog::ER_NOT_SUPPORTED_YET,
            "scalar subquery must return rows",
        ));
    };
    if result.columns.len() != 1 {
        return Err(error(
            crate::error::catalog::ER_OPERAND_COLUMNS,
            "Operand should contain 1 column(s)",
        ));
    }
    if result.rows.len() > 1 {
        return Err(error(
            crate::error::catalog::ER_SUBQUERY_NO_1_ROW,
            "Subquery returns more than 1 row",
        ));
    }
    Ok(result
        .rows
        .first()
        .and_then(|row| row.first())
        .cloned()
        .unwrap_or(Value::Null))
}

/// Session-aware scalar-subquery evaluation for contexts such as `SET`
/// assignments. This qualifies table names first, then enforces the same
/// single-column and single-row shape as DML evaluation.
/// Shared locking-read entry for expression subqueries. Use the current
/// transaction and refresh the read view after acquiring row locks.
pub(super) fn execute_expression_subquery(
    db: &Arc<Database>,
    txn: TxnView,
    stmt: Select,
    registry: &crate::engine::session_registry::SessionRegistry,
) -> Result<QueryResponse, QueryError> {
    if stmt.for_update {
        if txn.txn_id == 0 {
            return Err(error(
                crate::error::catalog::ER_NOT_SUPPORTED_YET,
                "locking subquery requires an active transaction",
            ));
        }
        let Some(ids) = lock_select_rows_for_update(db, txn, &stmt)? else {
            return Err(error(
                crate::error::catalog::ER_NOT_SUPPORTED_YET,
                "locking subquery shape is not supported",
            ));
        };
        let current = db.session_statement_view(txn.txn_id, txn.isolation, None);
        if let Some(result) = execute_locked_single_table_select(db, current, &stmt, ids)? {
            return Ok(result);
        }
        return execute_select(db, current, stmt, registry);
    }
    execute_select(db, txn, stmt, registry)
}

pub(super) fn execute_scalar_subquery_strict(
    db: &Arc<Database>,
    session: &SessionContext,
    txn: TxnView,
    subquery: &Select,
) -> Result<Value, QueryError> {
    let mut subquery = subquery.clone();
    super::qualification::qualify_select_tables(db, session, &mut subquery)?;
    let response =
        execute_expression_subquery(db, txn, subquery, dml_subquery_registry())?.materialize()?;
    scalar_subquery_value(response)
}

fn execute_in_subquery_values(
    db: &Arc<Database>,
    txn: TxnView,
    subquery: &Select,
) -> Result<Vec<Value>, QueryError> {
    let response = execute_expression_subquery(db, txn, subquery.clone(), dml_subquery_registry())?
        .materialize()?;
    let QueryResponse::ResultSet(result) = response else {
        return Err(error(
            crate::error::catalog::ER_NOT_SUPPORTED_YET,
            "IN subquery must return rows",
        ));
    };
    Ok(result
        .rows
        .into_iter()
        .filter_map(|row| row.into_iter().next())
        .collect())
}

fn execute_quantified_subquery_values(
    db: &Arc<Database>,
    txn: TxnView,
    subquery: &Select,
) -> Result<Vec<Value>, QueryError> {
    let response = execute_expression_subquery(db, txn, subquery.clone(), dml_subquery_registry())?
        .materialize()?;
    let QueryResponse::ResultSet(result) = response else {
        return Err(error(
            crate::error::catalog::ER_NOT_SUPPORTED_YET,
            "ANY/ALL/SOME subquery must return rows",
        ));
    };
    if result.columns.len() != 1 {
        return Err(error(
            crate::error::catalog::ER_OPERAND_COLUMNS,
            "Operand should contain 1 column(s)",
        ));
    }
    Ok(result
        .rows
        .into_iter()
        .filter_map(|row| row.into_iter().next())
        .collect())
}

fn execute_exists_subquery(
    db: &Arc<Database>,
    txn: TxnView,
    subquery: &Select,
) -> Result<bool, QueryError> {
    let mut subquery = subquery.clone();
    if subquery.from.is_empty() && subquery.joins.is_empty() {
        for item in &mut subquery.projection {
            if matches!(item, SelectItem::Wildcard) {
                *item = SelectItem::Expr {
                    expr: Expr::Literal(Literal::Int(1)),
                    alias: None,
                };
            }
        }
    }
    let response = execute_select(db, txn, subquery, dml_subquery_registry())?.materialize()?;
    let QueryResponse::ResultSet(result) = response else {
        return Err(error(
            crate::error::catalog::ER_NOT_SUPPORTED_YET,
            "EXISTS subquery must return rows",
        ));
    };
    Ok(!result.rows.is_empty())
}

fn literal_value_to_expr(value: Value) -> Expr {
    // Preserve the runtime type and arbitrary binary bytes across subqueries.
    Expr::BoundValue(value)
}

static DML_SUBQUERY_REGISTRY: OnceLock<SessionRegistry> = OnceLock::new();

fn dml_subquery_registry() -> &'static SessionRegistry {
    DML_SUBQUERY_REGISTRY.get_or_init(SessionRegistry::new)
}

#[inline]
fn check_safe_updates(
    session: &SessionContext,
    selection: Option<&Expr>,
    limit: Option<&crate::parser::LimitValue>,
) -> Result<(), QueryError> {
    if session.sysvars.sql_safe_updates && selection.is_none() && limit.is_none() {
        return Err(crate::storage::error(
            crate::error::catalog::ER_UPDATE_WITHOUT_KEY_IN_SAFE_MODE,
            "You are using safe update mode and you tried to update a table without a WHERE that uses a KEY column.",
        ));
    }
    Ok(())
}
