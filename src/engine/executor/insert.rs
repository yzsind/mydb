use super::*;
use std::collections::HashMap;

pub(super) fn resolve_column_default(column: &crate::storage::ColumnSchema) -> Value {
    crate::engine::expr::resolve_column_default_value(column)
}

pub(super) fn row_default_plans(
    table: &TableSchema,
    supplied: &[usize],
) -> Result<Vec<(usize, Expr)>, QueryError> {
    if supplied.len() == table.columns.len() {
        return Ok(Vec::new());
    }
    let mut plans = Vec::new();
    for (idx, column) in table.columns.iter().enumerate() {
        if column.state == crate::storage::ColumnState::Dropped {
            continue;
        }
        let Some(sql) = column.default_expr.as_deref() else {
            continue;
        };
        if supplied.contains(&idx) {
            continue;
        }
        // The common clock/default-function path has no row dependencies.
        // Do not add a second parser invocation to ordinary timestamp inserts.
        if sql.eq_ignore_ascii_case("CURRENT_TIMESTAMP")
            || sql.strip_suffix("()").is_some_and(|name| {
                !name.is_empty() && name.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'_')
            })
        {
            continue;
        }
        let expr = crate::parser::parse_catalog_expr(sql)
            .map_err(|err| error(crate::error::catalog::ER_PARSE_ERROR, err.to_string()))?;
        let depends_on_row = crate::parser::visit::try_for_each_expr(&expr, |node| {
            if matches!(node, Expr::Identifier(_) | Expr::CompoundIdentifier(_)) {
                Err(())
            } else {
                Ok(true)
            }
        })
        .is_err();
        if depends_on_row {
            plans.push((idx, expr));
        }
    }
    Ok(plans)
}

pub(super) fn materialize_row_defaults(
    db: &Database,
    table: &TableSchema,
    values: &mut [Value],
    plans: &[(usize, Expr)],
) -> Result<(), QueryError> {
    if plans.is_empty() {
        return Ok(());
    }
    db.coerce_row_for_load(table, values)?;
    for (idx, expr) in plans {
        values[*idx] = crate::engine::expr::eval_expr(expr, table, values)?;
    }
    Ok(())
}

/// Value used to backfill an INSTANT-added column's slot when reading a heap
/// row that pre-dates the ADD COLUMN. Normally the column's catalog DEFAULT
/// (or NULL). For a NOT NULL column added without an explicit DEFAULT, MySQL
/// fills existing rows with the type's implicit default (INT→0, VARCHAR→''),
/// distinct from a persisted column DEFAULT — so INSERT-omit still errors
/// 1364 while old rows read a concrete value (issue #84).
pub(crate) fn column_read_backfill_default(column: &crate::storage::ColumnSchema) -> Value {
    if let Some(value) = &column.read_backfill_default {
        return value.clone();
    }
    if let Some(v) = &column.default {
        return v.clone();
    }
    // NOT NULL, no static DEFAULT and no DEFAULT(<expr>): backfill old rows
    // with the type's implicit default rather than NULL. Columns with a
    // DEFAULT(<expr>) keep the original NULL-cache behavior (the expr is
    // resolved on the write path, not cached here).
    if !column.nullable && column.default_expr.is_none() {
        if let Some(implicit) = implicit_default_for_type(&column.data_type) {
            return implicit;
        }
    }
    Value::Null
}

/// True when the session is in strict sql_mode for DML coercion. Same
/// posture as the storage-layer numeric/temporal coercion (`STRICT_*` set,
/// or an empty/unset sql_mode = mydb's strict default); only an explicit
/// non-strict sql_mode relaxes it.
fn insert_strict_mode() -> bool {
    let bits = crate::engine::expr::session_temporal_sql_mode_bits();
    bits & crate::engine::api::TEMPORAL_MODE_STRICT != 0
        || bits & crate::engine::api::TEMPORAL_MODE_EXPLICIT == 0
}

/// Non-strict multi-row INSERT and INSERT IGNORE repair explicit NULL in a
/// NOT NULL column with the type's implicit default and warning 1048. A
/// strict single-row statement without IGNORE keeps the hard error.
fn coerce_ignorable_insert_nulls(
    db: &Arc<Database>,
    table_name: &str,
    supplied_columns: Option<&[String]>,
    rows: &mut [Vec<Value>],
    ignore: bool,
) -> Result<(), QueryError> {
    if insert_strict_mode() && !ignore {
        return Ok(());
    }
    let table = db.table(table_name)?;
    let target_columns = insert_column_indexes(&table, supplied_columns)?;
    for row in rows {
        for (value_idx, &column_idx) in target_columns.iter().enumerate() {
            let column = &table.columns[column_idx];
            if column.nullable
                || column.generated_expr.is_some()
                || column.auto_increment
                || !matches!(row.get(value_idx), Some(Value::Null))
            {
                continue;
            }
            let Some(default) = implicit_default_for_type(&column.data_type) else {
                return Err(crate::error::messages::bad_null(&column.name));
            };
            crate::engine::api::push_statement_warning(
                crate::error::ConditionLevel::Warning,
                crate::error::catalog::ER_BAD_NULL_ERROR,
                format!("Column '{}' cannot be null", column.name),
            );
            if ignore {
                db.dml_stats().record_ignore_warning();
            }
            row[value_idx] = default;
        }
    }
    Ok(())
}

fn coerce_legacy_timestamp_nulls(
    db: &Arc<Database>,
    table_name: &str,
    supplied_columns: Option<&[String]>,
    rows: &mut [Vec<Value>],
) -> Result<(), QueryError> {
    if crate::engine::expr::session_explicit_defaults_for_timestamp() {
        return Ok(());
    }
    let table = db.table(table_name)?;
    let target_columns = insert_column_indexes(&table, supplied_columns)?;
    for row in rows {
        for (value_idx, &column_idx) in target_columns.iter().enumerate() {
            let column = &table.columns[column_idx];
            if matches!(
                column.data_type,
                crate::storage::DataTypeKind::Timestamp { .. }
            ) && !column.nullable
                && column.generated_expr.is_none()
                && matches!(row.get(value_idx), Some(Value::Null))
            {
                // Legacy explicit_defaults_for_timestamp=OFF semantics:
                // assigning NULL to a NOT NULL TIMESTAMP means the current
                // timestamp even when the column declares another default.
                row[value_idx] = Value::Timestamp(
                    crate::engine::expr::session_utc_now()
                        .and_utc()
                        .timestamp_micros(),
                );
            }
        }
    }
    Ok(())
}

fn warn_explicit_lob_default_without_value(expr: &Expr, column: &crate::storage::ColumnSchema) {
    if !expr_is_default_for_column(expr, &column.name)
        || insert_strict_mode()
        || column.default.is_some()
        || column.default_expr.is_some()
        || column.auto_increment
        || column.generated_expr.is_some()
        || !matches!(
            column.data_type,
            crate::storage::DataTypeKind::Text { .. }
                | crate::storage::DataTypeKind::Blob { .. }
                | crate::storage::DataTypeKind::Json
                | crate::storage::DataTypeKind::Geometry { .. }
        )
    {
        return;
    }
    crate::engine::api::push_statement_warning(
        crate::error::ConditionLevel::Warning,
        crate::error::catalog::ER_NO_DEFAULT_FOR_FIELD,
        format!("Field '{}' doesn't have a default value", column.name),
    );
}

/// MySQL's type-implicit default used when a required column is omitted in
/// non-strict mode (numeric → 0, string → '', temporal → zero value, ENUM →
/// first member). Returns `None` for types that have no implicit default
/// (JSON / GEOMETRY / VECTOR) — those error 1364 even in non-strict mode.
pub(crate) fn implicit_default_for_type(kind: &crate::storage::DataTypeKind) -> Option<Value> {
    use crate::storage::DataTypeKind as K;
    Some(match kind {
        K::TinyInt
        | K::SmallInt
        | K::MediumInt
        | K::Int
        | K::BigInt
        | K::Bool
        | K::Bit { .. }
        | K::Decimal { .. }
        | K::Float { .. }
        | K::Double { .. }
        | K::Year => Value::Int(0),
        K::Char { .. } | K::VarChar { .. } | K::Text { .. } | K::Set { .. } => {
            Value::String(String::new())
        }
        K::Binary { .. } | K::VarBinary { .. } | K::Blob { .. } => Value::Bytes(Vec::new()),
        K::Date => Value::String("0000-00-00".to_string()),
        K::DateTime { .. } | K::Timestamp { .. } => {
            Value::String("0000-00-00 00:00:00".to_string())
        }
        K::Time { .. } => Value::String("00:00:00".to_string()),
        K::Enum { values } => Value::String(values.first().cloned().unwrap_or_default()),
        K::Json | K::Geometry { .. } | K::Vector { .. } => return None,
    })
}

/// Physical indexes of columns omitted from `supplied` that MySQL treats as
/// "field has no default value": NOT NULL, no DEFAULT / DEFAULT(expr), not
/// AUTO_INCREMENT, not generated, not a dropped tombstone.
pub(super) fn missing_required_columns(table: &TableSchema, supplied: &[usize]) -> Vec<usize> {
    table
        .columns
        .iter()
        .enumerate()
        .filter_map(|(idx, column)| {
            if supplied.contains(&idx)
                || column.state == crate::storage::ColumnState::Dropped
                || column.generated_expr.is_some()
                || column.auto_increment
                || column.nullable
                || column.default.is_some()
                || column.default_expr.is_some()
            {
                None
            } else {
                Some(idx)
            }
        })
        .collect()
}

/// Apply MySQL's "field has no default value" policy to a built row for the
/// pre-computed `missing` required columns: strict sql_mode errors 1364;
/// non-strict stores the type-implicit default and records warning 1364.
///
/// `ignore` reflects the statement's `INSERT IGNORE` modifier. MySQL downgrades
/// the hard 1364 error to a warning + implicit default under IGNORE even in
/// strict sql_mode (mirroring how `ignorable_update_error` downgrades the
/// UPDATE IGNORE 1048/1364 class), so IGNORE takes the non-strict branch.
pub(super) fn enforce_missing_required_columns(
    table: &TableSchema,
    values: &mut [Value],
    missing: &[usize],
    ignore: bool,
) -> Result<(), QueryError> {
    if missing.is_empty() {
        return Ok(());
    }
    // IGNORE forces the warn+default path even in strict mode; otherwise strict
    // sql_mode raises the hard error.
    let downgrade = ignore || !insert_strict_mode();
    for &idx in missing {
        let column = &table.columns[idx];
        let msg = format!("Field '{}' doesn't have a default value", column.name);
        match implicit_default_for_type(&column.data_type) {
            Some(default) if downgrade => {
                // MySQL reports one 1364 condition per omitted column for the
                // statement, not once per VALUES row. The message includes
                // the column name, so statement-scoped dedup still preserves
                // separate diagnostics when multiple required columns are
                // omitted.
                crate::engine::expr::with_expression_warning_dedup(
                    0x4d49_5353_494e_475f_u64,
                    || {
                        crate::engine::api::push_statement_warning(
                            crate::error::ConditionLevel::Warning,
                            crate::error::catalog::ER_NO_DEFAULT_FOR_FIELD,
                            msg,
                        );
                    },
                );
                values[idx] = default;
            }
            _ => return Err(error(crate::error::catalog::ER_NO_DEFAULT_FOR_FIELD, msg)),
        }
    }
    Ok(())
}

/// Build the `Records: N Duplicates: D Warnings: W` OK-packet info trailer
/// (MySQL's multi-row / conflict / INSERT…SELECT diagnostic). `records` is
/// the number of source rows the statement attempted; the running duplicate
/// count (IGNORE-skipped / ODKU-matched / REPLACE-deleted rows) is drained
/// from the thread-local accumulator. A plain single-row INSERT with no
/// duplicates gets no trailer — MySQL reports just "1 row affected" there.
pub(super) fn finalize_insert_info(records: u64, is_select: bool) {
    let duplicates = crate::engine::api::take_insert_duplicates();
    if records == 1 && duplicates == 0 && !is_select {
        return;
    }
    let warnings = crate::engine::api::statement_warning_count();
    crate::engine::api::set_statement_info(format!(
        "Records: {records} Duplicates: {duplicates} Warnings: {warnings}"
    ));
}

/// M3.B1 — Per-chunk size for streaming INSERT…SELECT. Picked to amortize
/// the per-call storage overhead (table lock acquisition is already
/// per-statement, but each `insert_rows_detailed` builds row headers,
/// index entries vectors etc.) without holding millions of rows in
/// the executor heap at once. 4 KiB rows ≈ 16 MiB per chunk worst-case.
#[cfg(not(test))]
const INSERT_SELECT_CHUNK_ROWS: usize = 4096;
// Unit tests need to cross the same chunk/queue/state-machine boundaries,
// but using the production 4K-row chunk turns every semantic regression into
// a storage throughput benchmark.  A smaller test-only chunk preserves the
// control flow while keeping fixtures bounded.
#[cfg(test)]
const INSERT_SELECT_CHUNK_ROWS: usize = 64;

pub(super) fn execute_insert(
    db: &Arc<Database>,
    session: &mut SessionContext,
    txn: TxnView,
    mut stmt: Insert,
    registry: &SessionRegistry,
) -> Result<QueryResponse, QueryError> {
    let _dml_expression_guard = crate::engine::expr::DmlExpressionGuard::new();
    // M5.C2 — `INSERT INTO t PARTITION (p0, p1) ...` resolves to a set of
    // allowed storage_ids. Every routed row must land in one of them; if
    // not, MySQL error 1748. Resolve once, hand the set into the dispatch
    // helper which validates each row before delegating to the storage layer.
    let partition_filter = if stmt.partitions.is_empty() {
        None
    } else {
        super::partition_clause::resolve_partition_clause(db, &stmt.table, &stmt.partitions)?
    };
    // Take ownership of `source` separately so the rest of `stmt`
    // (table name, columns, conflict flags) stays available to the
    // per-chunk dispatch helper below.
    let source = std::mem::replace(&mut stmt.source, InsertSource::Values(Vec::new()));
    match source {
        InsertSource::Values(mut rows) => {
            for row in &mut rows {
                for expr in row {
                    if super::prepared_dml::expr_contains_subquery(expr) {
                        super::dml::materialize_subqueries(db, txn, expr)?;
                    }
                }
            }
            let mut rows = eval_insert_value_rows(db, &stmt.table, stmt.columns.as_deref(), rows)?;
            coerce_legacy_timestamp_nulls(db, &stmt.table, stmt.columns.as_deref(), &mut rows)?;
            if rows.len() > 1 || stmt.ignore {
                coerce_ignorable_insert_nulls(
                    db,
                    &stmt.table,
                    stmt.columns.as_deref(),
                    &mut rows,
                    stmt.ignore,
                )?;
            }
            let records = rows.len() as u64;
            if let Some(allowed) = &partition_filter {
                filter_insert_partition_set(db, &stmt, allowed, &mut rows)?;
            }
            let ctx = InsertChunkCtx::compute(db, session, &stmt);
            let resp = dispatch_insert_chunk(db, session, txn, &stmt, &ctx, rows, registry)?;
            finalize_insert_info(records, false);
            Ok(resp)
        }
        InsertSource::Query(select) => {
            let table = db.table(&stmt.table)?.clone();
            if insert_targets_generated(&table, stmt.columns.as_deref())? {
                return Err(generated_column_assignment_error());
            }
            // For INSERT … SELECT the PARTITION clause similarly enforces
            // the routed-partition constraint. The per-chunk path below
            // catches each chunk before dispatch.
            execute_insert_from_select_stream(
                db,
                session,
                txn,
                &stmt,
                select,
                registry,
                partition_filter,
            )
        }
    }
}

/// Borrowed-AST variant of [`execute_insert`] for the prepared-DML fast
/// path: a plain `INSERT … VALUES` executes without cloning the cached
/// statement. The owned path only uses ownership to move literal bytes
/// out of the AST (`Expr::Literal` / `Expr::BoundValue` fast paths in
/// `eval_insert_value_rows`); a prepared insert's rows are placeholders
/// that are re-evaluated per execute anyway, so borrowing costs nothing
/// there and the occasional literal falls back to a per-value clone —
/// still far cheaper than cloning the whole `Insert`. Returns
/// `Ok(None)` for `INSERT … SELECT`, which keeps the owned streaming
/// path (the caller clones and delegates).
pub(super) fn execute_insert_borrowed(
    db: &Arc<Database>,
    session: &mut SessionContext,
    txn: TxnView,
    stmt: &Insert,
    registry: &SessionRegistry,
) -> Result<Option<QueryResponse>, QueryError> {
    let _dml_expression_guard = crate::engine::expr::DmlExpressionGuard::new();
    let partition_filter = if stmt.partitions.is_empty() {
        None
    } else {
        super::partition_clause::resolve_partition_clause(db, &stmt.table, &stmt.partitions)?
    };
    match &stmt.source {
        InsertSource::Values(expr_rows) => {
            if expr_rows
                .iter()
                .flatten()
                .any(super::prepared_dml::expr_contains_subquery)
            {
                let mut resolved_rows = expr_rows.clone();
                for row in &mut resolved_rows {
                    for expr in row {
                        if super::prepared_dml::expr_contains_subquery(expr) {
                            super::dml::materialize_subqueries(db, txn, expr)?;
                        }
                    }
                }
                let mut owned = stmt.clone();
                owned.source = InsertSource::Values(resolved_rows);
                return execute_insert(db, session, txn, owned, registry).map(Some);
            }
            let mut rows = eval_insert_value_rows_borrowed(
                db,
                &stmt.table,
                stmt.columns.as_deref(),
                expr_rows,
            )?;
            coerce_legacy_timestamp_nulls(db, &stmt.table, stmt.columns.as_deref(), &mut rows)?;
            if rows.len() > 1 || stmt.ignore {
                coerce_ignorable_insert_nulls(
                    db,
                    &stmt.table,
                    stmt.columns.as_deref(),
                    &mut rows,
                    stmt.ignore,
                )?;
            }
            let records = rows.len() as u64;
            if let Some(allowed) = &partition_filter {
                filter_insert_partition_set(db, &stmt, allowed, &mut rows)?;
            }
            let ctx = InsertChunkCtx::compute(db, session, stmt);
            let resp = dispatch_insert_chunk(db, session, txn, stmt, &ctx, rows, registry)?;
            finalize_insert_info(records, false);
            Ok(Some(resp))
        }
        // P4 — prepared INSERT…SELECT streams through the borrowed cached
        // `Insert`, cloning only the source query (which the executor
        // must own to plan/rewrite) instead of deep-cloning the whole `Insert`
        // wrapper on every execute.
        InsertSource::Query(select) => {
            let table = db.table(&stmt.table)?;
            if insert_targets_generated(&table, stmt.columns.as_deref())? {
                return Err(generated_column_assignment_error());
            }
            let resp = execute_insert_from_select_stream(
                db,
                session,
                txn,
                stmt,
                select.clone(),
                registry,
                partition_filter,
            )?;
            Ok(Some(resp))
        }
    }
}

/// M5.C2 — assert every row in `rows` routes to a partition whose
/// `storage_id` is in `allowed`. Called from the executor *before* handing
/// rows to the storage layer so the failure happens at the same parse-time
/// step a real RANGE/LIST mismatch would, and so the row never lands on
/// disk only to be rolled back.
fn validate_insert_partition_set(
    db: &Arc<Database>,
    table_name: &str,
    allowed: &[u64],
    rows: &[Vec<Value>],
) -> Result<(), QueryError> {
    if rows.is_empty() {
        return Ok(());
    }
    let table = db.table(table_name)?;
    if !table.is_partitioned() {
        // resolve_partition_clause already errored on this; the check is
        // re-asserted defensively in case the catalog changed between
        // resolve and validate.
        return Ok(());
    }
    for row in rows {
        let routed = table.route_row_to_partition(row)?;
        super::partition_clause::assert_insert_partition_allowed(allowed, routed.storage_id)?;
    }
    Ok(())
}

/// Validate routed physical rows, preserving IGNORE row skipping and diagnostics.
fn filter_insert_partition_set(
    db: &Arc<Database>,
    stmt: &Insert,
    allowed: &[u64],
    rows: &mut Vec<Vec<Value>>,
) -> Result<(), QueryError> {
    let table = db.table(&stmt.table)?;
    let indexes = insert_column_indexes(&table, stmt.columns.as_deref())?;
    let template: Vec<_> = table.columns.iter().map(resolve_column_default).collect();
    let mut failure = None;
    rows.retain(|row| {
        if failure.is_some() {
            return true;
        }
        let result = (|| {
            if row.len() != indexes.len() {
                return Err(error(
                    crate::error::catalog::ER_WRONG_VALUE_COUNT_ON_ROW,
                    "column count does not match value count",
                ));
            }
            let mut physical = template.clone();
            for (&idx, value) in indexes.iter().zip(row) {
                physical[idx] = value.clone();
            }
            let routed = table.route_row_to_partition(&physical)?;
            super::partition_clause::assert_insert_partition_allowed(allowed, routed.storage_id)
        })();
        match result {
            Ok(()) => true,
            Err(err)
                if stmt.ignore
                    && err.is_any(&[
                        crate::error::catalog::ER_NO_PARTITION_FOR_GIVEN_VALUE,
                        crate::error::catalog::ER_ROW_DOES_NOT_MATCH_GIVEN_PARTITION_SET,
                    ]) =>
            {
                crate::engine::api::push_statement_condition(
                    crate::error::ConditionLevel::Warning,
                    &err,
                );
                crate::engine::api::add_insert_duplicates(1);
                db.dml_stats().record_ignore_warning();
                false
            }
            Err(err) => {
                failure = Some(err);
                true
            }
        }
    });
    failure.map_or(Ok(()), Err)
}

/// Apply a single (possibly partial) batch of rows through the same
/// trigger / conflict / fast paths the original `execute_insert`
/// used. Each call is independent: view-check, trigger dispatch and
/// conflict handling all run per chunk.
/// Per-statement INSERT dispatch context, computed once and reused across
/// every chunk of an INSERT…SELECT. The trigger-routing decision and the
/// AUTO_INCREMENT-fast-path eligibility are stable for the statement's whole
/// lifetime (they depend only on the catalog, the statement, and the session's
/// autocommit state — none of which change between chunks), so resolving them
/// once avoids repeating the catalog-lock lookups per 4096-row chunk.
struct InsertChunkCtx {
    needs_trigger_path: bool,
    autoinc_fast: bool,
}

impl InsertChunkCtx {
    fn compute(db: &Arc<Database>, session: &SessionContext, stmt: &Insert) -> Self {
        // REPLACE deletes the conflicting row before inserting, so it must fire
        // DELETE triggers; ON DUPLICATE KEY UPDATE takes the UPDATE branch on
        // conflict, so it must fire UPDATE triggers. Otherwise a table with
        // only a DELETE / UPDATE trigger would silently skip it on REPLACE /
        // ODKU.
        let needs_trigger_path = table_has_insert_triggers(db, &stmt.table)
            || (stmt.replace && table_has_delete_triggers(db, &stmt.table))
            || (stmt.on_duplicate_key_update.is_some()
                && table_has_update_triggers(db, &stmt.table));
        // Only consulted on the plain path (no triggers / conflict modifiers);
        // resolving it here keeps the per-chunk dispatch branch-free.
        let autoinc_fast = can_use_autoinc_fast_insert(db, session, stmt);
        Self {
            needs_trigger_path,
            autoinc_fast,
        }
    }
}

fn dispatch_insert_chunk(
    db: &Arc<Database>,
    session: &mut SessionContext,
    txn: TxnView,
    stmt: &Insert,
    ctx: &InsertChunkCtx,
    mut rows: Vec<Vec<Value>>,
    registry: &SessionRegistry,
) -> Result<QueryResponse, QueryError> {
    let _ignore_coercion = crate::storage::DmlIgnoreCoercionGuard::new(stmt.ignore);
    if let Some(predicate) = &stmt.view_check_predicate {
        if stmt.ignore {
            let mut accepted = Vec::with_capacity(rows.len());
            for row in rows {
                match validate_insert_view_check(
                    db,
                    &stmt.table,
                    stmt.columns.as_deref(),
                    std::slice::from_ref(&row),
                    predicate,
                ) {
                    Ok(()) => accepted.push(row),
                    Err(err) if err.is(crate::error::catalog::ER_VIEW_CHECK_FAILED) => {
                        record_ignore_warning(db, &err);
                    }
                    Err(err) => return Err(err),
                }
            }
            rows = accepted;
        } else {
            validate_insert_view_check(db, &stmt.table, stmt.columns.as_deref(), &rows, predicate)?;
        }
    }
    if ctx.needs_trigger_path {
        return execute_insert_rows_with_triggers(
            db,
            session,
            txn,
            &stmt.table,
            stmt.columns.as_deref(),
            rows,
            stmt.ignore,
            stmt.replace,
            stmt.on_duplicate_key_update.as_deref(),
            stmt.insert_alias.as_ref(),
            stmt.view_check_predicate.as_ref(),
            stmt.returning.as_ref(),
            stmt.target_alias.as_deref(),
            registry,
        );
    }
    if stmt.ignore || stmt.replace || stmt.on_duplicate_key_update.is_some() {
        return execute_insert_with_conflict(
            db,
            txn,
            &stmt.table,
            stmt.columns.as_deref(),
            rows,
            stmt.ignore,
            stmt.replace,
            stmt.on_duplicate_key_update.as_deref(),
            stmt.insert_alias.as_ref(),
            stmt.view_check_predicate.as_ref(),
            stmt.returning.as_ref(),
            stmt.target_alias.as_deref(),
        );
    }
    execute_insert_values(
        db,
        txn,
        &stmt.table,
        stmt.columns.as_deref(),
        rows,
        ctx.autoinc_fast,
        stmt.returning.as_ref(),
        stmt.target_alias.as_deref(),
    )
}

/// Decide whether the plain INSERT chunk can route through the
/// AUTO_INCREMENT-only fast path (skip redundant in-batch dedup and row
/// locks, while retaining unique-key locks and storage conflict checks). Returning `true` only when
/// EVERY following condition holds is what keeps the safety argument
/// in [`Database::insert_rows_autoinc_fast_detailed`] intact.
fn can_use_autoinc_fast_insert(
    db: &Arc<Database>,
    session: &SessionContext,
    stmt: &Insert,
) -> bool {
    // The fast path is autocommit-only: the row locks we skip are
    // the same machinery that gives a *multi-
    // statement* txn its in-txn UPDATE/DELETE safety. With autocommit
    // the txn ends right after the INSERT, so the skipped state is
    // never observed. `current_txn` is intentionally NOT in this guard
    // — `execute_dml_in_transaction_with_session` opens the txn for
    // this statement before calling us, so it's always Some here; the
    // autocommit + !explicit_txn pair is the actual contract.
    if !session.autocommit || session.explicit_txn {
        return false;
    }
    // Conflict modifiers carry their own row-level reconciliation paths.
    if stmt.ignore || stmt.replace || stmt.on_duplicate_key_update.is_some() {
        return false;
    }
    // PARTITION (...) routes through the partition-aware insert path
    // which has its own lock structure — keep the slow path for now.
    if !stmt.partitions.is_empty() {
        return false;
    }
    let Ok(table) = db.table(&stmt.table) else {
        return false;
    };
    if table.is_partitioned() {
        return false;
    }
    // FK child-side checks acquire parent locks per row — skipping
    // the row lock here breaks the conflict-detection symmetry on
    // the parent side. Bail.
    if !table.foreign_keys.is_empty() {
        return false;
    }
    // The fast path's "no in-batch dedup needed" argument rests on
    // the AUTO_INCREMENT reservation handing out a strictly-
    // monotonic range. Without an AUTO_INCREMENT column the rows
    // carry user values that may collide.
    let Some(autoinc_idx) = table.auto_increment_column_index() else {
        return false;
    };
    // Every unique index must be over exactly the AUTO_INCREMENT
    // column (one and only one column). A composite or
    // non-AUTOINC-covering unique index could still have a collision
    // from user-supplied values in the non-AUTOINC parts.
    let autoinc_name = &table.columns[autoinc_idx].name;
    for index in &table.indexes {
        if !index.enforces_uniqueness() {
            continue;
        }
        let only_autoinc =
            index.columns.len() == 1 && index.columns[0].eq_ignore_ascii_case(autoinc_name);
        if !only_autoinc {
            return false;
        }
    }
    // The INSERT statement must NOT mention the AUTO_INCREMENT
    // column in its column list — otherwise the per-row value may
    // be a user-supplied literal and lose the reservation's
    // uniqueness guarantee. We only allow the explicit
    // `INSERT INTO t (col_a, col_b) VALUES …` shape that omits the
    // AUTO_INCREMENT column. Without a column list (`INSERT INTO t
    // VALUES …`) every column including the AUTO_INCREMENT one is
    // positionally supplied — bail.
    let Some(columns) = stmt.columns.as_deref() else {
        return false;
    };
    if columns.iter().any(|c| c.eq_ignore_ascii_case(autoinc_name)) {
        return false;
    }
    true
}

#[derive(Default)]
struct InsertSelectSourceBindings {
    expressions: Vec<Expr>,
    slots: Vec<InsertSelectSourceSlot>,
    extra_columns: usize,
}

enum InsertSelectSourceSlot {
    FromStart(usize),
    FromEnd(usize),
    Extra(usize),
}

/// Carry SELECT-source values separately from the target row. In particular,
/// a source reference need not occur in the INSERT projection and must retain
/// its original type before target-column coercion.
fn insert_select_source_bindings(
    db: &Arc<Database>,
    stmt: &Insert,
    query: &mut crate::parser::QueryExpr,
) -> Result<InsertSelectSourceBindings, QueryError> {
    use crate::parser::visit::{visit_expr, AstVisitor};
    let Some(updates) = &stmt.on_duplicate_key_update else {
        return Ok(InsertSelectSourceBindings::default());
    };
    let table = db.table(&stmt.table)?;
    struct Collect<'a> {
        table: &'a TableSchema,
        target_alias: Option<&'a str>,
        found: Vec<Expr>,
        depth: usize,
    }
    impl AstVisitor for Collect<'_> {
        fn walk_expr_children(&self, expr: &Expr) -> bool {
            !matches!(expr, Expr::Function { name, .. } if name.eq_ignore_ascii_case("VALUES") || name.eq_ignore_ascii_case("DEFAULT"))
        }
        fn enter_select(&mut self, _: &Select) {
            self.depth += 1;
        }
        fn exit_select(&mut self, _: &Select) {
            self.depth -= 1;
        }
        fn enter_expr(&mut self, expr: &Expr) {
            if self.depth != 0 {
                return;
            }
            let source = match expr {
                Expr::Identifier(name) => self.table.column_index(name).is_err(),
                Expr::CompoundIdentifier(parts) if parts.len() >= 2 => {
                    let qualifier = &parts[parts.len() - 2];
                    !qualifier.eq_ignore_ascii_case(&self.table.name)
                        && !self
                            .target_alias
                            .is_some_and(|alias| alias.eq_ignore_ascii_case(qualifier))
                }
                _ => false,
            };
            if source && !self.found.contains(expr) {
                self.found.push(expr.clone());
            }
        }
    }
    let mut collect = Collect {
        table: &table,
        target_alias: stmt.target_alias.as_deref(),
        found: Vec::new(),
        depth: 0,
    };
    for assignment in updates {
        visit_expr(&mut collect, &assignment.value);
    }
    // A set query exposes only its output names. Never append a column to
    // set branches: it would change DISTINCT/INTERSECT/EXCEPT semantics.
    let projection = &query
        .leftmost_select()
        .expect("query has a SELECT leaf")
        .projection;
    let mut extra_projection = Vec::new();
    let mut result = InsertSelectSourceBindings::default();
    for source in collect.found {
        let projected = projection.iter().position(|item| match item {
            SelectItem::Expr { expr, alias } => expr == &source
                || matches!(&source, Expr::Identifier(name) if alias.as_ref().is_some_and(|a| a.eq_ignore_ascii_case(name))),
            _ => false,
        });
        let explicit = |item: &SelectItem| matches!(item, SelectItem::Expr { .. });
        let slot = if let Some(index) = projected {
            // Reuse the actual projected value, especially RAND/UUID and other
            // volatile aliases. Evaluating the expression a second time would
            // make ODKU observe a different row from the INSERT candidate.
            if projection[..index].iter().all(explicit) {
                InsertSelectSourceSlot::FromStart(index)
            } else if projection[index + 1..].iter().all(explicit) {
                InsertSelectSourceSlot::FromEnd(projection.len() - index)
            } else {
                return Err(error(
                    crate::error::catalog::ER_NOT_SUPPORTED_YET,
                    "ODKU aliases between multiple wildcard projections are not supported",
                ));
            }
        } else {
            let select = query.as_select().ok_or_else(|| error(
                crate::error::catalog::ER_NOT_SUPPORTED_YET,
                "ON DUPLICATE KEY UPDATE source columns outside a set query projection are not supported",
            ))?;
            if select.distinct
                || (!select.group_by.is_empty() && !select.group_by.contains(&source))
            {
                return Err(error(crate::error::catalog::ER_NOT_SUPPORTED_YET,
                    "ON DUPLICATE KEY UPDATE source columns outside a DISTINCT/GROUP BY projection are not supported"));
            }
            extra_projection.push(SelectItem::Expr {
                expr: source.clone(),
                alias: None,
            });
            let slot = InsertSelectSourceSlot::Extra(result.extra_columns);
            result.extra_columns += 1;
            slot
        };
        result.expressions.push(source);
        result.slots.push(slot);
    }
    if !extra_projection.is_empty() {
        query
            .as_select_mut()
            .expect("extra columns require a SELECT")
            .projection
            .extend(extra_projection);
    }
    Ok(result)
}

fn bind_insert_select_source(stmt: &Insert, bindings: &[Expr], values: &[Value]) -> Insert {
    use crate::parser::visit::{visit_expr_mut, AstVisitorMut};
    struct Bind<'a> {
        bindings: &'a [Expr],
        values: &'a [Value],
        depth: usize,
    }
    impl AstVisitorMut for Bind<'_> {
        fn enter_select(&mut self, _: &mut Select) {
            self.depth += 1;
        }
        fn exit_select(&mut self, _: &mut Select) {
            self.depth -= 1;
        }
        fn enter_expr(&mut self, expr: &mut Expr) {
            if self.depth != 0 {
                return;
            }
            if let Some(index) = self.bindings.iter().position(|source| source == expr) {
                *expr = Expr::BoundValue(self.values[index].clone());
            }
        }
    }
    let mut row_stmt = stmt.clone();
    let mut bind = Bind {
        bindings,
        values,
        depth: 0,
    };
    if let Some(updates) = &mut row_stmt.on_duplicate_key_update {
        for assignment in updates {
            visit_expr_mut(&mut bind, &mut assignment.value);
        }
    }
    row_stmt
}

/// Bind binary destinations once, preserving source charsets at the sink.
fn insert_select_binary_targets(db: &Database, stmt: &Insert) -> Result<Vec<usize>, QueryError> {
    let table = db.table(&stmt.table)?;
    let targets = insert_column_indexes(&table, stmt.columns.as_deref())?;
    Ok(targets
        .iter()
        .enumerate()
        .filter_map(|(position, &column)| {
            matches!(
                table.columns[column].data_type,
                DataTypeKind::Binary { .. }
                    | DataTypeKind::VarBinary { .. }
                    | DataTypeKind::Blob { .. }
            )
            .then_some(position)
        })
        .collect())
}

fn insert_select_binary_sources(
    response: &QueryResponse,
    targets: &[usize],
) -> Vec<(usize, &'static str)> {
    let columns: &[Column] = match response {
        QueryResponse::ResultSet(result) => &result.columns,
        QueryResponse::SharedResultSet(result) => &result.columns,
        QueryResponse::StreamingResultSet(result) => &result.columns,
        _ => &[],
    };
    targets
        .iter()
        .map(|&position| {
            let charset = columns
                .get(position)
                .and_then(|column| column.source.collation_id)
                .and_then(crate::charset::resolve_collation_id)
                .map_or_else(crate::engine::expr::connection_charset, |collation| {
                    collation.charset
                });
            (position, charset)
        })
        .collect()
}

fn validate_insert_select_width(
    response: &QueryResponse,
    expected: usize,
) -> Result<(), QueryError> {
    let width = match response {
        QueryResponse::ResultSet(result) => result.columns.len(),
        QueryResponse::SharedResultSet(result) => result.columns.len(),
        QueryResponse::StreamingResultSet(result) => result.columns.len(),
        _ => 0,
    };
    if width != expected {
        return Err(error(
            crate::error::catalog::ER_WRONG_VALUE_COUNT_ON_ROW,
            "column count does not match value count",
        ));
    }
    Ok(())
}

fn encode_insert_select_binary_sources(
    rows: &mut [Vec<Value>],
    sources: &[(usize, &'static str)],
) -> Result<(), QueryError> {
    if sources.is_empty() {
        return Ok(());
    }
    for row in rows {
        for &(position, charset) in sources {
            if let Some(value) = row.get_mut(position) {
                *value = crate::engine::expr::encode_binary_assignment_value(
                    std::mem::replace(value, Value::Null),
                    charset,
                )?;
            }
        }
    }
    Ok(())
}

/// Stream a query into the INSERT sink, materializing self-referencing
/// sources before the first write. Same-transaction writes are visible to
/// later reads, so an interleaved source could otherwise read its own output.
fn execute_insert_from_select_stream(
    db: &Arc<Database>,
    session: &mut SessionContext,
    txn: TxnView,
    stmt: &Insert,
    mut select: crate::parser::QueryExpr,
    registry: &SessionRegistry,
    partition_filter: Option<Vec<u64>>,
) -> Result<QueryResponse, QueryError> {
    let target = db.table(&stmt.table)?;
    let target_width = insert_column_indexes(&target, stmt.columns.as_deref())?.len();
    let source_bindings = insert_select_source_bindings(db, stmt, &mut select)?;
    let binary_targets = insert_select_binary_targets(db, stmt)?;
    let mut total_affected: u64 = 0;
    let mut total_records: u64 = 0;
    let mut returning_columns = None;
    let mut returning_rows = Vec::new();
    // Resolve the trigger-routing / autoinc-fast decision once for the whole
    // statement instead of per chunk (P6): both are stable across chunks.
    let ctx = InsertChunkCtx::compute(db, session, stmt);
    // P1 — let a param-carrying source (prepared / auto-parameterised
    // INSERT…SELECT) stream instead of being forced eager. The chunk loop
    // below consumes rows incrementally and the streaming scan worker
    // re-installs the snapshotted params, so streaming is param-safe here.
    let _param_stream_guard = crate::engine::params::StreamingParamSourceGuard::enter();

    // P2 — attempt the parallel-insert fast path for shapes proved safe by
    // `insert_select_parallel_gate`: no autoinc, no triggers, no conflict
    // modifiers, no FK, non-partitioned, no view check, no RETURNING, and the
    // source doesn't reference the target table. When admitted, chunks are
    // dispatched to a worker pool that shares the same `TxnView` and calls
    // `Database::insert_rows` directly (chunk-internal atomicity is
    // unchanged). Everything else routes through the serial sink below,
    // preserving the existing semantics.
    if let Some(parallel_degree) = insert_select_parallel_gate(db, session, stmt, &select, &ctx) {
        let (affected, records) = execute_insert_from_select_parallel(
            db,
            session,
            txn,
            &stmt.table,
            partition_filter.as_deref(),
            select,
            registry,
            parallel_degree,
            &binary_targets,
            target_width,
        )?;
        total_affected = affected;
        total_records = records;
    } else {
        // Reading from the target table must complete before the first write.
        // Same-transaction rows are visible by design, so interleaving a live
        // scan with inserts is a Halloween hazard when a heap grows or an
        // index splits. Materialize only this semantic dependency shape; all
        // other INSERT ... SELECT statements retain bounded streaming.
        let source_references_target = source_references_target(&select, &stmt.table);
        let source_response =
            super::dispatch::execute_set_operation_branch_response(db, txn, select, registry)?;
        validate_insert_select_width(
            &source_response,
            target_width + source_bindings.extra_columns,
        )?;
        let source_response = if source_references_target {
            source_response.materialize()?
        } else {
            source_response
        };
        let binary_sources = insert_select_binary_sources(&source_response, &binary_targets);
        crate::engine::executor::select::drive_response_chunks(
            source_response,
            INSERT_SELECT_CHUNK_ROWS,
            |mut chunk_rows| {
                encode_insert_select_binary_sources(&mut chunk_rows, &binary_sources)?;
                if !crate::engine::expr::strict_dml_expression() {
                    crate::engine::api::retain_statement_warnings(|warning| {
                        !(warning.is(crate::error::catalog::ER_TRUNCATED_WRONG_VALUE)
                            && (warning
                                .message()
                                .starts_with("Truncated incorrect DOUBLE value:")
                                || warning
                                    .message()
                                    .starts_with("Truncated incorrect INTEGER value:")))
                    });
                }
                crate::engine::expr::reject_strict_dml_numeric_warnings()?;
                if !source_bindings.expressions.is_empty() {
                    for mut row in chunk_rows {
                        let width = row
                            .len()
                            .checked_sub(source_bindings.extra_columns)
                            .ok_or_else(|| {
                                error(
                                    crate::error::catalog::ER_WRONG_VALUE_COUNT_ON_ROW,
                                    "invalid INSERT SELECT source width",
                                )
                            })?;
                        let source_values: Vec<_> = source_bindings
                            .slots
                            .iter()
                            .map(|slot| {
                                let index = match *slot {
                                    InsertSelectSourceSlot::FromStart(index) => index,
                                    InsertSelectSourceSlot::FromEnd(distance) => width - distance,
                                    InsertSelectSourceSlot::Extra(index) => width + index,
                                };
                                row[index].clone()
                            })
                            .collect();
                        let row_stmt = bind_insert_select_source(
                            stmt,
                            &source_bindings.expressions,
                            &source_values,
                        );
                        row.truncate(width);
                        let mut rows = vec![row];
                        total_records += 1;
                        coerce_legacy_timestamp_nulls(
                            db,
                            &stmt.table,
                            stmt.columns.as_deref(),
                            &mut rows,
                        )?;
                        coerce_ignorable_insert_nulls(
                            db,
                            &stmt.table,
                            stmt.columns.as_deref(),
                            &mut rows,
                            stmt.ignore,
                        )?;
                        if let Some(allowed) = &partition_filter {
                            filter_insert_partition_set(db, stmt, allowed, &mut rows)?;
                        }
                        match dispatch_insert_chunk(
                            db, session, txn, &row_stmt, &ctx, rows, registry,
                        )? {
                            QueryResponse::OkAffected { affected_rows } => {
                                total_affected += affected_rows
                            }
                            QueryResponse::ResultSet(mut result) if stmt.returning.is_some() => {
                                total_affected += result.rows.len() as u64;
                                if returning_columns.is_none() {
                                    returning_columns = Some(result.columns);
                                }
                                returning_rows.append(&mut result.rows);
                            }
                            _ => {
                                return Err(error(
                                    crate::error::catalog::ER_NOT_SUPPORTED_YET,
                                    "unexpected INSERT SELECT response",
                                ))
                            }
                        }
                    }
                    return Ok(());
                }
                coerce_legacy_timestamp_nulls(
                    db,
                    &stmt.table,
                    stmt.columns.as_deref(),
                    &mut chunk_rows,
                )?;
                coerce_ignorable_insert_nulls(
                    db,
                    &stmt.table,
                    stmt.columns.as_deref(),
                    &mut chunk_rows,
                    stmt.ignore,
                )?;
                total_records = total_records.saturating_add(chunk_rows.len() as u64);
                if let Some(allowed) = &partition_filter {
                    filter_insert_partition_set(db, stmt, allowed, &mut chunk_rows)?;
                }
                let response =
                    dispatch_insert_chunk(db, session, txn, stmt, &ctx, chunk_rows, registry)?;
                match response {
                    QueryResponse::OkAffected { affected_rows } => {
                        total_affected = total_affected.saturating_add(affected_rows);
                        Ok(())
                    }
                    QueryResponse::ResultSet(mut result) if stmt.returning.is_some() => {
                        total_affected = total_affected.saturating_add(result.rows.len() as u64);
                        if returning_columns.is_none() {
                            returning_columns = Some(result.columns);
                        }
                        returning_rows.append(&mut result.rows);
                        Ok(())
                    }
                    // dispatch_insert_chunk only emits OkAffected today,
                    // but guard against future changes by surfacing the
                    // mismatch as a clear error rather than silently
                    // dropping the chunk's contribution.
                    _ => Err(error(
                        crate::error::catalog::ER_NOT_SUPPORTED_YET,
                        "INSERT SELECT chunk produced an unexpected response shape",
                    )),
                }
            },
        )?;
    }
    // first_insert_id is propagated through the thread-local set by
    // each chunk's `execute_insert_values` / `execute_insert_with_conflict`
    // — `set_statement_insert_id` keeps the *first* non-zero value,
    // matching MySQL's LAST_INSERT_ID() semantics for a multi-chunk
    // INSERT…SELECT.
    finalize_insert_info(total_records, true);
    if let Some(columns) = returning_columns {
        Ok(QueryResponse::ResultSet(ResultSet {
            columns,
            rows: returning_rows,
        }))
    } else {
        Ok(QueryResponse::OkAffected {
            affected_rows: total_affected,
        })
    }
}

/// P2 admission gate: decide whether an INSERT…SELECT can dispatch chunks to
/// a worker pool instead of the serial sink. Every listed condition removes a
/// dependency on per-chunk ordering, side-effect visibility, or the calling
/// thread's local state, so parallel dispatch is provably equivalent to the
/// serial path for the shapes that pass. Returns the parallel degree to use,
/// or `None` for shapes that must stay serial. Any doubt keeps the shape on
/// the serial path — expanding coverage is cheap; a wrong parallel admission
/// is a correctness bug.
///
/// `MYDB_INSERT_SELECT_PARALLEL=0` is a global kill-switch and differential
/// oracle. `MYDB_INSERT_SELECT_PARALLEL_MIN_DEGREE=<N>` overrides the default
/// derived from `MYDB_PARALLEL_DEGREE`.
fn insert_select_parallel_gate(
    db: &Arc<Database>,
    session: &SessionContext,
    stmt: &Insert,
    select: &crate::parser::QueryExpr,
    ctx: &InsertChunkCtx,
) -> Option<usize> {
    // Non-strict INSERT ... SELECT repairs explicit NULLs and emits
    // connection-local warnings in the serial sink.
    if !insert_strict_mode() || !crate::engine::expr::session_explicit_defaults_for_timestamp() {
        return None;
    }
    if crate::util::env::var("MYDB_INSERT_SELECT_PARALLEL")
        .map(|v| v == "0")
        .unwrap_or(false)
    {
        return None;
    }
    // Any per-row side-effect / trigger / conflict logic falls back — the
    // worker path calls `insert_rows` directly.
    if super::tx::query_has_locking_read(select) {
        return None;
    }
    if ctx.needs_trigger_path {
        return None;
    }
    if stmt.ignore || stmt.replace || stmt.on_duplicate_key_update.is_some() {
        return None;
    }
    if stmt.view_check_predicate.is_some() {
        return None;
    }
    if stmt.returning.is_some() {
        return None;
    }
    if !stmt.partitions.is_empty() {
        return None;
    }
    if stmt.insert_alias.is_some() {
        return None;
    }
    // Explicit txn: the caller expects a single sequence of writes on this
    // txn — reserved for a future round after the concurrency semantics of
    // multiple worker threads sharing an interactive txn are audited.
    if session.explicit_txn || !session.autocommit {
        return None;
    }
    // Column list requires per-row scatter into full-width slots + default
    // fill. `Database::insert_rows` doesn't do that, so restrict to the
    // no-column-list shape where the SELECT already provides every column
    // positionally.
    if stmt.columns.is_some() {
        return None;
    }
    // Non-strict sql_mode coercion (out-of-range → saturate + warning,
    // string truncation → downgrade + warning) is executed by
    // `execute_insert_values`, not by the low-level `insert_rows` used by the
    // worker path — so the parallel path is only sound in strict mode.
    if !insert_strict_mode() {
        return None;
    }
    let Ok(table) = db.table(&stmt.table) else {
        return None;
    };
    // Transaction-owned unique-key locks are reentrant across these workers.
    // A unique precheck and publication must therefore remain ordered.
    if table
        .indexes
        .iter()
        .any(|index| index.enforces_uniqueness())
        || table
            .columns
            .iter()
            .any(|column| column.invisible || column.state != crate::storage::ColumnState::Public)
    {
        return None;
    }
    if table.is_partitioned() {
        return None;
    }
    if !table.foreign_keys.is_empty() {
        return None;
    }
    if table.auto_increment_column_index().is_some() {
        return None;
    }
    // Generated columns are populated by `materialize_generated_columns_for_insert`
    // in the serial path; `insert_rows` alone doesn't run that pass.
    if table.columns.iter().any(|c| c.generated_expr.is_some()) {
        return None;
    }
    // Any CHECK constraint validation runs in the serial validator path.
    if !table.check_constraints.is_empty() {
        return None;
    }
    // Binlog record ordering across chunks is important — punt while the
    // parallel path doesn't stamp a chunk sequence number.
    if crate::binlog::enabled() {
        return None;
    }
    // Source must NOT reference the target table (Halloween). This is a
    // strict "no reference" check — a self-INSERT is safe under the current
    // serial cursor invariants but not audited under concurrent workers.
    if source_references_target(select, &stmt.table) {
        return None;
    }
    // Ceiling on worker count. Default follows the general parallel-query
    // degree; MYDB_INSERT_SELECT_PARALLEL_MIN_DEGREE overrides if set.
    let base = crate::util::env::var("MYDB_PARALLEL_DEGREE")
        .ok()
        .and_then(|s| s.parse::<usize>().ok())
        .unwrap_or(2)
        .max(1);
    let cap = crate::util::env::var("MYDB_INSERT_SELECT_PARALLEL_MIN_DEGREE")
        .ok()
        .and_then(|s| s.parse::<usize>().ok())
        .unwrap_or(base)
        .max(2);
    Some(cap)
}

/// Case-insensitive comparison of a table reference (possibly qualified) to
/// the INSERT target. Both sides are lowercased and the target's bare name
/// is derived by stripping a leading `<db>.` prefix.
fn insert_table_matches(candidate: &str, target: &str) -> bool {
    let cand = candidate.to_ascii_lowercase();
    let tgt = target.to_ascii_lowercase();
    if cand == tgt {
        return true;
    }
    let cand_bare = cand
        .rsplit_once('.')
        .map(|(_, t)| t)
        .unwrap_or(cand.as_str());
    let tgt_bare = tgt.rsplit_once('.').map(|(_, t)| t).unwrap_or(tgt.as_str());
    cand_bare == tgt_bare
}

fn source_references_target(query: &crate::parser::QueryExpr, target: &str) -> bool {
    super::dispatch::collect_statement_table_uses(query)
        .iter()
        .any(|usage| insert_table_matches(&usage.table, target))
}

fn parallel_insert_worker_error(
    first_worker_error: &std::sync::Mutex<Option<QueryError>>,
) -> QueryError {
    first_worker_error
        .lock()
        .unwrap()
        .clone()
        .unwrap_or_else(|| {
            error(
                crate::error::catalog::ER_NOT_SUPPORTED_YET,
                "INSERT SELECT parallel workers terminated early",
            )
        })
}

/// Parallel INSERT…SELECT chunk dispatcher. Consumes chunks from the streaming
/// source on the coordinator thread and hands them to a pool of insert workers
/// over an mpsc channel; each worker calls `Database::insert_rows` with the
/// shared `TxnView`. Aggregated affected / record counts come back over a
/// separate result channel so the coordinator can report totals with the same
/// semantics as the serial sink. The first worker error cancels the run.
fn execute_insert_from_select_parallel(
    db: &Arc<Database>,
    _session: &mut SessionContext,
    txn: TxnView,
    table_name: &str,
    partition_filter: Option<&[u64]>,
    select: crate::parser::QueryExpr,
    registry: &SessionRegistry,
    degree: usize,
    binary_targets: &[usize],
    target_width: usize,
) -> Result<(u64, u64), QueryError> {
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::mpsc;
    use std::thread;
    let degree = degree.max(1);
    let (input_tx, input_rx) = mpsc::sync_channel::<Vec<Vec<Value>>>(degree * 2);
    let (result_tx, result_rx) = mpsc::channel::<Result<u64, QueryError>>();
    let input_rx = std::sync::Arc::new(std::sync::Mutex::new(input_rx));
    let stop = std::sync::Arc::new(AtomicBool::new(false));
    // Preserve the exact first storage error independently of the result
    // channel. In particular, if the last input receiver disappears between
    // the worker publishing 1062 and the coordinator retrying a full queue,
    // the caller must still receive 1062 rather than a synthetic 1235.
    let first_worker_error = std::sync::Arc::new(std::sync::Mutex::new(None::<QueryError>));
    let cancellation = crate::engine::cancel::CancellationSnapshot::capture();
    // `Database::insert_rows` performs schema coercion on the worker thread.
    // Carry the caller's owned expression/session snapshot into each worker
    // so TIMESTAMP conversion and sql_mode-sensitive coercion observe the
    // same session settings as the serial INSERT path.
    let warning_sink = crate::engine::api::current_statement_warning_sink();
    let session_info = crate::engine::expr::current_session_expr_info();
    let mut handles = Vec::with_capacity(degree);
    for _ in 0..degree {
        let db_w = db.clone();
        let table_owned = table_name.to_string();
        let input_rx = input_rx.clone();
        let result_tx = result_tx.clone();
        let warning_sink = warning_sink.clone();
        let session_info = session_info.clone();
        let stop = stop.clone();
        let first_worker_error = first_worker_error.clone();
        let cancellation = cancellation.clone();
        handles.push(thread::spawn(move || {
            let _cancel_scope = cancellation.enter();
            loop {
                if stop.load(Ordering::Acquire) {
                    break;
                }
                let chunk_opt = {
                    let rx = input_rx.lock().unwrap();
                    rx.recv().ok()
                };
                let Some(chunk) = chunk_opt else {
                    break;
                };
                // A peer may have failed while this worker was blocked in
                // recv. Discard queued work instead of extending a statement
                // that is already guaranteed to roll back.
                if stop.load(Ordering::Acquire) {
                    break;
                }
                let count = chunk.len() as u64;
                let inserted = crate::engine::cancel::check_cancelled().and_then(|()| {
                    crate::engine::expr::with_session_expr_info_option(&session_info, || {
                        if let Some(sink) = &warning_sink {
                            crate::engine::api::with_statement_warning_sink(sink.clone(), || {
                                db_w.insert_rows(txn, &table_owned, chunk)
                            })
                        } else {
                            db_w.insert_rows(txn, &table_owned, chunk)
                        }
                    })
                    .map(|_| ())
                });
                match inserted {
                    Ok(()) => {
                        if result_tx.send(Ok(count)).is_err() {
                            break;
                        }
                    }
                    Err(e) => {
                        {
                            let mut first = first_worker_error.lock().unwrap();
                            if first.is_none() {
                                *first = Some(e.clone());
                            }
                        }
                        stop.store(true, Ordering::Release);
                        let _ = result_tx.send(Err(e));
                        break;
                    }
                }
            }
        }));
    }
    drop(result_tx);
    // Only workers may own an input receiver. Retaining this coordinator-side
    // Arc kept the bounded channel logically connected after every worker had
    // returned on 1062, so SyncSender::send parked forever on a full queue.
    drop(input_rx);

    let mut total_records: u64 = 0;
    let mut total_affected: u64 = 0;
    let mut coord_error: Option<QueryError> = None;
    let source_result = (|| {
        let response = match select {
            crate::parser::QueryExpr::Select(select) => execute_select(db, txn, *select, registry)?,
            query => {
                super::dispatch::execute_set_operation_branch_response(db, txn, query, registry)?
            }
        };
        validate_insert_select_width(&response, target_width)?;
        let binary_sources = insert_select_binary_sources(&response, binary_targets);
        super::select::drive_response_chunks(
            response,
            INSERT_SELECT_CHUNK_ROWS,
            |mut chunk_rows| {
                encode_insert_select_binary_sources(&mut chunk_rows, &binary_sources)?;
                if let Some(allowed) = partition_filter {
                    validate_insert_partition_set(db, table_name, allowed, &chunk_rows)?;
                }
                total_records = total_records.saturating_add(chunk_rows.len() as u64);
                let mut pending = chunk_rows;
                loop {
                    crate::engine::cancel::check_cancelled()?;
                    if stop.load(Ordering::Acquire) {
                        return Err(parallel_insert_worker_error(&first_worker_error));
                    }
                    match input_tx.try_send(pending) {
                        Ok(()) => {
                            // A fast-failing worker may already have published its
                            // result even though this enqueue succeeded. Consume
                            // every ready outcome so the source stops before it
                            // produces another full chunk.
                            loop {
                                match result_rx.try_recv() {
                                    Ok(Ok(n)) => {
                                        total_affected = total_affected.saturating_add(n);
                                    }
                                    Ok(Err(e)) => {
                                        stop.store(true, Ordering::Release);
                                        return Err(e);
                                    }
                                    Err(mpsc::TryRecvError::Empty) => return Ok(()),
                                    Err(mpsc::TryRecvError::Disconnected) => {
                                        stop.store(true, Ordering::Release);
                                        return Err(parallel_insert_worker_error(
                                            &first_worker_error,
                                        ));
                                    }
                                }
                            }
                        }
                        Err(mpsc::TrySendError::Full(chunk)) => {
                            pending = chunk;
                            // Do not park in SyncSender::send: while the queue is
                            // full, wait for either worker progress/error or a
                            // short cancellation polling interval.
                            match result_rx.recv_timeout(std::time::Duration::from_millis(10)) {
                                Ok(Ok(n)) => {
                                    total_affected = total_affected.saturating_add(n);
                                }
                                Ok(Err(e)) => {
                                    stop.store(true, Ordering::Release);
                                    return Err(e);
                                }
                                Err(mpsc::RecvTimeoutError::Timeout) => {}
                                Err(mpsc::RecvTimeoutError::Disconnected) => {
                                    stop.store(true, Ordering::Release);
                                    return Err(parallel_insert_worker_error(&first_worker_error));
                                }
                            }
                        }
                        Err(mpsc::TrySendError::Disconnected(_)) => {
                            stop.store(true, Ordering::Release);
                            return Err(parallel_insert_worker_error(&first_worker_error));
                        }
                    }
                }
            },
        )
    })();
    if let Err(e) = &source_result {
        coord_error.get_or_insert_with(|| e.clone());
        stop.store(true, Ordering::Release);
    }
    drop(input_tx);

    for outcome in result_rx.iter() {
        match outcome {
            Ok(n) => total_affected = total_affected.saturating_add(n),
            Err(e) => {
                if coord_error.is_none() {
                    coord_error = Some(e);
                }
            }
        }
    }
    for h in handles {
        let _ = h.join();
    }
    if let Some(e) = coord_error {
        return Err(e);
    }
    Ok((total_affected, total_records))
}

fn validate_insert_view_check(
    db: &Arc<Database>,
    table_name: &str,
    columns: Option<&[String]>,
    rows: &[Vec<Value>],
    predicate: &Expr,
) -> Result<(), QueryError> {
    let table = db.table(table_name)?.clone();
    let column_indexes = insert_column_indexes(&table, columns)?;
    for row in rows {
        crate::engine::cancel::check_cancelled()?;
        if row.len() != column_indexes.len() {
            return Err(error(
                crate::error::catalog::ER_WRONG_VALUE_COUNT_ON_ROW,
                "column count does not match value count",
            ));
        }
        let mut values = table
            .columns
            .iter()
            .map(resolve_column_default)
            .collect::<Vec<_>>();
        for (idx, value) in column_indexes.iter().copied().zip(row.iter()) {
            values[idx] = value.clone();
        }
        db.materialize_generated_columns_for_insert(&table, &mut values)?;
        if !eval_bool(predicate, &table, &values)? {
            return Err(error(
                crate::error::catalog::ER_VIEW_CHECK_FAILED,
                "CHECK OPTION failed for view",
            ));
        }
    }
    Ok(())
}

fn eval_insert_value_rows(
    db: &Arc<Database>,
    table_name: &str,
    columns: Option<&[String]>,
    expr_rows: Vec<Vec<Expr>>,
) -> Result<Vec<Vec<Value>>, QueryError> {
    eval_insert_value_rows_impl::<false, _>(
        db,
        table_name,
        columns,
        expr_rows,
        |expr, table, column, current, _| match expr {
            Expr::Literal(lit) => Ok(insert_literal_value(lit, &table.columns[column].data_type)),
            Expr::BoundValue(value) => Ok(value),
            other => eval_insert_default_bound(&other, table, column, current),
        },
    )
}

#[cfg(test)]
pub(super) static SIMPLE_INSERT_VALUE_ROWS: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);

fn eval_insert_value_rows_borrowed(
    db: &Arc<Database>,
    table_name: &str,
    columns: Option<&[String]>,
    expr_rows: &[Vec<Expr>],
) -> Result<Vec<Vec<Value>>, QueryError> {
    eval_insert_value_rows_impl::<true, _>(
        db,
        table_name,
        columns,
        expr_rows,
        |expr, table, column, current, simple| match expr {
            Expr::Literal(lit) => Ok(insert_literal_value(
                lit.clone(),
                &table.columns[column].data_type,
            )),
            Expr::BoundValue(value) => Ok(value.clone()),
            Expr::Placeholder(index) if simple => crate::engine::params::current_param(*index)
                .ok_or_else(|| {
                    error(
                        crate::error::catalog::ER_WRONG_ARGUMENTS,
                        "parameter is not bound",
                    )
                }),
            other => eval_insert_default_bound(other, table, column, current),
        },
    )
}

fn eval_insert_default_bound(
    expr: &Expr,
    table: &TableSchema,
    column: usize,
    current: &[Value],
) -> Result<Value, QueryError> {
    let bound = crate::engine::expr::bind_column_defaults(expr, table, Some(column))?;
    crate::engine::expr::eval_expr(&bound, table, current)
}

/// One validation/evaluation order for owned and prepared VALUES. The static
/// evaluator moves owned literal payloads and clones only borrowed payloads;
/// complex rows still see defaults and earlier assignments left to right.
fn eval_insert_value_rows_impl<const BORROWED: bool, R>(
    db: &Arc<Database>,
    table_name: &str,
    columns: Option<&[String]>,
    expr_rows: impl IntoIterator<Item = R>,
    mut evaluate: impl FnMut(R::Item, &TableSchema, usize, &[Value], bool) -> Result<Value, QueryError>,
) -> Result<Vec<Vec<Value>>, QueryError>
where
    R: AsRef<[Expr]> + IntoIterator,
    R::Item: std::borrow::Borrow<Expr>,
{
    use std::borrow::Borrow;
    let table = db.table(table_name)?.clone();
    let column_indexes = insert_column_indexes(&table, columns)?;
    let has_binary_target = column_indexes.iter().any(|&column| {
        matches!(
            table.columns[column].data_type,
            DataTypeKind::Binary { .. }
                | DataTypeKind::VarBinary { .. }
                | DataTypeKind::Blob { .. }
        )
    });
    let has_generated_target = column_indexes
        .iter()
        .any(|&column| table.columns[column].generated_expr.is_some());
    let expr_rows = expr_rows.into_iter();
    let mut rows = Vec::with_capacity(expr_rows.size_hint().0);
    for row in expr_rows {
        crate::engine::cancel::check_cancelled()?;
        let expressions = row.as_ref();
        if expressions.len() != column_indexes.len() {
            return Err(error(
                crate::error::catalog::ER_WRONG_VALUE_COUNT_ON_ROW,
                "column count does not match value count",
            ));
        }
        let simple = !has_generated_target
            && expressions.iter().all(|expr| {
                matches!(expr, Expr::Literal(_) | Expr::BoundValue(_))
                    || (BORROWED && matches!(expr, Expr::Placeholder(_)))
            });
        #[cfg(test)]
        if BORROWED && simple {
            SIMPLE_INSERT_VALUE_ROWS.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        }
        // These expressions cannot contain DEFAULT, generated assignments or
        // references to earlier columns. Character literals also need no
        // expression-charset resolution unless the destination is binary.
        if simple && !has_binary_target {
            rows.push(
                row.into_iter()
                    .zip(column_indexes.iter().copied())
                    .map(|(expr, column)| evaluate(expr, &table, column, &[], true))
                    .collect::<Result<Vec<Value>, QueryError>>()?,
            );
            continue;
        }
        let mut current = if simple {
            Vec::new()
        } else {
            table
                .columns
                .iter()
                .map(crate::engine::expr::resolve_column_default_value)
                .collect::<Vec<_>>()
        };
        // Collect directly from the owned expression iterator. Its allocation
        // can become the value vector, avoiding a second per-row buffer. The
        // borrowed prepared-statement iterator retains ordinary collection.
        let values = row
            .into_iter()
            .zip(column_indexes.iter().copied())
            .map(|(expr, column)| {
                warn_explicit_lob_default_without_value(expr.borrow(), &table.columns[column]);
                if !simple && table.columns[column].generated_expr.is_some() {
                    if !expr_is_default_for_column(expr.borrow(), &table.columns[column].name) {
                        return Err(generated_column_assignment_error());
                    }
                    return Ok(Value::Null);
                }
                let source_charset = matches!(
                    table.columns[column].data_type,
                    DataTypeKind::Binary { .. }
                        | DataTypeKind::VarBinary { .. }
                        | DataTypeKind::Blob { .. }
                )
                .then(|| {
                    crate::engine::expr::expr_collation(expr.borrow(), &table)
                        .map_or_else(crate::engine::expr::connection_charset, |c| c.charset)
                });
                let mut value = evaluate(expr, &table, column, &current, simple)?;
                if let Some(source_charset) = source_charset {
                    value =
                        crate::engine::expr::encode_binary_assignment_value(value, source_charset)?;
                }
                if !simple {
                    current[column] = value.clone();
                }
                Ok(value)
            })
            .collect::<Result<Vec<Value>, QueryError>>()?;
        rows.push(values);
    }
    Ok(rows)
}

#[cfg(test)]
#[test]
fn owned_literal_row_collection_reuses_compatible_buffer() {
    std::fs::create_dir_all("/tmp/mydb").unwrap();
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let engine = StorageQueryEngine::open(dir.path()).unwrap();
    engine
        .execute("CREATE TABLE reuse_rows (a BIGINT, b VARCHAR(32), c INT)")
        .unwrap();
    let expr_rows = vec![vec![
        Expr::Literal(crate::parser::Literal::Int(7)),
        Expr::Literal(crate::parser::Literal::String("seven".into())),
        Expr::Literal(crate::parser::Literal::Int(8)),
    ]];
    let original = expr_rows[0].as_ptr() as usize;
    let rows =
        eval_insert_value_rows(engine.database(), "test.reuse_rows", None, expr_rows).unwrap();
    assert_eq!(
        rows,
        vec![vec![
            Value::Int(7),
            Value::String("seven".into()),
            Value::Int(8)
        ]]
    );
    if std::mem::size_of::<Expr>() * 3 % std::mem::size_of::<Value>() == 0
        && std::mem::align_of::<Expr>() == std::mem::align_of::<Value>()
    {
        assert_eq!(original, rows[0].as_ptr() as usize);
    }
}

/// Evaluate a literal in assignment context without erasing the distinction
/// between a contextual hex/bit literal and an ordinary binary value.
///
/// The runtime `Value::Bytes` lane is also used by BINARY/BLOB columns and
/// prepared byte parameters, so storage coercion cannot safely interpret
/// every byte buffer as a big-endian number. At the INSERT AST boundary the
/// provenance is still available: for ordinary numeric destinations MySQL
/// treats a hex/bit literal of at most 64 bits as an unsigned integer;
/// string/binary destinations retain the original bytes. BIT is deliberately
/// excluded from that numeric conversion because an over-width `b'…'` uses
/// the binary-string 1406 diagnostic, unlike an out-of-range integer's 1264.
/// This also keeps prepared `?` bytes and `INSERT ... SELECT binary_column`
/// byte-exact.
pub(super) fn insert_literal_value(lit: crate::parser::Literal, target: &DataTypeKind) -> Value {
    let numeric_target = matches!(
        target,
        DataTypeKind::TinyInt
            | DataTypeKind::SmallInt
            | DataTypeKind::MediumInt
            | DataTypeKind::Int
            | DataTypeKind::BigInt
            | DataTypeKind::Bool
            | DataTypeKind::Float { .. }
            | DataTypeKind::Double { .. }
            | DataTypeKind::Decimal { .. }
            | DataTypeKind::Year
    );
    let value = crate::engine::expr::literal_into_value(lit);
    if numeric_target {
        crate::engine::expr::coerce_hex_bytes_to_uint(value)
    } else {
        value
    }
}

/// MySQL INSERT IGNORE swallows a fixed set of *data* errors and
/// downgrades them to warnings (the affected row is skipped, the
/// statement keeps going). This is the subset of error codes we
/// recognise — picked to match MySQL 8.0's docs:
/// - 1048: column 'x' cannot be null
/// - 1062: duplicate entry for key (handled separately below because
///         it also drives the REPLACE / ON DUP KEY UPDATE branches)
/// - 1264: out-of-range value for column
/// - 1265: data truncated for column
/// - 1366: incorrect value for column
/// - 1452: cannot add or update a child row: FK constraint fails
/// - 3819: check constraint violated
///
/// Constraint errors that *can't* be safely ignored (1064 syntax,
/// 1146 table-missing, 1062's REPLACE handling above, lock-wait
/// timeouts, deadlocks, etc.) still propagate.
fn ignore_data_error(error: &QueryError) -> bool {
    // E3: partition-routing errors (1526 = no partition for value,
    // 1748 = row doesn't match PARTITION clause) are downgraded to
    // warnings under INSERT IGNORE by MySQL 8.0. Keep in sync with
    // `storage::ignorable_update_error` for the UPDATE IGNORE side.
    error.is_any(&[
        crate::error::catalog::ER_BAD_NULL_ERROR,
        crate::error::catalog::ER_WARN_DATA_OUT_OF_RANGE,
        crate::error::catalog::WARN_DATA_TRUNCATED,
        crate::error::catalog::ER_TRUNCATED_WRONG_VALUE_FOR_FIELD,
        crate::error::catalog::ER_NO_REFERENCED_ROW_2,
        crate::error::catalog::ER_NO_PARTITION_FOR_GIVEN_VALUE,
        crate::error::catalog::ER_ROW_DOES_NOT_MATCH_GIVEN_PARTITION_SET,
        crate::error::catalog::ER_VIEW_CHECK_FAILED,
        crate::error::catalog::ER_CHECK_CONSTRAINT_VIOLATED,
    ])
}

/// Record a swallowed-by-IGNORE error: surface a SQL warning to the
/// client and bump `dml_ignore_warnings` for SHOW ENGINE MYDB STATUS.
fn record_ignore_warning(db: &Arc<Database>, err: &QueryError) {
    crate::engine::api::push_statement_condition(crate::error::ConditionLevel::Warning, &err);
    db.dml_stats().record_ignore_warning();
}

/// `MYDB_REPLACE_PROBE_FIRST` (default on). When on, REPLACE probes the
/// unique indexes up-front and deletes conflicts before a single INSERT,
/// instead of speculatively inserting and catching 1062. Probing first avoids
/// burning an AUTO_INCREMENT value and a wasted heap write on every conflicting
/// REPLACE. `=0` restores the pure speculative path as a differential oracle.
pub(super) fn replace_probe_first() -> bool {
    match crate::util::env::var("MYDB_REPLACE_PROBE_FIRST") {
        Ok(v) => !matches!(v.trim(), "0" | "off" | "false" | "no"),
        Err(_) => true,
    }
}

/// Bound on the probe→delete→insert retry loop. A retry only happens when a
/// concurrent transaction inserts a *new* conflicting row between our probe and
/// our INSERT; the unique-key locks taken while deleting make this rare, so a
/// small bound is ample. Exceeding it surfaces the underlying 1062.
pub(super) const REPLACE_MAX_RETRY: u32 = 8;

/// Pin the partition-router prune set to a REPLACE row's routed partition for
/// the duration of its conflict handling (returns `None` for non-partitioned
/// tables). Per-partition RowIds are ambiguous across partitions — every
/// partition numbers its rows from the same origin — so the unique-index probe
/// (`find_all_conflicting_row_ids`) and the subsequent `delete_row_by_id`
/// (`locate_row_storage_id`) must both resolve within the *same* single
/// partition. Without this, a conflict in partition N can make the delete land
/// on an identically-numbered row in partition 0 (silent wrong-row delete +
/// duplicate insert). A partitioned unique key always includes the partition
/// column, so every conflict for this row lives in its one routed partition.
pub(super) fn replace_partition_guard(
    table: &TableSchema,
    values: &[Value],
) -> Result<Option<crate::engine::executor::partition_router::PrunedPartitionsGuard>, QueryError> {
    if !table.is_partitioned() {
        return Ok(None);
    }
    let routed = table.route_row_to_partition(values)?;
    Ok(Some(
        crate::engine::executor::partition_router::push_pruned_partitions(vec![routed.storage_id]),
    ))
}

const BATCH_ODKU_MIN_ROWS: usize = 64;
// Keep a normal replication batch together. Splitting a 10K-row,
// conflict-free statement at 4096 made storage visit every secondary index
// three times. Under a small buffer pool the first pass's dirty leaves were
// commonly flushed and evicted before the second pass returned to that index,
// turning an executor safety chunk into avoidable read/write amplification.
//
// 16K remains a hard, workload-independent bound for unusually large SQL
// statements. Storage builds one index-entry vector at a time, so raising this
// bound does not multiply the batch by the table's index count.
const BATCH_ODKU_MAX_ROWS: usize = 16 * 1024;
const BATCH_ODKU_MAX_LOW_YIELD_PROBES: usize = 2;

fn batch_odku_enabled() -> bool {
    match crate::util::env::var("MYDB_BATCH_ODKU") {
        Ok(value) => !matches!(
            value.trim().to_ascii_lowercase().as_str(),
            "0" | "off" | "false" | "no"
        ),
        Err(_) => true,
    }
}

fn validate_on_duplicate_assignments(
    table: &TableSchema,
    on_duplicate: Option<&[crate::parser::Assignment]>,
) -> Result<(), QueryError> {
    let Some(updates) = on_duplicate else {
        return Ok(());
    };
    // A generated column may only be assigned DEFAULT. Validate before the
    // batch fast path: a conflict-free batch must not accidentally bypass the
    // prepare-time MySQL 3105 error.
    for assignment in updates {
        let idx = table.column_index(&assignment.column)?;
        if table.columns[idx].generated_expr.is_some()
            && !expr_is_default_for_column(&assignment.value, &table.columns[idx].name)
        {
            return Err(generated_column_assignment_error());
        }
    }
    Ok(())
}

// Do not reserve generated IDs ahead of conflicts: ODKU may update the
// allocator column and later rows must observe that in source order.
fn batch_has_explicit_auto_ids(
    table: &TableSchema,
    columns: Option<&[String]>,
    rows: &[Vec<Value>],
) -> bool {
    table
        .columns
        .iter()
        .enumerate()
        .filter(|(_, c)| c.auto_increment)
        .all(|(index, column)| {
            let pos = columns.map_or(Some(index), |cols| {
                cols.iter()
                    .position(|c| c.eq_ignore_ascii_case(&column.name))
            });
            pos.is_some_and(|pos| {
                rows.iter().all(|row| {
                    row.get(pos).is_some_and(|v| match v {
                        Value::Int(n) => *n > 0,
                        Value::UInt(n) => *n > 0,
                        Value::String(s) => s.parse::<u64>().is_ok_and(|n| n > 0),
                        _ => false,
                    })
                })
            })
        })
}

fn can_use_batch_odku(
    table: &TableSchema,
    row_count: usize,
    ignore: bool,
    replace: bool,
    on_duplicate: Option<&[crate::parser::Assignment]>,
    returning_clause: Option<&crate::parser::ReturningClause>,
) -> bool {
    batch_odku_enabled()
        && row_count >= BATCH_ODKU_MIN_ROWS
        && on_duplicate.is_some()
        && !ignore
        && !replace
        && returning_clause.is_none()
        && table.foreign_keys.is_empty()
}

#[allow(clippy::too_many_arguments)]
fn execute_batch_odku(
    db: &Arc<Database>,
    txn: TxnView,
    table: &TableSchema,
    table_name: &str,
    columns: Option<&[String]>,
    source_rows: Vec<Vec<Value>>,
    updates: &[crate::parser::Assignment],
    insert_alias: Option<&crate::parser::InsertAlias>,
    view_check_predicate: Option<&Expr>,
    target_alias: Option<&str>,
) -> Result<QueryResponse, QueryError> {
    let column_indexes = insert_column_indexes(table, columns)?;
    let missing_required = missing_required_columns(table, &column_indexes);
    let total_rows = source_rows.len();
    let mut full_rows = Vec::with_capacity(source_rows.len());
    let full_width_identity = table.visible_column_count() == table.columns.len()
        && column_indexes.len() == table.columns.len()
        && column_indexes
            .iter()
            .enumerate()
            .all(|(expected, actual)| expected == *actual);
    let row_defaults = row_default_plans(&table, &column_indexes)?;
    for row in source_rows {
        crate::engine::cancel::check_cancelled()?;
        if row.len() != column_indexes.len() {
            return Err(error(
                crate::error::catalog::ER_WRONG_VALUE_COUNT_ON_ROW,
                "column count does not match value count",
            ));
        }
        if full_width_identity {
            full_rows.push(row);
            continue;
        }
        let mut values = table
            .columns
            .iter()
            .map(resolve_column_default)
            .collect::<Vec<_>>();
        for (idx, value) in column_indexes.iter().copied().zip(row) {
            values[idx] = value;
        }
        materialize_row_defaults(db, &table, &mut values, &row_defaults)?;
        enforce_missing_required_columns(table, &mut values, &missing_required, false)?;
        full_rows.push(values);
    }

    let prepared = db.prepare_insert_rows_for_batch(txn, table_name, full_rows)?;
    debug_assert_eq!(prepared.table_id, table.id);
    db.dml_stats().record_upsert_batch_attempt();
    crate::engine::api::record_statement_odku_batch_attempt();

    let mut ids = InsertStatementIds::default();
    let mut affected = 0u64;
    let mut batch_inserted = 0u64;
    let mut cursor = 0usize;
    let mut low_yield_probes = 0usize;
    while cursor < total_rows {
        crate::engine::cancel::check_cancelled()?;
        let remaining = total_rows - cursor;
        if remaining < BATCH_ODKU_MIN_ROWS || low_yield_probes >= BATCH_ODKU_MAX_LOW_YIELD_PROBES {
            let fallback = prepared.rows[cursor..].to_vec();
            db.dml_stats()
                .record_upsert_batch_fallback_rows(fallback.len() as u64);
            crate::engine::api::record_statement_odku_legacy_rows(fallback.len() as u64);
            affected += legacy_odku_affected(execute_insert_rows_with_ids::<false>(
                db,
                None,
                txn,
                table_name,
                columns,
                fallback,
                false,
                false,
                Some(updates),
                insert_alias,
                view_check_predicate,
                None,
                target_alias,
                true,
                &mut ids,
            )?)?;
            break;
        }

        let chunk_end = (cursor + BATCH_ODKU_MAX_ROWS).min(total_rows);
        let conflict =
            db.first_unique_conflict_ordinal(table, txn, &prepared.rows[cursor..chunk_end])?;
        let batch_len = conflict.unwrap_or(chunk_end - cursor);

        if batch_len >= BATCH_ODKU_MIN_ROWS {
            let savepoint = db.statement_savepoint(txn.txn_id)?;
            let batch_rows = prepared.rows[cursor..cursor + batch_len].to_vec();
            match db.insert_prepared_rows_detailed(txn, table_name, batch_rows) {
                Ok(_) => {
                    ids.last_copied = auto_increment_row_id(
                        &prepared.rows[cursor + batch_len - 1],
                        table.auto_increment_column_index(),
                    );
                    affected += batch_len as u64;
                    batch_inserted += batch_len as u64;
                    cursor += batch_len;
                    low_yield_probes = 0;
                    db.dml_stats().record_upsert_batch_rows(batch_len as u64);
                    crate::engine::api::record_statement_odku_batched_rows(batch_len as u64);
                }
                Err(err) if err.is(crate::error::catalog::ER_DUP_ENTRY) => {
                    db.rollback_to_statement_savepoint(txn.txn_id, savepoint)?;
                    db.dml_stats().record_upsert_batch_race_rollback();
                    crate::engine::api::record_statement_odku_race_rollback();
                    let fallback = prepared.rows[cursor..].to_vec();
                    db.dml_stats()
                        .record_upsert_batch_fallback_rows(fallback.len() as u64);
                    crate::engine::api::record_statement_odku_legacy_rows(fallback.len() as u64);
                    affected += legacy_odku_affected(execute_insert_rows_with_ids::<false>(
                        db,
                        None,
                        txn,
                        table_name,
                        columns,
                        fallback,
                        false,
                        false,
                        Some(updates),
                        insert_alias,
                        view_check_predicate,
                        None,
                        target_alias,
                        true,
                        &mut ids,
                    )?)?;
                    break;
                }
                Err(err) => return Err(err),
            }
        } else {
            low_yield_probes += 1;
        }

        let Some(conflict_ordinal) = conflict else {
            continue;
        };
        let rows_through_conflict = if batch_len >= BATCH_ODKU_MIN_ROWS {
            1
        } else {
            conflict_ordinal + 1
        };
        let end = (cursor + rows_through_conflict).min(total_rows);
        let fallback = prepared.rows[cursor..end].to_vec();
        db.dml_stats().record_upsert_batch_conflict_row();
        db.dml_stats()
            .record_upsert_batch_fallback_rows(fallback.len() as u64);
        crate::engine::api::record_statement_odku_conflict_row();
        crate::engine::api::record_statement_odku_legacy_rows(fallback.len() as u64);
        affected += legacy_odku_affected(execute_insert_rows_with_ids::<false>(
            db,
            None,
            txn,
            table_name,
            columns,
            fallback,
            false,
            false,
            Some(updates),
            insert_alias,
            view_check_predicate,
            None,
            target_alias,
            true,
            &mut ids,
        )?)?;
        cursor = end;
    }

    let matched = crate::engine::api::statement_matched_rows().unwrap_or(0);
    crate::engine::api::set_statement_matched_rows(matched + batch_inserted);
    crate::engine::api::set_statement_insert_id(ids.last_copied);
    crate::engine::api::set_statement_ok_insert_id(ids.last_updated);
    Ok(QueryResponse::OkAffected {
        affected_rows: affected,
    })
}

fn legacy_odku_affected(response: QueryResponse) -> Result<u64, QueryError> {
    match response {
        QueryResponse::OkAffected { affected_rows } => Ok(affected_rows),
        QueryResponse::Ok => Ok(0),
        _ => Err(error(
            crate::error::catalog::ER_UNKNOWN_ERROR,
            "internal batch ODKU fallback returned an unexpected result shape",
        )),
    }
}

#[allow(clippy::too_many_arguments)]
pub(super) fn execute_insert_with_conflict(
    db: &Arc<Database>,
    txn: TxnView,
    table_name: &str,
    columns: Option<&[String]>,
    source_rows: Vec<Vec<Value>>,
    ignore: bool,
    replace: bool,
    on_duplicate: Option<&[crate::parser::Assignment]>,
    insert_alias: Option<&crate::parser::InsertAlias>,
    view_check_predicate: Option<&Expr>,
    returning_clause: Option<&crate::parser::ReturningClause>,
    target_alias: Option<&str>,
) -> Result<QueryResponse, QueryError> {
    let table = db.table(table_name)?.clone();
    if let Some(updates) = on_duplicate {
        for assignment in updates {
            super::dml::validate_assignment_qualifier(assignment, table_name, target_alias)?;
        }
    }
    validate_on_duplicate_assignments(&table, on_duplicate)?;
    if on_duplicate.is_some() {
        crate::engine::api::record_statement_odku_input(source_rows.len() as u64);
    }
    if can_use_batch_odku(
        &table,
        source_rows.len(),
        ignore,
        replace,
        on_duplicate,
        returning_clause,
    ) && batch_has_explicit_auto_ids(&table, columns, &source_rows)
    {
        return execute_batch_odku(
            db,
            txn,
            &table,
            table_name,
            columns,
            source_rows,
            on_duplicate.expect("batch ODKU requires assignments"),
            insert_alias,
            view_check_predicate,
            target_alias,
        );
    }
    if on_duplicate.is_some() && source_rows.len() >= BATCH_ODKU_MIN_ROWS {
        db.dml_stats()
            .record_upsert_batch_fallback_rows(source_rows.len() as u64);
        crate::engine::api::record_statement_odku_legacy_rows(source_rows.len() as u64);
    }
    execute_insert_with_conflict_legacy(
        db,
        txn,
        table_name,
        columns,
        source_rows,
        ignore,
        replace,
        on_duplicate,
        insert_alias,
        view_check_predicate,
        returning_clause,
        target_alias,
        false,
    )
}

/// The row-at-a-time fallback is shared by triggered INSERT and batch ODKU.
/// Plain/batch insert and prepared writes retain their specialized entry points.
#[allow(clippy::too_many_arguments)]
fn execute_insert_with_conflict_legacy(
    db: &Arc<Database>,
    txn: TxnView,
    table_name: &str,
    columns: Option<&[String]>,
    source_rows: Vec<Vec<Value>>,
    ignore: bool,
    replace: bool,
    on_duplicate: Option<&[crate::parser::Assignment]>,
    insert_alias: Option<&crate::parser::InsertAlias>,
    view_check_predicate: Option<&Expr>,
    returning_clause: Option<&crate::parser::ReturningClause>,
    target_alias: Option<&str>,
    rows_prepared: bool,
) -> Result<QueryResponse, QueryError> {
    execute_insert_rows::<false>(
        db,
        None,
        txn,
        table_name,
        columns,
        source_rows,
        ignore,
        replace,
        on_duplicate,
        insert_alias,
        view_check_predicate,
        returning_clause,
        target_alias,
        rows_prepared,
    )
}

pub(super) struct InsertTriggers<'a> {
    pub session: &'a mut SessionContext,
    pub registry: &'a SessionRegistry,
}

#[allow(clippy::too_many_arguments)]
pub(super) fn execute_insert_rows<const TRIGGERS: bool>(
    db: &Arc<Database>,
    hooks: Option<InsertTriggers<'_>>,
    txn: TxnView,
    table_name: &str,
    columns: Option<&[String]>,
    source_rows: Vec<Vec<Value>>,
    ignore: bool,
    replace: bool,
    on_duplicate: Option<&[crate::parser::Assignment]>,
    insert_alias: Option<&crate::parser::InsertAlias>,
    view_check_predicate: Option<&Expr>,
    returning_clause: Option<&crate::parser::ReturningClause>,
    target_alias: Option<&str>,
    rows_prepared: bool,
) -> Result<QueryResponse, QueryError> {
    let mut ids = InsertStatementIds::default();
    let result = execute_insert_rows_with_ids::<TRIGGERS>(
        db,
        hooks,
        txn,
        table_name,
        columns,
        source_rows,
        ignore,
        replace,
        on_duplicate,
        insert_alias,
        view_check_predicate,
        returning_clause,
        target_alias,
        rows_prepared,
        &mut ids,
    )?;
    crate::engine::api::set_statement_insert_id(ids.last_copied);
    crate::engine::api::set_statement_ok_insert_id(ids.last_updated);
    Ok(result)
}

#[derive(Default)]
struct InsertStatementIds {
    first_generated: u64,
    last_copied: u64,
    last_updated: u64,
}

#[allow(clippy::too_many_arguments)]
fn execute_insert_rows_with_ids<const TRIGGERS: bool>(
    db: &Arc<Database>,
    mut hooks: Option<InsertTriggers<'_>>,
    txn: TxnView,
    table_name: &str,
    columns: Option<&[String]>,
    source_rows: Vec<Vec<Value>>,
    ignore: bool,
    replace: bool,
    on_duplicate: Option<&[crate::parser::Assignment]>,
    insert_alias: Option<&crate::parser::InsertAlias>,
    view_check_predicate: Option<&Expr>,
    returning_clause: Option<&crate::parser::ReturningClause>,
    target_alias: Option<&str>,
    rows_prepared: bool,
    ids: &mut InsertStatementIds,
) -> Result<QueryResponse, QueryError> {
    let table = db.table(table_name)?.clone();
    let mut returning = super::returning::ReturningCollector::new(
        returning_clause,
        &table,
        super::returning::DefaultImage::New,
        target_alias,
    )?;
    let column_indexes = if rows_prepared && insert_alias.is_none() {
        (0..table.columns.len()).collect()
    } else {
        insert_column_indexes(&table, columns)?
    };
    let alias_binding = build_insert_alias_binding(&table, &column_indexes, insert_alias)?;
    let missing = missing_required_columns(&table, &column_indexes);
    let has_before = TRIGGERS
        && !db
            .triggers_for_table_event(
                &table.qualified_name(),
                TriggerEventSchema::Insert,
                TriggerTimingSchema::Before,
            )
            .is_empty();
    let mut affected = 0u64;
    let mut unchanged_duplicates = 0u64;
    // Nested trigger statements replace these TLS counters. Accumulate locally,
    // including previous INSERT SELECT chunks, and publish only the outer totals.
    let mut duplicates = crate::engine::api::take_insert_duplicates();
    let previous_matched = crate::engine::api::statement_matched_rows().unwrap_or(0);
    let probe_first = replace && replace_probe_first();
    let needs_image = TRIGGERS || returning_clause.is_some();
    let row_defaults = row_default_plans(&table, &column_indexes)?;
    for row in source_rows {
        crate::engine::cancel::check_cancelled()?;
        let expected = if rows_prepared {
            table.columns.len()
        } else {
            column_indexes.len()
        };
        if row.len() != expected {
            return Err(error(
                crate::error::catalog::ER_WRONG_VALUE_COUNT_ON_ROW,
                "column count does not match value count",
            ));
        }
        let mut values = if rows_prepared {
            row
        } else {
            let mut values = table
                .columns
                .iter()
                .map(|column| {
                    if column.state == crate::storage::ColumnState::Dropped {
                        Value::Null
                    } else {
                        resolve_column_default(column)
                    }
                })
                .collect::<Vec<_>>();
            for (idx, value) in column_indexes.iter().zip(row) {
                values[*idx] = value;
            }
            materialize_row_defaults(db, &table, &mut values, &row_defaults)?;
            values
        };
        if TRIGGERS {
            let hook = hooks.as_mut().expect("trigger context");
            let defer_missing = has_before && hook.session.is_strict_sql_mode() && !ignore;
            if !defer_missing {
                enforce_missing_required_columns(&table, &mut values, &missing, ignore)?;
            }
            values = fire_before_insert(db, hook.session, &table, values, hook.registry)?;
            if defer_missing {
                let unresolved = missing
                    .iter()
                    .copied()
                    .filter(|&i| matches!(values[i], Value::Null))
                    .collect::<Vec<_>>();
                enforce_missing_required_columns(&table, &mut values, &unresolved, ignore)?;
            }
        } else if !rows_prepared {
            enforce_missing_required_columns(&table, &mut values, &missing, ignore)?;
            db.materialize_generated_columns_for_insert(&table, &mut values)?;
        }
        // LOAD general rows carry a full-width layout but can still contain
        // strings. Conflict probes must see the same typed keys as storage,
        // including when only DELETE triggers activated this path.
        if on_duplicate.is_some() || replace {
            db.coerce_row_for_load(&table, &mut values)?;
            db.materialize_generated_columns_for_insert(&table, &mut values)?;
        }
        // Keep the speculative INSERT on triggered ODKU: it participates in
        // AUTO_INCREMENT allocation. The ordinary probe-first optimization stays.
        if !TRIGGERS && on_duplicate.is_some() && !replace && !ignore {
            let _partition = replace_partition_guard(&table, &values)?;
            if let Some(row_id) = find_conflicting_row_id(db, txn, &table, &values)? {
                db.dml_stats().record_upsert_fast_path_hit();
                let (changed, updated_id) = apply_insert_duplicate::<TRIGGERS>(
                    db,
                    &mut hooks,
                    txn,
                    &table,
                    row_id,
                    &values,
                    on_duplicate.unwrap(),
                    alias_binding.as_ref(),
                    view_check_predicate,
                    needs_image,
                    &mut returning,
                )?;
                if changed {
                    ids.last_updated = updated_id;
                }
                duplicates += 1;
                affected += if changed { 2 } else { 0 };
                unchanged_duplicates += u64::from(!changed);
                continue;
            }
        }
        // Preserve the existing IGNORE boundary: BEFORE INSERT failures propagate.
        let row_savepoint = if ignore && txn.txn_id != 0 {
            Some(db.statement_savepoint(txn.txn_id)?)
        } else {
            None
        };
        let mut deleted = 0u64;
        let insertion = if probe_first {
            replace_insert_row::<TRIGGERS>(db, &mut hooks, txn, &table, table_name, &values, true)
                .map(|(inserted, count)| {
                    deleted = count;
                    inserted
                })
        } else {
            db.insert_rows_detailed(txn, table_name, vec![values.clone()])
        };
        let inserted = match insertion {
            Ok(inserted) => inserted,
            Err(err) if err.is(crate::error::catalog::ER_DUP_ENTRY) && !probe_first => {
                if let Some(updates) = on_duplicate {
                    let _partition = replace_partition_guard(&table, &values)?;
                    let Some(row_id) = find_conflicting_row_id(db, txn, &table, &values)? else {
                        // Never silently lose a source row when a conflict vanished.
                        return Err(err);
                    };
                    let (changed, updated_id) = apply_insert_duplicate::<TRIGGERS>(
                        db,
                        &mut hooks,
                        txn,
                        &table,
                        row_id,
                        &values,
                        updates,
                        alias_binding.as_ref(),
                        view_check_predicate,
                        needs_image,
                        &mut returning,
                    )?;
                    if changed {
                        ids.last_updated = updated_id;
                    }
                    duplicates += 1;
                    affected += if changed { 2 } else { 0 };
                    unchanged_duplicates += u64::from(!changed);
                    continue;
                } else if replace {
                    let (inserted, count) = replace_insert_row::<TRIGGERS>(
                        db, &mut hooks, txn, &table, table_name, &values, false,
                    )?;
                    deleted = count;
                    inserted
                } else if ignore {
                    record_ignore_warning(db, &err);
                    duplicates += 1;
                    continue;
                } else {
                    return Err(err);
                }
            }
            Err(err) if ignore && !probe_first && ignore_data_error(&err) => {
                if let Some(savepoint) = row_savepoint {
                    db.rollback_to_statement_savepoint(txn.txn_id, savepoint)?;
                }
                record_ignore_warning(db, &err);
                continue;
            }
            Err(err) => return Err(err),
        };
        if ids.first_generated == 0 {
            ids.first_generated = inserted.first_insert_id;
        }
        if let Some(idx) = table.auto_increment_column_index() {
            ids.last_copied = if inserted.first_insert_id != 0 {
                inserted.first_insert_id
            } else {
                auto_increment_row_id(&values, Some(idx))
            };
        }
        if needs_image {
            if inserted.first_insert_id != 0 {
                if let Some(idx) = table.columns.iter().position(|c| c.auto_increment) {
                    values[idx] = Value::UInt(inserted.first_insert_id);
                }
            }
            let _partition = replace_partition_guard(&table, &values)?;
            let row_id = inserted.row_ids.first().copied().ok_or_else(|| {
                error(
                    crate::error::catalog::ER_UNKNOWN_ERROR,
                    "INSERT did not return a row identity",
                )
            })?;
            let after = db
                .dml_candidate_row_for_write(&table, row_id, txn)?
                .ok_or_else(|| {
                    error(
                        crate::error::catalog::ER_UNKNOWN_ERROR,
                        "inserted row disappeared before AFTER/RETURNING",
                    )
                })?;
            if TRIGGERS {
                let hook = hooks.as_mut().expect("trigger context");
                fire_after_insert(
                    db,
                    hook.session,
                    &table,
                    after.values.clone(),
                    hook.registry,
                )?;
            }
            returning.push(None, Some(&after.values))?;
        }
        affected += 1 + deleted;
        duplicates += deleted;
    }
    if on_duplicate.is_some() {
        crate::engine::api::set_statement_matched_rows(
            previous_matched + affected + unchanged_duplicates,
        );
    }
    crate::engine::api::take_insert_duplicates();
    crate::engine::api::add_insert_duplicates(duplicates);
    crate::engine::api::set_statement_insert_id(ids.first_generated);
    Ok(returning.finish(QueryResponse::OkAffected {
        affected_rows: affected,
    }))
}

#[allow(clippy::too_many_arguments)]
fn apply_insert_duplicate<const TRIGGERS: bool>(
    db: &Arc<Database>,
    hooks: &mut Option<InsertTriggers<'_>>,
    txn: TxnView,
    table: &TableSchema,
    row_id: RowId,
    proposed: &[Value],
    updates: &[crate::parser::Assignment],
    alias: Option<&InsertAliasBinding>,
    view_check: Option<&Expr>,
    needs_image: bool,
    returning: &mut super::returning::ReturningCollector,
) -> Result<(bool, u64), QueryError> {
    let changed_columns = odku_changed_columns(table, updates)?;
    let on_update = super::dml::on_update_timestamp_columns(table, &changed_columns);
    let mut write_columns = changed_columns.clone();
    write_columns.extend_from_slice(&on_update);
    let all_columns = if TRIGGERS {
        (0..table.columns.len()).collect::<Vec<_>>()
    } else {
        Vec::new()
    };
    let compare = if TRIGGERS {
        &all_columns
    } else {
        &changed_columns
    };
    let auto_idx = table.auto_increment_column_index();
    let needs_image = needs_image || auto_idx.is_some_and(|idx| changed_columns.contains(&idx));
    let mut updated_id = 0;
    let apply = |existing: &[Value]| {
        updated_id = auto_increment_row_id(existing, auto_idx);
        let mut new_values = existing.to_vec();
        for assignment in updates {
            let idx = table.column_index(&assignment.column)?;
            let mut expr =
                bind_on_duplicate_expr(&assignment.value, table, Some(idx), proposed, alias)?;
            if super::prepared_dml::expr_contains_subquery(&expr) {
                super::dml::materialize_subqueries(db, txn, &mut expr)?;
            }
            new_values[idx] = crate::engine::expr::binary_assignment_value(
                eval_expr(&expr, table, &new_values)?,
                &expr,
                table,
                &table.columns[idx].data_type,
            )?;
        }
        if TRIGGERS {
            let hook = hooks.as_mut().expect("trigger context");
            new_values = fire_before_update(
                db,
                hook.session,
                table,
                existing.to_vec(),
                new_values,
                hook.registry,
            )?;
            // Only timestamp no-op detection and view predicates need an early
            // normalized image. Otherwise storage normalizes once, after BEFORE.
            if !on_update.is_empty() || view_check.is_some() {
                db.coerce_row_for_load(table, &mut new_values)?;
                db.materialize_generated_columns_for_insert(table, &mut new_values)?;
                if new_values != existing {
                    let automatic = on_update
                        .iter()
                        .copied()
                        .filter(|&i| new_values[i] == existing[i])
                        .collect::<Vec<_>>();
                    super::dml::apply_on_update_timestamps(table, &mut new_values, &automatic);
                }
            }
        } else {
            super::dml::apply_on_update_timestamps(table, &mut new_values, &on_update);
        }
        if let Some(predicate) = view_check {
            if !eval_bool(predicate, table, &new_values)? {
                return Err(error(
                    crate::error::catalog::ER_VIEW_CHECK_FAILED,
                    "CHECK OPTION failed for view",
                ));
            }
        }
        Ok(new_values)
    };
    let writes = if TRIGGERS {
        None
    } else {
        Some(write_columns.as_slice())
    };
    if needs_image {
        let (changed, images) =
            db.update_row_with_images_at(txn, table, None, row_id, writes, Some(compare), apply)?;
        if let Some((old, new)) = images {
            updated_id = auto_increment_row_id(&new, auto_idx);
            if TRIGGERS {
                let hook = hooks.as_mut().expect("trigger context");
                // A matched no-op ODKU still fires AFTER UPDATE.
                fire_after_update(
                    db,
                    hook.session,
                    table,
                    old.clone(),
                    new.clone(),
                    hook.registry,
                )?;
            }
            returning.push(Some(&old), Some(&new))?;
        }
        Ok((changed, if changed { updated_id } else { 0 }))
    } else {
        let changed = db.update_row_by_id_with_changed_columns_noop(
            txn,
            table,
            row_id,
            writes,
            Some(compare),
            apply,
        )?;
        Ok((changed, if changed { updated_id } else { 0 }))
    }
}

fn auto_increment_row_id(values: &[Value], index: Option<usize>) -> u64 {
    match index.and_then(|idx| values.get(idx)) {
        Some(Value::UInt(value)) => *value,
        // The OK packet carries unsigned 64-bit bits even for explicit signed keys.
        Some(Value::Int(value)) => *value as u64,
        _ => 0,
    }
}

fn replace_insert_row<const TRIGGERS: bool>(
    db: &Arc<Database>,
    hooks: &mut Option<InsertTriggers<'_>>,
    txn: TxnView,
    table: &TableSchema,
    table_name: &str,
    values: &[Value],
    retry: bool,
) -> Result<(crate::storage::InsertRowsResult, u64), QueryError> {
    let _partition = replace_partition_guard(table, values)?;
    let mut deleted = 0u64;
    let mut attempt = 0;
    loop {
        for row_id in find_all_conflicting_row_ids(db, txn, table, values)? {
            crate::engine::cancel::check_cancelled()?;
            let old = if TRIGGERS {
                let sid = if table.is_partitioned() {
                    table.route_row_to_partition(values)?.storage_id
                } else {
                    table.id
                };
                db.lock_row_for_update_at(txn, table, sid, row_id)?;
                let Some(row) = db.dml_candidate_row_for_write(table, row_id, txn)? else {
                    continue;
                };
                let hook = hooks.as_mut().expect("trigger context");
                fire_before_delete(db, hook.session, table, row.values.clone(), hook.registry)?;
                Some(row.values)
            } else {
                None
            };
            if db.delete_row_by_id(txn, table, row_id)? {
                if TRIGGERS {
                    let hook = hooks.as_mut().expect("trigger context");
                    fire_after_delete(db, hook.session, table, old.unwrap(), hook.registry)?;
                }
                deleted += 1;
            }
        }
        match db.insert_rows_detailed(txn, table_name, vec![values.to_vec()]) {
            Ok(inserted) => return Ok((inserted, deleted)),
            Err(err)
                if retry
                    && err.is(crate::error::catalog::ER_DUP_ENTRY)
                    && attempt < REPLACE_MAX_RETRY =>
            {
                attempt += 1;
            }
            Err(err) => return Err(err),
        }
    }
}

/// B8 — the distinct column indices an `ON DUPLICATE KEY UPDATE` clause
/// assigns. Used as both the write set and the no-op-comparison set so a
/// dup-key UPDATE that changes nothing reports 0 affected rows.
pub(super) fn odku_changed_columns(
    table: &TableSchema,
    updates: &[crate::parser::Assignment],
) -> Result<Vec<usize>, QueryError> {
    let mut cols = Vec::with_capacity(updates.len());
    for assignment in updates {
        let idx = table.column_index(&assignment.column)?;
        if !cols.contains(&idx) {
            cols.push(idx);
        }
    }
    Ok(cols)
}

pub(super) fn bind_on_duplicate_expr(
    expr: &Expr,
    table: &TableSchema,
    target_idx: Option<usize>,
    proposed: &[Value],
    insert_alias: Option<&InsertAliasBinding>,
) -> Result<Expr, QueryError> {
    let expr = bind_proposed_row_expr(expr, table, proposed, insert_alias)?;
    crate::engine::expr::bind_column_defaults(&expr, table, target_idx)
}

pub(super) struct InsertAliasBinding {
    row_alias: String,
    columns: HashMap<String, usize>,
}

pub(super) fn build_insert_alias_binding(
    table: &TableSchema,
    column_indexes: &[usize],
    insert_alias: Option<&crate::parser::InsertAlias>,
) -> Result<Option<InsertAliasBinding>, QueryError> {
    let Some(alias) = insert_alias else {
        return Ok(None);
    };
    let mut columns = HashMap::new();
    if alias.column_aliases.is_empty() {
        for (idx, column) in table.columns.iter().enumerate() {
            columns.insert(column.name.to_ascii_lowercase(), idx);
        }
    } else {
        if alias.column_aliases.len() != column_indexes.len() {
            return Err(error(
                crate::error::catalog::ER_WRONG_VALUE_COUNT_ON_ROW,
                "column count does not match value alias count",
            ));
        }
        for (alias_name, idx) in alias
            .column_aliases
            .iter()
            .zip(column_indexes.iter().copied())
        {
            if columns
                .insert(alias_name.to_ascii_lowercase(), idx)
                .is_some()
            {
                return Err(error(
                    crate::error::catalog::ER_DUP_FIELDNAME,
                    format!("duplicate column name '{}'", alias_name),
                ));
            }
        }
    }
    Ok(Some(InsertAliasBinding {
        row_alias: alias.row_alias.to_ascii_lowercase(),
        columns,
    }))
}

fn bind_proposed_row_expr(
    expr: &Expr,
    table: &TableSchema,
    proposed: &[Value],
    insert_alias: Option<&InsertAliasBinding>,
) -> Result<Expr, QueryError> {
    crate::parser::visit::try_map_value_expr(expr, &mut |expr| {
        Ok(Some(match expr {
            Expr::Function { name, args, .. } if name.eq_ignore_ascii_case("VALUES") => {
                crate::engine::expr::with_expression_warning_dedup(0x5641_4c55_4553_u64, || {
                    crate::engine::api::push_statement_warning(
                        crate::error::ConditionLevel::Warning,
                        crate::error::catalog::ER_WARN_DEPRECATED_SYNTAX,
                        "'VALUES function' is deprecated and will be removed in a future release. \
                         Please use an alias (INSERT INTO ... VALUES (...) AS alias) and replace \
                         VALUES(col) in the ON DUPLICATE KEY UPDATE clause with alias.col instead",
                    );
                });
                if args.len() != 1 {
                    return Err(error(
                        crate::error::catalog::ER_WRONG_PARAMCOUNT_TO_NATIVE_FCT,
                        "VALUES() requires one column argument",
                    ));
                }
                let column = expr_column_name(&args[0]).ok_or_else(|| {
                    error(
                        crate::error::catalog::ER_BAD_FIELD_ERROR,
                        "VALUES() requires a column name argument",
                    )
                })?;
                let idx = table.column_index(column)?;
                Expr::BoundValue(proposed.get(idx).cloned().unwrap_or(Value::Null))
            }
            Expr::CompoundIdentifier(parts) if parts.len() >= 2 => {
                let qualifier = parts[parts.len() - 2].to_ascii_lowercase();
                let column = &parts[parts.len() - 1];
                if let Some(alias) = insert_alias {
                    if qualifier == alias.row_alias {
                        let idx =
                            alias
                                .columns
                                .get(&column.to_ascii_lowercase())
                                .ok_or_else(|| {
                                    error(
                                        crate::error::catalog::ER_BAD_FIELD_ERROR,
                                        format!("unknown column '{}'", column),
                                    )
                                })?;
                        Expr::BoundValue(proposed.get(*idx).cloned().unwrap_or(Value::Null))
                    } else {
                        expr.clone()
                    }
                } else {
                    expr.clone()
                }
            }
            _ => return Ok(None),
        }))
    })
}

pub(super) fn find_conflicting_row_id(
    db: &Arc<Database>,
    txn: TxnView,
    table: &TableSchema,
    values: &[Value],
) -> Result<Option<RowId>, QueryError> {
    for index in table.indexes.iter().filter(|i| i.enforces_uniqueness()) {
        let key = table.index_key(index, values)?;
        if key.0.iter().any(|p| matches!(p, KeyPart::Null)) && !index.primary {
            continue;
        }
        let ids = db.lookup_index_for_write(txn, table, index, key)?;
        if let Some(id) = ids.into_iter().next() {
            return Ok(Some(id));
        }
    }
    Ok(None)
}

pub(super) fn find_all_conflicting_row_ids(
    db: &Arc<Database>,
    txn: TxnView,
    table: &TableSchema,
    values: &[Value],
) -> Result<Vec<RowId>, QueryError> {
    let mut all_ids: Vec<RowId> = Vec::new();
    for index in table.indexes.iter().filter(|i| i.enforces_uniqueness()) {
        let key = table.index_key(index, values)?;
        if key.0.iter().any(|p| matches!(p, KeyPart::Null)) && !index.primary {
            continue;
        }
        let ids = db.lookup_index_for_write(txn, table, index, key)?;
        for id in ids {
            if !all_ids.contains(&id) {
                all_ids.push(id);
            }
        }
    }
    Ok(all_ids)
}

/// Scatter an ordered projection backwards in the row's existing allocation.
/// Callers have validated width and excluded expression defaults.
fn expand_ordered_insert_row(
    table: &TableSchema,
    columns: &[usize],
    missing: &[usize],
    row: &mut Vec<Value>,
) {
    if let [column] = missing {
        let value = if table.columns[*column].state == crate::storage::ColumnState::Dropped {
            Value::Null
        } else {
            resolve_column_default(&table.columns[*column])
        };
        // One gap needs a single overlap-safe move, rather than swapping each
        // source value through a temporary. This preserves every owned payload.
        row.insert(*column, value);
        return;
    }
    row.resize_with(table.columns.len(), || Value::Null);
    for (source, &destination) in columns.iter().enumerate().rev() {
        row.swap(source, destination);
    }
    for &column in missing {
        row[column] = if table.columns[column].state == crate::storage::ColumnState::Dropped {
            Value::Null
        } else {
            resolve_column_default(&table.columns[column])
        };
    }
}

#[cfg(test)]
#[test]
fn ordered_insert_projection_reuses_rows_and_matches_default_scatter() {
    std::fs::create_dir_all("/tmp/mydb").unwrap();
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let engine = StorageQueryEngine::open(dir.path()).unwrap();
    engine.execute("CREATE TABLE projected_rows(a BIGINT DEFAULT 9,b VARCHAR(8) DEFAULT 'text',c BLOB,d INT DEFAULT 7)").unwrap();
    let table = engine.database().table("test.projected_rows").unwrap();
    for mask in 0..16 {
        let columns: Vec<_> = (0..4).filter(|n| mask & (1 << n) != 0).collect();
        let missing: Vec<_> = (0..4).filter(|n| !columns.contains(n)).collect();
        let inputs = [
            Value::Int(13),
            Value::String("中".into()),
            Value::Bytes(vec![0, 255]),
            Value::Null,
        ];
        let mut row = Vec::with_capacity(4);
        row.extend(columns.iter().map(|&column| inputs[column].clone()));
        let original = row.as_ptr();
        let mut expected: Vec<_> = table.columns.iter().map(resolve_column_default).collect();
        for &column in &columns {
            expected[column] = inputs[column].clone();
        }
        expand_ordered_insert_row(&table, &columns, &missing, &mut row);
        assert_eq!(row, expected, "projection {columns:?}");
        assert_eq!(row.as_ptr(), original);
    }
}

pub(super) fn execute_insert_values(
    db: &Arc<Database>,
    txn: TxnView,
    table_name: &str,
    columns: Option<&[String]>,
    source_rows: Vec<Vec<Value>>,
    autoinc_fast: bool,
    returning: Option<&crate::parser::ReturningClause>,
    target_alias: Option<&str>,
) -> Result<QueryResponse, QueryError> {
    let table = db.table(table_name)?.clone();
    let column_indexes = insert_column_indexes(&table, columns)?;
    let missing_required = missing_required_columns(&table, &column_indexes);
    let mut rows = source_rows;
    // Fast-path detection: when the user listed every visible column
    // in physical-position order, the supplied row already matches the
    // heap layout slot-for-slot (modulo Dropped tombstones, which need
    // a NULL placeholder slot — handled below).
    let visible_count = table.visible_column_count();
    let no_tombstones = visible_count == table.columns.len();
    let full_width_identity = no_tombstones
        && column_indexes.len() == table.columns.len()
        && column_indexes
            .iter()
            .enumerate()
            .all(|(expected, actual)| expected == *actual);
    let row_defaults = row_default_plans(&table, &column_indexes)?;
    // A sorted projection can be expanded backwards without overwriting any
    // source value. Defaults with expressions retain the canonical builder's
    // evaluation order, including expressions on explicitly supplied columns.
    let expand_in_place = column_indexes.windows(2).all(|pair| pair[0] < pair[1])
        && table
            .columns
            .iter()
            .all(|column| column.default_expr.is_none());
    let missing_positions: Vec<_> = if expand_in_place && !full_width_identity {
        let mut supplied = column_indexes.iter().peekable();
        (0..table.columns.len())
            .filter(|column| {
                if supplied.peek().is_some_and(|&&index| index == *column) {
                    supplied.next();
                    false
                } else {
                    true
                }
            })
            .collect()
    } else {
        Vec::new()
    };
    let has_generated = table
        .columns
        .iter()
        .any(|column| column.generated_expr.is_some());
    for row in &mut rows {
        crate::engine::cancel::check_cancelled()?;
        if row.len() != column_indexes.len() {
            return Err(error(
                crate::error::catalog::ER_WRONG_VALUE_COUNT_ON_ROW,
                "column count does not match value count",
            ));
        }
        if full_width_identity {
            if has_generated {
                db.materialize_generated_columns_for_insert(&table, row)?;
            }
            continue;
        }
        if expand_in_place {
            expand_ordered_insert_row(&table, &column_indexes, &missing_positions, row);
            enforce_missing_required_columns(&table, row, &missing_required, false)?;
            if has_generated {
                db.materialize_generated_columns_for_insert(&table, row)?;
            }
            continue;
        }
        let supplied = std::mem::take(row);
        // Build a full-width row indexed by physical_pos. Dropped
        // tombstones get NULL — the read path filters them out, but
        // the heap row codec is positional so we must still emit a
        // value for the slot to keep later columns at the correct
        // index. Public/WriteOnly columns get their declared default
        // unless overridden by the user-supplied value below.
        let mut values: Vec<Value> = table
            .columns
            .iter()
            .map(|column| {
                if column.state == crate::storage::ColumnState::Dropped {
                    Value::Null
                } else {
                    resolve_column_default(column)
                }
            })
            .collect();
        for (idx, value) in column_indexes.iter().zip(supplied) {
            values[*idx] = value;
        }
        materialize_row_defaults(db, &table, &mut values, &row_defaults)?;
        // The plain (non-IGNORE) VALUES path: IGNORE/REPLACE/ODKU are routed to
        // `execute_insert_with_conflict`, so `ignore` is always false here.
        enforce_missing_required_columns(&table, &mut values, &missing_required, false)?;
        db.materialize_generated_columns_for_insert(&table, &mut values)?;
        *row = values;
    }
    let inserted = if autoinc_fast {
        db.insert_rows_autoinc_fast_detailed(txn, table_name, rows)?
    } else {
        db.insert_rows_detailed(txn, table_name, rows)?
    };
    crate::engine::api::set_statement_insert_id(inserted.first_insert_id);
    let affected_rows = inserted.row_ids.len() as u64;
    if returning.is_none() {
        return Ok(QueryResponse::OkAffected { affected_rows });
    }
    let mut collector = super::returning::ReturningCollector::new(
        returning,
        &table,
        super::returning::DefaultImage::New,
        target_alias,
    )?;
    for (ordinal, row_id) in inserted.row_ids.into_iter().enumerate() {
        let _partition = inserted
            .storage_ids
            .get(ordinal)
            .map(|storage_id| super::partition_router::push_pruned_partitions(vec![*storage_id]));
        let row = db.read_row(&table, row_id, txn)?.ok_or_else(|| {
            error(
                crate::error::catalog::ER_UNKNOWN_ERROR,
                "inserted row disappeared before RETURNING",
            )
        })?;
        collector.push(None, Some(&row.values))?;
    }
    Ok(collector.finish(QueryResponse::OkAffected { affected_rows }))
}

pub(super) fn insert_column_indexes(
    table: &TableSchema,
    columns: Option<&[String]>,
) -> Result<Vec<usize>, QueryError> {
    let insert_columns = match columns {
        Some(columns) => columns,
        None => {
            // INSERT without an explicit column list expects values
            // for every logically-visible column — Dropped tombstones
            // get filled with NULL by the row-build path. Indices are
            // returned in `iter_visible_columns` (declaration) order
            // so that user-supplied positional values land in the
            // matching physical slot.
            return Ok(table.iter_select_columns().map(|(idx, _)| idx).collect());
        }
    };
    let mut column_indexes = Vec::new();
    let mut seen = HashSet::new();
    for column in insert_columns {
        let idx = table.column_index(column)?;
        if !seen.insert(idx) {
            return Err(error(
                crate::error::catalog::ER_FIELD_SPECIFIED_TWICE,
                format!("column '{}' specified twice", column),
            ));
        }
        column_indexes.push(idx);
    }
    Ok(column_indexes)
}

pub(super) fn insert_targets_generated(
    table: &TableSchema,
    columns: Option<&[String]>,
) -> Result<bool, QueryError> {
    let column_indexes = insert_column_indexes(table, columns)?;
    Ok(column_indexes
        .iter()
        .any(|idx| table.columns[*idx].generated_expr.is_some()))
}

fn expr_is_default_for_column(expr: &Expr, column: &str) -> bool {
    match expr {
        Expr::Default(None) => true,
        Expr::Default(Some(name)) => name.eq_ignore_ascii_case(column),
        _ => false,
    }
}

fn generated_column_assignment_error() -> QueryError {
    error(
        crate::error::catalog::ER_NON_DEFAULT_VALUE_FOR_GENERATED_COLUMN,
        "value specified for generated column is not allowed; use DEFAULT",
    )
}
