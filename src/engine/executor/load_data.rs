use crate::engine::executor::mem_budget::load_data_ancillary_headroom as load_data_ancillary_headroom_for_limit;
use crate::engine::executor::mem_budget::load_data_reclaim_target as load_data_batch_reclaim_target_for_values;
use std::collections::BTreeMap;
use std::io::{BufRead, BufReader, Read, Write};
use std::sync::{mpsc, Arc};
use std::thread;

use memchr::{memchr, memchr2};

use crate::storage::LoadDataIndexKeyBatch;

use super::insert::{insert_column_indexes, resolve_column_default};
use super::tx::{ensure_statement_dml_txn, ensure_statement_txn, finish_statement_txn};
use super::*;

thread_local! {
    // IGNORE never enters the parallel decoder. Only conversion failures use
    // this state; successful cells retain the ordinary specialized kernels.
    static LOAD_DECODE_POLICY: std::cell::Cell<(bool, bool)> = const { std::cell::Cell::new((false, false)) };
}

struct LoadDecodePolicyGuard((bool, bool));

impl LoadDecodePolicyGuard {
    fn new(ignore: bool) -> Self {
        Self(LOAD_DECODE_POLICY.with(|policy| policy.replace((ignore, false))))
    }
}

impl Drop for LoadDecodePolicyGuard {
    fn drop(&mut self) {
        LOAD_DECODE_POLICY.with(|policy| policy.set(self.0));
    }
}

#[cold]
fn defer_ignored_load_conversion(bytes: &[u8], err: QueryError) -> Result<Value, QueryError> {
    let ignore = LOAD_DECODE_POLICY.with(|policy| policy.get().0);
    if !ignore
        || !err.is_any(&[
            crate::error::catalog::ER_WARN_DATA_OUT_OF_RANGE,
            crate::error::catalog::ER_TRUNCATED_WRONG_VALUE,
            crate::error::catalog::ER_WRONG_VALUE_FOR_TYPE,
            crate::error::catalog::ER_TRUNCATED_WRONG_VALUE_FOR_FIELD,
            crate::error::catalog::ER_DATA_TOO_LONG,
            crate::error::catalog::WARN_DATA_TRUNCATED,
            crate::error::catalog::ER_INVALID_CHARACTER_STRING,
        ])
    {
        return Err(err);
    }
    let value = str_to_string_value(load_utf8(bytes)?);
    LOAD_DECODE_POLICY.with(|policy| policy.set((true, true)));
    Ok(value)
}

fn coerce_deferred_load_rows(
    db: &Database,
    table: &TableSchema,
    rows: &mut [Vec<Value>],
) -> Result<(), QueryError> {
    let deferred = LOAD_DECODE_POLICY.with(|policy| {
        let (ignore, deferred) = policy.get();
        policy.set((ignore, false));
        deferred
    });
    if LOAD_DECODE_POLICY.with(|policy| policy.get().0) {
        for row in rows.iter_mut() {
            coerce_load_nulls(table, row)?;
        }
    }
    if deferred {
        // Keep error classification, clamping and warnings in the shared
        // storage coercer. Complete conversion before triggers or partition
        // routing inspect the row, including generated-column dependencies.
        for row in rows {
            db.coerce_row_for_load(table, row)?;
        }
    }
    Ok(())
}

const BULK_CHUNK_SIZE: usize = 50_000;

#[cfg(test)]
thread_local! {
    // Scoped to the invoking test thread; production and other tests retain
    // the normal batch size. This lets rollback tests cross real flushes
    // without executing tens of thousands of row triggers.
    static TEST_BULK_CHUNK_ROWS: std::cell::Cell<usize> = const { std::cell::Cell::new(BULK_CHUNK_SIZE) };
}

fn bulk_chunk_rows() -> usize {
    #[cfg(test)]
    {
        TEST_BULK_CHUNK_ROWS.get()
    }
    #[cfg(not(test))]
    {
        BULK_CHUNK_SIZE
    }
}

const READ_BUF_SIZE: usize = 4 * 1024 * 1024;
// Upper bound for a parallel LOAD batch. The actual row count is clamped by
// the governed in-flight budget and the prepared index-key footprint, while
// a larger ceiling amortizes channel, WAL, map, and per-index allocation
// costs for wide bulk inputs.
const PARALLEL_CHUNK_SIZE: usize = 500_000;
const PARALLEL_MIN_ROWS: usize = 2_048;
const PARALLEL_CHUNK_TARGET_BYTES: usize = 64 * 1024 * 1024;

/// D2 — RAII guard for the in-flight LOAD DATA counter. Bumps
/// `load_data_active` on construction; the `Drop` impl pops it back
/// down so a panic mid-ingest can't strand the counter at a wrong value.
struct LoadDataProgressGuard<'a> {
    db: &'a Database,
}

impl<'a> LoadDataProgressGuard<'a> {
    fn new(db: &'a Database) -> Self {
        db.dml_stats().load_data_enter();
        Self { db }
    }
}

impl Drop for LoadDataProgressGuard<'_> {
    fn drop(&mut self) {
        self.db.dml_stats().load_data_exit();
    }
}

/// D2 — Helper called from inside each LOAD DATA execute_* variant after
/// the storage layer ingests a row chunk; bumps the cumulative
/// `load_data_rows_loaded` counter for SHOW ENGINE MYDB STATUS.
fn bump_load_data_rows(db: &Database, n: u64) {
    db.dml_stats().record_load_data_rows(n);
}

// G8 — per-statement LOAD DATA counters feeding the MySQL
// `Records: N Deleted: M Skipped: K Warnings: W` OK-packet info trailer.
// Kept as thread-locals so the parallel/fast hot paths don't have to
// thread an extra `&mut` counter through their worker plumbing; only the
// once-per-statement finalize reads them. `Records` = source rows read;
// `Deleted` = rows removed by REPLACE; `Skipped` = rows not stored
// (IGNORE duplicate/constraint). All are reset at the top of every
// `execute_load_data` and materialised into the info trailer at the end.
thread_local! {
    static LOAD_RECORDS: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
    static LOAD_DELETED: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
    static LOAD_SKIPPED: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
}

fn reset_load_counters() {
    LOAD_RECORDS.with(|c| c.set(0));
    LOAD_DELETED.with(|c| c.set(0));
    LOAD_SKIPPED.with(|c| c.set(0));
}

fn add_load_records(n: u64) {
    LOAD_RECORDS.with(|c| c.set(c.get() + n));
}

fn add_load_deleted(n: u64) {
    LOAD_DELETED.with(|c| c.set(c.get() + n));
}

fn add_load_skipped(n: u64) {
    LOAD_SKIPPED.with(|c| c.set(c.get() + n));
}

fn record_triggered_load_batch(
    db: &Arc<Database>,
    records: u64,
    affected: u64,
    ignore: bool,
    replace: bool,
) {
    if replace {
        add_load_deleted(affected.saturating_sub(records));
    }
    if ignore {
        add_load_skipped(records.saturating_sub(affected));
    }
    bump_load_data_rows(db, affected);
}

/// Build and stash the `Records/Deleted/Skipped/Warnings` info trailer.
/// `affected` is the statement's reported affected-row count, used only
/// as the `Records` fallback for paths that don't track source rows
/// explicitly (the non-conflict fast/parallel paths, where every source
/// row is stored so Records == affected, Deleted == Skipped == 0).
fn finalize_load_info(affected: u64) {
    let records = {
        let tracked = LOAD_RECORDS.with(|c| c.get());
        if tracked > 0 {
            tracked
        } else {
            affected
        }
    };
    let deleted = LOAD_DELETED.with(|c| c.get());
    let skipped = LOAD_SKIPPED.with(|c| c.get());
    let warnings = crate::engine::api::statement_warning_count();
    crate::engine::api::set_statement_info(format!(
        "Records: {} Deleted: {} Skipped: {} Warnings: {}",
        records, deleted, skipped, warnings
    ));
}

pub(super) fn execute_load_data(
    db: &Arc<Database>,
    session: &mut SessionContext,
    stmt: crate::parser::LoadData,
    registry: &SessionRegistry,
) -> Result<QueryResponse, QueryError> {
    execute_load_data_with_reader(db, session, stmt, registry, None)
}

pub(super) fn execute_load_data_with_reader(
    db: &Arc<Database>,
    session: &mut SessionContext,
    mut stmt: crate::parser::LoadData,
    registry: &SessionRegistry,
    supplied_reader: Option<Box<dyn Read + Send>>,
) -> Result<QueryResponse, QueryError> {
    // LOCAL cannot stop the client midway through its upload. Without
    // REPLACE it follows IGNORE semantics for duplicate and conversion errors.
    stmt.ignore_duplicates |= stmt.local && !stmt.replace;
    session.note_write_intent();
    let _ignore_coercion = crate::storage::DmlIgnoreCoercionGuard::new(stmt.ignore_duplicates);
    let _decode_policy = LoadDecodePolicyGuard::new(stmt.ignore_duplicates);
    // D2 — Track in-flight LOAD DATA count. Bump on entry; the guard's
    // Drop decrements on exit so a panicking ingest still leaves the
    // counter consistent. Rows-loaded is updated inline as batches land
    // (see `bump_load_data_rows` calls inside the various execute_*
    // helpers below).
    let _progress_guard = LoadDataProgressGuard::new(db);
    reset_load_counters();
    let table_name = qualify_table_name(session, &stmt.table);
    let table = db
        .table(&table_name)
        .map_err(|err| {
            if db
                .view_definition(&session.current_database, &stmt.table)
                .is_some()
            {
                error(
                    crate::error::catalog::ER_WRONG_OBJECT,
                    format!("'{table_name}' is not BASE TABLE; LOAD into views is not supported"),
                )
            } else {
                err
            }
        })?
        .clone();
    let partition_filter =
        super::partition_clause::resolve_partition_clause(db, &table_name, &stmt.partitions)?;

    // Validate every target before consuming input, including XML, user-variable
    // and SET paths that do not call insert_column_indexes.
    let load_columns = stmt.column_names();
    let load_column_indexes = insert_column_indexes(&table, load_columns.as_deref())?;
    for &idx in &load_column_indexes {
        reject_load_generated_column(&table, idx)?;
    }
    for (name, _) in &stmt.set_clauses {
        reject_load_generated_column(&table, table.column_index(name)?)?;
    }

    if stmt.line_terminated_by.is_empty() && !stmt.is_xml {
        return Err(error(
            crate::error::catalog::ER_NOT_SUPPORTED_YET,
            "LOAD DATA requires a nonempty line terminator",
        ));
    }
    let _warning_scope = crate::engine::api::StatementWarningSinkScope::enter();
    let parser = FieldParser {
        input_charset: LoadLegacyCharset::from_name(stmt.charset.as_deref()),
        field_terminator: stmt.field_terminated_by.as_bytes().to_vec(),
        line_terminator: stmt.line_terminated_by.as_bytes().to_vec(),
        enclosed_by: stmt.field_enclosed_by.map(|c| c as u8),
        escaped_by: stmt.field_escaped_by.map(|c| c as u8),
        line_starting_by: stmt.line_starting_by.as_bytes().to_vec(),
    };

    let data: Box<dyn Read> = if let Some(reader) = supplied_reader {
        reader
    } else if let Some(ref data) = stmt.data {
        Box::new(std::io::Cursor::new(data.clone()))
    } else {
        // LOCAL input must come from the client, never the server filesystem.
        if stmt.local {
            return Err(error(
                crate::error::catalog::ER_UNSUPPORTED_PS,
                "LOAD DATA LOCAL requires client-supplied data",
            ));
        }
        // G6 — `secure_file_priv` gate for server-side (non-LOCAL) LOAD
        // DATA. The file is read off the server's own filesystem, so
        // MySQL restricts which paths are legal:
        //   * value = NULL  → all server-side file import/export refused
        //   * value = dir   → the file must resolve under `dir`
        //   * value = ""    → no restriction (MySQL's unsafe default)
        check_secure_file_priv(db, session, &stmt.file_path)?;
        let file = std::fs::File::open(&stmt.file_path).map_err(|e| {
            error(
                crate::error::catalog::ER_TEXTFILE_NOT_READABLE,
                format!("File '{}' not found (OS errno {})", stmt.file_path, e),
            )
        })?;
        Box::new(file)
    };
    let mut reader = BufReader::with_capacity(READ_BUF_SIZE, data);

    if stmt.ignore_rows > 0 {
        skip_lines(&mut reader, &parser, stmt.ignore_rows)?;
    }

    let has_conflict_handling = stmt.ignore_duplicates || stmt.replace;

    // Decide whether the row-by-row "general" path is required. The
    // general path handles the features the optimized fast/parallel paths
    // deliberately don't: LOAD XML, `SET col=expr`, `@user_var` targets,
    // input charset transcoding, and — crucially for G4 — non-strict
    // sql_mode, where type-conversion failures must become warnings +
    // fallback values (via the storage `coerce_row` single source)
    // instead of the fast path's hard errors. Strict sql_mode (mydb's
    // default posture) keeps the untouched fast path: its hard errors
    // already match strict semantics, so performance never regresses for
    // the common bulk-import case.
    // Outbound foreign keys force the general path: the optimized
    // fast/parallel paths write heap-only (`insert_rows_heap_only_detailed`
    // / `insert_pre_encoded_rows_heap_only_detailed`) and never call
    // `fk_check_child_references`, so a fast-path LOAD into a child table
    // would silently import orphan rows that violate the constraint. The
    // general path funnels through `insert_rows_bulk_detailed`, which
    // enforces the child-side check. Gated on `fk_checks_enabled()` so
    // `SET foreign_key_checks=0` (every mysqldump body) keeps the fast path.
    let needs_general = stmt.is_xml
        || !stmt.set_clauses.is_empty()
        || stmt.has_user_var_targets()
        || charset_needs_transcode(stmt.charset.as_deref())
        || (!table.foreign_keys.is_empty() && crate::storage::fk_checks_enabled())
        || (stmt.ignore_duplicates
            && table.columns.iter().any(|column| {
                !column.nullable
                    && !column.auto_increment
                    && column.generated_expr.is_none()
                    && super::insert::implicit_default_for_type(&column.data_type).is_none()
            }))
        || !load_is_strict();

    let result = if needs_general {
        execute_load_data_general(
            db,
            session,
            registry,
            &table_name,
            &table,
            &stmt,
            &parser,
            &mut reader,
            partition_filter.as_deref(),
        )
    } else {
        let column_indexes = load_column_indexes;
        let num_fields = column_indexes.len();
        let full_width_identity = column_indexes.len() == table.columns.len()
            && column_indexes
                .iter()
                .enumerate()
                .all(|(expected, actual)| expected == *actual);
        let coercion_plan = build_coercion_plan(&table, &column_indexes);
        let columns = load_columns;

        if load_has_triggers(db, &table_name, &stmt) {
            execute_load_data_with_triggers(
                db,
                session,
                registry,
                &table_name,
                &table,
                &column_indexes,
                full_width_identity,
                &coercion_plan,
                &parser,
                &mut reader,
                num_fields,
                stmt.ignore_duplicates,
                stmt.replace,
                columns.as_deref(),
                partition_filter.as_deref(),
            )
        } else if has_conflict_handling {
            let degree = load_data_parallel_degree(&stmt.query_hints);
            // Non-Unix LOCAL already owns a protocol staging file. Keep its
            // existing path rather than charging a second input replay copy.
            let parallel_ignore = cfg!(unix)
                && stmt.ignore_duplicates
                && !stmt.replace
                && degree > 1
                && full_width_identity
                && session.next_txn_isolation.is_none()
                && session.next_txn_read_only.is_none()
                && table.auto_increment_column_index().is_none()
                && !table_has_generated_columns(&table)
                && table.check_constraints.is_empty()
                && table
                    .columns
                    .iter()
                    .all(|column| column.default_expr.is_none())
                && load_data_shadow_shape_eligible(db, session, &table)?;
            if let Some(format) = fast_parse_format(&parser).filter(|_| parallel_ignore) {
                execute_load_data_ignore_parallel(
                    db,
                    session,
                    &table_name,
                    &table,
                    &column_indexes,
                    full_width_identity,
                    &coercion_plan,
                    &parser,
                    format,
                    &mut reader,
                    num_fields,
                    degree,
                    partition_filter.as_deref(),
                )
            } else {
                execute_load_data_with_conflict(
                    db,
                    session,
                    &table_name,
                    &table,
                    &column_indexes,
                    full_width_identity,
                    &coercion_plan,
                    &parser,
                    &mut reader,
                    num_fields,
                    stmt.ignore_duplicates,
                    stmt.replace,
                    columns.as_deref(),
                    partition_filter.as_deref(),
                )
            }
        } else {
            let parallel_degree = load_data_parallel_degree(&stmt.query_hints);
            if let Some(format) = fast_parse_format(&parser) {
                if parallel_degree > 1 {
                    let use_shadow_generation =
                        load_data_shadow_shape_eligible(db, session, &table)?;
                    if !use_shadow_generation {
                        db.record_load_shadow_fallback();
                    }
                    execute_load_data_fast_parallel(
                        db,
                        session,
                        &table_name,
                        &table,
                        &column_indexes,
                        full_width_identity,
                        &coercion_plan,
                        format,
                        &mut reader,
                        num_fields,
                        parallel_degree,
                        partition_filter.as_deref(),
                        use_shadow_generation,
                        false,
                    )
                } else {
                    execute_load_data_fast(
                        db,
                        session,
                        &table_name,
                        &table,
                        &column_indexes,
                        full_width_identity,
                        &coercion_plan,
                        &parser,
                        &mut reader,
                        num_fields,
                        partition_filter.as_deref(),
                    )
                }
            } else {
                execute_load_data_fast(
                    db,
                    session,
                    &table_name,
                    &table,
                    &column_indexes,
                    full_width_identity,
                    &coercion_plan,
                    &parser,
                    &mut reader,
                    num_fields,
                    partition_filter.as_deref(),
                )
            }
        }
    };

    _warning_scope.drain_to_local();
    // G8 — attach the `Records/Deleted/Skipped/Warnings` info trailer.
    if let Ok(QueryResponse::OkAffected { affected_rows }) = &result {
        finalize_load_info(*affected_rows);
    }
    // B1 — the fast/parallel LOAD paths write heap-only and never run the
    // `note_vector_*` maintenance hooks, so any row bulk-loaded into a table
    // with an already-*warm* vector index (HNSW or IVF) would be absent
    // from KNN results until restart/OPTIMIZE. Evict the warm graphs on a
    // successful load (no file deletion — the next KNN query rebuilds from the
    // now-complete heap). Cold graphs are unaffected, and a bulk rebuild is the
    // right maintenance shape for a bulk import anyway (it dodges the per-row
    // write amplification of incremental maintenance). Defensive: today the
    // VECTOR accepts text (`[x,...]`) and, with
    // MYDB_LOAD_DATA_VECTOR_FORMAT=base64, raw little-endian f32 bytes. Bulk
    // rebuild is preferable to per-row graph write amplification.
    if result.is_ok() && table.indexes.iter().any(|index| index.is_vector()) {
        db.evict_table_vector_graphs(table.id);
    }
    // A benchmark or migration commonly issues several large LOAD DATA
    // statements back-to-back. The general idle reclaimer waits for 250 ms
    // with no active statement, which that workload never provides; glibc can
    // consequently retain every table's already-freed parser/index-sort arenas
    // until RSS exceeds the host limit even though governor accounting is
    // back below MYDB_MEMORY_TARGET. All LOAD-owned batches/builders are gone
    // here. Trim only when the measured process footprint has actually crossed
    // the hard target, so the ordinary low-pressure path pays no allocator
    // collection cost.
    crate::engine::memory_reclaimer::reclaim_bulk_statement_boundary();
    result
}

fn reject_load_generated_column(table: &TableSchema, idx: usize) -> Result<(), QueryError> {
    let column = &table.columns[idx];
    if column.generated_expr.is_some() {
        return Err(
            crate::error::catalog::ER_NON_DEFAULT_VALUE_FOR_GENERATED_COLUMN.error(format!(
                "The value specified for generated column '{}' in table '{}' is not allowed",
                column.name, table.name
            )),
        );
    }
    Ok(())
}

fn load_has_triggers(db: &Arc<Database>, table_name: &str, stmt: &crate::parser::LoadData) -> bool {
    table_has_insert_triggers(db, table_name)
        || (stmt.replace && table_has_delete_triggers(db, table_name))
}

/// Repair explicit NULL using the type default, never a declared DEFAULT.
fn coerce_load_nulls(table: &TableSchema, row: &mut [Value]) -> Result<(), QueryError> {
    for (column, value) in table.columns.iter().zip(row.iter_mut()) {
        if !column.nullable
            && !column.auto_increment
            && column.generated_expr.is_none()
            && matches!(value, Value::Null)
        {
            *value = super::insert::implicit_default_for_type(&column.data_type)
                .ok_or_else(|| crate::error::messages::bad_null(&column.name))?;
            crate::engine::api::push_statement_warning(
                crate::error::ConditionLevel::Warning,
                crate::error::catalog::ER_BAD_NULL_ERROR,
                format!("Column '{}' cannot be null", column.name),
            );
        }
    }
    Ok(())
}

/// True when the session's effective sql_mode is strict for DML — i.e.
/// STRICT_* is set, or sql_mode was not explicitly set (the default posture).
/// Mirrors the storage-layer `temporal_violation` gate so LOAD DATA and
/// INSERT/UPDATE agree on the strict/non-strict boundary.
fn load_is_strict() -> bool {
    let bits = crate::engine::expr::session_temporal_sql_mode_bits();
    bits & crate::engine::api::TEMPORAL_MODE_STRICT != 0
        || bits & crate::engine::api::TEMPORAL_MODE_EXPLICIT == 0
}

fn load_data_shadow_disabled() -> bool {
    std::env::var("MYDB_LOAD_DATA_SHADOW_DISABLE").is_ok_and(|value| {
        matches!(
            value.to_ascii_lowercase().as_str(),
            "1" | "on" | "true" | "yes"
        )
    })
}

fn load_data_shadow_shape_eligible(
    db: &Database,
    session: &SessionContext,
    table: &TableSchema,
) -> Result<bool, QueryError> {
    if load_data_shadow_disabled()
        // Memory durability has no fsync/WAL write-amplification to avoid,
        // and some in-memory index generations intentionally have no durable
        // file to exchange. Keep that mode on the ordinary heap path.
        || db.durability_mode() == crate::storage::DurabilityMode::Memory
    {
        return Ok(false);
    }
    load_data_exclusive_insert_shape_eligible(db, session, table)
}

fn load_data_exclusive_insert_shape_eligible(
    db: &Database,
    session: &SessionContext,
    table: &TableSchema,
) -> Result<bool, QueryError> {
    if !session.autocommit
        || session.explicit_txn
        || session.current_txn.is_some()
        || session.xa_branch.is_some()
        || crate::binlog::enabled()
        || crate::storage::is_temp_table_id(table.id)
        || table.is_partitioned()
        || !table.foreign_keys.is_empty()
        || table.indexes.iter().any(|index| {
            index.state != crate::storage::IndexState::Public
                || index.has_expression_parts()
                || index.is_fulltext()
                || index.is_spatial()
                || index.is_vector()
                || index.has_multi_valued_parts()
        })
    {
        return Ok(false);
    }
    db.table_is_physically_empty_for_shadow_load(table)
}

/// Whether the declared input charset requires transcoding to the
/// internal utf8mb4 representation. `None` (connection default, already
/// utf8-family) and `binary` need no transcode.
fn charset_needs_transcode(charset: Option<&str>) -> bool {
    match charset {
        None => false,
        Some(cs) => {
            let cs = cs.trim();
            !(cs.eq_ignore_ascii_case("binary")
                || cs.eq_ignore_ascii_case("utf8")
                || cs.eq_ignore_ascii_case("utf8mb3")
                || cs.eq_ignore_ascii_case("utf8mb4")
                || cs.eq_ignore_ascii_case("ascii")
                || cs.eq_ignore_ascii_case("us-ascii"))
        }
    }
}

fn is_binary_column(dt: &DataTypeKind) -> bool {
    matches!(
        dt,
        DataTypeKind::Binary { .. }
            | DataTypeKind::VarBinary { .. }
            | DataTypeKind::Blob { .. }
            | DataTypeKind::Vector { .. }
    )
}

/// Turn a raw field into a pre-coercion `Value`: NULL passes through;
/// binary-family columns keep raw bytes; text/numeric/temporal columns
/// become a `String` (transcoded from the declared input charset when
/// one applies) that the storage `coerce_row` single source then types.
fn resolve_load_missing_default(column: &crate::storage::ColumnSchema) -> Value {
    let value = resolve_column_default(column);
    if !column.nullable && matches!(value, Value::Null) {
        super::insert::implicit_default_for_type(&column.data_type).unwrap_or(value)
    } else {
        value
    }
}

fn field_to_value(
    field: Option<Vec<u8>>,
    col: &crate::storage::ColumnSchema,
    charset: Option<&str>,
) -> Result<Value, QueryError> {
    let Some(bytes) = field else {
        return Ok(Value::Null);
    };
    if is_binary_column(&col.data_type) {
        return Ok(Value::Bytes(bytes));
    }
    if charset_needs_transcode(charset) {
        let text = decode_field_text(&bytes, charset)?;
        return if text.len() <= crate::types::SMALL_STRING_INLINE_CAP {
            Ok(str_to_string_value(&text))
        } else {
            Ok(Value::String(text))
        };
    }
    // The raw parser already owns these bytes. Validate inline values without
    // allocating, and transfer long buffers into the final String directly.
    if bytes.len() <= crate::types::SMALL_STRING_INLINE_CAP {
        Ok(str_to_string_value(load_utf8(&bytes)?))
    } else {
        String::from_utf8(bytes).map(Value::String).map_err(|_| {
            error(
                crate::error::catalog::ER_INVALID_CHARACTER_STRING,
                "Invalid utf8mb4 character string in LOAD DATA",
            )
        })
    }
}

/// Decode raw field bytes into a Rust `String`, honouring the LOAD's
/// `CHARACTER SET` (G3). No/utf8/ascii charset → validated utf8; anything
/// else → `crate::charset::decode_bytes` (errors 1115/1273 on unknown).
fn decode_field_text(bytes: &[u8], charset: Option<&str>) -> Result<String, QueryError> {
    if charset_needs_transcode(charset) {
        crate::charset::decode_bytes(charset.unwrap(), bytes)
    } else {
        Ok(load_utf8(bytes)?.to_string())
    }
}

/// G1/G2/G3/G4/G11/G12 — the row-by-row "general" LOAD path. Handles
/// `SET col=expr`, `@user_var` targets, `CHARACTER SET` transcoding, LOAD
/// XML, non-strict sql_mode coercion (via the storage `coerce_row` single
/// source), and column-count warnings. Reused by both LOAD DATA and LOAD
/// XML: only the field-extraction differs.
#[allow(clippy::too_many_arguments)]
fn execute_load_data_general(
    db: &Arc<Database>,
    session: &mut SessionContext,
    registry: &SessionRegistry,
    table_name: &str,
    table: &TableSchema,
    stmt: &crate::parser::LoadData,
    parser: &FieldParser,
    reader: &mut BufReader<Box<dyn Read>>,
    partition_filter: Option<&[u64]>,
) -> Result<QueryResponse, QueryError> {
    let has_triggers = load_has_triggers(db, table_name, stmt);
    let exclusive = stmt.ignore_duplicates && !stmt.replace && !has_triggers;
    with_load_insert_txn(db, session, table, exclusive, |db, session, _txn| {
        use crate::parser::LoadTarget;

        // XML is transcoded as a document before markup parsing; its field
        // bytes are already UTF-8 and must not be transcoded a second time.
        let charset = if stmt.is_xml {
            None
        } else {
            stmt.charset.as_deref()
        };
        let has_conflict = stmt.ignore_duplicates || stmt.replace;

        // Resolve targets. `None` → all select columns in order.
        let default_targets: Vec<LoadTarget>;
        let targets: &[LoadTarget] = match &stmt.targets {
            Some(t) => t,
            None => {
                default_targets = table
                    .iter_select_columns()
                    .map(|(_, c)| LoadTarget::Column(c.name.clone()))
                    .collect();
                &default_targets
            }
        };
        let num_targets = targets.len();
        let decimal_columns: Vec<_> = table
            .columns
            .iter()
            .enumerate()
            .filter_map(|(index, column)| {
                if let DataTypeKind::Decimal { scale, .. } = column.data_type {
                    Some((index, scale.unwrap_or(0)))
                } else {
                    None
                }
            })
            .collect();

        let mut total_rows: u64 = 0;
        let mut batch_charge = reserve_load_data_batch_memory(db, 0)?;
        let mut batch: Vec<Vec<Value>> = Vec::new();
        let batch_rows = bulk_chunk_rows();
        let batch_limit =
            (mem_budget::memory_target_bytes() / 16).clamp(1024 * 1024, 256 * 1024 * 1024);
        let mut row_index = 0usize;
        loop {
            // DATA is consumed in record-aligned chunks, including from a FIFO.
            // XML still needs the complete document, but its input is admitted
            // incrementally by the governor instead of using unbounded read_to_end.
            let (source_rows, _source_charge, eof) = if stmt.is_xml {
                let (content, _input_charge, eof) = read_general_load_chunk(reader, &[])?;
                let source_charge =
                    reserve_load_data_batch_memory(db, content.capacity().saturating_mul(128))?;
                (parse_xml_rows(&content, stmt)?, source_charge, eof)
            } else {
                read_general_data_rows(reader, parser, num_targets)?
            };
            if source_rows.is_empty() && eof {
                break;
            }
            add_load_records(source_rows.len() as u64);
            for source in source_rows {
                row_index += 1;
                crate::engine::cancel::check_cancelled()?;
                // Build a full-width row seeded with column defaults.
                let mut row: Vec<Value> = table
                    .columns
                    .iter()
                    .map(resolve_load_missing_default)
                    .collect();

                if stmt.is_xml {
                    // Name-based binding. Build a lookup from the row's name→value
                    // pairs, then satisfy each target by name.
                    for target in targets {
                        let name = match target {
                            LoadTarget::Column(n) | LoadTarget::UserVar(n) => n.as_str(),
                        };
                        let field = source
                            .iter()
                            .find(|(k, _)| {
                                k.as_deref().map(|k| k.eq_ignore_ascii_case(name)) == Some(true)
                            })
                            .and_then(|(_, v)| v.clone());
                        bind_target(db, session, table, target, field, charset, &mut row)?;
                    }
                } else {
                    // Positional binding for DATA. G11 — column-count mismatch.
                    let field_count = source.len();
                    if field_count < num_targets {
                        crate::engine::api::push_statement_warning(
                            crate::error::ConditionLevel::Warning,
                            crate::error::catalog::ER_WARN_TOO_FEW_RECORDS,
                            format!("Row {row_index} doesn't contain data for all columns"),
                        );
                    } else if field_count > num_targets {
                        crate::engine::api::push_statement_warning(
                            crate::error::ConditionLevel::Warning,
                            crate::error::catalog::ER_WARN_TOO_MANY_RECORDS,
                            format!(
                        "Row {row_index} was truncated; it contained more data than there were \
                         input columns"
                    ),
                        );
                    }
                    let mut fields = source.into_iter();
                    for target in targets {
                        let field = match fields.next() {
                            Some((_, value)) => value,
                            None if matches!(target, LoadTarget::Column(_)) => continue,
                            None => None,
                        };
                        bind_target(db, session, table, target, field, charset, &mut row)?;
                    }
                }

                // G1 — apply SET clauses left-to-right; each sees prior results.
                for (col, expr) in &stmt.set_clauses {
                    let idx = table.column_index(col)?;
                    let value = if matches!(expr, Expr::Default(None)) {
                        resolve_column_default(&table.columns[idx])
                    } else {
                        crate::engine::expr::eval_expr(expr, table, &row)?
                    };
                    row[idx] = value;
                }

                // General/SET/charset paths must use the same assignment rounding
                // as the compact decoder, including DECIMAL(65,S).
                for &(index, scale) in &decimal_columns {
                    let value = &mut row[index];
                    let text = match &*value {
                        Value::String(s) | Value::Decimal(s) => Some(s.as_str()),
                        Value::SmallString(bytes) => std::str::from_utf8(bytes.as_slice()).ok(),
                        _ => None,
                    };
                    if let Some(rounded) =
                        text.and_then(|text| round_load_decimal_text(text, scale))
                    {
                        *value = Value::Decimal(rounded);
                    }
                }

                if stmt.ignore_duplicates || !load_is_strict() {
                    if let Err(err) = coerce_load_nulls(table, &mut row) {
                        if !stmt.ignore_duplicates {
                            return Err(err);
                        }
                        crate::engine::api::push_statement_condition(
                            crate::error::ConditionLevel::Warning,
                            &err,
                        );
                        add_load_skipped(1);
                        continue;
                    }
                }
                let row_bytes = row
                    .capacity()
                    .saturating_mul(std::mem::size_of::<Value>())
                    .saturating_add(
                        row.iter()
                            .map(|v| {
                                mem_budget::estimate_value_bytes(v)
                                    .saturating_sub(std::mem::size_of::<Value>())
                            })
                            .sum::<usize>(),
                    );
                batch_charge.try_grow(row_bytes)?;
                if batch.len() == batch.capacity() {
                    let additional = batch.capacity().max(256);
                    batch_charge.try_grow(additional * std::mem::size_of::<Vec<Value>>())?;
                    batch.reserve_exact(additional);
                }
                batch.push(row);

                if batch.len() >= batch_rows || batch_charge.bytes() >= batch_limit {
                    total_rows += flush_general_batch(
                        db,
                        session,
                        registry,
                        table_name,
                        std::mem::take(&mut batch),
                        stmt,
                        has_triggers,
                        has_conflict,
                        partition_filter,
                    )?;
                    batch_charge.shrink(batch_charge.bytes());
                }
            }

            if eof {
                break;
            }
        }
        if !batch.is_empty() {
            total_rows += flush_general_batch(
                db,
                session,
                registry,
                table_name,
                std::mem::take(&mut batch),
                stmt,
                has_triggers,
                has_conflict,
                partition_filter,
            )?;
        }

        crate::engine::cancel::check_cancelled()?;
        Ok(QueryResponse::OkAffected {
            affected_rows: total_rows,
        })
    })
}

/// Read at most 64 KiB plus the last record. Each allocation is admitted
/// before growth and remains StorageScratch-owned until the returned Vec dies.
/// Oversized records/documents fail with 1037 instead of bypassing the target.
fn read_general_load_chunk(
    reader: &mut impl BufRead,
    terminator: &[u8],
) -> Result<(Vec<u8>, mem_budget::ControlledMemoryCharge, bool), QueryError> {
    let mut content = Vec::new();
    let mut charge = mem_budget::ControlledMemoryCharge::try_new(
        mem_budget::ControlledMemoryCategory::StorageScratch,
        0,
    )?;
    loop {
        crate::engine::cancel::check_cancelled()?;
        let available = reader.fill_buf().map_err(io_err)?;
        if available.is_empty() {
            return Ok((content, charge, true));
        }
        // Stop at a possible terminator ending; ends_with checks also handle
        // multi-byte delimiters split across BufReader boundaries.
        let take = terminator
            .last()
            .and_then(|last| memchr(*last, available).map(|pos| pos + 1))
            .unwrap_or(available.len());
        let required = content.len().saturating_add(take);
        if required > content.capacity() {
            let capacity = required.max(content.capacity().saturating_mul(2)).max(4096);
            charge.try_grow(capacity - content.capacity())?;
            content.reserve_exact(capacity - content.len());
        }
        content.extend_from_slice(&available[..take]);
        reader.consume(take);
        if !terminator.is_empty() && content.len() >= 64 * 1024 && content.ends_with(terminator) {
            return Ok((content, charge, false));
        }
    }
}

/// Bind one field to its target: a table column (typed later by
/// `coerce_row`) or a `@user_var` (stored as a string on the session).
fn bind_target(
    _db: &Arc<Database>,
    session: &mut SessionContext,
    table: &TableSchema,
    target: &crate::parser::LoadTarget,
    field: Option<Vec<u8>>,
    charset: Option<&str>,
    row: &mut [Value],
) -> Result<(), QueryError> {
    use crate::parser::LoadTarget;
    match target {
        LoadTarget::Column(name) => {
            let idx = table.column_index(name)?;
            row[idx] = field_to_value(field, &table.columns[idx], charset)?;
        }
        LoadTarget::UserVar(name) => {
            let value = match field {
                None => Value::Null,
                Some(bytes) => str_to_string_value(&decode_field_text(&bytes, charset)?),
            };
            session.set_user_variable(name, value);
        }
    }
    Ok(())
}

/// LOAD has already expanded its input into physical table slots. Preserve
/// that shape through conflict and trigger handling, including invisible and
/// dropped columns, rather than interpreting it as a positional INSERT again.
#[allow(clippy::too_many_arguments)]
fn execute_load_rows_with_conflict(
    db: &Arc<Database>,
    txn: crate::storage::TxnView,
    table_name: &str,
    columns: Option<&[String]>,
    rows: Vec<Vec<Value>>,
    ignore: bool,
    replace: bool,
    on_duplicate: Option<&[crate::parser::Assignment]>,
    insert_alias: Option<&crate::parser::InsertAlias>,
    view_check: Option<&Expr>,
    returning: Option<&crate::parser::ReturningClause>,
    target_alias: Option<&str>,
) -> Result<QueryResponse, QueryError> {
    super::insert::execute_insert_rows::<false>(
        db,
        None,
        txn,
        table_name,
        columns,
        rows,
        ignore,
        replace,
        on_duplicate,
        insert_alias,
        view_check,
        returning,
        target_alias,
        true,
    )
}

#[allow(clippy::too_many_arguments)]
fn execute_load_rows_with_triggers(
    db: &Arc<Database>,
    session: &mut SessionContext,
    txn: crate::storage::TxnView,
    table_name: &str,
    columns: Option<&[String]>,
    rows: Vec<Vec<Value>>,
    ignore: bool,
    replace: bool,
    on_duplicate: Option<&[crate::parser::Assignment]>,
    insert_alias: Option<&crate::parser::InsertAlias>,
    view_check: Option<&Expr>,
    returning: Option<&crate::parser::ReturningClause>,
    target_alias: Option<&str>,
    registry: &SessionRegistry,
) -> Result<QueryResponse, QueryError> {
    super::insert::execute_insert_rows::<true>(
        db,
        Some(super::insert::InsertTriggers { session, registry }),
        txn,
        table_name,
        columns,
        rows,
        ignore,
        replace,
        on_duplicate,
        insert_alias,
        view_check,
        returning,
        target_alias,
        true,
    )
}

/// Insert one general-path batch, honouring triggers / IGNORE / REPLACE
/// and updating the Deleted/Skipped counters. Returns rows affected.
#[allow(clippy::too_many_arguments)]
fn flush_general_batch(
    db: &Arc<Database>,
    session: &mut SessionContext,
    registry: &SessionRegistry,
    table_name: &str,
    rows: Vec<Vec<Value>>,
    stmt: &crate::parser::LoadData,
    has_triggers: bool,
    has_conflict: bool,
    partition_filter: Option<&[u64]>,
) -> Result<u64, QueryError> {
    // Partition-clause validation mirrors the fast path.
    let table = db.table(table_name)?.clone();
    validate_load_data_partition_set(&table, partition_filter, &rows)?;

    if has_triggers {
        let records = rows.len() as u64;
        let txn = ensure_statement_txn(db, session)?;
        let result = execute_load_rows_with_triggers(
            db,
            session,
            txn,
            table_name,
            None,
            rows,
            stmt.ignore_duplicates,
            stmt.replace,
            None,
            None,
            None,
            None,
            None,
            registry,
        );
        return match result? {
            QueryResponse::OkAffected { affected_rows } => {
                record_triggered_load_batch(
                    db,
                    records,
                    affected_rows,
                    stmt.ignore_duplicates,
                    stmt.replace,
                );
                Ok(affected_rows)
            }
            _ => Ok(0),
        };
    }

    if has_conflict {
        // Try a bulk insert; on a duplicate-key/CHECK conflict fall back
        // to per-row IGNORE/REPLACE, counting Skipped/Deleted.
        let txn = ensure_statement_txn(db, session)?;
        let batch_savepoint = db.statement_savepoint(txn.txn_id)?;
        match db.insert_rows_bulk_detailed(txn, table_name, rows.clone(), false) {
            Ok(inserted) => {
                crate::engine::api::set_statement_insert_id(inserted.first_insert_id);
                let n = inserted.row_ids.len() as u64;

                bump_load_data_rows(db, n);
                Ok(n)
            }
            Err(e)
                if e.is(crate::error::catalog::ER_DUP_ENTRY)
                    || (stmt.ignore_duplicates
                        && e.is(crate::error::catalog::ER_CHECK_CONSTRAINT_VIOLATED)) =>
            {
                db.rollback_to_statement_savepoint(txn.txn_id, batch_savepoint)?;
                let mut affected = 0u64;
                for row in rows {
                    crate::engine::cancel::check_cancelled()?;
                    let result = execute_load_rows_with_conflict(
                        db,
                        ensure_statement_txn(db, session)?,
                        table_name,
                        None,
                        vec![row],
                        stmt.ignore_duplicates,
                        stmt.replace,
                        None,
                        None,
                        None,
                        None,
                        None,
                    );
                    match result {
                        Ok(QueryResponse::OkAffected { affected_rows }) => {
                            // REPLACE of an existing row reports 2 (delete +
                            // insert); attribute the extra to Deleted.
                            if stmt.replace && affected_rows >= 2 {
                                add_load_deleted(affected_rows - 1);
                            }
                            if affected_rows == 0 {
                                add_load_skipped(1);
                            }
                            affected += affected_rows;
                            bump_load_data_rows(db, affected_rows);
                        }
                        Ok(_) => {}
                        Err(e2)
                            if stmt.ignore_duplicates
                                && e2.is(crate::error::catalog::ER_DUP_ENTRY) =>
                        {
                            add_load_skipped(1);
                        }
                        Err(e2)
                            if stmt.ignore_duplicates
                                && e2.is(crate::error::catalog::ER_CHECK_CONSTRAINT_VIOLATED) =>
                        {
                            add_load_skipped(1);
                            crate::engine::api::push_statement_condition(
                                crate::error::ConditionLevel::Warning,
                                &e2,
                            );
                        }
                        Err(e2) => return Err(e2),
                    }
                }
                Ok(affected)
            }
            Err(e) => Err(e),
        }
    } else {
        let txn = ensure_statement_txn(db, session)?;
        let result = db
            .insert_rows_bulk_detailed(txn, table_name, rows, false)
            .map(|inserted| {
                crate::engine::api::set_statement_insert_id(inserted.first_insert_id);
                inserted.row_ids.len() as u64
            });
        let n = result?;
        bump_load_data_rows(db, n);
        Ok(n)
    }
}

type RawLoadRow = Vec<(Option<String>, Option<Vec<u8>>)>;

/// Build a bounded batch directly from records. Do not concatenate and then
/// re-scan the input: enclosure state and field decoding each run only once.
fn read_general_data_rows(
    reader: &mut impl BufRead,
    parser: &FieldParser,
    num_fields: usize,
) -> Result<(Vec<RawLoadRow>, mem_budget::ControlledMemoryCharge, bool), QueryError> {
    let mut record = Vec::new();
    let mut rows = Vec::new();
    let mut bytes = 0usize;
    let mut charge = mem_budget::ControlledMemoryCharge::try_new(
        mem_budget::ControlledMemoryCategory::StorageScratch,
        0,
    )?;
    loop {
        if !read_load_record(reader, parser, &mut record, num_fields)? {
            return Ok((rows, charge, true));
        }
        bytes = bytes.saturating_add(record.len().max(1));
        let actual = if parser.line_starting_by.is_empty() {
            record.as_slice()
        } else if let Some(pos) = find_subsequence(&record, &parser.line_starting_by) {
            &record[pos + parser.line_starting_by.len()..]
        } else {
            continue;
        };
        // Covers field vectors, byte copies and the row-list backing storage,
        // even for empty records and one-byte empty fields.
        charge.try_grow(actual.len().max(1).saturating_mul(128))?;
        rows.push(
            parser
                .parse_row_raw(actual)
                .into_iter()
                .map(|v| (None, v))
                .collect(),
        );
        if bytes >= 64 * 1024 {
            return Ok((rows, charge, false));
        }
    }
}

/// Parse LOAD XML input into per-row name→value lists. Supports the three
/// MySQL XML row shapes simultaneously so a mixed/either input works:
///   * attribute:  `<row col1="v1" col2="v2"/>`
///   * nested:     `<row><col1>v1</col1><col2>v2</col2></row>`
///   * field:      `<row><field name="col1">v1</field>...</row>`
/// The row element tag is taken from `ROWS IDENTIFIED BY '<tag>'`
/// (defaulting to `<row>`). A `xsi:nil="true"` element/attribute maps to
/// SQL NULL.
fn parse_xml_rows(
    content: &[u8],
    stmt: &crate::parser::LoadData,
) -> Result<Vec<Vec<(Option<String>, Option<Vec<u8>>)>>, QueryError> {
    let text = decode_field_text(content, stmt.charset.as_deref())?;
    let tag = stmt
        .xml_rows_identified_by
        .as_deref()
        .unwrap_or("<row>")
        .trim()
        .trim_start_matches('<')
        .trim_end_matches('>')
        .trim()
        .to_string();
    if tag.is_empty() {
        return Err(error(
            crate::error::catalog::ER_NONEXISTING_TABLE_GRANT,
            "LOAD XML: empty ROWS IDENTIFIED BY tag",
        ));
    }

    let bytes = text.as_bytes();
    let mut rows = Vec::new();
    let mut i = 0usize;
    while i < bytes.len() {
        // Find the next `<tag` whose following char is a tag boundary.
        let Some(open) = find_element_open(bytes, i, &tag) else {
            break;
        };
        // Locate the end of the opening tag `>`; note self-closing `/>`.
        let mut j = open + 1 + tag.len();
        let mut self_closing = false;
        while j < bytes.len() && bytes[j] != b'>' {
            j += 1;
        }
        if j >= bytes.len() {
            break;
        }
        if j > 0 && bytes[j - 1] == b'/' {
            self_closing = true;
        }
        let open_tag_inner = &text[open + 1 + tag.len()..if self_closing { j - 1 } else { j }];
        let mut fields: Vec<(Option<String>, Option<Vec<u8>>)> = Vec::new();
        parse_xml_attributes(open_tag_inner, &mut fields);

        if self_closing {
            rows.push(fields);
            i = j + 1;
            continue;
        }

        // Find the matching `</tag>` (no nested same-name rows expected).
        let body_start = j + 1;
        let close_marker = format!("</{tag}>");
        let Some(rel_close) = text[body_start..].find(&close_marker) else {
            break;
        };
        let body = &text[body_start..body_start + rel_close];
        parse_xml_children(body, &mut fields);
        rows.push(fields);
        i = body_start + rel_close + close_marker.len();
    }
    Ok(rows)
}

/// Find `<tag` at or after `from` with a proper tag-name boundary.
fn find_element_open(bytes: &[u8], from: usize, tag: &str) -> Option<usize> {
    let needle = tag.as_bytes();
    let mut i = from;
    while i + 1 + needle.len() <= bytes.len() {
        if bytes[i] == b'<' && bytes[i + 1..i + 1 + needle.len()].eq_ignore_ascii_case(needle) {
            let after = bytes.get(i + 1 + needle.len()).copied();
            if matches!(after, Some(b) if b.is_ascii_whitespace() || b == b'>' || b == b'/') {
                return Some(i);
            }
        }
        i += 1;
    }
    None
}

/// Parse `name="value"` / `name='value'` attributes from an opening-tag
/// interior. `xsi:nil="true"` sets no field. `col-with-xsi:nil` handling
/// is best-effort.
fn parse_xml_attributes(inner: &str, out: &mut Vec<(Option<String>, Option<Vec<u8>>)>) {
    let bytes = inner.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        while i < bytes.len() && (bytes[i].is_ascii_whitespace()) {
            i += 1;
        }
        let name_start = i;
        while i < bytes.len() && bytes[i] != b'=' && !bytes[i].is_ascii_whitespace() {
            i += 1;
        }
        if name_start == i {
            break;
        }
        let name = inner[name_start..i].trim().to_string();
        while i < bytes.len() && bytes[i].is_ascii_whitespace() {
            i += 1;
        }
        if i >= bytes.len() || bytes[i] != b'=' {
            continue;
        }
        i += 1; // skip '='
        while i < bytes.len() && bytes[i].is_ascii_whitespace() {
            i += 1;
        }
        if i >= bytes.len() || (bytes[i] != b'"' && bytes[i] != b'\'') {
            continue;
        }
        let quote = bytes[i];
        i += 1;
        let val_start = i;
        while i < bytes.len() && bytes[i] != quote {
            i += 1;
        }
        let raw = &inner[val_start..i.min(inner.len())];
        i += 1; // skip closing quote
        if name.eq_ignore_ascii_case("xsi:nil") {
            continue;
        }
        // Skip namespace declarations.
        if name.eq_ignore_ascii_case("xmlns") || name.to_ascii_lowercase().starts_with("xmlns:") {
            continue;
        }
        out.push((Some(name), Some(xml_unescape(raw).into_bytes())));
    }
}

/// Parse child elements of a row body into name→value pairs, covering both
/// the nested (`<col>v</col>`) and field (`<field name="col">v</field>`)
/// shapes.
fn parse_xml_children(body: &str, out: &mut Vec<(Option<String>, Option<Vec<u8>>)>) {
    let bytes = body.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] != b'<' {
            i += 1;
            continue;
        }
        // Skip closing tags / comments / declarations.
        if bytes.get(i + 1) == Some(&b'/')
            || bytes.get(i + 1) == Some(&b'!')
            || bytes.get(i + 1) == Some(&b'?')
        {
            i += 1;
            continue;
        }
        // Read element name.
        let name_start = i + 1;
        let mut k = name_start;
        while k < bytes.len()
            && !bytes[k].is_ascii_whitespace()
            && bytes[k] != b'>'
            && bytes[k] != b'/'
        {
            k += 1;
        }
        let elem_name = body[name_start..k].to_string();
        // Find end of opening tag.
        let mut t = k;
        while t < bytes.len() && bytes[t] != b'>' {
            t += 1;
        }
        if t >= bytes.len() {
            break;
        }
        let self_closing = t > 0 && bytes[t - 1] == b'/';
        let attr_inner = &body[k..if self_closing { t - 1 } else { t }];

        if elem_name.eq_ignore_ascii_case("field") {
            // field shape: name attr + inner text value.
            let mut attr_fields = Vec::new();
            parse_xml_attributes(attr_inner, &mut attr_fields);
            let col = attr_fields
                .iter()
                .find(|(k, _)| k.as_deref().map(|k| k.eq_ignore_ascii_case("name")) == Some(true))
                .and_then(|(_, v)| v.clone())
                .map(|b| String::from_utf8_lossy(&b).to_string());
            let is_nil = attr_inner.to_ascii_lowercase().contains("xsi:nil=\"true\"");
            if self_closing {
                if let Some(c) = col {
                    out.push((Some(c), None));
                }
                i = t + 1;
                continue;
            }
            let body_start = t + 1;
            let close = format!("</{elem_name}>");
            if let Some(rel) = body[body_start..].find(&close) {
                let value = &body[body_start..body_start + rel];
                if let Some(c) = col {
                    out.push((
                        Some(c),
                        if is_nil || value.is_empty() {
                            None
                        } else {
                            Some(xml_unescape(value).into_bytes())
                        },
                    ));
                }
                i = body_start + rel + close.len();
            } else {
                break;
            }
        } else {
            // nested shape: <col>value</col>.
            let is_nil = attr_inner.to_ascii_lowercase().contains("xsi:nil=\"true\"");
            if self_closing {
                out.push((Some(elem_name), None));
                i = t + 1;
                continue;
            }
            let body_start = t + 1;
            let close = format!("</{elem_name}>");
            if let Some(rel) = body[body_start..].find(&close) {
                let value = &body[body_start..body_start + rel];
                out.push((
                    Some(elem_name),
                    if is_nil || value.is_empty() {
                        None
                    } else {
                        Some(xml_unescape(value).into_bytes())
                    },
                ));
                i = body_start + rel + close.len();
            } else {
                break;
            }
        }
    }
}

/// Minimal XML entity unescape for the five predefined entities plus
/// numeric character references.
fn xml_unescape(s: &str) -> String {
    if !s.contains('&') {
        return s.to_string();
    }
    let mut out = String::with_capacity(s.len());
    let mut chars = s.char_indices().peekable();
    while let Some((idx, c)) = chars.next() {
        if c != '&' {
            out.push(c);
            continue;
        }
        // Read up to ';'.
        let rest = &s[idx + 1..];
        if let Some(semi) = rest.find(';') {
            let entity = &rest[..semi];
            let replacement = match entity {
                "lt" => Some('<'),
                "gt" => Some('>'),
                "amp" => Some('&'),
                "quot" => Some('"'),
                "apos" => Some('\''),
                _ if entity.starts_with("#x") || entity.starts_with("#X") => {
                    u32::from_str_radix(&entity[2..], 16)
                        .ok()
                        .and_then(char::from_u32)
                }
                _ if entity.starts_with('#') => {
                    entity[1..].parse::<u32>().ok().and_then(char::from_u32)
                }
                _ => None,
            };
            if let Some(r) = replacement {
                out.push(r);
                for _ in 0..entity.len() + 1 {
                    chars.next();
                }
                continue;
            }
        }
        out.push('&');
    }
    out
}

/// G6 — enforce `secure_file_priv` for server-side (non-LOCAL) LOAD DATA.
/// Errors with ER_OPTION_PREVENTS_STATEMENT (1290) when the requested
/// file is not permitted by the current `secure_file_priv` policy.
fn check_secure_file_priv(
    db: &Arc<Database>,
    session: &SessionContext,
    file_path: &str,
) -> Result<(), QueryError> {
    let value = crate::engine::sysvar::read_var(
        "secure_file_priv",
        crate::engine::sysvar::Scope::Global,
        Some(session),
        Some(db.as_ref()),
    )?;
    let value = match value {
        Value::String(s) => s,
        Value::Null => "NULL".to_string(),
        _ => {
            return Err(crate::error::catalog::ER_OPTION_PREVENTS_STATEMENT
                .error("Cannot determine the secure_file_priv policy"))
        }
    };

    let trimmed = value.trim();
    // Empty string → no restriction (MySQL's permissive default posture).
    if trimmed.is_empty() {
        return Ok(());
    }
    let opt_err = || {
        error(
            crate::error::catalog::ER_OPTION_PREVENTS_STATEMENT,
            "The MySQL server is running with the --secure-file-priv option so it cannot execute \
             this statement",
        )
    };
    // Literal NULL → all server-side file import/export is disabled.
    if trimmed.eq_ignore_ascii_case("NULL") {
        return Err(opt_err());
    }
    // Otherwise the file must resolve under the configured directory.
    // Canonicalize the directory once; canonicalize the file's parent so a
    // not-yet-existing leaf name (irrelevant for LOAD's read) still checks
    // its containing directory. Fall back to lexical prefix matching when
    // canonicalization fails (e.g. the dir does not exist on this host).
    let dir = std::path::Path::new(trimmed);
    let file = std::path::Path::new(file_path);
    let canon_dir = std::fs::canonicalize(dir).unwrap_or_else(|_| dir.to_path_buf());
    let canon_file = std::fs::canonicalize(file).unwrap_or_else(|_| file.to_path_buf());
    if canon_file.starts_with(&canon_dir) {
        Ok(())
    } else {
        Err(opt_err())
    }
}

fn load_data_parallel_degree(hints: &[crate::parser::QueryHint]) -> usize {
    let mut requested = None;
    let mut suppressed = false;
    for hint in hints {
        match hint {
            crate::parser::QueryHint::NoParallel => suppressed = true,
            crate::parser::QueryHint::Parallel { degree } => requested = Some(*degree),
            _ => {}
        }
    }
    if suppressed {
        return 1;
    }
    // Honour an explicit `/*+ PARALLEL(n) */` first; otherwise fall through
    // to the auto-parallel default. Mirrors planner SELECT behaviour: large
    // CPU-bound work runs in parallel unless the user opts out via
    // `MYDB_PARALLEL_DISABLE=1` or `/*+ NO_PARALLEL */`.
    //
    // Why default-on: profile (TPC-H SF=1 lineitem, 2026-06-03) showed the
    // serial path's writer thread saturated at ~525K rows/s while the
    // worker pool sat idle. The parallel-LOAD probe (PARALLEL_MIN_ROWS
    // / EOF check) inside execute_load_data_fast_parallel already routes
    // small files back to the synchronous path, so the default is safe
    // for tiny inputs.
    let degree = requested.unwrap_or_else(default_load_data_parallel_degree);
    let degree = if degree == 0 {
        default_load_data_parallel_degree()
    } else {
        degree
    };
    degree.max(1) as usize
}

fn default_load_data_parallel_degree() -> u32 {
    // Global parallel kill switch (matches the planner's parser at
    // `src/planner.rs:3651`). Accepts `1` / `true` / `on` case-insensitive.
    if crate::util::env::var("MYDB_PARALLEL_DISABLE")
        .ok()
        .map(|v| v == "1" || v.eq_ignore_ascii_case("true") || v.eq_ignore_ascii_case("on"))
        .unwrap_or(false)
    {
        return 1;
    }
    crate::util::env::var("MYDB_PARALLEL_DEGREE")
        .ok()
        .and_then(|s| s.parse::<u32>().ok())
        .filter(|&n| n >= 1)
        .unwrap_or(2)
}

fn parallel_load_chunk_size(
    prepared_indexes: &[PreparedLoadIndex],
    degree: usize,
    pipeline_budget: usize,
) -> usize {
    // Bound the whole pipeline, rather than just one prepared batch, by the
    // per-query watermark.  With one input slot per worker and a one-slot
    // result channel, the coordinator/writer and workers can collectively own
    // up to roughly two batches per worker plus one batch at each end of the
    // pipeline.  Dividing by that actual fan-out prevents otherwise-safe
    // batches from jointly exhausting MYDB_MEMORY_TARGET once the buffer pool
    // is resident.
    //
    // The fixed per-row estimate covers raw line offsets/payload and encoded
    // row Vec headers; every ordinary index adds one inline IndexKeysForRow
    // (240 B on the current layout). Exact capacity is still charged before
    // queueing, so unusually wide variable values fail safely instead of
    // escaping the governor.
    parallel_load_chunk_size_for_limit(prepared_indexes, degree, pipeline_budget)
}

fn parallel_load_chunk_size_for_limit(
    prepared_indexes: &[PreparedLoadIndex],
    degree: usize,
    query_limit: usize,
) -> usize {
    // Pair-int LOAD keys stay as two i64s from parser to sorter. Charging
    // them as the old 240-byte generic IndexKey shrank strict SF=1 batches to
    // ~19k rows and forced hundreds of WAL durability barriers. Keep the
    // conservative raw/encoded-row allowance, but size the index portion by
    // its actual queued representation. Exact capacities are still charged
    // by RawLoadDataBatch/LoadDataPreparedBatch before either channel send.
    let index_bytes = prepared_indexes.iter().fold(0usize, |sum, prepared| {
        sum.saturating_add(match prepared {
            PreparedLoadIndex::Encoded { program, .. } => program
                .fixed_record_width()
                .unwrap_or(std::mem::size_of::<IndexKeysForRow>() + 64),
            PreparedLoadIndex::Fallback { .. } => std::mem::size_of::<IndexKeysForRow>(),
        })
    });
    let per_row = 256usize.saturating_add(index_bytes);
    let pipeline_slots = degree.max(1).saturating_mul(2).saturating_add(2);
    query_limit
        .saturating_div(pipeline_slots)
        .saturating_div(per_row.max(1))
        .clamp(PARALLEL_MIN_ROWS, PARALLEL_CHUNK_SIZE)
}

fn parallel_load_observed_chunk_rows(
    configured_rows: usize,
    observed_rows: usize,
    observed_bytes: usize,
    degree: usize,
) -> usize {
    if observed_rows == 0 || observed_bytes == 0 {
        return configured_rows;
    }
    let bytes_per_row = observed_bytes.div_ceil(observed_rows);
    // Bound the dispatch working set across the pipeline, not per worker.
    // Otherwise a medium input can fit in one large batch and leave nearly
    // every already-created worker idle until EOF.
    let slots = degree.max(1).saturating_mul(2).saturating_add(2);
    let target_bytes = PARALLEL_CHUNK_TARGET_BYTES / slots;
    configured_rows
        .min(target_bytes / bytes_per_row.max(1))
        .max(PARALLEL_MIN_ROWS.min(configured_rows))
}

fn load_data_zone_map_observe_enabled() -> bool {
    crate::util::env::var("MYDB_ZONE_MAP_LOAD_OBSERVE").as_deref() == Ok("1")
}

/// Only generated identities must be assigned before index-key preparation.
/// Explicit-only batches retain the storage insert path's single update of
/// the high-water mark, avoiding a second reservation/coercion pass.
fn assign_load_identities_before_keys(
    db: &Database,
    txn: TxnView,
    table: &TableSchema,
    rows: &mut [Vec<Value>],
) -> Result<u64, QueryError> {
    let Some(index) = table.auto_increment_column_index() else {
        return Ok(0);
    };
    let generate_zero = crate::engine::expr::session_temporal_sql_mode_bits()
        & crate::engine::api::SQL_MODE_NO_AUTO_VALUE_ON_ZERO
        == 0;
    let needs_identity = rows.iter().any(|row| {
        matches!(row.get(index), Some(Value::Null))
            || (generate_zero && matches!(row.get(index), Some(Value::Int(0) | Value::UInt(0))))
    });
    if needs_identity {
        db.assign_load_auto_increment_values(txn, table, rows)
    } else {
        Ok(0)
    }
}

fn execute_load_data_fast(
    db: &Arc<Database>,
    session: &mut SessionContext,
    table_name: &str,
    table: &TableSchema,
    column_indexes: &[usize],
    full_width_identity: bool,
    coercion_plan: &[CoercionOp],
    parser: &FieldParser,
    reader: &mut BufReader<Box<dyn Read>>,
    num_fields: usize,
    partition_filter: Option<&[u64]>,
) -> Result<QueryResponse, QueryError> {
    super::tx::execute_in_transaction_with_session(db, session, |db, _session, txn| {
        let mut total_rows: u64 = 0;
        let mut buf = Vec::with_capacity(READ_BUF_SIZE);
        let prepared_indexes = prepare_load_indexes(table)?;

        let mut index_builders = db.load_data_index_builders(table)?;

        loop {
            crate::engine::cancel::check_cancelled()?;
            let (rows, eof) = parse_chunk(
                reader,
                &mut buf,
                parser,
                coercion_plan,
                num_fields,
                bulk_chunk_rows(),
            )?;

            if rows.is_empty() {
                break;
            }

            let mut mapped_rows = map_rows(rows, table, column_indexes, full_width_identity);
            let insert_id = assign_load_identities_before_keys(db, txn, table, &mut mapped_rows)?;
            crate::engine::api::set_statement_insert_id(insert_id);
            materialize_generated_rows(db, table, &mut mapped_rows)?;
            validate_load_data_partition_set(table, partition_filter, &mapped_rows)?;

            let chunk_keys = precompute_index_keys(table, &prepared_indexes, &mapped_rows)?;

            let inserted = db.insert_rows_heap_only_detailed(txn, table_name, mapped_rows)?;
            crate::engine::api::set_statement_insert_id(inserted.first_insert_id);
            let row_ids = inserted.row_ids;
            total_rows += row_ids.len() as u64;
            bump_load_data_rows(db, row_ids.len() as u64);

            index_builders.push_batch(db, txn.txn_id, chunk_keys, &row_ids)?;

            if eof {
                break;
            }
        }

        let result = if total_rows > 0 {
            index_builders
                .finish(db, txn.txn_id)
                .map(|()| QueryResponse::OkAffected {
                    affected_rows: total_rows,
                })
        } else {
            Ok(QueryResponse::OkAffected {
                affected_rows: total_rows,
            })
        };
        result
    })
}

enum LoadReplayStorage {
    Memory {
        bytes: std::io::Cursor<Vec<u8>>,
        _charge: mem_budget::ControlledMemoryCharge,
    },
    Spill(super::spill_quota::ProtocolSpillFile),
}

impl LoadReplayStorage {
    fn rewind(&mut self) -> Result<(), QueryError> {
        match self {
            Self::Memory { bytes, .. } => {
                bytes.set_position(0);
                Ok(())
            }
            Self::Spill(file) => file.rewind(),
        }
    }
}

impl Read for LoadReplayStorage {
    fn read(&mut self, output: &mut [u8]) -> std::io::Result<usize> {
        match self {
            Self::Memory { bytes, .. } => bytes.read(output),
            Self::Spill(file) => file.read(output),
        }
    }
}

type LoadReplayFile = std::rc::Rc<std::cell::RefCell<LoadReplayStorage>>;

struct LoadReplayReader(Option<LoadReplayFile>);

impl Read for LoadReplayReader {
    fn read(&mut self, output: &mut [u8]) -> std::io::Result<usize> {
        if output.is_empty() {
            return Ok(0);
        }
        let n = match &self.0 {
            Some(file) => file.borrow_mut().read(output)?,
            None => return Ok(0),
        };
        if n == 0 {
            self.0 = None;
        }
        Ok(n)
    }
}

struct LoadReplayRemainder {
    bytes: std::io::Cursor<Vec<u8>>,
    _charge: mem_budget::ControlledMemoryCharge,
}

impl Read for LoadReplayRemainder {
    fn read(&mut self, output: &mut [u8]) -> std::io::Result<usize> {
        self.bytes.read(output)
    }
}

/// A private-generation attempt may discover a duplicate only after sorting
/// the complete input. Retain a quota-accounted replay copy, never the complete
/// input in RAM. Temp space is optional: if admission fails partway through,
/// join the written prefix, unwritten bytes and original stream without losing
/// or duplicating a byte, and continue on the ordinary IGNORE path.
fn stage_ignore_load_input(
    db: &Database,
    mut source: Box<dyn Read>,
    connection_id: u32,
) -> Result<(Box<dyn Read>, Option<LoadReplayFile>), QueryError> {
    const COPY_BYTES: usize = 1024 * 1024;
    let Some(charge) = mem_budget::ControlledMemoryCharge::try_new_optional(
        mem_budget::ControlledMemoryCategory::StorageScratch,
        COPY_BYTES,
    ) else {
        return Ok((source, None));
    };
    let mut bytes = Vec::new();
    if bytes.try_reserve_exact(COPY_BYTES).is_err() {
        return Ok((source, None));
    }
    bytes.resize(64 * 1024, 0);
    let mut used = 0usize;
    while used < COPY_BYTES {
        crate::engine::cancel::check_cancelled()?;
        if used == bytes.len() {
            bytes.resize((bytes.len() * 2).min(COPY_BYTES), 0);
        }
        match source.read(&mut bytes[used..]) {
            Ok(0) => {
                bytes.truncate(used);
                let file = std::rc::Rc::new(std::cell::RefCell::new(LoadReplayStorage::Memory {
                    bytes: std::io::Cursor::new(bytes),
                    _charge: charge,
                }));
                return Ok((Box::new(LoadReplayReader(Some(file.clone()))), Some(file)));
            }
            Ok(n) => used += n,
            Err(err) if err.kind() == std::io::ErrorKind::Interrupted => continue,
            Err(err) => return Err(io_err(err)),
        }
    }
    let mut file = match super::spill_quota::ProtocolSpillFile::create(db.data_dir(), connection_id)
    {
        Ok(file) => file,
        Err(_) => {
            let remainder = LoadReplayRemainder {
                bytes: std::io::Cursor::new(bytes),
                _charge: charge,
            };
            return Ok((Box::new(remainder.chain(source)), None));
        }
    };
    let mut n = used;
    loop {
        crate::engine::cancel::check_cancelled()?;
        let mut offset = 0usize;
        while offset < n {
            match file.write(&bytes[offset..n]) {
                Ok(0) | Err(_) => {
                    file.rewind()?;
                    bytes.truncate(n);
                    let mut remainder = std::io::Cursor::new(bytes);
                    remainder.set_position(offset as u64);
                    let remainder = LoadReplayRemainder {
                        bytes: remainder,
                        _charge: charge,
                    };
                    let prefix =
                        std::rc::Rc::new(std::cell::RefCell::new(LoadReplayStorage::Spill(file)));
                    return Ok((
                        Box::new(
                            LoadReplayReader(Some(prefix))
                                .chain(remainder)
                                .chain(source),
                        ),
                        None,
                    ));
                }
                Ok(written) => offset += written,
            }
        }
        n = 0;
        while n < COPY_BYTES {
            crate::engine::cancel::check_cancelled()?;
            match source.read(&mut bytes[n..]) {
                Ok(0) => break,
                Ok(read) => n += read,
                Err(err) if err.kind() == std::io::ErrorKind::Interrupted => continue,
                Err(err) => return Err(io_err(err)),
            }
        }
        if n == 0 {
            file.rewind()?;
            let file = std::rc::Rc::new(std::cell::RefCell::new(LoadReplayStorage::Spill(file)));
            return Ok((Box::new(LoadReplayReader(Some(file.clone()))), Some(file)));
        }
    }
}

const IGNORE_LOAD_REPLAY: &str = "parallel IGNORE load requires ordered replay";

fn ignore_load_replay_error() -> QueryError {
    error(
        crate::error::catalog::ER_NOT_SUPPORTED_YET,
        IGNORE_LOAD_REPLAY,
    )
}

fn ignore_load_can_replay(err: &QueryError) -> bool {
    err.sql_state().as_str().starts_with("22")
        || err.message() == IGNORE_LOAD_REPLAY
        || (err.is(crate::error::catalog::ER_GET_ERRNO)
            && err.message() == "shadow generation requires a physically empty table")
        || err.is_any(&[
            crate::error::catalog::ER_DUP_ENTRY,
            crate::error::catalog::ER_LOCK_WAIT_TIMEOUT,
            crate::error::catalog::ER_OUTOFMEMORY,
            crate::error::catalog::ER_OUT_OF_RESOURCES,
            crate::error::catalog::ER_BAD_NULL_ERROR,
            crate::error::catalog::ER_WARN_DATA_OUT_OF_RANGE,
            crate::error::catalog::ER_TRUNCATED_WRONG_VALUE,
            crate::error::catalog::ER_WRONG_VALUE_FOR_TYPE,
            crate::error::catalog::ER_TRUNCATED_WRONG_VALUE_FOR_FIELD,
            crate::error::catalog::ER_DATA_TOO_LONG,
            crate::error::catalog::WARN_DATA_TRUNCATED,
            crate::error::catalog::ER_INVALID_CHARACTER_STRING,
        ])
}

#[allow(clippy::too_many_arguments)]
fn execute_load_data_ignore_parallel(
    db: &Arc<Database>,
    session: &mut SessionContext,
    table_name: &str,
    table: &TableSchema,
    column_indexes: &[usize],
    full_width_identity: bool,
    coercion_plan: &[CoercionOp],
    parser: &FieldParser,
    format: FastLoadDataFormat,
    reader: &mut BufReader<Box<dyn Read>>,
    num_fields: usize,
    degree: usize,
    partition_filter: Option<&[u64]>,
) -> Result<QueryResponse, QueryError> {
    let source = std::mem::replace(reader, BufReader::new(Box::new(std::io::empty())));
    let (source, replay) = stage_ignore_load_input(db, Box::new(source), session.connection_id)?;
    *reader = BufReader::with_capacity(READ_BUF_SIZE, source);
    if let Some(replay) = replay {
        // Trial diagnostics are private. Any warning forces an ordered replay
        // before publication, preserving both conversion and warning order.
        let warnings = crate::engine::api::StatementWarningSink::default();
        let result = crate::engine::api::with_statement_warning_sink(warnings, || {
            let _ignore = crate::storage::DmlIgnoreCoercionGuard::new(false);
            let _decode = LoadDecodePolicyGuard::new(false);
            execute_load_data_fast_parallel(
                db,
                session,
                table_name,
                table,
                column_indexes,
                full_width_identity,
                coercion_plan,
                format,
                reader,
                num_fields,
                degree,
                partition_filter,
                true,
                true,
            )
        });
        match result {
            Ok(response) => return Ok(response),
            Err(err) => {
                // Decode/I/O admission can fail before the writer's normal
                // finalization. Workers have stopped before rollback/replay.
                let err = finish_statement_txn::<()>(db, session, Err(err)).unwrap_err();
                if !ignore_load_can_replay(&err) {
                    return Err(err);
                }
                replay.borrow_mut().rewind()?;
                *reader = BufReader::with_capacity(
                    READ_BUF_SIZE,
                    Box::new(LoadReplayReader(Some(replay))),
                );
                reset_load_counters();
            }
        }
    }
    execute_load_data_with_conflict(
        db,
        session,
        table_name,
        table,
        column_indexes,
        full_width_identity,
        coercion_plan,
        parser,
        reader,
        num_fields,
        true,
        false,
        None,
        partition_filter,
    )
}

#[allow(clippy::too_many_arguments)]
fn execute_load_data_fast_parallel(
    db: &Arc<Database>,
    session: &mut SessionContext,
    table_name: &str,
    table: &TableSchema,
    column_indexes: &[usize],
    full_width_identity: bool,
    coercion_plan: &[CoercionOp],
    format: FastLoadDataFormat,
    reader: &mut BufReader<Box<dyn Read>>,
    num_fields: usize,
    degree: usize,
    partition_filter: Option<&[u64]>,
    use_shadow_generation: bool,
    ignore_trial: bool,
) -> Result<QueryResponse, QueryError> {
    let prepared_indexes = prepare_load_indexes(table)?;
    let sorter_count = if table.indexes.is_empty() || table.is_partitioned() {
        0
    } else {
        table.indexes.len()
    };
    let (_, pipeline_budget) = db.load_data_memory_budgets(sorter_count, !use_shadow_generation);
    let mut chunk_rows = parallel_load_chunk_size(&prepared_indexes, degree, pipeline_budget);
    // Probe the input to decide whether parallel execution is worth the
    // setup cost. We read in chunks until we either: (a) hit EOF, in which
    // case we know the whole file is small and we run a single-pass
    // synchronous path; or (b) accumulate at least PARALLEL_MIN_ROWS, in
    // which case we know the file is large enough to warrant spinning up
    // workers.
    //
    // The earlier version also required `first_batches.len() > 1` to break,
    // and fell back to the synchronous path whenever only one chunk had
    // been read so far. With the smaller chunk size used there, that meant any
    // file > PARALLEL_MIN_ROWS but where the first chunk wasn't itself
    // smaller than PARALLEL_MIN_ROWS would silently run serial.
    // Concretely: TPC-H lineitem (~6M rows) always read exactly one
    // single probe chunk on the first iteration, then fell straight into
    // `execute_preraw_load_data_fast` — the parallel workers never saw a
    // single batch. profile-p2 showed 100 % of LOAD DATA cycles on the
    // single executor thread despite `PARALLEL(2)`. Drop the spurious
    // single-batch guard; only EOF (truly small input) sends us to the
    // synchronous path.
    let mut first_batches = Vec::new();
    let mut total_input_rows = 0usize;
    let mut probe_eof = false;
    let mut raw_data_capacity_hint = 0usize;

    loop {
        crate::engine::cancel::check_cancelled()?;
        let (batch, eof) = parse_raw_chunk_fast(
            db,
            reader,
            chunk_rows.min(PARALLEL_MIN_ROWS),
            &mut raw_data_capacity_hint,
            format,
        )?;
        if !batch.is_empty() {
            chunk_rows = parallel_load_observed_chunk_rows(
                chunk_rows,
                batch.len(),
                batch.data.len(),
                degree,
            );
            raw_data_capacity_hint = raw_data_capacity_hint
                .min(PARALLEL_CHUNK_TARGET_BYTES + PARALLEL_CHUNK_TARGET_BYTES / 4);
            total_input_rows += batch.len();
            first_batches.push(batch);
        }
        if eof {
            probe_eof = true;
            break;
        }
        if total_input_rows >= PARALLEL_MIN_ROWS {
            break;
        }
    }

    if probe_eof && total_input_rows < PARALLEL_MIN_ROWS {
        if ignore_trial {
            return Err(ignore_load_replay_error());
        }
        return execute_preraw_load_data_fast(
            db,
            session,
            table_name,
            table,
            column_indexes,
            full_width_identity,
            coercion_plan,
            format,
            num_fields,
            first_batches,
            partition_filter,
        );
    }

    // AUTO_INCREMENT no longer forces every row through the writer. On an
    // empty-table shadow generation, explicit-only batches are validated and
    // encoded by workers; the coordinator only advances the live sequence
    // high-water mark. A batch containing NULL/zero still returns decoded rows
    // so the writer can allocate identities in input order before appending it
    // to the same shadow stream. Non-shadow and binlog paths retain the
    // authoritative coordinator route.
    let workers = degree;
    let auto_increment_idx = table.auto_increment_column_index();
    let writer_encode =
        crate::binlog::enabled() || (auto_increment_idx.is_some() && !use_shadow_generation);
    let no_auto_value_on_zero = crate::engine::expr::session_temporal_sql_mode_bits()
        & crate::engine::api::SQL_MODE_NO_AUTO_VALUE_ON_ZERO
        != 0;
    // ensure_statement_txn before spawning workers so they can stamp
    // the in-payload MVCC header with the actual txn id. The header
    // value must match what `insert_pre_encoded_rows_heap_only` will
    // pass through; using a placeholder here would corrupt
    // visibility checks at read time.
    let txn = if use_shadow_generation {
        ensure_statement_dml_txn(db, session)?
    } else {
        ensure_statement_txn(db, session)?
    };
    let txn_id = txn.txn_id;
    // Acquire the table X lock and create the unreachable generation before
    // workers start. Eligibility is revalidated under that lock by storage,
    // closing the empty-table race with a concurrent writer.
    let shadow_builder = if use_shadow_generation {
        Some(db.begin_shadow_generation(txn_id, Arc::new(table.clone()))?)
    } else {
        None
    };
    let shadow_heap_appender = shadow_builder
        .as_ref()
        .map(crate::storage::ShadowGenerationBuilder::heap_appender);
    let table_name_owned = table_name.to_owned();
    let partition_filter = partition_filter.map(|ids| ids.to_vec());
    // The byte parser below already produces storage-canonical values for
    // this common subset.  Remember that once per statement so workers can
    // replace the generic second coercion pass with cheap invariant checks.
    // Unsupported types keep the authoritative storage coercer unchanged.
    let parsed_values_are_canonical = load_fast_values_are_canonical(table);
    // Statement-level option: resolving it in the per-row loop serialises all
    // parse workers on libc's process-environment lock.  At TPC-H scale that
    // meant one getenv for every source row (and millions of contended lock
    // acquisitions) even though the value cannot change during a statement.
    let observe_zone_map = load_data_zone_map_observe_enabled();
    let compiled_row_program = if !writer_encode && !table.is_partitioned() && !observe_zone_map {
        CompiledLoadRowProgram::compile(
            table,
            full_width_identity,
            format,
            coercion_plan,
            num_fields,
            &prepared_indexes,
        )
    } else {
        None
    };

    let mut writer = LoadDataPreparedWriter::new_with_txn(
        db,
        session,
        table_name,
        table,
        txn,
        shadow_builder,
        shadow_heap_appender.clone(),
        partition_filter.clone(),
    )?;

    // Keep both sides of the pipeline bounded. One queued input per worker
    // plus one queued result prevents arbitrary run-ahead while still letting
    // the coordinator enqueue before draining a worker that is blocked on its
    // result send (zero-capacity input would form a circular wait here).
    let (result_tx, result_rx) = mpsc::sync_channel(1);
    let mut input_txs = Vec::with_capacity(workers);
    let mut handles = Vec::with_capacity(workers);
    let session_info = crate::engine::expr::current_session_expr_info();
    let first_worker_error = Arc::new(parking_lot::Mutex::new(None::<QueryError>));
    for _ in 0..workers {
        let (input_tx, input_rx) = mpsc::sync_channel::<(usize, RawLoadDataBatch)>(1);
        input_txs.push(input_tx);
        let result_tx = result_tx.clone();
        let table = table.clone();
        let column_indexes = column_indexes.to_vec();
        let coercion_plan = coercion_plan.to_vec();
        let prepared_indexes = prepared_indexes.clone();
        let partition_filter = partition_filter.clone();
        let db = db.clone();
        let table_name = table_name_owned.clone();
        let compiled_row_program = compiled_row_program.clone();
        let shadow_heap_appender = shadow_heap_appender.clone();
        let error_heap_appender = shadow_heap_appender.clone();
        let warning_sink =
            crate::engine::api::current_statement_warning_sink().expect("LOAD warning scope");
        let session_info = session_info.clone();
        let first_worker_error = first_worker_error.clone();
        handles.push(thread::spawn(move || {
            // Clone a sender for the terminal-error channel: on `?` the
            // worker would otherwise just drop `result_tx` and the
            // coordinator would see a bare `Disconnected` (generic 1969),
            // losing the real MySQL error code (e.g. 1264 out-of-range from
            // `coerce_row`). Send the actual error before returning.
            let err_tx = result_tx.clone();
            let run = move || -> Result<(), QueryError> {
                let _ignore =
                    ignore_trial.then(|| crate::storage::DmlIgnoreCoercionGuard::new(false));
                let _decode = ignore_trial.then(|| LoadDecodePolicyGuard::new(false));
                let codec = table.row_codec.clone();
                let has_generated = table_has_generated_columns(&table);
                // Per-worker scratch buffer for the parsed row. With
                // `full_width_identity` (the LOAD DATA shape that doesn't
                // list explicit columns — i.e. the lineitem-shaped TPC-H
                // / sysbench path) we parse straight into this buffer
                // and reuse the inner String/Vec backing capacity across
                // every row in the batch. Previous shape allocated a
                // fresh `Vec<Value>` per row (~400 B for lineitem); the
                // resulting 6M-row × 400 B churn showed up in profile as
                // 15.93% of total CPU on `parse_row_fast_single_byte`
                // alone and another 2.17% on the eventual mass
                // `drop_in_place::<[Vec<Value>]>`.
                let mut row_scratch: Vec<Value> = Vec::with_capacity(num_fields);
                // Mixed fixed-width indexes (for example INT+DATE+TIMESTAMP)
                // retain only one table-width scalar scratch per worker. It is
                // mandatory statement memory, charged by capacity to the same
                // StorageScratch category as the surrounding LOAD pipeline and
                // released when this worker exits.
                let index_scratch_bytes = num_fields.saturating_mul(std::mem::size_of::<Value>());
                let _index_scratch_charge =
                    reserve_load_data_batch_memory(&db, index_scratch_bytes)?;
                let mut index_value_scratch = Vec::<Value>::with_capacity(num_fields);
                let mut escaped_field_scratch = EscapedLoadFieldScratch::new()?;
                for (batch_idx, raw_batch) in input_rx {
                    let row_count = raw_batch.len();
                    let direct_program = compiled_row_program.as_ref().filter(|_| {
                        auto_increment_idx.is_none()
                            || !raw_batch.lines().any(|line| {
                                load_line_auto_increment_needs_assignment(
                                    line,
                                    format,
                                    auto_increment_idx.expect("checked above"),
                                    no_auto_value_on_zero,
                                )
                            })
                    });
                    let retain_decoded_rows = writer_encode || auto_increment_idx.is_some();
                    let mut decoded_rows: Vec<Vec<Value>> = if retain_decoded_rows {
                        Vec::with_capacity(row_count)
                    } else {
                        Vec::new()
                    };
                    let mut payloads: Vec<Vec<u8>> = if writer_encode {
                        Vec::new()
                    } else {
                        Vec::with_capacity(row_count)
                    };
                    let mut auto_increment_needs_assignment = false;
                    let mut explicit_auto_increment_max = 0u64;
                    // In the worker-encode path we build a single compact
                    // per-batch aggregate rather than allocating per-row
                    // projection data for a wide table. Precision
                    // within a batch is intentionally coarse — the bulk-
                    // load pipeline runs `ANALYZE TABLE` afterwards which
                    // rebuilds tight per-zone summaries from the heap.
                    let zone_operation = db.zone_map_operation();
                    let mut zone_map_worker_batch = if writer_encode || zone_operation.is_none() {
                        crate::storage::ZoneMapWorkerBatch::default()
                    } else {
                        crate::storage::ZoneMapWorkerBatch::for_table(&table)
                    };
                    let mut per_index_keys: Vec<LoadDataIndexKeyBatch> = if table.indexes.is_empty()
                    {
                        Vec::new()
                    } else {
                        prepared_indexes
                            .iter()
                            .map(|prepared| match prepared {
                                PreparedLoadIndex::Encoded { program, .. } => {
                                    new_encoded_load_key_batch(program, row_count)
                                }
                                PreparedLoadIndex::Fallback { .. } => {
                                    LoadDataIndexKeyBatch::Generic(Vec::with_capacity(row_count))
                                }
                            })
                            .collect()
                    };
                    let parse_encode_started = Instant::now();
                    // Validate shared lexical properties once over the raw
                    // batch. ASCII without escapes stays ASCII in every field,
                    // so direct CHAR/VARCHAR validation need not rescan it.
                    let plain_ascii_batch = direct_program.is_some_and(|program| {
                        raw_batch.data.is_ascii()
                            && program
                                .escaped_by
                                .is_none_or(|escape| memchr(escape, &raw_batch.data).is_none())
                    });
                    let mut direct_decode_rows = 0u64;
                    let mut direct_decode_fallback_rows = 0u64;
                    for line in raw_batch.lines() {
                        row_scratch.clear();
                        let direct_payload = match direct_program {
                            Some(program) => program.decode_encode(
                                line,
                                plain_ascii_batch,
                                txn_id,
                                &mut index_value_scratch,
                                &mut escaped_field_scratch,
                                &mut per_index_keys,
                                &mut explicit_auto_increment_max,
                            )?,
                            None => None,
                        };
                        if let Some(payload) = direct_payload {
                            direct_decode_rows += 1;
                            payloads.push(payload);
                            continue;
                        } else {
                            if direct_program.is_some() {
                                direct_decode_fallback_rows += 1;
                            }
                            parse_row_fast_single_byte_into(
                                line,
                                format,
                                &coercion_plan,
                                num_fields,
                                &mut row_scratch,
                            )?;
                        }
                        // `map_rows` reorders an explicit-columns row into
                        // the table's column order. `full_width_identity`
                        // (no column list given by the user) skips the
                        // reorder, so we operate in place on `row_scratch`.
                        let values_ref: &mut Vec<Value> = if full_width_identity {
                            row_scratch.extend(
                                table.columns[row_scratch.len()..]
                                    .iter()
                                    .map(resolve_load_missing_default),
                            );
                            &mut row_scratch
                        } else {
                            // Slow path: allocate a fresh table-shaped row
                            // per source row. Kept for correctness; rare
                            // in practice (TPC-H/sysbench/BenchmarkSQL all
                            // hit the identity path).
                            let mut mapped: Vec<Value> = table
                                .columns
                                .iter()
                                .map(|c| resolve_load_missing_default(c))
                                .collect();
                            for (idx, value) in column_indexes.iter().zip(row_scratch.drain(..)) {
                                mapped[*idx] = value;
                            }
                            decoded_rows.push(mapped);
                            decoded_rows.last_mut().unwrap()
                        };

                        if let Some(auto_idx) = auto_increment_idx {
                            match values_ref.get(auto_idx) {
                                Some(Value::Null) => auto_increment_needs_assignment = true,
                                Some(Value::Int(0) | Value::UInt(0)) if !no_auto_value_on_zero => {
                                    auto_increment_needs_assignment = true
                                }
                                Some(Value::Int(value)) if *value > 0 => {
                                    explicit_auto_increment_max =
                                        explicit_auto_increment_max.max(*value as u64);
                                }
                                Some(Value::UInt(value)) if *value > 0 => {
                                    explicit_auto_increment_max =
                                        explicit_auto_increment_max.max(*value);
                                }
                                Some(_) => {}
                                None => {
                                    return Err(error(
                                        crate::error::catalog::ER_WRONG_VALUE_COUNT_ON_ROW,
                                        "column count does not match AUTO_INCREMENT column",
                                    ));
                                }
                            }
                        }

                        let row_needs_identity = auto_increment_idx.is_some_and(|idx| {
                            matches!(values_ref.get(idx), Some(Value::Null))
                                || (!no_auto_value_on_zero
                                    && matches!(
                                        values_ref.get(idx),
                                        Some(Value::Int(0) | Value::UInt(0))
                                    ))
                        });
                        if row_needs_identity {
                            if full_width_identity {
                                decoded_rows.push(std::mem::take(&mut row_scratch));
                            }
                            // Keys, generated columns and partition membership
                            // depend on the identity that the writer will assign.
                            continue;
                        }

                        if has_generated {
                            db.materialize_generated_columns_for_insert(&table, values_ref)?;
                        }
                        // B3 — the parallel-encode branch (`!writer_encode`)
                        // pre-encodes rows and inserts them via
                        // `insert_pre_encoded_rows_heap_only_detailed`, which
                        // does NOT run `coerce_row`. Without this the fast
                        // per-field `coerce_field` (which never range-checks
                        // integers or validates column charset) would let
                        // out-of-range / invalid values land silently — a
                        // divergence from the serial path (which coerces in
                        // `insert_rows_heap_only_detailed`) triggered purely by
                        // a PARALLEL hint. Coerce here so index keys AND the
                        // encoded payload both reflect the authoritative value.
                        // The `writer_encode` branch hands decoded rows to
                        // `insert_rows_heap_only_detailed`, which coerces there.
                        if !writer_encode {
                            if parsed_values_are_canonical {
                                // The identity parser's coercion plan already
                                // enforces integer ranges and DECIMAL(P,S)
                                // bounds per field. Avoid a second full-width
                                // schema/value walk before encoding. Explicit
                                // column mappings also contain defaulted table
                                // values, so they retain the row oracle below.
                                if !full_width_identity {
                                    validate_canonical_load_row(&table, values_ref)?;
                                }
                            } else {
                                db.coerce_row_for_load(&table, values_ref)?;
                            }
                        }
                        validate_load_data_partition_row(
                            &table,
                            partition_filter.as_deref(),
                            values_ref,
                        )?;

                        // Per-row index key emission, written directly into
                        // the per-index batch buffer. Same shape as
                        // `precompute_index_keys`, inlined to avoid
                        // re-materialising `mapped_rows`.
                        if !table.indexes.is_empty() {
                            for (slot_idx, prepared) in prepared_indexes.iter().enumerate() {
                                match prepared {
                                    PreparedLoadIndex::Encoded { program, .. } => {
                                        append_encoded_load_key(
                                            program,
                                            &table,
                                            values_ref,
                                            &mut per_index_keys[slot_idx],
                                        )?;
                                    }
                                    PreparedLoadIndex::Fallback { index_idx } => {
                                        let keys = table
                                            .index_keys(&table.indexes[*index_idx], values_ref)?;
                                        let LoadDataIndexKeyBatch::Generic(per_row) =
                                            &mut per_index_keys[slot_idx]
                                        else {
                                            unreachable!("prepared fallback LOAD key shape changed")
                                        };
                                        per_row.push(keys.into_iter().collect());
                                    }
                                }
                            }
                        }

                        if retain_decoded_rows {
                            // Writer thread still does constraint checks +
                            // encode (needed when AUTO_INCREMENT must
                            // mutate values before encoding). For the
                            // identity path, hand the row over so the
                            // writer can read/mutate it.
                            if full_width_identity {
                                decoded_rows.push(std::mem::take(&mut row_scratch));
                                // Restore the scratch's capacity for the
                                // next iteration. `mem::take` left an
                                // empty Vec behind.
                                row_scratch = Vec::with_capacity(num_fields);
                            }
                        } else {
                            // Enforce constraints on the worker so the
                            // writer can go straight from "received
                            // payloads" to page-insert / WAL-append. NOT
                            // NULL / CHECK surfaces a user-visible error,
                            // so it must run before any heap insert
                            // observes the row.
                            db.check_not_null_constraints(&table, values_ref)?;
                            db.check_check_constraints(&table, values_ref, None)?;
                            // LOAD-path zone-map observation is opt-in
                            // (`MYDB_ZONE_MAP_LOAD_OBSERVE=1`). The default
                            // skips per-row observe/compare on the worker,
                            // and `apply_worker_batch` marks the touched
                            // zones Unknown so the next query conservatively
                            // full-scans until `ANALYZE TABLE` (which the
                            // bulk-load pipeline runs) rebuilds tight
                            // per-zone summaries. Enable observe when the
                            // workload runs queries before ANALYZE and
                            // wants loose LOAD-time pruning at the cost of
                            // per-row observation.
                            if observe_zone_map {
                                zone_map_worker_batch.observe_row(&table, values_ref);
                            }
                            payloads.push(crate::storage::encode_visible_row_payload(
                                values_ref, &codec, txn_id,
                            )?);
                            // For the identity-encoded path we don't move
                            // the row anywhere; clear() at the top of the
                            // next iteration reclaims the inner-String /
                            // SmallString slots and leaves the Vec backing
                            // capacity in place.
                        }
                    }
                    let rows = if writer_encode || auto_increment_needs_assignment {
                        BatchRows::Decoded(decoded_rows)
                    } else {
                        // Explicit-only AUTO_INCREMENT batches were retained
                        // until their identity classification was known. They
                        // can now follow the same worker-owned constraint and
                        // payload path as a non-AUTO table. Drain the decoded
                        // rows so their String/Value allocations are released
                        // before the prepared batch obtains its governed
                        // capacity charge.
                        if auto_increment_idx.is_some() {
                            for values in decoded_rows.drain(..) {
                                db.check_not_null_constraints(&table, &values)?;
                                db.check_check_constraints(&table, &values, None)?;
                                if observe_zone_map {
                                    zone_map_worker_batch.observe_row(&table, &values);
                                }
                                payloads.push(crate::storage::encode_visible_row_payload(
                                    &values, &codec, txn_id,
                                )?);
                            }
                        }
                        BatchRows::Encoded(payloads)
                    };
                    db.record_load_direct_decode_rows(
                        direct_decode_rows,
                        direct_decode_fallback_rows,
                    );
                    if use_shadow_generation {
                        db.record_load_shadow_parse_encode_us(
                            parse_encode_started
                                .elapsed()
                                .as_micros()
                                .min(u128::from(u64::MAX)) as u64,
                        );
                    }
                    drop(zone_operation);
                    if ignore_trial {
                        let warnings = crate::engine::api::current_statement_warning_sink();
                        if warnings.is_some_and(|warnings| !warnings.lock().is_empty()) {
                            return Err(ignore_load_replay_error());
                        }
                    }
                    let prepared = LoadDataPreparedBatch {
                        batch_idx,
                        rows,
                        index_keys: per_index_keys,
                        zone_map_worker_batch,
                        explicit_auto_increment_max: (!auto_increment_needs_assignment
                            && auto_increment_idx.is_some()
                            && explicit_auto_increment_max != 0)
                            .then_some(explicit_auto_increment_max),
                        auto_increment_needs_assignment,
                        memory_charge: None,
                    };
                    // The prepared batch owns copies/encodings of everything
                    // needed by the writer. Release the governed raw input
                    // before reserving the prepared capacity; retaining both
                    // until the blocking result send needlessly double-charged
                    // every worker at the pipeline high-water mark.
                    drop(raw_batch);
                    let mut prepared = prepared.with_memory_charge(&db)?;
                    // The encoded fast path has no AUTO_INCREMENT mutation,
                    // partition routing, or binlog row-image work left for a
                    // coordinator. Insert its heap batch on this worker so
                    // PARALLEL(N) also covers page construction, WAL append,
                    // and dirty-page writeback. HeapFile reserves a disjoint
                    // append extent for every batch; the coordinator still
                    // consumes results by batch_idx, preserving deterministic
                    // index-key input order and statement accounting.
                    if matches!(&prepared.rows, BatchRows::Encoded(_)) {
                        let BatchRows::Encoded(payloads) =
                            std::mem::replace(&mut prepared.rows, BatchRows::Inserted(Vec::new()))
                        else {
                            unreachable!("worker-encoded batch changed representation")
                        };
                        let row_ids = if let Some(appender) = shadow_heap_appender.as_ref() {
                            appender.append_batch(batch_idx, payloads)?
                        } else {
                            db.insert_pre_encoded_rows_heap_only_detailed(
                                txn,
                                &table_name,
                                payloads,
                            )?
                            .row_ids
                        };
                        prepared.rows = BatchRows::Inserted(row_ids);
                    }
                    if result_tx.send(Ok(prepared)).is_err() {
                        return Ok(());
                    }
                }
                Ok(())
            };
            let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                crate::engine::expr::with_session_expr_info_option(&session_info, || {
                    crate::engine::api::with_batched_statement_warning_sink(warning_sink, run)
                })
            }))
            .unwrap_or_else(|_| {
                Err(error(
                    crate::error::catalog::ER_UNKNOWN_ERROR,
                    "parallel LOAD DATA worker panicked",
                ))
            });
            if let Err(e) = &result {
                // Record the cause before cancellation wakes sibling appenders.
                first_worker_error.lock().get_or_insert_with(|| e.clone());
                if let Some(appender) = error_heap_appender.as_ref() {
                    appender.cancel();
                }
                let _ = err_tx.send(Err(e.clone()));
            }
            result
        }));
    }
    drop(result_tx);

    let mut sent_batches = 0usize;
    let mut received_batches = 0usize;
    let mut next_to_write = 0usize;
    let mut pending = BTreeMap::new();

    fn receive_ready_batches(
        result_rx: &mpsc::Receiver<Result<LoadDataPreparedBatch, QueryError>>,
        writer: &mut LoadDataPreparedWriter<'_>,
        pending: &mut BTreeMap<usize, LoadDataPreparedBatch>,
        next_to_write: &mut usize,
        received_batches: &mut usize,
    ) -> Result<(), QueryError> {
        loop {
            match result_rx.try_recv() {
                Ok(batch) => {
                    let batch = batch?;
                    pending.insert(batch.batch_idx, batch);
                    *received_batches += 1;
                    while let Some(batch) = pending.remove(next_to_write) {
                        writer.write_batch(batch)?;
                        *next_to_write += 1;
                    }
                }
                Err(mpsc::TryRecvError::Empty) => return Ok(()),
                Err(mpsc::TryRecvError::Disconnected) => {
                    return Err(error(
                        crate::error::catalog::ER_UNKNOWN_ERROR,
                        "parallel LOAD DATA worker stopped",
                    ))
                }
            }
        }
    }

    fn receive_one_batch(
        result_rx: &mpsc::Receiver<Result<LoadDataPreparedBatch, QueryError>>,
        writer: &mut LoadDataPreparedWriter<'_>,
        pending: &mut BTreeMap<usize, LoadDataPreparedBatch>,
        next_to_write: &mut usize,
        received_batches: &mut usize,
    ) -> Result<(), QueryError> {
        let batch = result_rx.recv().map_err(|_| {
            error(
                crate::error::catalog::ER_UNKNOWN_ERROR,
                "parallel LOAD DATA worker stopped",
            )
        })??;
        pending.insert(batch.batch_idx, batch);
        *received_batches += 1;
        while let Some(batch) = pending.remove(next_to_write) {
            writer.write_batch(batch)?;
            *next_to_write += 1;
        }
        Ok(())
    }

    let pipeline_result = (|| -> Result<(), QueryError> {
        let mut send_batch = |batch: RawLoadDataBatch,
                              writer: &mut LoadDataPreparedWriter<'_>,
                              pending: &mut BTreeMap<usize, LoadDataPreparedBatch>,
                              next_to_write: &mut usize,
                              received_batches: &mut usize|
         -> Result<(), QueryError> {
            let worker_idx = sent_batches % workers;
            let mut message = (sent_batches, batch);
            loop {
                match input_txs[worker_idx].try_send(message) {
                    Ok(()) => break,
                    Err(mpsc::TrySendError::Full(returned)) => {
                        message = returned;
                        // The target worker may be blocked publishing its prior
                        // result. Drain one result before retrying the bounded
                        // input so sender/worker cannot wait on each other.
                        receive_one_batch(
                            &result_rx,
                            writer,
                            pending,
                            next_to_write,
                            received_batches,
                        )?;
                    }
                    Err(mpsc::TrySendError::Disconnected(_)) => {
                        return Err(error(
                            crate::error::catalog::ER_UNKNOWN_ERROR,
                            "parallel LOAD DATA worker stopped",
                        ));
                    }
                }
            }
            sent_batches += 1;
            receive_ready_batches(&result_rx, writer, pending, next_to_write, received_batches)?;
            Ok(())
        };

        for batch in first_batches {
            send_batch(
                batch,
                &mut writer,
                &mut pending,
                &mut next_to_write,
                &mut received_batches,
            )?;
        }
        loop {
            let (batch, eof) =
                parse_raw_chunk_fast(db, reader, chunk_rows, &mut raw_data_capacity_hint, format)?;
            if !batch.is_empty() {
                send_batch(
                    batch,
                    &mut writer,
                    &mut pending,
                    &mut next_to_write,
                    &mut received_batches,
                )?;
            }
            if eof {
                break;
            }
        }
        while received_batches < sent_batches {
            crate::engine::cancel::check_cancelled()?;
            let batch = result_rx.recv().map_err(|_| {
                error(
                    crate::error::catalog::ER_UNKNOWN_ERROR,
                    "parallel LOAD DATA worker stopped",
                )
            })??;
            pending.insert(batch.batch_idx, batch);
            received_batches += 1;
            while let Some(batch) = pending.remove(&next_to_write) {
                writer.write_batch(batch)?;
                next_to_write += 1;
            }
        }

        debug_assert!(pending.is_empty());
        Ok(())
    })();
    if pipeline_result.is_err() {
        if let Some(appender) = &shadow_heap_appender {
            appender.cancel();
        }
    }
    // Stop every writer before dropping an unpublished generation or replaying
    // its input. Closing both channels also releases blocked result senders.
    input_txs.clear();
    drop(result_rx);
    let mut joined_error = None;
    for handle in handles {
        let result = handle
            .join()
            .map_err(|_| {
                error(
                    crate::error::catalog::ER_UNKNOWN_ERROR,
                    "parallel LOAD DATA worker panicked",
                )
            })
            .and_then(|result| result);
        if let Err(err) = result {
            joined_error.get_or_insert(err);
        }
    }
    let mut failure = pipeline_result.err().or(joined_error);
    if failure
        .as_ref()
        .is_some_and(|err| err.is(crate::error::catalog::ER_QUERY_INTERRUPTED))
    {
        if let Some(cause) = first_worker_error.lock().take() {
            failure = Some(cause);
        }
    }
    if failure.is_none() && ignore_trial {
        let warnings = crate::engine::api::current_statement_warning_sink();
        if warnings.is_some_and(|warnings| !warnings.lock().is_empty()) {
            failure = Some(ignore_load_replay_error());
        }
    }
    if let Some(err) = failure {
        drop(pending);
        drop(writer);
        return finish_statement_txn(db, session, Err(err));
    }
    writer.finish()
}

fn execute_preraw_load_data_fast(
    db: &Arc<Database>,
    session: &mut SessionContext,
    table_name: &str,
    table: &TableSchema,
    column_indexes: &[usize],
    full_width_identity: bool,
    coercion_plan: &[CoercionOp],
    format: FastLoadDataFormat,
    num_fields: usize,
    raw_batches: Vec<RawLoadDataBatch>,
    partition_filter: Option<&[u64]>,
) -> Result<QueryResponse, QueryError> {
    let txn = ensure_statement_txn(db, session)?;
    let mut prepared = Vec::with_capacity(raw_batches.len());
    let prepared_indexes = prepare_load_indexes(table)?;
    for (batch_idx, raw_batch) in raw_batches.into_iter().enumerate() {
        let mut rows = Vec::with_capacity(raw_batch.len());
        for line in raw_batch.lines() {
            rows.push(parse_row_fast_single_byte(
                line,
                format,
                coercion_plan,
                num_fields,
            )?);
        }
        let mut mapped_rows = map_rows(rows, table, column_indexes, full_width_identity);
        let insert_id = assign_load_identities_before_keys(db, txn, table, &mut mapped_rows)?;
        crate::engine::api::set_statement_insert_id(insert_id);
        materialize_generated_rows(db, table, &mut mapped_rows)?;
        validate_load_data_partition_set(table, partition_filter, &mapped_rows)?;
        let index_keys = precompute_index_keys(table, &prepared_indexes, &mapped_rows)?;
        prepared.push(LoadDataPreparedBatch {
            batch_idx,
            rows: BatchRows::Decoded(mapped_rows),
            index_keys,
            zone_map_worker_batch: crate::storage::ZoneMapWorkerBatch::default(),
            explicit_auto_increment_max: None,
            auto_increment_needs_assignment: false,
            memory_charge: None,
        });
    }
    execute_prepared_load_data_fast(db, session, table_name, table, prepared)
}

fn execute_prepared_load_data_fast(
    db: &Arc<Database>,
    session: &mut SessionContext,
    table_name: &str,
    table: &TableSchema,
    prepared_batches: Vec<LoadDataPreparedBatch>,
) -> Result<QueryResponse, QueryError> {
    let mut total_rows: u64 = 0;
    let txn = ensure_statement_txn(db, session)?;
    let mut index_builders = db.load_data_index_builders(table)?;

    for batch in prepared_batches {
        if batch.rows.is_empty() {
            continue;
        }
        let encoded = matches!(&batch.rows, BatchRows::Encoded(_) | BatchRows::Inserted(_));
        let inserted = match batch.rows {
            BatchRows::Decoded(rows) => db.insert_rows_heap_only_detailed(txn, table_name, rows)?,
            BatchRows::Encoded(payloads) => {
                db.insert_pre_encoded_rows_heap_only_detailed(txn, table_name, payloads)?
            }
            BatchRows::Inserted(row_ids) => crate::storage::InsertRowsResult {
                storage_ids: Vec::new(),
                row_ids,
                first_insert_id: 0,
            },
        };
        crate::engine::api::set_statement_insert_id(inserted.first_insert_id);
        let row_ids = inserted.row_ids;
        if encoded {
            db.apply_worker_batch(txn.txn_id, table.id, &row_ids, &batch.zone_map_worker_batch)?;
        }
        total_rows += row_ids.len() as u64;
        bump_load_data_rows(db, row_ids.len() as u64);

        index_builders.push_batch(db, txn.txn_id, batch.index_keys, &row_ids)?;
    }

    let result = if total_rows > 0 {
        index_builders
            .finish(db, txn.txn_id)
            .map(|()| QueryResponse::OkAffected {
                affected_rows: total_rows,
            })
    } else {
        Ok(QueryResponse::OkAffected {
            affected_rows: total_rows,
        })
    };
    finish_statement_txn(db, session, result)
}

struct LoadDataPreparedWriter<'a> {
    db: &'a Arc<Database>,
    session: &'a mut SessionContext,
    table_name: &'a str,
    table: &'a TableSchema,
    txn: TxnView,
    total_rows: u64,
    partition_filter: Option<Vec<u64>>,
    first_insert_id: u64,
    index_builders: Option<crate::storage::LoadDataIndexBuilders>,
    shadow_builder: Option<crate::storage::ShadowGenerationBuilder>,
    shadow_heap_appender: Option<crate::storage::ShadowHeapAppender>,
}

impl<'a> LoadDataPreparedWriter<'a> {
    fn new_with_txn(
        db: &'a Arc<Database>,
        session: &'a mut SessionContext,
        table_name: &'a str,
        table: &'a TableSchema,
        txn: TxnView,
        shadow_builder: Option<crate::storage::ShadowGenerationBuilder>,
        shadow_heap_appender: Option<crate::storage::ShadowHeapAppender>,
        partition_filter: Option<Vec<u64>>,
    ) -> Result<Self, QueryError> {
        let index_builders = if shadow_builder.is_some() {
            None
        } else {
            Some(db.load_data_index_builders(table)?)
        };
        Ok(Self {
            db,
            session,
            table_name,
            table,
            txn,
            total_rows: 0,
            partition_filter,
            first_insert_id: 0,
            index_builders,
            shadow_builder,
            shadow_heap_appender,
        })
    }

    fn write_batch(&mut self, mut batch: LoadDataPreparedBatch) -> Result<(), QueryError> {
        if batch.rows.is_empty() {
            return Ok(());
        }
        if self.table.auto_increment_column_index().is_some() {
            self.db.record_load_auto_increment_batch(
                batch.rows.len() as u64,
                batch.auto_increment_needs_assignment,
            );
        }
        if let Some(explicit_max) = batch.explicit_auto_increment_max {
            self.db
                .advance_load_auto_increment_explicit_max(self.table, explicit_max)?;
        }
        if batch.auto_increment_needs_assignment {
            let prepare = (|| -> Result<(), QueryError> {
                let BatchRows::Decoded(rows) = &mut batch.rows else {
                    return Err(error(
                        crate::error::catalog::ER_GET_ERRNO,
                        "LOAD DATA identity assignment requires decoded rows",
                    ));
                };
                let first = self
                    .db
                    .assign_load_auto_increment_values(self.txn, self.table, rows)?;
                if self.first_insert_id == 0 {
                    self.first_insert_id = first;
                }
                for values in rows.iter_mut() {
                    self.db.coerce_row_for_load(self.table, values)?;
                }
                materialize_generated_rows(self.db, self.table, rows)?;
                validate_load_data_partition_set(
                    self.table,
                    self.partition_filter.as_deref(),
                    rows,
                )?;
                for values in rows.iter() {
                    self.db.check_not_null_constraints(self.table, values)?;
                    self.db.check_check_constraints(self.table, values, None)?;
                }
                let indexes = prepare_load_indexes(self.table)?;
                batch.index_keys = precompute_index_keys(self.table, &indexes, rows)?;
                Ok(())
            })();
            if let Err(error) = prepare {
                if let Some(appender) = &self.shadow_heap_appender {
                    appender.cancel();
                }
                return Err(error);
            }
        }
        if let Some(shadow) = self.shadow_builder.as_mut() {
            let (row_ids, index_keys) = match batch.rows {
                BatchRows::Inserted(row_ids) => (row_ids, batch.index_keys),
                BatchRows::Decoded(rows) if batch.auto_increment_needs_assignment => {
                    let appender = self.shadow_heap_appender.as_ref().ok_or_else(|| {
                        error(
                            crate::error::catalog::ER_GET_ERRNO,
                            "shadow LOAD DATA has no ordered heap appender",
                        )
                    })?;
                    let prepare = (|| {
                        let index_keys = batch.index_keys;
                        let codec = self.table.row_codec.clone();
                        let mut payloads = Vec::with_capacity(rows.len());
                        for values in rows {
                            payloads.push(crate::storage::encode_visible_row_payload(
                                &values,
                                &codec,
                                self.txn.txn_id,
                            )?);
                        }
                        let row_ids = appender.append_batch(batch.batch_idx, payloads)?;
                        Ok::<_, QueryError>((row_ids, index_keys))
                    })();
                    match prepare {
                        Ok(prepared) => prepared,
                        Err(err) => {
                            appender.cancel();
                            return Err(err);
                        }
                    }
                }
                BatchRows::Decoded(_) | BatchRows::Encoded(_) => {
                    return Err(error(
                        crate::error::catalog::ER_GET_ERRNO,
                        "shadow LOAD DATA received an invalid prepared batch",
                    ));
                }
            };
            shadow.feed_prepared_batch(index_keys, &row_ids)?;
            self.total_rows = self.total_rows.saturating_add(row_ids.len() as u64);
            bump_load_data_rows(self.db, row_ids.len() as u64);
            return Ok(());
        }
        let encoded = matches!(&batch.rows, BatchRows::Encoded(_) | BatchRows::Inserted(_));
        let inserted = match batch.rows {
            BatchRows::Decoded(rows) => {
                self.db
                    .insert_rows_heap_only_detailed(self.txn, self.table_name, rows)?
            }
            BatchRows::Encoded(payloads) => self.db.insert_pre_encoded_rows_heap_only_detailed(
                self.txn,
                self.table_name,
                payloads,
            )?,
            BatchRows::Inserted(row_ids) => crate::storage::InsertRowsResult {
                storage_ids: Vec::new(),
                row_ids,
                first_insert_id: 0,
            },
        };
        if self.first_insert_id == 0 && inserted.first_insert_id != 0 {
            self.first_insert_id = inserted.first_insert_id;
        }
        let row_ids = inserted.row_ids;
        if encoded {
            self.db.apply_worker_batch(
                self.txn.txn_id,
                self.table.id,
                &row_ids,
                &batch.zone_map_worker_batch,
            )?;
        }
        self.total_rows += row_ids.len() as u64;
        bump_load_data_rows(self.db, row_ids.len() as u64);

        self.index_builders
            .as_mut()
            .expect("live LOAD DATA index builder present")
            .push_batch(self.db, self.txn.txn_id, batch.index_keys, &row_ids)?;
        Ok(())
    }

    fn finish(mut self) -> Result<QueryResponse, QueryError> {
        if let Some(shadow) = self.shadow_builder.take() {
            let mut published = match shadow.finish_and_publish() {
                Ok(published) => published,
                Err(err) => return finish_statement_txn(self.db, self.session, Err(err)),
            };
            debug_assert_eq!(published.total_rows(), self.total_rows);
            let response = QueryResponse::OkAffected {
                affected_rows: self.total_rows,
            };
            crate::engine::api::set_statement_insert_id(self.first_insert_id);
            return match finish_statement_txn(self.db, self.session, Ok(response)) {
                Ok(response) => {
                    if let Err(err) = published.mark_committed() {
                        // The transaction is already durably committed, so
                        // reporting failure (or rolling the files back from
                        // Drop) would be false. Normal finalization below can
                        // still remove the intent/backups; if the process dies
                        // first, WAL status remains the recovery fallback.
                        crate::util::logging::log_message(format_args!(
                            "bulk LOAD committed-marker write failed: {err}"
                        ));
                    }
                    if let Err(err) = published.finalize_committed() {
                        // Publication is already committed and visible. A
                        // cleanup failure must not turn that durable success
                        // into a client-visible rollback error; the retained
                        // intent/backups are idempotently finished at startup.
                        crate::util::logging::log_message(format_args!(
                            "bulk LOAD post-commit cleanup failed: {err}"
                        ));
                    }
                    Ok(response)
                }
                Err(err) => {
                    published.rollback_uncommitted();
                    Err(err)
                }
            };
        }
        let result = if self.total_rows > 0 {
            self.index_builders
                .take()
                .expect("live LOAD DATA index builder present")
                .finish(self.db, self.txn.txn_id)
                .map(|()| QueryResponse::OkAffected {
                    affected_rows: self.total_rows,
                })
        } else {
            Ok(QueryResponse::OkAffected {
                affected_rows: self.total_rows,
            })
        };
        if result.is_ok() {
            crate::engine::api::set_statement_insert_id(self.first_insert_id);
        }
        finish_statement_txn(self.db, self.session, result)
    }
}

struct LoadDataPreparedBatch {
    batch_idx: usize,
    rows: BatchRows,
    /// Index keys for the batch, materialised per-index in row order.
    /// For each index by ordinal, the inner `Vec<IndexKeysForRow>` holds
    /// one slot per row in the same order as `rows`. Each slot is a
    /// `SmallVec` because the Direct (single-keypart-per-row) path —
    /// which covers every B-tree index that isn't multi-valued — emits
    /// exactly one key, and the inline-capacity-1 SmallVec keeps that
    /// case allocation-free. Multi-valued JSON indexes and Fallback
    /// shapes that emit >1 key per row spill to the heap as before.
    ///
    /// Profile motivation: before flattening, the worker thread allocated
    /// three Vecs per row (outer per-row, inner per-index wrapper,
    /// per-index `vec![IndexKey::from(_)]` singleton) — `_xzm_xzone_malloc_tiny`
    /// attributed 188 samples to `precompute_index_keys` alone (~3% of
    /// total CPU). The flat shape allocates only the top-level
    /// `Vec<IndexKey>` per index (pre-sized to batch row count) plus
    /// IndexKey's own inner `Vec<KeyPart>`.
    index_keys: Vec<LoadDataIndexKeyBatch>,
    /// Compact per-batch aggregate produced by the parallel worker-encode
    /// path in one linear observe/compare pass, replacing the per-row
    /// projection Vec above for wide bulk loads. The writer distributes
    /// this envelope over every zone the freshly-inserted `row_ids` touch;
    /// tight per-zone precision is restored by a subsequent
    /// `ANALYZE TABLE` (which the bulk-load pipeline runs).
    zone_map_worker_batch: crate::storage::ZoneMapWorkerBatch,
    /// Maximum positive explicit AUTO_INCREMENT value in a worker-encoded
    /// batch. The coordinator advances the live sequence before publishing
    /// the batch's prepared primary-index keys.
    explicit_auto_increment_max: Option<u64>,
    /// True when this batch contains NULL/zero identities that must be
    /// allocated in input order by the coordinator before shadow append.
    auto_increment_needs_assignment: bool,
    /// Full-capacity charge for the batch while it is queued or owned by the
    /// writer. The index sorter's separate reservation begins before these
    /// keys are moved into it, so ownership transfer is conservatively
    /// double-charged for the short hand-off window rather than under-counted.
    memory_charge: Option<crate::engine::executor::mem_budget::ControlledMemoryCharge>,
}

type IndexKeysForRow = smallvec::SmallVec<[IndexKey; 1]>;

/// Either the parsed-value rows the writer thread still needs to encode
/// (and run NOT NULL / CHECK over), or the worker-encoded heap payloads
/// the writer can hand straight to the heap. Lineitem-shaped LOAD DATA
/// goes through `Encoded` while it is being prepared, then `Inserted`
/// after its worker has appended the heap extent. AUTO_INCREMENT tables
/// fall back to `Decoded` because only the writer thread holds the auto-inc
/// lock.
enum BatchRows {
    Decoded(Vec<Vec<Value>>),
    Encoded(Vec<Vec<u8>>),
    Inserted(Vec<crate::storage::RowId>),
}

impl BatchRows {
    fn len(&self) -> usize {
        match self {
            BatchRows::Decoded(rows) => rows.len(),
            BatchRows::Encoded(payloads) => payloads.len(),
            BatchRows::Inserted(row_ids) => row_ids.len(),
        }
    }

    fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

/// Reserve one raw/prepared pipeline batch. The buffer pool is an optional
/// cache, while a batch already parsed from the client is mandatory statement
/// state. Under concurrent LOAD DATA the per-statement sorter reservations can
/// leave the process just below its operational ceiling; failing the next
/// exact-capacity batch charge in that state is incorrect when clean buffer
/// pages can make room.
///
/// Serialize only the reclaim/admission transition. The batch remains charged
/// through its existing RAII owner, and the ordinary parser/writer pipeline is
/// still fully parallel. A bounded ancillary slice remains free for heap/index
/// page images needed while the charged batch is being consumed.
fn reserve_load_data_batch_memory(
    db: &Database,
    bytes: usize,
) -> Result<crate::engine::executor::mem_budget::ControlledMemoryCharge, QueryError> {
    use crate::engine::executor::mem_budget::{ControlledMemoryCategory, ControlledMemoryCharge};

    if let Some(charge) =
        ControlledMemoryCharge::try_new_optional(ControlledMemoryCategory::StorageScratch, bytes)
    {
        return Ok(charge);
    }

    static ADMISSION: std::sync::OnceLock<std::sync::Mutex<()>> = std::sync::OnceLock::new();
    let admission = ADMISSION.get_or_init(|| std::sync::Mutex::new(()));
    let _guard = admission
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());

    // Another batch may have released its reservation while this caller was
    // waiting for admission. Avoid an unnecessary buffer walk in that case.
    if let Some(charge) =
        ControlledMemoryCharge::try_new_optional(ControlledMemoryCategory::StorageScratch, bytes)
    {
        return Ok(charge);
    }

    let (controlled, _, categories, _) =
        crate::engine::executor::mem_budget::controlled_memory_snapshot();
    let operational_limit = crate::engine::executor::mem_budget::operational_memory_limit_bytes();
    let ancillary_headroom = load_data_ancillary_headroom_for_limit(operational_limit);
    let min_buffer_bytes = (16 * 1024 * 1024)
        .min(crate::engine::executor::mem_budget::memory_target_bytes() / 4)
        .max(crate::storage::PAGE_SIZE);
    if let Some(target) = load_data_batch_reclaim_target_for_values(
        controlled,
        categories[0],
        bytes,
        ancillary_headroom,
        min_buffer_bytes,
        operational_limit,
    ) {
        // Reclaim errors here normally mean pinned/non-evictable pages. Some
        // progress may still have been made, so retry the authoritative hard
        // reservation below and preserve its MySQL 1037/HY001 diagnostic if
        // the mandatory allocation genuinely cannot fit.
        let _ = db.reclaim_for_memory_target(target);
        let _ = crate::engine::memory_reclaimer::release_allocator_cached_memory();
    }

    ControlledMemoryCharge::try_new(ControlledMemoryCategory::StorageScratch, bytes)
}

impl LoadDataPreparedBatch {
    fn with_memory_charge(mut self, db: &Database) -> Result<Self, QueryError> {
        let bytes = self.capacity_bytes();
        self.memory_charge = Some(reserve_load_data_batch_memory(db, bytes)?);
        Ok(self)
    }

    fn capacity_bytes(&self) -> usize {
        let mut bytes = std::mem::size_of::<Self>();
        bytes = bytes.saturating_add(match &self.rows {
            BatchRows::Decoded(rows) => rows
                .capacity()
                .saturating_mul(std::mem::size_of::<Vec<Value>>())
                .saturating_add(rows.iter().fold(0usize, |sum, row| {
                    sum.saturating_add(
                        row.capacity()
                            .saturating_mul(std::mem::size_of::<Value>())
                            .saturating_add(row.iter().fold(0usize, |value_sum, value| {
                                value_sum.saturating_add(
                                    crate::engine::executor::mem_budget::estimate_value_bytes(
                                        value,
                                    )
                                    .saturating_sub(std::mem::size_of::<Value>()),
                                )
                            })),
                    )
                })),
            BatchRows::Encoded(payloads) => payloads
                .capacity()
                .saturating_mul(std::mem::size_of::<Vec<u8>>())
                .saturating_add(payloads.iter().fold(0usize, |sum, payload| {
                    sum.saturating_add(payload.capacity())
                })),
            BatchRows::Inserted(row_ids) => row_ids
                .capacity()
                .saturating_mul(std::mem::size_of::<crate::storage::RowId>()),
        });
        bytes = bytes.saturating_add(
            self.index_keys
                .capacity()
                .saturating_mul(std::mem::size_of::<LoadDataIndexKeyBatch>()),
        );
        for per_index in &self.index_keys {
            if let LoadDataIndexKeyBatch::EncodedFixed { keys, .. } = per_index {
                bytes = bytes.saturating_add(keys.capacity());
                continue;
            }
            if let LoadDataIndexKeyBatch::Encoded(per_row) = per_index {
                bytes = bytes.saturating_add(
                    per_row
                        .capacity()
                        .saturating_mul(std::mem::size_of::<IndexKeysForRow>()),
                );
                for records in per_row {
                    if records.spilled() {
                        bytes = bytes.saturating_add(records.capacity().saturating_mul(
                            std::mem::size_of::<crate::storage::EncodedPendingIndexRecord>(),
                        ));
                    }
                    for record in records {
                        bytes = bytes.saturating_add(record.capacity_bytes());
                    }
                }
                continue;
            }
            let LoadDataIndexKeyBatch::Generic(per_index) = per_index else {
                unreachable!("encoded LOAD batch handled above")
            };
            bytes = bytes.saturating_add(
                per_index
                    .capacity()
                    .saturating_mul(std::mem::size_of::<IndexKeysForRow>()),
            );
            for keys in per_index {
                if keys.spilled() {
                    bytes = bytes.saturating_add(
                        keys.capacity()
                            .saturating_mul(std::mem::size_of::<IndexKey>()),
                    );
                }
                for key in keys {
                    if key.0.spilled() {
                        bytes = bytes.saturating_add(
                            key.0
                                .capacity()
                                .saturating_mul(std::mem::size_of::<KeyPart>()),
                        );
                    }
                    for part in &key.0 {
                        bytes = bytes.saturating_add(match part {
                            KeyPart::Bytes(value) => value.capacity(),
                            KeyPart::String(value) => value.capacity(),
                            KeyPart::CollatedString { value, .. } => value.capacity(),
                            KeyPart::DecimalWide { value, .. } => value.bits() as usize / 8 + 1,
                            _ => 0,
                        });
                    }
                }
            }
        }
        bytes.saturating_add(std::mem::size_of_val(&self.zone_map_worker_batch))
    }
}

struct RawLoadDataBatch {
    data: Vec<u8>,
    lines: Vec<(usize, usize)>,
    memory_charge: Option<crate::engine::executor::mem_budget::ControlledMemoryCharge>,
}

impl RawLoadDataBatch {
    fn with_capacity(rows: usize, data_bytes: usize) -> Self {
        Self {
            data: Vec::with_capacity(data_bytes),
            lines: Vec::with_capacity(rows),
            memory_charge: None,
        }
    }

    fn is_empty(&self) -> bool {
        self.lines.is_empty()
    }

    fn len(&self) -> usize {
        self.lines.len()
    }

    fn with_memory_charge(mut self, db: &Database) -> Result<Self, QueryError> {
        let bytes = std::mem::size_of::<Self>()
            .saturating_add(self.data.capacity())
            .saturating_add(
                self.lines
                    .capacity()
                    .saturating_mul(std::mem::size_of::<(usize, usize)>()),
            );
        self.memory_charge = Some(reserve_load_data_batch_memory(db, bytes)?);
        Ok(self)
    }

    fn push_line(&mut self, line: &[u8]) {
        let start = self.data.len();
        self.data.extend_from_slice(line);
        let end = self.data.len();
        self.lines.push((start, end));
    }

    fn lines(&self) -> impl Iterator<Item = &[u8]> {
        self.lines
            .iter()
            .map(|&(start, end)| &self.data[start..end])
    }
}

#[derive(Clone)]
enum PreparedLoadIndex {
    Encoded {
        program: Arc<crate::storage::CompiledIndexKeyProgram>,
        direct_fixed_columns: Option<Vec<usize>>,
    },
    Fallback {
        index_idx: usize,
    },
}

fn prepare_load_indexes(table: &TableSchema) -> Result<Vec<PreparedLoadIndex>, QueryError> {
    let mut prepared = Vec::with_capacity(table.indexes.len());
    for (index_idx, index) in table.indexes.iter().enumerate() {
        if index.index_type.eq_ignore_ascii_case("BTREE")
            && !index.is_fulltext()
            && !index.is_spatial()
            && !index.is_vector()
        {
            let program = Arc::new(crate::storage::CompiledIndexKeyProgram::compile(
                table, index,
            )?);
            let direct_fixed_columns = (!index.has_expression_parts()
                && !index.has_multi_valued_parts()
                && index.included_columns.is_empty())
            .then(|| {
                index
                    .effective_columns()
                    .iter()
                    .map(|name| table.column_index(name).ok())
                    .collect::<Option<Vec<_>>>()
            })
            .flatten()
            .filter(|columns| {
                program.fixed_key_width().is_some()
                    && columns
                        .iter()
                        .all(|column| !table.columns[*column].nullable)
            });
            prepared.push(PreparedLoadIndex::Encoded {
                program,
                direct_fixed_columns,
            });
            continue;
        }
        prepared.push(PreparedLoadIndex::Fallback { index_idx });
    }
    Ok(prepared)
}

/// Per-index, row-ordered index key lists for a parsed batch.
///
/// Outer Vec is indexed by `table.indexes` ordinal; inner Vec has one
/// `IndexKeysForRow` slot per row in the same order as the batch's row
/// values. The SmallVec inline capacity is sized for the Direct
/// (single-keypart-per-row) case so non-multi-valued B-tree indexes
/// stay allocation-free at the per-row level.
fn new_encoded_load_key_batch(
    program: &crate::storage::CompiledIndexKeyProgram,
    row_count: usize,
) -> LoadDataIndexKeyBatch {
    if let Some(key_width) = program.fixed_key_width() {
        LoadDataIndexKeyBatch::EncodedFixed {
            key_width,
            keys: Vec::with_capacity(row_count.saturating_mul(key_width)),
        }
    } else {
        LoadDataIndexKeyBatch::Encoded(Vec::with_capacity(row_count))
    }
}

fn append_encoded_load_key(
    program: &crate::storage::CompiledIndexKeyProgram,
    table: &TableSchema,
    values: &[Value],
    batch: &mut LoadDataIndexKeyBatch,
) -> Result<(), QueryError> {
    match batch {
        LoadDataIndexKeyBatch::EncodedFixed { key_width, keys } => {
            let start = keys.len();
            program.encode_fixed_values_into(values, keys)?;
            debug_assert_eq!(keys.len() - start, *key_width);
        }
        LoadDataIndexKeyBatch::Encoded(per_row) => {
            per_row.push(program.encode_row(table, values)?.into_iter().collect());
        }
        _ => unreachable!("prepared encoded LOAD key shape changed"),
    }
    Ok(())
}

fn precompute_index_keys(
    table: &TableSchema,
    prepared_indexes: &[PreparedLoadIndex],
    mapped_rows: &[Vec<Value>],
) -> Result<Vec<LoadDataIndexKeyBatch>, QueryError> {
    if table.indexes.is_empty() {
        return Ok(Vec::new());
    }
    let row_count = mapped_rows.len();
    let mut per_index: Vec<LoadDataIndexKeyBatch> = prepared_indexes
        .iter()
        .map(|prepared| match prepared {
            PreparedLoadIndex::Encoded { program, .. } => {
                new_encoded_load_key_batch(program, row_count)
            }
            PreparedLoadIndex::Fallback { .. } => {
                LoadDataIndexKeyBatch::Generic(Vec::with_capacity(row_count))
            }
        })
        .collect();
    for values in mapped_rows {
        for (slot_idx, prepared) in prepared_indexes.iter().enumerate() {
            match prepared {
                PreparedLoadIndex::Encoded { program, .. } => {
                    append_encoded_load_key(program, table, values, &mut per_index[slot_idx])?
                }
                PreparedLoadIndex::Fallback { index_idx } => {
                    let keys = table.index_keys(&table.indexes[*index_idx], values)?;
                    let LoadDataIndexKeyBatch::Generic(per_row) = &mut per_index[slot_idx] else {
                        unreachable!("prepared fallback LOAD key shape changed")
                    };
                    per_row.push(keys.into_iter().collect());
                }
            }
        }
    }
    Ok(per_index)
}

fn materialize_generated_rows(
    db: &Arc<Database>,
    table: &TableSchema,
    rows: &mut [Vec<Value>],
) -> Result<(), QueryError> {
    if !table_has_generated_columns(table) {
        return Ok(());
    }
    for values in rows {
        db.materialize_generated_columns_for_insert(table, values)?;
    }
    Ok(())
}

fn validate_load_data_partition_set(
    table: &TableSchema,
    partition_filter: Option<&[u64]>,
    rows: &[Vec<Value>],
) -> Result<(), QueryError> {
    let Some(allowed) = partition_filter else {
        return Ok(());
    };
    if !table.is_partitioned() {
        return Ok(());
    }
    for row in rows {
        validate_load_data_partition_row(table, Some(allowed), row)?;
    }
    Ok(())
}

fn validate_load_data_partition_row(
    table: &TableSchema,
    partition_filter: Option<&[u64]>,
    row: &[Value],
) -> Result<(), QueryError> {
    let Some(allowed) = partition_filter else {
        return Ok(());
    };
    if !table.is_partitioned() {
        return Ok(());
    }
    let routed = table.route_row_to_partition(row)?;
    super::partition_clause::assert_insert_partition_allowed(allowed, routed.storage_id)
}

fn table_has_generated_columns(table: &TableSchema) -> bool {
    table
        .columns
        .iter()
        .any(|column| column.generated_expr.is_some())
}

/// Empty-table autocommit imports already exclude writers in the private-
/// generation path. IGNORE still needs incremental duplicate probes, but one
/// table X lock can cover its insert locks instead of retaining a lock directory
/// proportional to the complete input. Keep conflict, undo and WAL semantics.
fn with_load_insert_txn<F>(
    db: &Arc<Database>,
    session: &mut SessionContext,
    table: &TableSchema,
    exclusive: bool,
    execute: F,
) -> Result<QueryResponse, QueryError>
where
    F: FnOnce(&Arc<Database>, &mut SessionContext, TxnView) -> Result<QueryResponse, QueryError>,
{
    let exclusive_inserts =
        exclusive && load_data_exclusive_insert_shape_eligible(db, session, table)?;
    super::tx::execute_in_transaction_with_session(db, session, |db, session, txn| {
        if exclusive_inserts {
            db.acquire_table_exclusive(txn.txn_id, table.id)?;
            db.with_exclusive_insert_locks(txn.txn_id, table.id, || execute(db, session, txn))
        } else {
            execute(db, session, txn)
        }
    })
}

#[allow(clippy::too_many_arguments)]
fn execute_load_data_with_conflict(
    db: &Arc<Database>,
    session: &mut SessionContext,
    table_name: &str,
    table: &TableSchema,
    column_indexes: &[usize],
    full_width_identity: bool,
    coercion_plan: &[CoercionOp],
    parser: &FieldParser,
    reader: &mut BufReader<Box<dyn Read>>,
    num_fields: usize,
    ignore: bool,
    replace: bool,
    _columns: Option<&[String]>,
    partition_filter: Option<&[u64]>,
) -> Result<QueryResponse, QueryError> {
    let exclusive = ignore && !replace;
    with_load_insert_txn(db, session, table, exclusive, |db, session, _txn| {
        let mut total_rows: u64 = 0;
        let mut buf = Vec::with_capacity(READ_BUF_SIZE);

        loop {
            crate::engine::cancel::check_cancelled()?;
            let (rows, eof) = parse_chunk(
                reader,
                &mut buf,
                parser,
                coercion_plan,
                num_fields,
                bulk_chunk_rows(),
            )?;

            if rows.is_empty() {
                break;
            }

            let mut mapped_rows = map_rows(rows, table, column_indexes, full_width_identity);
            coerce_deferred_load_rows(db, table, &mut mapped_rows)?;
            materialize_generated_rows(db, table, &mut mapped_rows)?;
            validate_load_data_partition_set(table, partition_filter, &mapped_rows)?;
            // G8 — every source row in the chunk counts toward Records.
            add_load_records(mapped_rows.len() as u64);

            let txn = ensure_statement_txn(db, session)?;
            let batch_savepoint = db.statement_savepoint(txn.txn_id)?;
            match db.insert_rows_bulk_detailed(txn, table_name, mapped_rows.clone(), false) {
                Ok(inserted) => {
                    crate::engine::api::set_statement_insert_id(inserted.first_insert_id);
                    total_rows += inserted.row_ids.len() as u64;
                    bump_load_data_rows(db, inserted.row_ids.len() as u64);
                }
                Err(e)
                    if e.is(crate::error::catalog::ER_DUP_ENTRY)
                        || (ignore
                            && e.is(crate::error::catalog::ER_CHECK_CONSTRAINT_VIOLATED)) =>
                {
                    db.rollback_to_statement_savepoint(txn.txn_id, batch_savepoint)?;
                    for row in mapped_rows {
                        crate::engine::cancel::check_cancelled()?;
                        let result = execute_load_rows_with_conflict(
                            db,
                            ensure_statement_txn(db, session)?,
                            table_name,
                            None,
                            vec![row],
                            ignore,
                            replace,
                            None,
                            None,
                            None,
                            None,
                            None,
                        );
                        match result {
                            Ok(QueryResponse::OkAffected { affected_rows }) => {
                                // REPLACE that deleted an existing row reports 2
                                // (delete + insert); the extra is a Deleted.
                                if replace && affected_rows >= 2 {
                                    add_load_deleted(affected_rows - 1);
                                }
                                if affected_rows == 0 {
                                    add_load_skipped(1);
                                }
                                total_rows += affected_rows;
                                bump_load_data_rows(db, affected_rows);
                            }
                            Ok(_) => {}
                            Err(e2) if ignore && e2.is(crate::error::catalog::ER_DUP_ENTRY) => {
                                add_load_skipped(1);
                            }
                            Err(e2)
                                if ignore
                                    && e2.is(
                                        crate::error::catalog::ER_CHECK_CONSTRAINT_VIOLATED,
                                    ) =>
                            {
                                add_load_skipped(1);
                                crate::engine::api::push_statement_condition(
                                    crate::error::ConditionLevel::Warning,
                                    &e2,
                                );
                            }
                            Err(e2) => return Err(e2),
                        }
                    }
                }
                Err(e) => {
                    return Err(e);
                }
            }

            if eof {
                break;
            }
        }

        crate::engine::cancel::check_cancelled()?;
        Ok(QueryResponse::OkAffected {
            affected_rows: total_rows,
        })
    })
}

#[allow(clippy::too_many_arguments)]
fn execute_load_data_with_triggers(
    db: &Arc<Database>,
    session: &mut SessionContext,
    registry: &SessionRegistry,
    table_name: &str,
    table: &TableSchema,
    column_indexes: &[usize],
    full_width_identity: bool,
    coercion_plan: &[CoercionOp],
    parser: &FieldParser,
    reader: &mut BufReader<Box<dyn Read>>,
    num_fields: usize,
    ignore: bool,
    replace: bool,
    _columns: Option<&[String]>,
    partition_filter: Option<&[u64]>,
) -> Result<QueryResponse, QueryError> {
    super::tx::execute_in_transaction_with_session(db, session, |db, session, _txn| {
        let mut total_rows = 0u64;
        let mut buf = Vec::with_capacity(READ_BUF_SIZE);
        loop {
            crate::engine::cancel::check_cancelled()?;
            let (rows, eof) = parse_chunk(
                reader,
                &mut buf,
                parser,
                coercion_plan,
                num_fields,
                bulk_chunk_rows(),
            )?;
            if rows.is_empty() {
                break;
            }
            let mut mapped_rows = map_rows(rows, table, column_indexes, full_width_identity);
            coerce_deferred_load_rows(db, table, &mut mapped_rows)?;
            materialize_generated_rows(db, table, &mut mapped_rows)?;
            validate_load_data_partition_set(table, partition_filter, &mapped_rows)?;
            add_load_records(mapped_rows.len() as u64);
            let records = mapped_rows.len() as u64;
            let txn = ensure_statement_txn(db, session)?;
            let result = execute_load_rows_with_triggers(
                db,
                session,
                txn,
                table_name,
                None,
                mapped_rows,
                ignore,
                replace,
                None,
                None,
                None,
                None,
                None,
                registry,
            );
            match result {
                Ok(QueryResponse::OkAffected { affected_rows }) => {
                    record_triggered_load_batch(db, records, affected_rows, ignore, replace);
                    total_rows += affected_rows;
                }
                Ok(_) => {}
                Err(err) => return Err(err),
            }
            if eof {
                break;
            }
        }
        crate::engine::cancel::check_cancelled()?;
        Ok(QueryResponse::OkAffected {
            affected_rows: total_rows,
        })
    })
}

fn map_rows(
    mut rows: Vec<Vec<Value>>,
    table: &TableSchema,
    column_indexes: &[usize],
    full_width_identity: bool,
) -> Vec<Vec<Value>> {
    if full_width_identity {
        for row in &mut rows {
            row.extend(
                table.columns[row.len()..]
                    .iter()
                    .map(resolve_load_missing_default),
            );
        }
        return rows;
    }
    let mut mapped = Vec::with_capacity(rows.len());
    for row in rows {
        let mut values: Vec<Value> = table
            .columns
            .iter()
            .map(|c| resolve_load_missing_default(c))
            .collect();
        for (idx, value) in column_indexes.iter().zip(row) {
            values[*idx] = value;
        }
        mapped.push(values);
    }
    mapped
}

fn skip_lines(
    reader: &mut BufReader<Box<dyn Read>>,
    parser: &FieldParser,
    count: usize,
) -> Result<(), QueryError> {
    let mut record = Vec::new();
    for _ in 0..count {
        if !read_load_record(reader, parser, &mut record, 1)? {
            break;
        }
    }
    Ok(())
}

#[derive(Debug, Clone, Copy)]
struct FastLoadDataFormat {
    field_terminator: u8,
    escaped_by: Option<u8>,
    crlf: bool,
}

impl FastLoadDataFormat {
    #[inline]
    fn ends_line(self, spill: &[u8], remaining: &[u8], lf: usize) -> bool {
        let delimiter_start = if self.crlf {
            let prior = if lf == 0 {
                spill.last()
            } else {
                remaining.get(lf - 1)
            };
            if prior != Some(&b'\r') {
                return false;
            }
            if lf == 0 {
                return self.escaped_by.is_none()
                    || !newline_is_escaped(&spill[..spill.len() - 1], remaining, 0);
            }
            lf - 1
        } else {
            lf
        };
        self.escaped_by.is_none() || !newline_is_escaped(spill, remaining, delimiter_start)
    }

    #[inline]
    fn terminated_line(self, line: &[u8]) -> &[u8] {
        if self.crlf {
            &line[..line.len() - 1]
        } else {
            line
        }
    }
}

/// Conservative batch gate for the fused AUTO_INCREMENT decoder. A batch
/// containing a generated identity must stay decoded until the coordinator
/// assigns values in input order; explicit-only batches can be encoded by the
/// worker. Invalid/escaped fields also return true so the ordinary parser
/// remains the error/compatibility oracle.
fn load_line_auto_increment_needs_assignment(
    line: &[u8],
    format: FastLoadDataFormat,
    auto_increment_idx: usize,
    no_auto_value_on_zero: bool,
) -> bool {
    let mut start = 0usize;
    let mut field_idx = 0usize;
    for end in memchr::memchr_iter(format.field_terminator, line).chain(std::iter::once(line.len()))
    {
        if field_idx == auto_increment_idx {
            let field = &line[start..end];
            if field.windows(2).any(|pair| pair == b"\\N") || memchr(b'\\', field).is_some() {
                return true;
            }
            if no_auto_value_on_zero {
                return false;
            }
            return parse_i64_fast(field).map_or(true, |value| value == 0);
        }
        field_idx += 1;
        start = end.saturating_add(1);
    }
    true
}

/// Schema-compiled delimiter-only row decoder. It is deliberately narrow:
/// unsupported column kinds and any row containing an escape byte fall back
/// to `parse_row_fast_single_byte_into`, which remains the semantic oracle.
/// The common identity LOAD shape scans delimiters exactly once rather than
/// restarting `memchr2` for every field.
#[derive(Clone)]
struct CompiledLoadRowProgram {
    field_terminator: u8,
    escaped_by: Option<u8>,
    num_fields: usize,
    coercion_plan: Arc<[CoercionOp]>,
    payload_encoder: crate::storage::CompiledDirectRowEncoder,
    prepared_indexes: Arc<[PreparedLoadIndex]>,
    indexed_columns: Arc<[bool]>,
    auto_increment_idx: Option<usize>,
    timestamp_converter: Option<crate::engine::expr::SessionLocalTimestampConverter>,
}

struct EscapedLoadFieldScratch {
    bytes: Vec<u8>,
    memory_charge: crate::engine::executor::mem_budget::ControlledMemoryCharge,
}

impl EscapedLoadFieldScratch {
    fn new() -> Result<Self, QueryError> {
        use crate::engine::executor::mem_budget::{
            ControlledMemoryCategory, ControlledMemoryCharge,
        };
        Ok(Self {
            bytes: Vec::new(),
            memory_charge: ControlledMemoryCharge::try_new(
                ControlledMemoryCategory::StorageScratch,
                0,
            )?,
        })
    }

    fn unescape<'a>(&'a mut self, field: &[u8]) -> Result<&'a [u8], QueryError> {
        self.bytes.clear();
        if self.bytes.capacity() < field.len() {
            let additional = field.len() - self.bytes.capacity();
            // Reserve with the governor before the allocator. reserve_exact's
            // logical capacity is the requested size, so the charge follows
            // the reusable buffer's full capacity rather than its current len.
            self.memory_charge.try_grow(additional)?;
            self.bytes.reserve_exact(additional);
        }
        let mut pos = 0usize;
        while pos < field.len() {
            if field[pos] == b'\\' && pos + 1 < field.len() {
                pos += 1;
                self.bytes.push(match field[pos] {
                    b'0' => 0,
                    b'b' => 8,
                    b'n' => b'\n',
                    b'r' => b'\r',
                    b't' => b'\t',
                    b'\\' => b'\\',
                    b'Z' => 0x1a,
                    other => other,
                });
            } else {
                self.bytes.push(field[pos]);
            }
            pos += 1;
        }
        Ok(&self.bytes)
    }
}

impl CompiledLoadRowProgram {
    fn compile(
        table: &TableSchema,
        full_width_identity: bool,
        format: FastLoadDataFormat,
        coercion_plan: &[CoercionOp],
        num_fields: usize,
        prepared_indexes: &[PreparedLoadIndex],
    ) -> Option<Self> {
        if std::env::var("MYDB_LOAD_DATA_DIRECT_DECODE_DISABLE").is_ok_and(|value| {
            matches!(
                value.to_ascii_lowercase().as_str(),
                "1" | "on" | "true" | "yes"
            )
        }) || !full_width_identity
            || num_fields != table.columns.len()
            || !table.check_constraints.is_empty()
            || table
                .columns
                .iter()
                .any(|column| column.generated_expr.is_some())
            || coercion_plan.iter().any(|op| {
                !matches!(
                    op,
                    CoercionOp::SignedInt { .. }
                        | CoercionOp::UnsignedInt { .. }
                        | CoercionOp::Decimal {
                            precision: Some(_),
                            ..
                        }
                        | CoercionOp::CharPad { .. }
                        | CoercionOp::VarCharLimit { .. }
                        | CoercionOp::BinaryPad(_)
                        | CoercionOp::VarBinaryLimit(_)
                        | CoercionOp::Text { .. }
                        | CoercionOp::Date
                        | CoercionOp::Timestamp { .. }
                )
            })
            // The direct opcode writes an i64 DECIMAL slot. Wider schemas
            // require the ordinary row codec even when this value is small.
            || coercion_plan.iter().any(|op| matches!(op,
                CoercionOp::Decimal { precision: Some(precision), .. } if *precision > 18))
            || prepared_indexes.iter().any(|index| match index {
                PreparedLoadIndex::Encoded {
                    direct_fixed_columns,
                    ..
                } => direct_fixed_columns.is_none(),
                PreparedLoadIndex::Fallback { .. } => true,
            })
        {
            return None;
        }
        let payload_encoder = crate::storage::CompiledDirectRowEncoder::compile(&table.row_codec)?;
        let mut indexed_columns = vec![false; num_fields];
        for index in prepared_indexes {
            match index {
                PreparedLoadIndex::Encoded {
                    direct_fixed_columns: Some(columns),
                    ..
                } => {
                    for column in columns {
                        indexed_columns[*column] = true;
                    }
                }
                PreparedLoadIndex::Encoded {
                    direct_fixed_columns: None,
                    ..
                } => unreachable!("unsupported encoded index rejected above"),
                PreparedLoadIndex::Fallback { .. } => {
                    unreachable!("unsupported index rejected above")
                }
            }
        }
        let timestamp_converter = if coercion_plan
            .iter()
            .any(|op| matches!(op, CoercionOp::Timestamp { .. }))
        {
            Some(crate::engine::expr::SessionLocalTimestampConverter::capture().ok()?)
        } else {
            None
        };
        Some(Self {
            field_terminator: format.field_terminator,
            escaped_by: format.escaped_by,
            num_fields,
            coercion_plan: Arc::from(coercion_plan.to_vec()),
            payload_encoder,
            prepared_indexes: Arc::from(prepared_indexes.to_vec()),
            indexed_columns: Arc::from(indexed_columns),
            auto_increment_idx: table.auto_increment_column_index(),
            timestamp_converter,
        })
    }

    /// Fused delimiter scan, coercion and final RowCodec emission. Index
    /// integer values are retained in caller-owned scratch only for columns
    /// referenced by a fixed-width B-tree plan.
    fn decode_encode(
        &self,
        line: &[u8],
        plain_ascii_batch: bool,
        txn_id: u64,
        index_value_scratch: &mut Vec<Value>,
        escaped_field_scratch: &mut EscapedLoadFieldScratch,
        per_index_keys: &mut [LoadDataIndexKeyBatch],
        explicit_auto_increment_max: &mut u64,
    ) -> Result<Option<Vec<u8>>, QueryError> {
        if index_value_scratch.len() < self.num_fields {
            index_value_scratch.resize(self.num_fields, Value::Null);
        }
        let mut payload = self.payload_encoder.begin_row(txn_id, line.len());
        let mut field_idx = 0usize;
        let mut truncation_warnings = 0usize;
        let mut empty_number_warnings = 0usize;
        let mut start = 0usize;
        // Most rows contain no escapes. Reuse one delimiter iterator for the
        // whole row instead of restarting the two-byte search for every field.
        // Rows with escapes retain the escape-aware scanner below, including
        // escaped delimiters, doubled escapes and SQL NULL markers.
        let mut delimiters = (plain_ascii_batch
            || self
                .escaped_by
                .is_none_or(|escape| memchr(escape, line).is_none()))
        .then(|| memchr::memchr_iter(self.field_terminator, line));
        while field_idx < self.num_fields && start <= line.len() {
            let mut end = start;
            let mut has_escape = false;
            if let Some(delimiters) = delimiters.as_mut() {
                end = delimiters.next().unwrap_or(line.len());
            } else {
                loop {
                    let next = match self.escaped_by {
                        Some(escape) => memchr2(self.field_terminator, escape, &line[end..]),
                        None => memchr(self.field_terminator, &line[end..]),
                    };
                    let Some(offset) = next else {
                        end = line.len();
                        break;
                    };
                    end += offset;
                    if line[end] == self.field_terminator {
                        break;
                    }
                    has_escape = true;
                    end += if end + 1 < line.len() { 2 } else { 1 };
                }
            }
            let raw_field = &line[start..end];
            // Keep SQL NULL on the established row parser/oracle path. The
            // direct program is selected only for fixed-width non-null index
            // keys, but non-indexed nullable columns can still contain \\N.
            if self.escaped_by.is_some() && raw_field == b"\\N" {
                return Ok(None);
            }
            let field = if has_escape {
                escaped_field_scratch.unescape(raw_field)?
            } else {
                raw_field
            };
            match &self.coercion_plan[field_idx] {
                CoercionOp::SignedInt { min, max, column } => {
                    if let Some(value) = parse_i64_fast(field) {
                        if !(*min..=*max).contains(&value) {
                            return Err(load_out_of_range(column));
                        }
                        if self.indexed_columns[field_idx] {
                            index_value_scratch[field_idx] = Value::Int(value);
                        }
                        if self.auto_increment_idx == Some(field_idx) && value > 0 {
                            *explicit_auto_increment_max =
                                (*explicit_auto_increment_max).max(value as u64);
                        }
                        payload.push_i64(value)?;
                    } else {
                        return Ok(None);
                    }
                }
                CoercionOp::UnsignedInt { max, column } => {
                    if let Some(value) = parse_u64_fast(field) {
                        if value > *max {
                            return Err(load_out_of_range(column));
                        }
                        if self.indexed_columns[field_idx] {
                            index_value_scratch[field_idx] = Value::UInt(value);
                        }
                        if self.auto_increment_idx == Some(field_idx) && value > 0 {
                            *explicit_auto_increment_max =
                                (*explicit_auto_increment_max).max(value);
                        }
                        payload.push_u64(value)?;
                    } else {
                        return Ok(None);
                    }
                }
                CoercionOp::Decimal {
                    precision: Some(precision),
                    scale,
                    unsigned,
                    column,
                } => {
                    let target_scale = scale.unwrap_or(0);
                    if let Some((magnitude, value_scale, discarded)) =
                        parse_decimal_compact_parts_fast(field, *scale)
                    {
                        truncation_warnings += usize::from(discarded);
                        empty_number_warnings += usize::from(trim_ascii_bytes(field).is_empty());
                        validate_decimal_compact_parts(
                            magnitude,
                            value_scale,
                            *precision,
                            target_scale,
                            *unsigned,
                            column,
                        )?;
                        if self.indexed_columns[field_idx] {
                            index_value_scratch[field_idx] =
                                Value::DecimalCompact(magnitude, value_scale);
                        }
                        payload.push_decimal_compact(magnitude, value_scale)?;
                    } else {
                        let value = coerce_field_borrowed(field, &self.coercion_plan[field_idx])?;
                        if self.indexed_columns[field_idx] {
                            index_value_scratch[field_idx] = value.clone();
                        }
                        payload.push_value(&value)?;
                    }
                }
                CoercionOp::Date => {
                    let days = crate::storage::parse_date_to_days_bytes(field)?;
                    if self.indexed_columns[field_idx] {
                        index_value_scratch[field_idx] = Value::Date(days);
                    }
                    payload.push_date_days(days)?;
                }
                CoercionOp::Timestamp { fsp, column } => {
                    let value = match std::str::from_utf8(field) {
                        Ok(text) => {
                            let wall = crate::storage::quantize_datetime_text_micros(
                                crate::storage::parse_datetime_to_micros(text)?,
                                text,
                                *fsp,
                            );
                            let utc = self
                                .timestamp_converter
                                .as_ref()
                                .expect("TIMESTAMP opcode has a captured session converter")
                                .convert(wall)?;
                            crate::storage::canonicalize_loaded_timestamp_value(
                                utc, wall, *fsp, column,
                            )?
                        }
                        Err(_) => coerce_field_borrowed(field, &self.coercion_plan[field_idx])?,
                    };
                    if self.indexed_columns[field_idx] {
                        index_value_scratch[field_idx] = value.clone();
                    }
                    payload.push_value(&value)?;
                }
                CoercionOp::Text {
                    max_len,
                    column,
                    charset,
                } => {
                    let text = load_utf8(field)?;
                    if text.len() as u64 > u64::from(*max_len) {
                        return Err(error(
                            crate::error::catalog::ER_DATA_TOO_LONG,
                            format!("data too long for column '{column}'"),
                        ));
                    }
                    if let Some(charset) = charset.as_deref() {
                        crate::charset::validate_text_for_charset(charset, &text)?;
                    }
                    payload.push_string_bytes(text.as_bytes())?;
                }
                CoercionOp::CharPad {
                    len,
                    column,
                    charset: _,
                } if plain_ascii_batch || field.is_ascii() => {
                    let compact = crate::util::pad_space::pad_space_trim_end(field);
                    if compact.len() > *len {
                        return Err(error(
                            crate::error::catalog::ER_DATA_TOO_LONG,
                            format!("data too long for column '{column}'"),
                        ));
                    }
                    payload.push_string_bytes(compact)?;
                }
                CoercionOp::VarCharLimit {
                    len,
                    column,
                    charset: _,
                } if plain_ascii_batch || field.is_ascii() => {
                    let kept = if field.len() <= *len {
                        field
                    } else {
                        let (kept, removed) = field.split_at(*len);
                        if !removed.iter().all(|byte| *byte == b' ') {
                            return Err(error(
                                crate::error::catalog::ER_DATA_TOO_LONG,
                                format!("data too long for column '{column}'"),
                            ));
                        }
                        truncation_warnings += 1;
                        kept
                    };
                    payload.push_string_bytes(kept)?;
                }
                CoercionOp::BinaryPad(len) | CoercionOp::VarBinaryLimit(len)
                    if field.len() > *len =>
                {
                    return Ok(None);
                }
                _ => {
                    let value = coerce_field_borrowed(field, &self.coercion_plan[field_idx])?;
                    payload.push_value(&value)?;
                }
            }
            field_idx += 1;
            start = if end < line.len() {
                end + 1
            } else {
                line.len() + 1
            };
        }
        if field_idx != self.num_fields {
            return Ok(None);
        }
        if start <= line.len() {
            warn_load_field_count(self.num_fields + 1, self.num_fields);
        }
        for (plan, keys) in self.prepared_indexes.iter().zip(per_index_keys) {
            match (plan, keys) {
                (
                    PreparedLoadIndex::Encoded {
                        program,
                        direct_fixed_columns: Some(_),
                    },
                    LoadDataIndexKeyBatch::EncodedFixed { key_width, keys },
                ) => {
                    let start = keys.len();
                    program.encode_fixed_values_into(index_value_scratch, keys)?;
                    debug_assert_eq!(keys.len() - start, *key_width);
                }
                _ => {
                    return Err(error(
                        crate::error::catalog::ER_GET_ERRNO,
                        "compiled LOAD index-key plan mismatch",
                    ));
                }
            }
        }
        let payload = payload.finish()?;
        for _ in 0..truncation_warnings {
            warn_load_decimal_scale();
        }
        for _ in 0..empty_number_warnings {
            warn_load_empty_number();
        }
        Ok(Some(payload))
    }
}

fn fast_parse_format(parser: &FieldParser) -> Option<FastLoadDataFormat> {
    if parser.input_charset != LoadLegacyCharset::None
        || parser.field_terminator.len() != 1
        || !(parser.line_terminator == b"\n" || parser.line_terminator == b"\r\n")
        || parser.enclosed_by.is_some()
        || !parser.line_starting_by.is_empty()
        || !matches!(parser.escaped_by, None | Some(b'\\'))
    {
        return None;
    }
    let field_terminator = parser.field_terminator[0];
    if field_terminator == b'\n' || field_terminator == b'\r' || field_terminator == b'\\' {
        return None;
    }
    Some(FastLoadDataFormat {
        field_terminator,
        escaped_by: parser.escaped_by,
        crlf: parser.line_terminator == b"\r\n",
    })
}

fn parse_chunk_fast(
    reader: &mut BufReader<Box<dyn Read>>,
    format: FastLoadDataFormat,
    coercion_plan: &[CoercionOp],
    num_fields: usize,
    max_rows: usize,
) -> Result<(Vec<Vec<Value>>, bool), QueryError> {
    let mut rows = Vec::with_capacity(max_rows);
    let mut eof = false;
    let mut spill = Vec::new();

    while rows.len() < max_rows {
        let available = reader.fill_buf().map_err(io_err)?;
        if available.is_empty() {
            if !spill.is_empty() {
                let line = &spill[..];
                if !line.is_empty() {
                    rows.push(parse_row_fast_single_byte(
                        line,
                        format,
                        coercion_plan,
                        num_fields,
                    )?);
                }
                spill.clear();
            }
            eof = true;
            break;
        }

        let mut consumed = 0;
        let buf = available;

        while consumed < buf.len() && rows.len() < max_rows {
            let remaining = &buf[consumed..];
            let nl_pos = memchr(b'\n', remaining);

            match nl_pos {
                Some(pos) => {
                    if !format.ends_line(&spill, remaining, pos) {
                        spill.extend_from_slice(&remaining[..=pos]);
                        consumed += pos + 1;
                        continue;
                    }
                    if spill.is_empty() {
                        let line = format.terminated_line(&remaining[..pos]);
                        {
                            rows.push(parse_row_fast_single_byte(
                                line,
                                format,
                                coercion_plan,
                                num_fields,
                            )?);
                        }
                    } else {
                        spill.extend_from_slice(&remaining[..pos]);
                        let line = format.terminated_line(&spill);
                        {
                            rows.push(parse_row_fast_single_byte(
                                line,
                                format,
                                coercion_plan,
                                num_fields,
                            )?);
                        }
                        spill.clear();
                    }
                    consumed += pos + 1;
                }
                None => {
                    spill.extend_from_slice(remaining);
                    consumed = buf.len();
                }
            }
        }

        reader.consume(consumed);
    }

    Ok((rows, eof))
}

fn parse_raw_chunk_fast(
    db: &Database,
    reader: &mut BufReader<Box<dyn Read>>,
    max_rows: usize,
    data_capacity_hint: &mut usize,
    format: FastLoadDataFormat,
) -> Result<(RawLoadDataBatch, bool), QueryError> {
    let mut rows = RawLoadDataBatch::with_capacity(max_rows, *data_capacity_hint);
    let mut eof = false;
    let mut spill = Vec::new();

    while rows.len() < max_rows {
        let available = reader.fill_buf().map_err(io_err)?;
        if available.is_empty() {
            if !spill.is_empty() {
                rows.push_line(&spill);
                spill.clear();
            }
            eof = true;
            break;
        }

        let mut consumed = 0;
        let buf = available;

        while consumed < buf.len() && rows.len() < max_rows {
            let remaining = &buf[consumed..];
            let nl_pos = memchr(b'\n', remaining);

            match nl_pos {
                Some(pos) => {
                    if !format.ends_line(&spill, remaining, pos) {
                        spill.extend_from_slice(&remaining[..=pos]);
                        consumed += pos + 1;
                        continue;
                    }
                    if spill.is_empty() {
                        rows.push_line(format.terminated_line(&remaining[..pos]));
                    } else {
                        spill.extend_from_slice(&remaining[..pos]);
                        rows.push_line(format.terminated_line(&spill));
                        spill.clear();
                    }
                    consumed += pos + 1;
                }
                None => {
                    spill.extend_from_slice(remaining);
                    consumed = buf.len();
                }
            }
        }

        reader.consume(consumed);
    }

    // Row widths are stable within one LOAD statement. After the first
    // batch, reserve the observed byte volume plus bounded headroom so the
    // remaining batches avoid geometric realloc/copy without pessimistically
    // reserving a large fixed buffer for small or narrow inputs.
    let observed = rows.data.len();
    if observed != 0 {
        *data_capacity_hint = observed.saturating_add(observed / 4);
    }
    Ok((rows.with_memory_charge(db)?, eof))
}

/// A physical newline is data, not a row terminator, when preceded by an odd
/// run of backslashes. The scan is normally one byte and touches `spill` only
/// at a buffer boundary, keeping the common LOAD DATA path branch-cheap.
fn newline_is_escaped(spill: &[u8], remaining: &[u8], newline: usize) -> bool {
    let mut escapes = 0usize;
    for byte in remaining[..newline].iter().rev() {
        if *byte != b'\\' {
            return escapes % 2 == 1;
        }
        escapes += 1;
    }
    for byte in spill.iter().rev() {
        if *byte != b'\\' {
            break;
        }
        escapes += 1;
    }
    escapes % 2 == 1
}

fn parse_row_fast_single_byte(
    line: &[u8],
    format: FastLoadDataFormat,
    coercion_plan: &[CoercionOp],
    num_fields: usize,
) -> Result<Vec<Value>, QueryError> {
    let mut values = Vec::with_capacity(num_fields);
    parse_row_fast_single_byte_into(line, format, coercion_plan, num_fields, &mut values)?;
    Ok(values)
}

/// Same as `parse_row_fast_single_byte` but pushes parsed values into a
/// caller-supplied buffer. Lets the parallel-LOAD worker reuse one
/// `Vec<Value>` across every row in a batch instead of allocating a
/// fresh 16-slot Vec per row.
fn parse_row_fast_single_byte_into(
    line: &[u8],
    format: FastLoadDataFormat,
    coercion_plan: &[CoercionOp],
    num_fields: usize,
    values: &mut Vec<Value>,
) -> Result<(), QueryError> {
    if values.capacity() < num_fields {
        values.reserve(num_fields - values.capacity());
    }
    let mut pos = 0;

    for field_idx in 0..num_fields {
        if pos > line.len() {
            warn_load_field_count(field_idx, num_fields);
            return Ok(());
        }

        let field_start = pos;
        let mut has_escape = false;
        while pos < line.len() {
            let next = match format.escaped_by {
                Some(escape) => memchr2(format.field_terminator, escape, &line[pos..]),
                None => memchr(format.field_terminator, &line[pos..]),
            };
            let Some(offset) = next else {
                pos = line.len();
                break;
            };
            pos += offset;
            if line[pos] == format.field_terminator {
                break;
            }
            if pos + 1 < line.len() {
                has_escape = true;
                pos += 2;
            } else {
                pos += 1;
            }
        }

        let field_bytes = &line[field_start..pos];

        let value = if format.escaped_by.is_some() && field_bytes == b"\\N" {
            Value::Null
        } else if has_escape {
            let unescaped = unescape_field(field_bytes);
            if field_idx < coercion_plan.len() {
                coerce_field(&unescaped, &coercion_plan[field_idx])?
            } else {
                bytes_to_string_value(&unescaped)
            }
        } else if field_idx < coercion_plan.len() {
            coerce_field_borrowed(field_bytes, &coercion_plan[field_idx])?
        } else {
            bytes_to_string_value(field_bytes)
        };

        values.push(value);

        if pos < line.len() && line[pos] == format.field_terminator {
            pos += 1;
        } else {
            pos = line.len() + 1;
        }
    }
    if pos <= line.len() {
        warn_load_field_count(num_fields + 1, num_fields);
    }

    Ok(())
}

fn warn_load_field_count(actual: usize, expected: usize) {
    if actual != expected {
        crate::engine::api::push_static_statement_warning(
            crate::error::ConditionLevel::Warning,
            if actual < expected {
                crate::error::catalog::ER_WARN_TOO_FEW_RECORDS
            } else {
                crate::error::catalog::ER_WARN_TOO_MANY_RECORDS
            },
            if actual < expected {
                "Row doesn't contain data for all columns"
            } else {
                "Row contained more data than input columns"
            },
        );
    }
}

fn unescape_field(field: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(field.len());
    let mut i = 0;
    while i < field.len() {
        if field[i] == b'\\' && i + 1 < field.len() {
            i += 1;
            out.push(match field[i] {
                b'0' => 0,
                b'b' => 8,
                b'n' => b'\n',
                b'r' => b'\r',
                b't' => b'\t',
                b'\\' => b'\\',
                b'Z' => 0x1a,
                other => other,
            });
        } else {
            out.push(field[i]);
        }
        i += 1;
    }
    out
}

/// Coerce a field from a borrowed byte slice, avoiding allocation for int fast paths.
#[inline]
fn coerce_field_borrowed(field_bytes: &[u8], op: &CoercionOp) -> Result<Value, QueryError> {
    coerce_field_borrowed_strict(field_bytes, op)
        .or_else(|err| defer_ignored_load_conversion(field_bytes, err))
}

fn coerce_field_borrowed_strict(field_bytes: &[u8], op: &CoercionOp) -> Result<Value, QueryError> {
    match op {
        CoercionOp::SignedInt { min, max, column } => {
            if let Some(v) = parse_i64_fast(field_bytes) {
                return checked_signed_load_value(v, *min, *max, column);
            }
            coerce_field(field_bytes, op)
        }
        CoercionOp::UnsignedInt { max, column } => {
            if let Some(v) = parse_u64_fast(field_bytes) {
                return checked_unsigned_load_value(v, *max, column);
            }
            coerce_field(field_bytes, op)
        }
        CoercionOp::CharPad {
            len,
            column,
            charset,
        } => {
            if field_bytes.is_ascii() {
                let compact = crate::util::pad_space::pad_space_trim_end(field_bytes);
                if compact.len() > *len {
                    return Err(error(
                        crate::error::catalog::ER_DATA_TOO_LONG,
                        format!("data too long for column '{column}'"),
                    ));
                }
                let value = ascii_bytes_to_string_value(compact);
                validate_load_text_charset(&value, charset.as_deref())?;
                return Ok(value);
            }
            coerce_field(field_bytes, op)
        }
        CoercionOp::VarCharLimit {
            len,
            column,
            charset,
        } => {
            if field_bytes.is_ascii() {
                if field_bytes.len() <= *len {
                    let value = ascii_bytes_to_string_value(field_bytes);
                    validate_load_text_charset(&value, charset.as_deref())?;
                    return Ok(value);
                }
                let (kept, removed) = field_bytes.split_at(*len);
                if removed.iter().all(|byte| *byte == b' ') {
                    warn_load_decimal_scale();
                    let value = ascii_bytes_to_string_value(kept);
                    validate_load_text_charset(&value, charset.as_deref())?;
                    return Ok(value);
                }
                return Err(error(
                    crate::error::catalog::ER_DATA_TOO_LONG,
                    format!("data too long for column '{column}'"),
                ));
            }
            coerce_field(field_bytes, op)
        }
        CoercionOp::Decimal {
            precision,
            scale,
            unsigned,
            column,
        } => {
            if precision.is_some_and(|precision| precision <= 18) {
                checked_decimal_load_value(
                    parse_decimal_compact_value(field_bytes, *scale)?,
                    *precision,
                    scale.unwrap_or(0),
                    *unsigned,
                    column,
                )
            } else {
                // Wider/unspecified DECIMAL retains the arbitrary-precision
                // string path and is canonicalized by the storage coercer.
                coerce_field(field_bytes, op)
            }
        }
        CoercionOp::Date => {
            // Byte-level parse skips the utf8 validation + str-side
            // trim that `parse_date_to_days(&str)` would otherwise
            // run on each cell. profile-p2-fix3 attributed ~7% of
            // worker CPU to that trim alone on sf=1 lineitem (three
            // DATE columns × 6M rows).
            crate::storage::parse_date_to_days_bytes(field_bytes).map(Value::Date)
        }
        CoercionOp::Time => match std::str::from_utf8(field_bytes) {
            Ok(s) => crate::storage::parse_time_to_micros(s).map(Value::Time),
            Err(_) => coerce_field(field_bytes, op),
        },
        CoercionOp::DateTime => match std::str::from_utf8(field_bytes) {
            Ok(s) => crate::storage::parse_datetime_to_micros(s).map(Value::DateTime),
            Err(_) => coerce_field(field_bytes, op),
        },
        CoercionOp::Timestamp { fsp, column } => match std::str::from_utf8(field_bytes) {
            Ok(s) => {
                let wall = crate::storage::quantize_datetime_text_micros(
                    crate::storage::parse_datetime_to_micros(s)?,
                    s,
                    *fsp,
                );
                let utc =
                    crate::engine::expr::session_local_datetime_to_utc_timestamp_micros(wall)?;
                crate::storage::canonicalize_loaded_timestamp_value(utc, wall, *fsp, column)
            }
            Err(_) => coerce_field(field_bytes, op),
        },
        CoercionOp::Year => {
            if let Some(v) = parse_i64_fast(field_bytes) {
                return crate::util::temporal::year_byte_from_int(v).map(Value::Year);
            }
            match std::str::from_utf8(field_bytes) {
                Ok(s) => parse_year_byte(s).map(Value::Year),
                Err(_) => coerce_field(field_bytes, op),
            }
        }
        _ => coerce_field(field_bytes, op),
    }
}

fn load_out_of_range(column: &str) -> QueryError {
    error(
        crate::error::catalog::ER_WARN_DATA_OUT_OF_RANGE,
        format!("out of range value for column '{column}'"),
    )
}

fn validate_load_text_charset(value: &Value, charset: Option<&str>) -> Result<(), QueryError> {
    let Some(charset) = charset else {
        return Ok(());
    };
    let text = match value {
        Value::String(text) => text.as_str(),
        Value::SmallString(bytes) => {
            // SmallString is only built after UTF-8/ASCII parsing.
            unsafe { std::str::from_utf8_unchecked(bytes.as_slice()) }
        }
        _ => return Ok(()),
    };
    crate::charset::validate_text_for_charset(charset, text)
}

fn checked_signed_load_value(
    value: i64,
    min: i64,
    max: i64,
    column: &str,
) -> Result<Value, QueryError> {
    if (min..=max).contains(&value) {
        Ok(Value::Int(value))
    } else {
        Err(load_out_of_range(column))
    }
}

fn checked_unsigned_load_value(value: u64, max: u64, column: &str) -> Result<Value, QueryError> {
    if value <= max {
        Ok(Value::UInt(value))
    } else {
        Err(load_out_of_range(column))
    }
}

fn checked_decimal_load_value(
    value: Value,
    precision: Option<u8>,
    target_scale: u8,
    unsigned: bool,
    column: &str,
) -> Result<Value, QueryError> {
    let Some(precision) = precision else {
        return Ok(value);
    };
    let Value::DecimalCompact(magnitude, value_scale) = value else {
        return Err(load_out_of_range(column));
    };
    let absolute = magnitude.unsigned_abs();
    // DecimalCompact stores the value already scaled to `target_scale`, so
    // DECIMAL(P,S) fits exactly iff |magnitude| < 10^P. This avoids an
    // integer logarithm for every loaded decimal while preserving the zero,
    // sign and scale rules below. A precision beyond u64's power range
    // necessarily accepts every compact i64 magnitude.
    let precision_fits = 10u64
        .checked_pow(u32::from(precision))
        .map_or(true, |limit| absolute < limit);
    if value_scale == target_scale && (!unsigned || magnitude >= 0) && precision_fits {
        Ok(Value::DecimalCompact(magnitude, value_scale))
    } else {
        Err(load_out_of_range(column))
    }
}

#[inline]
fn validate_decimal_compact_parts(
    magnitude: i64,
    value_scale: u8,
    precision: u8,
    target_scale: u8,
    unsigned: bool,
    column: &str,
) -> Result<(), QueryError> {
    let precision_fits = 10u64
        .checked_pow(u32::from(precision))
        .map_or(true, |limit| magnitude.unsigned_abs() < limit);
    if value_scale == target_scale && (!unsigned || magnitude >= 0) && precision_fits {
        Ok(())
    } else {
        Err(load_out_of_range(column))
    }
}

fn parse_chunk(
    reader: &mut BufReader<Box<dyn Read>>,
    buf: &mut Vec<u8>,
    parser: &FieldParser,
    coercion_plan: &[CoercionOp],
    num_fields: usize,
    max_rows: usize,
) -> Result<(Vec<Vec<Value>>, bool), QueryError> {
    if let Some(format) = fast_parse_format(parser) {
        return parse_chunk_fast(reader, format, coercion_plan, num_fields, max_rows);
    }

    let mut rows = Vec::with_capacity(max_rows);
    while rows.len() < max_rows {
        if !read_load_record(reader, parser, buf, num_fields)? {
            return Ok((rows, true));
        }
        parse_line_into_rows(parser, buf, coercion_plan, num_fields, &mut rows)?;
    }
    Ok((rows, false))
}

/// Byte-oriented, enclosure-aware record reader. State survives BufRead
/// boundaries and escaped/doubled enclosures; only the exact delimiter is removed.
fn read_load_record(
    reader: &mut impl BufRead,
    parser: &FieldParser,
    out: &mut Vec<u8>,
    num_fields: usize,
) -> Result<bool, QueryError> {
    out.clear();
    let mut quoted = false;
    let mut field_start = true;
    let mut escaped = false;
    let mut quote_pending = false;
    let mut multibyte_state = 0u8;
    let shared_terminator = parser.field_terminator == parser.line_terminator;
    let mut fields = 1usize;
    let mut delimiter_end = 0usize;
    let mut charge = mem_budget::ControlledMemoryCharge::try_new(
        mem_budget::ControlledMemoryCategory::StorageScratch,
        out.capacity(),
    )?;
    // Newline bytes cannot be a trail byte in these legacy encodings.
    // Keep the bulk record reader; only the rare trailing escape run needs
    // a forward character-boundary check. Other delimiters retain the scanner.
    let bulk_record_boundary = parser.input_charset == LoadLegacyCharset::None
        || matches!(parser.line_terminator.as_slice(), b"\n" | b"\r\n");
    if bulk_record_boundary
        && parser.enclosed_by.is_none()
        && !(shared_terminator && num_fields > 1)
    {
        let terminator = &parser.line_terminator;
        let last = *terminator.last().ok_or_else(|| {
            error(
                crate::error::catalog::ER_NOT_SUPPORTED_YET,
                "Empty LOAD line terminator",
            )
        })?;
        loop {
            crate::engine::cancel::check_cancelled()?;
            let available = reader.fill_buf().map_err(io_err)?;
            if available.is_empty() {
                return Ok(!out.is_empty());
            }
            let take = memchr(last, available).map_or(available.len(), |pos| pos + 1);
            let required = out.len().saturating_add(take);
            if required > out.capacity() {
                let capacity = required.max(out.capacity().saturating_mul(2)).max(4096);
                charge.try_grow(capacity - out.capacity())?;
                out.reserve_exact(capacity - out.len());
            }
            out.extend_from_slice(&available[..take]);
            reader.consume(take);
            if out.ends_with(terminator) {
                let end = out.len() - terminator.len();
                let escaped = parser.record_ending_is_escaped(&out[..end]);
                if !escaped {
                    out.truncate(end);
                    return Ok(true);
                }
            }
        }
    }
    loop {
        crate::engine::cancel::check_cancelled()?;
        let available = reader.fill_buf().map_err(io_err)?;
        if available.is_empty() {
            let present = !out.is_empty();
            if shared_terminator && fields > 1 && out.len() == delimiter_end {
                // EOF after a field delimiter supplies no next field. An
                // explicit empty enclosed field remains a real empty value.
                out.truncate(out.len() - parser.field_terminator.len());
            }
            return Ok(present);
        }
        let mut consumed = 0;
        let mut ended = false;
        for &byte in available {
            if out.len() == out.capacity() {
                let capacity = out.capacity().saturating_mul(2).max(4096);
                charge.try_grow(capacity - out.capacity())?;
                out.reserve_exact(capacity - out.len());
            }
            out.push(byte);
            consumed += 1;
            // Carry character boundaries across BufRead chunks. Syntax bytes
            // inside a legacy multibyte character are data, including 0x5c.
            if multibyte_state != 0 {
                let continuation = match multibyte_state {
                    1 if parser.input_charset == LoadLegacyCharset::Gb18030
                        && byte.is_ascii_digit() =>
                    {
                        multibyte_state = 2;
                        true
                    }
                    1 => {
                        multibyte_state = 0;
                        parser.input_charset.trail(byte)
                    }
                    2 => {
                        multibyte_state = 3;
                        (0x81..=0xfe).contains(&byte)
                    }
                    _ => {
                        multibyte_state = 0;
                        byte.is_ascii_digit()
                    }
                };
                if continuation {
                    continue;
                }
                multibyte_state = 0;
            }
            if escaped {
                escaped = false;
                if parser.input_charset.lead(byte) {
                    multibyte_state = 1;
                }
                continue;
            }
            if quote_pending {
                quote_pending = false;
                if parser.enclosed_by == Some(byte) {
                    continue;
                }
                quoted = false;
            }
            if parser.input_charset.lead(byte) {
                multibyte_state = 1;
                field_start = false;
                continue;
            }
            if parser.escaped_by == Some(byte) {
                escaped = true;
                field_start = false;
                continue;
            }
            if quoted {
                if parser.enclosed_by == Some(byte) {
                    quote_pending = true;
                }
                continue;
            }
            if field_start && parser.enclosed_by == Some(byte) {
                quoted = true;
                field_start = false;
                continue;
            }
            if !parser.line_terminator.is_empty() && out.ends_with(&parser.line_terminator) {
                // MySQL consumes an identical delimiter as a field separator
                // until every input target has been read, then ends the row.
                // Do not recognize overlapping occurrences of a multi-byte token.
                if shared_terminator && fields < num_fields {
                    if out.len() >= delimiter_end + parser.line_terminator.len() {
                        fields += 1;
                        delimiter_end = out.len();
                        field_start = true;
                    }
                    continue;
                }
                if shared_terminator && out.len() < delimiter_end + parser.line_terminator.len() {
                    continue;
                }
                out.truncate(out.len() - parser.line_terminator.len());
                ended = true;
                break;
            }
            field_start =
                !parser.field_terminator.is_empty() && out.ends_with(&parser.field_terminator);
            if !parser.line_starting_by.is_empty() && out.ends_with(&parser.line_starting_by) {
                field_start = true;
            }
        }
        reader.consume(consumed);
        if ended {
            return Ok(true);
        }
    }
}

fn parse_line_into_rows(
    parser: &FieldParser,
    line_bytes: &[u8],
    coercion_plan: &[CoercionOp],
    num_fields: usize,
    rows: &mut Vec<Vec<Value>>,
) -> Result<(), QueryError> {
    let actual_line = if !parser.line_starting_by.is_empty() {
        if let Some(pos) = find_subsequence(line_bytes, &parser.line_starting_by) {
            &line_bytes[pos + parser.line_starting_by.len()..]
        } else {
            return Ok(());
        }
    } else {
        line_bytes
    };
    let row = parser.parse_row(actual_line, coercion_plan, num_fields)?;
    rows.push(row);
    Ok(())
}

fn find_subsequence(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    if needle.len() == 1 {
        return haystack.iter().position(|&b| b == needle[0]);
    }
    haystack
        .windows(needle.len())
        .position(|window| window == needle)
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum LoadLegacyCharset {
    None,
    Gbk,
    Gb18030,
    Big5,
    ShiftJis,
}

impl LoadLegacyCharset {
    fn from_name(name: Option<&str>) -> Self {
        match name.unwrap_or("").to_ascii_lowercase().as_str() {
            "gbk" => Self::Gbk,
            "gb18030" => Self::Gb18030,
            "big5" => Self::Big5,
            "sjis" | "cp932" => Self::ShiftJis,
            _ => Self::None,
        }
    }
    fn lead(self, byte: u8) -> bool {
        match self {
            Self::Gbk | Self::Gb18030 | Self::Big5 => (0x81..=0xfe).contains(&byte),
            Self::ShiftJis => (0x81..=0x9f).contains(&byte) || (0xe0..=0xfc).contains(&byte),
            Self::None => false,
        }
    }
    fn trail(self, byte: u8) -> bool {
        match self {
            Self::Gbk | Self::Gb18030 => (0x40..=0xfe).contains(&byte) && byte != 0x7f,
            Self::Big5 => (0x40..=0x7e).contains(&byte) || (0xa1..=0xfe).contains(&byte),
            Self::ShiftJis => (0x40..=0x7e).contains(&byte) || (0x80..=0xfc).contains(&byte),
            Self::None => false,
        }
    }
    fn width(self, bytes: &[u8]) -> usize {
        if bytes.len() < 2 || !self.lead(bytes[0]) {
            return 1;
        }
        if self == Self::Gb18030
            && bytes.len() >= 4
            && (0x30..=0x39).contains(&bytes[1])
            && (0x81..=0xfe).contains(&bytes[2])
            && (0x30..=0x39).contains(&bytes[3])
        {
            return 4;
        }
        if self.trail(bytes[1]) {
            2
        } else {
            1
        }
    }
}

struct FieldParser {
    input_charset: LoadLegacyCharset,
    field_terminator: Vec<u8>,
    line_terminator: Vec<u8>,
    enclosed_by: Option<u8>,
    // OPTIONALLY affects output formatting, not LOAD input acceptance.
    #[allow(dead_code)]
    /// Escape byte. `None` = `ESCAPED BY ''`, escape processing disabled
    /// (G10). `Some(b'\\')` is the MySQL default.
    escaped_by: Option<u8>,
    line_starting_by: Vec<u8>,
}

impl FieldParser {
    fn record_ending_is_escaped(&self, bytes: &[u8]) -> bool {
        let Some(escape) = self.escaped_by else {
            return false;
        };
        if bytes.last() != Some(&escape) {
            return false;
        }
        if self.input_charset == LoadLegacyCharset::None {
            return bytes
                .iter()
                .rev()
                .take_while(|byte| **byte == escape)
                .count()
                % 2
                == 1;
        }
        let mut position = 0;
        let mut trailing_escapes = 0usize;
        while position < bytes.len() {
            let width = if bytes[position].is_ascii() {
                1
            } else {
                self.input_charset.width(&bytes[position..])
            };
            if width == 1 && bytes[position] == escape {
                trailing_escapes += 1;
            } else {
                trailing_escapes = 0;
            }
            position += width;
        }
        trailing_escapes % 2 == 1
    }

    fn parse_row(
        &self,
        line: &[u8],
        coercion_plan: &[CoercionOp],
        num_fields: usize,
    ) -> Result<Vec<Value>, QueryError> {
        let fields = self.parse_row_raw(line);
        warn_load_field_count(fields.len(), num_fields);
        (0..num_fields.min(fields.len()))
            .map(|i| match fields.get(i).and_then(|v| v.as_deref()) {
                None => Ok(Value::Null),
                Some(bytes) => match coercion_plan.get(i) {
                    Some(op) => coerce_field(bytes, op),
                    None => Ok(str_to_string_value(load_utf8(bytes)?)),
                },
            })
            .collect()
    }

    /// Split a line into raw field byte-vectors (post-unescape, post-
    /// unenclose) without any type coercion. `None` marks a SQL NULL
    /// (`\N`). Used by the general LOAD path, which binds fields to
    /// `@user_var`s / columns and defers typing to `coerce_row`.
    fn parse_row_raw(&self, line: &[u8]) -> Vec<Option<Vec<u8>>> {
        let mut values: Vec<Option<Vec<u8>>> = Vec::new();
        let mut pos = 0;
        let ft = &self.field_terminator;
        let ft_len = ft.len();

        loop {
            let (field_bytes, next_pos) = if let Some(enc) = self.enclosed_by {
                if pos < line.len() && line[pos] == enc {
                    self.parse_enclosed_field(line, pos + 1, enc, ft)
                } else {
                    self.parse_unenclosed_field(line, pos, ft)
                }
            } else {
                self.parse_unenclosed_field(line, pos, ft)
            };

            let enclosed = self
                .enclosed_by
                .is_some_and(|enc| line.get(pos) == Some(&enc));
            // NULL is determined from source bytes, before unescaping.
            // An escaped escape followed by N is a literal string, not NULL.
            let mut raw = &line[pos..next_pos];
            if enclosed {
                raw = &raw[1..];
                if self.enclosed_by.is_some_and(|enc| raw.last() == Some(&enc)) {
                    raw = &raw[..raw.len() - 1];
                }
            }
            let null_escape = self.escaped_by.is_some_and(|escape| raw == [escape, b'N']);
            if null_escape || (self.enclosed_by.is_some() && !enclosed && field_bytes == b"NULL") {
                values.push(None);
            } else {
                values.push(Some(field_bytes));
            }
            pos = next_pos;

            // Consume the field terminator; stop when the line is exhausted.
            if ft_len > 0 && pos + ft_len <= line.len() && line[pos..pos + ft_len] == ft[..] {
                pos += ft_len;
                // A trailing terminator yields a final empty field.
                if pos == line.len() {
                    values.push(Some(Vec::new()));
                    break;
                }
            } else {
                break;
            }
        }
        values
    }

    fn parse_unenclosed_field(&self, line: &[u8], start: usize, ft: &[u8]) -> (Vec<u8>, usize) {
        let mut pos = start;
        let mut has_escape = false;

        // A delimiter that cannot be a continuation byte may be searched
        // in bulk. Only escape candidates require establishing a character
        // boundary; ordinary fields keep the byte-scanner throughput.
        let safe_delimiter = ft.len() == 1
            && ft[0].is_ascii()
            && !self.input_charset.trail(ft[0])
            && !(self.input_charset == LoadLegacyCharset::Gb18030 && ft[0].is_ascii_digit());
        if safe_delimiter && self.escaped_by.is_none_or(|escape| escape.is_ascii()) {
            let mut boundary = start;
            loop {
                let candidate = match self.escaped_by {
                    Some(escape) => memchr2(ft[0], escape, &line[pos..]),
                    None => memchr(ft[0], &line[pos..]),
                };
                let Some(candidate) = candidate.map(|offset| pos + offset) else {
                    pos = line.len();
                    break;
                };
                if line[candidate] == ft[0] {
                    pos = candidate;
                    break;
                }
                while boundary < candidate {
                    boundary += if line[boundary].is_ascii() {
                        1
                    } else {
                        self.input_charset.width(&line[boundary..])
                    };
                }
                if boundary > candidate {
                    pos = boundary;
                    continue;
                }
                if candidate + 1 < line.len() {
                    has_escape = true;
                    pos = candidate + 1 + self.input_charset.width(&line[candidate + 1..]);
                } else {
                    pos = line.len();
                }
                boundary = pos;
            }
        } else {
            while pos < line.len() {
                let width = if line[pos].is_ascii() {
                    1
                } else {
                    self.input_charset.width(&line[pos..])
                };
                if width > 1 {
                    pos += width;
                    continue;
                }
                if ft.len() == 1 {
                    if line[pos] == ft[0] {
                        break;
                    }
                } else if !ft.is_empty()
                    && pos + ft.len() <= line.len()
                    && &line[pos..pos + ft.len()] == &ft[..]
                {
                    break;
                }
                if self.escaped_by == Some(line[pos]) && pos + 1 < line.len() {
                    has_escape = true;
                    pos += 1 + self.input_charset.width(&line[pos + 1..]);
                } else {
                    pos += 1;
                }
            }
        }

        if !has_escape {
            return (line[start..pos].to_vec(), pos);
        }

        let mut out = Vec::with_capacity(pos - start);
        let mut i = start;
        while i < pos {
            let width = if line[i].is_ascii() {
                1
            } else {
                self.input_charset.width(&line[i..pos])
            };
            if width > 1 {
                out.extend_from_slice(&line[i..i + width]);
                i += width;
                continue;
            }
            if self.escaped_by == Some(line[i]) && i + 1 < pos {
                i += 1;
                let width = if line[i].is_ascii() {
                    1
                } else {
                    self.input_charset.width(&line[i..pos])
                };
                if width > 1 {
                    out.extend_from_slice(&line[i..i + width]);
                    i += width;
                    continue;
                }
                out.push(match line[i] {
                    b'0' => 0,
                    b'b' => 8,
                    b'n' => b'\n',
                    b'r' => b'\r',
                    b't' => b'\t',
                    b'\\' => b'\\',
                    b'Z' => 0x1a,
                    other => other,
                });
            } else {
                out.push(line[i]);
            }
            i += 1;
        }
        (out, pos)
    }

    fn parse_enclosed_field(
        &self,
        line: &[u8],
        start: usize,
        enc: u8,
        ft: &[u8],
    ) -> (Vec<u8>, usize) {
        let mut pos = start;
        let mut out = Vec::new();

        while pos < line.len() {
            let width = if line[pos].is_ascii() {
                1
            } else {
                self.input_charset.width(&line[pos..])
            };
            if width > 1 {
                out.extend_from_slice(&line[pos..pos + width]);
                pos += width;
                continue;
            }
            if line[pos] == enc {
                pos += 1;
                if pos < line.len() && line[pos] == enc {
                    out.push(enc);
                    pos += 1;
                } else {
                    break;
                }
            } else if self.escaped_by == Some(line[pos]) && pos + 1 < line.len() {
                pos += 1;
                let width = if line[pos].is_ascii() {
                    1
                } else {
                    self.input_charset.width(&line[pos..])
                };
                if width > 1 {
                    out.extend_from_slice(&line[pos..pos + width]);
                    pos += width;
                    continue;
                }
                out.push(match line[pos] {
                    b'0' => 0,
                    b'b' => 8,
                    b'Z' => 0x1a,
                    b'n' => b'\n',
                    b'r' => b'\r',
                    b't' => b'\t',
                    b'\\' => b'\\',
                    other => other,
                });
                pos += 1;
            } else {
                out.push(line[pos]);
                pos += 1;
            }
        }

        if pos + ft.len() <= line.len() && &line[pos..pos + ft.len()] == &ft[..] {
            // skip field terminator after closing enclosure
        }

        (out, pos)
    }
}

#[derive(Debug, Clone)]
#[allow(dead_code)]
enum CoercionOp {
    SignedInt {
        min: i64,
        max: i64,
        column: String,
    },
    UnsignedInt {
        max: u64,
        column: String,
    },
    FloatF32,
    FloatF64,
    Decimal {
        precision: Option<u8>,
        scale: Option<u8>,
        unsigned: bool,
        column: String,
    },
    CharPad {
        len: usize,
        column: String,
        charset: Option<String>,
    },
    VarCharLimit {
        len: usize,
        column: String,
        charset: Option<String>,
    },
    BinaryPad(usize),
    VarBinaryLimit(usize),
    Text {
        max_len: u32,
        column: String,
        charset: Option<String>,
    },
    PassThroughString,
    PassThroughBytes,
    VectorBase64 {
        datatype: crate::vector::VectorDataType,
        dimension: Option<u32>,
    },
    Date,
    Time,
    DateTime,
    Timestamp {
        fsp: u8,
        column: String,
    },
    Year,
    Enum,
    Set,
    Bit,
}

fn build_coercion_plan(table: &TableSchema, column_indexes: &[usize]) -> Vec<CoercionOp> {
    column_indexes
        .iter()
        .map(|&idx| {
            let col = &table.columns[idx];
            match &col.data_type {
                DataTypeKind::TinyInt
                | DataTypeKind::SmallInt
                | DataTypeKind::MediumInt
                | DataTypeKind::Int
                | DataTypeKind::BigInt => integer_coercion_op(col),
                // BOOL is a pure alias for signed TINYINT — load as a
                // signed integer, no 0/1 truthy clamp (issue #59).
                DataTypeKind::Bool => integer_coercion_op(col),
                DataTypeKind::Float { .. } => CoercionOp::FloatF32,
                DataTypeKind::Double { .. } => CoercionOp::FloatF64,
                DataTypeKind::Decimal { precision, scale } => CoercionOp::Decimal {
                    precision: *precision,
                    scale: *scale,
                    unsigned: col.unsigned,
                    column: col.name.clone(),
                },
                DataTypeKind::Char { len } => CoercionOp::CharPad {
                    len: len.unwrap_or(1),
                    column: col.name.clone(),
                    charset: col.charset.clone(),
                },
                DataTypeKind::VarChar { len } => CoercionOp::VarCharLimit {
                    len: len.unwrap_or(255),
                    column: col.name.clone(),
                    charset: col.charset.clone(),
                },
                DataTypeKind::Binary { len } => CoercionOp::BinaryPad(len.unwrap_or(1)),
                DataTypeKind::VarBinary { len } => CoercionOp::VarBinaryLimit(len.unwrap_or(255)),
                DataTypeKind::Text { max_len } => CoercionOp::Text {
                    max_len: *max_len,
                    column: col.name.clone(),
                    charset: col.charset.clone(),
                },
                DataTypeKind::Json => CoercionOp::PassThroughString,
                // Geometry is loaded as raw text (WKT) or bytes (WKB/internal)
                // and coerced to internal form by the column constraint path.
                DataTypeKind::Geometry { .. } => CoercionOp::PassThroughString,
                DataTypeKind::Vector { dim, datatype } => {
                    if std::env::var("MYDB_LOAD_DATA_VECTOR_FORMAT")
                        .is_ok_and(|v| v.eq_ignore_ascii_case("base64"))
                    {
                        CoercionOp::VectorBase64 {
                            datatype: *datatype,
                            dimension: *dim,
                        }
                    } else {
                        CoercionOp::PassThroughString
                    }
                }
                DataTypeKind::Blob { .. } => CoercionOp::PassThroughBytes,
                DataTypeKind::Date => CoercionOp::Date,
                DataTypeKind::Time { .. } => CoercionOp::Time,
                DataTypeKind::DateTime { .. } => CoercionOp::DateTime,
                DataTypeKind::Timestamp { fsp } => CoercionOp::Timestamp {
                    fsp: *fsp,
                    column: col.name.clone(),
                },
                DataTypeKind::Year => CoercionOp::Year,
                DataTypeKind::Enum { .. } => CoercionOp::Enum,
                DataTypeKind::Set { .. } => CoercionOp::Set,
                DataTypeKind::Bit { .. } => CoercionOp::Bit,
            }
        })
        .collect()
}

fn integer_coercion_op(column: &crate::storage::ColumnSchema) -> CoercionOp {
    let (signed_min, signed_max, unsigned_max) = match column.data_type {
        DataTypeKind::TinyInt | DataTypeKind::Bool => (-128, 127, 255),
        DataTypeKind::SmallInt => (-32_768, 32_767, 65_535),
        DataTypeKind::MediumInt => (-8_388_608, 8_388_607, 16_777_215),
        DataTypeKind::Int => (-2_147_483_648, 2_147_483_647, 4_294_967_295),
        DataTypeKind::BigInt => (i64::MIN, i64::MAX, u64::MAX),
        _ => unreachable!("integer coercion requested for a non-integer column"),
    };
    if column.unsigned {
        CoercionOp::UnsignedInt {
            max: unsigned_max,
            column: column.name.clone(),
        }
    } else {
        CoercionOp::SignedInt {
            min: signed_min,
            max: signed_max,
            column: column.name.clone(),
        }
    }
}

/// Whether the fast byte parser produces the exact value variant and domain
/// expected by the row codec for every column in this table.
///
/// This is deliberately a type-family gate, not a workload or table-name
/// special case.  Types whose assignment semantics still need the full DML
/// kernel (FLOAT(M,D), JSON, ENUM/SET, temporal session policy, vectors, ...)
/// return false and retain `coerce_row_for_load`.  The covered families are
/// the ordinary fixed integers, compact DECIMAL, CHAR/VARCHAR, and DATE.
fn load_fast_values_are_canonical(table: &TableSchema) -> bool {
    if table_has_generated_columns(table) {
        return false;
    }
    let temporal_bits = crate::engine::expr::session_temporal_sql_mode_bits();
    if temporal_bits
        & (crate::engine::api::TEMPORAL_MODE_NO_ZERO_DATE
            | crate::engine::api::TEMPORAL_MODE_ALLOW_INVALID_DATES)
        != 0
    {
        return false;
    }
    table.columns.iter().all(|column| match &column.data_type {
        DataTypeKind::TinyInt
        | DataTypeKind::SmallInt
        | DataTypeKind::MediumInt
        | DataTypeKind::Int
        | DataTypeKind::BigInt
        | DataTypeKind::Bool
        | DataTypeKind::Char { .. }
        | DataTypeKind::VarChar { .. }
        | DataTypeKind::Date
        | DataTypeKind::Timestamp { .. } => true,
        // A valid DECIMAL(P,S) with P <= 18 always fits the parser's compact
        // i64 magnitude lane.  Wider/unspecified decimals keep the general
        // arbitrary-precision coercer.
        DataTypeKind::Decimal {
            precision: Some(precision),
            ..
        } => *precision <= 18,
        _ => false,
    })
}

/// Validate the few assignment-domain properties that a lexical parser
/// cannot encode in the `Value` discriminant itself.  This replaces a second
/// full conversion of every already-typed cell; it does not weaken errors.
fn validate_canonical_load_row(table: &TableSchema, values: &[Value]) -> Result<(), QueryError> {
    debug_assert_eq!(table.columns.len(), values.len());
    for (value, column) in values.iter().zip(&table.columns) {
        if matches!(value, Value::Null) {
            continue;
        }
        let in_range = match (&column.data_type, column.unsigned, value) {
            (DataTypeKind::TinyInt | DataTypeKind::Bool, false, Value::Int(v)) => {
                (-128..=127).contains(v)
            }
            (DataTypeKind::TinyInt | DataTypeKind::Bool, true, Value::UInt(v)) => *v <= 255,
            (DataTypeKind::SmallInt, false, Value::Int(v)) => (-32_768..=32_767).contains(v),
            (DataTypeKind::SmallInt, true, Value::UInt(v)) => *v <= 65_535,
            (DataTypeKind::MediumInt, false, Value::Int(v)) => (-8_388_608..=8_388_607).contains(v),
            (DataTypeKind::MediumInt, true, Value::UInt(v)) => *v <= 16_777_215,
            (DataTypeKind::Int, false, Value::Int(v)) => {
                (-2_147_483_648..=2_147_483_647).contains(v)
            }
            (DataTypeKind::Int, true, Value::UInt(v)) => *v <= 4_294_967_295,
            (DataTypeKind::BigInt, false, Value::Int(_))
            | (DataTypeKind::BigInt, true, Value::UInt(_)) => true,
            (
                DataTypeKind::Decimal {
                    precision: Some(precision),
                    scale,
                },
                unsigned,
                Value::DecimalCompact(magnitude, value_scale),
            ) => {
                let target_scale = scale.unwrap_or(0);
                let absolute = magnitude.unsigned_abs();
                let total_digits = if absolute == 0 {
                    1
                } else {
                    absolute.ilog10() as usize + 1
                };
                let integer_digits = if *magnitude == 0 {
                    0
                } else {
                    total_digits.saturating_sub(target_scale as usize)
                };
                *value_scale == target_scale
                    && (!unsigned || *magnitude >= 0)
                    && integer_digits <= precision.saturating_sub(target_scale) as usize
            }
            (DataTypeKind::Char { .. } | DataTypeKind::VarChar { .. }, _, value) => {
                let text = match value {
                    Value::String(text) => Some(text.as_str()),
                    Value::SmallString(bytes) => {
                        // Fast LOAD constructs this lane only after UTF-8
                        // validation (ASCII is its dominant case).
                        Some(unsafe { std::str::from_utf8_unchecked(bytes.as_slice()) })
                    }
                    _ => None,
                };
                if let (Some(charset), Some(text)) = (column.charset.as_deref(), text) {
                    crate::charset::validate_text_for_charset(charset, text)?;
                }
                text.is_some()
            }
            (DataTypeKind::Date, _, Value::Date(_)) => true,
            (DataTypeKind::Timestamp { .. }, _, Value::Timestamp(_)) => true,
            _ => false,
        };
        if !in_range {
            return Err(error(
                crate::error::catalog::ER_WARN_DATA_OUT_OF_RANGE,
                format!("out of range value for column '{}'", column.name),
            ));
        }
    }
    Ok(())
}

fn parse_i64_fast(bytes: &[u8]) -> Option<i64> {
    if bytes.is_empty() {
        return None;
    }
    let (negative, start) = if bytes[0] == b'-' {
        (true, 1)
    } else if bytes[0] == b'+' {
        (false, 1)
    } else {
        (false, 0)
    };
    if start >= bytes.len() {
        return None;
    }
    let mut val: i64 = 0;
    for &b in &bytes[start..] {
        if !b.is_ascii_digit() {
            return None;
        }
        val = val.checked_mul(10)?.checked_add((b - b'0') as i64)?;
    }
    if negative {
        Some(-val)
    } else {
        Some(val)
    }
}

fn parse_u64_fast(bytes: &[u8]) -> Option<u64> {
    if bytes.is_empty() {
        return None;
    }
    let start = if bytes[0] == b'+' { 1 } else { 0 };
    if start >= bytes.len() {
        return None;
    }
    let mut val: u64 = 0;
    for &b in &bytes[start..] {
        if !b.is_ascii_digit() {
            return None;
        }
        val = val.checked_mul(10)?.checked_add((b - b'0') as u64)?;
    }
    Some(val)
}

fn trim_ascii_bytes(mut bytes: &[u8]) -> &[u8] {
    while bytes.first().is_some_and(u8::is_ascii_whitespace) {
        bytes = &bytes[1..];
    }
    while bytes.last().is_some_and(u8::is_ascii_whitespace) {
        bytes = &bytes[..bytes.len() - 1];
    }
    bytes
}

fn parse_decimal_compact_value(field_bytes: &[u8], scale: Option<u8>) -> Result<Value, QueryError> {
    if let Some((magnitude, scale, discarded)) =
        parse_decimal_compact_parts_fast(field_bytes, scale)
    {
        if discarded {
            warn_load_decimal_scale();
        }
        if trim_ascii_bytes(field_bytes).is_empty() {
            warn_load_empty_number();
        }
        return Ok(Value::DecimalCompact(magnitude, scale));
    }
    parse_decimal_compact_value_wide(field_bytes, scale)
}

/// Round general/wide decimal assignments before storage coercion.
fn round_load_decimal_text(text: &str, scale: u8) -> Option<String> {
    let text = text.trim();
    let fractional_digits = text.split_once('.').map_or(0, |(_, fraction)| {
        fraction.bytes().take_while(u8::is_ascii_digit).count()
    });
    if fractional_digits <= usize::from(scale) && !text.contains(['e', 'E']) {
        return None;
    }
    let rounded = crate::util::decimal::rescale_decimal_big_str(text, scale)?;
    if fractional_digits > usize::from(scale) {
        warn_load_decimal_scale();
    }
    Some(rounded)
}

fn warn_load_empty_number() {
    crate::engine::api::push_statement_warning(
        crate::error::ConditionLevel::Warning,
        crate::error::catalog::ER_TRUNCATED_WRONG_VALUE_FOR_FIELD,
        "Empty numeric field converted to zero in LOAD DATA",
    );
}

fn warn_load_decimal_scale() {
    crate::engine::api::push_statement_warning(
        crate::error::ConditionLevel::Warning,
        crate::error::catalog::WARN_DATA_TRUNCATED,
        "Data truncated in LOAD DATA",
    );
}

/// Allocation-free compact parser. Diagnostics are deferred so a failed
/// compiled-row attempt cannot duplicate them when the row is decoded again.
fn parse_decimal_compact_parts_fast(
    field_bytes: &[u8],
    scale: Option<u8>,
) -> Option<(i64, u8, bool)> {
    let scale = scale.unwrap_or(0);
    let trimmed = trim_ascii_bytes(field_bytes);
    if trimmed.is_empty() {
        return Some((0, scale, false));
    }

    let mut i = 0usize;
    let mut negative = false;
    match trimmed[i] {
        b'-' => {
            negative = true;
            i += 1;
        }
        b'+' => i += 1,
        _ => {}
    }

    // This entry point is selected only for DECIMAL(P,S) with P <= 18, so
    // every valid result fits in i64. Keep the common parser in native u64;
    // ARM64 otherwise lowers checked u128 multiply/divide into noticeably
    // heavier sequences for each digit. Inputs outside that compact domain
    // fall back to the original wide parser to preserve its exact errors.
    let mut magnitude = 0u64;
    let mut any_digit = false;
    while i < trimmed.len() && trimmed[i].is_ascii_digit() {
        any_digit = true;
        let digit = u64::from(trimmed[i] - b'0');
        let Some(next) = magnitude
            .checked_mul(10)
            .and_then(|value| value.checked_add(digit))
        else {
            return None;
        };
        magnitude = next;
        i += 1;
    }

    let mut round_up = false;
    let mut discarded = false;
    let mut frac_digits = 0u32;
    let scale_u32 = u32::from(scale);
    if i < trimmed.len() && trimmed[i] == b'.' {
        i += 1;
        while i < trimmed.len() && trimmed[i].is_ascii_digit() {
            any_digit = true;
            if frac_digits == scale_u32 && !discarded {
                round_up = trimmed[i] >= b'5';
                discarded = true;
            }
            if frac_digits < scale_u32 {
                let digit = u64::from(trimmed[i] - b'0');
                let Some(next) = magnitude
                    .checked_mul(10)
                    .and_then(|value| value.checked_add(digit))
                else {
                    return None;
                };
                magnitude = next;
                frac_digits += 1;
                any_digit = true;
            }
            i += 1;
        }
    }

    if !any_digit || i != trimmed.len() {
        return None;
    }
    while frac_digits < scale_u32 {
        let Some(next) = magnitude.checked_mul(10) else {
            return None;
        };
        magnitude = next;
        frac_digits += 1;
    }
    if round_up {
        magnitude = magnitude.checked_add(1)?;
    }
    if magnitude > i64::MAX as u64 {
        return None;
    }
    if magnitude == 0 {
        negative = false;
    }
    let signed = if negative {
        -(magnitude as i64)
    } else {
        magnitude as i64
    };
    Some((signed, scale, discarded))
}

fn parse_decimal_compact_value_wide(
    field_bytes: &[u8],
    scale: Option<u8>,
) -> Result<Value, QueryError> {
    let scale = scale.unwrap_or(0);
    let trimmed = trim_ascii_bytes(field_bytes);
    if trimmed.is_empty() {
        return Ok(Value::DecimalCompact(0, scale));
    }

    let mut i = 0usize;
    let mut negative = false;
    match trimmed[i] {
        b'-' => {
            negative = true;
            i += 1;
        }
        b'+' => i += 1,
        _ => {}
    }

    let mut magnitude: u128 = 0;
    let mut any_digit = false;
    while i < trimmed.len() && trimmed[i].is_ascii_digit() {
        any_digit = true;
        let d = (trimmed[i] - b'0') as u128;
        magnitude = magnitude
            .checked_mul(10)
            .and_then(|m| m.checked_add(d))
            .ok_or_else(|| {
                error(
                    crate::error::catalog::ER_WARN_DATA_OUT_OF_RANGE,
                    "decimal integer part overflow",
                )
            })?;
        i += 1;
    }

    let mut round_up = false;
    let mut discarded = false;
    let mut frac_digits = 0u32;
    let scale_u32 = scale as u32;
    if i < trimmed.len() && trimmed[i] == b'.' {
        i += 1;
        while i < trimmed.len() && trimmed[i].is_ascii_digit() {
            any_digit = true;
            if frac_digits == scale_u32 && !discarded {
                round_up = trimmed[i] >= b'5';
                discarded = true;
            }
            if frac_digits < scale_u32 {
                let d = (trimmed[i] - b'0') as u128;
                magnitude = magnitude
                    .checked_mul(10)
                    .and_then(|m| m.checked_add(d))
                    .ok_or_else(|| {
                        error(
                            crate::error::catalog::ER_WARN_DATA_OUT_OF_RANGE,
                            "decimal fractional overflow",
                        )
                    })?;
                frac_digits += 1;
                any_digit = true;
            }
            i += 1;
        }
    }

    if !any_digit || i != trimmed.len() {
        let s = lossy_utf8(field_bytes);
        return Err(error(
            crate::error::catalog::ER_TRUNCATED_WRONG_VALUE_FOR_FIELD,
            format!("incorrect decimal value: '{}'", s.trim()),
        ));
    }

    while frac_digits < scale_u32 {
        magnitude = magnitude.checked_mul(10).ok_or_else(|| {
            error(
                crate::error::catalog::ER_WARN_DATA_OUT_OF_RANGE,
                "decimal magnitude overflow",
            )
        })?;
        frac_digits += 1;
    }
    if round_up {
        magnitude = magnitude
            .checked_add(1)
            .ok_or_else(|| load_out_of_range("DECIMAL"))?;
    }
    if discarded {
        warn_load_decimal_scale();
    }
    if magnitude == 0 {
        negative = false;
    }
    if magnitude <= i64::MAX as u128 {
        let signed = if negative {
            -(magnitude as i64)
        } else {
            magnitude as i64
        };
        Ok(Value::DecimalCompact(signed, scale))
    } else {
        let mut digits = magnitude.to_string();
        let scale = usize::from(scale);
        if scale > 0 {
            if digits.len() <= scale {
                digits = format!("{}{}", "0".repeat(scale + 1 - digits.len()), digits);
            }
            digits.insert(digits.len() - scale, '.');
        }
        if negative {
            digits.insert(0, '-');
        }
        Ok(Value::Decimal(digits))
    }
}

fn parse_year_byte(s: &str) -> Result<u8, QueryError> {
    let s = s.trim();
    if s.is_empty() || s == "0" || s == "0000" {
        return Ok(0);
    }
    let v = s.parse::<i64>().map_err(|_| {
        error(
            crate::error::catalog::ER_WRONG_VALUE_FOR_TYPE,
            format!("incorrect year value: '{}'", s),
        )
    })?;
    crate::util::temporal::year_byte_from_int(v)
}

#[inline]
fn coerce_field(field_bytes: &[u8], op: &CoercionOp) -> Result<Value, QueryError> {
    coerce_field_strict(field_bytes, op)
        .or_else(|err| defer_ignored_load_conversion(field_bytes, err))
}

fn coerce_field_strict(field_bytes: &[u8], op: &CoercionOp) -> Result<Value, QueryError> {
    match op {
        CoercionOp::SignedInt { min, max, column } => {
            if let Some(v) = parse_i64_fast(field_bytes) {
                return checked_signed_load_value(v, *min, *max, column);
            }
            let s = load_utf8(field_bytes)?;
            let trimmed = s.trim();
            if trimmed.is_empty() {
                warn_load_empty_number();
                return checked_signed_load_value(0, *min, *max, column);
            }
            if let Some(dot) = trimmed.find('.') {
                if let Ok(f) = trimmed.parse::<f64>() {
                    let _ = dot;
                    if f.fract() != 0.0 {
                        warn_load_decimal_scale();
                    }
                    return checked_signed_load_value(f as i64, *min, *max, column);
                }
            }
            let value = trimmed.parse::<i64>().map_err(|_| {
                error(
                    crate::error::catalog::ER_TRUNCATED_WRONG_VALUE_FOR_FIELD,
                    format!("incorrect integer value: '{}'", trimmed),
                )
            })?;
            checked_signed_load_value(value, *min, *max, column)
        }
        CoercionOp::UnsignedInt { max, column } => {
            if let Some(v) = parse_u64_fast(field_bytes) {
                return checked_unsigned_load_value(v, *max, column);
            }
            let s = load_utf8(field_bytes)?;
            let trimmed = s.trim();
            if trimmed.is_empty() {
                warn_load_empty_number();
                return checked_unsigned_load_value(0, *max, column);
            }
            let value = trimmed.parse::<u64>().map_err(|_| {
                error(
                    crate::error::catalog::ER_TRUNCATED_WRONG_VALUE_FOR_FIELD,
                    format!("incorrect integer value: '{}'", trimmed),
                )
            })?;
            checked_unsigned_load_value(value, *max, column)
        }
        CoercionOp::FloatF32 => {
            let s = load_utf8(field_bytes)?;
            let trimmed = s.trim();
            if trimmed.is_empty() {
                warn_load_empty_number();
                return Ok(Value::Float(0.0));
            }
            trimmed.parse::<f32>().map(Value::Float).map_err(|_| {
                error(
                    crate::error::catalog::ER_TRUNCATED_WRONG_VALUE_FOR_FIELD,
                    format!("incorrect float value: '{}'", trimmed),
                )
            })
        }
        CoercionOp::FloatF64 => {
            let s = load_utf8(field_bytes)?;
            let trimmed = s.trim();
            if trimmed.is_empty() {
                warn_load_empty_number();
                return Ok(Value::Double(0.0));
            }
            trimmed.parse::<f64>().map(Value::Double).map_err(|_| {
                error(
                    crate::error::catalog::ER_TRUNCATED_WRONG_VALUE_FOR_FIELD,
                    format!("incorrect double value: '{}'", trimmed),
                )
            })
        }
        CoercionOp::Decimal {
            precision,
            scale,
            unsigned,
            column,
        } => {
            if precision.is_some_and(|precision| precision <= 18) {
                checked_decimal_load_value(
                    parse_decimal_compact_value(field_bytes, *scale)?,
                    *precision,
                    scale.unwrap_or(0),
                    *unsigned,
                    column,
                )
            } else {
                let s = load_utf8(field_bytes)?;
                Ok(Value::Decimal(
                    round_load_decimal_text(s.trim(), scale.unwrap_or(0))
                        .unwrap_or_else(|| s.trim().to_string()),
                ))
            }
        }
        CoercionOp::CharPad {
            len,
            column,
            charset,
        } => {
            let s = load_utf8(field_bytes)?;
            let compact = crate::util::pad_space::pad_space_trim_end_str(&s);
            if compact.chars().count() > *len {
                return Err(error(
                    crate::error::catalog::ER_DATA_TOO_LONG,
                    format!("data too long for column '{column}'"),
                ));
            }
            let value = str_to_string_value(compact);
            validate_load_text_charset(&value, charset.as_deref())?;
            Ok(value)
        }
        CoercionOp::VarCharLimit {
            len,
            column,
            charset,
        } => {
            let s = load_utf8(field_bytes)?;
            let split = s.char_indices().nth(*len).map(|(offset, _)| offset);
            if let Some(split) = split {
                let (kept, removed) = s.split_at(split);
                if !removed.chars().all(|ch| ch == ' ') {
                    return Err(error(
                        crate::error::catalog::ER_DATA_TOO_LONG,
                        format!("data too long for column '{column}'"),
                    ));
                }
                warn_load_decimal_scale();
                let value = str_to_string_value(kept);
                validate_load_text_charset(&value, charset.as_deref())?;
                Ok(value)
            } else {
                let value = str_to_string_value(&s);
                validate_load_text_charset(&value, charset.as_deref())?;
                Ok(value)
            }
        }
        CoercionOp::BinaryPad(len) => {
            let mut bytes = field_bytes.to_vec();
            if bytes.len() < *len {
                bytes.resize(*len, 0);
            } else if bytes.len() > *len {
                crate::engine::api::push_statement_warning(
                    crate::error::ConditionLevel::Warning,
                    crate::error::catalog::ER_DATA_TOO_LONG,
                    "Binary field truncated in LOAD DATA",
                );
                bytes.truncate(*len);
            }
            Ok(Value::Bytes(bytes))
        }
        CoercionOp::VarBinaryLimit(len) => {
            let mut bytes = field_bytes.to_vec();
            if bytes.len() > *len {
                crate::engine::api::push_statement_warning(
                    crate::error::ConditionLevel::Warning,
                    crate::error::catalog::ER_DATA_TOO_LONG,
                    "Binary field truncated in LOAD DATA",
                );
                bytes.truncate(*len);
            }
            Ok(Value::Bytes(bytes))
        }
        CoercionOp::Text { .. } | CoercionOp::PassThroughString => {
            Ok(str_to_string_value(load_utf8(field_bytes)?))
        }
        CoercionOp::PassThroughBytes => Ok(Value::Bytes(field_bytes.to_vec())),
        CoercionOp::VectorBase64 {
            datatype,
            dimension,
        } => {
            use base64::Engine;
            let clean: Vec<u8> = field_bytes
                .iter()
                .copied()
                .filter(|b| !b.is_ascii_whitespace())
                .collect();
            let bytes = base64::engine::general_purpose::STANDARD
                .decode(clean)
                .map_err(|_| {
                    error(
                        crate::error::catalog::MYDB_ER_INVALID_VECTOR_VALUE,
                        "invalid base64 VECTOR value in LOAD DATA",
                    )
                })?;
            let dimension = dimension.unwrap_or((bytes.len() / 4) as u32);
            let value = crate::vector::VectorValue::new(*datatype, dimension, bytes);
            crate::engine::vector::validate_internal(&value)?;
            Ok(Value::Vector(value))
        }
        CoercionOp::Date
        | CoercionOp::Time
        | CoercionOp::DateTime
        | CoercionOp::Timestamp { .. } => Ok(str_to_string_value(load_utf8(field_bytes)?.trim())),
        CoercionOp::Year => {
            if let Some(v) = parse_i64_fast(field_bytes) {
                return Ok(Value::Int(v));
            }
            let s = load_utf8(field_bytes)?;
            Ok(str_to_string_value(s.trim()))
        }
        CoercionOp::Enum | CoercionOp::Set => Ok(str_to_string_value(load_utf8(field_bytes)?)),
        CoercionOp::Bit => {
            // BIT is unsigned up to 64 bits, so parse the field as u64 (a
            // valid BIT(64) value can exceed i64::MAX). On a non-numeric
            // field, pass the string through to `coerce_row` — like the
            // integer path above — which applies sql_mode-aware coercion
            // (warning/1366 or error), instead of silently storing 0.
            let s = load_utf8(field_bytes)?;
            let trimmed = s.trim();
            if let Ok(v) = trimmed.parse::<u64>() {
                Ok(Value::UInt(v))
            } else {
                Ok(str_to_string_value(trimmed))
            }
        }
    }
}

fn bytes_to_string_value(bytes: &[u8]) -> Value {
    if bytes.len() <= crate::types::SMALL_STRING_INLINE_CAP && std::str::from_utf8(bytes).is_ok() {
        Value::SmallString(crate::types::SmallStringBuf::from_slice(bytes))
    } else {
        Value::String(lossy_utf8(bytes).to_string())
    }
}

fn ascii_bytes_to_string_value(bytes: &[u8]) -> Value {
    debug_assert!(bytes.is_ascii());
    if bytes.len() <= crate::types::SMALL_STRING_INLINE_CAP {
        Value::SmallString(crate::types::SmallStringBuf::from_slice(bytes))
    } else {
        Value::String(unsafe { String::from_utf8_unchecked(bytes.to_vec()) })
    }
}

fn str_to_string_value(s: &str) -> Value {
    if s.len() <= crate::types::SMALL_STRING_INLINE_CAP {
        Value::SmallString(crate::types::SmallStringBuf::from_slice(s.as_bytes()))
    } else {
        Value::String(s.to_string())
    }
}

fn load_utf8(bytes: &[u8]) -> Result<&str, QueryError> {
    std::str::from_utf8(bytes).map_err(|_| {
        error(
            crate::error::catalog::ER_INVALID_CHARACTER_STRING,
            "Invalid utf8mb4 character string in LOAD DATA",
        )
    })
}

fn lossy_utf8(bytes: &[u8]) -> std::borrow::Cow<'_, str> {
    String::from_utf8_lossy(bytes)
}

fn io_err(e: std::io::Error) -> QueryError {
    error(
        crate::error::catalog::ER_TEXTFILE_NOT_READABLE,
        format!("I/O error reading file: {}", e),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::{DurabilityMode, StartupValidationMode, StorageConfig};
    use std::time::Duration;
    use tempfile::TempDir;

    fn signed_int_op() -> CoercionOp {
        CoercionOp::SignedInt {
            min: i64::MIN,
            max: i64::MAX,
            column: "test_col".to_string(),
        }
    }

    fn setup_engine() -> (StorageQueryEngine, TempDir) {
        let tmp = TempDir::new().unwrap();
        let config = StorageConfig {
            durability: DurabilityMode::Memory,
            checkpoint_interval: Duration::from_millis(60_000),
            checkpoint_wal_bytes: 64 * 1024 * 1024,
            lock_wait: Duration::from_millis(5_000),
            group_commit_window: Duration::from_micros(0),
            wal_segment_bytes: 256 * 1024 * 1024,
            // LOAD DATA fixtures are tiny; a 1 GiB pool would violate the
            // default controlled-memory split once executor memory is added.
            buffer_pool_pages: 2_048,
            buffer_pool_io_threads: 1,
            buffer_pool_prefetch_pages: 4,
            undo_retention: Duration::from_millis(60_000),
            heap_update_reserve_bytes: 2048,
            startup_validation: StartupValidationMode::Auto,
            startup_validate_indexes: true,
        };
        let engine = StorageQueryEngine::open_with_storage_config(tmp.path(), config).unwrap();
        crate::engine::sysvar::builtin::query::allow_test_server_files();
        (engine, tmp)
    }

    fn audit_parser() -> FieldParser {
        FieldParser {
            input_charset: LoadLegacyCharset::None,
            field_terminator: b",".to_vec(),
            line_terminator: b"\n".to_vec(),
            enclosed_by: Some(b'"'),
            escaped_by: Some(b'\\'),
            line_starting_by: Vec::new(),
        }
    }

    #[test]
    fn audit_records_survive_every_buffer_boundary() {
        let parser = audit_parser();
        let input = b"abc,def\n\"a\nb\",2\n\"a\"\"b\",3\n\"a\\\"b\",4\n\nlast,";
        for capacity in 1..20 {
            let mut reader = BufReader::with_capacity(capacity, &input[..]);
            let mut record = Vec::new();
            let mut rows = Vec::new();
            while read_load_record(&mut reader, &parser, &mut record, 2).unwrap() {
                rows.push(parser.parse_row_raw(&record));
            }
            let expected: Vec<Vec<Option<Vec<u8>>>> = [
                vec![Some(b"abc".to_vec()), Some(b"def".to_vec())],
                vec![Some(b"a\nb".to_vec()), Some(b"2".to_vec())],
                vec![Some(b"a\"b".to_vec()), Some(b"3".to_vec())],
                vec![Some(b"a\"b".to_vec()), Some(b"4".to_vec())],
                vec![Some(vec![])],
                vec![Some(b"last".to_vec()), Some(vec![])],
            ]
            .into();
            assert_eq!(rows, expected, "buffer capacity {capacity}");
        }
    }

    #[test]
    fn audit_multibyte_record_and_empty_field_terminators() {
        let mut parser = audit_parser();
        parser.line_terminator = b"\r\n".to_vec();
        parser.field_terminator = b"||".to_vec();
        let mut reader = BufReader::with_capacity(1, &b"\"x\r\ny\"||x\r\r\n"[..]);
        let mut record = Vec::new();
        assert!(read_load_record(&mut reader, &parser, &mut record, 2).unwrap());
        assert_eq!(
            parser.parse_row_raw(&record),
            vec![Some(b"x\r\ny".to_vec()), Some(b"x\r".to_vec())]
        );
        assert_eq!(
            parser.parse_row_raw(br"\\N||\N"),
            vec![Some(br"\N".to_vec()), None]
        );
        assert_eq!(
            parser.parse_row_raw(br#""\N"||"\\N""#),
            vec![None, Some(br"\N".to_vec())]
        );
        parser.field_terminator.clear();
        assert_eq!(parser.parse_row_raw(b"123"), vec![Some(b"123".to_vec())]);
        assert_eq!(parser.parse_row_raw(b"NULL"), vec![None]);
        assert_eq!(
            parser.parse_row_raw(b"\"NULL\""),
            vec![Some(b"NULL".to_vec())]
        );
    }

    #[test]
    fn audit_decimal_rounding_and_utf8() {
        for (input, scale, magnitude) in [
            ("1.235", 2, 124),
            ("-1.235", 2, -124),
            ("9.995", 2, 1000),
            (".5", 0, 1),
            ("-.5", 0, -1),
            ("1.234", 2, 123),
        ] {
            assert_eq!(
                parse_decimal_compact_value(input.as_bytes(), Some(scale)).unwrap(),
                Value::DecimalCompact(magnitude, scale)
            );
            assert_eq!(
                parse_decimal_compact_value_wide(input.as_bytes(), Some(scale)).unwrap(),
                Value::DecimalCompact(magnitude, scale)
            );
        }
        assert!(coerce_field(b"a\xffb", &CoercionOp::PassThroughString)
            .unwrap_err()
            .is(crate::error::catalog::ER_INVALID_CHARACTER_STRING));
        assert_eq!(
            coerce_field(b"a\xffb", &CoercionOp::PassThroughBytes).unwrap(),
            Value::Bytes(b"a\xffb".to_vec())
        );
    }

    #[test]
    fn audit_missing_empty_extra_fields_and_parallel_warnings() {
        let (engine, tmp) = setup_engine();
        engine
            .execute("CREATE TABLE audit_fields (a INT, b VARCHAR(8))")
            .unwrap();
        let path = tmp.path().join("fields.csv");
        std::fs::write(&path, b"1\n2,\n\n3,x,extra\n").unwrap();
        engine
            .execute(&format!(
                "LOAD DATA INFILE '{}' INTO TABLE audit_fields FIELDS TERMINATED BY ','",
                crate::util::file::sql_path(&path)
            ))
            .unwrap();
        let QueryResponse::ResultSet(rs) = engine
            .execute("SELECT a,b FROM audit_fields ORDER BY a")
            .unwrap()
        else {
            panic!()
        };
        assert_eq!(
            rs.rows,
            vec![
                vec![Value::Int(0), Value::Null],
                vec![Value::Int(1), Value::Null],
                vec![Value::Int(2), Value::String(String::new())],
                vec![Value::Int(3), Value::String("x".into())]
            ]
        );
    }

    #[test]
    fn parallel_load_preserves_every_extra_field_warning() {
        let (engine, tmp) = setup_engine();
        engine
            .execute("CREATE TABLE warning_rows (id INT PRIMARY KEY)")
            .unwrap();
        let path = tmp.path().join("warnings.tbl");
        let input: String = (0..5003).map(|id| format!("{id}|\n")).collect();
        std::fs::write(&path, input).unwrap();
        let mut session = SessionContext::default();
        let response = engine.execute_with_session(&mut session, &format!(
            "LOAD DATA /*+ PARALLEL(8) */ INFILE '{}' INTO TABLE warning_rows FIELDS TERMINATED BY '|'",
            crate::util::file::sql_path(&path)
        )).unwrap();
        assert!(matches!(
            response,
            QueryResponse::OkAffected {
                affected_rows: 5003
            }
        ));
        assert_eq!(session.warning_count(), 5003);
        let QueryResponse::ResultSet(warnings) = engine
            .execute_with_session(&mut session, "SHOW WARNINGS")
            .unwrap()
        else {
            panic!("expected warnings");
        };
        assert_eq!(warnings.rows.len(), 5003);
        for row in warnings.rows {
            assert_eq!(row[1].to_display_string(), "1262");
            assert_eq!(
                row[2].to_display_string(),
                "Row contained more data than input columns"
            );
        }
    }

    #[test]
    fn audit_late_failure_rolls_back_every_chunk_and_trigger_effect() {
        struct BatchRowsReset(usize);
        impl Drop for BatchRowsReset {
            fn drop(&mut self) {
                TEST_BULK_CHUNK_ROWS.set(self.0);
            }
        }
        const BATCH_ROWS: usize = 32;
        let _reset = BatchRowsReset(TEST_BULK_CHUNK_ROWS.replace(BATCH_ROWS));
        let (engine, tmp) = setup_engine();
        engine
            // Invalid JSON remains a hard error even under IGNORE. Explicit
            // NULL under IGNORE is covered by the warning-and-skip regression.
            .execute("CREATE TABLE audit_atomic (a INT PRIMARY KEY, b JSON NOT NULL)")
            .unwrap();
        engine.execute("CREATE TABLE audit_log (a INT)").unwrap();
        let path = tmp.path().join("atomic.csv");
        // Two completed batches followed by a partial batch that fails.
        let rows = 2 * BATCH_ROWS + 1;
        let mut data = (1..=rows).map(|i| format!("{i},1\n")).collect::<String>();
        data.push_str(&format!("{},not-json\n", rows + 1));
        std::fs::write(&path, data).unwrap();
        for (flags, suffix, trigger) in [
            ("", " SET b=b", false),
            ("IGNORE", "", false),
            ("", "", true),
        ] {
            if trigger {
                engine.execute("CREATE TRIGGER audit_tr AFTER INSERT ON audit_atomic FOR EACH ROW INSERT INTO audit_log VALUES (NEW.a)").unwrap();
            }
            let before = engine.database().dml_stats_snapshot().insert_rows;
            let err = engine.execute(&format!("LOAD DATA INFILE '{}' {flags} INTO TABLE audit_atomic FIELDS TERMINATED BY ','{suffix}", crate::util::file::sql_path(&path))).unwrap_err();
            assert!(
                err.is(crate::error::catalog::ER_INVALID_JSON_TEXT),
                "{err:?}"
            );
            let inserted = engine.database().dml_stats_snapshot().insert_rows - before;
            let completed_rows = (2 * BATCH_ROWS * if trigger { 2 } else { 1 }) as u64;
            assert!(
                inserted >= completed_rows,
                "failure must follow two successful batch flushes, including trigger writes: \
                 {flags}/{suffix}/{trigger}: inserted {inserted}, expected at least {completed_rows}"
            );
            for table in ["audit_atomic", "audit_log"] {
                let QueryResponse::ResultSet(rs) = engine
                    .execute(&format!("SELECT COUNT(*) FROM {table}"))
                    .unwrap()
                else {
                    panic!()
                };
                assert_eq!(
                    rs.rows[0][0],
                    Value::Int(0),
                    "{flags}/{suffix}/{trigger}/{table}"
                );
            }
        }
    }

    #[test]
    fn general_load_reads_bounded_records_across_buffer_boundaries() {
        let data = b"prefix:a\t\\N\r\n".repeat(20_000);
        let mut reader = BufReader::with_capacity(7, std::io::Cursor::new(&data));
        let mut restored = Vec::new();
        let mut chunks = 0;
        loop {
            let (chunk, charge, eof) = read_general_load_chunk(&mut reader, b"\r\n").unwrap();
            assert!(charge.bytes() >= chunk.capacity());
            assert!(chunk.len() < 64 * 1024 + 32);
            restored.extend_from_slice(&chunk);
            chunks += 1;
            if eof {
                break;
            }
        }
        assert!(chunks > 1);
        assert_eq!(restored, data);
    }

    #[test]
    fn general_load_oversized_record_obeys_memory_target() {
        let before = mem_budget::controlled_memory_snapshot().0;
        // Occupy the budget without allocating backing memory. This isolates
        // admission failure without changing process-wide target settings.
        let held = mem_budget::ControlledMemoryCharge::try_new(
            mem_budget::ControlledMemoryCategory::StorageScratch,
            mem_budget::operational_memory_limit_bytes() - before - 1024 * 1024,
        )
        .unwrap();
        let charged = mem_budget::controlled_memory_snapshot().0;
        let mut reader =
            BufReader::with_capacity(4096, std::io::repeat(b'x').take(4 * 1024 * 1024));
        let err = read_general_load_chunk(&mut reader, b"\n").unwrap_err();
        assert_eq!(err.code(), 1037);
        assert_eq!(mem_budget::controlled_memory_snapshot().0, charged);
        drop(held);
        assert_eq!(mem_budget::controlled_memory_snapshot().0, before);
    }

    #[test]
    fn general_load_non_strict_raw_tsv_spans_chunks() {
        let (engine, tmp) = setup_engine();
        let mut session = SessionContext::default();
        engine
            .execute_with_session(&mut session, "SET sql_mode=''")
            .unwrap();
        engine
            .execute("CREATE TABLE general_raw (id INT PRIMARY KEY, body TEXT NOT NULL)")
            .unwrap();
        let path = tmp.path().join("general.tsv");
        let rows = 12_000;
        std::fs::write(
            &path,
            (0..rows)
                .map(|id| format!("{id}\t\\N\n"))
                .collect::<String>(),
        )
        .unwrap();
        assert_eq!(
            engine
                .execute_with_session(
                    &mut session,
                    &format!(
                        "LOAD DATA INFILE '{}' INTO TABLE general_raw FIELDS ESCAPED BY ''",
                        crate::util::file::sql_path(&path)
                    )
                )
                .unwrap(),
            QueryResponse::OkAffected {
                affected_rows: rows
            }
        );
        let QueryResponse::ResultSet(result) =
            engine.execute("SELECT COUNT(*) FROM general_raw").unwrap()
        else {
            panic!("expected count")
        };
        assert_eq!(result.rows[0][0], Value::Int(rows as i64));
        let QueryResponse::ResultSet(result) = engine
            .execute("SELECT body FROM general_raw WHERE id=11999")
            .unwrap()
        else {
            panic!("expected row")
        };
        assert_eq!(result.rows[0][0].to_display_string(), "\\N");
    }

    #[test]
    fn raw_tsv_parallel_and_serial_preserve_backslashes_and_temporal_indexes() {
        let (engine, tmp) = setup_strict_load_engine();
        let rows = PARALLEL_MIN_ROWS + 257;
        let data_path = tmp.path().join("raw.tsv");
        let data: String = (1..=rows)
            .map(|id| format!("{id}\t2026-01-02\t2026-01-02 03:04:05\t\\N\tpayload\\t\\\\tail\n"))
            .collect();
        std::fs::write(&data_path, data).unwrap();
        for degree in [1, 4] {
            engine
                .execute(&format!(
                    "CREATE TABLE raw{degree} (id BIGINT NOT NULL, d DATE NOT NULL, \
                 ts TIMESTAMP NOT NULL, body TEXT NOT NULL, tag VARCHAR(64) NOT NULL, \
                 PRIMARY KEY(id,d,ts))"
                ))
                .unwrap();
            let before = engine.db.load_shadow_perf_snapshot();
            assert_eq!(engine.execute(&format!(
                "LOAD DATA /*+ PARALLEL({degree}) */ INFILE '{}' INTO TABLE raw{degree} FIELDS ESCAPED BY ''",
                crate::util::file::sql_path(&data_path)
            )).unwrap(), QueryResponse::OkAffected { affected_rows: rows as u64 });
            if degree == 4 {
                let after = engine.db.load_shadow_perf_snapshot();
                assert_eq!(after[10] - before[10], rows as u64);
            }
            let QueryResponse::ResultSet(result) = engine.execute(&format!(
                "SELECT body,tag FROM raw{degree} WHERE id=42 AND d='2026-01-02' AND ts='2026-01-02 03:04:05'"
            )).unwrap() else { panic!("expected row") };
            assert_eq!(result.rows.len(), 1);
            assert_eq!(result.rows[0][0].to_display_string(), "\\N");
            assert_eq!(result.rows[0][1].to_display_string(), "payload\\t\\\\tail");
        }
    }

    #[test]
    fn parallel_chunk_budget_is_divided_across_pipeline_slots() {
        let limit = 64 * 1024 * 1024;
        let generic = [PreparedLoadIndex::Fallback { index_idx: 0 }];
        let per_row = 256 + std::mem::size_of::<IndexKeysForRow>();
        assert_eq!(
            parallel_load_chunk_size_for_limit(&generic, 4, limit),
            (limit / 10 / per_row).clamp(PARALLEL_MIN_ROWS, PARALLEL_CHUNK_SIZE)
        );
        assert!(
            parallel_load_chunk_size_for_limit(&generic, 4, limit)
                < parallel_load_chunk_size_for_limit(&generic, 1, limit)
        );
    }

    #[test]
    fn parallel_chunk_rows_adapt_to_observed_wide_input_capacity() {
        assert_eq!(
            parallel_load_observed_chunk_rows(500_000, 500_000, 0, 4),
            500_000
        );
        assert_eq!(
            parallel_load_observed_chunk_rows(500_000, 500_000, 500_000 * 1024, 4),
            PARALLEL_CHUNK_TARGET_BYTES / 10 / 1024
        );
        assert_eq!(
            parallel_load_observed_chunk_rows(10_000, 10_000, 10_000 * 32, 4),
            10_000
        );
    }

    #[test]
    fn batch_admission_reclaims_clean_cache_before_returning_1037() {
        // Values captured from the four-way dbbench TPC-C failure: the
        // mandatory ~17.4 MiB stock/customer batch arrived with the process
        // only ~6.2 MiB below its operational limit, while ~1.1 GiB of clean
        // buffer pages was immediately evictable.
        let controlled = 1_671_227_866;
        let requested = 18_241_900;
        let operational = 1_677_721_600;
        let buffer_raw = 1_162_592_256;
        let headroom = 64 * 1024 * 1024;
        let minimum_buffer = 16 * 1024 * 1024;
        let target = load_data_batch_reclaim_target_for_values(
            controlled,
            buffer_raw,
            requested,
            headroom,
            minimum_buffer,
            operational,
        )
        .expect("clean buffer cache can satisfy the mandatory batch");
        assert!(target < controlled);
        assert_eq!(
            target.saturating_add(requested).saturating_add(headroom),
            operational
        );
    }

    #[test]
    fn batch_admission_does_not_claim_non_evictable_memory() {
        assert_eq!(
            load_data_batch_reclaim_target_for_values(
                95 * 1024 * 1024,
                16 * 1024 * 1024,
                8 * 1024 * 1024,
                4 * 1024 * 1024,
                16 * 1024 * 1024,
                100 * 1024 * 1024,
            ),
            None
        );
    }

    #[test]
    fn test_parse_i64_fast() {
        assert_eq!(parse_i64_fast(b"0"), Some(0));
        assert_eq!(parse_i64_fast(b"123"), Some(123));
        assert_eq!(parse_i64_fast(b"-456"), Some(-456));
        assert_eq!(parse_i64_fast(b"+789"), Some(789));
        assert_eq!(parse_i64_fast(b""), None);
        assert_eq!(parse_i64_fast(b"abc"), None);
        assert_eq!(parse_i64_fast(b"12.3"), None);
        assert_eq!(parse_i64_fast(b"9223372036854775807"), Some(i64::MAX));
    }

    #[test]
    fn compact_decimal_native_parser_matches_wide_oracle() {
        for (input, scale) in [
            (b"0".as_slice(), 0),
            (b"0.00".as_slice(), 2),
            (b"-12.30".as_slice(), 2),
            (b"+9999999999999999.99".as_slice(), 2),
            (b" 42.5 ".as_slice(), 3),
            (b"1.2345".as_slice(), 2),
        ] {
            assert_eq!(
                parse_decimal_compact_value(input, Some(scale)).unwrap(),
                parse_decimal_compact_value_wide(input, Some(scale)).unwrap(),
                "input={:?} scale={scale}",
                String::from_utf8_lossy(input),
            );
        }
    }

    #[test]
    fn compact_decimal_precision_check_uses_scaled_magnitude_boundary() {
        assert!(checked_decimal_load_value(
            Value::DecimalCompact(99_999, 2),
            Some(5),
            2,
            false,
            "d",
        )
        .is_ok());
        assert!(checked_decimal_load_value(
            Value::DecimalCompact(100_000, 2),
            Some(5),
            2,
            false,
            "d",
        )
        .is_err());
        assert!(
            checked_decimal_load_value(Value::DecimalCompact(-1, 2), Some(5), 2, true, "d",)
                .is_err()
        );
    }

    #[test]
    fn test_parse_u64_fast() {
        assert_eq!(parse_u64_fast(b"0"), Some(0));
        assert_eq!(parse_u64_fast(b"18446744073709551615"), Some(u64::MAX));
        assert_eq!(parse_u64_fast(b"abc"), None);
    }

    #[test]
    fn test_field_parser_tab_delimited() {
        let parser = FieldParser {
            input_charset: LoadLegacyCharset::None,
            field_terminator: b"\t".to_vec(),
            line_terminator: b"\n".to_vec(),
            enclosed_by: None,
            escaped_by: Some(b'\\'),
            line_starting_by: Vec::new(),
        };
        let plan = vec![
            signed_int_op(),
            signed_int_op(),
            CoercionOp::PassThroughString,
        ];
        let line = b"42\t100\thello world";
        let row = parser.parse_row(line, &plan, 3).unwrap();
        assert_eq!(row[0], Value::Int(42));
        assert_eq!(row[1], Value::Int(100));
        assert_eq!(row[2], Value::String("hello world".to_string()));
    }

    #[test]
    fn test_field_parser_csv_enclosed() {
        let parser = FieldParser {
            input_charset: LoadLegacyCharset::None,
            field_terminator: b",".to_vec(),
            line_terminator: b"\n".to_vec(),
            enclosed_by: Some(b'"'),
            escaped_by: Some(b'\\'),
            line_starting_by: Vec::new(),
        };
        let plan = vec![
            signed_int_op(),
            CoercionOp::PassThroughString,
            CoercionOp::PassThroughString,
        ];
        let line = b"1,\"hello, world\",plain";
        let row = parser.parse_row(line, &plan, 3).unwrap();
        assert_eq!(row[0], Value::Int(1));
        assert_eq!(row[1], Value::String("hello, world".to_string()));
        assert_eq!(row[2], Value::String("plain".to_string()));
    }

    #[test]
    fn test_field_parser_null() {
        let parser = FieldParser {
            input_charset: LoadLegacyCharset::None,
            field_terminator: b"\t".to_vec(),
            line_terminator: b"\n".to_vec(),
            enclosed_by: None,
            escaped_by: Some(b'\\'),
            line_starting_by: Vec::new(),
        };
        let plan = vec![signed_int_op(), CoercionOp::PassThroughString];
        let line = b"\\N\t\\N";
        let row = parser.parse_row(line, &plan, 2).unwrap();
        assert_eq!(row[0], Value::Null);
        assert_eq!(row[1], Value::Null);
    }

    #[test]
    fn test_field_parser_escaped_chars() {
        let parser = FieldParser {
            input_charset: LoadLegacyCharset::None,
            field_terminator: b"\t".to_vec(),
            line_terminator: b"\n".to_vec(),
            enclosed_by: None,
            escaped_by: Some(b'\\'),
            line_starting_by: Vec::new(),
        };
        let plan = vec![CoercionOp::PassThroughString];
        let line = b"hello\\tworld\\n";
        let row = parser.parse_row(line, &plan, 1).unwrap();
        assert_eq!(row[0], Value::String("hello\tworld\n".to_string()));
    }

    #[test]
    fn test_fast_single_byte_pipe_matches_generic_parser() {
        let parser = FieldParser {
            input_charset: LoadLegacyCharset::None,
            field_terminator: b"|".to_vec(),
            line_terminator: b"\n".to_vec(),
            enclosed_by: None,
            escaped_by: Some(b'\\'),
            line_starting_by: Vec::new(),
        };
        let format = fast_parse_format(&parser).expect("pipe should be fast-path eligible");
        let plan = vec![
            signed_int_op(),
            CoercionOp::PassThroughString,
            CoercionOp::PassThroughString,
            CoercionOp::PassThroughString,
            signed_int_op(),
        ];

        for line in [
            &b"42|hello|plain||7"[..],
            &b"42|hello\\|world|\\N|tail|7"[..],
            &b"42|two\\\\|fields|tail|7"[..],
            &b"42|three\\\\\\|one|tail|7"[..],
            &b"42|hello\\nworld|abc\\tdef|tail"[..],
            &b"42|short"[..],
            &b"42|trailing|field|7|"[..],
        ] {
            let generic = parser.parse_row(line, &plan, 5).unwrap();
            let fast = parse_row_fast_single_byte(line, format, &plan, 5).unwrap();
            assert_eq!(fast, generic, "line={}", String::from_utf8_lossy(line));
        }
    }

    #[test]
    fn test_parse_chunk_fast_pipe_crlf() {
        let data = b"1|Alice\r\n2|Bob\r\n";
        let mut reader = BufReader::with_capacity(
            READ_BUF_SIZE,
            Box::new(std::io::Cursor::new(data.to_vec())) as Box<dyn Read>,
        );
        let parser = FieldParser {
            input_charset: LoadLegacyCharset::None,
            field_terminator: b"|".to_vec(),
            line_terminator: b"\r\n".to_vec(),
            enclosed_by: None,
            escaped_by: Some(b'\\'),
            line_starting_by: Vec::new(),
        };
        let format = fast_parse_format(&parser).unwrap();
        let plan = vec![signed_int_op(), CoercionOp::PassThroughString];
        let (rows, eof) = parse_chunk_fast(&mut reader, format, &plan, 2, 10).unwrap();
        assert!(eof);
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0], vec![Value::Int(1), Value::String("Alice".into())]);
        assert_eq!(rows[1], vec![Value::Int(2), Value::String("Bob".into())]);
    }

    #[test]
    fn critical_audit_fast_line_terminators_are_exact_across_buffers() {
        for capacity in [1, 2, 7, 64] {
            for (terminator, input, expected) in [
                (
                    b"\n".as_slice(),
                    b"1|Alice\r\n2|Bob\r".as_slice(),
                    ["Alice\r", "Bob\r"],
                ),
                (
                    b"\r\n".as_slice(),
                    b"1|Alice\nX\r\n2|Bob\r".as_slice(),
                    ["Alice\nX", "Bob\r"],
                ),
            ] {
                let parser = FieldParser {
                    input_charset: LoadLegacyCharset::None,
                    field_terminator: b"|".to_vec(),
                    line_terminator: terminator.to_vec(),
                    enclosed_by: None,
                    escaped_by: Some(b'\\'),
                    line_starting_by: Vec::new(),
                };
                let format = fast_parse_format(&parser).unwrap();
                let mut reader = BufReader::with_capacity(
                    capacity,
                    Box::new(std::io::Cursor::new(input.to_vec())) as Box<dyn Read>,
                );
                let (rows, eof) = parse_chunk_fast(
                    &mut reader,
                    format,
                    &[signed_int_op(), CoercionOp::PassThroughString],
                    2,
                    10,
                )
                .unwrap();
                assert!(eof);
                assert_eq!(rows.len(), 2);
                for (row, text) in rows.iter().zip(expected) {
                    assert_eq!(row[1].to_display_string(), text, "capacity={capacity}");
                }
            }
        }
    }

    #[test]
    fn test_load_data_basic() {
        let (engine, tmp) = setup_engine();
        engine
            .execute("CREATE TABLE t (id INT PRIMARY KEY, name VARCHAR(100))")
            .unwrap();

        let data_path = tmp.path().join("test.tsv");
        std::fs::write(&data_path, "1\tAlice\n2\tBob\n3\tCharlie\n").unwrap();

        let sql = format!(
            "LOAD DATA INFILE '{}' INTO TABLE t",
            crate::util::file::sql_path(&data_path)
        );
        let result = engine.execute(&sql).unwrap();
        assert_eq!(result, QueryResponse::OkAffected { affected_rows: 3 });

        let result = engine.execute("SELECT COUNT(*) FROM t").unwrap();
        if let QueryResponse::ResultSet(rs) = result {
            assert_eq!(rs.rows[0][0], Value::Int(3));
        } else {
            panic!("expected result set");
        }
    }

    #[test]
    fn test_load_data_csv() {
        let (engine, tmp) = setup_engine();
        engine
            .execute("CREATE TABLE t (id INT PRIMARY KEY, val DOUBLE, name VARCHAR(50))")
            .unwrap();

        let data_path = tmp.path().join("test.csv");
        std::fs::write(&data_path, "1,3.14,\"hello, world\"\n2,2.72,\"foo\"\n").unwrap();

        let sql = format!(
            "LOAD DATA INFILE '{}' INTO TABLE t FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '\"'",
            crate::util::file::sql_path(&data_path)
        );
        let result = engine.execute(&sql).unwrap();
        assert_eq!(result, QueryResponse::OkAffected { affected_rows: 2 });
    }

    #[test]
    fn test_load_data_pipe_lineitem_shape_compact_values() {
        let (engine, tmp) = setup_engine();
        engine
            .execute(
                "CREATE TABLE lineitem (
                l_orderkey INTEGER NOT NULL,
                l_partkey INTEGER NOT NULL,
                l_suppkey INTEGER NOT NULL,
                l_linenumber INTEGER NOT NULL,
                l_quantity DECIMAL(15,2) NOT NULL,
                l_extendedprice DECIMAL(15,2) NOT NULL,
                l_discount DECIMAL(15,2) NOT NULL,
                l_tax DECIMAL(15,2) NOT NULL,
                l_returnflag CHAR(1) NOT NULL,
                l_linestatus CHAR(1) NOT NULL,
                l_shipdate DATE NOT NULL,
                l_commitdate DATE NOT NULL,
                l_receiptdate DATE NOT NULL,
                l_shipinstruct CHAR(25) NOT NULL,
                l_shipmode CHAR(10) NOT NULL,
                l_comment VARCHAR(44) NOT NULL,
                PRIMARY KEY (l_orderkey, l_linenumber)
            )",
            )
            .unwrap();

        let data_path = tmp.path().join("lineitem.tbl");
        std::fs::write(
            &data_path,
            "1|2|3|1|17.00|21168.23|0.04|0.02|N|O|1996-03-13|1996-02-12|1996-03-22|DELIVER IN PERSON|TRUCK|regular comment|\n\
             2|3|4|1|36.00|45983.16|0.09|0.06|R|F|1996-04-12|1996-03-13|1996-04-20|TAKE BACK RETURN|MAIL|second comment|\n",
        )
        .unwrap();

        let sql = format!(
            "LOAD DATA INFILE '{}' INTO TABLE lineitem FIELDS TERMINATED BY '|'",
            crate::util::file::sql_path(&data_path)
        );
        assert_eq!(
            engine.execute(&sql).unwrap(),
            QueryResponse::OkAffected { affected_rows: 2 }
        );

        let result = engine
            .execute(
                "SELECT l_quantity, l_extendedprice, l_shipdate, l_shipmode \
                 FROM lineitem WHERE l_orderkey = 1",
            )
            .unwrap();
        let QueryResponse::ResultSet(rs) = result else {
            panic!("expected result set");
        };
        assert_eq!(rs.rows[0][0].to_display_string(), "17.00");
        assert_eq!(rs.rows[0][1].to_display_string(), "21168.23");
        assert_eq!(rs.rows[0][2].to_display_string(), "1996-03-13");
        assert_eq!(rs.rows[0][3], Value::String("TRUCK".into()));
    }

    #[test]
    fn test_load_data_with_columns() {
        let (engine, tmp) = setup_engine();
        engine
            .execute("CREATE TABLE t (id INT PRIMARY KEY, a VARCHAR(20), b VARCHAR(20))")
            .unwrap();

        let data_path = tmp.path().join("test.tsv");
        std::fs::write(&data_path, "1\tX\n2\tY\n").unwrap();

        let sql = format!(
            "LOAD DATA INFILE '{}' INTO TABLE t (id, a)",
            crate::util::file::sql_path(&data_path)
        );
        let result = engine.execute(&sql).unwrap();
        assert_eq!(result, QueryResponse::OkAffected { affected_rows: 2 });
    }

    #[test]
    fn test_load_data_ignore_lines() {
        let (engine, tmp) = setup_engine();
        engine
            .execute("CREATE TABLE t (id INT PRIMARY KEY, name VARCHAR(50))")
            .unwrap();

        let data_path = tmp.path().join("test.tsv");
        std::fs::write(&data_path, "id\tname\n1\tAlice\n2\tBob\n").unwrap();

        let sql = format!(
            "LOAD DATA INFILE '{}' INTO TABLE t IGNORE 1 LINES",
            crate::util::file::sql_path(&data_path)
        );
        let result = engine.execute(&sql).unwrap();
        assert_eq!(result, QueryResponse::OkAffected { affected_rows: 2 });
    }

    #[test]
    fn test_load_data_file_not_found() {
        let (engine, _tmp) = setup_engine();
        engine
            .execute("CREATE TABLE t (id INT PRIMARY KEY)")
            .unwrap();
        let result = engine.execute("LOAD DATA INFILE '/nonexistent/file.tsv' INTO TABLE t");
        assert!(result.is_err());
    }

    #[test]
    fn test_load_data_all_types() {
        let (engine, tmp) = setup_engine();
        engine
            .execute(
                "CREATE TABLE t (
                id INT PRIMARY KEY,
                tiny TINYINT,
                big BIGINT,
                f FLOAT,
                d DOUBLE,
                c CHAR(10),
                v VARCHAR(50),
                tx TEXT,
                dt DATE,
                ts TIMESTAMP
            )",
            )
            .unwrap();

        let data_path = tmp.path().join("types.tsv");
        std::fs::write(
            &data_path,
            "1\t127\t9223372036854775807\t3.14\t2.718281828\thello     \tworld\tlong text\t2024-01-15\t2024-01-15 10:30:00\n",
        ).unwrap();

        let sql = format!(
            "LOAD DATA INFILE '{}' INTO TABLE t",
            crate::util::file::sql_path(&data_path)
        );
        let result = engine.execute(&sql).unwrap();
        assert_eq!(result, QueryResponse::OkAffected { affected_rows: 1 });

        let result = engine
            .execute("SELECT id, tiny, big, c FROM t WHERE id = 1")
            .unwrap();
        if let QueryResponse::ResultSet(rs) = result {
            assert_eq!(rs.rows[0][0], Value::Int(1));
            assert_eq!(rs.rows[0][1], Value::Int(127));
            assert_eq!(rs.rows[0][2], Value::Int(9223372036854775807));
        } else {
            panic!("expected result set");
        }
    }

    #[test]
    fn test_load_data_large_batch() {
        let (engine, tmp) = setup_engine();
        engine.execute("CREATE TABLE t (id INT PRIMARY KEY, k INT NOT NULL, c CHAR(20) NOT NULL DEFAULT '', pad CHAR(10) NOT NULL DEFAULT '')").unwrap();
        engine.execute("CREATE INDEX k_idx ON t(k)").unwrap();

        let data_path = tmp.path().join("large.tsv");
        let row_count = 10_000;
        let mut data = String::with_capacity(row_count * 50);
        for i in 0..row_count {
            data.push_str(&format!(
                "{}\t{}\t{}\t{}\n",
                i,
                i % 1000,
                "abcdefghijklmnopqrst",
                "0123456789"
            ));
        }
        std::fs::write(&data_path, &data).unwrap();

        let sql = format!(
            "LOAD DATA INFILE '{}' INTO TABLE t",
            crate::util::file::sql_path(&data_path)
        );
        let result = engine.execute(&sql).unwrap();
        assert_eq!(
            result,
            QueryResponse::OkAffected {
                affected_rows: row_count as u64
            }
        );

        let result = engine.execute("SELECT COUNT(*) FROM t").unwrap();
        if let QueryResponse::ResultSet(rs) = result {
            assert_eq!(rs.rows[0][0], Value::Int(row_count as i64));
        }
    }

    #[test]
    fn test_load_data_parallel_hint_matches_serial_with_indexes() {
        let (engine, tmp) = setup_engine();
        engine.execute("CREATE TABLE parallel_t (id INT PRIMARY KEY, k INT NOT NULL, c CHAR(20) NOT NULL DEFAULT '', pad CHAR(10) NOT NULL DEFAULT '')").unwrap();
        engine
            .execute("CREATE INDEX parallel_k_idx ON parallel_t(k)")
            .unwrap();

        let data_path = tmp.path().join("parallel.tsv");
        let row_count = 60_000;
        let mut data = String::with_capacity(row_count * 50);
        for i in 0..row_count {
            data.push_str(&format!(
                "{}\t{}\t{}\t{}\n",
                i,
                i % 997,
                "abcdefghijklmnopqrst",
                "0123456789"
            ));
        }
        std::fs::write(&data_path, &data).unwrap();

        let parallel_sql = format!(
            "LOAD DATA /*+ PARALLEL(4) */ INFILE '{}' INTO TABLE parallel_t",
            crate::util::file::sql_path(&data_path)
        );
        assert_eq!(
            engine.execute(&parallel_sql).unwrap(),
            QueryResponse::OkAffected {
                affected_rows: row_count as u64
            }
        );

        let QueryResponse::ResultSet(count) =
            engine.execute("SELECT COUNT(*) FROM parallel_t").unwrap()
        else {
            panic!("expected count result set");
        };
        assert_eq!(count.rows[0][0], Value::Int(row_count as i64));

        let QueryResponse::ResultSet(parallel) = engine
            .execute("SELECT id, k FROM parallel_t WHERE k = 42 ORDER BY id LIMIT 5")
            .unwrap()
        else {
            panic!("expected sample result set");
        };
        let expected: Vec<Vec<Value>> = (0..5)
            .map(|n| vec![Value::Int(42 + 997 * n), Value::Int(42)])
            .collect();
        assert_eq!(parallel.rows, expected);
    }

    fn strict_load_config() -> StorageConfig {
        StorageConfig {
            durability: DurabilityMode::Strict,
            checkpoint_interval: Duration::from_millis(60_000),
            checkpoint_wal_bytes: 64 * 1024 * 1024,
            lock_wait: Duration::from_millis(5_000),
            group_commit_window: Duration::from_micros(0),
            wal_segment_bytes: 256 * 1024 * 1024,
            buffer_pool_pages: 2_048,
            buffer_pool_io_threads: 2,
            buffer_pool_prefetch_pages: 4,
            undo_retention: Duration::from_millis(60_000),
            heap_update_reserve_bytes: 2_048,
            startup_validation: StartupValidationMode::Auto,
            startup_validate_indexes: true,
        }
    }

    fn setup_strict_load_engine() -> (StorageQueryEngine, TempDir) {
        let tmp = TempDir::new().unwrap();
        let config = strict_load_config();
        let engine = StorageQueryEngine::open_with_storage_config(tmp.path(), config).unwrap();
        crate::engine::sysvar::builtin::query::allow_test_server_files();
        (engine, tmp)
    }

    fn check_bulk_load_index_visibility(strict: bool) {
        let (engine, tmp) = if strict {
            setup_strict_load_engine()
        } else {
            setup_engine()
        };
        let rows = 4096;
        let path = tmp.path().join("visibility.tsv");
        let data = (1..=rows)
            .map(|id| format!("{id}\t{id}\t{}\t2024-01-{:02}\tvalue-{id:08}\traw-{id}\t{}\t{id}.25\t{id}.5\tpayload-{id}\n", id * 2, id % 28 + 1, if id % 7 == 0 { "\\N".to_owned() } else { (u64::MAX - id as u64).to_string() }))
            .collect::<String>();
        std::fs::write(&path, data).unwrap();
        for explicit in [false, true] {
            for commit in [false, true] {
                for populated in [false, true] {
                    engine.execute("DROP TABLE IF EXISTS visibility_t").unwrap();
                    engine.execute("CREATE TABLE visibility_t(id INT PRIMARY KEY,a BIGINT NOT NULL,b BIGINT NOT NULL,d DATE NOT NULL,s VARCHAR(32) COLLATE utf8mb4_bin NOT NULL,raw VARBINARY(32) NOT NULL,u BIGINT UNSIGNED NULL,n DECIMAL(20,2) NOT NULL,f DOUBLE NOT NULL,payload TEXT,KEY pair_idx(a,b),KEY date_idx(d),KEY string_idx(s),KEY binary_idx(raw),UNIQUE KEY unsigned_idx(u),KEY decimal_idx(n),KEY float_idx(f))").unwrap();
                    if populated {
                        engine.execute("INSERT INTO visibility_t VALUES(-1,-1,-1,'2020-01-01','seed','seed',1,1.25,1.5,'seed')").unwrap();
                    }
                    let initial = i64::from(populated);
                    let mut writer = SessionContext::default();
                    let mut reader = SessionContext::default();
                    let mut snapshot = SessionContext::default();
                    engine
                        .execute_with_session(
                            &mut reader,
                            "SET SESSION TRANSACTION ISOLATION LEVEL READ COMMITTED",
                        )
                        .unwrap();
                    engine
                        .execute_with_session(
                            &mut snapshot,
                            "SET SESSION TRANSACTION ISOLATION LEVEL REPEATABLE READ",
                        )
                        .unwrap();
                    engine
                        .execute_with_session(
                            &mut snapshot,
                            "START TRANSACTION WITH CONSISTENT SNAPSHOT",
                        )
                        .unwrap();
                    let count = |session: &mut SessionContext, sql: &str| {
                        let response = engine
                            .execute_with_session(session, sql)
                            .unwrap()
                            .materialize()
                            .unwrap();
                        let QueryResponse::ResultSet(rs) = response else {
                            panic!("expected rows for {sql}")
                        };
                        assert_eq!(rs.rows[0][0], Value::Int(initial), "strict={strict} explicit={explicit} commit={commit} populated={populated}: {sql}");
                    };
                    count(&mut snapshot, "SELECT COUNT(*) FROM visibility_t");
                    engine
                        .execute_with_session(
                            &mut writer,
                            if explicit {
                                "BEGIN"
                            } else {
                                "SET autocommit=0"
                            },
                        )
                        .unwrap();
                    engine
                        .execute_with_session(
                            &mut writer,
                            &format!(
                                "LOAD DATA /*+ PARALLEL(4) */ INFILE '{}' INTO TABLE visibility_t",
                                crate::util::file::sql_path(&path)
                            ),
                        )
                        .unwrap();
                    for index in [
                        "PRIMARY",
                        "pair_idx",
                        "date_idx",
                        "string_idx",
                        "binary_idx",
                        "unsigned_idx",
                        "decimal_idx",
                        "float_idx",
                    ] {
                        let sql = format!("SELECT COUNT(*) FROM visibility_t FORCE INDEX({index})");
                        count(&mut reader, &sql);
                        count(&mut snapshot, &sql);
                    }
                    for (index, columns) in [
                        ("PRIMARY", "id"),
                        ("pair_idx", "a,b"),
                        ("date_idx", "d"),
                        ("string_idx", "s"),
                        ("binary_idx", "raw"),
                        ("unsigned_idx", "u"),
                        ("decimal_idx", "n"),
                        ("float_idx", "f"),
                    ] {
                        let sql = format!(
                            "SELECT {columns} FROM visibility_t FORCE INDEX({index}) LIMIT 3"
                        );
                        let QueryResponse::ResultSet(rs) = engine
                            .execute_with_session(&mut reader, &sql)
                            .unwrap()
                            .materialize()
                            .unwrap()
                        else {
                            panic!("expected covering rows")
                        };
                        assert_eq!(rs.rows.len(), initial as usize, "{sql}");
                    }
                    let QueryResponse::ResultSet(own) = engine
                        .execute_with_session(&mut writer, "SELECT COUNT(*) FROM visibility_t")
                        .unwrap()
                        .materialize()
                        .unwrap()
                    else {
                        panic!("expected owner count")
                    };
                    assert_eq!(own.rows[0][0], Value::Int(rows + initial));
                    engine
                        .execute_with_session(
                            &mut writer,
                            if commit { "COMMIT" } else { "ROLLBACK" },
                        )
                        .unwrap();
                    count(&mut snapshot, "SELECT COUNT(*) FROM visibility_t");
                    engine
                        .execute_with_session(&mut snapshot, "COMMIT")
                        .unwrap();
                    for index in [
                        "PRIMARY",
                        "pair_idx",
                        "date_idx",
                        "string_idx",
                        "binary_idx",
                        "unsigned_idx",
                        "decimal_idx",
                        "float_idx",
                    ] {
                        let sql = format!("SELECT COUNT(*) FROM visibility_t FORCE INDEX({index})");
                        let QueryResponse::ResultSet(rs) = engine
                            .execute_with_session(&mut reader, &sql)
                            .unwrap()
                            .materialize()
                            .unwrap()
                        else {
                            panic!("expected committed count")
                        };
                        assert_eq!(
                            rs.rows[0][0],
                            Value::Int(initial + if commit { rows } else { 0 }),
                            "{sql}"
                        );
                    }
                }
            }
        }
    }

    #[test]
    fn bulk_load_index_visibility_strict() {
        check_bulk_load_index_visibility(true);
    }

    #[test]
    fn bulk_load_index_visibility_memory() {
        check_bulk_load_index_visibility(false);
    }

    fn import_local_for_test(
        engine: &StorageQueryEngine,
        table: &str,
        degree: usize,
        input: &str,
    ) -> (
        QueryResponse,
        crate::engine::api::SqlWarnings,
        Option<String>,
    ) {
        use crate::engine::api::QueryEngine;
        let mut session = SessionContext::default();
        let sql = format!("LOAD DATA /*+ PARALLEL({degree}) */ LOCAL INFILE 'client-input' INTO TABLE {table} FIELDS TERMINATED BY '|'");
        let response = engine
            .execute_load_data_statement_from_reader(
                &mut session,
                crate::parser::parse_statement(&sql).unwrap(),
                Box::new(std::io::Cursor::new(input.as_bytes().to_vec())),
            )
            .unwrap();
        (response, session.warnings, session.pending_statement_info)
    }

    #[test]
    #[cfg(unix)]
    fn local_parallel_reuses_shadow_for_valid_mixed_types_and_recovers() {
        let (engine, tmp) = setup_strict_load_engine();
        let input = (0..5000)
            .map(|id| {
                format!("{id}|key-{id}|12.3456|17.25|2024-05-06|汉字-{id}|ab01|{{\"v\":{id}}}\n")
            })
            .collect::<String>();
        let mut outputs = Vec::new();
        for degree in [1, 4] {
            let table = format!("mixed_{degree}");
            engine.execute(&format!("CREATE TABLE {table} (id BIGINT PRIMARY KEY, code VARCHAR(64) UNIQUE, n DECIMAL(18,4), score DOUBLE, happened DATE, payload VARCHAR(80), raw VARBINARY(20), doc JSON)")).unwrap();
            let before = engine.db.load_shadow_perf_snapshot()[0];
            let (response, warnings, info) = import_local_for_test(&engine, &table, degree, &input);
            assert_eq!(
                response,
                QueryResponse::OkAffected {
                    affected_rows: 5000
                }
            );
            assert!(warnings.is_empty());
            assert_eq!(
                engine.db.load_shadow_perf_snapshot()[0] - before,
                u64::from(degree > 1)
            );
            let QueryResponse::ResultSet(rows) = engine
                .execute(&format!("SELECT * FROM {table} ORDER BY id"))
                .unwrap()
            else {
                panic!("expected rows")
            };
            outputs.push((rows.rows, info));
        }
        assert_eq!(outputs[0], outputs[1]);
        drop(engine);
        let engine =
            StorageQueryEngine::open_with_storage_config(tmp.path(), strict_load_config()).unwrap();
        let QueryResponse::ResultSet(rows) = engine
            .execute("SELECT id FROM mixed_4 WHERE code='key-4321'")
            .unwrap()
        else {
            panic!("expected rows")
        };
        assert_eq!(rows.rows, vec![vec![Value::Int(4321)]]);
    }

    #[test]
    fn local_parallel_duplicate_replay_keeps_first_accepted_row_across_unique_keys() {
        let (engine, _tmp) = setup_strict_load_engine();
        let mut input = (1..=5000)
            .map(|id| format!("{id}|a{id}|b{id}\n"))
            .collect::<String>();
        input.push_str("6000|dup|one\n6001|dup|two\n6002|new|two\n6003|a1|free\n6004|free|b1\n6005|free|other\n1|x|x\n");
        let mut outputs = Vec::new();
        for degree in [1, 4] {
            let table = "dups";
            engine.execute(&format!("CREATE TABLE {table} (id INT PRIMARY KEY, a VARCHAR(40) UNIQUE, b VARCHAR(40) UNIQUE)")).unwrap();
            let before = engine.db.load_shadow_perf_snapshot()[0];
            let diagnostics = import_local_for_test(&engine, &table, degree, &input);
            assert_eq!(
                diagnostics.0,
                QueryResponse::OkAffected {
                    affected_rows: 5003
                }
            );
            assert_eq!(engine.db.load_shadow_perf_snapshot()[0], before);
            let QueryResponse::ResultSet(rows) = engine
                .execute(&format!("SELECT * FROM {table} ORDER BY id"))
                .unwrap()
            else {
                panic!("expected rows")
            };
            outputs.push((rows.rows, diagnostics));
            engine.execute("DROP TABLE dups").unwrap();
        }
        assert_eq!(outputs[0], outputs[1]);
    }

    #[test]
    fn local_parallel_replays_warning_only_and_conversion_errors_without_duplicate_diagnostics() {
        let (engine, _tmp) = setup_strict_load_engine();
        for (case, tail) in [
            ("rounding", "6000|7|12.345|ok\n"),
            ("range", "6000|999|12.34|ok\n6001||12.34|ok\n"),
            ("width", "6000|7|12.34|too long a value\n"),
            ("fields", "6000|7|12.34\n6001|7|12.34|ok|extra\n"),
            ("null", "6000|\\N|12.34|ok\n"),
        ] {
            let mut input = (1..=5000)
                .map(|id| format!("{id}|7|12.34|中文\n"))
                .collect::<String>();
            input.push_str(tail);
            let mut outputs = Vec::new();
            for degree in [1, 4] {
                let table = format!("warn_{case}_{degree}");
                engine.execute(&format!("CREATE TABLE {table} (id INT PRIMARY KEY, n TINYINT NOT NULL, amount DECIMAL(8,2), note VARCHAR(8))")).unwrap();
                let before = engine.db.load_shadow_perf_snapshot()[0];
                let diagnostics = import_local_for_test(&engine, &table, degree, &input);
                assert!(!diagnostics.1.is_empty(), "case={case}");
                assert_eq!(
                    engine.db.load_shadow_perf_snapshot()[0],
                    before,
                    "case={case}"
                );
                let QueryResponse::ResultSet(rows) = engine
                    .execute(&format!("SELECT * FROM {table} ORDER BY id"))
                    .unwrap()
                else {
                    panic!("expected rows")
                };
                outputs.push((rows.rows, diagnostics));
            }
            assert_eq!(outputs[0], outputs[1], "case={case}");
        }
    }

    #[test]
    fn local_parallel_quota_fallback_preserves_every_byte_and_releases_prefix() {
        let (engine, _tmp) = setup_strict_load_engine();
        struct Chunks(std::io::Cursor<Vec<u8>>);
        impl Read for Chunks {
            fn read(&mut self, out: &mut [u8]) -> std::io::Result<usize> {
                let len = out.len().min(32768);
                self.0.read(&mut out[..len])
            }
        }
        let input = (0..(3 * 1024 * 1024 + 117))
            .map(|n| (n % 251) as u8)
            .collect::<Vec<_>>();
        let prior_limit = super::super::spill_quota::global_limit_bytes();
        let prior_query = super::super::spill_quota::per_query_limit_bytes();
        struct Restore(u64, u64);
        impl Drop for Restore {
            fn drop(&mut self) {
                super::super::spill_quota::set_global_limit_bytes(self.0);
                super::super::spill_quota::set_per_query_limit_bytes(self.1);
            }
        }
        let _restore = Restore(prior_limit, prior_query);
        assert!(mem_budget::current_budget().is_none());
        for (global, per_query) in [(1024 * 1024 + 100000, 0), (0, 1024 * 1024 + 100000)] {
            super::super::spill_quota::set_global_limit_bytes(global);
            super::super::spill_quota::set_per_query_limit_bytes(per_query);
            let before = super::super::spill_quota::global_spilled_bytes();
            let (mut reader, replay) = stage_ignore_load_input(
                &engine.db,
                Box::new(Chunks(std::io::Cursor::new(input.clone()))),
                0,
            )
            .unwrap();
            assert!(replay.is_none());
            assert!(super::super::spill_quota::global_spilled_bytes() > before);
            let mut actual = Vec::new();
            reader.read_to_end(&mut actual).unwrap();
            assert_eq!(actual, input);
            assert_eq!(super::super::spill_quota::global_spilled_bytes(), before);
        }
    }

    #[test]
    #[cfg(unix)]
    fn local_parallel_header_is_not_skipped_again_after_replay() {
        use crate::engine::api::QueryEngine;
        let (engine, _tmp) = setup_strict_load_engine();
        for duplicate in [false, true] {
            let table = if duplicate { "header_dup" } else { "header_ok" };
            engine
                .execute(&format!(
                    "CREATE TABLE {table} (id INT PRIMARY KEY, label VARCHAR(40))"
                ))
                .unwrap();
            let mut input = "id|label\n".to_owned();
            input.extend((0..5000).map(|id| format!("{id}|value-{id}\n")));
            if duplicate {
                input.push_str("1|later duplicate\n");
            }
            let before = engine.db.load_shadow_perf_snapshot()[0];
            let mut session = SessionContext::default();
            let sql = format!("LOAD DATA /*+ PARALLEL(4) */ LOCAL INFILE 'client-input' INTO TABLE {table} FIELDS TERMINATED BY '|' IGNORE 1 LINES");
            let response = engine
                .execute_load_data_statement_from_reader(
                    &mut session,
                    crate::parser::parse_statement(&sql).unwrap(),
                    Box::new(std::io::Cursor::new(input.into_bytes())),
                )
                .unwrap();
            assert_eq!(
                response,
                QueryResponse::OkAffected {
                    affected_rows: 5000
                }
            );
            assert_eq!(
                engine.db.load_shadow_perf_snapshot()[0] - before,
                u64::from(!duplicate)
            );
            assert!(session
                .pending_statement_info
                .unwrap()
                .contains(if duplicate {
                    "Records: 5001"
                } else {
                    "Records: 5000"
                }));
            let QueryResponse::ResultSet(rows) = engine
                .execute(&format!("SELECT label FROM {table} WHERE id=0"))
                .unwrap()
            else {
                panic!("expected rows")
            };
            assert_eq!(rows.rows, vec![vec![Value::String("value-0".into())]]);
        }
    }

    #[test]
    fn local_parallel_fatal_data_error_preserves_prior_warnings_and_atomicity() {
        use crate::engine::api::QueryEngine;
        let (engine, _tmp) = setup_strict_load_engine();
        let mut input = (0..5000)
            .map(|id| format!("{id}|12.34|{{\"v\":{id}}}\n"))
            .collect::<String>();
        input.push_str("6000|12.345|{}\n6001|12.34|not-json\n");
        let mut outputs = Vec::new();
        for degree in [1, 4] {
            engine
                .execute(
                    "CREATE TABLE fatal_json (id INT PRIMARY KEY, amount DECIMAL(8,2), doc JSON)",
                )
                .unwrap();
            let mut session = SessionContext::default();
            let sql = format!("LOAD DATA /*+ PARALLEL({degree}) */ LOCAL INFILE 'client-input' INTO TABLE fatal_json FIELDS TERMINATED BY '|'");
            let err = engine
                .execute_load_data_statement_from_reader(
                    &mut session,
                    crate::parser::parse_statement(&sql).unwrap(),
                    Box::new(std::io::Cursor::new(input.as_bytes().to_vec())),
                )
                .unwrap_err();
            assert!(err.sql_state().as_str().starts_with("22"));
            assert!(!session.warnings.is_empty());
            outputs.push((err.code(), err.message().to_owned(), session.warnings));
            let QueryResponse::ResultSet(rows) =
                engine.execute("SELECT COUNT(*) FROM fatal_json").unwrap()
            else {
                panic!("expected rows")
            };
            assert_eq!(rows.rows[0][0], Value::Int(0));
            engine.execute("DROP TABLE fatal_json").unwrap();
        }
        assert_eq!(outputs[0], outputs[1]);
    }

    #[test]
    fn local_parallel_metadata_lease_covers_upload_and_replay() {
        use crate::engine::api::QueryEngine;
        let (engine, _tmp) = setup_strict_load_engine();
        engine
            .execute("CREATE TABLE leased_input (id INT PRIMARY KEY, label VARCHAR(40))")
            .unwrap();
        struct InspectInput {
            db: Arc<Database>,
            bytes: std::io::Cursor<Vec<u8>>,
            checked: bool,
        }
        fn request() -> Vec<crate::storage::MdlRequest> {
            vec![crate::storage::MdlRequest {
                key: crate::storage::MdlObjectKey::table("test", "leased_input"),
                lock_type: crate::storage::MdlLockType::Exclusive,
            }]
        }
        impl Read for InspectInput {
            fn read(&mut self, out: &mut [u8]) -> std::io::Result<usize> {
                if !self.checked {
                    self.checked = true;
                    assert!(self
                        .db
                        .acquire_mdl(
                            42,
                            Some(42),
                            crate::storage::MdlDuration::Statement,
                            request(),
                            "test/ddl"
                        )
                        .is_err());
                    self.db.release_mdl_owner(42);
                }
                self.bytes.read(out)
            }
        }
        let mut input = (0..5000)
            .map(|id| format!("{id}|v{id}\n"))
            .collect::<String>();
        input.push_str("1|later duplicate\n");
        let mut session = SessionContext::default();
        session.connection_id = 41;
        let stmt = crate::parser::parse_statement("LOAD DATA /*+ PARALLEL(4) */ LOCAL INFILE 'client-input' INTO TABLE leased_input FIELDS TERMINATED BY '|'").unwrap();
        let response = engine
            .execute_load_data_statement_from_reader(
                &mut session,
                stmt,
                Box::new(InspectInput {
                    db: engine.db.clone(),
                    bytes: std::io::Cursor::new(input.into_bytes()),
                    checked: false,
                }),
            )
            .unwrap();
        assert_eq!(
            response,
            QueryResponse::OkAffected {
                affected_rows: 5000
            }
        );
        engine
            .db
            .acquire_mdl(
                42,
                Some(42),
                crate::storage::MdlDuration::Statement,
                request(),
                "test/ddl",
            )
            .unwrap();
        engine.db.release_mdl_owner(42);
    }

    #[test]
    fn local_parallel_auto_increment_keeps_ordered_allocation_and_duplicate_semantics() {
        let (engine, _tmp) = setup_strict_load_engine();
        let mut input = (0..5000)
            .map(|id| format!("0|code-{id}\n"))
            .collect::<String>();
        input.push_str("0|code-1\n0|tail\n");
        let mut outputs = Vec::new();
        for degree in [1, 4] {
            engine.execute("CREATE TABLE identities (id INT AUTO_INCREMENT PRIMARY KEY, code VARCHAR(64) UNIQUE)").unwrap();
            let before = engine.db.load_shadow_perf_snapshot()[0];
            let diagnostics = import_local_for_test(&engine, "identities", degree, &input);
            assert_eq!(engine.db.load_shadow_perf_snapshot()[0], before);
            engine
                .execute("INSERT INTO identities (code) VALUES ('after')")
                .unwrap();
            let QueryResponse::ResultSet(rows) = engine
                .execute("SELECT * FROM identities ORDER BY id")
                .unwrap()
            else {
                panic!("expected rows")
            };
            outputs.push((rows.rows, diagnostics));
            engine.execute("DROP TABLE identities").unwrap();
        }
        assert_eq!(outputs[0], outputs[1]);
    }

    #[test]
    fn local_parallel_cancelled_input_eof_never_commits_partial_rows() {
        use crate::engine::api::QueryEngine;
        let (engine, _tmp) = setup_strict_load_engine();
        struct CancelAtEof {
            bytes: std::io::Cursor<Vec<u8>>,
            token: crate::engine::cancel::QueryCancelToken,
        }
        impl Read for CancelAtEof {
            fn read(&mut self, out: &mut [u8]) -> std::io::Result<usize> {
                let n = self.bytes.read(out)?;
                if n == 0 {
                    self.token.cancel_local();
                }
                Ok(n)
            }
        }
        for (table, targets) in [("abort_simple", ""), ("abort_set", "(id,@v) SET label=@v")] {
            engine
                .execute(&format!(
                    "CREATE TABLE {table} (id INT PRIMARY KEY, label VARCHAR(40))"
                ))
                .unwrap();
            let token = crate::engine::cancel::QueryCancelToken::noop();
            let mut session = SessionContext::default();
            session.current_query_token = Some(token.clone());
            let sql=format!("LOAD DATA /*+ PARALLEL(1) */ LOCAL INFILE 'client-input' INTO TABLE {table} FIELDS TERMINATED BY '|' {targets}");
            let err = engine
                .execute_load_data_statement_from_reader(
                    &mut session,
                    crate::parser::parse_statement(&sql).unwrap(),
                    Box::new(CancelAtEof {
                        bytes: std::io::Cursor::new(b"1|one\n2|two\n".to_vec()),
                        token,
                    }),
                )
                .unwrap_err();
            assert!(err.is(crate::error::catalog::ER_QUERY_INTERRUPTED));
            let QueryResponse::ResultSet(rows) = engine
                .execute(&format!("SELECT COUNT(*) FROM {table}"))
                .unwrap()
            else {
                panic!("expected rows")
            };
            assert_eq!(rows.rows[0][0], Value::Int(0));
        }
    }

    #[test]
    fn local_load_empty_table_bounds_insert_locks_and_preserves_ignore() {
        use crate::engine::api::QueryEngine;

        let (engine, tmp) = setup_strict_load_engine();
        engine.execute("CREATE TABLE local_bounded (id BIGINT PRIMARY KEY, u VARCHAR(50) UNIQUE, n TINYINT, amount DECIMAL(12,2), d DATE, pad VARCHAR(100), CHECK (amount >= 0))").unwrap();
        let mut input = String::new();
        for id in 0..1024 {
            input.push_str(&format!("{id}|key-{id}|7|12.34|2024-03-05|中文 text\n"));
        }
        // Duplicate keys across parsing batches retain the first row, while
        // conversion and CHECK errors retain ordinary LOCAL/IGNORE semantics.
        input.push_str("0|other|8|99.00|2024-03-05|duplicate primary\n");
        input.push_str("2048|key-1|8|99.00|2024-03-05|duplicate secondary\n");
        input.push_str("2049|converted|999|12.34|2024-03-05|clamped\n");
        input.push_str("2050|check-failure|8|-1.00|2024-03-05|skipped\n");
        let statement = crate::parser::parse_statement("LOAD DATA LOCAL INFILE 'client-only' INTO TABLE local_bounded FIELDS TERMINATED BY '|' ESCAPED BY ''").unwrap();
        let mut session = SessionContext::default();
        struct BatchRowsReset(usize);
        impl Drop for BatchRowsReset {
            fn drop(&mut self) {
                TEST_BULK_CHUNK_ROWS.set(self.0);
            }
        }
        let _reset = BatchRowsReset(TEST_BULK_CHUNK_ROWS.replace(32));
        let response = crate::util::env::with_vars(&[("MYDB_MAX_LOCKS", Some("64"))], || {
            engine.execute_load_data_statement_from_reader(
                &mut session,
                statement,
                Box::new(std::io::Cursor::new(input.into_bytes())),
            )
        })
        .unwrap();
        assert_eq!(
            response,
            QueryResponse::OkAffected {
                affected_rows: 1025
            }
        );
        assert!(session.warning_count() >= 2);
        let QueryResponse::ResultSet(rows) = engine
            .execute("SELECT n, amount, pad FROM local_bounded WHERE id IN (0,2049) ORDER BY id")
            .unwrap()
        else {
            panic!("expected rows")
        };
        assert_eq!(rows.rows.len(), 2);
        assert_eq!(rows.rows[0][0], Value::Int(7));
        assert_eq!(rows.rows[1][0], Value::Int(127));
        drop(engine);
        let engine =
            StorageQueryEngine::open_with_storage_config(tmp.path(), strict_load_config()).unwrap();
        let QueryResponse::ResultSet(rows) = engine
            .execute("SELECT COUNT(*) FROM local_bounded")
            .unwrap()
        else {
            panic!("expected rows")
        };
        assert_eq!(rows.rows[0][0], Value::Int(1025));
        let QueryResponse::ResultSet(rows) = engine
            .execute("SELECT id FROM local_bounded WHERE u='converted'")
            .unwrap()
        else {
            panic!("expected rows")
        };
        assert_eq!(rows.rows, vec![vec![Value::Int(2049)]]);
    }

    #[test]
    fn local_load_general_path_and_late_input_failure_keep_atomicity() {
        use crate::engine::api::QueryEngine;

        let (engine, _tmp) = setup_strict_load_engine();
        engine
            .execute("CREATE TABLE local_json (id INT PRIMARY KEY, doc JSON NOT NULL)")
            .unwrap();
        let input = (0..1024)
            .map(|id| format!("{id}|{{\"value\":{id}}}\n"))
            .collect::<String>();
        let sql = "LOAD DATA LOCAL INFILE 'client-only' INTO TABLE local_json FIELDS TERMINATED BY '|' ESCAPED BY '' (id,@doc) SET doc=@doc";
        let mut session = SessionContext::default();
        let result = crate::util::env::with_vars(&[("MYDB_MAX_LOCKS", Some("64"))], || {
            engine.execute_load_data_statement_from_reader(
                &mut session,
                crate::parser::parse_statement(sql).unwrap(),
                Box::new(std::io::Cursor::new(input.into_bytes())),
            )
        })
        .unwrap();
        assert_eq!(
            result,
            QueryResponse::OkAffected {
                affected_rows: 1024
            }
        );

        struct FailedInput(std::io::Cursor<Vec<u8>>);
        impl Read for FailedInput {
            fn read(&mut self, out: &mut [u8]) -> std::io::Result<usize> {
                let n = self.0.read(out)?;
                if n == 0 {
                    Err(std::io::Error::other("injected input failure"))
                } else {
                    Ok(n)
                }
            }
        }
        engine
            .execute("CREATE TABLE local_failed (id INT PRIMARY KEY, doc JSON NOT NULL)")
            .unwrap();
        let input = (0..(BULK_CHUNK_SIZE + 1))
            .map(|id| format!("{id}|{{\"value\":{id}}}\n"))
            .collect::<String>();
        let result = engine.execute_load_data_statement_from_reader(
            &mut session,
            crate::parser::parse_statement(&sql.replace("local_json", "local_failed")).unwrap(),
            Box::new(FailedInput(std::io::Cursor::new(input.into_bytes()))),
        );
        assert!(result
            .unwrap_err()
            .message()
            .contains("injected input failure"));
        let QueryResponse::ResultSet(rows) =
            engine.execute("SELECT COUNT(*) FROM local_failed").unwrap()
        else {
            panic!("expected rows")
        };
        assert_eq!(rows.rows[0][0], Value::Int(0));
        // The failed scope must not affect the next ordinary transaction.
        engine
            .execute("INSERT INTO local_failed VALUES (1,'{}')")
            .unwrap();
    }

    #[test]
    fn local_load_explicit_transaction_retains_ordinary_insert_locks() {
        use crate::engine::api::QueryEngine;

        let (engine, _tmp) = setup_strict_load_engine();
        engine
            .execute("CREATE TABLE local_explicit (id INT PRIMARY KEY)")
            .unwrap();
        let mut session = SessionContext::default();
        engine.execute_with_session(&mut session, "BEGIN").unwrap();
        let input = (0..256).map(|id| format!("{id}\n")).collect::<String>();
        let result = crate::util::env::with_vars(&[("MYDB_MAX_LOCKS", Some("64"))], || {
            engine.execute_load_data_statement_from_reader(
                &mut session,
                crate::parser::parse_statement(
                    "LOAD DATA LOCAL INFILE 'client-only' INTO TABLE local_explicit",
                )
                .unwrap(),
                Box::new(std::io::Cursor::new(input.into_bytes())),
            )
        });
        assert_eq!(
            result.unwrap_err().code(),
            crate::error::catalog::ER_LOCK_TABLE_FULL.code()
        );
        engine
            .execute_with_session(&mut session, "ROLLBACK")
            .unwrap();
        let QueryResponse::ResultSet(rows) = engine
            .execute("SELECT COUNT(*) FROM local_explicit")
            .unwrap()
        else {
            panic!("expected rows")
        };
        assert_eq!(rows.rows[0][0], Value::Int(0));
    }

    #[test]
    fn direct_delimiter_scan_matches_serial_for_escaped_and_plain_rows() {
        let (engine, tmp) = setup_strict_load_engine();
        for (case, delimiter, escape, options) in [
            ("ascii", '|', '\\', "FIELDS TERMINATED BY '|'"),
            ("pipe", '|', '\\', "FIELDS TERMINATED BY '|'"),
            (
                "custom",
                ',',
                '!',
                "FIELDS TERMINATED BY ',' ESCAPED BY '!'",
            ),
            ("raw", '|', '\\', "FIELDS TERMINATED BY '|' ESCAPED BY ''"),
        ] {
            let path = tmp.path().join(format!("{case}.tbl"));
            let mut input = String::new();
            // Escaped newlines merge some physical lines; retain enough
            // logical records to exercise the parallel decoder as well.
            for id in 0..2 * PARALLEL_MIN_ROWS + 257 {
                let fields = match if case == "ascii" { id % 2 } else { id % 7 } {
                    0 => format!("plain{delimiter}{}", "wide text ".repeat(24)),
                    1 => format!("{delimiter}"),
                    2 => format!("中文é{delimiter}end{delimiter}"),
                    3 => format!("escaped{escape}{delimiter}field{delimiter}tail{escape}{escape}"),
                    4 => format!("\\N{delimiter}nullable"),
                    5 => "missing final field".to_owned(),
                    _ => format!("plain{delimiter}trailing{escape}"),
                };
                input.push_str(&format!("{id}{delimiter}{fields}\n"));
            }
            std::fs::write(&path, input).unwrap();
            let mut results = Vec::new();
            let mut warning_codes = Vec::new();
            for degree in [1, 4] {
                let table = format!("scan_{case}_{degree}");
                engine
                    .execute(&format!(
                    "CREATE TABLE {table} (id BIGINT PRIMARY KEY, a VARCHAR(300), b VARCHAR(300))"
                ))
                    .unwrap();
                let mut session = SessionContext::default();
                let before = engine.db.load_shadow_perf_snapshot();
                engine
                    .execute_with_session(
                        &mut session,
                        &format!(
                    "LOAD DATA /*+ PARALLEL({degree}) */ INFILE '{}' INTO TABLE {table} {options}",
                    crate::util::file::sql_path(&path)
                ),
                    )
                    .unwrap();
                if degree == 4 && case != "custom" {
                    assert!(
                        engine.db.load_shadow_perf_snapshot()[10] > before[10],
                        "format={case}"
                    );
                }
                let mut codes = session
                    .warnings
                    .iter()
                    .map(|w| w.code())
                    .collect::<Vec<_>>();
                codes.sort_unstable();
                warning_codes.push(codes);
                let QueryResponse::ResultSet(rows) = engine
                    .execute(&format!("SELECT * FROM {table} ORDER BY id"))
                    .unwrap()
                else {
                    panic!("expected rows")
                };
                results.push(rows.rows);
            }
            assert_eq!(results[0], results[1], "format={case}");
            assert_eq!(warning_codes[0], warning_codes[1], "format={case}");
        }
    }

    #[test]
    fn parallel_load_explicit_auto_increment_uses_shadow_and_advances_sequence() {
        let (engine, tmp) = setup_strict_load_engine();
        engine
            .execute(
                "CREATE TABLE auto_explicit (id BIGINT AUTO_INCREMENT PRIMARY KEY, v INT NOT NULL, ts TIMESTAMP)",
            )
            .unwrap();
        let before = engine.db.load_shadow_perf_snapshot();
        let rows = PARALLEL_MIN_ROWS + 257;
        let data_path = tmp.path().join("auto-explicit.tsv");
        let mut data = String::with_capacity(rows * 36);
        for id in 1..=rows {
            data.push_str(&format!("{id}\t{}\t2026-01-01 00:00:00.600000\n", id % 17));
        }
        std::fs::write(&data_path, data).unwrap();
        let sql = format!(
            "LOAD DATA /*+ PARALLEL(4) */ INFILE '{}' INTO TABLE auto_explicit",
            crate::util::file::sql_path(&data_path)
        );
        assert_eq!(
            engine.execute(&sql).unwrap(),
            QueryResponse::OkAffected {
                affected_rows: rows as u64
            }
        );
        let after = engine.db.load_shadow_perf_snapshot();
        assert_eq!(after[0], before[0] + 1);
        assert_eq!(after[10] - before[10], rows as u64);
        let QueryResponse::ResultSet(timestamp) = engine
            .execute("SELECT ts FROM auto_explicit WHERE id=1")
            .unwrap()
        else {
            panic!("expected timestamp row");
        };
        assert_eq!(
            timestamp.rows[0][0].to_display_string(),
            "2026-01-01 00:00:01"
        );
        engine
            .execute("INSERT INTO auto_explicit(v,ts) VALUES (999,'2026-01-02 00:00:00')")
            .unwrap();
        let QueryResponse::ResultSet(inserted) = engine
            .execute("SELECT id FROM auto_explicit WHERE v=999")
            .unwrap()
        else {
            panic!("expected inserted identity");
        };
        assert_eq!(inserted.rows, vec![vec![Value::Int(rows as i64 + 1)]]);
    }

    #[test]
    fn parallel_load_direct_decoder_handles_text_and_mixed_fixed_primary_key() {
        let (engine, tmp) = setup_strict_load_engine();
        engine
            .execute(
                "CREATE TABLE direct_wide (\
                    id BIGINT NOT NULL, d DATE NOT NULL, ts TIMESTAMP NOT NULL, \
                    body TEXT NOT NULL, tag VARCHAR(32) NOT NULL, \
                    PRIMARY KEY (id, d, ts))",
            )
            .unwrap();
        let before = engine.db.load_shadow_perf_snapshot();
        let rows = PARALLEL_MIN_ROWS + 257;
        let data_path = tmp.path().join("direct-wide.tsv");
        let mut data = String::with_capacity(rows * 64);
        for id in 1..=rows {
            data.push_str(&format!(
                "{id}\t2026-01-02\t2026-01-02 03:04:05\tpayload-中文\\f-{id}\ttag{}\n",
                id % 17
            ));
        }
        std::fs::write(&data_path, data).unwrap();
        let sql = format!(
            "LOAD DATA /*+ PARALLEL(4) */ INFILE '{}' INTO TABLE direct_wide",
            crate::util::file::sql_path(&data_path)
        );
        assert_eq!(
            engine.execute(&sql).unwrap(),
            QueryResponse::OkAffected {
                affected_rows: rows as u64
            }
        );
        let after = engine.db.load_shadow_perf_snapshot();
        assert_eq!(after[10] - before[10], rows as u64);
        assert_eq!(after[11] - before[11], 0);

        let QueryResponse::ResultSet(result) = engine
            .execute(
                "SELECT body,tag FROM direct_wide \
                 WHERE id=42 AND d='2026-01-02' AND ts='2026-01-02 03:04:05'",
            )
            .unwrap()
        else {
            panic!("expected indexed row");
        };
        assert_eq!(
            result.rows,
            vec![vec![
                Value::String("payload-中文f-42".into()),
                Value::String("tag8".into())
            ]]
        );
    }

    #[test]
    fn parallel_load_direct_text_preserves_length_error() {
        let (engine, tmp) = setup_strict_load_engine();
        engine
            .execute("CREATE TABLE direct_text_limit (v TINYTEXT NOT NULL)")
            .unwrap();
        let data_path = tmp.path().join("direct-text-limit.tsv");
        let mut data = String::with_capacity(PARALLEL_MIN_ROWS * 3 + 257);
        for _ in 0..PARALLEL_MIN_ROWS {
            data.push_str("ok\n");
        }
        data.push_str(&"x".repeat(256));
        data.push('\n');
        std::fs::write(&data_path, data).unwrap();
        let sql = format!(
            "LOAD DATA /*+ PARALLEL(4) */ INFILE '{}' INTO TABLE direct_text_limit",
            crate::util::file::sql_path(&data_path)
        );
        let err = engine.execute(&sql).unwrap_err();
        assert_eq!(err.code(), crate::error::catalog::ER_DATA_TOO_LONG.code());
    }

    #[test]
    fn parallel_load_mixed_auto_increment_assigns_in_input_order_on_shadow() {
        let (engine, tmp) = setup_strict_load_engine();
        engine
            .execute("CREATE TABLE auto_mixed (id INT AUTO_INCREMENT PRIMARY KEY, v INT NOT NULL)")
            .unwrap();
        let rows = PARALLEL_MIN_ROWS + 257;
        let data_path = tmp.path().join("auto-mixed.tsv");
        let mut data = String::with_capacity(rows * 12);
        data.push_str("100\t0\n");
        for value in 1..rows {
            data.push_str(&format!("0\t{value}\n"));
        }
        std::fs::write(&data_path, data).unwrap();
        let sql = format!(
            "LOAD DATA /*+ PARALLEL(4) */ INFILE '{}' INTO TABLE auto_mixed",
            crate::util::file::sql_path(&data_path)
        );
        assert_eq!(
            engine.execute(&sql).unwrap(),
            QueryResponse::OkAffected {
                affected_rows: rows as u64
            }
        );
        let QueryResponse::ResultSet(count) =
            engine.execute("SELECT COUNT(*) FROM auto_mixed").unwrap()
        else {
            panic!("expected count");
        };
        assert_eq!(count.rows[0][0], Value::Int(rows as i64));
        engine
            .execute("INSERT INTO auto_mixed(v) VALUES (999999)")
            .unwrap();
        let QueryResponse::ResultSet(inserted) = engine
            .execute("SELECT id FROM auto_mixed WHERE v=999999")
            .unwrap()
        else {
            panic!("expected inserted identity");
        };
        assert_eq!(inserted.rows, vec![vec![Value::Int(rows as i64 + 100)]]);
    }

    #[test]
    fn test_parallel_load_index_build_spills_and_releases_reservation() {
        let (engine, tmp) = setup_engine();
        engine
            .execute(
                "CREATE TABLE spill_t (id INT PRIMARY KEY, k VARCHAR(16) NOT NULL, pad CHAR(16) NOT NULL, KEY k_idx(k))",
            )
            .unwrap();

        // The encoded sorter may now keep this batch resident because it no
        // longer stores one allocation-heavy IndexKey per row. Forced
        // multi-run spill/merge is covered by the encoded-sorter unit test;
        // this end-to-end case verifies statement reservation lifetime and
        // lookup correctness regardless of the adaptive spill decision.
        let data_path = tmp.path().join("spill.tsv");
        let row_count = 25_000usize;
        let mut data = String::with_capacity(row_count * 48);
        for i in 0..row_count {
            data.push_str(&format!("{i}\tkey{:012}\tabcdefghijklmnop\n", i % 4093));
        }
        std::fs::write(&data_path, data).unwrap();

        let scratch_before = crate::engine::executor::mem_budget::controlled_memory_snapshot().2[1];
        let sql = format!(
            "LOAD DATA /*+ PARALLEL(4) */ INFILE '{}' INTO TABLE spill_t",
            crate::util::file::sql_path(&data_path)
        );
        assert_eq!(
            engine.execute(&sql).unwrap(),
            QueryResponse::OkAffected {
                affected_rows: row_count as u64
            }
        );
        assert_eq!(
            crate::engine::executor::mem_budget::controlled_memory_snapshot().2[1],
            scratch_before,
            "LOAD index/batch reservations must end with the statement"
        );

        let QueryResponse::ResultSet(rows) = engine
            .execute("SELECT id FROM spill_t WHERE k='key000000000042' ORDER BY id LIMIT 3")
            .unwrap()
        else {
            panic!("expected indexed result set");
        };
        assert_eq!(
            rows.rows,
            vec![
                vec![Value::Int(42)],
                vec![Value::Int(42 + 4093)],
                vec![Value::Int(42 + 4093 * 2)],
            ]
        );
    }

    #[test]
    fn test_load_into_nonempty_index_preserves_existing_entries() {
        let (engine, tmp) = setup_engine();
        engine
            .execute("CREATE TABLE append_t (id INT PRIMARY KEY, k INT NOT NULL, KEY k_idx(k))")
            .unwrap();
        engine
            .execute("INSERT INTO append_t VALUES (1, 7)")
            .unwrap();

        let data_path = tmp.path().join("append.tsv");
        let mut data = String::new();
        for i in 2..=2_000 {
            data.push_str(&format!("{i}\t{}\n", i % 17));
        }
        std::fs::write(&data_path, data).unwrap();
        let sql = format!(
            "LOAD DATA /*+ PARALLEL(4) */ INFILE '{}' INTO TABLE append_t",
            crate::util::file::sql_path(&data_path)
        );
        assert_eq!(
            engine.execute(&sql).unwrap(),
            QueryResponse::OkAffected {
                affected_rows: 1_999
            }
        );

        let QueryResponse::ResultSet(existing) = engine
            .execute("SELECT id FROM append_t WHERE k=7 AND id=1")
            .unwrap()
        else {
            panic!("expected indexed result set");
        };
        assert_eq!(existing.rows, vec![vec![Value::Int(1)]]);
        let QueryResponse::ResultSet(count) = engine
            .execute("SELECT COUNT(*) FROM append_t WHERE k=7")
            .unwrap()
        else {
            panic!("expected indexed count");
        };
        assert_eq!(count.rows[0][0], Value::Int(119));
    }

    #[test]
    fn test_parallel_load_duplicate_primary_key_rolls_back_streamed_build() {
        let (engine, tmp) = setup_engine();
        engine
            .execute("CREATE TABLE dup_t (a INT NOT NULL, b INT NOT NULL, PRIMARY KEY(a,b))")
            .unwrap();

        let data_path = tmp.path().join("duplicate.tsv");
        let mut data = String::new();
        for i in 0..PARALLEL_MIN_ROWS {
            data.push_str(&format!("{i}\t{}\n", i % 7));
        }
        data.push_str("42\t0\n");
        std::fs::write(&data_path, data).unwrap();
        let sql = format!(
            "LOAD DATA /*+ PARALLEL(4) */ INFILE '{}' INTO TABLE dup_t",
            crate::util::file::sql_path(&data_path)
        );
        let err = engine.execute(&sql).unwrap_err();
        assert_eq!(err.code(), crate::error::catalog::ER_DUP_ENTRY.code());

        let QueryResponse::ResultSet(count) = engine.execute("SELECT COUNT(*) FROM dup_t").unwrap()
        else {
            panic!("expected count result set");
        };
        assert_eq!(count.rows[0][0], Value::Int(0));
    }

    #[test]
    fn test_parallel_load_encoded_compound_key_is_queryable() {
        let tmp = TempDir::new().unwrap();
        let config = StorageConfig {
            durability: DurabilityMode::Strict,
            checkpoint_interval: Duration::from_millis(60_000),
            checkpoint_wal_bytes: 64 * 1024 * 1024,
            lock_wait: Duration::from_millis(5_000),
            group_commit_window: Duration::from_micros(0),
            wal_segment_bytes: 256 * 1024 * 1024,
            buffer_pool_pages: 2_048,
            buffer_pool_io_threads: 2,
            buffer_pool_prefetch_pages: 4,
            undo_retention: Duration::from_millis(60_000),
            heap_update_reserve_bytes: 2_048,
            startup_validation: StartupValidationMode::Auto,
            startup_validate_indexes: true,
        };
        let engine = StorageQueryEngine::open_with_storage_config(tmp.path(), config).unwrap();
        crate::engine::sysvar::builtin::query::allow_test_server_files();
        engine
            .execute("CREATE TABLE pair_t (a INT NOT NULL, b INT NOT NULL, pad CHAR(8), PRIMARY KEY(a,b))")
            .unwrap();

        let rows = 20_000usize;
        let data_path = tmp.path().join("pair-direct.tsv");
        let mut data = String::with_capacity(rows * 20);
        for value in 0..rows {
            data.push_str(&format!("{}\t{}\tpadding\n", value / 7, value % 7));
        }
        std::fs::write(&data_path, data).unwrap();
        let sql = format!(
            "LOAD DATA /*+ PARALLEL(4) */ INFILE '{}' INTO TABLE pair_t",
            crate::util::file::sql_path(&data_path)
        );
        assert_eq!(
            engine.execute(&sql).unwrap(),
            QueryResponse::OkAffected {
                affected_rows: rows as u64,
            }
        );

        let QueryResponse::ResultSet(point) = engine
            .execute("SELECT pad FROM pair_t WHERE a=1234 AND b=5")
            .unwrap()
        else {
            panic!("expected indexed point lookup");
        };
        assert_eq!(point.rows, vec![vec![Value::String("padding".into())]]);
        let QueryResponse::ResultSet(count) =
            engine.execute("SELECT COUNT(*) FROM pair_t").unwrap()
        else {
            panic!("expected count result set");
        };
        assert_eq!(count.rows[0][0], Value::Int(rows as i64));
    }

    #[test]
    fn test_parallel_load_canonical_fast_path_preserves_domain_errors() {
        let (engine, tmp) = setup_engine();
        let cases = [
            (
                "tiny_range",
                "CREATE TABLE tiny_range (v TINYINT NOT NULL)",
                "1",
                "128",
                crate::error::catalog::ER_WARN_DATA_OUT_OF_RANGE,
            ),
            (
                "unsigned_tiny_range",
                "CREATE TABLE unsigned_tiny_range (v TINYINT UNSIGNED NOT NULL)",
                "255",
                "256",
                crate::error::catalog::ER_WARN_DATA_OUT_OF_RANGE,
            ),
            (
                "decimal_range",
                "CREATE TABLE decimal_range (v DECIMAL(4,2) NOT NULL)",
                "1.25",
                "100.00",
                crate::error::catalog::ER_WARN_DATA_OUT_OF_RANGE,
            ),
            (
                "unsigned_decimal_range",
                "CREATE TABLE unsigned_decimal_range (v DECIMAL(4,2) UNSIGNED NOT NULL)",
                "1.25",
                "-1.00",
                crate::error::catalog::ER_WARN_DATA_OUT_OF_RANGE,
            ),
            (
                "char_width",
                "CREATE TABLE char_width (v CHAR(2) NOT NULL)",
                "ok",
                "abc",
                crate::error::catalog::ER_DATA_TOO_LONG,
            ),
        ];

        for (table, ddl, good, bad, expected_code) in cases {
            engine.execute(ddl).unwrap();
            let path = tmp.path().join(format!("{table}.tsv"));
            let mut data = String::new();
            // Exceed PARALLEL_MIN_ROWS so this exercises worker-side parsing
            // and the fused field-domain checks, not the small-file fallback.
            for _ in 0..PARALLEL_MIN_ROWS {
                data.push_str(good);
                data.push('\n');
            }
            data.push_str(bad);
            data.push('\n');
            std::fs::write(&path, data).unwrap();

            let sql = format!(
                "LOAD DATA /*+ PARALLEL(4) */ INFILE '{}' INTO TABLE {table}",
                crate::util::file::sql_path(&path)
            );
            let err = engine.execute(&sql).unwrap_err();
            assert_eq!(err.code(), expected_code.code(), "table={table}: {err}");
        }
    }

    #[test]
    fn audit_parallel_wide_decimal_small_value_uses_wide_codec() {
        let (engine, tmp) = setup_engine();
        engine
            .execute("CREATE TABLE wide_small (a INT PRIMARY KEY, b DECIMAL(40,2))")
            .unwrap();
        let path = tmp.path().join("wide-small.csv");
        let data = (1..=3000)
            .map(|i| format!("{i},1.235\n"))
            .collect::<String>();
        std::fs::write(&path, data).unwrap();
        engine.execute(&format!("LOAD DATA /*+ PARALLEL(4) */ INFILE '{}' INTO TABLE wide_small FIELDS TERMINATED BY ','", crate::util::file::sql_path(&path))).unwrap();
        let QueryResponse::ResultSet(rs) = engine
            .execute("SELECT COUNT(*) FROM wide_small WHERE b=1.24")
            .unwrap()
        else {
            panic!()
        };
        assert_eq!(rs.rows[0][0], Value::Int(3000));
    }

    #[test]
    fn test_parallel_load_wide_decimal_retains_arbitrary_precision_path() {
        let (engine, tmp) = setup_engine();
        engine
            .execute("CREATE TABLE wide_decimal (v DECIMAL(65,0) NOT NULL)")
            .unwrap();

        let path = tmp.path().join("wide_decimal.tsv");
        let wide_value = "12345678901234567890123456789012345678901234567890";
        let mut data = String::new();
        for _ in 0..=PARALLEL_MIN_ROWS {
            data.push_str(wide_value);
            data.push('\n');
        }
        std::fs::write(&path, data).unwrap();

        let sql = format!(
            "LOAD DATA /*+ PARALLEL(4) */ INFILE '{}' INTO TABLE wide_decimal",
            crate::util::file::sql_path(&path)
        );
        assert_eq!(
            engine.execute(&sql).unwrap(),
            QueryResponse::OkAffected {
                affected_rows: (PARALLEL_MIN_ROWS + 1) as u64,
            }
        );

        let QueryResponse::ResultSet(count) =
            engine.execute("SELECT COUNT(*) FROM wide_decimal").unwrap()
        else {
            panic!("expected count result set");
        };
        assert_eq!(count.rows[0][0], Value::Int((PARALLEL_MIN_ROWS + 1) as i64));
    }

    #[test]
    fn test_load_data_no_parallel_hint_still_loads() {
        let (engine, tmp) = setup_engine();
        engine
            .execute("CREATE TABLE t (id INT PRIMARY KEY, name VARCHAR(100))")
            .unwrap();

        let data_path = tmp.path().join("no_parallel.tsv");
        std::fs::write(&data_path, "1\tAlice\n2\tBob\n").unwrap();

        let sql = format!(
            "LOAD DATA /*+ NO_PARALLEL */ INFILE '{}' INTO TABLE t",
            crate::util::file::sql_path(&data_path)
        );
        let result = engine.execute(&sql).unwrap();
        assert_eq!(result, QueryResponse::OkAffected { affected_rows: 2 });
    }

    #[test]
    fn test_load_data_zone_map_observe_option_is_statement_level() {
        crate::util::env::with_vars(&[("MYDB_ZONE_MAP_LOAD_OBSERVE", None)], || {
            assert!(!load_data_zone_map_observe_enabled());
        });
        crate::util::env::with_vars(&[("MYDB_ZONE_MAP_LOAD_OBSERVE", Some("1"))], || {
            assert!(load_data_zone_map_observe_enabled());
        });
        crate::util::env::with_vars(&[("MYDB_ZONE_MAP_LOAD_OBSERVE", Some("true"))], || {
            assert!(!load_data_zone_map_observe_enabled());
        });
    }

    #[test]
    fn test_load_data_parallel_hint_ignore_uses_conflict_path() {
        let (engine, tmp) = setup_engine();
        engine
            .execute("CREATE TABLE t (id INT PRIMARY KEY, name VARCHAR(100))")
            .unwrap();
        engine.execute("INSERT INTO t VALUES (1, 'old')").unwrap();

        let data_path = tmp.path().join("ignore.tsv");
        std::fs::write(&data_path, "1\tnew\n2\tBob\n").unwrap();

        let sql = format!(
            "LOAD DATA /*+ PARALLEL(4) */ INFILE '{}' IGNORE INTO TABLE t",
            crate::util::file::sql_path(&data_path)
        );
        let result = engine.execute(&sql).unwrap();
        assert_eq!(result, QueryResponse::OkAffected { affected_rows: 1 });

        let result = engine.execute("SELECT name FROM t WHERE id = 1").unwrap();
        if let QueryResponse::ResultSet(rs) = result {
            assert_eq!(rs.rows[0][0], Value::String("old".to_string()));
        } else {
            panic!("expected result set");
        }
    }
}

#[cfg(test)]
mod audit1022_multibyte_tests {
    use super::*;
    #[test]
    fn audit1022_load_delimiters_inside_legacy_characters_are_not_fields() {
        for (charset, terminator, source, expected) in [
            ("gbk", b'|', b"\x81|X|z".as_slice(), b"\x81|X".as_slice()),
            (
                "gb18030",
                b'3',
                b"\x813\x8133z".as_slice(),
                b"\x813\x813".as_slice(),
            ),
            (
                "gbk",
                b',',
                b"\x81\x5cX,z".as_slice(),
                b"\x81\x5cX".as_slice(),
            ),
        ] {
            let parser = FieldParser {
                input_charset: LoadLegacyCharset::from_name(Some(charset)),
                field_terminator: vec![terminator],
                line_terminator: b"\n".to_vec(),
                enclosed_by: None,
                escaped_by: Some(b'\\'),
                line_starting_by: Vec::new(),
            };
            assert_eq!(
                parser.parse_row_raw(source),
                vec![Some(expected.to_vec()), Some(b"z".to_vec())],
                "{charset}"
            );
        }
    }

    #[test]
    fn audit1022_load_bulk_records_ignore_multibyte_trailing_backslashes() {
        for charset in ["gbk", "big5", "sjis", "cp932", "gb18030"] {
            let parser = FieldParser {
                input_charset: LoadLegacyCharset::from_name(Some(charset)),
                field_terminator: b",".to_vec(),
                line_terminator: b"\n".to_vec(),
                enclosed_by: None,
                escaped_by: Some(b'\\'),
                line_starting_by: Vec::new(),
            };
            for capacity in 1..=4 {
                let source = b"\x81\x5c\n\x81\x5c\\\nX\n";
                let mut reader = BufReader::with_capacity(capacity, source.as_slice());
                let mut record = Vec::new();
                assert!(read_load_record(&mut reader, &parser, &mut record, 1).unwrap());
                assert_eq!(
                    parser.parse_row_raw(&record),
                    vec![Some(b"\x81\x5c".to_vec())]
                );
                assert!(read_load_record(&mut reader, &parser, &mut record, 1).unwrap());
                assert_eq!(
                    parser.parse_row_raw(&record),
                    vec![Some(b"\x81\x5c\nX".to_vec())]
                );
                assert!(!read_load_record(&mut reader, &parser, &mut record, 1).unwrap());
            }
        }
    }

    #[test]
    fn audit1022_load_multibyte_syntax_bytes_and_stream_boundaries() {
        for charset in ["gbk", "gb18030", "big5", "sjis", "cp932"] {
            let parser = FieldParser {
                input_charset: LoadLegacyCharset::from_name(Some(charset)),
                field_terminator: b",".to_vec(),
                line_terminator: b"\n".to_vec(),
                enclosed_by: Some(b'"'),
                escaped_by: Some(b'\\'),
                line_starting_by: Vec::new(),
            };
            for source in [
                b"\x81\x5cX,z\n".as_slice(),
                b"\"\x81\x5c\",z\n".as_slice(),
                b"\x81\x5c,z\n".as_slice(),
            ] {
                for capacity in 1..=4 {
                    let mut reader = BufReader::with_capacity(capacity, source);
                    let mut record = Vec::new();
                    assert!(read_load_record(&mut reader, &parser, &mut record, 2).unwrap());
                    let fields = parser.parse_row_raw(&record);
                    let expected = if source[2] == b'X' {
                        b"\x81\x5cX".as_slice()
                    } else {
                        b"\x81\x5c".as_slice()
                    };
                    assert_eq!(
                        fields,
                        vec![Some(expected.to_vec()), Some(b"z".to_vec())],
                        "{charset}: {source:?}, chunk={capacity}"
                    );
                }
            }
        }
    }
}
