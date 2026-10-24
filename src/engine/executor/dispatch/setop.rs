//! Set operations, spillable combination and subquery dispatch.
use super::*;

mod external_reduce;

/// Turn a branch response into `(columns, cursor)` without materialising:
/// a `StreamingResultSet` hands over its cursor directly, an in-memory
/// `ResultSet` is wrapped in a `VecResultCursor`. Powers the streaming
/// `UNION ALL` fast path.
pub(in crate::engine::executor) fn response_into_cursor(
    resp: QueryResponse,
) -> Result<(Vec<Column>, Box<dyn crate::engine::api::ResultCursor>), QueryError> {
    match resp {
        QueryResponse::ResultSet(rs) => Ok((
            rs.columns,
            Box::new(crate::engine::api::VecResultCursor::new(rs.rows)),
        )),
        QueryResponse::StreamingResultSet(s) => Ok((s.columns, s.cursor)),
        _ => Err(error(
            crate::error::catalog::ER_NOT_SUPPORTED_YET,
            "set operation requires a SELECT result",
        )),
    }
}

/// Build a streaming `UNION ALL` from two branch responses by chaining
/// their cursors end-to-end — no intermediate materialised `Vec`. The
/// left branch's columns name the result (matching the materialised
/// `combine_set_operation_results` path).
fn chained_union_all(
    left: QueryResponse,
    right: QueryResponse,
) -> Result<QueryResponse, QueryError> {
    let (mut lcols, lcur) = response_into_cursor(left)?;
    let (mut rcols, rcur) = response_into_cursor(right)?;
    if lcols.len() != rcols.len() {
        return Err(error(
            crate::error::catalog::ER_WRONG_NUMBER_OF_COLUMNS_IN_SELECT,
            "the used SELECT statements have a different number of columns",
        ));
    }
    unify_setop_declared_column_metadata(&mut lcols, &mut rcols)?;
    Ok(QueryResponse::StreamingResultSet(StreamingResultSet {
        columns: lcols,
        found_rows: None,
        cursor: Box::new(crate::engine::api::ChainedCursor::new(vec![lcur, rcur])),
    }))
}

pub(in crate::engine::executor) fn execute_set_operation_dispatch(
    db: &Arc<Database>,
    session: &mut SessionContext,
    stmt: SetOperation,
    registry: &SessionRegistry,
) -> Result<QueryResponse, QueryError> {
    fn all_selects<'q>(
        statement: impl Into<crate::parser::QueryRef<'q>>,
        predicate: &impl Fn(&Select) -> bool,
    ) -> bool {
        let statement = statement.into();
        match statement {
            crate::parser::QueryRef::Select(select) => predicate(select),
            crate::parser::QueryRef::SetOperation(set) => {
                all_selects(&set.left, predicate) && all_selects(&set.right, predicate)
            }
            _ => false,
        }
    }

    // Lower the complete query expression before opening either branch. This
    // is the ordinary path for table-backed set operations; constant-only and
    // WITH branches retain their dedicated source setup and are rejected by
    // `plan_query_physical` before any I/O.
    let mut physical_statement = Statement::SetOperation(stmt.clone());
    fn has_locks<'q>(statement: impl Into<crate::parser::QueryRef<'q>>) -> bool {
        let statement = statement.into();
        match statement {
            crate::parser::QueryRef::Select(select) => {
                crate::engine::executor::tx::select_has_locking_read(select)
            }
            crate::parser::QueryRef::SetOperation(set) => {
                has_locks(&set.left) || has_locks(&set.right)
            }
            crate::parser::QueryRef::WithCte(with) => {
                has_locks(&with.body) || with.ctes.iter().any(|cte| has_locks(&cte.query))
            }
            _ => false,
        }
    }
    if has_locks(&physical_statement) {
        return execute_in_transaction_with_session(db, session, |db, _session, txn| {
            execute_set_operation_rows(db, txn, stmt.clone(), registry)?.materialize()
        });
    }
    let read_only = all_selects(&physical_statement, &|select| {
        can_use_read_only_select(session, select)
    });
    // A set operation is itself read-only even when a leaf is a fromless
    // SELECT that the table-oriented transaction router does not classify.
    // Always attempt to lower the complete query graph; unsupported leaves
    // return `None` before opening a source and continue to the scalar lane.
    {
        let (txn, _snapshot_guard) = if read_only {
            let (txn, guard) = tx::autocommit_read_only_view_with_guard(db, session);
            (txn, Some(guard))
        } else if session.current_txn.is_none() {
            tx::lazy_read_only_transaction_view_with_guard(db, session)
        } else {
            tx::current_transaction_read_select_view(db, session)?
        };
        let mut inline_ctes = std::collections::HashMap::new();
        materialize_setop_inline_derived(
            db,
            txn,
            &mut physical_statement,
            registry,
            &mut inline_ctes,
        )?;
        let native = crate::engine::executor::cte::with_cte_registry_extended(inline_ctes, || {
            let Some(plan) = crate::planner::plan_query_physical(db, &physical_statement)? else {
                return Ok(None);
            };
            let budget =
                crate::engine::executor::mem_budget::current_budget().unwrap_or_else(|| {
                    std::sync::Arc::new(crate::engine::executor::mem_budget::MemBudget::from_env())
                });
            let context = crate::engine::executor::select::execution_context::ExecutionContext::capture_with_budget(
                    txn,
                    budget,
                );
            crate::engine::executor::select::vec_bridge::try_stream_native_plan(
                Arc::clone(db),
                txn,
                Arc::new(plan),
                context,
            )
        })?;
        if let Some(result) = native {
            return Ok(QueryResponse::StreamingResultSet(result));
        }
    }
    let SetOperation {
        op,
        all,
        left,
        right,
        order_by,
        limit,
        offset,
    } = stmt;
    // Streaming `UNION ALL` fast path: an unmodified UNION ALL (no set-op
    // level ORDER BY / LIMIT / OFFSET) needs no dedup, sort, or global view
    // — its output is the left branch's rows followed by the right's. Chain
    // the two branch cursors instead of materialising both into `Vec`s and
    // concatenating. Each branch keeps its own streaming/parallel scan, so
    // a downstream streaming consumer (top-level result, `count(*)`, or the
    // spill-aware derived-table bridge) pulls rows through without ever
    // building the full intermediate row set.
    if matches!(op, SetOp::Union)
        && all
        && order_by.is_empty()
        && limit.is_none()
        && offset.is_none()
    {
        let left = execute_qualified_statement(db, session, left.into_statement(), registry)?;
        let right = execute_qualified_statement(db, session, right.into_statement(), registry)?;
        return chained_union_all(left, right);
    }
    // A branch may run on the streaming path (e.g. an aggregating SELECT that
    // returns a `StreamingResultSet`) — collapse it to a materialised
    // `ResultSet` before combining, mirroring the derived-table branch path
    // (`execute_set_operation_branch`). Without this, a set operation whose
    // sides stream (a `UNION ALL` of GROUP BY queries, as in the TPC-DS
    // Q4/Q11/Q74 CTE bodies) is wrongly rejected with error 1235.
    let left_result =
        execute_qualified_statement(db, session, left.into_statement(), registry)?.materialize()?;
    let right_result = execute_qualified_statement(db, session, right.into_statement(), registry)?
        .materialize()?;
    let QueryResponse::ResultSet(left) = left_result else {
        return Err(error(
            crate::error::catalog::ER_NOT_SUPPORTED_YET,
            "set operation requires SELECT result on the left",
        ));
    };
    let QueryResponse::ResultSet(right) = right_result else {
        return Err(error(
            crate::error::catalog::ER_NOT_SUPPORTED_YET,
            "set operation requires SELECT result on the right",
        ));
    };
    combine_set_operation_results(op, all, left, right, order_by, limit, offset, db.data_dir())
}

/// Combine two already-materialised set-operation branches into the final
/// result: apply `UNION` / `INTERSECT` / `EXCEPT [ALL]` semantics, then the
/// set operation's own `ORDER BY` / `LIMIT` / `OFFSET`. Shared by the
/// session-based [`execute_set_operation_dispatch`] (top-level set operations)
/// and the session-free [`execute_set_operation_rows`] (a set operation in a
/// FROM-clause derived table).
/// Number of fractional digits in a plain decimal text (`"1.20"` → 2).
fn decimal_text_scale(text: &str) -> u8 {
    match text.split_once('.') {
        Some((_, frac)) => frac
            .trim_end_matches(|c: char| !c.is_ascii_digit())
            .len()
            .min(30) as u8,
        None => 0,
    }
}

/// Number of integer-part digits of a numeric value (`123.45` → 3).
fn numeric_int_digits(value: &Value) -> u8 {
    let s = crate::storage::value_to_string(value);
    let s = s.trim_start_matches('-');
    let int_part = s.split('.').next().unwrap_or("0");
    (int_part.len().max(1)).min(65) as u8
}

/// Merge the declared domain of approximate numeric set-operation branches.
/// Runtime values are insufficient here: an empty branch still contributes
/// its FLOAT/DOUBLE(M,D) range to the result and a subsequent CTAS must not
/// clone only the first branch's narrower catalog type.
fn unify_setop_approximate_column_metadata(left: &mut [Column], right: &mut [Column]) {
    use crate::mysql::protocol::{NOT_NULL_FLAG, UNSIGNED_FLAG};
    let is_approximate = |ty: ColumnType| matches!(ty, ColumnType::Float | ColumnType::Double);
    let numeric_spec = |column: &Column| match column.column_type {
        ColumnType::Float | ColumnType::Double => {
            column.source.numeric_precision.zip(column.source.decimals)
        }
        ColumnType::Decimal { precision, scale } => Some((precision, scale)),
        ColumnType::Tiny => Some((3, 0)),
        ColumnType::Short => Some((5, 0)),
        ColumnType::MediumInt => Some((8, 0)),
        ColumnType::Long => Some((10, 0)),
        ColumnType::LongLong => Some((20, 0)),
        ColumnType::Year => Some((4, 0)),
        ColumnType::Bit { bits } => Some((((u32::from(bits) * 30_103) / 100_000 + 1) as u8, 0)),
        _ => None,
    };
    let is_numeric = |ty: ColumnType| {
        is_approximate(ty)
            || matches!(
                ty,
                ColumnType::Decimal { .. }
                    | ColumnType::Tiny
                    | ColumnType::Short
                    | ColumnType::MediumInt
                    | ColumnType::Long
                    | ColumnType::LongLong
                    | ColumnType::Year
                    | ColumnType::Bit { .. }
            )
    };
    for (left, right) in left.iter_mut().zip(right.iter_mut()) {
        if !(is_approximate(left.column_type) || is_approximate(right.column_type))
            || !is_numeric(left.column_type)
            || !is_numeric(right.column_type)
        {
            continue;
        }
        let unified =
            if left.column_type == ColumnType::Float && right.column_type == ColumnType::Float {
                ColumnType::Float
            } else {
                ColumnType::Double
            };
        let merged_spec = match (numeric_spec(left), numeric_spec(right)) {
            (Some((lp, ls)), Some((rp, rs))) => {
                let scale = ls.max(rs);
                let integer_digits = lp.saturating_sub(ls).max(rp.saturating_sub(rs));
                Some((integer_digits.saturating_add(scale), scale))
            }
            _ => None,
        };
        let not_null =
            left.source.flags & NOT_NULL_FLAG != 0 && right.source.flags & NOT_NULL_FLAG != 0;
        let unsigned =
            left.source.flags & UNSIGNED_FLAG != 0 && right.source.flags & UNSIGNED_FLAG != 0;
        for column in [left, right] {
            column.column_type = unified;
            column.source.schema.clear();
            column.source.table.clear();
            column.source.org_table.clear();
            column.source.org_name.clear();
            column.source.wire_type = None;
            column.source.length = None;
            column.source.numeric_precision = merged_spec.map(|spec| spec.0);
            column.source.decimals = merged_spec.map(|spec| spec.1);
            column.source.collation_id = None;
            column.source.flags = (if not_null { NOT_NULL_FLAG } else { 0 })
                | (if unsigned { UNSIGNED_FLAG } else { 0 });
        }
    }
}

pub(crate) fn unify_setop_declared_column_metadata(
    left: &mut [Column],
    right: &mut [Column],
) -> Result<(), QueryError> {
    unify_setop_approximate_column_metadata(left, right);
    for (left_column, right_column) in left.iter_mut().zip(right.iter_mut()) {
        let left_type = crate::engine::expr_type::ExprType::from_column_type(
            left_column.column_type,
            &left_column.source,
        );
        let right_type = crate::engine::expr_type::ExprType::from_column_type(
            right_column.column_type,
            &right_column.source,
        );
        let descriptor = crate::engine::expr_type::checked_branch_types(left_type, right_type)?;
        let wire_type = (left_column.source.wire_type == right_column.source.wire_type)
            .then_some(left_column.source.wire_type)
            .flatten();
        for column in [left_column, right_column] {
            column.source.wire_type = wire_type;
            column.column_type = descriptor.column_type();
            column.source.schema.clear();
            column.source.table.clear();
            column.source.org_table.clear();
            column.source.org_name.clear();
            descriptor.apply_to_source(&mut column.source);
        }
    }
    Ok(())
}

/// Normalize a row payload to the already-resolved UNION domain. Shared by
/// native batches and spill-aware CTE concatenation.
pub(in crate::engine::executor) fn coerce_setop_value(
    value: Value,
    column_type: ColumnType,
    source: &crate::types::ColumnSource,
) -> Result<Value, QueryError> {
    if matches!(value, Value::Null) {
        return Ok(value);
    }
    Ok(match column_type {
        ColumnType::VarString | ColumnType::Blob => {
            if source.collation_id == Some(63) {
                Value::Bytes(crate::engine::expr::value_bytes(&value))
            } else {
                Value::String(crate::engine::expr::string_value(&value))
            }
        }
        ColumnType::Decimal { precision, scale }
            if matches!(
                value,
                Value::Int(_)
                    | Value::UInt(_)
                    | Value::Year(_)
                    | Value::Decimal(_)
                    | Value::DecimalCompact(..)
                    | Value::Decimal128(..)
                    | Value::DecimalWide(_)
            ) =>
        {
            crate::engine::expr::cast_to_decimal(&value, precision, scale)?
        }
        ColumnType::Float | ColumnType::Double
            if matches!(
                value,
                Value::Int(_)
                    | Value::UInt(_)
                    | Value::Year(_)
                    | Value::Float(_)
                    | Value::Double(_)
                    | Value::Decimal(_)
                    | Value::DecimalCompact(..)
                    | Value::Decimal128(..)
                    | Value::DecimalWide(_)
            ) =>
        {
            Value::Double(crate::engine::expr::numeric(&value)?)
        }
        ColumnType::Tiny
        | ColumnType::Short
        | ColumnType::MediumInt
        | ColumnType::Long
        | ColumnType::LongLong
            if matches!(value, Value::Year(_)) =>
        {
            let Value::Year(year) = value else {
                unreachable!()
            };
            Value::Int(if year == 0 { 0 } else { 1900 + i64::from(year) })
        }
        ColumnType::Date | ColumnType::DateTime | ColumnType::Timestamp => {
            crate::engine::expr::coerce_result_to_expr_type(
                value,
                crate::engine::expr_type::ExprType::from_column_type(column_type, source),
            )?
        }
        _ => value,
    })
}

/// MySQL unifies a set-operation column's type across all branches. When a
/// column mixes exact-integer and DECIMAL values (or DECIMALs of differing
/// scale), the result column is DECIMAL and every value is rendered at the
/// common scale — so `SELECT 1 UNION SELECT 2.5` yields `1.0 / 2.5`, not
/// `1 / 2.5` (issue #68). Approximate numeric columns normalize all payloads
/// to DOUBLE before equality keys are constructed.
fn unify_setop_numeric_columns(
    left: &mut ResultSet,
    right: &mut ResultSet,
) -> Result<(), QueryError> {
    use crate::mysql::protocol::{NOT_NULL_FLAG, UNSIGNED_FLAG};

    // Aggregate every branch's declared domain before looking at rows. Empty
    // branches, branch execution order, and cache hits therefore cannot alter
    // the output descriptor. This is the same algebra used by CASE/IF.
    unify_setop_declared_column_metadata(&mut left.columns, &mut right.columns)?;
    let is_integer = |column_type: ColumnType| {
        matches!(
            column_type,
            ColumnType::Tiny
                | ColumnType::Short
                | ColumnType::MediumInt
                | ColumnType::Long
                | ColumnType::LongLong
                | ColumnType::Year
                | ColumnType::Bit { .. }
        )
    };
    let ncols = left.columns.len().min(right.columns.len());
    for i in 0..ncols {
        let left_type = left.columns[i].column_type;
        let right_type = right.columns[i].column_type;
        if matches!(left_type, ColumnType::VarString | ColumnType::Blob) {
            let binary = left.columns[i].source.collation_id == Some(63);
            for row in left.rows.iter_mut().chain(right.rows.iter_mut()) {
                if let Some(value) = row.get_mut(i) {
                    if !matches!(value, Value::Null) {
                        *value = if binary {
                            Value::Bytes(crate::engine::expr::value_bytes(value))
                        } else {
                            Value::String(crate::engine::expr::string_value(value))
                        };
                    }
                }
            }
            continue;
        }
        if is_integer(left_type) && is_integer(right_type) {
            // Set-operation metadata is aggregated from every branch, not
            // inferred from whichever runtime value happens to be first.
            // Pick the narrowest integer family that covers both declared
            // domains. YEAR needs SMALLINT (0..2155), not BIGINT; this is
            // observable through UNION CTAS metadata.
            let unified = if left_type == right_type {
                left_type
            } else {
                let rank = |column_type: ColumnType| match column_type {
                    ColumnType::Tiny => 1,
                    ColumnType::Short | ColumnType::Year => 2,
                    ColumnType::MediumInt => 3,
                    ColumnType::Long | ColumnType::Bit { bits: 1..=32 } => 4,
                    ColumnType::LongLong | ColumnType::Bit { .. } => 5,
                    _ => 5,
                };
                match rank(left_type).max(rank(right_type)) {
                    1 => ColumnType::Tiny,
                    2 => ColumnType::Short,
                    3 => ColumnType::MediumInt,
                    4 => ColumnType::Long,
                    _ => ColumnType::LongLong,
                }
            };
            let unsigned = (left.columns[i].source.flags & UNSIGNED_FLAG != 0
                && left_type != ColumnType::Year)
                || (right.columns[i].source.flags & UNSIGNED_FLAG != 0
                    && right_type != ColumnType::Year)
                || matches!(left_type, ColumnType::Bit { .. })
                || matches!(right_type, ColumnType::Bit { .. });
            for column in [&mut left.columns[i], &mut right.columns[i]] {
                column.column_type = unified;
                // The result no longer has one catalog origin. Retaining the
                // first branch's org_name makes CTAS rediscover that narrower
                // catalog type and discard the aggregate type computed here.
                column.source.schema.clear();
                column.source.table.clear();
                column.source.org_table.clear();
                column.source.org_name.clear();
                column.source.length = None;
                column.source.numeric_precision = None;
                column.source.decimals = None;
                column.source.collation_id = None;
                column.source.flags &= UNSIGNED_FLAG | NOT_NULL_FLAG;
                if unsigned {
                    column.source.flags |= UNSIGNED_FLAG;
                }
            }
            // YEAR uses an internal offset byte (2024 is stored as 124).
            // Once set-operation aggregation changes the declared domain to
            // an integer, normalize the payload too; otherwise text/binary
            // protocols would encode 124 under SMALLINT metadata.
            for rows in [&mut left.rows, &mut right.rows] {
                for row in rows.iter_mut() {
                    if let Some(value) = row.get_mut(i) {
                        if let Value::Year(year) = value {
                            *value = Value::Int(if *year == 0 {
                                0
                            } else {
                                1900 + i64::from(*year)
                            });
                        }
                    }
                }
            }
            continue;
        }
        if matches!(
            left.columns[i].column_type,
            ColumnType::Float | ColumnType::Double
        ) {
            for rows in [&mut left.rows, &mut right.rows] {
                for row in rows {
                    if let Some(value) = row.get_mut(i) {
                        if !matches!(value, Value::Null) {
                            *value = Value::Double(crate::engine::expr::numeric(value)?);
                        }
                    }
                }
            }
            continue;
        }
        let mut has_float = false;
        let declared_decimal = match left.columns[i].column_type {
            ColumnType::Decimal { precision, scale } => Some((precision, scale)),
            _ => None,
        };
        let mut has_decimal = declared_decimal.is_some();
        let mut has_int = false;
        let mut has_other = false;
        let mut max_scale = declared_decimal.map(|(_, scale)| scale).unwrap_or(0);
        let mut max_int_digits = declared_decimal
            .map(|(precision, scale)| precision.saturating_sub(scale).max(1))
            .unwrap_or(1);
        for rows in [&left.rows, &right.rows] {
            for row in rows {
                let Some(v) = row.get(i) else { continue };
                match v {
                    Value::Null => {}
                    Value::Float(_) | Value::Double(_) => has_float = true,
                    Value::Int(_) | Value::UInt(_) | Value::Year(_) => {
                        has_int = true;
                        max_int_digits = max_int_digits.max(numeric_int_digits(v));
                    }
                    Value::DecimalCompact(_, s) => {
                        has_decimal = true;
                        max_scale = max_scale.max(*s);
                        max_int_digits = max_int_digits.max(numeric_int_digits(v));
                    }
                    Value::Decimal(text) => {
                        has_decimal = true;
                        max_scale = max_scale.max(decimal_text_scale(text));
                        max_int_digits = max_int_digits.max(numeric_int_digits(v));
                    }
                    _ => has_other = true,
                }
            }
        }
        // Only promote a pure-numeric column that actually mixes DECIMAL with
        // integer / other DECIMAL scales. Float columns aggregate to DOUBLE
        // (integers already display fine there); nothing to do.
        let _ = has_int;
        if has_float || has_other || !has_decimal {
            continue;
        }
        let precision = (max_int_digits as u16 + max_scale as u16).clamp(1, 65) as u8;
        for rows in [&mut left.rows, &mut right.rows] {
            for row in rows.iter_mut() {
                if let Some(v) = row.get_mut(i) {
                    if matches!(
                        v,
                        Value::Int(_)
                            | Value::UInt(_)
                            | Value::Year(_)
                            | Value::Decimal(_)
                            | Value::DecimalCompact(..)
                    ) {
                        if let Ok(nv) =
                            crate::engine::expr::cast_to_decimal(v, precision, max_scale)
                        {
                            *v = nv;
                        }
                    }
                }
            }
        }
        let ct = ColumnType::Decimal {
            precision,
            scale: max_scale,
        };
        if let Some(c) = left.columns.get_mut(i) {
            c.column_type = ct.clone();
        }
        if let Some(c) = right.columns.get_mut(i) {
            c.column_type = ct;
        }
    }
    Ok(())
}

#[derive(Debug)]
struct SetCountEntry {
    row: Vec<Value>,
    left: u64,
    right: u64,
    first_ordinal: u64,
}

type SetRowKey = crate::engine::expr::InlineKeyBuf;

pub(crate) static SETOP_SPILL_COUNT: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);
pub(crate) static SETOP_SPILL_BYTES_WRITTEN: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);
pub(crate) static SETOP_SPILL_BYTES_READ: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);
pub(crate) static SETOP_SPILL_PARTITIONS: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);
pub(crate) static SETOP_SPILL_FILE_OPENS: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);
pub(crate) static SETOP_SPILL_REPARTITIONS: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);
pub(crate) static SETOP_SPILL_MAX_DEPTH: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);
pub(crate) static SETOP_SPILL_MERGE_PASSES: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);

const SETOP_SPILL_FANOUT: usize = 16;
const SETOP_SPILL_MAX_REPARTITION_DEPTH: u32 = 4;

fn set_row_capacity_bytes(row: &Vec<Value>) -> usize {
    crate::engine::executor::mem_budget::estimate_row_bytes(row).saturating_add(
        row.capacity()
            .saturating_sub(row.len())
            .saturating_mul(std::mem::size_of::<Value>()),
    )
}

#[derive(Default)]
struct SetOpReplayMetrics {
    bytes_written: u64,
    bytes_read: u64,
    partitions: u64,
    file_opens: u64,
    repartitions: u64,
    max_depth: u64,
    merge_passes: u64,
}

fn setop_partition_for(collations: &[Option<u16>], row: &[Value], depth: u32) -> usize {
    use std::hash::{Hash, Hasher};
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    depth.hash(&mut hasher);
    set_row_key(collations, row).hash(&mut hasher);
    (hasher.finish() as usize) & (SETOP_SPILL_FANOUT - 1)
}

struct SetOpMergeItem {
    ordinal: u64,
    source: usize,
    row: Vec<Value>,
    charged_bytes: usize,
}

impl PartialEq for SetOpMergeItem {
    fn eq(&self, other: &Self) -> bool {
        self.ordinal == other.ordinal && self.source == other.source
    }
}

impl Eq for SetOpMergeItem {}

impl PartialOrd for SetOpMergeItem {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for SetOpMergeItem {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        // BinaryHeap is a max heap; reverse ordinal/source for a stable
        // left-first minimum heap.
        other
            .ordinal
            .cmp(&self.ordinal)
            .then_with(|| other.source.cmp(&self.source))
    }
}

fn setop_u64(values: &[Value], index: usize) -> Result<u64, QueryError> {
    match values.get(index) {
        Some(Value::UInt(value)) => Ok(*value),
        _ => {
            Err(crate::error::catalog::ER_UNKNOWN_ERROR
                .error("invalid set-operation spill counter"))
        }
    }
}

fn read_setop_compact_record(
    reader: &mut crate::engine::executor::select::grace::ValueRowReader,
) -> Result<Option<(u64, Vec<Value>)>, QueryError> {
    let Some(row) = reader.next()? else {
        return Ok(None);
    };
    let ordinal = setop_u64(&row, 0)?;
    if setop_u64(&row, 1)? == 0 {
        return Err(crate::error::catalog::ER_UNKNOWN_ERROR
            .error("invalid zero set-operation output count"));
    }
    Ok(Some((ordinal, row)))
}

fn merge_setop_runs(
    ncols: usize,
    dir: &crate::engine::executor::select::grace::SpillTmpDir,
    mut runs: Vec<std::path::PathBuf>,
    work_handle: &crate::engine::executor::work_area::WorkAreaHandle,
    metrics: &mut SetOpReplayMetrics,
) -> Result<Option<std::path::PathBuf>, QueryError> {
    const FAN_IN: usize = 16;
    let mut generation = 0usize;
    while runs.len() > 1 {
        crate::engine::cancel::check_cancelled()?;
        let mut next_runs = Vec::with_capacity(runs.len().div_ceil(FAN_IN));
        for (group_index, group) in runs.chunks(FAN_IN).enumerate() {
            if group.len() == 1 {
                next_runs.push(group[0].clone());
                continue;
            }
            let output_path = dir
                .path()
                .join(format!("setmerge-{generation}-{group_index}"));
            let mut writer = crate::engine::executor::select::grace::ValueRowWriter::create(
                &output_path,
                ncols.saturating_add(2),
                dir.charger(),
            )?;
            metrics.file_opens = metrics.file_opens.saturating_add(1);
            let mut readers = Vec::with_capacity(group.len());
            for path in group {
                let reader = crate::engine::executor::select::grace::ValueRowReader::open(
                    path,
                    dir.charger(),
                )?
                .expect("set-op merge run exists");
                readers.push(reader);
                metrics.file_opens = metrics.file_opens.saturating_add(1);
                metrics.bytes_read = metrics
                    .bytes_read
                    .saturating_add(path.metadata().map(|metadata| metadata.len()).unwrap_or(0));
            }
            let mut heap = std::collections::BinaryHeap::with_capacity(readers.len());
            let mut heap_charge = crate::engine::executor::mem_budget::ScopedCharge::new(0);
            for (source, reader) in readers.iter_mut().enumerate() {
                if let Some((ordinal, row)) = read_setop_compact_record(reader)? {
                    let bytes = set_row_capacity_bytes(&row)
                        .saturating_add(std::mem::size_of::<SetOpMergeItem>());
                    if matches!(
                        work_handle.try_charge(bytes),
                        crate::engine::executor::work_area::SpillHint::Spill
                    ) {
                        let _ = work_handle.try_borrow_for_pipeline_drain();
                    }
                    if let Err(error) = heap_charge.add_force(bytes) {
                        work_handle.release(bytes);
                        return Err(error);
                    }
                    heap.push(SetOpMergeItem {
                        ordinal,
                        source,
                        row,
                        charged_bytes: bytes,
                    });
                }
            }
            while let Some(item) = heap.pop() {
                crate::engine::cancel::check_cancelled()?;
                writer.append(&item.row)?;
                heap_charge.release_bytes(item.charged_bytes);
                work_handle.release(item.charged_bytes);
                if let Some((ordinal, row)) = read_setop_compact_record(&mut readers[item.source])?
                {
                    let bytes = set_row_capacity_bytes(&row)
                        .saturating_add(std::mem::size_of::<SetOpMergeItem>());
                    if matches!(
                        work_handle.try_charge(bytes),
                        crate::engine::executor::work_area::SpillHint::Spill
                    ) {
                        let _ = work_handle.try_borrow_for_pipeline_drain();
                    }
                    if let Err(error) = heap_charge.add_force(bytes) {
                        work_handle.release(bytes);
                        return Err(error);
                    }
                    heap.push(SetOpMergeItem {
                        ordinal,
                        source: item.source,
                        row,
                        charged_bytes: bytes,
                    });
                }
            }
            drop(readers);
            metrics.bytes_written = metrics.bytes_written.saturating_add(writer.finish()?);
            for path in group {
                crate::engine::executor::spill_quota::remove_spill_file(path, &dir.charger())?;
            }
            next_runs.push(output_path);
        }
        runs = next_runs;
        generation = generation.saturating_add(1);
        metrics.merge_passes = metrics.merge_passes.saturating_add(1);
    }
    Ok(runs.pop())
}

#[allow(clippy::too_many_arguments)]
fn reduce_setop_spill_partition(
    collations: &[Option<u16>],
    op: SetOp,
    all: bool,
    ncols: usize,
    path: &std::path::Path,
    partition_tag: &str,
    depth: u32,
    replay_cap: usize,
    dir: &crate::engine::executor::select::grace::SpillTmpDir,
    output_runs: &mut Vec<std::path::PathBuf>,
    next_run: &mut usize,
    work_handle: &crate::engine::executor::work_area::WorkAreaHandle,
    memory_charge: &mut crate::engine::executor::mem_budget::ScopedCharge,
    peak_state_bytes: &mut usize,
    metrics: &mut SetOpReplayMetrics,
) -> Result<(), QueryError> {
    crate::engine::cancel::check_cancelled()?;
    let Some(mut reader) =
        crate::engine::executor::select::grace::ValueRowReader::open(path, dir.charger())?
    else {
        return Ok(());
    };
    if reader.column_count() != ncols.saturating_add(3) {
        return Err(
            crate::error::catalog::ER_UNKNOWN_ERROR.error("invalid set-operation partition width")
        );
    }
    metrics.file_opens = metrics.file_opens.saturating_add(1);
    metrics.bytes_read = metrics
        .bytes_read
        .saturating_add(path.metadata().map(|metadata| metadata.len()).unwrap_or(0));
    metrics.max_depth = metrics.max_depth.max(depth as u64);

    if depth >= SETOP_SPILL_MAX_REPARTITION_DEPTH {
        return external_reduce::reduce(
            reader,
            path,
            collations,
            op,
            all,
            ncols,
            replay_cap,
            dir,
            output_runs,
            next_run,
            memory_charge,
            peak_state_bytes,
            metrics,
        );
    }

    let mut state = hashbrown::HashMap::<SetRowKey, SetCountEntry, ahash::RandomState>::with_hasher(
        ahash::RandomState::new(),
    );
    let mut state_bytes = 0usize;
    let cap = replay_cap;

    while let Some(mut tagged) = reader.next()? {
        crate::engine::cancel::check_cancelled()?;
        let left = setop_u64(&tagged, 0)?;
        let right = setop_u64(&tagged, 1)?;
        let ordinal = setop_u64(&tagged, 2)?;
        tagged.drain(..tagged.len().min(3));
        let key = set_row_key(collations, &tagged);
        if let Some(entry) = state.get_mut(&key) {
            entry.left = entry.left.saturating_add(left);
            entry.right = entry.right.saturating_add(right);
            entry.first_ordinal = entry.first_ordinal.min(ordinal);
            continue;
        }
        if left == 0 && matches!(op, SetOp::Intersect | SetOp::Except) {
            // INTERSECT/EXCEPT can only emit keys originating on the left.
            // Input files preserve left-before-right order, including after
            // recursive repartition, so a right-only key never needs a row or
            // hash entry.
            continue;
        }
        let key_heap_bytes = key.spilled().then_some(key.capacity()).unwrap_or(0);
        let bucket_bytes = if state.len() == state.capacity() {
            state
                .capacity()
                .max(2)
                .saturating_mul(std::mem::size_of::<(SetRowKey, SetCountEntry)>())
        } else {
            0
        };
        let grow_bytes = set_row_capacity_bytes(&tagged)
            .saturating_add(key_heap_bytes)
            .saturating_add(96)
            .saturating_add(bucket_bytes);
        if state_bytes.saturating_add(grow_bytes) > cap {
            let child_prefix = format!("setop-r{}-{partition_tag}", depth + 1);
            let mut writers = crate::engine::executor::select::grace::LazyValueRowWriterPool::new(
                dir.path(),
                &child_prefix,
                SETOP_SPILL_FANOUT,
                ncols.saturating_add(3),
                dir.charger(),
                SETOP_SPILL_FANOUT,
            );
            let append =
                |writers: &mut crate::engine::executor::select::grace::LazyValueRowWriterPool,
                 row: Vec<Value>,
                 left: u64,
                 right: u64,
                 ordinal: u64|
                 -> Result<(), QueryError> {
                    let partition = setop_partition_for(collations, &row, depth + 1);
                    let mut record = Vec::with_capacity(ncols.saturating_add(3));
                    record.push(Value::UInt(left));
                    record.push(Value::UInt(right));
                    record.push(Value::UInt(ordinal));
                    record.extend(row);
                    writers.append(partition, &record)
                };
            for entry in state.drain().map(|(_, entry)| entry) {
                append(
                    &mut writers,
                    entry.row,
                    entry.left,
                    entry.right,
                    entry.first_ordinal,
                )?;
            }
            append(&mut writers, tagged, left, right, ordinal)?;
            while let Some(mut remaining) = reader.next()? {
                crate::engine::cancel::check_cancelled()?;
                let left = setop_u64(&remaining, 0)?;
                let right = setop_u64(&remaining, 1)?;
                let ordinal = setop_u64(&remaining, 2)?;
                remaining.drain(..remaining.len().min(3));
                append(&mut writers, remaining, left, right, ordinal)?;
            }
            drop(reader);
            metrics.file_opens = metrics.file_opens.saturating_add(writers.opens());
            metrics.bytes_written = metrics.bytes_written.saturating_add(writers.finish()?);
            metrics.partitions = metrics.partitions.saturating_add(SETOP_SPILL_FANOUT as u64);
            metrics.repartitions = metrics.repartitions.saturating_add(1);
            drop(state);
            memory_charge.release_bytes(state_bytes);
            work_handle.release(state_bytes);
            crate::engine::executor::spill_quota::remove_spill_file(path, &dir.charger())?;
            for child in 0..SETOP_SPILL_FANOUT {
                let child_path = dir.path().join(format!("{child_prefix}-{child}"));
                reduce_setop_spill_partition(
                    collations,
                    op,
                    all,
                    ncols,
                    &child_path,
                    &format!("{partition_tag}-{child}"),
                    depth + 1,
                    replay_cap,
                    dir,
                    output_runs,
                    next_run,
                    work_handle,
                    memory_charge,
                    peak_state_bytes,
                    metrics,
                )?;
            }
            return Ok(());
        }
        if matches!(
            work_handle.try_charge(grow_bytes),
            crate::engine::executor::work_area::SpillHint::Spill
        ) {
            let _ = work_handle.try_borrow_for_pipeline_drain();
        }
        if let Err(error) = memory_charge.add_force(grow_bytes) {
            work_handle.release(grow_bytes);
            return Err(error);
        }
        state_bytes = state_bytes.saturating_add(grow_bytes);
        *peak_state_bytes = (*peak_state_bytes).max(state_bytes);
        state.insert(
            key,
            SetCountEntry {
                row: tagged,
                left,
                right,
                first_ordinal: ordinal,
            },
        );
    }

    drop(reader);
    crate::engine::executor::spill_quota::remove_spill_file(path, &dir.charger())?;
    let entries = compact_set_count_state(op, all, state);
    if !entries.is_empty() {
        let run_path = dir.path().join(format!("setrun-{}", *next_run));
        *next_run = (*next_run).saturating_add(1);
        let mut writer = crate::engine::executor::select::grace::ValueRowWriter::create(
            &run_path,
            ncols.saturating_add(2),
            dir.charger(),
        )?;
        metrics.file_opens = metrics.file_opens.saturating_add(1);
        for (ordinal, repeats, row) in entries {
            crate::engine::cancel::check_cancelled()?;
            let mut tagged = Vec::with_capacity(ncols.saturating_add(2));
            tagged.push(Value::UInt(ordinal));
            tagged.push(Value::UInt(repeats));
            tagged.extend(row);
            writer.append(&tagged)?;
        }
        metrics.bytes_written = metrics.bytes_written.saturating_add(writer.finish()?);
        output_runs.push(run_path);
    }
    memory_charge.release_bytes(state_bytes);
    work_handle.release(state_bytes);
    Ok(())
}

fn set_row_key(collations: &[Option<u16>], row: &[Value]) -> SetRowKey {
    let mut key = SetRowKey::new();
    for (index, value) in row.iter().enumerate() {
        let text = match value {
            Value::String(text) => Some(text.as_str()),
            Value::SmallString(bytes) => std::str::from_utf8(bytes.as_slice()).ok(),
            _ => None,
        };
        if let (Some(text), Some(Some(collation))) = (text, collations.get(index)) {
            key.push(3);
            let length_offset = key.len();
            key.extend_from_slice(&0u32.to_le_bytes());
            crate::charset::with_collation_key_by_id(*collation, text, |bytes| {
                key.extend_from_slice(bytes)
            });
            let length = (key.len() - length_offset - 4) as u32;
            key[length_offset..length_offset + 4].copy_from_slice(&length.to_le_bytes());
        } else {
            crate::engine::expr::write_value_inline_key(value, &mut key);
        }
    }
    key
}

fn set_count_repeats(op: SetOp, all: bool, entry: &SetCountEntry) -> u64 {
    match (op, all) {
        (SetOp::Union, false) => u64::from(entry.left > 0 || entry.right > 0),
        (SetOp::Intersect, false) => u64::from(entry.left > 0 && entry.right > 0),
        (SetOp::Intersect, true) => entry.left.min(entry.right),
        (SetOp::Except, false) => u64::from(entry.left > 0 && entry.right == 0),
        (SetOp::Except, true) => entry.left.saturating_sub(entry.right),
        (SetOp::Union, true) => entry.left.saturating_add(entry.right),
    }
}

fn compact_set_count_state(
    op: SetOp,
    all: bool,
    state: hashbrown::HashMap<SetRowKey, SetCountEntry, ahash::RandomState>,
) -> Vec<(u64, u64, Vec<Value>)> {
    let mut entries = state
        .into_values()
        .filter_map(|entry| {
            let repeats = set_count_repeats(op, all, &entry);
            (repeats > 0).then_some((entry.first_ordinal, repeats, entry.row))
        })
        .collect::<Vec<_>>();
    entries.sort_unstable_by_key(|(ordinal, _, _)| *ordinal);
    entries
}

struct SpilledSetOutput {
    dir: crate::engine::executor::select::grace::SpillTmpDir,
    output_path: Option<std::path::PathBuf>,
    reader: Option<crate::engine::executor::select::grace::ValueRowReader>,
    output_spill_bytes: u64,
    output_read_recorded: bool,
}

enum IncrementalSetOutputInner {
    InMemory(std::vec::IntoIter<(u64, u64, Vec<Value>)>),
    Spilled(SpilledSetOutput),
}

/// Pull-based result of a blocking set operation. Multiplicity stays compact
/// until `next_row`, so `INTERSECT ALL`/`EXCEPT ALL` never allocate N clones of
/// the same row. A spilled hash-count state produces range-partitioned output
/// and re-opens only one ordered range at a time.
pub(crate) struct IncrementalSetOutput {
    inner: IncrementalSetOutputInner,
    repeated: Option<(Vec<Value>, u64)>,
    // The in-memory result owns the hash-state reservation until its final
    // representative row has been consumed.  This is deliberately
    // conservative: compacting drops keys/buckets but never makes the
    // reservation disappear before the surviving rows do.
    _memory_charge: Option<crate::engine::executor::mem_budget::ScopedCharge>,
    _work_area: Option<crate::engine::executor::work_area::WorkAreaHandle>,
}

impl IncrementalSetOutput {
    fn in_memory(
        entries: Vec<(u64, u64, Vec<Value>)>,
        memory_charge: Option<crate::engine::executor::mem_budget::ScopedCharge>,
        work_area: Option<crate::engine::executor::work_area::WorkAreaHandle>,
    ) -> Self {
        Self {
            inner: IncrementalSetOutputInner::InMemory(entries.into_iter()),
            repeated: None,
            _memory_charge: memory_charge,
            _work_area: work_area,
        }
    }

    fn next_compact(&mut self) -> Result<Option<(u64, u64, Vec<Value>)>, QueryError> {
        match &mut self.inner {
            IncrementalSetOutputInner::InMemory(entries) => Ok(entries.next()),
            IncrementalSetOutputInner::Spilled(output) => {
                if output.reader.is_none() {
                    let Some(path) = output.output_path.as_ref() else {
                        return Ok(None);
                    };
                    output.reader = crate::engine::executor::select::grace::ValueRowReader::open(
                        path,
                        output.dir.charger(),
                    )?;
                    if output.reader.is_some() {
                        SETOP_SPILL_FILE_OPENS.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    }
                }
                let Some(reader) = output.reader.as_mut() else {
                    return Ok(None);
                };
                let Some(mut tagged) = reader.next()? else {
                    if !output.output_read_recorded {
                        SETOP_SPILL_BYTES_READ.fetch_add(
                            output.output_spill_bytes,
                            std::sync::atomic::Ordering::Relaxed,
                        );
                        output.output_read_recorded = true;
                    }
                    return Ok(None);
                };
                let ordinal = setop_u64(&tagged, 0)?;
                let repeats = setop_u64(&tagged, 1)?;
                if repeats == 0 {
                    return Err(crate::error::catalog::ER_UNKNOWN_ERROR
                        .error("invalid zero set-operation output count"));
                }
                tagged.drain(..2);
                Ok(Some((ordinal, repeats, tagged)))
            }
        }
    }

    pub(crate) fn next_row(&mut self) -> Result<Option<Vec<Value>>, QueryError> {
        crate::engine::cancel::check_cancelled()?;
        if let Some((row, remaining)) = self.repeated.as_mut() {
            if *remaining > 0 {
                *remaining -= 1;
                return Ok(Some(row.clone()));
            }
        }
        self.repeated = None;
        let Some((_, repeats, row)) = self.next_compact()? else {
            return Ok(None);
        };
        if repeats > 1 {
            self.repeated = Some((row.clone(), repeats - 1));
        }
        Ok(Some(row))
    }
}

/// Incremental counterpart of [`combine_set_rows_spillable`]. Producers are
/// pulled one row at a time, so a streaming branch is never first collapsed
/// into `Vec<Vec<Value>>`. Once the bounded in-memory count table reaches its
/// work area, compact `(left_count,right_count,ordinal,row)` records are radix
/// partitioned and each partition is reduced independently.
pub(crate) fn combine_set_sources_spillable<L, R>(
    collations: &[Option<u16>],
    op: SetOp,
    all: bool,
    ncols: usize,
    data_dir: &std::path::Path,
    mut next_left: L,
    mut next_right: R,
) -> Result<IncrementalSetOutput, QueryError>
where
    L: FnMut() -> Result<Option<Vec<Value>>, QueryError>,
    R: FnMut() -> Result<Option<Vec<Value>>, QueryError>,
{
    crate::engine::cancel::check_cancelled()?;
    if op == SetOp::Union && all {
        let mut output = Vec::new();
        let mut ordinal = 0u64;
        while let Some(row) = next_left()? {
            output.push((ordinal, 1, row));
            ordinal = ordinal.saturating_add(1);
        }
        while let Some(row) = next_right()? {
            output.push((ordinal, 1, row));
            ordinal = ordinal.saturating_add(1);
        }
        return Ok(IncrementalSetOutput::in_memory(output, None, None));
    }

    use std::hash::{Hash, Hasher};
    struct SpillState {
        dir: crate::engine::executor::select::grace::SpillTmpDir,
        writers: crate::engine::executor::select::grace::LazyValueRowWriterPool,
        partitions: usize,
    }

    let query_limit = crate::engine::executor::mem_budget::current_budget()
        .map(|budget| budget.limit())
        .unwrap_or(64 << 20)
        .max(1 << 20);
    let work_handle = crate::engine::executor::work_area::WorkAreaHandle::register();
    let mut memory_charge = crate::engine::executor::mem_budget::ScopedCharge::new(0);
    let partitions = SETOP_SPILL_FANOUT;
    let partition_for = |row: &[Value]| {
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        set_row_key(collations, row).hash(&mut hasher);
        (hasher.finish() as usize) & (partitions - 1)
    };
    // Set operators hash every projected value of every input row.  SipHash's
    // DOS-resistant rounds are a poor fit for this trusted, statement-local
    // state and used to dominate wide UNION DISTINCT inputs.  Keep the same
    // canonical equality while using the executor's compact inline-key
    // encoding and standard fast hasher, as hash aggregate already does.
    let mut state = hashbrown::HashMap::<SetRowKey, SetCountEntry, ahash::RandomState>::with_hasher(
        ahash::RandomState::new(),
    );
    let mut state_bytes = 0usize;
    let mut peak_state_bytes = 0usize;
    let mut spill: Option<SpillState> = None;

    let append_spill = |spill: &mut SpillState,
                        row: Vec<Value>,
                        left: u64,
                        right: u64,
                        ordinal: u64|
     -> Result<(), QueryError> {
        let partition = partition_for(&row);
        let mut tagged = Vec::with_capacity(ncols.saturating_add(3));
        tagged.push(Value::UInt(left));
        tagged.push(Value::UInt(right));
        tagged.push(Value::UInt(ordinal));
        tagged.extend(row);
        spill.writers.append(partition, &tagged)
    };

    let mut push = |row: Vec<Value>, left: u64, right: u64, ordinal: u64| {
        crate::engine::cancel::check_cancelled()?;
        if let Some(spill) = spill.as_mut() {
            return append_spill(spill, row, left, right, ordinal);
        }
        let key = set_row_key(collations, &row);
        if let Some(entry) = state.get_mut(&key) {
            entry.left = entry.left.saturating_add(left);
            entry.right = entry.right.saturating_add(right);
            entry.first_ordinal = entry.first_ordinal.min(ordinal);
            return Ok(());
        }
        if spill.is_none() && left == 0 && matches!(op, SetOp::Intersect | SetOp::Except) {
            return Ok(());
        }
        // Inline bytes live inside the hash-table entry.  Only a spilled
        // SmallVec owns a separate allocation; charge that allocation by
        // capacity, never by logical length.
        let key_heap_bytes = key.spilled().then_some(key.capacity()).unwrap_or(0);
        let entry_bytes = set_row_capacity_bytes(&row)
            .saturating_add(key_heap_bytes)
            .saturating_add(96);
        // Account for the next hash-table growth before asking either
        // arbiter.  hashbrown's exact load factor is private; twice the old
        // capacity is a conservative upper bound for the next reserve.
        let bucket_bytes = if state.len() == state.capacity() {
            state
                .capacity()
                .max(2)
                .saturating_mul(std::mem::size_of::<(SetRowKey, SetCountEntry)>())
        } else {
            0
        };
        let grow_bytes = entry_bytes.saturating_add(bucket_bytes);
        let mut work_hint = work_handle.try_charge(grow_bytes);
        if matches!(
            work_hint,
            crate::engine::executor::work_area::SpillHint::Spill
        ) {
            // SetOp consumes its branches serially. A producer's transient
            // work area can overlap the already-built SetOp state, but these
            // are dependent pipeline stages rather than independent queries.
            // Borrow idle executor-pool headroom before choosing a needless
            // spill; the query charge and unified governor remain unchanged.
            work_hint = work_handle.try_borrow_for_pipeline_drain();
        }
        let query_granted = memory_charge.add(grow_bytes).is_ok();
        if matches!(
            work_hint,
            crate::engine::executor::work_area::SpillHint::Continue
        ) && query_granted
        {
            state_bytes = state_bytes.saturating_add(grow_bytes);
            peak_state_bytes = peak_state_bytes.max(state_bytes);
            state.insert(
                key,
                SetCountEntry {
                    row,
                    left,
                    right,
                    first_ordinal: ordinal,
                },
            );
            return Ok(());
        }
        if query_granted {
            memory_charge.release_bytes(grow_bytes);
        }
        work_handle.release(grow_bytes);
        let dir =
            crate::engine::executor::select::grace::SpillTmpDir::create_kind(data_dir, "setop")?;
        let writers = crate::engine::executor::select::grace::LazyValueRowWriterPool::new(
            dir.path(),
            "setop",
            partitions,
            ncols.saturating_add(3),
            dir.charger(),
            16,
        );
        let mut new_spill = SpillState {
            dir,
            writers,
            partitions,
        };
        for entry in state.drain().map(|(_, entry)| entry) {
            append_spill(
                &mut new_spill,
                entry.row,
                entry.left,
                entry.right,
                entry.first_ordinal,
            )?;
        }
        // `drain` retains hashbrown's bucket allocation. Drop the old table
        // before returning its reservation to either governor.
        state = hashbrown::HashMap::with_hasher(ahash::RandomState::new());
        memory_charge.release_bytes(state_bytes);
        work_handle.release(state_bytes);
        state_bytes = 0;
        append_spill(&mut new_spill, row, left, right, ordinal)?;
        spill = Some(new_spill);
        Ok(())
    };

    let mut left_rows = 0u64;
    while let Some(row) = next_left()? {
        push(row, 1, 0, left_rows)?;
        left_rows = left_rows.saturating_add(1);
    }
    let mut right_rows = 0u64;
    while let Some(row) = next_right()? {
        push(row, 0, 1, left_rows.saturating_add(right_rows))?;
        right_rows = right_rows.saturating_add(1);
    }
    let Some(spill) = spill else {
        return Ok(IncrementalSetOutput::in_memory(
            compact_set_count_state(op, all, state),
            Some(memory_charge),
            Some(work_handle),
        ));
    };
    let SpillState {
        dir,
        writers,
        partitions,
    } = spill;
    let input_writer_opens = writers.opens();
    let input_spill_bytes = writers.finish()?;
    drop(state);
    let mut output_runs = Vec::new();
    let mut next_run = 0usize;
    let replay_cap = (query_limit / 2).max(1 << 20);
    let mut replay_metrics = SetOpReplayMetrics {
        partitions: partitions as u64,
        ..SetOpReplayMetrics::default()
    };
    for partition in 0..partitions {
        let path = dir.path().join(format!("setop-{partition}"));
        reduce_setop_spill_partition(
            collations,
            op,
            all,
            ncols,
            &path,
            &partition.to_string(),
            0,
            replay_cap,
            &dir,
            &mut output_runs,
            &mut next_run,
            &work_handle,
            &mut memory_charge,
            &mut peak_state_bytes,
            &mut replay_metrics,
        )?;
    }
    let output_path =
        merge_setop_runs(ncols, &dir, output_runs, &work_handle, &mut replay_metrics)?;
    let output_spill_bytes = output_path
        .as_ref()
        .and_then(|path| path.metadata().ok())
        .map(|metadata| metadata.len())
        .unwrap_or(0);
    let total_spill_bytes = input_spill_bytes.saturating_add(replay_metrics.bytes_written);
    use std::sync::atomic::Ordering as AtomicOrdering;
    SETOP_SPILL_COUNT.fetch_add(1, AtomicOrdering::Relaxed);
    SETOP_SPILL_BYTES_WRITTEN.fetch_add(total_spill_bytes, AtomicOrdering::Relaxed);
    // The complete input spill was replayed above. Output reads happen lazily,
    // so account those when the output object is consumed in a later change;
    // this counter remains an exact lower bound meanwhile.
    SETOP_SPILL_BYTES_READ.fetch_add(replay_metrics.bytes_read, AtomicOrdering::Relaxed);
    SETOP_SPILL_PARTITIONS.fetch_add(replay_metrics.partitions, AtomicOrdering::Relaxed);
    SETOP_SPILL_REPARTITIONS.fetch_add(replay_metrics.repartitions, AtomicOrdering::Relaxed);
    SETOP_SPILL_MAX_DEPTH.fetch_max(replay_metrics.max_depth, AtomicOrdering::Relaxed);
    SETOP_SPILL_MERGE_PASSES.fetch_add(replay_metrics.merge_passes, AtomicOrdering::Relaxed);
    SETOP_SPILL_FILE_OPENS.fetch_add(
        input_writer_opens.saturating_add(replay_metrics.file_opens),
        AtomicOrdering::Relaxed,
    );
    crate::engine::executor::select::analyze_stats::add_pending_setop_spill(
        total_spill_bytes,
        replay_metrics.partitions,
        input_writer_opens.saturating_add(replay_metrics.file_opens),
        peak_state_bytes as u64,
        replay_metrics.repartitions,
        replay_metrics.max_depth,
        replay_metrics.merge_passes,
    );
    Ok(IncrementalSetOutput {
        inner: IncrementalSetOutputInner::Spilled(SpilledSetOutput {
            dir,
            output_path,
            reader: None,
            output_spill_bytes,
            output_read_recorded: false,
        }),
        repeated: None,
        _memory_charge: Some(memory_charge),
        _work_area: Some(work_handle),
    })
}

/// Compatibility collection over the same bounded replay used by native
/// set-operation streams. The returned owner spans the caller's row/schema
/// conversion; the final result adapter remains responsible for its handoff.
pub(crate) fn combine_set_rows_spillable_charged(
    collations: &[Option<u16>],
    op: SetOp,
    all: bool,
    left: Vec<Vec<Value>>,
    right: Vec<Vec<Value>>,
    ncols: usize,
    data_dir: &std::path::Path,
) -> Result<
    (
        Vec<Vec<Value>>,
        crate::engine::executor::mem_budget::ScopedCharge,
    ),
    QueryError,
> {
    use crate::engine::executor::{mem_budget, memory_footprint::AllocatedBytes};
    struct Inputs {
        left: Vec<Vec<Value>>,
        right: Vec<Vec<Value>>,
        charge: mem_budget::ScopedCharge,
    }
    struct Output {
        rows: Vec<Vec<Value>>,
        charge: mem_budget::ScopedCharge,
    }
    crate::engine::cancel::check_cancelled()?;
    let mut inputs = Inputs {
        left,
        right,
        charge: mem_budget::ScopedCharge::new(0),
    };
    inputs.charge.add_force(
        inputs
            .left
            .allocated_bytes()
            .saturating_add(inputs.right.allocated_bytes()),
    )?;
    if op == SetOp::Union && all {
        mem_budget::try_reserve_vec_hard(
            &mut inputs.left,
            inputs.right.len(),
            &mut inputs.charge,
            "materialized UNION ALL rows",
        )?;
        inputs.left.append(&mut inputs.right);
        drop(std::mem::take(&mut inputs.right));
        let rows = std::mem::take(&mut inputs.left);
        let actual = rows.allocated_bytes();
        if actual > inputs.charge.charged() {
            inputs.charge.add_force(actual - inputs.charge.charged())?;
        } else {
            inputs
                .charge
                .release_bytes(inputs.charge.charged() - actual);
        }
        return Ok((rows, inputs.charge));
    }
    // The codec retains the original payload. A doubled capacity bound plus
    // transport slots covers one decoded row before its allocation, including
    // the hidden counts/ordinal stripped by the compatibility reader.
    let row_bound = inputs
        .left
        .iter()
        .chain(&inputs.right)
        .map(AllocatedBytes::allocated_bytes)
        .max()
        .unwrap_or(0)
        .saturating_mul(2)
        .saturating_add(256);
    let mut left = std::mem::take(&mut inputs.left).into_iter();
    let mut right = std::mem::take(&mut inputs.right).into_iter();
    let mut combined = combine_set_sources_spillable(
        collations,
        op,
        all,
        ncols,
        data_dir,
        || Ok(left.next()),
        || Ok(right.next()),
    )?;
    drop(left);
    drop(right);
    // All input allocations have been consumed. Internal replay retains its
    // own owner; output rows acquire separate reservations before decoding.
    inputs.charge.release_bytes(inputs.charge.charged());
    let mut output = Output {
        rows: Vec::new(),
        charge: inputs.charge.sibling(),
    };
    loop {
        let mut charge = output.charge.sibling();
        charge.add_force(row_bound)?;
        let Some(values) = combined.next_row()? else {
            break;
        };
        let mut owned = crate::engine::executor::row_spool::OwnedRow { values, charge };
        let actual = owned.values.allocated_bytes();
        if actual > owned.charge.charged() {
            return Err(crate::error::catalog::ER_UNKNOWN_ERROR
                .error("set-operation decoded row exceeded its admitted input bound"));
        }
        owned.charge.release_bytes(owned.charge.charged() - actual);
        mem_budget::try_reserve_vec_hard(
            &mut output.rows,
            1,
            &mut output.charge,
            "materialized set-operation output",
        )?;
        output.charge.absorb(&mut owned.charge);
        output.rows.push(owned.values);
    }
    Ok((output.rows, output.charge))
}

#[cfg(test)]
fn combine_set_rows_spillable(
    collations: &[Option<u16>],
    op: SetOp,
    all: bool,
    left: Vec<Vec<Value>>,
    right: Vec<Vec<Value>>,
    ncols: usize,
    data_dir: &std::path::Path,
) -> Result<Vec<Vec<Value>>, QueryError> {
    combine_set_rows_spillable_charged(collations, op, all, left, right, ncols, data_dir)
        .map(|(rows, _charge)| rows)
}

#[cfg(test)]
mod set_count_spill_tests {
    #[test]
    fn exec_audit_legacy_union_numeric_keys() {
        let make = |column_type, value| ResultSet {
            columns: vec![Column {
                name: "v".into(),
                column_type,
                source: Default::default(),
            }],
            rows: vec![vec![value]],
        };
        let mut left = make(
            ColumnType::Decimal {
                precision: 3,
                scale: 1,
            },
            Value::Decimal("1.5".into()),
        );
        let mut right = make(ColumnType::Double, Value::Double(1.5));
        unify_setop_numeric_columns(&mut left, &mut right).unwrap();
        assert_eq!(
            set_row_key(&[], &left.rows[0]),
            set_row_key(&[], &right.rows[0])
        );
    }

    use super::*;
    use std::sync::Arc;

    fn wide_row(value: usize) -> Vec<Value> {
        vec![Value::String(format!("{value:05}-{}", "x".repeat(400)))]
    }

    #[test]
    fn depth_ceiling_set_partition_uses_bounded_external_reduction() {
        use crate::engine::executor::{
            mem_budget,
            select::grace::{SpillTmpDir, ValueRowReader, ValueRowWriter},
        };
        let root = tempfile::tempdir_in("/tmp/mydb").unwrap();
        let budget = Arc::new(mem_budget::MemBudget::new(1024 * 1024));
        mem_budget::with_query_budget(budget.clone(), || {
            let dir = SpillTmpDir::create_kind(root.path(), "setop").unwrap();
            let path = dir.path().join("deep-partition");
            let mut writer = ValueRowWriter::create(&path, 4, dir.charger()).unwrap();
            for ordinal in 0..5000u64 {
                writer
                    .append(&[
                        Value::UInt(1),
                        Value::UInt(0),
                        Value::UInt(ordinal),
                        Value::String(format!("{ordinal:08}{}", "x".repeat(2048))),
                    ])
                    .unwrap();
            }
            writer.finish().unwrap();
            let work = crate::engine::executor::work_area::WorkAreaHandle::register();
            let mut memory = mem_budget::ScopedCharge::new(0);
            let mut runs = Vec::new();
            let mut next = 0;
            let mut peak = 0;
            let mut metrics = SetOpReplayMetrics::default();
            reduce_setop_spill_partition(
                &[],
                SetOp::Union,
                false,
                1,
                &path,
                "deep",
                SETOP_SPILL_MAX_REPARTITION_DEPTH,
                32 * 1024,
                &dir,
                &mut runs,
                &mut next,
                &work,
                &mut memory,
                &mut peak,
                &mut metrics,
            )
            .unwrap();
            let merged = merge_setop_runs(1, &dir, runs, &work, &mut metrics)
                .unwrap()
                .unwrap();
            let mut reader = ValueRowReader::open(&merged, dir.charger())
                .unwrap()
                .unwrap();
            for ordinal in 0..5000u64 {
                let row = reader.next().unwrap().unwrap();
                assert_eq!(&row[..2], &[Value::UInt(ordinal), Value::UInt(1)]);
                assert_eq!(
                    row[2],
                    Value::String(format!("{ordinal:08}{}", "x".repeat(2048)))
                );
            }
            assert!(reader.next().unwrap().is_none());
            assert!(
                budget.peak_used() < 4 * 1024 * 1024,
                "depth ceiling rebuilt a whole partition: peak {}",
                budget.peak_used()
            );
        });
        assert_eq!(budget.used(), 0);
        assert_eq!(budget.spill_bytes(), 0);
    }

    #[test]
    fn depth_ceiling_preserves_all_counts_domains_and_first_representatives() {
        use crate::engine::executor::{
            mem_budget,
            select::grace::{SpillTmpDir, ValueRowReader, ValueRowWriter},
        };
        let root = tempfile::tempdir_in("/tmp/mydb").unwrap();
        let records = vec![
            (1, 0, 50, Value::String("A ".into())),
            (0, 2, 100, Value::String("a".into())),
            (2, 0, 2, Value::String("b".into())),
            (0, 1, 200, Value::String("B".into())),
            (1, 0, 3, Value::Null),
            (0, 1, 250, Value::Null),
            (0, 1, 0, Value::String("right-only".into())),
            (3, 0, 1, Value::Bytes(vec![b'A'])),
            (1, 1, 104, Value::Double(1.0)),
            (2, 1, 4, Value::Int(1)),
        ];
        // Ordered by the original representative's ordinal. Numeric runtime
        // variants share equality; binary values keep their separate domain.
        let groups = vec![
            (0, 0, 1, Value::String("right-only".into())),
            (1, 3, 0, Value::Bytes(vec![b'A'])),
            (2, 2, 1, Value::String("b".into())),
            (3, 1, 1, Value::Null),
            (4, 3, 2, Value::Int(1)),
            (50, 1, 2, Value::String("A ".into())),
        ];
        for (op, all) in [
            (SetOp::Union, false),
            (SetOp::Intersect, false),
            (SetOp::Intersect, true),
            (SetOp::Except, false),
            (SetOp::Except, true),
        ] {
            let budget = Arc::new(mem_budget::MemBudget::new(1024 * 1024));
            mem_budget::with_query_budget(budget.clone(), || {
                let dir = SpillTmpDir::create_kind(root.path(), "setop").unwrap();
                let path = dir.path().join("deep");
                let mut writer = ValueRowWriter::create(&path, 4, dir.charger()).unwrap();
                for (left, right, ordinal, value) in &records {
                    writer
                        .append(&[
                            Value::UInt(*left),
                            Value::UInt(*right),
                            Value::UInt(*ordinal),
                            value.clone(),
                        ])
                        .unwrap();
                }
                writer.finish().unwrap();
                let work = crate::engine::executor::work_area::WorkAreaHandle::register();
                let mut memory = mem_budget::ScopedCharge::new(0);
                let mut runs = Vec::new();
                let mut next = 0;
                let mut peak = 0;
                let mut metrics = SetOpReplayMetrics::default();
                reduce_setop_spill_partition(
                    &[Some(33)],
                    op,
                    all,
                    1,
                    &path,
                    "deep",
                    SETOP_SPILL_MAX_REPARTITION_DEPTH,
                    64,
                    &dir,
                    &mut runs,
                    &mut next,
                    &work,
                    &mut memory,
                    &mut peak,
                    &mut metrics,
                )
                .unwrap();
                assert!(
                    !path.exists(),
                    "consumed source must release its file immediately"
                );
                let merged = merge_setop_runs(1, &dir, runs, &work, &mut metrics)
                    .unwrap()
                    .unwrap();
                let mut reader = ValueRowReader::open(&merged, dir.charger())
                    .unwrap()
                    .unwrap();
                for (ordinal, left, right, value) in &groups {
                    let repeats = match (op, all) {
                        (SetOp::Union, false) => 1,
                        (SetOp::Intersect, false) => u64::from(*left > 0 && *right > 0),
                        (SetOp::Intersect, true) => (*left).min(*right),
                        (SetOp::Except, false) => u64::from(*left > 0 && *right == 0),
                        (SetOp::Except, true) => (*left).saturating_sub(*right),
                        _ => unreachable!(),
                    };
                    if repeats != 0 {
                        assert_eq!(
                            reader.next().unwrap().unwrap(),
                            vec![Value::UInt(*ordinal), Value::UInt(repeats), value.clone()],
                            "{op:?} all={all}"
                        );
                    }
                }
                assert!(reader.next().unwrap().is_none());
            });
            assert_eq!(budget.used(), 0);
            assert_eq!(budget.spill_bytes(), 0);
        }
    }

    #[test]
    fn malformed_set_partition_counters_are_errors_at_every_replay_route() {
        use crate::engine::executor::select::grace::{SpillTmpDir, ValueRowWriter};
        let root = tempfile::tempdir_in("/tmp/mydb").unwrap();
        for depth in [0, SETOP_SPILL_MAX_REPARTITION_DEPTH] {
            let dir = SpillTmpDir::create_kind(root.path(), "setop").unwrap();
            let path = dir.path().join("invalid-counter");
            let mut writer = ValueRowWriter::create(&path, 4, dir.charger()).unwrap();
            writer
                .append(&[
                    Value::String("not a count".into()),
                    Value::UInt(0),
                    Value::UInt(0),
                    Value::Int(42),
                ])
                .unwrap();
            writer.finish().unwrap();
            let work = crate::engine::executor::work_area::WorkAreaHandle::register();
            let mut memory = crate::engine::executor::mem_budget::ScopedCharge::new(0);
            let mut runs = Vec::new();
            let mut next = 0;
            let mut peak = 0;
            let mut metrics = SetOpReplayMetrics::default();
            let error = reduce_setop_spill_partition(
                &[],
                SetOp::Union,
                false,
                1,
                &path,
                "invalid",
                depth,
                32 * 1024,
                &dir,
                &mut runs,
                &mut next,
                &work,
                &mut memory,
                &mut peak,
                &mut metrics,
            )
            .unwrap_err();
            assert_eq!(error.code(), 1105);
            assert!(error.message().contains("spill counter"));
        }
    }

    #[test]
    fn zero_compact_set_count_cannot_silently_end_the_result() {
        use crate::engine::executor::select::grace::{SpillTmpDir, ValueRowWriter};
        let root = tempfile::tempdir_in("/tmp/mydb").unwrap();
        let dir = SpillTmpDir::create_kind(root.path(), "setop").unwrap();
        let path = dir.path().join("invalid-zero");
        let mut writer = ValueRowWriter::create(&path, 3, dir.charger()).unwrap();
        writer
            .append(&[Value::UInt(0), Value::UInt(0), Value::Int(41)])
            .unwrap();
        writer
            .append(&[Value::UInt(1), Value::UInt(1), Value::Int(42)])
            .unwrap();
        let bytes = writer.finish().unwrap();
        let mut output = IncrementalSetOutput {
            inner: IncrementalSetOutputInner::Spilled(SpilledSetOutput {
                dir,
                output_path: Some(path),
                reader: None,
                output_spill_bytes: bytes,
                output_read_recorded: false,
            }),
            repeated: None,
            _memory_charge: None,
            _work_area: None,
        };
        let error = output.next_row().unwrap_err();
        assert_eq!(error.code(), 1105);
        assert!(error.message().contains("zero set-operation output count"));
    }

    #[test]
    fn intersect_distinct_spills_hash_count_state_and_keeps_left_order() {
        let root = tempfile::tempdir().expect("temporary data directory");
        let left = (0..3_000).map(wide_row).collect::<Vec<_>>();
        let right = (1_500..4_500).map(wide_row).collect::<Vec<_>>();
        let budget = Arc::new(crate::engine::executor::mem_budget::MemBudget::new(1024));
        let rows = crate::engine::executor::mem_budget::with_query_budget(budget.clone(), || {
            combine_set_rows_spillable(&[], SetOp::Intersect, false, left, right, 1, root.path())
        })
        .expect("spillable INTERSECT");
        assert_eq!(rows.len(), 1_500);
        assert_eq!(rows.first(), Some(&wide_row(1_500)));
        assert_eq!(rows.last(), Some(&wide_row(2_999)));
        assert!(
            budget.spill_bytes() == 0,
            "spill files must release quota on drop"
        );
    }

    #[test]
    fn round2_spilled_sets_keep_binary_and_pad_space_domains() {
        for (collation, right_suffix, expected) in [(46, "A", 0), (45, "a ", 120)] {
            let root = tempfile::tempdir().unwrap();
            let make = |index, suffix: &str| {
                vec![Value::String(format!(
                    "{index:05}-{}{suffix}",
                    "x".repeat(400)
                ))]
            };
            let mut left = (0..120).map(|i| make(i, "a"));
            let mut right = (0..120).map(|i| make(i, right_suffix));
            let budget = Arc::new(crate::engine::executor::mem_budget::MemBudget::new(1024));
            let mut output =
                crate::engine::executor::mem_budget::with_query_budget(budget.clone(), || {
                    combine_set_sources_spillable(
                        &[Some(collation)],
                        SetOp::Intersect,
                        false,
                        1,
                        root.path(),
                        || Ok(left.next()),
                        || Ok(right.next()),
                    )
                })
                .unwrap();
            assert!(matches!(
                output.inner,
                IncrementalSetOutputInner::Spilled(_)
            ));
            let mut count = 0;
            while output.next_row().unwrap().is_some() {
                count += 1;
            }
            assert_eq!(count, expected);
        }
    }

    #[test]
    fn incremental_intersect_spills_inputs_and_streams_ordered_output() {
        let root = tempfile::tempdir().expect("temporary data directory");
        let mut left = (0..120).map(wide_row);
        let mut right = (60..180).map(wide_row);
        let budget = Arc::new(crate::engine::executor::mem_budget::MemBudget::new(1024));
        let mut output =
            crate::engine::executor::mem_budget::with_query_budget(budget.clone(), || {
                combine_set_sources_spillable(
                    &[],
                    SetOp::Intersect,
                    false,
                    1,
                    root.path(),
                    || Ok(left.next()),
                    || Ok(right.next()),
                )
            })
            .expect("incremental spillable INTERSECT");
        let spill_dir = match &output.inner {
            IncrementalSetOutputInner::Spilled(spilled) => spilled.dir.path().to_path_buf(),
            IncrementalSetOutputInner::InMemory(_) => panic!("tiny budget must force spill"),
        };
        let spill_files = std::fs::read_dir(&spill_dir)
            .expect("set-op spill directory")
            .count();
        assert!(
            spill_files <= SETOP_SPILL_FANOUT * 2,
            "bounded fanout must not create one file per fixed 1024-way bucket: {spill_files}"
        );
        let mut rows = Vec::new();
        while let Some(row) = output.next_row().expect("spilled output row") {
            rows.push(row);
        }
        drop(output);
        assert!(
            !spill_dir.exists(),
            "set-op spill directory must be removed"
        );
        assert_eq!(rows.len(), 60);
        assert_eq!(rows.first(), Some(&wide_row(60)));
        assert_eq!(rows.last(), Some(&wide_row(119)));
        assert_eq!(budget.spill_bytes(), 0, "spill quota must be released");
    }

    #[test]
    fn incremental_intersect_all_keeps_large_multiplicity_compact() {
        let root = tempfile::tempdir().expect("temporary data directory");
        let row = vec![Value::String("same".repeat(128))];
        let mut left = std::iter::repeat_n(row.clone(), 20_000);
        let mut right = std::iter::repeat_n(row.clone(), 12_000);
        let mut output = combine_set_sources_spillable(
            &[],
            SetOp::Intersect,
            true,
            1,
            root.path(),
            || Ok(left.next()),
            || Ok(right.next()),
        )
        .expect("incremental INTERSECT ALL");
        for _ in 0..12_000 {
            assert_eq!(output.next_row().unwrap(), Some(row.clone()));
        }
        assert_eq!(output.next_row().unwrap(), None);
    }

    #[test]
    fn incremental_intersect_recursively_repartitions_without_writer_churn() {
        let root = tempfile::tempdir().expect("temporary data directory");
        // Concentrate distinct keys in one first-level partition: it exceeds
        // the 1 MiB replay floor with a small fixture and must split again.
        use std::hash::{Hash, Hasher};
        let keys = (0..)
            .filter(|value| {
                let mut hasher = std::collections::hash_map::DefaultHasher::new();
                set_row_key(&[], &wide_row(*value)).hash(&mut hasher);
                (hasher.finish() as usize) & (SETOP_SPILL_FANOUT - 1) == 0
            })
            .take(4_500)
            .collect::<Vec<_>>();
        let mut left = keys[..3_000].iter().copied().map(wide_row);
        let mut right = keys[1_500..].iter().copied().map(wide_row);
        let budget = Arc::new(crate::engine::executor::mem_budget::MemBudget::new(1024));
        let repartitions_before =
            SETOP_SPILL_REPARTITIONS.load(std::sync::atomic::Ordering::Relaxed);
        let mut output =
            crate::engine::executor::mem_budget::with_query_budget(budget.clone(), || {
                combine_set_sources_spillable(
                    &[],
                    SetOp::Intersect,
                    false,
                    1,
                    root.path(),
                    || Ok(left.next()),
                    || Ok(right.next()),
                )
            })
            .expect("recursive set-op spill");
        let spill_dir = match &output.inner {
            IncrementalSetOutputInner::Spilled(spilled) => spilled.dir.path().to_path_buf(),
            IncrementalSetOutputInner::InMemory(_) => panic!("tiny budget must force spill"),
        };
        let names = std::fs::read_dir(&spill_dir)
            .expect("set-op spill directory")
            .map(|entry| entry.unwrap().file_name().to_string_lossy().into_owned())
            .collect::<Vec<_>>();
        assert!(
            SETOP_SPILL_REPARTITIONS.load(std::sync::atomic::Ordering::Relaxed)
                > repartitions_before,
            "oversized replay partition must recursively repartition"
        );
        assert!(
            !names.iter().any(|name| name.starts_with("setop-")),
            "consumed ancestor and leaf partitions must be reclaimed before output"
        );
        let mut rows = 0usize;
        let mut first = None;
        let mut last = None;
        while let Some(row) = output.next_row().expect("recursive spill output") {
            first.get_or_insert_with(|| row.clone());
            last = Some(row);
            rows += 1;
        }
        assert_eq!(rows, 1_500);
        assert_eq!(first, Some(wide_row(keys[1_500])));
        assert_eq!(last, Some(wide_row(keys[2_999])));
        drop(output);
        assert_eq!(
            budget.used(),
            0,
            "all recursive replay charges must release"
        );
        assert!(
            !spill_dir.exists(),
            "recursive spill directory must be removed"
        );
    }

    #[test]
    fn materialized_set_adapter_uses_bounded_recursive_partition_replay() {
        use crate::engine::executor::memory_footprint::AllocatedBytes;
        use std::hash::{Hash, Hasher};
        let root = tempfile::tempdir_in("/tmp/mydb").unwrap();
        let keys = (0..)
            .filter(|value| {
                let mut hasher = std::collections::hash_map::DefaultHasher::new();
                set_row_key(&[], &wide_row(*value)).hash(&mut hasher);
                (hasher.finish() as usize) & (SETOP_SPILL_FANOUT - 1) == 0
            })
            .take(4500)
            .collect::<Vec<_>>();
        let left = keys[..3000].iter().copied().map(wide_row).collect();
        let right = keys[1500..].iter().copied().map(wide_row).collect();
        let budget = Arc::new(crate::engine::executor::mem_budget::MemBudget::new(1024));
        let before = SETOP_SPILL_REPARTITIONS.load(std::sync::atomic::Ordering::Relaxed);
        let (rows, charge) =
            crate::engine::executor::mem_budget::with_query_budget(budget.clone(), || {
                combine_set_rows_spillable_charged(
                    &[],
                    SetOp::Intersect,
                    false,
                    left,
                    right,
                    1,
                    root.path(),
                )
            })
            .unwrap();
        assert_eq!(
            rows,
            keys[1500..3000]
                .iter()
                .copied()
                .map(wide_row)
                .collect::<Vec<_>>()
        );
        assert!(
            SETOP_SPILL_REPARTITIONS.load(std::sync::atomic::Ordering::Relaxed) > before,
            "materialized adapter rebuilt an unbounded whole partition"
        );
        assert_eq!(budget.used(), charge.charged());
        assert!(charge.charged() >= rows.allocated_bytes());
        drop(rows);
        drop(charge);
        assert_eq!(budget.used(), 0);
        assert_eq!(budget.spill_bytes(), 0);
    }

    #[test]
    fn set_count_all_multiplicity_and_null_equality_match_sql_semantics() {
        let root = tempfile::tempdir().expect("temporary data directory");
        let left = vec![
            vec![Value::Null],
            vec![Value::Null],
            vec![Value::Int(1)],
            vec![Value::Int(1)],
            vec![Value::Int(1)],
        ];
        let right = vec![vec![Value::Null], vec![Value::Int(1)], vec![Value::Int(1)]];
        let intersect = combine_set_rows_spillable(
            &[],
            SetOp::Intersect,
            true,
            left.clone(),
            right.clone(),
            1,
            root.path(),
        )
        .expect("INTERSECT ALL");
        assert_eq!(
            intersect,
            vec![vec![Value::Null], vec![Value::Int(1)], vec![Value::Int(1)]]
        );
        let except =
            combine_set_rows_spillable(&[], SetOp::Except, true, left, right, 1, root.path())
                .expect("EXCEPT ALL");
        assert_eq!(except, vec![vec![Value::Null], vec![Value::Int(1)]]);
    }
}

pub(in crate::engine::executor) fn combine_set_operation_results(
    op: SetOp,
    all: bool,
    mut left: ResultSet,
    mut right: ResultSet,
    order_by: Vec<OrderBy>,
    limit: Option<LimitValue>,
    offset: Option<LimitValue>,
    data_dir: &std::path::Path,
) -> Result<QueryResponse, QueryError> {
    if left.columns.len() != right.columns.len() {
        return Err(error(
            crate::error::catalog::ER_WRONG_NUMBER_OF_COLUMNS_IN_SELECT,
            "the used SELECT statements have a different number of columns",
        ));
    }
    // MySQL aggregate-type unification: promote mixed INT/DECIMAL columns to a
    // common DECIMAL scale before combining (issue #68).
    unify_setop_numeric_columns(&mut left, &mut right)?;
    // Set-operation metadata is owned by the first SELECT. For a computed
    // field MySQL repeats that branch's unaliased label in the protocol
    // org-name slot even though table/schema origin remains empty.
    for column in &mut left.columns {
        if column.source.org_name.is_empty() {
            column.source.org_name = column.name.clone();
        }
    }
    let columns = left.columns;
    let collations = &columns
        .iter()
        .map(|column| column.source.collation_id)
        .collect::<Vec<_>>();
    let (mut rows, _rows_charge) = combine_set_rows_spillable_charged(
        collations,
        op,
        all,
        left.rows,
        right.rows,
        columns.len(),
        data_dir,
    )?;
    if !order_by.is_empty() {
        // Resolve each ORDER BY item against the set operation's output
        // columns. MySQL semantics: bare column names and ordinals bind
        // to the first SELECT's result columns; arbitrary expressions
        // over those columns are allowed; table-qualified names are
        // rejected (1250) and aggregates are rejected (3028).
        enum SetOpOrderKey {
            Column(usize),
            Expr(Expr),
        }
        let mut keys: Vec<SetOpOrderKey> = Vec::with_capacity(order_by.len());
        let mut needs_expr_eval = false;
        for (n, item) in order_by.iter().enumerate() {
            let key = match &item.expr {
                Expr::Identifier(name) => SetOpOrderKey::Column(
                    columns
                        .iter()
                        .position(|c| c.name.eq_ignore_ascii_case(name))
                        .ok_or_else(|| {
                            error(
                                crate::error::catalog::ER_BAD_FIELD_ERROR,
                                format!("Unknown column '{name}' in 'order clause'"),
                            )
                        })?,
                ),
                // `ORDER BY N` on a set operation refers to the N-th output
                // column. The parser leaves the literal in place here (a set
                // operation's select list is not visible at that point), so
                // resolve it against the materialized columns.
                Expr::Literal(crate::parser::Literal::Int(pos)) if *pos >= 0 => {
                    SetOpOrderKey::Column(set_operation_order_index(*pos as u64, columns.len())?)
                }
                Expr::Literal(crate::parser::Literal::UInt(pos)) => {
                    SetOpOrderKey::Column(set_operation_order_index(*pos, columns.len())?)
                }
                Expr::CompoundIdentifier(parts) => {
                    let table = parts.first().map(String::as_str).unwrap_or("");
                    return Err(error(
                        crate::error::catalog::ER_TABLENAME_NOT_ALLOWED_HERE,
                        format!(
                            "Table '{table}' from one of the SELECTs cannot be used in \
                             global ORDER BY"
                        ),
                    ));
                }
                expr => {
                    if crate::engine::executor::select::expr_has_aggregate(expr) {
                        return Err(error(
                            crate::error::catalog::ER_AGGREGATE_ORDER_FOR_UNION,
                            format!(
                                "Expression #{} of ORDER BY contains aggregate function and \
                                 applies to a UNION",
                                n + 1
                            ),
                        ));
                    }
                    needs_expr_eval = true;
                    SetOpOrderKey::Expr(expr.clone())
                }
            };
            keys.push(key);
        }
        // Expression keys evaluate through the legacy evaluator against a
        // synthetic single-table view of the output columns. Built once;
        // `column_index` falls back to a linear name scan over `columns`,
        // whose order matches the row value layout.
        let synth = needs_expr_eval.then(|| synthetic_set_op_schema(&columns));
        let mut fields = crate::engine::executor::sort::fields_from_order_by(&order_by);
        let sort_schema = synthetic_set_op_schema(&columns);
        for (field, key) in fields.iter_mut().zip(&keys) {
            let expr = match key {
                SetOpOrderKey::Column(i) => Expr::Identifier(columns[*i].name.clone()),
                SetOpOrderKey::Expr(expr) => expr.clone(),
            };
            field.collation_id = crate::engine::expr::expr_collation(&expr, &sort_schema)
                .map(|info| info.collation_id);
        }
        let key_fn = |row: &Vec<Value>| -> Vec<Value> {
            keys.iter()
                .map(|key| match key {
                    SetOpOrderKey::Column(i) => row.get(*i).cloned().unwrap_or(Value::Null),
                    SetOpOrderKey::Expr(expr) => synth
                        .as_ref()
                        .and_then(|schema| crate::engine::expr::eval_expr(expr, schema, row).ok())
                        .unwrap_or(Value::Null),
                })
                .collect()
        };
        if !(crate::engine::executor::sort::key_encode::sort_key_encode_enabled()
            && crate::engine::executor::sort::key_encode::try_sort_by_encoded_keys(
                &mut rows, &fields, key_fn,
            ))
        {
            crate::engine::executor::sort::sort_by_cached_keys(&mut rows, &fields, key_fn);
        }
    }
    // Honor auto-parameterized LIMIT/OFFSET (`LimitValue::Placeholder`) by
    // resolving through the shared helpers rather than dropping non-`Rows`.
    let offset_n = crate::engine::executor::select::limit_value_or_zero(offset.as_ref())?;
    let limit_n = crate::engine::executor::select::optional_limit_value(limit.as_ref())?;
    if offset_n >= rows.len() {
        rows.clear();
    } else {
        rows.drain(..offset_n);
    }
    if let Some(limit_n) = limit_n {
        rows.truncate(limit_n);
    }
    Ok(QueryResponse::ResultSet(ResultSet { columns, rows }))
}

/// Execute a set operation (`UNION` / `INTERSECT` / `EXCEPT`) that appears in
/// a FROM-clause derived table, **without** a `SessionContext`. By the time a
/// derived table is materialised, the outer qualification and view-expansion
/// passes have already descended into the set operation's branches (see
/// `qualify_select_tables` / `expand_views_in_statement`), so each leaf SELECT
/// can run through the session-free [`execute_select`]. Returns the combined
/// rows so the derived-table bridge can feed them through the same path used
/// for an ordinary derived SELECT.
pub(in crate::engine::executor) fn execute_set_operation_rows(
    db: &Arc<Database>,
    txn: TxnView,
    stmt: SetOperation,
    registry: &SessionRegistry,
) -> Result<QueryResponse, QueryError> {
    let mut physical_statement = Statement::SetOperation(stmt.clone());
    let mut inline_ctes = std::collections::HashMap::new();
    materialize_setop_inline_derived(db, txn, &mut physical_statement, registry, &mut inline_ctes)?;
    let native = crate::engine::executor::cte::with_cte_registry_extended(inline_ctes, || {
        if let Some(plan) = crate::planner::plan_query_physical(db, &physical_statement)? {
            if let Some((columns, rows)) =
                crate::engine::executor::select::vec_bridge::execute_native_plan_values(
                    db, txn, &plan,
                )?
            {
                return Ok(Some(QueryResponse::ResultSet(ResultSet { columns, rows })));
            }
        }
        Ok(None)
    })?;
    if let Some(response) = native {
        return Ok(response);
    }
    let SetOperation {
        op,
        all,
        left,
        right,
        order_by,
        limit,
        offset,
    } = stmt;
    // Streaming `UNION ALL` fast path (mirrors `execute_set_operation_dispatch`):
    // chain the branch cursors instead of materialising + concatenating, so a
    // derived-table `UNION ALL` flows row-by-row into its consumer (e.g. the
    // spill-aware bridge in `execute_derived_select`, which feeds the stream
    // straight into a `CteSpillBuilder` — no intermediate row set).
    if matches!(op, SetOp::Union)
        && all
        && order_by.is_empty()
        && limit.is_none()
        && offset.is_none()
    {
        let left = execute_set_operation_branch_response(db, txn, left, registry)?;
        let right = execute_set_operation_branch_response(db, txn, right, registry)?;
        return chained_union_all(left, right);
    }
    let (left, right) = execute_set_op_branches_maybe_parallel(db, txn, left, right, registry)?;
    combine_set_operation_results(op, all, left, right, order_by, limit, offset, db.data_dir())
}

/// Lower inline-derived relations inside a session-free set-operation graph to
/// bounded CTE spools before physical planning starts. This removes the old
/// AST shape gate that diverted the complete UNION/INTERSECT/EXCEPT into the
/// row combiner merely because one leaf happened to contain a derived table.
fn materialize_setop_inline_derived<'q>(
    db: &Arc<Database>,
    txn: TxnView,
    statement: impl Into<crate::parser::QueryMut<'q>>,
    registry: &SessionRegistry,
    results: &mut std::collections::HashMap<String, std::sync::Arc<crate::engine::api::CteResult>>,
) -> Result<(), QueryError> {
    let statement = statement.into();
    fn materialize_one(
        db: &Arc<Database>,
        txn: TxnView,
        derived: &SubQuery,
        registry: &SessionRegistry,
    ) -> Result<std::sync::Arc<crate::engine::api::CteResult>, QueryError> {
        let response = execute_subquery_response(db, txn, derived, registry)?;
        Ok(std::sync::Arc::new(
            crate::engine::executor::select::materialize_response_with_spill(
                response,
                db.data_dir().to_path_buf(),
            )?,
        ))
    }

    match statement {
        crate::parser::QueryMut::Select(select) => {
            if let Some(derived) = select.derived.take() {
                let alias = select
                    .from
                    .first()
                    .map(|table| table.alias.clone().unwrap_or_else(|| table.name.clone()))
                    .unwrap_or_else(|| "derived".to_string());
                let result = materialize_one(db, txn, &derived, registry)?;
                let name = crate::engine::executor::cte::next_inline_derived_name();
                results.insert(name.clone(), result);
                if let Some(table) = select.from.first_mut() {
                    table.name = name;
                    table.alias = Some(alias);
                }
            }
            for join in &mut select.joins {
                let Some(derived) = join.derived.take() else {
                    continue;
                };
                if join.lateral {
                    return Err(crate::error::catalog::ER_NOT_SUPPORTED_YET.error(
                        "LATERAL derived table inside a set-operation branch requires Apply",
                    ));
                }
                let alias = join
                    .table
                    .alias
                    .clone()
                    .unwrap_or_else(|| join.table.name.clone());
                let result = materialize_one(db, txn, &derived, registry)?;
                let name = crate::engine::executor::cte::next_inline_derived_name();
                results.insert(name.clone(), result);
                join.table.name = name;
                join.table.alias = Some(alias);
            }
            Ok(())
        }
        crate::parser::QueryMut::SetOperation(set) => {
            materialize_setop_inline_derived(db, txn, &mut set.left, registry, results)?;
            materialize_setop_inline_derived(db, txn, &mut set.right, registry, results)
        }
        crate::parser::QueryMut::WithCte(mut with) => {
            for cte in with.ctes.iter_mut() {
                materialize_setop_inline_derived(db, txn, &mut cte.query, registry, results)?;
            }
            materialize_setop_inline_derived(db, txn, &mut with.body, registry, results)
        }
        _ => Ok(()),
    }
}

thread_local! {
    /// One query-level branch fork is enough to occupy both independent set
    /// inputs. Nested set nodes inherit this marker and execute on the same
    /// workers instead of recursively creating a thread tree.
    static SETOP_BRANCH_SCHEDULER_ACTIVE: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

fn with_setop_branch_scheduler<R>(f: impl FnOnce() -> R) -> R {
    SETOP_BRANCH_SCHEDULER_ACTIVE.with(|active| {
        let previous = active.replace(true);
        struct Restore<'a>(&'a std::cell::Cell<bool>, bool);
        impl Drop for Restore<'_> {
            fn drop(&mut self) {
                self.0.set(self.1);
            }
        }
        let _restore = Restore(active, previous);
        f()
    })
}

fn execute_set_op_branches_maybe_parallel(
    db: &Arc<Database>,
    txn: TxnView,
    left: crate::parser::QueryExpr,
    right: crate::parser::QueryExpr,
    registry: &SessionRegistry,
) -> Result<(ResultSet, ResultSet), QueryError> {
    let already_scheduled = SETOP_BRANCH_SCHEDULER_ACTIVE.with(std::cell::Cell::get);
    if !already_scheduled {
        let context =
            crate::engine::executor::select::execution_context::ExecutionContext::capture(txn);
        let left_context = context.clone();
        return std::thread::scope(|scope| {
            let left_handle = scope.spawn(move || {
                left_context.run(db, || {
                    with_setop_branch_scheduler(|| {
                        execute_set_operation_branch(db, txn, left, registry)
                    })
                })
            });
            let right_res = context.run(db, || {
                with_setop_branch_scheduler(|| {
                    execute_set_operation_branch(db, txn, right, registry)
                })
            });
            let left_res = match left_handle.join() {
                Ok(result) => result,
                Err(_) => Err(error(
                    crate::error::catalog::ER_UNKNOWN_ERROR,
                    "set operation branch execution panicked",
                )),
            };
            Ok((left_res?, right_res?))
        });
    }
    let left = execute_set_operation_branch(db, txn, left, registry)?;
    let right = execute_set_operation_branch(db, txn, right, registry)?;
    Ok((left, right))
}

/// One branch of a derived-table set operation, as a (possibly streaming)
/// [`QueryResponse`]. A branch is a SELECT or a nested set operation; both
/// run session-free. Anything else is rejected with a clear error.
pub(super) fn inline_single_use_with_in_select(
    db: &Database,
    select: &mut Select,
) -> Result<(), QueryError> {
    fn rewrite(db: &Database, subquery: &mut SubQuery) -> Result<(), QueryError> {
        if matches!(subquery, SubQuery::WithCte(_)) {
            crate::engine::executor::select::describe_subquery_columns(db, subquery)?;
        }
        if let SubQuery::WithCte(with) = subquery {
            crate::engine::executor::cte::inline_single_use_ctes(&mut with.ctes, &mut with.body);
            if with.ctes.is_empty() {
                // Move the existing body out without copying its query tree.
                let body = std::mem::replace(&mut with.body, Select::default().into());
                *subquery = body;
            }
        }
        match subquery {
            SubQuery::Select(select) => inline_single_use_with_in_select(db, select),
            SubQuery::SetOp(_) | SubQuery::WithCte(_) => Ok(()),
        }
    }
    if let Some(derived) = select.derived.as_mut() {
        rewrite(db, derived)?;
    }
    for join in &mut select.joins {
        if let Some(derived) = join.derived.as_mut() {
            rewrite(db, derived)?;
        }
    }
    Ok(())
}

pub(in crate::engine::executor) fn execute_set_operation_branch_response(
    db: &Arc<Database>,
    txn: TxnView,
    stmt: crate::parser::QueryExpr,
    registry: &SessionRegistry,
) -> Result<QueryResponse, QueryError> {
    match stmt {
        crate::parser::QueryExpr::Select(select) => {
            if let Some(response) =
                crate::engine::executor::locking_join::execute(db, txn, &select, registry)?
            {
                return Ok(response);
            }
            crate::engine::executor::dml::execute_expression_subquery(db, txn, *select, registry)
        }
        crate::parser::QueryExpr::SetOp(set_op) => {
            execute_set_operation_rows(db, txn, *set_op, registry)
        }
        crate::parser::QueryExpr::WithCte(mut with) => {
            crate::engine::executor::cte::describe_definitions(&with.ctes, |query| {
                crate::engine::executor::select::describe_subquery_columns(db, query)
            })?;
            let mut body = with.body;
            crate::engine::executor::cte::inline_single_use_ctes(&mut with.ctes, &mut body);
            let results = crate::engine::executor::cte::materialize_ctes_in_view(
                db, txn, &with.ctes, registry,
            )?;
            crate::engine::executor::cte::with_cte_registry_extended(results, || {
                execute_set_operation_branch_response(db, txn, body, registry)
            })
        }
    }
}

/// One branch of a derived-table set operation, materialised into a
/// [`ResultSet`]. Used by the non-streaming combine path (UNION/dedup,
/// INTERSECT, EXCEPT, or a set op carrying ORDER BY / LIMIT). A branch that
/// itself streams (e.g. a nested `UNION ALL`) is collapsed here.
fn execute_set_operation_branch(
    db: &Arc<Database>,
    txn: TxnView,
    stmt: crate::parser::QueryExpr,
    registry: &SessionRegistry,
) -> Result<ResultSet, QueryError> {
    let response = execute_set_operation_branch_response(db, txn, stmt, registry)?;
    match response.materialize()? {
        QueryResponse::ResultSet(rs) => Ok(rs),
        _ => Err(error(
            crate::error::catalog::ER_NOT_SUPPORTED_YET,
            "set operation branch did not produce a result set",
        )),
    }
}

/// Materialise a FROM-clause derived-table subquery — an ordinary SELECT or a
/// set operation — into a [`ResultSet`]. Single session-free entry point for
/// the derived-table bridge (`execute_derived_select`,
/// `materialize_join_source_with_derived`), so both the simple and the
/// JOIN-carrying derived paths handle set operations identically.
pub(in crate::engine::executor) fn execute_subquery(
    db: &Arc<Database>,
    txn: TxnView,
    sub: &SubQuery,
    registry: &SessionRegistry,
) -> Result<ResultSet, QueryError> {
    let stmt = sub.clone();
    execute_set_operation_branch(db, txn, stmt, registry)
}

/// Like [`execute_subquery`] but returns the (possibly streaming)
/// [`QueryResponse`] without materialising. Lets a spill-aware consumer
/// (the derived-table JOIN bridge) pull a streaming `UNION ALL` row-by-row
/// into a `CteSpillBuilder` instead of building the full row set first.
pub(in crate::engine::executor) fn execute_subquery_response(
    db: &Arc<Database>,
    txn: TxnView,
    sub: &SubQuery,
    registry: &SessionRegistry,
) -> Result<QueryResponse, QueryError> {
    let stmt = sub.clone();
    execute_set_operation_branch_response(db, txn, stmt, registry)
}

/// Single-table view over a set operation's output columns so ORDER BY
/// expressions can run through the legacy evaluator (`eval_expr`). Only
/// name → position resolution matters: `column_index` linear-scans
/// `columns`, whose order matches each row's value layout. Declared
/// types are irrelevant to evaluating already-materialized `Value`s, so
/// every column is typed as nullable TEXT.
pub(super) fn synthetic_set_op_schema(columns: &[Column]) -> crate::storage::TableSchema {
    crate::storage::TableSchema::synthetic_for_planning("<set operation>", columns)
}

fn set_operation_order_index(pos: u64, width: usize) -> Result<usize, QueryError> {
    if pos >= 1 && (pos as usize) <= width {
        Ok(pos as usize - 1)
    } else {
        Err(error(
            crate::error::catalog::ER_BAD_FIELD_ERROR,
            format!("Unknown column '{pos}' in 'order clause'"),
        ))
    }
}
