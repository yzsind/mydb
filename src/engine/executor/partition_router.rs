//! Per-row partition routing for partitioned tables.
//!
//! The entry point is [`route_row`], which evaluates the partition
//! function over a row and returns the matching partition index +
//! storage id. Called by:
//!
//! - INSERT in `src/storage/core/database/dml.rs` — to choose which
//!   physical heap a new row lands in.
//! - DML migration paths (UPDATE/DELETE) — when the partition column
//!   itself is updated, the new partition is computed here and the row
//!   is then deleted+inserted across partitions.
//!
//! The router is deliberately pure and synchronous: no IO, no
//! transactions. It only needs the table schema, the partition info,
//! and the row's column values.
//!
//! ## Partition pruning context
//!
//! [`with_pruned_partitions`] / [`current_pruned_partitions`] form a
//! thread-local channel used by the storage scan layer to learn which
//! partition `storage_id`s the planner picked for the current scan.
//! Set by the SeqScan executor right before the inner scan call;
//! cleared automatically when the closure returns.
//!
//! This avoids threading an extra parameter through every public scan
//! method while still letting the planner short-circuit the
//! "iterate all partitions" fan-out when WHERE/JOIN equalities (or
//! ranges) constrain the partition set to a known subset.

use std::cell::RefCell;
use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc,
};

use crate::storage::partitioning::{
    coerce_literal_for_column_type, compare_value, hash_value_to_bucket, linear_hash_to_bucket,
    no_partition_for_value, PartitionBounds, PartitionInfo, PartitionLiteral, PartitionMethod,
    RoutedPartition,
};
use crate::storage::TableSchema;
use crate::types::{QueryError, Value};

thread_local! {
    /// Active pruned partition set for the current scan. `None` means
    /// "no pruning — visit every partition". Wrapped in `Vec` (not
    /// `HashSet`) because partition counts are tiny (~16-128) and the
    /// linear `contains` check is faster than hashing.
    static PRUNED_PARTITIONS: RefCell<Option<Vec<u64>>> = const { RefCell::new(None) };
    /// Diagnostic counter: incremented each time a partition heap is
    /// actually visited during a scan. Tests inspect this to assert
    /// that pruning short-circuited the fan-out.
    static PARTITIONS_VISITED: std::cell::Cell<u64> =
        const { std::cell::Cell::new(0) };
    /// Optional EXPLAIN ANALYZE collector. Unlike `PARTITIONS_VISITED`,
    /// this can be cloned into worker threads so parallel partition scans
    /// report the same actual partition count as serial scans.
    static PARTITION_VISIT_COLLECTOR: RefCell<Option<Arc<AtomicU64>>> =
        const { RefCell::new(None) };
}

#[cfg(test)]
static TEST_PARTITIONS_VISITED: AtomicU64 = AtomicU64::new(0);

/// Test-only: reset the partition-visit counter to zero.
#[cfg(test)]
pub fn reset_partition_visit_counter() {
    PARTITIONS_VISITED.with(|c| c.set(0));
    TEST_PARTITIONS_VISITED.store(0, Ordering::Relaxed);
}

#[cfg(test)]
/// Read the per-thread partition-visit counter. The storage scan layer
/// increments this whenever it opens a partition heap, regardless of how
/// many rows that partition contains. Tests use it as a black-box pruning
/// signal; EXPLAIN ANALYZE samples it around scan nodes for actual partition
/// counts.
pub fn partition_visit_count() -> u64 {
    #[cfg(test)]
    {
        return TEST_PARTITIONS_VISITED.load(Ordering::Relaxed);
    }
    #[cfg(not(test))]
    PARTITIONS_VISITED.with(|c| c.get())
}

/// Storage scan layer hook: record that we just visited a partition.
pub(crate) fn record_partition_visit() {
    PARTITIONS_VISITED.with(|c| c.set(c.get().saturating_add(1)));
    #[cfg(test)]
    TEST_PARTITIONS_VISITED.fetch_add(1, Ordering::Relaxed);
    PARTITION_VISIT_COLLECTOR.with(|cell| {
        if let Some(counter) = cell.borrow().as_ref() {
            counter.fetch_add(1, Ordering::Relaxed);
        }
    });
}

/// Snapshot the active cross-thread partition-visit collector, if any.
pub(crate) fn current_partition_visit_collector() -> Option<Arc<AtomicU64>> {
    PARTITION_VISIT_COLLECTOR.with(|cell| cell.borrow().clone())
}

/// Install a cross-thread partition-visit collector for the duration of
/// `f`. Passing `None` preserves normal execution behavior.
pub(crate) fn with_partition_visit_collector<F, T>(collector: Option<Arc<AtomicU64>>, f: F) -> T
where
    F: FnOnce() -> T,
{
    let prev = PARTITION_VISIT_COLLECTOR.with(|cell| cell.replace(collector));
    let result = f();
    PARTITION_VISIT_COLLECTOR.with(|cell| {
        *cell.borrow_mut() = prev;
    });
    result
}

/// Install a pruned partition set for the duration of `f`. Nested
/// calls compose by saving and restoring the previous value.
pub fn with_pruned_partitions<F, T>(storage_ids: Vec<u64>, f: F) -> T
where
    F: FnOnce() -> T,
{
    let prev = PRUNED_PARTITIONS.with(|cell| {
        let previous = cell.borrow().clone();
        let effective = match previous.as_ref() {
            Some(outer) => storage_ids
                .into_iter()
                .filter(|storage_id| outer.contains(storage_id))
                .collect(),
            None => storage_ids,
        };
        cell.replace(Some(effective))
    });
    let result = f();
    PRUNED_PARTITIONS.with(|cell| {
        *cell.borrow_mut() = prev;
    });
    result
}

/// Snapshot the currently-active pruned partition set. Used by the
/// storage scan layer when a partitioned table is scanned; if `Some`,
/// the scan visits only those `storage_id`s.
pub fn current_pruned_partitions() -> Option<Vec<u64>> {
    PRUNED_PARTITIONS.with(|cell| cell.borrow().clone())
}

/// The partition `storage_id`s the current scan should visit: the
/// active prune set intersected with the table's partitions, or every
/// partition when no prune context is installed. For an unpartitioned
/// table this is just `[table.id]`.
///
/// This is the single source of truth for "which local index/heap
/// files does a partitioned index scan touch". A B-tree index over a
/// partitioned table is stored as one physical index per partition
/// (keyed by the partition's `storage_id`, not the base `table.id`),
/// and a `RowId` is only unique within one partition — so an index
/// scan spanning several partitions returns ambiguous `RowId`s. The
/// executor fans out such scans one partition at a time (installing a
/// single-`storage_id` prune context via [`with_pruned_partitions`]);
/// inside that context this returns exactly that one partition, which
/// makes both the index read and the subsequent heap read unambiguous.
pub fn in_scope_storage_ids(table: &TableSchema) -> Vec<u64> {
    if !table.is_partitioned() {
        return vec![table.id];
    }
    let all = table.storage_ids();
    match current_pruned_partitions() {
        Some(pruned) => all.into_iter().filter(|s| pruned.contains(s)).collect(),
        None => all,
    }
}

/// Intersect predicate pruning with an explicit/inherited partition scope.
/// The guard never widens PARTITION(...) and restores the caller on drop.
pub(super) fn push_filter_partition_scope(
    table: &TableSchema,
    filter: Option<&crate::parser::Expr>,
) -> Option<PrunedPartitionsGuard> {
    let mut ids = pruned_storage_ids_from_filter(table, filter)?;
    if let Some(inherited) = current_pruned_partitions() {
        ids.retain(|id| inherited.contains(id));
    }
    Some(push_pruned_partitions(ids))
}

/// RAII form of [`with_pruned_partitions`] for loop bodies that would
/// otherwise need a closure capturing `&mut` state (e.g. the per-partition
/// trigger DML pass). The previous prune set is restored on drop.
#[must_use]
pub struct PrunedPartitionsGuard {
    prev: Option<Option<Vec<u64>>>,
}

impl Drop for PrunedPartitionsGuard {
    fn drop(&mut self) {
        if let Some(prev) = self.prev.take() {
            PRUNED_PARTITIONS.with(|cell| *cell.borrow_mut() = prev);
        }
    }
}

pub fn push_pruned_partitions(storage_ids: Vec<u64>) -> PrunedPartitionsGuard {
    let prev = PRUNED_PARTITIONS.with(|cell| cell.replace(Some(storage_ids)));
    PrunedPartitionsGuard { prev: Some(prev) }
}

/// Nested statements must not inherit the caller's physical row location.
/// Move the context out (no allocation) and restore it even on error/unwind.
pub(super) fn suspend_pruned_partitions() -> PrunedPartitionsGuard {
    let prev = PRUNED_PARTITIONS.with(|cell| cell.take());
    PrunedPartitionsGuard { prev: Some(prev) }
}

/// Public router entry. Picks the partition for `row` according to
/// `part.method`.
pub fn route_row(
    table: &TableSchema,
    part: &PartitionInfo,
    row: &[Value],
) -> Result<RoutedPartition, QueryError> {
    let num = part.partitions.len();
    if num == 0 {
        return Err(crate::storage::error(
            crate::error::catalog::ER_WRONG_VALUE,
            "table has no partitions defined",
        ));
    }
    let routed = match part.method {
        PartitionMethod::Range => {
            let value = eval_partition_expr(table, part, row)?;
            route_range_single(part, &value)
        }
        PartitionMethod::RangeColumns => {
            let tuple = extract_column_tuple(table, part, row)?;
            let collations = partition_column_collations(table, part);
            route_range_columns(part, &tuple, &collations)
        }
        PartitionMethod::List => {
            let value = eval_partition_expr(table, part, row)?;
            route_list_single(table, part, &value)
        }
        PartitionMethod::ListColumns => {
            let tuple = extract_column_tuple(table, part, row)?;
            let collations = partition_column_collations(table, part);
            route_list_columns(table, part, &tuple, &collations)
        }
        PartitionMethod::Hash => {
            let value = eval_partition_expr(table, part, row)?;
            let idx = hash_value_to_bucket(&value, num as u32)? as usize;
            Ok(routed(part, idx))
        }
        PartitionMethod::LinearHash => {
            let value = eval_partition_expr(table, part, row)?;
            let idx = linear_hash_to_bucket(&value, num as u32)? as usize;
            Ok(routed(part, idx))
        }
        PartitionMethod::Key => {
            let tuple = extract_column_tuple(table, part, row)?;
            let idx = key_hash_bucket_for_table(
                table,
                &part.columns,
                &tuple,
                part.key_algorithm.unwrap_or(2),
                num as u32,
                false,
            ) as usize;
            Ok(routed(part, idx))
        }
        PartitionMethod::LinearKey => {
            let tuple = extract_column_tuple(table, part, row)?;
            let idx = key_hash_bucket_for_table(
                table,
                &part.columns,
                &tuple,
                part.key_algorithm.unwrap_or(2),
                num as u32,
                true,
            ) as usize;
            Ok(routed(part, idx))
        }
    }?;
    route_subpartition_if_needed(table, part, row, routed)
}

fn routed(part: &PartitionInfo, index: usize) -> RoutedPartition {
    let p = &part.partitions[index];
    RoutedPartition {
        index,
        storage_id: p.storage_id,
    }
}

/// MySQL KEY partition hashing starts with `(nr1, nr2) = (1, 4)` and feeds
/// every declared field through its collation hash. ALGORITHM=1 forces the
/// MySQL 5.1 binary hash for numeric/temporal/decimal fields; ALGORITHM=2
/// (the default) uses their latin1 numeric sort charset. String fields use
/// their declared collation in both algorithms. Keeping this implementation
/// beside routing ensures DML and predicate pruning cannot select different
/// buckets.
fn key_hash_bucket_for_table(
    table: &TableSchema,
    column_names: &[String],
    values: &[Value],
    algorithm: u8,
    num_partitions: u32,
    linear: bool,
) -> u32 {
    if num_partitions == 0 {
        return 0;
    }
    let mut nr1 = 1u64;
    let mut nr2 = 4u64;
    for (column_name, value) in column_names.iter().zip(values) {
        if matches!(value, Value::Null) {
            nr1 ^= (nr1 << 1) | 1;
            continue;
        }
        let Some(column_idx) = table
            .column_name_map
            .get(&column_name.to_ascii_lowercase())
            .copied()
        else {
            continue;
        };
        let Some(column) = table.columns.get(column_idx) else {
            continue;
        };
        let bytes = mysql_key_field_bytes(value, &column.data_type, column.unsigned);
        let string_type = matches!(
            column.data_type,
            crate::storage::DataTypeKind::Char { .. }
                | crate::storage::DataTypeKind::VarChar { .. }
        );
        let enum_or_set = matches!(
            column.data_type,
            crate::storage::DataTypeKind::Enum { .. } | crate::storage::DataTypeKind::Set { .. }
        );
        if string_type {
            let text = match value {
                Value::String(text) => text.as_str(),
                Value::SmallString(text) => std::str::from_utf8(text.as_slice()).unwrap_or(""),
                _ => "",
            };
            let collation = column.collation.as_deref();
            let weights = crate::charset::weight_string(text, collation);
            let pad_space = collation
                .and_then(|name| crate::charset::resolve_collation(name).ok())
                .is_some_and(|info| info.pad_space);
            mysql_hash_sort_bytes(&weights, pad_space, false, &mut nr1, &mut nr2);
        } else {
            // 5.1 special-cases ENUM/SET to latin1 hashing while 5.5+
            // hashes their packed integer bytes as binary. Other numeric
            // fields have the inverse selection described above.
            let latin1_sort = if enum_or_set {
                algorithm == 1
            } else {
                algorithm != 1
            };
            mysql_hash_sort_bytes(&bytes, latin1_sort, latin1_sort, &mut nr1, &mut nr2);
        }
    }
    let raw = nr1 as u32;
    if linear {
        mysql_linear_bucket(raw, num_partitions)
    } else {
        raw % num_partitions
    }
}

/// SQL equality is not always a binding of one storage representation (for
/// example VARCHAR = 1 also matches '01'). Hash pruning requires that proof;
/// never feed an unconverted temporal/numeric literal into the field codec.
fn key_pruning_value(table: &TableSchema, name: &str, value: &Value) -> Option<Value> {
    use crate::storage::DataTypeKind as K;
    let column = &table.columns[table.column_index(name).ok()?];
    if matches!(value, Value::Null) {
        return Some(Value::Null);
    }
    match (&column.data_type, value) {
        (
            K::TinyInt | K::SmallInt | K::MediumInt | K::Int | K::BigInt | K::Bool,
            Value::Int(_) | Value::UInt(_),
        ) => Some(value.clone()),
        (
            K::TinyInt | K::SmallInt | K::MediumInt | K::Int | K::BigInt | K::Bool,
            Value::String(text),
        ) => {
            if column.unsigned {
                text.trim().parse::<u64>().ok().map(Value::UInt)
            } else {
                text.trim().parse::<i64>().ok().map(Value::Int)
            }
        }
        (K::VarChar { .. }, Value::String(_) | Value::SmallString(_)) => Some(value.clone()),
        (kind @ (K::Date | K::DateTime { .. } | K::Timestamp { .. } | K::Year), _) => {
            let PartitionLiteral::Value(value) =
                coerce_literal_for_column_type(&PartitionLiteral::Value(value.clone()), kind)
            else {
                return None;
            };
            matches!(
                (kind, &value),
                (K::Date, Value::Date(_))
                    | (K::DateTime { .. }, Value::DateTime(_))
                    | (K::Timestamp { .. }, Value::Timestamp(_))
                    | (K::Year, Value::Year(_))
            )
            .then_some(value)
        }
        (K::Float { .. }, Value::Float(v)) if v.is_finite() && *v != 0.0 => Some(value.clone()),
        (K::Float { .. }, Value::Double(v))
            if v.is_finite() && *v != 0.0 && f64::from(*v as f32) == *v =>
        {
            Some(Value::Float(*v as f32))
        }
        (K::Double { .. }, Value::Double(v)) if v.is_finite() && *v != 0.0 => Some(value.clone()),
        (K::Double { .. }, Value::Float(v)) if v.is_finite() && *v != 0.0 => {
            Some(Value::Double(f64::from(*v)))
        }
        // Decimal scale, ENUM/SET ordinals, padded CHAR/BINARY and floating
        // signed zero need canonical equality classes, not a lossy cast.
        // Retain all leaves until their storage hash canonicalization is
        // proven. Predicate evaluation remains fully supported.
        _ => None,
    }
}

fn mysql_hash_sort_bytes(
    bytes: &[u8],
    trim_trailing_space: bool,
    latin1_sort: bool,
    nr1: &mut u64,
    nr2: &mut u64,
) {
    let bytes = if trim_trailing_space {
        bytes.strip_suffix(&[b' ']).map_or(bytes, |_| {
            let end = bytes
                .iter()
                .rposition(|byte| *byte != b' ')
                .map_or(0, |idx| idx + 1);
            &bytes[..end]
        })
    } else {
        bytes
    };
    for byte in bytes {
        let weight = if latin1_sort {
            byte.to_ascii_uppercase()
        } else {
            *byte
        };
        let mixed = ((*nr1 & 63).wrapping_add(*nr2))
            .wrapping_mul(u64::from(weight))
            .wrapping_add(*nr1 << 8);
        *nr1 ^= mixed;
        *nr2 = (*nr2).wrapping_add(3);
    }
}

fn mysql_key_field_bytes(
    value: &Value,
    data_type: &crate::storage::DataTypeKind,
    unsigned: bool,
) -> Vec<u8> {
    use crate::storage::DataTypeKind;
    let integer = || -> u64 {
        match value {
            Value::Int(value) => *value as u64,
            Value::UInt(value) => *value,
            Value::Year(value) => u64::from(*value),
            _ => 0,
        }
    };
    match data_type {
        DataTypeKind::TinyInt | DataTypeKind::Bool => vec![integer() as u8],
        DataTypeKind::SmallInt => (integer() as u16).to_le_bytes().to_vec(),
        DataTypeKind::MediumInt => (integer() as u32).to_le_bytes()[..3].to_vec(),
        DataTypeKind::Int => (integer() as u32).to_le_bytes().to_vec(),
        DataTypeKind::BigInt => {
            if unsigned {
                integer().to_le_bytes().to_vec()
            } else {
                (integer() as i64).to_le_bytes().to_vec()
            }
        }
        DataTypeKind::Float { .. } => match value {
            Value::Float(value) => value.to_bits().to_le_bytes().to_vec(),
            Value::Double(value) => (*value as f32).to_bits().to_le_bytes().to_vec(),
            _ => 0f32.to_bits().to_le_bytes().to_vec(),
        },
        DataTypeKind::Double { .. } => match value {
            Value::Float(value) => f64::from(*value).to_bits().to_le_bytes().to_vec(),
            Value::Double(value) => value.to_bits().to_le_bytes().to_vec(),
            _ => 0f64.to_bits().to_le_bytes().to_vec(),
        },
        DataTypeKind::Decimal { .. } => value.to_display_string().into_bytes(),
        DataTypeKind::Date => match value {
            Value::Date(value) => value.to_le_bytes()[..3].to_vec(),
            _ => vec![0; 3],
        },
        DataTypeKind::Time { .. } => match value {
            Value::Time(value) => value.to_le_bytes().to_vec(),
            _ => vec![0; 8],
        },
        DataTypeKind::DateTime { .. } => match value {
            Value::DateTime(value) => value.to_le_bytes().to_vec(),
            _ => vec![0; 8],
        },
        DataTypeKind::Timestamp { .. } => match value {
            Value::Timestamp(value) => value.to_le_bytes().to_vec(),
            _ => vec![0; 8],
        },
        DataTypeKind::Year => vec![integer() as u8],
        DataTypeKind::Enum { values } => {
            let text = value.to_display_string();
            let ordinal = values
                .iter()
                .position(|candidate| candidate == &text)
                .map_or(0, |idx| idx + 1);
            if values.len() < 256 {
                vec![ordinal as u8]
            } else {
                (ordinal as u16).to_le_bytes().to_vec()
            }
        }
        DataTypeKind::Set { values } => {
            let text = value.to_display_string();
            let selected: std::collections::HashSet<&str> = text.split(',').collect();
            let mask = values.iter().enumerate().fold(0u64, |mask, (idx, item)| {
                if selected.contains(item.as_str()) {
                    mask | (1u64 << idx)
                } else {
                    mask
                }
            });
            mask.to_le_bytes().to_vec()
        }
        DataTypeKind::Bit { len } => {
            let width = len.unwrap_or(1).div_ceil(8);
            let bytes = integer().to_be_bytes();
            bytes[bytes.len().saturating_sub(width)..].to_vec()
        }
        DataTypeKind::Char { .. } | DataTypeKind::VarChar { .. } => match value {
            Value::String(value) => value.as_bytes().to_vec(),
            Value::SmallString(value) => value.as_slice().to_vec(),
            _ => Vec::new(),
        },
        DataTypeKind::Binary { len } => {
            let mut bytes = match value {
                Value::Bytes(value) => value.clone(),
                Value::String(value) => value.as_bytes().to_vec(),
                Value::SmallString(value) => value.as_slice().to_vec(),
                _ => Vec::new(),
            };
            bytes.resize(len.unwrap_or(bytes.len()), 0);
            bytes
        }
        DataTypeKind::VarBinary { .. } => match value {
            Value::Bytes(value) => value.clone(),
            Value::String(value) => value.as_bytes().to_vec(),
            Value::SmallString(value) => value.as_slice().to_vec(),
            _ => Vec::new(),
        },
        DataTypeKind::Text { .. }
        | DataTypeKind::Blob { .. }
        | DataTypeKind::Json
        | DataTypeKind::Geometry { .. }
        | DataTypeKind::Vector { .. } => Vec::new(),
    }
}

fn mysql_linear_bucket(hash: u32, num_partitions: u32) -> u32 {
    let mut mask = num_partitions.next_power_of_two().wrapping_sub(1);
    let mut bucket = hash & mask;
    if bucket >= num_partitions {
        mask >>= 1;
        bucket = hash & mask;
    }
    bucket
}

fn route_subpartition_if_needed(
    table: &TableSchema,
    part: &PartitionInfo,
    row: &[Value],
    parent: RoutedPartition,
) -> Result<RoutedPartition, QueryError> {
    let Some(sub) = part.subpartitioning.as_ref() else {
        return Ok(parent);
    };
    let parent_partition = &part.partitions[parent.index];
    let count = parent_partition.subpartitions.len();
    if count == 0 {
        return Ok(parent);
    }
    let idx = match sub.method {
        PartitionMethod::Hash => {
            let value = eval_subpartition_expr(table, part, row)?;
            hash_value_to_bucket(&value, count as u32)? as usize
        }
        PartitionMethod::LinearHash => {
            let value = eval_subpartition_expr(table, part, row)?;
            linear_hash_to_bucket(&value, count as u32)? as usize
        }
        PartitionMethod::Key => {
            let tuple = extract_subpartition_column_tuple(table, sub, row)?;
            key_hash_bucket_for_table(
                table,
                &sub.columns,
                &tuple,
                sub.key_algorithm.unwrap_or(2),
                count as u32,
                false,
            ) as usize
        }
        PartitionMethod::LinearKey => {
            let tuple = extract_subpartition_column_tuple(table, sub, row)?;
            key_hash_bucket_for_table(
                table,
                &sub.columns,
                &tuple,
                sub.key_algorithm.unwrap_or(2),
                count as u32,
                true,
            ) as usize
        }
        _ => {
            return Err(crate::storage::error(
                crate::error::catalog::ER_NOT_SUPPORTED_YET,
                "SUBPARTITION BY supports HASH/KEY only",
            ))
        }
    };
    let leaf = &parent_partition.subpartitions[idx];
    Ok(RoutedPartition {
        index: parent.index,
        storage_id: leaf.storage_id,
    })
}

fn eval_partition_expr(
    table: &TableSchema,
    part: &PartitionInfo,
    row: &[Value],
) -> Result<Value, QueryError> {
    if let Some(program) = part.expr_program.as_ref() {
        return program.eval(table, row);
    }
    if let Some(ast) = part.expr_ast.as_ref() {
        return crate::engine::expr::eval_expr_legacy(ast, table, row);
    }
    Err(crate::storage::error(
        crate::error::catalog::ER_PARTITION_FUNCTION_IS_NOT_ALLOWED,
        "partition expression has not been compiled",
    ))
}

fn eval_subpartition_expr(
    table: &TableSchema,
    part: &PartitionInfo,
    row: &[Value],
) -> Result<Value, QueryError> {
    if let Some(program) = part.sub_expr_program.as_ref() {
        return program.eval(table, row);
    }
    if let Some(ast) = part.sub_expr_ast.as_ref() {
        return crate::engine::expr::eval_expr_legacy(ast, table, row);
    }
    Err(crate::storage::error(
        crate::error::catalog::ER_PARTITION_FUNCTION_IS_NOT_ALLOWED,
        "subpartition expression has not been compiled",
    ))
}

fn extract_column_tuple(
    table: &TableSchema,
    part: &PartitionInfo,
    row: &[Value],
) -> Result<Vec<Value>, QueryError> {
    let mut out = Vec::with_capacity(part.columns.len());
    for col_name in &part.columns {
        let lower = col_name.to_ascii_lowercase();
        let idx = table.column_name_map.get(&lower).copied().ok_or_else(|| {
            crate::storage::error(
                crate::error::catalog::ER_BAD_FIELD_ERROR,
                format!("unknown partition column '{}'", col_name),
            )
        })?;
        let v = row.get(idx).cloned().unwrap_or(Value::Null);
        out.push(v);
    }
    Ok(out)
}

fn extract_subpartition_column_tuple(
    table: &TableSchema,
    sub: &crate::storage::partitioning::SubpartitioningInfo,
    row: &[Value],
) -> Result<Vec<Value>, QueryError> {
    let mut out = Vec::with_capacity(sub.columns.len());
    for col_name in &sub.columns {
        let lower = col_name.to_ascii_lowercase();
        let idx = table.column_name_map.get(&lower).copied().ok_or_else(|| {
            crate::storage::error(
                crate::error::catalog::ER_BAD_FIELD_ERROR,
                format!("unknown subpartition column '{}'", col_name),
            )
        })?;
        let v = row.get(idx).cloned().unwrap_or(Value::Null);
        out.push(v);
    }
    Ok(out)
}

fn route_range_single(part: &PartitionInfo, value: &Value) -> Result<RoutedPartition, QueryError> {
    // RANGE partitions are stored in ascending bound order. The first
    // partition whose upper bound (LESS THAN) strictly exceeds the
    // value wins. `MAXVALUE` is treated as +∞. Catalog validation guarantees
    // strict bound ordering, so partition_point gives O(log P) routing even
    // at the 8192-leaf limit.
    let idx = part.partitions.partition_point(|partition| {
        let PartitionBounds::RangeLessThan(bound) = &partition.bounds else {
            return true;
        };
        match bound.first() {
            Some(PartitionLiteral::MaxValue) => false,
            Some(PartitionLiteral::Value(bound)) => {
                compare_value(value, bound) != std::cmp::Ordering::Less
            }
            None => true,
        }
    });
    if idx < part.partitions.len() {
        return Ok(routed(part, idx));
    }
    Err(no_partition_for_value(format!("{}", value_diag(value))))
}

fn route_range_columns(
    part: &PartitionInfo,
    row: &[Value],
    collations: &[Option<&str>],
) -> Result<RoutedPartition, QueryError> {
    let idx = part.partitions.partition_point(|partition| {
        let PartitionBounds::RangeLessThan(bound) = &partition.bounds else {
            return true;
        };
        crate::storage::partitioning::compare_row_to_range_bound_with_collations(
            row, bound, collations,
        ) != std::cmp::Ordering::Less
    });
    if idx < part.partitions.len() {
        return Ok(routed(part, idx));
    }
    let diag = row.iter().map(value_diag).collect::<Vec<_>>().join(", ");
    Err(no_partition_for_value(format!("({})", diag)))
}

/// A3: resolve the collation name for each partitioning column so
/// RANGE COLUMNS / LIST COLUMNS routing can compare CHAR/VARCHAR values
/// with the column's actual collation instead of raw bytes. Columns
/// with non-string types (or no explicit collation) return None and
/// fall through to `compare_value`'s type-specific arms.
fn partition_column_collations<'a>(
    table: &'a TableSchema,
    part: &PartitionInfo,
) -> Vec<Option<&'a str>> {
    part.columns
        .iter()
        .map(|col_name| {
            let idx = *table.column_name_map.get(&col_name.to_ascii_lowercase())?;
            let col = table.columns.get(idx)?;
            match &col.data_type {
                crate::storage::DataTypeKind::Char { .. }
                | crate::storage::DataTypeKind::VarChar { .. }
                | crate::storage::DataTypeKind::Text { .. } => {
                    col.collation.as_deref().filter(|s| !s.is_empty())
                }
                _ => None,
            }
        })
        .collect()
}

fn list_partition_runtime<'a>(
    table: &TableSchema,
    part: &'a PartitionInfo,
) -> Option<&'a crate::storage::partitioning::PartitionRuntime> {
    part.runtime
        .get_or_init(|| build_list_partition_runtime(table, part))
        .as_deref()
}

fn build_list_partition_runtime(
    table: &TableSchema,
    part: &PartitionInfo,
) -> Option<Arc<crate::storage::partitioning::PartitionRuntime>> {
    if !matches!(
        part.method,
        PartitionMethod::List | PartitionMethod::ListColumns
    ) {
        return None;
    }
    let entry_count = part
        .partitions
        .iter()
        .filter_map(|partition| match &partition.bounds {
            PartitionBounds::ListIn(groups) => Some(groups.len()),
            _ => None,
        })
        .sum::<usize>();
    let arity = if part.method == PartitionMethod::List {
        1
    } else {
        part.columns.len()
    };
    let estimated = entry_count
        .saturating_mul(
            std::mem::size_of::<(Vec<u8>, usize)>()
                .saturating_mul(2)
                .saturating_add(arity.saturating_mul(24)),
        )
        .saturating_add(4096);
    let mut reservation =
        crate::engine::executor::mem_budget::ControlledMemoryCharge::try_new_optional(
            crate::engine::executor::mem_budget::ControlledMemoryCategory::DerivedCache,
            estimated,
        )?;
    let collations = partition_column_collations(table, part);
    let mut lookup = std::collections::HashMap::with_capacity(entry_count);
    for (partition_index, partition) in part.partitions.iter().enumerate() {
        let PartitionBounds::ListIn(groups) = &partition.bounds else {
            continue;
        };
        for group in groups {
            let values = if part.method == PartitionMethod::List {
                let Some(first) = group.first() else {
                    continue;
                };
                let coerced = coerce_literal_for_column_type(first, &part_column_type(part, 0));
                match coerced {
                    PartitionLiteral::Value(value) => vec![value],
                    PartitionLiteral::MaxValue => continue,
                }
            } else {
                let mut values = Vec::with_capacity(group.len());
                let mut valid = true;
                for literal in group {
                    match literal {
                        PartitionLiteral::Value(value) => values.push(value.clone()),
                        PartitionLiteral::MaxValue => {
                            valid = false;
                            break;
                        }
                    }
                }
                if !valid {
                    continue;
                }
                values
            };
            lookup.insert(list_runtime_key(&values, &collations), partition_index);
        }
    }
    let retained = lookup
        .capacity()
        .saturating_mul(std::mem::size_of::<(Vec<u8>, usize)>().saturating_mul(2))
        .saturating_add(lookup.keys().map(|key| key.capacity()).sum::<usize>());
    if retained > reservation.bytes()
        && reservation
            .try_grow(retained.saturating_sub(reservation.bytes()))
            .is_err()
    {
        return None;
    }
    Some(Arc::new(crate::storage::partitioning::PartitionRuntime {
        list_lookup: lookup,
        _reservation: reservation,
    }))
}

fn list_runtime_key(values: &[Value], collations: &[Option<&str>]) -> Vec<u8> {
    let mut out = Vec::with_capacity(values.len().saturating_mul(16));
    for (index, value) in values.iter().enumerate() {
        let collation = collations.get(index).copied().flatten();
        append_list_runtime_value(&mut out, value, collation);
    }
    out
}

fn append_list_runtime_value(out: &mut Vec<u8>, value: &Value, collation: Option<&str>) {
    let (tag, bytes) = match value {
        Value::Null => (0u8, Vec::new()),
        Value::Int(value) => (1, i128::from(*value).to_le_bytes().to_vec()),
        Value::UInt(value) => (1, i128::from(*value).to_le_bytes().to_vec()),
        Value::Year(value) => (1, i128::from(*value).to_le_bytes().to_vec()),
        Value::String(value) => (2, list_runtime_string_weight(value, collation)),
        Value::SmallString(value) => (
            2,
            list_runtime_string_weight(
                std::str::from_utf8(value.as_slice()).unwrap_or(""),
                collation,
            ),
        ),
        Value::Bytes(value) => (3, value.clone()),
        Value::Date(value) => (4, value.to_le_bytes().to_vec()),
        Value::Time(value) => (5, value.to_le_bytes().to_vec()),
        Value::DateTime(value) => (6, value.to_le_bytes().to_vec()),
        Value::Timestamp(value) => (7, value.to_le_bytes().to_vec()),
        Value::Float(value) => (8, value.to_bits().to_le_bytes().to_vec()),
        Value::Double(value) => (9, value.to_bits().to_le_bytes().to_vec()),
        Value::Decimal(value) => (10, value.as_bytes().to_vec()),
        Value::DecimalCompact(value, scale) => {
            let mut bytes = value.to_le_bytes().to_vec();
            bytes.push(*scale);
            (10, bytes)
        }
        Value::Decimal128(..) | Value::DecimalWide(..) => {
            (10, value.to_display_string().into_bytes())
        }
        Value::Json(value) | Value::Geometry(value) => (11, value.clone()),
        Value::Vector(value) => (11, value.encode()),
    };
    out.push(tag);
    out.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
    out.extend_from_slice(&bytes);
}

fn list_runtime_string_weight(value: &str, collation: Option<&str>) -> Vec<u8> {
    let value = if collation
        .and_then(|name| crate::charset::resolve_collation(name).ok())
        .is_some_and(|info| info.pad_space)
    {
        value.trim_end_matches(' ')
    } else {
        value
    };
    crate::charset::weight_string(value, collation)
}

fn route_list_single(
    table: &TableSchema,
    part: &PartitionInfo,
    value: &Value,
) -> Result<RoutedPartition, QueryError> {
    if let Some(runtime) = list_partition_runtime(table, part) {
        let key = list_runtime_key(std::slice::from_ref(value), &[None]);
        if let Some(index) = runtime.list_lookup.get(&key) {
            return Ok(routed(part, *index));
        }
        return Err(no_partition_for_value(value_diag(value)));
    }
    for (idx, p) in part.partitions.iter().enumerate() {
        let PartitionBounds::ListIn(items) = &p.bounds else {
            continue;
        };
        for item in items {
            if let Some(first) = item.first() {
                let coerced = coerce_literal_for_column_type(first, &part_column_type(part, 0));
                if matches_list_literal(value, &coerced) {
                    return Ok(routed(part, idx));
                }
            }
        }
    }
    Err(no_partition_for_value(value_diag(value)))
}

fn route_list_columns(
    table: &TableSchema,
    part: &PartitionInfo,
    row: &[Value],
    collations: &[Option<&str>],
) -> Result<RoutedPartition, QueryError> {
    if let Some(runtime) = list_partition_runtime(table, part) {
        let key = list_runtime_key(row, collations);
        if let Some(index) = runtime.list_lookup.get(&key) {
            return Ok(routed(part, *index));
        }
        let diag = row.iter().map(value_diag).collect::<Vec<_>>().join(", ");
        return Err(no_partition_for_value(format!("({})", diag)));
    }
    for (idx, p) in part.partitions.iter().enumerate() {
        let PartitionBounds::ListIn(items) = &p.bounds else {
            continue;
        };
        for tuple in items {
            if tuple.len() != row.len() {
                continue;
            }
            let mut ok = true;
            for (pos, lit) in tuple.iter().enumerate() {
                let coll = collations.get(pos).copied().flatten();
                if !matches_list_literal_with_collation(&row[pos], lit, coll) {
                    ok = false;
                    break;
                }
            }
            if ok {
                return Ok(routed(part, idx));
            }
        }
    }
    let diag = row.iter().map(value_diag).collect::<Vec<_>>().join(", ");
    Err(no_partition_for_value(format!("({})", diag)))
}

fn matches_list_literal(value: &Value, lit: &PartitionLiteral) -> bool {
    matches_list_literal_with_collation(value, lit, None)
}

fn matches_list_literal_with_collation(
    value: &Value,
    lit: &PartitionLiteral,
    collation: Option<&str>,
) -> bool {
    match lit {
        PartitionLiteral::MaxValue => false,
        PartitionLiteral::Value(lv) => {
            if matches!(value, Value::Null) && matches!(lv, Value::Null) {
                return true;
            }
            crate::storage::partitioning::compare_value_with_collation(value, lv, collation)
                == std::cmp::Ordering::Equal
        }
    }
}

/// LIST (non-COLUMNS) partitioning is integer-only, so its list literals
/// are coerced to an integer before comparison. LIST COLUMNS routes
/// through `route_list_columns`, which compares via `compare_value` and
/// needs no up-front coercion — hence this only ever serves the integer
/// LIST path and always yields `BigInt`.
fn part_column_type(_part: &PartitionInfo, _pos: usize) -> crate::storage::DataTypeKind {
    crate::storage::DataTypeKind::BigInt
}

fn value_diag(v: &Value) -> String {
    match v {
        Value::Null => "NULL".to_string(),
        Value::Int(n) => n.to_string(),
        Value::UInt(n) => n.to_string(),
        Value::String(s) => format!("'{}'", s),
        Value::SmallString(b) => format!("'{}'", unsafe { std::str::from_utf8_unchecked(b) }),
        Value::Bytes(b) => {
            let mut out = String::with_capacity(2 + b.len() * 2);
            out.push_str("0x");
            crate::util::hex::append_lower(&mut out, b);
            out
        }
        Value::Float(f) => f.to_string(),
        Value::Double(f) => f.to_string(),
        Value::Decimal(s) => s.clone(),
        Value::DecimalCompact(m, _) => m.to_string(),
        Value::Decimal128(..) | Value::DecimalWide(..) => v.to_display_string(),
        Value::Date(d) => format!("date({})", d),
        Value::Time(t) => format!("time({})", t),
        Value::DateTime(t) => format!("datetime({})", t),
        Value::Timestamp(t) => format!("timestamp({})", t),
        Value::Year(y) => format!("year({})", y),
        Value::Json(b) => format!("json({})", crate::engine::json::to_text(b)),
        Value::Geometry(b) => format!("geometry({})", crate::engine::geo::to_text(b)),
        Value::Vector(b) => format!("vector({})", crate::engine::vector::to_text(b)),
    }
}

/// Compute the set of candidate partition indices that *could* match
/// any row satisfying the given column-equality constraints. Used by
/// the planner for static partition pruning.
///
/// `equalities[i]` corresponds to column `i` in
/// `partition_info.columns` (for RANGE COLUMNS / LIST COLUMNS / KEY)
/// or to the single partition expression (RANGE / LIST / HASH). The
/// vector length should match what the variant expects; missing
/// positions are treated as unknown (`None`).
///
/// Returns `None` to mean "no pruning — visit every partition";
/// returns `Some(set)` to mean "only these partitions need to be
/// scanned". A set with the full partition count is collapsed back to
/// `None` so callers don't pay the partition-array iteration cost for
/// a no-op prune.
pub fn prune_partitions_for_eq(
    table: &TableSchema,
    part: &PartitionInfo,
    equalities: &[Option<Value>],
) -> Option<Vec<usize>> {
    if equalities.is_empty() || equalities.iter().all(Option::is_none) {
        return None;
    }
    let total = part.partitions.len();
    let collations = partition_column_collations(table, part);
    let result = match part.method {
        PartitionMethod::Range => equalities
            .first()
            .and_then(|opt| opt.as_ref())
            .and_then(|value| route_range_single(part, value).ok())
            .map(|routed| vec![routed.index]),
        PartitionMethod::RangeColumns => {
            let prefix: Vec<Value> = equalities
                .iter()
                .take(part.columns.len())
                .take_while(|value| value.is_some())
                .map(|value| value.clone().unwrap())
                .collect();
            if prefix.is_empty() {
                None
            } else if prefix.len() == part.columns.len() {
                route_range_columns(part, &prefix, &collations)
                    .ok()
                    .map(|routed| vec![routed.index])
            } else {
                // The unknown suffix ranges from NULL to +infinity.
                // Search both prefix fences; an equal prefix can span
                // several adjacent partitions. Both searches are O(log P).
                let compare = |partition: &crate::storage::partitioning::Partition| {
                    let PartitionBounds::RangeLessThan(bound) = &partition.bounds else {
                        unreachable!("validated RANGE COLUMNS bounds")
                    };
                    crate::storage::partitioning::compare_row_to_range_bound_with_collations(
                        &prefix,
                        bound,
                        &collations,
                    )
                };
                let first = part
                    .partitions
                    .partition_point(|p| compare(p) == std::cmp::Ordering::Greater);
                let last = part
                    .partitions
                    .partition_point(|p| compare(p) != std::cmp::Ordering::Less);
                Some((first..last.saturating_add(1).min(total)).collect())
            }
        }
        PartitionMethod::List => equalities
            .first()
            .and_then(|opt| opt.as_ref())
            .and_then(|value| route_list_single(table, part, value).ok())
            .map(|routed| vec![routed.index]),
        PartitionMethod::ListColumns => {
            if equalities.len() != part.columns.len() {
                None
            } else if equalities.iter().all(Option::is_some) {
                let row: Vec<Value> = equalities.iter().map(|e| e.clone().unwrap()).collect();
                route_list_columns(table, part, &row, &collations)
                    .ok()
                    .map(|routed| vec![routed.index])
            } else {
                let mut matched = Vec::new();
                for (idx, partition) in part.partitions.iter().enumerate() {
                    let PartitionBounds::ListIn(groups) = &partition.bounds else {
                        continue;
                    };
                    if groups
                        .iter()
                        .any(|tuple| list_tuple_matches_equalities(tuple, equalities, &collations))
                    {
                        matched.push(idx);
                    }
                }
                Some(matched)
            }
        }
        PartitionMethod::Hash | PartitionMethod::LinearHash => equalities
            .first()
            .and_then(|opt| opt.as_ref())
            .and_then(|value| {
                let n = total as u32;
                let bucket = if part.method == PartitionMethod::Hash {
                    hash_value_to_bucket(value, n)
                } else {
                    linear_hash_to_bucket(value, n)
                };
                // A7: an integer-typed WHERE constant is guaranteed by
                // upstream constant-folding, but a mistyped literal (e.g.
                // `WHERE hash_col = 'x'`) yields non-integer here; skip
                // pruning rather than propagate — full-scan is safe.
                bucket.ok().map(|b| vec![b as usize])
            }),
        PartitionMethod::Key | PartitionMethod::LinearKey => {
            if equalities.iter().any(Option::is_none) || equalities.len() != part.columns.len() {
                None
            } else {
                let tuple: Vec<Value> = part
                    .columns
                    .iter()
                    .zip(equalities)
                    .map(|(name, value)| key_pruning_value(table, name, value.as_ref()?))
                    .collect::<Option<_>>()?;
                let n = total as u32;
                let idx = key_hash_bucket_for_table(
                    table,
                    &part.columns,
                    &tuple,
                    part.key_algorithm.unwrap_or(2),
                    n,
                    part.method == PartitionMethod::LinearKey,
                );
                Some(vec![idx as usize])
            }
        }
    };
    result.and_then(|mut v| {
        v.sort_unstable();
        v.dedup();
        if v.is_empty() || v.len() == total {
            None
        } else {
            Some(v)
        }
    })
}

fn list_tuple_matches_equalities(
    tuple: &[PartitionLiteral],
    equalities: &[Option<Value>],
    collations: &[Option<&str>],
) -> bool {
    if tuple.len() != equalities.len() {
        return false;
    }
    tuple
        .iter()
        .zip(equalities.iter())
        .enumerate()
        .all(|(position, (literal, equality))| match equality {
            None => true,
            Some(value) => match literal {
                PartitionLiteral::Value(tuple_value) => {
                    crate::storage::partitioning::compare_value_with_collation(
                        tuple_value,
                        value,
                        collations.get(position).copied().flatten(),
                    )
                    .is_eq()
                }
                PartitionLiteral::MaxValue => false,
            },
        })
}

/// Compute pruned partitions for a `WHERE col >= lo AND col < hi`
/// style range bound on a single-expression RANGE partitioning. The
/// bounds are inclusive at the low end and exclusive at the high end
/// to match MySQL's `VALUES LESS THAN`.
///
/// `lo` / `hi` are `None` for an open bound. Returns the index range
/// `[first_partition, last_partition]` (both inclusive) that may
/// contain matching rows.
fn prune_partitions_for_range(
    part: &PartitionInfo,
    lo: Option<&Value>,
    hi: Option<&Value>,
    hi_inclusive: bool,
) -> Option<Vec<usize>> {
    if !matches!(part.method, PartitionMethod::Range) {
        return None;
    }
    let total = part.partitions.len();
    if total == 0 {
        return None;
    }
    let first_idx = match lo {
        Some(value) => route_range_single(part, value).ok()?.index,
        None => 0,
    };
    let last_idx = match hi {
        Some(value) => {
            let routed = route_range_single(part, value).ok()?.index;
            // For an exclusive upper predicate exactly on a partition
            // boundary (`col < 10` with `p0 VALUES LESS THAN 10`), the
            // partition containing 10 cannot contribute. Values inside a
            // partition still require that partition (`col < 15`).
            if !hi_inclusive && routed > 0 {
                let previous_upper = match &part.partitions[routed - 1].bounds {
                    PartitionBounds::RangeLessThan(bound) => bound.first(),
                    _ => None,
                };
                if previous_upper.is_some_and(|bound| match bound {
                    PartitionLiteral::Value(bound) => compare_value(value, bound).is_eq(),
                    PartitionLiteral::MaxValue => false,
                }) {
                    Some(routed - 1)
                } else {
                    Some(routed)
                }
            } else if !hi_inclusive && routed == 0 {
                let first_upper = match &part.partitions[0].bounds {
                    PartitionBounds::RangeLessThan(bound) => bound.first(),
                    _ => None,
                };
                if first_upper.is_some_and(|bound| match bound {
                    PartitionLiteral::Value(bound) => compare_value(value, bound).is_eq(),
                    PartitionLiteral::MaxValue => false,
                }) {
                    // `col < first_upper` still uses p0; only a catalog with
                    // an explicit lower partition boundary could be empty,
                    // which RANGE does not have.
                    Some(0)
                } else {
                    Some(routed)
                }
            } else {
                Some(routed)
            }
        }
        None => Some(total.saturating_sub(1)),
    };
    let last_idx = last_idx?;
    if first_idx > last_idx {
        return Some(Vec::new());
    }
    let mut out: Vec<usize> = (first_idx..=last_idx.min(total - 1)).collect();
    out.dedup();
    if out.is_empty() || out.len() == total {
        None
    } else {
        Some(out)
    }
}

/// Prefix range pruning for `RANGE COLUMNS (c0, c1, ...)`: a range
/// constraint on the *first* partition column `c0` prunes to the
/// partitions whose first bound element can overlap `[lo, hi)`.
///
/// The partition tuples are compared lexicographically, so to stay
/// conservative (never under-include) we route the prefix padded with
/// `NULL` (the minimum) for the trailing columns:
///
/// - low bound `c0 >= lo` / `c0 > lo`: route `(lo, NULL, …)` — a tuple
///   `(lo, anything)` is `>= (lo, NULL)`, so its partition is the first
///   that can hold `c0 == lo`.
/// - high bound `c0 < hi`: route `(hi, NULL, …)` — every tuple with
///   `c0 < hi` is `< (hi, NULL)`, so it lands at or before that
///   partition. Inclusive `c0 <= hi` reaches through every boundary
///   sharing `hi` and into the partition following that boundary run.
///
/// Over-inclusion is always safe; the scanner just visits a few extra
/// partitions.
pub fn prune_partitions_for_range_columns_prefix(
    table: &TableSchema,
    part: &PartitionInfo,
    lo: Option<&Value>,
    hi: Option<&Value>,
    hi_inclusive: bool,
) -> Option<Vec<usize>> {
    if part.method != PartitionMethod::RangeColumns {
        return None;
    }
    let total = part.partitions.len();
    let arity = part.columns.len();
    if total == 0 || arity == 0 {
        return None;
    }
    let collations = partition_column_collations(table, part);
    let padded = |head: &Value| -> Vec<Value> {
        let mut tuple = Vec::with_capacity(arity);
        tuple.push(head.clone());
        while tuple.len() < arity {
            tuple.push(Value::Null);
        }
        tuple
    };
    let first_idx = match lo {
        Some(v) => {
            route_range_columns(part, &padded(v), &collations)
                .ok()?
                .index
        }
        None => 0,
    };
    let last_idx = match hi {
        Some(v) => {
            let mut idx = route_range_columns(part, &padded(v), &collations)
                .ok()?
                .index;
            if hi_inclusive {
                // Every boundary sharing the leading value can split that
                // value again on trailing columns. Include the partition
                // after the entire equal-prefix run, not just its first one.
                while idx + 1 < total {
                    let PartitionBounds::RangeLessThan(bound) = &part.partitions[idx].bounds else {
                        break;
                    };
                    let Some(PartitionLiteral::Value(head)) = bound.first() else {
                        break;
                    };
                    if crate::storage::partitioning::compare_value_with_collation(
                        head,
                        v,
                        collations.first().copied().flatten(),
                    ) != std::cmp::Ordering::Equal
                    {
                        break;
                    }
                    idx += 1;
                }
            }
            idx
        }
        None => total - 1,
    };
    if first_idx > last_idx {
        return Some(Vec::new());
    }
    let out: Vec<usize> = (first_idx..=last_idx).collect();
    if out.is_empty() || out.len() == total {
        None
    } else {
        Some(out)
    }
}

/// Top-level entry that mirrors what the planner needs: given a
/// WHERE/JOIN filter expression and a partitioned table, return the
/// `storage_id`s the scan should visit. `None` means "no pruning"
/// (visit every partition); `Some([])` is a valid contradiction result and
/// must not be widened back to every partition.
pub fn pruned_storage_ids_from_filter(
    table: &TableSchema,
    filter: Option<&crate::parser::Expr>,
) -> Option<Vec<u64>> {
    let part = table.partitioning.as_ref()?;
    let filter = filter?;
    if let crate::parser::Expr::Binary {
        op: crate::parser::BinaryOp::Or,
        left,
        right,
    } = filter
    {
        let mut ids = pruned_storage_ids_from_filter(table, Some(left))?;
        ids.extend(pruned_storage_ids_from_filter(table, Some(right))?);
        ids.sort_unstable();
        ids.dedup();
        return (ids.len() < part.storage_ids().len()).then_some(ids);
    }
    let pruner = FilterPruner::new(table, part);
    let pruned = pruner.compute_expr(filter);
    // A full equality binding for HASH/KEY subpartition columns determines
    // exactly one physical leaf below every surviving parent partition.  Do
    // this after parent pruning so both dimensions share the same predicate
    // walk without changing the cached physical plan topology.
    let subpartition_bucket = prune_subpartition_bucket(table, part, filter);
    if pruned.is_none() && subpartition_bucket.is_none() {
        return None;
    }
    let pruned = pruned.unwrap_or_else(|| (0..part.partitions.len()).collect());
    let mut storage_ids: Vec<u64> = Vec::new();
    for idx in pruned {
        let selected = match subpartition_bucket {
            Some(bucket) => part
                .partitions
                .get(idx)
                .and_then(|parent| parent.subpartitions.get(bucket))
                .map(|leaf| vec![leaf.storage_id])
                .unwrap_or_else(|| part.storage_ids_for_partition_index(idx)),
            None => part.storage_ids_for_partition_index(idx),
        };
        for storage_id in selected {
            // Parent indices are deduplicated by FilterPruner and physical
            // leaf ids are globally unique inside one table, so this append
            // is duplicate-free without an O(P) contains scan per leaf.
            storage_ids.push(storage_id);
        }
    }
    Some(storage_ids)
}

/// Return the single subpartition bucket implied by literal equalities in a
/// conjunction. Bound parameters and fully-bound compiled HASH expressions
/// use the same evaluation as row routing. Unknown bindings retain every
/// leaf below the parent set; top-level OR is combined by the caller.
fn prune_subpartition_bucket(
    table: &TableSchema,
    part: &PartitionInfo,
    filter: &crate::parser::Expr,
) -> Option<usize> {
    let sub = part.subpartitioning.as_ref()?;
    let leaf_count = part
        .partitions
        .first()?
        .subpartitions
        .len()
        .try_into()
        .ok()?;
    if leaf_count == 0 {
        return None;
    }

    let mut equalities = std::collections::HashMap::<usize, Value>::new();
    for conjunct in collect_and_conjuncts(filter) {
        if let Some((column, value)) = extract_eq_on_column(table, conjunct) {
            equalities.insert(column, value);
        } else if let Some(column) = extract_is_null_on_column(table, conjunct) {
            equalities.insert(column, Value::Null);
        }
    }

    let bucket = match sub.method {
        PartitionMethod::Hash | PartitionMethod::LinearHash => {
            let value =
                bound_partition_expression(table, part.sub_expr_program.as_deref()?, filter)?;
            if sub.method == PartitionMethod::LinearHash {
                linear_hash_to_bucket(&value, leaf_count).ok()?
            } else {
                hash_value_to_bucket(&value, leaf_count).ok()?
            }
        }
        PartitionMethod::Key | PartitionMethod::LinearKey => {
            let mut tuple = Vec::with_capacity(sub.columns.len());
            for name in &sub.columns {
                let column = table
                    .column_name_map
                    .get(&name.to_ascii_lowercase())
                    .copied()?;
                tuple.push(key_pruning_value(table, name, equalities.get(&column)?)?);
            }
            key_hash_bucket_for_table(
                table,
                &sub.columns,
                &tuple,
                sub.key_algorithm.unwrap_or(2),
                leaf_count,
                sub.method == PartitionMethod::LinearKey,
            )
        }
        _ => return None,
    };
    usize::try_from(bucket).ok()
}

/// Evaluate only when every input is known. This is forward evaluation,
/// not inversion: no monotonicity assumption is made for arbitrary functions.
fn bound_partition_expression(
    table: &TableSchema,
    program: &crate::engine::expr_program::CompiledTableExpr,
    filter: &crate::parser::Expr,
) -> Option<Value> {
    let mut dependencies = std::collections::HashSet::new();
    if !program.try_collect_referenced_columns(&mut dependencies) {
        return None;
    }
    let mut row = vec![Value::Null; table.columns.len()];
    for conjunct in collect_and_conjuncts(filter) {
        let binding = extract_eq_on_column(table, conjunct)
            .or_else(|| extract_is_null_on_column(table, conjunct).map(|idx| (idx, Value::Null)));
        let Some((idx, value)) = binding else {
            continue;
        };
        if !dependencies.contains(&idx) {
            continue;
        }
        // Integer/temporal bindings preserve the partition expression's
        // input domain. Do not route text/numeric coercion comparisons on
        // character columns: SQL equality can match several stored strings.
        use crate::storage::DataTypeKind as K;
        let value = match (&table.columns[idx].data_type, value) {
            (_, Value::Null) => Value::Null,
            (
                K::TinyInt | K::SmallInt | K::MediumInt | K::Int | K::BigInt | K::Bool,
                value @ (Value::Int(_) | Value::UInt(_)),
            ) => value,
            (kind @ (K::Date | K::DateTime { .. } | K::Timestamp { .. } | K::Year), value) => {
                let PartitionLiteral::Value(value) =
                    coerce_literal_for_column_type(&PartitionLiteral::Value(value), kind)
                else {
                    return None;
                };
                if matches!(
                    value,
                    Value::String(_) | Value::Double(_) | Value::Decimal(_)
                ) {
                    return None;
                }
                value
            }
            _ => return None,
        };
        row[idx] = value;
        dependencies.remove(&idx);
    }
    if !dependencies.is_empty() {
        return None;
    }
    program.eval(table, &row).ok()
}

/// State carried while walking the WHERE expression. Constructed once
/// per scan so the (small) AST walk doesn't re-resolve column indices
/// every step.
struct FilterPruner<'a> {
    table: &'a TableSchema,
    part: &'a PartitionInfo,
    /// Column indices of the partition columns (RANGE COLUMNS / LIST
    /// COLUMNS / KEY). Empty for the expression-based variants
    /// (Range / List / Hash), where we instead match on the literal
    /// SQL text of the partition expression.
    col_indices: Vec<Option<usize>>,
}

impl<'a> FilterPruner<'a> {
    fn expression_constraints(&self, filter: &crate::parser::Expr) -> Option<Vec<usize>> {
        use crate::parser::{BinaryOp, Expr};
        let program = self.part.expr_program.as_deref()?;
        if let Some(value) = bound_partition_expression(self.table, program, filter) {
            return prune_partitions_for_eq(self.table, self.part, &[Some(value)]);
        }
        let mut result = None;
        // Enumerate one finite input at a time; all remaining expression
        // inputs must already be bound. Bound work rather than constructing
        // an exponential Cartesian product of multiple IN lists.
        for conjunct in collect_and_conjuncts(filter) {
            if let Expr::InList {
                expr,
                list,
                negated: false,
            } = conjunct
            {
                if list.len() > 256 || column_index(self.table, expr).is_none() {
                    continue;
                }
                let mut union = Vec::new();
                let mut proven = true;
                for item in list {
                    let bound = Expr::Binary {
                        left: Box::new(filter.clone()),
                        op: BinaryOp::And,
                        right: Box::new(Expr::Binary {
                            left: expr.clone(),
                            op: BinaryOp::Eq,
                            right: Box::new(item.clone()),
                        }),
                    };
                    let Some(value) = bound_partition_expression(self.table, program, &bound)
                    else {
                        proven = false;
                        break;
                    };
                    let Some(local) =
                        prune_partitions_for_eq(self.table, self.part, &[Some(value)])
                    else {
                        proven = false;
                        break;
                    };
                    union.extend(local);
                }
                if proven {
                    union.sort_unstable();
                    union.dedup();
                    if union.len() < self.part.partitions.len() {
                        result = intersect(result, Some(union));
                    }
                }
            }
        }
        // These functions are nondecreasing on DATE/DATETIME. Transform
        // both endpoints inclusively: a strict timestamp upper bound can
        // still share its day/year bucket with matching rows. TIMESTAMP is
        // excluded because session timezone/DST needs a separate proof.
        if self.part.method == PartitionMethod::Range {
            if let Some(Expr::Function { name, args, .. }) = self.part.expr_ast.as_ref() {
                if args.len() == 1
                    && ["to_days", "to_seconds", "year"]
                        .iter()
                        .any(|f| name.eq_ignore_ascii_case(f))
                {
                    if let Some(idx) = column_index(self.table, &args[0]) {
                        if matches!(
                            self.table.columns[idx].data_type,
                            crate::storage::DataTypeKind::Date
                                | crate::storage::DataTypeKind::DateTime { .. }
                        ) {
                            for conjunct in collect_and_conjuncts(filter) {
                                if let Some((lo, _, hi, _)) =
                                    extract_range_on_column(self.table, conjunct, idx)
                                {
                                    let evaluate = |value: Value| {
                                        bound_partition_expression(
                                            self.table,
                                            program,
                                            &Expr::Binary {
                                                left: Box::new(args[0].clone()),
                                                op: BinaryOp::Eq,
                                                right: Box::new(Expr::BoundValue(value)),
                                            },
                                        )
                                    };
                                    let lo = lo.and_then(evaluate);
                                    let hi = hi.and_then(evaluate);
                                    if lo.is_some() || hi.is_some() {
                                        result = intersect(
                                            result,
                                            prune_partitions_for_range(
                                                self.part,
                                                lo.as_ref(),
                                                hi.as_ref(),
                                                true,
                                            ),
                                        );
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
        result
    }

    fn new(table: &'a TableSchema, part: &'a PartitionInfo) -> Self {
        let col_indices = part
            .columns
            .iter()
            .map(|name| {
                let lower = name.to_ascii_lowercase();
                table.column_name_map.get(&lower).copied()
            })
            .collect();
        Self {
            table,
            part,
            col_indices,
        }
    }

    fn compute(&self, expr: &crate::parser::Expr) -> Option<Vec<usize>> {
        self.compute_conjunction(expr)
    }

    fn compute_expr(&self, expr: &crate::parser::Expr) -> Option<Vec<usize>> {
        let candidate_count = self.part.partitions.len();
        if let crate::parser::Expr::Binary {
            op: crate::parser::BinaryOp::Or,
            left,
            right,
        } = expr
        {
            let mut left_set = self.compute_expr(left)?;
            let right_set = self.compute_expr(right)?;
            left_set.extend(right_set);
            left_set.sort_unstable();
            left_set.dedup();
            return if left_set.len() < candidate_count {
                Some(left_set)
            } else {
                None
            };
        }
        self.compute(expr)
    }

    fn compute_conjunction(&self, expr: &crate::parser::Expr) -> Option<Vec<usize>> {
        let candidate_count = self.part.partitions.len();
        // Collect all top-level AND conjuncts so a WHERE like
        // `a = 1 AND b = 2 AND c < 10` lets us combine RANGE-style
        // pruning with multi-column equality constraints.
        let conjuncts = collect_and_conjuncts(expr);
        // For Range/List/Hash (single-expression methods), only
        // equalities/ranges that match the partition expression text
        // contribute; we accept either a bare column ref or a CompoundIdentifier
        // ending in the partition column name.
        let target_expr_sql = self.part.expr_sql.trim().to_ascii_lowercase();
        let single_expr_col = if self.part.method.uses_columns() {
            None
        } else {
            // Strip surrounding parens / whitespace and try to resolve
            // as a single column. Only the trivial `PARTITION BY
            // RANGE (col_name)` form gets pruning via column refs.
            self.table.column_name_map.get(&target_expr_sql).copied()
        };
        let range_collation = if self.part.method.uses_columns() {
            partition_column_collations(self.table, self.part)
                .into_iter()
                .next()
                .flatten()
        } else {
            single_expr_col
                .and_then(|idx| self.table.columns.get(idx))
                .and_then(|column| column.collation.as_deref())
        };

        let mut equalities: Vec<Option<crate::types::Value>> = if self.part.method.uses_columns() {
            vec![None; self.part.columns.len()]
        } else {
            vec![None; 1]
        };
        let mut range_lo: Option<crate::types::Value> = None;
        let mut range_lo_inclusive = false;
        let mut range_hi: Option<crate::types::Value> = None;
        let mut range_hi_inclusive = false;
        let mut in_list: Option<Vec<crate::types::Value>> = None;
        let mut result: Option<Vec<usize>> =
            if !self.part.method.uses_columns() && single_expr_col.is_none() {
                self.expression_constraints(expr)
            } else {
                None
            };

        for c in conjuncts {
            if matches!(
                c,
                crate::parser::Expr::Binary {
                    op: crate::parser::BinaryOp::Or,
                    ..
                }
            ) {
                if let Some(set) = self.compute_expr(c) {
                    result = intersect(result, Some(set));
                }
                continue;
            }
            // Equality on a partition column: store into the right slot.
            if let Some((col_idx, value)) = extract_eq_on_column(self.table, c) {
                if self.part.method.uses_columns() {
                    if let Some(pos) = self
                        .col_indices
                        .iter()
                        .position(|maybe| matches!(maybe, Some(idx) if *idx == col_idx))
                    {
                        equalities[pos] = Some(value);
                    }
                } else if Some(col_idx) == single_expr_col {
                    equalities[0] = Some(value);
                }
                continue;
            }
            if let Some(col_idx) = extract_is_null_on_column(self.table, c) {
                if self.part.method.uses_columns() {
                    if let Some(pos) = self
                        .col_indices
                        .iter()
                        .position(|maybe| matches!(maybe, Some(idx) if *idx == col_idx))
                    {
                        equalities[pos] = Some(Value::Null);
                    }
                } else if Some(col_idx) == single_expr_col {
                    equalities[0] = Some(Value::Null);
                }
                continue;
            }
            // Range / IN on the single-expression partition column.
            if let Some(col_idx) = single_expr_col {
                if let Some((lo, lo_inc, hi, hi_inc)) =
                    extract_range_on_column(self.table, c, col_idx)
                {
                    if let Some(v) = lo {
                        tighten_lower_bound(
                            &mut range_lo,
                            &mut range_lo_inclusive,
                            v,
                            lo_inc,
                            range_collation,
                        );
                    }
                    if let Some(v) = hi {
                        tighten_upper_bound(
                            &mut range_hi,
                            &mut range_hi_inclusive,
                            v,
                            hi_inc,
                            range_collation,
                        );
                    }
                    continue;
                }
                if let Some(values) = extract_in_list_on_column(self.table, c, col_idx) {
                    in_list = Some(values);
                    continue;
                }
            }
            // RANGE COLUMNS prefix range: a range on the *first* partition
            // column prunes lexicographically. Only the leading column
            // contributes (a range on a trailing column needs the leading
            // one pinned, which the equality path already covers).
            if self.part.method == PartitionMethod::RangeColumns {
                if let Some(Some(first_col)) = self.col_indices.first() {
                    if let Some((lo, lo_inc, hi, hi_inc)) =
                        extract_range_on_column(self.table, c, *first_col)
                    {
                        if let Some(v) = lo {
                            tighten_lower_bound(
                                &mut range_lo,
                                &mut range_lo_inclusive,
                                v,
                                lo_inc,
                                range_collation,
                            );
                        }
                        if let Some(v) = hi {
                            tighten_upper_bound(
                                &mut range_hi,
                                &mut range_hi_inclusive,
                                v,
                                hi_inc,
                                range_collation,
                            );
                        }
                        continue;
                    }
                }
            }
        }

        // Combine the gathered constraints.
        if bounds_are_contradictory(
            range_lo.as_ref(),
            range_lo_inclusive,
            range_hi.as_ref(),
            range_hi_inclusive,
            range_collation,
        ) || equalities
            .first()
            .and_then(Option::as_ref)
            .is_some_and(|value| {
                value_violates_bounds(
                    value,
                    range_lo.as_ref(),
                    range_lo_inclusive,
                    range_hi.as_ref(),
                    range_hi_inclusive,
                    range_collation,
                )
            })
        {
            return Some(Vec::new());
        }
        if equalities.iter().any(Option::is_some) {
            if let Some(set) = prune_partitions_for_eq(self.table, self.part, &equalities) {
                result = intersect(result, Some(set));
            }
        }
        if let Some(values) = in_list {
            if let Some(set) = prune_partitions_for_in_list(self.table, self.part, &values) {
                result = intersect(result, Some(set));
            }
        }
        if matches!(self.part.method, PartitionMethod::Range)
            && (range_lo.is_some() || range_hi.is_some())
        {
            let lo_ref = range_lo.as_ref();
            let hi_ref = range_hi.as_ref();
            if let Some(set) =
                prune_partitions_for_range(self.part, lo_ref, hi_ref, range_hi_inclusive)
            {
                let _ = range_lo_inclusive;
                result = intersect(result, Some(set));
            }
        }
        if matches!(self.part.method, PartitionMethod::RangeColumns)
            && (range_lo.is_some() || range_hi.is_some())
        {
            let _ = range_lo_inclusive;
            if let Some(set) = prune_partitions_for_range_columns_prefix(
                self.table,
                self.part,
                range_lo.as_ref(),
                range_hi.as_ref(),
                range_hi_inclusive,
            ) {
                result = intersect(result, Some(set));
            }
        }
        // A full set means no pruning. An empty intersection is a valid
        // contradiction and must remain `Some([])` so no heap is opened.
        match result {
            Some(set) if set.len() < candidate_count => Some(set),
            _ => None,
        }
    }
}

fn tighten_lower_bound(
    current: &mut Option<Value>,
    current_inclusive: &mut bool,
    candidate: Value,
    candidate_inclusive: bool,
    collation: Option<&str>,
) {
    match current.as_ref().map(|value| {
        crate::storage::partitioning::compare_value_with_collation(&candidate, value, collation)
    }) {
        None | Some(std::cmp::Ordering::Greater) => {
            *current = Some(candidate);
            *current_inclusive = candidate_inclusive;
        }
        Some(std::cmp::Ordering::Equal) => {
            *current_inclusive &= candidate_inclusive;
        }
        Some(std::cmp::Ordering::Less) => {}
    }
}

fn tighten_upper_bound(
    current: &mut Option<Value>,
    current_inclusive: &mut bool,
    candidate: Value,
    candidate_inclusive: bool,
    collation: Option<&str>,
) {
    match current.as_ref().map(|value| {
        crate::storage::partitioning::compare_value_with_collation(&candidate, value, collation)
    }) {
        None | Some(std::cmp::Ordering::Less) => {
            *current = Some(candidate);
            *current_inclusive = candidate_inclusive;
        }
        Some(std::cmp::Ordering::Equal) => {
            *current_inclusive &= candidate_inclusive;
        }
        Some(std::cmp::Ordering::Greater) => {}
    }
}

fn bounds_are_contradictory(
    lo: Option<&Value>,
    lo_inclusive: bool,
    hi: Option<&Value>,
    hi_inclusive: bool,
    collation: Option<&str>,
) -> bool {
    let (Some(lo), Some(hi)) = (lo, hi) else {
        return false;
    };
    match crate::storage::partitioning::compare_value_with_collation(lo, hi, collation) {
        std::cmp::Ordering::Greater => true,
        std::cmp::Ordering::Equal => !lo_inclusive || !hi_inclusive,
        std::cmp::Ordering::Less => false,
    }
}

fn value_violates_bounds(
    value: &Value,
    lo: Option<&Value>,
    lo_inclusive: bool,
    hi: Option<&Value>,
    hi_inclusive: bool,
    collation: Option<&str>,
) -> bool {
    if let Some(lo) = lo {
        match crate::storage::partitioning::compare_value_with_collation(value, lo, collation) {
            std::cmp::Ordering::Less => return true,
            std::cmp::Ordering::Equal if !lo_inclusive => return true,
            _ => {}
        }
    }
    if let Some(hi) = hi {
        match crate::storage::partitioning::compare_value_with_collation(value, hi, collation) {
            std::cmp::Ordering::Greater => return true,
            std::cmp::Ordering::Equal if !hi_inclusive => return true,
            _ => {}
        }
    }
    false
}

fn intersect(a: Option<Vec<usize>>, b: Option<Vec<usize>>) -> Option<Vec<usize>> {
    match (a, b) {
        (Some(mut left), Some(mut right)) => {
            // Predicate pruning frequently intersects two large RANGE/LIST
            // sets. Vec::retain + Vec::contains was O(P²) at the 8192-leaf
            // limit. Canonical sorted sets give a linear merge and stable
            // declaration-order output.
            left.sort_unstable();
            left.dedup();
            right.sort_unstable();
            right.dedup();
            let mut out = Vec::with_capacity(left.len().min(right.len()));
            let (mut lhs, mut rhs) = (0, 0);
            while lhs < left.len() && rhs < right.len() {
                match left[lhs].cmp(&right[rhs]) {
                    std::cmp::Ordering::Less => lhs += 1,
                    std::cmp::Ordering::Greater => rhs += 1,
                    std::cmp::Ordering::Equal => {
                        out.push(left[lhs]);
                        lhs += 1;
                        rhs += 1;
                    }
                }
            }
            Some(out)
        }
        (None, other) | (other, None) => other,
    }
}

fn collect_and_conjuncts(expr: &crate::parser::Expr) -> Vec<&crate::parser::Expr> {
    let mut out: Vec<&crate::parser::Expr> = Vec::new();
    let mut stack: Vec<&crate::parser::Expr> = vec![expr];
    while let Some(e) = stack.pop() {
        match e {
            crate::parser::Expr::Binary {
                op: crate::parser::BinaryOp::And,
                left,
                right,
            } => {
                stack.push(left);
                stack.push(right);
            }
            other => out.push(other),
        }
    }
    out
}

fn extract_eq_on_column(
    table: &TableSchema,
    expr: &crate::parser::Expr,
) -> Option<(usize, crate::types::Value)> {
    use crate::parser::{BinaryOp, Expr};
    let Expr::Binary { left, op, right } = expr else {
        return None;
    };
    if !matches!(op, BinaryOp::Eq | BinaryOp::NullSafeEq) {
        return None;
    }
    if let Some(idx) = column_index(table, left) {
        if let Some(v) = pruning_literal(table, idx, right) {
            return Some((idx, v));
        }
    }
    if let Some(idx) = column_index(table, right) {
        if let Some(v) = pruning_literal(table, idx, left) {
            return Some((idx, v));
        }
    }
    None
}

fn extract_is_null_on_column(table: &TableSchema, expr: &crate::parser::Expr) -> Option<usize> {
    let crate::parser::Expr::IsNull(inner) = expr else {
        return None;
    };
    column_index(table, inner)
}

fn extract_range_on_column(
    table: &TableSchema,
    expr: &crate::parser::Expr,
    col_idx: usize,
) -> Option<(
    Option<crate::types::Value>,
    bool,
    Option<crate::types::Value>,
    bool,
)> {
    use crate::parser::{BinaryOp, Expr};
    if let Expr::Binary { left, op, right } = expr {
        let inclusive = matches!(op, BinaryOp::LtEq | BinaryOp::GtEq);
        let left_idx = column_index(table, left);
        let right_idx = column_index(table, right);
        if left_idx == Some(col_idx) {
            if let Some(v) = pruning_literal(table, col_idx, right) {
                return Some(match op {
                    BinaryOp::Lt | BinaryOp::LtEq => (None, false, Some(v), inclusive),
                    BinaryOp::Gt | BinaryOp::GtEq => (Some(v), inclusive, None, false),
                    _ => return None,
                });
            }
        }
        if right_idx == Some(col_idx) {
            if let Some(v) = pruning_literal(table, col_idx, left) {
                return Some(match op {
                    BinaryOp::Lt | BinaryOp::LtEq => (Some(v), inclusive, None, false),
                    BinaryOp::Gt | BinaryOp::GtEq => (None, false, Some(v), inclusive),
                    _ => return None,
                });
            }
        }
    }
    if let Expr::Between {
        expr: inner,
        low,
        high,
        negated: false,
    } = expr
    {
        if column_index(table, inner) == Some(col_idx) {
            let lo = pruning_literal(table, col_idx, low);
            let hi = pruning_literal(table, col_idx, high);
            if lo.is_some() || hi.is_some() {
                return Some((lo, true, hi, true));
            }
        }
    }
    None
}

fn extract_in_list_on_column(
    table: &TableSchema,
    expr: &crate::parser::Expr,
    col_idx: usize,
) -> Option<Vec<crate::types::Value>> {
    use crate::parser::Expr;
    let Expr::InList {
        expr: inner,
        list,
        negated: false,
    } = expr
    else {
        return None;
    };
    if column_index(table, inner) != Some(col_idx) {
        return None;
    }
    let mut values = Vec::with_capacity(list.len());
    for item in list {
        values.push(pruning_literal(table, col_idx, item)?);
    }
    Some(values)
}

fn pruning_literal(table: &TableSchema, idx: usize, expr: &crate::parser::Expr) -> Option<Value> {
    let value = literal_value(expr)?;
    let column = table.columns.get(idx)?;
    match (&column.data_type, &value) {
        (crate::storage::DataTypeKind::Char { .. }, Value::String(_) | Value::SmallString(_)) => {
            Some(value)
        }
        (crate::storage::DataTypeKind::Time { .. } | crate::storage::DataTypeKind::Date, _) => {
            crate::engine::expr::coerce_comparison_constant_for_type(&value, &column.data_type)
        }
        _ => key_pruning_value(table, &column.name, &value),
    }
}

fn column_index(table: &TableSchema, expr: &crate::parser::Expr) -> Option<usize> {
    crate::engine::expr::expr_column_index(expr, table)
}

fn literal_value(expr: &crate::parser::Expr) -> Option<crate::types::Value> {
    use crate::parser::{Expr, Literal};
    match expr {
        Expr::Literal(lit) => Some(match lit {
            Literal::Null => crate::types::Value::Null,
            Literal::Bool(b) => crate::types::Value::Int(if *b { 1 } else { 0 }),
            Literal::Int(v) => crate::types::Value::Int(*v),
            Literal::UInt(v) => crate::types::Value::UInt(*v),
            Literal::Float(v) => crate::types::Value::Double(*v),
            Literal::Decimal(s) => crate::types::Value::Decimal(s.clone()),
            Literal::String(s) => crate::types::Value::String(s.clone()),
            Literal::HexBytes(b) | Literal::BitBytes(b) => crate::types::Value::Bytes(b.clone()),
        }),
        Expr::BoundValue(v) => Some(v.clone()),
        // Auto-parameterised literals (e.g. `WHERE y = 5` may be
        // rewritten to `WHERE y = ?` so plan-cache entries collapse
        // across literal-only variants). Resolve against the
        // thread-local parameter context so pruning still sees the
        // concrete value.
        Expr::Placeholder(idx) => crate::engine::params::current_param(*idx),
        Expr::Unary {
            op: crate::parser::UnaryOp::Neg,
            expr,
        } => {
            let inner = literal_value(expr)?;
            Some(match inner {
                crate::types::Value::Int(v) => crate::types::Value::Int(-v),
                crate::types::Value::Double(v) => crate::types::Value::Double(-v),
                _ => return None,
            })
        }
        _ => None,
    }
}

/// Compute pruned partitions when a WHERE clause exposes an `IN
/// (list)` constraint on the partition key. Tries each element of the
/// list and unions the routed partition indices.
pub fn prune_partitions_for_in_list(
    table: &TableSchema,
    part: &PartitionInfo,
    values: &[Value],
) -> Option<Vec<usize>> {
    if values.is_empty() {
        return None;
    }
    let total = part.partitions.len();
    let mut acc: Vec<usize> = Vec::new();
    for v in values {
        let eqs = vec![Some(v.clone())];
        match prune_partitions_for_eq(table, part, &eqs) {
            Some(picked) => acc.extend(picked),
            None => return None, // can't reason about this value — give up.
        }
    }
    acc.sort_unstable();
    acc.dedup();
    if acc.is_empty() || acc.len() == total {
        None
    } else {
        Some(acc)
    }
}
