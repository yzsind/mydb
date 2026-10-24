//! M5.C2 — `PARTITION (p0, p1, …)` clause resolution.
//!
//! The clause appears on every DML and SELECT statement as a list of
//! partition names. This module resolves the names against the table's
//! [`PartitionInfo`] and produces the matching `storage_id` set that the
//! existing partition-router thread-local consumes
//! ([`crate::engine::executor::partition_router::with_pruned_partitions`]).
//!
//! For INSERT, the clause acts as an *assertion* rather than a pruning
//! hint — routed rows whose partition isn't in the allowed set raise
//! MySQL error 1748 (`ER_ROW_DOES_NOT_MATCH_GIVEN_PARTITION_SET`).

use crate::storage::{error, Database};
use crate::types::QueryError;

/// Resolve a list of partition names against `table_name`'s catalog entry,
/// returning the matching `storage_id` set ready to feed into
/// [`crate::engine::executor::partition_router::with_pruned_partitions`].
///
/// * Returns `Ok(None)` when `names` is empty — i.e. the statement carried
///   no `PARTITION (...)` clause.
/// * Returns an `Err` (MySQL error 1735, `ER_UNKNOWN_PARTITION`) when any
///   name doesn't match a declared partition.
/// * Returns an `Err` (MySQL error 1747, `ER_PARTITION_CLAUSE_ON_NONPARTITIONED`)
///   when the table isn't partitioned but a clause was supplied.
pub(crate) fn resolve_partition_clause(
    db: &Database,
    table_name: &str,
    names: &[String],
) -> Result<Option<Vec<u64>>, QueryError> {
    if names.is_empty() {
        return Ok(None);
    }
    let schema = db.table(table_name)?;
    let Some(info) = schema.partitioning.as_ref() else {
        return Err(error(
            crate::error::catalog::ER_PARTITION_CLAUSE_ON_NONPARTITIONED,
            format!(
                "PARTITION () clause on non partitioned table '{}'",
                schema.name
            ),
        ));
    };
    let mut out = Vec::with_capacity(names.len());
    for name in names {
        match info.storage_ids_for_name(name) {
            Some(storage_ids) => {
                for storage_id in storage_ids {
                    if !out.contains(&storage_id) {
                        out.push(storage_id);
                    }
                }
            }
            None => {
                return Err(error(
                    crate::error::catalog::ER_UNKNOWN_PARTITION,
                    format!("Unknown partition '{name}' in table '{}'", schema.name),
                ))
            }
        }
    }
    Ok(Some(out))
}

/// INSERT-only: validate that `chosen_storage_id` (the partition the row
/// routed to) is in the allowed list `allowed`. The `allowed` slice is
/// pre-resolved by [`resolve_partition_clause`] and represents the partition
/// names the user wrote on the statement.
pub(super) fn assert_insert_partition_allowed(
    allowed: &[u64],
    chosen_storage_id: u64,
) -> Result<(), QueryError> {
    if allowed.contains(&chosen_storage_id) {
        return Ok(());
    }
    Err(error(
        crate::error::catalog::ER_ROW_DOES_NOT_MATCH_GIVEN_PARTITION_SET,
        "Found a row not matching the given partition set",
    ))
}
