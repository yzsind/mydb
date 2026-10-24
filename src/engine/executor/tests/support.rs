//! Result helpers with explicit materialization and row-count contracts.

use super::*;

#[track_caller]
pub(super) fn execute_materialized(
    engine: &StorageQueryEngine,
    session: &mut SessionContext,
    sql: &str,
) -> Result<QueryResponse, QueryError> {
    <StorageQueryEngine as QueryEngine>::execute(engine, session, sql)
        .and_then(QueryResponse::materialize)
}

#[track_caller]
pub(super) fn query_rows(engine: &StorageQueryEngine, sql: &str) -> Vec<Vec<Value>> {
    let QueryResponse::ResultSet(result) = engine.execute(sql).unwrap() else {
        panic!("expected result set for {sql}");
    };
    result.rows
}

#[track_caller]
pub(super) fn result_rows(response: QueryResponse) -> Vec<Vec<Value>> {
    let QueryResponse::ResultSet(result) = response else {
        panic!("expected result set");
    };
    result.rows
}

#[track_caller]
pub(super) fn query_one_row(engine: &StorageQueryEngine, sql: &str) -> Vec<Value> {
    let QueryResponse::ResultSet(result) = engine.execute(sql).unwrap() else {
        panic!("expected result set for: {sql}");
    };
    assert_eq!(result.rows.len(), 1, "expected single row for: {sql}");
    result.rows.into_iter().next().unwrap()
}

#[track_caller]
pub(super) fn query_sorted_rows(engine: &StorageQueryEngine, sql: &str) -> Vec<Vec<Value>> {
    let QueryResponse::ResultSet(result) = engine.execute(sql).unwrap() else {
        panic!("expected result set for {sql}");
    };
    let mut rows = result.rows;
    // Preserve the existing Debug ordering, formatting each row only once
    // instead of allocating two strings for every sort comparison.
    rows.sort_by_cached_key(|row| format!("{row:?}"));
    rows
}
