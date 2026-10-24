//! Integration tests for the PGA-style WorkAreaManager.
//!
//! These exercise the end-to-end path: SELECT issued against an
//! engine wrapped in a tight `MemBudget`, which forces the operator
//! through its spill path. The result set must match the in-memory
//! baseline (differential check) and the work-area advisor view
//! must reflect that grants / spills happened.

use super::support::query_rows as rows_of;
use super::*;
use crate::engine::executor::mem_budget::{with_query_budget, MemBudget};
use std::sync::Arc;

fn populate_groupy_table(engine: &StorageQueryEngine, rows: usize, groups: usize) {
    engine.execute("CREATE TABLE t (g INT, v INT)").unwrap();
    for start in (0..rows).step_by(200) {
        insert_fixture_rows(
            engine,
            "test.t",
            (start..(start + 200).min(rows))
                .map(|r| vec![Value::Int((r % groups) as i64), Value::Int(r as i64)]),
        );
    }
}

#[test]
fn query_admission_reclaim_releases_buffer_memory_before_1037() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    populate_groupy_table(&engine, 2_000, 2_000);
    let _ = rows_of(&engine, "SELECT g, v FROM t ORDER BY g");

    let before = crate::engine::executor::mem_budget::controlled_memory_snapshot();
    let raw_before = before.2[0];
    assert!(raw_before >= crate::storage::PAGE_SIZE);
    let target = before.0.saturating_sub(crate::storage::PAGE_SIZE);
    engine
        .db_for_tests()
        .reclaim_for_query_admission(target)
        .unwrap();
    let after = crate::engine::executor::mem_budget::controlled_memory_snapshot();
    assert!(after.0 <= target);
    assert!(after.2[0] < raw_before);
}

#[test]
fn advisory_clean_reclaim_preserves_lightweight_zone_handles() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    populate_groupy_table(&engine, 2_000, 100);
    engine.execute("ANALYZE TABLE t").unwrap();
    engine.execute("ALTER SYSTEM CHECKPOINT").unwrap();
    let expected = rows_of(&engine, "SELECT g, SUM(v) FROM t GROUP BY g ORDER BY g");
    let database = engine.db_for_tests();
    let handles = || {
        database
            .zone_map_status_rows()
            .into_iter()
            .find(|(name, _)| *name == "zone_map_handles")
            .unwrap()
            .1
            .parse::<usize>()
            .unwrap()
    };
    let before_handles = handles();
    assert!(before_handles > 0);
    let before = crate::engine::executor::mem_budget::controlled_memory_snapshot();
    database
        .reclaim_clean_for_query_admission(before.0.saturating_sub(crate::storage::PAGE_SIZE))
        .unwrap();
    assert_eq!(
        handles(),
        before_handles,
        "cache pressure discarded lightweight descriptors"
    );
    assert_eq!(
        rows_of(&engine, "SELECT g, SUM(v) FROM t GROUP BY g ORDER BY g"),
        expected
    );
}

/// Same result under generous vs tight per-query budget. Tight
/// budget exercises HashAgg's spill loop without losing rows.
#[test]
fn hash_agg_spill_under_tight_budget_matches_baseline() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    populate_groupy_table(&engine, 2_000, 32);
    let sql = "SELECT g, COUNT(*), SUM(v) FROM t GROUP BY g ORDER BY g";

    let big = Arc::new(MemBudget::new(1 << 30));
    let expected = with_query_budget(Arc::clone(&big), || rows_of(&engine, sql));
    assert_eq!(expected.len(), 32);

    let tiny = Arc::new(MemBudget::new(64 * 1024));
    let got = with_query_budget(Arc::clone(&tiny), || rows_of(&engine, sql));
    assert_eq!(got, expected, "tight-budget HashAgg must match baseline");
    assert_eq!(tiny.used(), 0, "no charge may leak after the query");
}

/// Same result for ORDER BY under generous vs tight budget — exercises
/// external sort + WorkArea coordination.
#[test]
fn external_sort_spill_under_tight_budget_matches_baseline() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    populate_groupy_table(&engine, 5_000, 1);
    let sql = "SELECT v FROM t ORDER BY v";

    let big = Arc::new(MemBudget::new(1 << 30));
    let expected = with_query_budget(Arc::clone(&big), || rows_of(&engine, sql));
    assert_eq!(expected.len(), 5_000);

    let tiny = Arc::new(MemBudget::new(32 * 1024));
    let got = with_query_budget(Arc::clone(&tiny), || rows_of(&engine, sql));
    assert_eq!(got, expected, "tight-budget Sort must match baseline");
    assert_eq!(tiny.used(), 0);
}

/// SELECT DISTINCT is spill-capable, so query-budget pressure must not
/// surface as 1037. The spilled result must match the in-memory oracle and
/// release every byte charged to the query.
#[test]
fn vectorized_distinct_spills_under_pressure() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    populate_groupy_table(&engine, 4_000, 4_000);
    let sql = "SELECT DISTINCT v FROM t ORDER BY v";

    let big = Arc::new(MemBudget::new(1 << 30));
    let expected = with_query_budget(Arc::clone(&big), || rows_of(&engine, sql));
    assert_eq!(expected.len(), 4_000);

    let tiny = Arc::new(MemBudget::new(32 * 1024));
    let got = with_query_budget(Arc::clone(&tiny), || rows_of(&engine, sql));
    assert_eq!(got, expected, "tight-budget Distinct must spill correctly");
    assert_eq!(tiny.used(), 0, "Distinct leaked query memory charge");
}

/// Advisor view rolls up live work-area state into a friendly form.
#[test]
fn memory_advisor_view_is_populated() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let rows = rows_of(
        &engine,
        "SELECT METRIC, VALUE, UNIT \
         FROM information_schema.mydb_memory_advisor \
         WHERE METRIC IN ('memory_target_bytes', 'executor_memory_target_bytes', \
                          'work_area_utilization_pct', 'optimal_grants', \
                          'query_memory_reservation_rejections', \
                          'process_memory_reservation_rejections', \
                          'executor_terminal_oom_cancellations', \
                          'physical_memory_pressure_rejections', \
                          'effective_memory_limit_bytes', \
                          'sql_memory_hard_limit_bytes', \
                          'process_memory_hard_limit_bytes', \
                          'memory_reclaim_attempts', \
                          'active_spill_files', 'orphan_spill_bytes') \
         ORDER BY METRIC",
    );
    assert_eq!(rows.len(), 14, "expected all selected advisor metrics");
    // memory_target_bytes must be positive (default 1 GiB or env override).
    let memory_target = rows
        .iter()
        .find(|row| row[0] == Value::String("memory_target_bytes".into()))
        .expect("memory_target_bytes row");
    let Value::Int(value) = &memory_target[1] else {
        panic!("value column not an int")
    };
    assert!(*value > 0, "memory_target_bytes must be positive");
}

/// SHOW ENGINE MYDB STATUS exposes the work-area summary fields.
#[test]
fn show_engine_status_has_work_area_summary() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let QueryResponse::ResultSet(result) = engine.execute("SHOW ENGINE MYDB STATUS").unwrap()
    else {
        panic!("SHOW ENGINE MYDB STATUS did not return a result set");
    };
    let names: Vec<String> = result
        .rows
        .iter()
        .map(|r| match &r[1] {
            // (Type, Name, Status) columns; metric name is the 2nd col.
            Value::String(s) => s.clone(),
            _ => String::new(),
        })
        .collect();
    for want in [
        "memory_target_bytes",
        "executor_memory_target_bytes",
        "work_area_allocated_bytes",
        "work_area_optimal_grants",
        "work_area_spill_requests",
        "query_memory_reservation_rejections",
        "process_memory_reservation_rejections",
        "temp_disk_global_quota_rejections",
        "temp_disk_query_quota_rejections",
        "active_spill_files",
        "orphan_spill_bytes",
        "spill_cleanup_failures",
        "sort_spill_count",
        "hash_join_spill_count",
        "hash_agg_spill_count",
        "distinct_spill_count",
        "cte_spill_count",
        "setop_spill_count",
        "setop_spill_bytes_written",
        "setop_spill_bytes_read",
        "setop_spill_partitions",
        "setop_spill_repartitions",
        "setop_spill_max_depth",
        "setop_spill_file_opens",
        "setop_spill_merge_passes",
    ] {
        assert!(
            names.iter().any(|n| n == want),
            "SHOW ENGINE MYDB STATUS missing field `{want}`"
        );
    }
}

#[test]
fn show_status_exposes_setop_spill_counters() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let QueryResponse::ResultSet(result) = engine
        .execute("SHOW GLOBAL STATUS LIKE 'Mydb_setop_spill_%'")
        .unwrap()
    else {
        panic!("SHOW GLOBAL STATUS did not return a result set");
    };
    assert_eq!(
        result.rows.len(),
        8,
        "all set-op spill counters must be visible"
    );
}
