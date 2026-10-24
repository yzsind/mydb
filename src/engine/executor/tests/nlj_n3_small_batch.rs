//! WS-N3: lightweight batched INLJ regressions.
//!
//! New admission allows right>=256 and left>=8, replacing the previous
//! left>=64/right>=1000 requirement. Test left=10/right=500,
//! comparing batching on/off with MYDB_INLJ_BATCH_MIN_LEFT_ROWS
//! forcing per-row ground truth. Results must agree; per-row counters
//! increase only with batching off.

use super::support::query_sorted_rows as sorted_rows;
use super::*;

fn build_small_join_dataset(engine: &StorageQueryEngine) {
    // Dimension r has 500 rows, above the 256 threshold.
    engine
        .execute("CREATE TABLE smallinl_r (k INT PRIMARY KEY, v INT)")
        .unwrap();
    let mut sql = String::from("INSERT INTO smallinl_r VALUES ");
    for i in 0..500 {
        if i > 0 {
            sql.push(',');
        }
        sql.push_str(&format!("({}, {})", i, i * 3));
    }
    engine.execute(&sql).unwrap();

    // Outer l has ten rows, >=8 and <64, within the new N3 range.
    engine
        .execute("CREATE TABLE smallinl_l (id INT PRIMARY KEY, k INT)")
        .unwrap();
    let mut sql = String::from("INSERT INTO smallinl_l VALUES ");
    for i in 0..10 {
        if i > 0 {
            sql.push(',');
        }
        sql.push_str(&format!("({}, {})", i, i * 10));
    }
    engine.execute(&sql).unwrap();

    engine.execute("ANALYZE TABLE smallinl_r").unwrap();
    engine.execute("ANALYZE TABLE smallinl_l").unwrap();
}

#[test]
fn small_batch_inlj_matches_per_row_baseline() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    build_small_join_dataset(&engine);

    // Force INLJ with a hint: ten outer rows, 500 inner rows, PK equality.
    let sql = "SELECT /*+ INDEX_NLJ(smallinl_l, smallinl_r) */ \
               smallinl_l.id, smallinl_r.v \
               FROM smallinl_l JOIN smallinl_r ON smallinl_l.k = smallinl_r.k";

    // Raise batching thresholds beyond reach to force the per-row oracle.
    let per_row = with_test_env(&[("MYDB_INLJ_BATCH_MIN_LEFT_ROWS", Some("100000"))], || {
        sorted_rows(&engine, sql)
    });

    // Defaults admit ten left rows plus 500 right rows to batching under N3.
    let batched = sorted_rows(&engine, sql);

    assert_eq!(per_row, batched, "small-batch INLJ result diverged");
    assert!(!batched.is_empty(), "expected at least one match");
}

#[test]
fn small_batch_inlj_per_row_fallback_counter_advances_when_disabled() {
    use std::sync::atomic::Ordering;
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    build_small_join_dataset(&engine);

    // Make right much larger than left (500 versus 10) and raise NLJ-to-hash
    // thresholds to ensure INLJ rather than hash join or plain NLJ.
    let sql =
        "SELECT /*+ INDEX_NLJ(smallinl_l, smallinl_r) NO_HASH_JOIN(smallinl_l, smallinl_r) */ \
               smallinl_l.id FROM smallinl_l \
               JOIN smallinl_r ON smallinl_l.k = smallinl_r.k";

    let before =
        crate::engine::executor::select::plan::INLJ_PER_ROW_EXECUTIONS.load(Ordering::Relaxed);
    with_test_env(
        &[
            ("MYDB_INLJ_BATCH_MIN_LEFT_ROWS", Some("100000")),
            ("MYDB_INLJ_BATCH_MIN_RIGHT_ROWS", Some("4294967295")),
            ("MYDB_ADAPTIVE_JOIN_NLJ_MAX_COMPARISONS", Some("1000000000")),
        ],
        || sorted_rows(&engine, sql),
    );
    let after =
        crate::engine::executor::select::plan::INLJ_PER_ROW_EXECUTIONS.load(Ordering::Relaxed);
    assert!(
        after > before,
        "inlj_per_row_executions did not advance (before={before}, after={after})"
    );
}
