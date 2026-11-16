//! WS-N4: batched INLJ supports promotion of constant ranges.
//!
//! When both Range bounds are constant, compile_index_lookup_plan moves the predicate
//! to the right-only residual and clears plan.range, preventing batched INLJ
//! from bailing out on plan.range.is_some().
//!
//! Right PK is (id,ts); o.id=r.id AND r.ts<'2024-06-01'
//! uses equality prefix id and constant range ts. Compare MYDB_INLJ_BATCH_RANGE_PROMOTE
//! on/off results and require activation counters on.

use super::support::query_sorted_rows as sorted_rows;
use super::*;

fn build_dataset(engine: &StorageQueryEngine) {
    // Inner r uses composite PK(id,ts); 400 IDs times two ts slots give 800 rows,
    // crossing 256 to combine N3 lightweight batching and N4 range promotion.
    engine
        .execute("CREATE TABLE rng_r (id INT, ts INT, v INT, PRIMARY KEY (id, ts))")
        .unwrap();
    let mut sql = String::from("INSERT INTO rng_r VALUES ");
    for i in 0..400 {
        if i > 0 {
            sql.push(',');
        }
        // ts spans [0,99]; bound 50 passes half and filters half through the residual.
        sql.push_str(&format!("({}, {}, {})", i, i % 100, i * 7));
        sql.push(',');
        sql.push_str(&format!("({}, {}, {})", i, (i % 100) + 100, i * 11));
    }
    engine.execute(&sql).unwrap();

    // Outer o has 64 rows, meeting the N3 batching threshold.
    engine
        .execute("CREATE TABLE rng_o (oid INT PRIMARY KEY, id INT)")
        .unwrap();
    let mut sql = String::from("INSERT INTO rng_o VALUES ");
    for i in 0..64 {
        if i > 0 {
            sql.push(',');
        }
        sql.push_str(&format!("({}, {})", i, i * 4));
    }
    engine.execute(&sql).unwrap();

    engine.execute("ANALYZE TABLE rng_r").unwrap();
    engine.execute("ANALYZE TABLE rng_o").unwrap();
}

/// Promotion enabled/disabled must return identical results.
#[test]
fn const_range_promote_matches_legacy() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    build_dataset(&engine);

    // Equality plus constant range: rng_o.id=rng_r.id AND rng_r.ts<50.
    // Force INLJ and disable hash switching to guarantee the intended path.
    let sql = "SELECT /*+ INDEX_NLJ(rng_o, rng_r) NO_HASH_JOIN(rng_o, rng_r) */ \
               rng_o.oid, rng_r.ts, rng_r.v \
               FROM rng_o JOIN rng_r ON rng_o.id = rng_r.id AND rng_r.ts < 50";

    let off = with_test_env(&[("MYDB_INLJ_BATCH_RANGE_PROMOTE", Some("off"))], || {
        sorted_rows(&engine, sql)
    });
    let on = sorted_rows(&engine, sql);
    assert_eq!(off, on, "range-promote diverged from legacy");
}

/// inlj_batch_range_promoted increases only when promotion is enabled.
#[test]
fn const_range_promote_counter_advances() {
    use std::sync::atomic::Ordering;
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    build_dataset(&engine);

    let sql = "SELECT /*+ INDEX_NLJ(rng_o, rng_r) NO_HASH_JOIN(rng_o, rng_r) */ \
               rng_o.oid \
               FROM rng_o JOIN rng_r ON rng_o.id = rng_r.id AND rng_r.ts <= 30";

    let before =
        crate::engine::executor::select::plan::INLJ_BATCH_RANGE_PROMOTED.load(Ordering::Relaxed);
    sorted_rows(&engine, sql);
    let after =
        crate::engine::executor::select::plan::INLJ_BATCH_RANGE_PROMOTED.load(Ordering::Relaxed);
    assert!(
        after > before,
        "inlj_batch_range_promoted did not advance (before={before}, after={after})"
    );

    // The disabled-path counter must not increase.
    let before_off = after;
    with_test_env(&[("MYDB_INLJ_BATCH_RANGE_PROMOTE", Some("off"))], || {
        sorted_rows(&engine, sql)
    });
    let after_off =
        crate::engine::executor::select::plan::INLJ_BATCH_RANGE_PROMOTED.load(Ordering::Relaxed);
    assert_eq!(
        before_off, after_off,
        "off path should not advance promote counter"
    );
}

/// Nonconstant bounds referencing outer columns retain plan.range and are not promoted.
#[test]
fn non_const_range_keeps_range_in_plan() {
    use std::sync::atomic::Ordering;
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    build_dataset(&engine);

    // A bound of rng_o.id references outer and must not be promoted.
    let sql = "SELECT /*+ INDEX_NLJ(rng_o, rng_r) NO_HASH_JOIN(rng_o, rng_r) */ \
               rng_o.oid \
               FROM rng_o JOIN rng_r ON rng_o.id = rng_r.id AND rng_r.ts < rng_o.id";

    let before =
        crate::engine::executor::select::plan::INLJ_BATCH_RANGE_PROMOTED.load(Ordering::Relaxed);
    sorted_rows(&engine, sql);
    let after =
        crate::engine::executor::select::plan::INLJ_BATCH_RANGE_PROMOTED.load(Ordering::Relaxed);
    assert_eq!(
        before, after,
        "non-constant range must not trigger promote (advanced from {before} to {after})"
    );
}
