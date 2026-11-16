//! WS-N5: InljChunkStream retains InljMrrSharedCaches so entries/IDs caches
//! are reused across next() calls.
//!
//! Acceptance:
//! inlj_mrr_chunk_shared_hits increases per chunk with default shared caching enabled.
//! MYDB_INLJ_MRR_CHUNK_SHARED=off restores per-call local caches without increasing the counter.
//! Both paths return identical results.

use super::*;

fn setup_inlj_chain_tables(engine: &StorageQueryEngine) {
    engine
        .execute("CREATE TABLE n5_t1 (id INT PRIMARY KEY, k1 INT, fk3 INT, v1 INT)")
        .unwrap();
    engine
        .execute("CREATE TABLE n5_t2 (id INT PRIMARY KEY, k1 INT, v2 INT)")
        .unwrap();
    engine
        .execute("CREATE TABLE n5_t3 (id INT PRIMARY KEY, v3 TEXT)")
        .unwrap();
    insert_fixture_rows(
        engine,
        "test.n5_t1",
        (0..600).map(|i| {
            vec![
                Value::Int(i),
                Value::Int(i % 15),
                Value::Int(if i % 7 == 0 {
                    9_000_000 + i
                } else {
                    i * 9 % 5000
                }),
                Value::Int(i * 3),
            ]
        }),
    );
    insert_fixture_rows(
        engine,
        "test.n5_t2",
        (0..45).map(|i| vec![Value::Int(i), Value::Int(i % 15), Value::Int(i * 11)]),
    );
    insert_fixture_rows(
        engine,
        "test.n5_t3",
        (0..5000).map(|i| vec![Value::Int(i), Value::String(format!("name-{}", i % 97))]),
    );
}

const SQL: &str = "SELECT /*+ ORDERED */ n5_t1.id, n5_t2.v2, n5_t3.v3 FROM n5_t1 \
     JOIN n5_t3 ON n5_t1.fk3 = n5_t3.id \
     JOIN n5_t2 ON n5_t1.k1 = n5_t2.k1 \
     ORDER BY n5_t1.id, n5_t2.v2, n5_t3.v3";

fn rows_of(engine: &StorageQueryEngine, sql: &str) -> Vec<Vec<Value>> {
    let QueryResponse::ResultSet(result) = engine.execute(sql).unwrap() else {
        panic!("expected result set");
    };
    result.rows
}

/// Small chunk_rows forces multiple chunks; shared-cache activation must
/// increase inlj_mrr_chunk_shared_hits.
#[test]
fn mrr_chunk_shared_hits_advance_when_enabled() {
    use std::sync::atomic::Ordering;
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    setup_inlj_chain_tables(&engine);

    let baseline = with_test_env(&[("MYDB_HASH_JOIN_STREAM", Some("off"))], || {
        rows_of(&engine, SQL)
    });
    assert!(!baseline.is_empty());

    // Default-on small chunks with an INLJ-tail chain.
    let before =
        crate::engine::executor::select::plan::INLJ_MRR_CHUNK_SHARED_HITS.load(Ordering::Relaxed);
    let streamed = with_test_env(
        &[
            ("MYDB_HASH_JOIN_STREAM_INLJ_CHUNK", Some("64")),
            // Force per-row execution to observe MRR cache hits; batching does not use those caches.
            ("MYDB_INLJ_BATCH_MIN_LEFT_ROWS", Some("1000000")),
            ("MYDB_INLJ_BATCH_MIN_RIGHT_ROWS", Some("4294967295")),
        ],
        || rows_of(&engine, SQL),
    );
    assert_eq!(streamed, baseline);
    let after =
        crate::engine::executor::select::plan::INLJ_MRR_CHUNK_SHARED_HITS.load(Ordering::Relaxed);
    assert!(
        after > before,
        "inlj_mrr_chunk_shared_hits did not advance (before={before}, after={after})"
    );
}

/// Disabled caching leaves the counter unchanged and results correct.
#[test]
fn mrr_chunk_shared_off_yields_same_result_and_counter_stays() {
    use std::sync::atomic::Ordering;
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    setup_inlj_chain_tables(&engine);

    let baseline = with_test_env(&[("MYDB_HASH_JOIN_STREAM", Some("off"))], || {
        rows_of(&engine, SQL)
    });

    let before =
        crate::engine::executor::select::plan::INLJ_MRR_CHUNK_SHARED_HITS.load(Ordering::Relaxed);
    let off = with_test_env(
        &[
            ("MYDB_HASH_JOIN_STREAM_INLJ_CHUNK", Some("64")),
            ("MYDB_INLJ_BATCH_MIN_LEFT_ROWS", Some("1000000")),
            ("MYDB_INLJ_BATCH_MIN_RIGHT_ROWS", Some("4294967295")),
            ("MYDB_INLJ_MRR_CHUNK_SHARED", Some("off")),
        ],
        || rows_of(&engine, SQL),
    );
    let after =
        crate::engine::executor::select::plan::INLJ_MRR_CHUNK_SHARED_HITS.load(Ordering::Relaxed);
    assert_eq!(off, baseline, "MRR chunk-shared off must match baseline");
    assert_eq!(
        before, after,
        "MRR chunk-shared off must not advance counter"
    );
}
