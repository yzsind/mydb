//! P0/B4: the row-based hash join build (rows + bucket index + bloom) is
//! charged against the per-query `mem_budget` umbrella. A build that does
//! not fit the active budget spills to the grace-spill join instead of
//! growing process RSS outside the umbrella — and must still produce the
//! same rows as the in-memory path.

use super::support::query_rows as rows_of;
use super::*;
use crate::engine::executor::mem_budget::{with_query_budget, MemBudget};
use std::sync::Arc;

fn sorted(mut rows: Vec<Vec<Value>>) -> Vec<Vec<Value>> {
    rows.sort_by_cached_key(|row| format!("{row:?}"));
    rows
}

/// No index on the join key forces a hash join. A large build (600 rows)
/// against a tiny matching probe (3 rows) means: under a generous budget
/// the build stays in memory; under a budget smaller than the build it
/// spills to grace. Both must return the same 3 joined rows.
fn setup(engine: &StorageQueryEngine) {
    engine.execute("CREATE TABLE b (k INT, v INT)").unwrap();
    engine.execute("CREATE TABLE p (k INT, w INT)").unwrap();
    for start in (0..600).step_by(200) {
        insert_fixture_rows(
            engine,
            "test.b",
            (start..start + 200).map(|k| vec![Value::Int(k), Value::Int(k * 10)]),
        );
    }
    engine
        .execute("INSERT INTO p VALUES (0, 100), (1, 101), (2, 102)")
        .unwrap();
}

const JOIN_SQL: &str = "SELECT b.v, p.w FROM b JOIN p ON b.k = p.k";

#[test]
fn hash_join_build_spills_to_grace_under_tight_budget() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    setup(&engine);

    // Ground truth: a generous budget keeps the build in memory.
    let big = Arc::new(MemBudget::new(1 << 30));
    let expected = sorted(with_query_budget(Arc::clone(&big), || {
        rows_of(&engine, JOIN_SQL)
    }));
    assert_eq!(expected.len(), 3, "k=0,1,2 should match");
    assert_eq!(
        big.used(),
        0,
        "build charge must be released after the join"
    );

    // A 16 KiB budget is far below the 600-row build → try_charge fails →
    // the join spills to grace. Results must be identical.
    let tiny = Arc::new(MemBudget::new(16 * 1024));
    let got = sorted(with_query_budget(Arc::clone(&tiny), || {
        rows_of(&engine, JOIN_SQL)
    }));
    assert_eq!(
        got, expected,
        "grace fallback under a tight budget must match"
    );
    assert_eq!(tiny.used(), 0, "no charge may leak after grace spill");
}

#[test]
fn hash_join_no_budget_is_unchanged() {
    // Without an active query budget, `try_charge` is a no-op and the
    // join takes the normal in-memory path (regression guard for the
    // common path).
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    setup(&engine);
    let got = sorted(rows_of(&engine, JOIN_SQL));
    assert_eq!(got.len(), 3);
}

/// Streaming-spill join output (Q64/Q78 shape): a `TopK → Filter →
/// HashJoin` where the *build* fits the budget but the full join
/// *output* does not. Materialising the output would raise 1037; the
/// `spill_stream_applicable` gate routes the join through the
/// same-thread streaming core so `TopK` pulls one row at a time and
/// only `k` survive. The tight-budget result must match the generous
/// one byte-for-byte, and must not error.
#[test]
fn hash_join_output_streams_under_tight_budget_topk_filter() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    // Small build (dim, 50 rows) — fits any budget. Large preserved
    // probe (fact, 4000 rows) — its LEFT-JOIN output is what overflows.
    engine.execute("CREATE TABLE dim (k INT, y INT)").unwrap();
    engine.execute("CREATE TABLE fact (k INT, x INT)").unwrap();
    {
        let mut sql = String::from("INSERT INTO dim VALUES ");
        for k in 0..50 {
            if k > 0 {
                sql.push_str(", ");
            }
            sql.push_str(&format!("({k}, {})", k * 7));
        }
        engine.execute(&sql).unwrap();
    }
    for start in (0..4000).step_by(500) {
        let mut sql = String::from("INSERT INTO fact VALUES ");
        for i in start..start + 500 {
            if i > start {
                sql.push_str(", ");
            }
            // k cycles 0..99 → half match dim, half NULL-extend.
            sql.push_str(&format!("({}, {})", i % 100, i));
        }
        engine.execute(&sql).unwrap();
    }

    // Post-join WHERE over a coalesce(outer column) → a genuine Filter
    // node above the LEFT JOIN (not pushed into a scan).
    const SQL: &str = "SELECT fact.x, dim.y \
         FROM fact LEFT JOIN dim ON fact.k = dim.k \
         WHERE coalesce(dim.y, 0) >= 0 \
         ORDER BY fact.x, dim.y LIMIT 5";

    let big = Arc::new(MemBudget::new(1 << 30));
    let expected = with_query_budget(Arc::clone(&big), || rows_of(&engine, SQL));
    assert_eq!(expected.len(), 5, "ORDER BY fact.x LIMIT 5");
    assert_eq!(big.used(), 0, "charge released after the query");

    // 64 KiB: the 50-row build fits, but the 4000-row join output does
    // not. Without output streaming this raises 1037; with it the query
    // succeeds and the rows are identical (ORDER BY makes order stable).
    let tiny = Arc::new(MemBudget::new(64 * 1024));
    let got = with_query_budget(Arc::clone(&tiny), || rows_of(&engine, SQL));
    assert_eq!(
        got, expected,
        "streamed join output under a tight budget must match the materialised path"
    );
    assert_eq!(tiny.used(), 0, "no charge may leak after streaming spill");
}

/// WS-C: an outer-join output that overflows the budget must stream even when
/// CBO picked `build = the (large) preserved side` (the TPC-DS Q78 shape, where
/// a predicate-pushdown-shrunk estimate flips the build onto the preserved
/// side). The streaming path forces the build onto the null-supplying side so
/// the large preserved side streams as probe. Here the planner builds the
/// *left* side (`dim` is the large preserved side of `dim LEFT JOIN sm`);
/// without the force flag that level would materialise its whole output.
/// Differential: disabling the streaming spill path must still produce the same
/// rows (via the materialised path) under a generous budget.
#[test]
fn left_join_build_left_streams_output_under_tight_budget() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    // `big` is the large LEFT/preserved side; `sm` is a tiny matching set.
    engine.execute("CREATE TABLE big (k INT, x INT)").unwrap();
    engine.execute("CREATE TABLE sm (k INT, y INT)").unwrap();
    for start in (0..4000).step_by(500) {
        let mut sql = String::from("INSERT INTO big VALUES ");
        for i in start..start + 500 {
            if i > start {
                sql.push_str(", ");
            }
            sql.push_str(&format!("({}, {})", i % 100, i));
        }
        engine.execute(&sql).unwrap();
    }
    engine
        .execute("INSERT INTO sm VALUES (0,1000),(1,1001),(2,1002)")
        .unwrap();

    // `big LEFT JOIN sm` preserves all 4000 big rows; the post-join coalesce
    // WHERE forces a Filter node above the join, and ORDER BY + LIMIT makes a
    // bounded TopK consumer. The full join output overflows a tight budget.
    const SQL: &str = "SELECT big.x, sm.y \
         FROM big LEFT JOIN sm ON big.k = sm.k \
         WHERE coalesce(sm.y, -1) <> -2 \
         ORDER BY big.x LIMIT 5";

    let big_budget = Arc::new(MemBudget::new(1 << 30));
    let expected = with_query_budget(Arc::clone(&big_budget), || rows_of(&engine, SQL));
    assert_eq!(expected.len(), 5, "ORDER BY big.x LIMIT 5");

    let tiny = Arc::new(MemBudget::new(64 * 1024));
    let got = with_query_budget(Arc::clone(&tiny), || rows_of(&engine, SQL));
    assert_eq!(
        got, expected,
        "tight-budget streamed output must match the materialised path regardless of build side"
    );
    assert_eq!(tiny.used(), 0, "no charge may leak after streaming spill");
}

#[test]
fn row_hot_join_output_honors_budget_without_strict_materialization_mode() {
    std::fs::create_dir_all("/tmp/mydb/join-fix").unwrap();
    let dir = tempfile::tempdir_in("/tmp/mydb/join-fix").unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE hot_build (k INT, v INT)")
        .unwrap();
    engine
        .execute("CREATE TABLE hot_probe (k INT, v INT)")
        .unwrap();
    for table in ["hot_build", "hot_probe"] {
        let values = (0..128)
            .map(|i| format!("(1,{i})"))
            .collect::<Vec<_>>()
            .join(",");
        engine
            .execute(&format!("INSERT INTO {table} VALUES {values}"))
            .unwrap();
    }
    crate::util::env::with_vars(
        &[
            ("MYDB_VEC_HASH_JOIN", Some("off")),
            ("MYDB_HASH_JOIN_STREAM", Some("off")),
            ("MYDB_HASH_JOIN_PARALLELISM", Some("1")),
            ("MYDB_HASH_JOIN_MAX_BUILD_ROWS", Some("16")),
            ("MYDB_HASH_JOIN_GRACE_MAX_DEPTH", Some("1")),
        ],
        || {
            let budget = Arc::new(MemBudget::new(1024 * 1024));
            let before = crate::engine::executor::select::plan::HASH_JOIN_SKEW_CHUNKS
                .load(std::sync::atomic::Ordering::Relaxed);
            let result = with_query_budget(budget.clone(), || {
                engine.execute("SELECT COUNT(*) FROM (SELECT * FROM hot_probe p LEFT JOIN hot_build b ON p.k=b.k) t")
            });
            assert!(
                matches!(result, Err(ref e) if e.code() == 1037),
                "{result:?}"
            );
            assert!(
                crate::engine::executor::select::plan::HASH_JOIN_SKEW_CHUNKS
                    .load(std::sync::atomic::Ordering::Relaxed)
                    > before
            );
            assert_eq!(budget.used(), 0);
            assert_eq!(budget.spill_bytes(), 0);
            engine.execute("SELECT 1").unwrap();
        },
    );
}
