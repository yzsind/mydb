//! Correctness of build-preserved outer joins with prefilter_build_from_probe.
//!
//! materialize_hash_join_inputs prefilters build rows using the probe key set,
//! injected through inject_runtime_filters_from_hash_build. It removes
//! build rows whose keys do not occur on the probe side. This is safe for INNER,
//! SEMI, and ANTI here because those rows produce no output; for LEFT/build-left
//! or RIGHT/build-right, they are preserved unmatched rows that must receive NULL extensions.
//! execute_hash_join_from_rows can compensate only for rows still present in build_rows;
//! prefiltered rows would be permanently lost.
//!
//! After ANALYZE, LEFT chooses BuildSide::Left when left_rows < right_rows*0.7,
//! so preserved-build shapes occur naturally. A residual LEFT join exercises materialization
//! under default settings because streaming-inner rejects residuals;
//! for pure equality, disable streaming with MYDB_HASH_JOIN_STREAMING_INNER_DISABLE=1.
//! Set MYDB_HASH_JOIN_BLOOM_MAX_BUILD_ROWS=0 to disable all runtime filter injection
//! and obtain the correctness baseline.
//!
//! MYDB_HASH_JOIN_BLOOM_MIN_BUILD_ROWS=1 lowers the normal 128k build minimum
//! to test scale; 1000 probe rows remain below the default 1M maximum.

use super::*;

/// Unindexed join keys leave only hash candidates; after ANALYZE,
/// 302 tl rows < 1000 tr rows * 0.7, so LEFT selects BuildSide::Left.
/// tl has 302 rows: IDs [0,200) use k=id%20, ten per key, all matching tr, with w=0.
/// IDs [200,300) use k=10000+id, absent from probe and vulnerable to prefilter removal.
/// IDs 300/301 have NULL keys, which Bloom also rejects.
/// tr has 1000 rows with k=id%20 and v=id%2.
/// Since 20 is even, v=k%2 within each group. Residual tr.v > tl.w (v=1)
/// accepts entire odd-key groups and rejects entire even-key groups.
///
/// Hand-calculated expectations:
/// Equality pairs = 200*50=10000; residual pairs = 100 odd-key tl rows * 50=5000.
/// For equality plus residual LEFT JOIN, NULL extensions include 100 even-key rows
/// whose keys match but all residuals fail, so prefiltering retains them,
/// plus 100 keys outside the set and two NULL keys: COUNT(*)=5202, COUNT(tr.k)=5000.
/// For equality-only LEFT JOIN: COUNT(*)=10102 and COUNT(tr.k)=10000.
fn setup_tables(engine: &StorageQueryEngine) {
    engine
        .execute("CREATE TABLE tl (id INT PRIMARY KEY, k INT, w INT)")
        .unwrap();
    engine
        .execute("CREATE TABLE tr (id INT PRIMARY KEY, k INT, v INT)")
        .unwrap();
    insert_fixture_rows(
        engine,
        "test.tl",
        (0..302).map(|i| {
            vec![
                Value::Int(i),
                if i >= 300 {
                    Value::Null
                } else {
                    Value::Int(if i < 200 { i % 20 } else { 10000 + i })
                },
                Value::Int(0),
            ]
        }),
    );
    insert_fixture_rows(
        engine,
        "test.tr",
        (0..1000).map(|i| vec![Value::Int(i), Value::Int(i % 20), Value::Int(i % 2)]),
    );
    engine.execute("ANALYZE TABLE tl").unwrap();
    engine.execute("ANALYZE TABLE tr").unwrap();
}

/// Read the single result row from SELECT COUNT(*), COUNT(probe column).
fn count_pair(engine: &StorageQueryEngine, sql: &str) -> (Value, Value) {
    let QueryResponse::ResultSet(rs) = engine.execute(sql).unwrap() else {
        panic!("expected result set for {sql}");
    };
    assert_eq!(rs.rows.len(), 1, "single aggregate row for {sql}");
    let row = rs.rows.into_iter().next().unwrap();
    assert_eq!(row.len(), 2, "two aggregates for {sql}");
    let mut it = row.into_iter();
    (it.next().unwrap(), it.next().unwrap())
}

/// Assert EXPLAIN uses the expected hash join build side; fail here if planner
/// heuristics change rather than silently losing the test's premise.
fn assert_explain_contains(engine: &StorageQueryEngine, sql: &str, needle: &str) {
    let QueryResponse::ResultSet(rs) = engine.execute(&format!("EXPLAIN {sql}")).unwrap() else {
        panic!("expected result set for EXPLAIN {sql}");
    };
    let text = format!("{:?}", rs.rows);
    assert!(
        text.contains(needle),
        "EXPLAIN for {sql} should contain {needle:?}, got: {text}"
    );
}

/// Compare identical SQL with runtime filtering disabled and check hand-calculated results.
fn assert_counts_with_baseline(engine: &StorageQueryEngine, sql: &str, expected: (i64, i64)) {
    let filtered = count_pair(engine, sql);
    let baseline = with_test_env(
        &[("MYDB_HASH_JOIN_BLOOM_MAX_BUILD_ROWS", Some("0"))],
        || count_pair(engine, sql),
    );
    assert_eq!(
        baseline,
        (Value::Int(expected.0), Value::Int(expected.1)),
        "baseline (runtime filters off) disagrees with hand-computed expectation for {sql}"
    );
    assert_eq!(
        filtered, baseline,
        "prefiltered execution lost rows vs no-runtime-filter baseline for {sql}"
    );
}

/// LEFT with preserved build and residual naturally exercises materialization
/// because streaming-inner rejects residuals. Removing build rows outside the probe key set
/// lost 102 COUNT(*) rows before the fix.
#[test]
fn left_build_preserved_residual_keeps_unmatched_build_rows() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    setup_tables(&engine);

    let sql = "SELECT COUNT(*), COUNT(tr.k) FROM tl LEFT JOIN tr ON tl.k = tr.k AND tr.v > tl.w";
    assert_explain_contains(&engine, sql, "build=left");
    assert_explain_contains(&engine, sql, "residual=");
    with_test_env(
        &[("MYDB_HASH_JOIN_BLOOM_MIN_BUILD_ROWS", Some("1"))],
        || assert_counts_with_baseline(&engine, sql, (5202, 5000)),
    );
}

/// Streaming-inner correctly handles preserved-build equality LEFT joins;
/// disable it to independently test materialized semantics, preventing regressions
/// if fast-path admission narrows in the future.
#[test]
fn left_build_preserved_equi_keeps_unmatched_build_rows() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    setup_tables(&engine);

    let sql = "SELECT COUNT(*), COUNT(tr.k) FROM tl LEFT JOIN tr ON tl.k = tr.k";
    assert_explain_contains(&engine, sql, "build=left");
    with_test_env(
        &[
            ("MYDB_HASH_JOIN_BLOOM_MIN_BUILD_ROWS", Some("1")),
            ("MYDB_HASH_JOIN_STREAMING_INNER_DISABLE", Some("1")),
        ],
        || assert_counts_with_baseline(&engine, sql, (10102, 10000)),
    );
}

/// RIGHT/preserved-build is symmetric; the planner chooses BuildSide::Right
/// when the right side is smaller. The residual variant exercises materialization.
#[test]
fn right_build_preserved_residual_keeps_unmatched_build_rows() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    setup_tables(&engine);

    let sql = "SELECT COUNT(*), COUNT(tr.k) FROM tr RIGHT JOIN tl ON tl.k = tr.k AND tr.v > tl.w";
    assert_explain_contains(&engine, sql, "build=right");
    assert_explain_contains(&engine, sql, "residual=");
    with_test_env(
        &[("MYDB_HASH_JOIN_BLOOM_MIN_BUILD_ROWS", Some("1"))],
        || assert_counts_with_baseline(&engine, sql, (5202, 5000)),
    );
}

/// Probe-to-build prefiltering is valid for INNER and must remain unaffected;
/// baseline comparison guards against behavioral regressions.
#[test]
fn inner_prefilter_remains_correct() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    setup_tables(&engine);

    let sql = "SELECT COUNT(*), COUNT(tr.k) FROM tl JOIN tr ON tl.k = tr.k";
    with_test_env(
        &[("MYDB_HASH_JOIN_BLOOM_MIN_BUILD_ROWS", Some("1"))],
        || assert_counts_with_baseline(&engine, sql, (10000, 10000)),
    );
}

/// For preserved-probe LEFT (build-right), do not prefilter away probe rows.
/// The build side is the inner side, so prefiltering it is safe.
/// With small tl probe and large tr build, preserve behavior under BuildSide::Right;
/// results must match the baseline.
#[test]
fn probe_preserved_left_unaffected() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    setup_tables(&engine);

    // Reverse sizes: tr(1000) LEFT JOIN tl(302) makes left_rows > right_rows.
    // The planner chooses right build tl and preserved probe tr; probe filtering is blocked
    // by probe_unmatched_must_output, while build prefiltering remains safe.
    let sql = "SELECT COUNT(*), COUNT(tl.k) FROM tr LEFT JOIN tl ON tr.k = tl.k AND tl.w < tr.v";
    assert_explain_contains(&engine, sql, "build=right");
    with_test_env(
        &[("MYDB_HASH_JOIN_BLOOM_MIN_BUILD_ROWS", Some("1"))],
        || {
            let filtered = count_pair(&engine, sql);
            let baseline = with_test_env(
                &[("MYDB_HASH_JOIN_BLOOM_MAX_BUILD_ROWS", Some("0"))],
                || count_pair(&engine, sql),
            );
            assert_eq!(
                filtered, baseline,
                "probe-preserved LEFT mismatch for {sql}"
            );
        },
    );
}
