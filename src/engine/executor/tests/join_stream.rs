//! WS-S1 chained streaming hash join tests.
//!
//! Differential baseline MYDB_HASH_JOIN_STREAM=off uses materialized cascading joins
//! as the oracle. Normalize with ORDER BY because hash joins do not preserve order.

use super::support::query_rows as rows_of;
use super::*;

fn stream_chained() -> u64 {
    native_batch_hits()
}

/// Three-table chain: unindexed k1/k2 leave only hash candidates. Use
/// /*+ ORDERED */ to fix ((t1 join t2) join t3); costs choose smaller t2/t3 builds,
/// leaving the inner join as probe, the standard streaming chain shape.
/// Without the hint, DPsub may put a smaller subtree on build and a leaf on probe;
/// that valid plan does not activate chaining, which requires a join probe.
fn setup_chain_tables(engine: &StorageQueryEngine) {
    setup_chain_tables_with_sizes(engine, 150, 45, 6);
}

fn setup_chain_tables_with_sizes(
    engine: &StorageQueryEngine,
    probe: i64,
    build1: i64,
    build2: i64,
) {
    engine
        .execute("CREATE TABLE t1 (id INT PRIMARY KEY, k1 INT, v1 INT)")
        .unwrap();
    engine
        .execute("CREATE TABLE t2 (id INT PRIMARY KEY, k1 INT, k2 INT, v2 INT)")
        .unwrap();
    engine
        .execute("CREATE TABLE t3 (id INT PRIMARY KEY, k2 INT, v3 TEXT)")
        .unwrap();
    insert_fixture_rows(
        engine,
        "test.t1",
        (0..probe).map(|i| vec![Value::Int(i), Value::Int(i % 15), Value::Int(i * 7)]),
    );
    insert_fixture_rows(
        engine,
        "test.t2",
        (0..build1).map(|i| {
            vec![
                Value::Int(i),
                Value::Int(i % 15),
                Value::Int(i % 9),
                Value::Int(i * 11),
            ]
        }),
    );
    insert_fixture_rows(
        engine,
        "test.t3",
        (0..build2).map(|i| {
            vec![
                Value::Int(i),
                Value::Int(i % 6),
                Value::String(format!("name-{i}")),
            ]
        }),
    );
}

const CHAIN_SQL: &str = "SELECT /*+ ORDERED */ t1.id, t2.id, t3.v3 FROM t1 \
     JOIN t2 ON t1.k1 = t2.k1 \
     JOIN t3 ON t2.k2 = t3.k2 \
     ORDER BY t1.id, t2.id, t3.v3";

#[test]
fn three_table_chain_matches_materialized_baseline() {
    let _guard = test_env_lock();
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    setup_chain_tables(&engine);

    let baseline = with_test_env(&[("MYDB_HASH_JOIN_STREAM", Some("off"))], || {
        rows_of(&engine, CHAIN_SQL)
    });
    assert!(!baseline.is_empty(), "chain query must produce rows");

    let before = stream_chained();
    let streamed = rows_of(&engine, CHAIN_SQL);
    assert_eq!(streamed, baseline, "chained stream must match materialized");
    assert!(
        stream_chained() > before,
        "chain streaming must engage for a 3-table hash join spine"
    );
}

#[test]
fn pipeline_off_serial_chain_matches() {
    let _guard = test_env_lock();
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    setup_chain_tables(&engine);

    let baseline = with_test_env(&[("MYDB_HASH_JOIN_STREAM", Some("off"))], || {
        rows_of(&engine, CHAIN_SQL)
    });
    let before = stream_chained();
    let serial = with_test_env(&[("MYDB_HASH_JOIN_STREAM_PIPELINE", Some("off"))], || {
        rows_of(&engine, CHAIN_SQL)
    });
    assert_eq!(serial, baseline);
    assert!(stream_chained() > before, "serial chain must still engage");
}

#[test]
fn explain_analyze_keeps_chain_stream_and_operator_stats() {
    let _guard = test_env_lock();
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    setup_chain_tables(&engine);

    let before = stream_chained();
    let QueryResponse::ResultSet(result) = engine
        .execute(&format!("EXPLAIN ANALYZE FORMAT=TREE {CHAIN_SQL}"))
        .unwrap()
    else {
        panic!("expected EXPLAIN ANALYZE result set");
    };
    let Value::String(tree) = &result.rows[0][0] else {
        panic!("expected tree output");
    };

    assert!(
        stream_chained() > before,
        "EXPLAIN ANALYZE must use the same chain stream as normal execution: {tree}"
    );
    let hash_join_lines: Vec<_> = tree
        .lines()
        .filter(|line| line.contains("Hash join"))
        .collect();
    assert!(
        hash_join_lines.len() >= 2,
        "expected a multi-join chain: {tree}"
    );
    assert!(
        hash_join_lines
            .iter()
            .all(|line| line.contains("actual time=") && !line.contains("never executed")),
        "every streamed hash-join level must retain runtime stats: {tree}"
    );
}

#[test]
fn left_join_mixed_chain_matches() {
    let _guard = test_env_lock();
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    setup_chain_tables(&engine);

    // Add preserved-probe LEFT over an INNER chain; absent t3 k2=6/7/8 yields NULL extensions.
    let sql = "SELECT /*+ ORDERED */ t1.id, t2.id, t3.v3 FROM t1 \
         JOIN t2 ON t1.k1 = t2.k1 \
         LEFT JOIN t3 ON t2.k2 = t3.k2 \
         ORDER BY t1.id, t2.id, t3.v3";
    let baseline = with_test_env(&[("MYDB_HASH_JOIN_STREAM", Some("off"))], || {
        rows_of(&engine, sql)
    });
    assert!(
        baseline.iter().any(|row| row[2] == Value::Null),
        "fixture must exercise NULL extension"
    );
    let streamed = rows_of(&engine, sql);
    assert_eq!(streamed, baseline);
}

#[test]
fn residual_chain_matches() {
    let _guard = test_env_lock();
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    setup_chain_tables(&engine);

    let sql = "SELECT /*+ ORDERED */ t1.id, t2.id, t3.v3 FROM t1 \
         JOIN t2 ON t1.k1 = t2.k1 AND t1.v1 < t2.v2 \
         JOIN t3 ON t2.k2 = t3.k2 AND t3.v3 <> 'name-2' \
         ORDER BY t1.id, t2.id, t3.v3";
    let baseline = with_test_env(&[("MYDB_HASH_JOIN_STREAM", Some("off"))], || {
        rows_of(&engine, sql)
    });
    assert!(!baseline.is_empty());
    let streamed = rows_of(&engine, sql);
    assert_eq!(streamed, baseline);
}

#[test]
fn empty_build_short_circuits_inner_chain() {
    let _guard = test_env_lock();
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    setup_chain_tables(&engine);
    engine.execute("DELETE FROM t3").unwrap();

    let baseline = with_test_env(&[("MYDB_HASH_JOIN_STREAM", Some("off"))], || {
        rows_of(&engine, CHAIN_SQL)
    });
    assert!(baseline.is_empty());
    let streamed = rows_of(&engine, CHAIN_SQL);
    assert!(streamed.is_empty());
}

#[test]
fn aggregate_over_chain_matches() {
    let _guard = test_env_lock();
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    setup_chain_tables(&engine);

    let sql = "SELECT /*+ ORDERED */ t3.v3, COUNT(*), SUM(t1.v1) FROM t1 \
         JOIN t2 ON t1.k1 = t2.k1 \
         JOIN t3 ON t2.k2 = t3.k2 \
         GROUP BY t3.v3 ORDER BY t3.v3";
    let baseline = with_test_env(&[("MYDB_HASH_JOIN_STREAM", Some("off"))], || {
        rows_of(&engine, sql)
    });
    assert!(!baseline.is_empty());
    let streamed = rows_of(&engine, sql);
    assert_eq!(streamed, baseline);
}

/// Join chain plus LIMIT without ORDER BY stops early; emitted rows should be far below
/// the full join count, apart from the channel's in-flight window.
#[test]
fn limit_early_stops_chain() {
    let _guard = test_env_lock();
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    setup_chain_tables(&engine);

    let full = rows_of(&engine, CHAIN_SQL).len() as u64;
    assert!(full > 200, "fixture should fan out (got {full})");

    let limited = rows_of(
        &engine,
        "SELECT /*+ ORDERED */ t1.id, t2.id, t3.v3 FROM t1 \
         JOIN t2 ON t1.k1 = t2.k1 \
         JOIN t3 ON t2.k2 = t3.k2 LIMIT 5",
    );
    assert_eq!(limited.len(), 5);
    assert!((limited.len() as u64) < full / 2);
}

/// S2 join projection: multilevel narrow SELECT carries only referenced intermediate columns.
/// EXPLAIN should contain Projected join; enabled/disabled results must agree.
#[test]
fn join_output_pruning_matches_and_marks_explain() {
    let _guard = test_env_lock();
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    setup_chain_tables(&engine);

    // Unreferenced t1.v1/t2.v2 must be removed from the inner join output.
    let sql = "SELECT /*+ ORDERED */ t1.id, t3.v3 FROM t1 \
         JOIN t2 ON t1.k1 = t2.k1 \
         JOIN t3 ON t2.k2 = t3.k2 \
         ORDER BY t1.id, t3.v3";

    let QueryResponse::ResultSet(explain) = engine.execute(&format!("EXPLAIN {sql}")).unwrap()
    else {
        panic!("expected EXPLAIN result set");
    };
    let extras: String = explain
        .rows
        .iter()
        .filter_map(|row| match row.last() {
            Some(Value::String(s)) => Some(s.clone()),
            _ => None,
        })
        .collect::<Vec<_>>()
        .join(" | ");
    assert!(
        extras.contains("Projected join"),
        "narrow SELECT over a join chain must prune join output, got: {extras}"
    );

    let pruned = rows_of(&engine, sql);
    let unpruned = with_test_env(&[("MYDB_JOIN_OUTPUT_PRUNE", Some("off"))], || {
        rows_of(&engine, sql)
    });
    assert_eq!(pruned, unpruned, "pruning must not change results");
    // Also compare with both pruning and streaming disabled.
    let baseline = with_test_env(
        &[
            ("MYDB_JOIN_OUTPUT_PRUNE", Some("off")),
            ("MYDB_HASH_JOIN_STREAM", Some("off")),
        ],
        || rows_of(&engine, sql),
    );
    assert_eq!(pruned, baseline);
}

/// Same-name columns: qualified references to each table's id must preserve both columns.
#[test]
fn join_output_pruning_keeps_ambiguous_named_columns() {
    let _guard = test_env_lock();
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    setup_chain_tables(&engine);

    let sql = "SELECT /*+ ORDERED */ t1.id, t2.id, t3.id FROM t1 \
         JOIN t2 ON t1.k1 = t2.k1 \
         JOIN t3 ON t2.k2 = t3.k2 \
         ORDER BY t1.id, t2.id, t3.id";
    let pruned = rows_of(&engine, sql);
    let baseline = with_test_env(
        &[
            ("MYDB_JOIN_OUTPUT_PRUNE", Some("off")),
            ("MYDB_HASH_JOIN_STREAM", Some("off")),
        ],
        || rows_of(&engine, sql),
    );
    assert_eq!(pruned, baseline);
}

/// Four-table chain with string and NULL keys covers generic noninteger buckets
/// and NULL-key skipping semantics.
#[test]
fn four_table_chain_with_string_keys_and_nulls_matches() {
    let _guard = test_env_lock();
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE a (id INT PRIMARY KEY, ka TEXT, v INT)")
        .unwrap();
    engine
        .execute("CREATE TABLE b (id INT PRIMARY KEY, ka TEXT, kb INT)")
        .unwrap();
    engine
        .execute("CREATE TABLE c (id INT PRIMARY KEY, kb INT, kc TEXT)")
        .unwrap();
    engine
        .execute("CREATE TABLE d (id INT PRIMARY KEY, kc TEXT, w TEXT)")
        .unwrap();
    let mut a = String::from("INSERT INTO a VALUES ");
    for i in 0..200 {
        if i > 0 {
            a.push(',');
        }
        if i % 11 == 0 {
            a.push_str(&format!("({i}, NULL, {i})"));
        } else {
            a.push_str(&format!("({i}, 'k{}', {i})", i % 8));
        }
    }
    engine.execute(&a).unwrap();
    engine
        .execute(
            "INSERT INTO b VALUES (0,'k0',0),(1,'k1',1),(2,'k2',2),(3,'k3',0),\
             (4,'k4',1),(5,'k5',2),(6,NULL,0),(7,'k7',1)",
        )
        .unwrap();
    engine
        .execute("INSERT INTO c VALUES (0,0,'x'),(1,1,'y'),(2,2,NULL),(3,0,'y')")
        .unwrap();
    engine
        .execute("INSERT INTO d VALUES (0,'x','dx'),(1,'y','dy'),(2,'z','dz')")
        .unwrap();

    let sql = "SELECT /*+ ORDERED */ a.id, b.id, c.id, d.w FROM a \
         JOIN b ON a.ka = b.ka \
         JOIN c ON b.kb = c.kb \
         JOIN d ON c.kc = d.kc \
         ORDER BY a.id, b.id, c.id, d.w";
    let baseline = with_test_env(
        &[
            ("MYDB_HASH_JOIN_STREAM", Some("off")),
            ("MYDB_JOIN_OUTPUT_PRUNE", Some("off")),
        ],
        || rows_of(&engine, sql),
    );
    assert!(!baseline.is_empty());
    let streamed = rows_of(&engine, sql);
    assert_eq!(streamed, baseline);
}

/// An EXISTS-derived Semi join uses the inner chain as its left probe.
#[test]
fn semi_join_over_chain_matches() {
    let _guard = test_env_lock();
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    setup_chain_tables(&engine);
    engine
        .execute("CREATE TABLE marks (id INT PRIMARY KEY, k1 INT)")
        .unwrap();
    engine
        .execute("INSERT INTO marks VALUES (1, 3), (2, 5), (3, 9), (4, 12)")
        .unwrap();

    let sql = "SELECT t1.id, t2.id FROM t1 \
         JOIN t2 ON t1.k1 = t2.k1 \
         WHERE EXISTS (SELECT 1 FROM marks m WHERE m.k1 = t1.k1) \
         ORDER BY t1.id, t2.id";
    let baseline = with_test_env(&[("MYDB_HASH_JOIN_STREAM", Some("off"))], || {
        rows_of(&engine, sql)
    });
    assert!(!baseline.is_empty());
    let streamed = rows_of(&engine, sql);
    assert_eq!(streamed, baseline);
}

/// S3a parallel prebuild: when probe keys all come from the tail leaf, no deep build
/// injection is possible; materialize builds concurrently and match serial/materialized results.
#[test]
fn parallel_prebuild_engages_and_matches() {
    let _guard = test_env_lock();
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE f (id INT PRIMARY KEY, k1 INT, k2 INT)")
        .unwrap();
    engine
        .execute("CREATE TABLE d1 (id INT PRIMARY KEY, k1 INT, v TEXT)")
        .unwrap();
    engine
        .execute("CREATE TABLE d2 (id INT PRIMARY KEY, k2 INT, w TEXT)")
        .unwrap();
    let mut f = String::from("INSERT INTO f VALUES ");
    for i in 0..400 {
        if i > 0 {
            f.push(',');
        }
        f.push_str(&format!("({}, {}, {})", i, i % 10, i % 7));
    }
    engine.execute(&f).unwrap();
    let mut d1 = String::from("INSERT INTO d1 VALUES ");
    for i in 0..10 {
        if i > 0 {
            d1.push(',');
        }
        d1.push_str(&format!("({}, {}, 'a-{}')", i, i, i));
    }
    engine.execute(&d1).unwrap();
    let mut d2 = String::from("INSERT INTO d2 VALUES ");
    for i in 0..7 {
        if i > 0 {
            d2.push(',');
        }
        d2.push_str(&format!("({}, {}, 'b-{}')", i, i, i));
    }
    engine.execute(&d2).unwrap();

    // Both levels' probe keys reference tail leaf f, leaving no deep build injection
    // opportunity and activating parallel prebuild.
    let sql = "SELECT /*+ ORDERED */ f.id, d1.v, d2.w FROM f \
         JOIN d1 ON f.k1 = d1.k1 \
         JOIN d2 ON f.k2 = d2.k2 \
         ORDER BY f.id";
    let baseline = with_test_env(
        &[
            ("MYDB_HASH_JOIN_STREAM", Some("off")),
            ("MYDB_JOIN_OUTPUT_PRUNE", Some("off")),
        ],
        || rows_of(&engine, sql),
    );
    assert_eq!(baseline.len(), 400);

    let before = native_batch_hits();
    let streamed = rows_of(&engine, sql);
    assert_eq!(streamed, baseline);
    let after = native_batch_hits();
    assert!(
        after > before,
        "leaf-keyed chain must engage parallel prebuild"
    );

    // Disabling parallel prebuild must preserve results.
    let serial_build = with_test_env(&[("MYDB_JOIN_BUILD_PARALLEL", Some("off"))], || {
        rows_of(&engine, sql)
    });
    assert_eq!(serial_build, baseline);
}

/// Runtime build overflow makes this level fall back to materialized grace without changing results.
#[test]
fn oversized_build_falls_back_to_materialized() {
    let _guard = test_env_lock();
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    // Both builds exceed the cap, but individual hash partitions fit. Tiny
    // caps on the larger shared fixture exercise recursive spill stress
    // instead of this test's chain-to-materialized fallback contract.
    setup_chain_tables_with_sizes(&engine, 30, 18, 18);

    let baseline = with_test_env(&[("MYDB_HASH_JOIN_STREAM", Some("off"))], || {
        rows_of(&engine, CHAIN_SQL)
    });
    let constrained = with_test_env(&[("MYDB_HASH_JOIN_MAX_BUILD_ROWS", Some("16"))], || {
        rows_of(&engine, CHAIN_SQL)
    });
    assert_eq!(baseline.len(), 72);
    assert_eq!(constrained, baseline);
}

// ---------------------------------------------------------------------------
// S5: INLJ as a chain probe source, streaming chunks into the chain.
// ---------------------------------------------------------------------------

fn stream_inlj_tail() -> u64 {
    native_batch_hits()
}

/// S5 fixture: t1 joins t3 by PK through INLJ, then hash joins t2. t3 greatly exceeds
/// the lookup count, favoring INLJ; small unindexed t2 leaves only a top-level hash candidate,
/// whose build is t2 and whose probe is INLJ.
fn setup_inlj_chain_tables(engine: &StorageQueryEngine) {
    engine
        .execute("CREATE TABLE s5_t1 (id INT PRIMARY KEY, k1 INT, fk3 INT, v1 INT)")
        .unwrap();
    engine
        .execute("CREATE TABLE s5_t2 (id INT PRIMARY KEY, k1 INT, v2 INT)")
        .unwrap();
    engine
        .execute("CREATE TABLE s5_t3 (id INT PRIMARY KEY, v3 TEXT)")
        .unwrap();
    insert_fixture_rows(
        engine,
        "test.s5_t1",
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
        "test.s5_t2",
        (0..45).map(|i| vec![Value::Int(i), Value::Int(i % 15), Value::Int(i * 11)]),
    );
    insert_fixture_rows(
        engine,
        "test.s5_t3",
        (0..5000).map(|i| vec![Value::Int(i), Value::String(format!("name-{}", i % 97))]),
    );
}

const S5_SQL: &str = "SELECT /*+ ORDERED */ s5_t1.id, s5_t2.v2, s5_t3.v3 FROM s5_t1 \
     JOIN s5_t3 ON s5_t1.fk3 = s5_t3.id \
     JOIN s5_t2 ON s5_t1.k1 = s5_t2.k1 \
     ORDER BY s5_t1.id, s5_t2.v2, s5_t3.v3";

/// INNER with INLJ tail: three-way differential comparison and counter assertions.
#[test]
fn inlj_tail_chain_matches_materialized_baseline() {
    let _guard = test_env_lock();
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    setup_inlj_chain_tables(&engine);

    let baseline = with_test_env(&[("MYDB_HASH_JOIN_STREAM", Some("off"))], || {
        rows_of(&engine, S5_SQL)
    });
    assert!(!baseline.is_empty(), "S5 fixture must produce rows");

    let before_tail = stream_inlj_tail();
    let before_chained = stream_chained();
    let streamed = rows_of(&engine, S5_SQL);
    assert_eq!(streamed, baseline, "S5 INLJ-tail chain must match baseline");
    assert!(
        stream_inlj_tail() > before_tail,
        "INLJ tail stream must engage (planner may not have picked INLJ probe shape)"
    );
    assert!(stream_chained() > before_chained, "chain must engage");

    // Disabling only S5 restores the old depth-one gate; both that and full disable preserve results.
    let s5_off = with_test_env(&[("MYDB_HASH_JOIN_STREAM_INLJ", Some("off"))], || {
        rows_of(&engine, S5_SQL)
    });
    assert_eq!(s5_off, baseline);
}

#[test]
fn explain_analyze_inlj_tail_keeps_nested_stats() {
    let _guard = test_env_lock();
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    setup_inlj_chain_tables(&engine);
    let expected_rows = rows_of(&engine, S5_SQL).len();

    let QueryResponse::ResultSet(result) = engine
        .execute(&format!("EXPLAIN ANALYZE FORMAT=TREE {S5_SQL}"))
        .unwrap()
    else {
        panic!("expected EXPLAIN ANALYZE result set");
    };
    let Value::String(tree) = &result.rows[0][0] else {
        panic!("expected tree output");
    };
    let inlj_line = tree
        .lines()
        .find(|line| line.contains("Index nested loop join"))
        .unwrap_or_else(|| panic!("expected streamed INLJ tail: {tree}"));
    assert!(
        inlj_line.contains("actual time=") && !inlj_line.contains("never executed"),
        "the moved INLJ clone must map stats back to the display node: {tree}"
    );
    let hash_join_line = tree
        .lines()
        .find(|line| line.contains("Hash join"))
        .unwrap_or_else(|| panic!("expected streamed hash join: {tree}"));
    assert!(
        hash_join_line.contains(&format!("rows={expected_rows} loops=1")),
        "ANALYZE must not double the hash join's rows or loops: {tree}"
    );
    for join_line in tree
        .lines()
        .filter(|line| line.contains("Hash join") || line.contains("Index nested loop join"))
    {
        assert!(
            join_line.contains("loops=1"),
            "one physical join execution must produce one ANALYZE loop: {tree}"
        );
    }
}

/// Force chunk size seven to test many boundaries without changing results.
#[test]
fn inlj_tail_chain_tiny_chunks_match() {
    let _guard = test_env_lock();
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    setup_inlj_chain_tables(&engine);

    let baseline = with_test_env(&[("MYDB_HASH_JOIN_STREAM", Some("off"))], || {
        rows_of(&engine, S5_SQL)
    });
    let tiny = with_test_env(&[("MYDB_HASH_JOIN_STREAM_INLJ_CHUNK", Some("7"))], || {
        rows_of(&engine, S5_SQL)
    });
    assert_eq!(tiny, baseline);
}

/// LEFT variant: t1 LEFT t3 is itself INLJ; missing fk3 creates NULL extensions
/// that must pass through the top hash join and chain.
#[test]
fn inlj_tail_chain_left_inlj_matches() {
    let _guard = test_env_lock();
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    setup_inlj_chain_tables(&engine);

    let sql = "SELECT /*+ ORDERED */ s5_t1.id, s5_t2.v2, s5_t3.v3 FROM s5_t1 \
         LEFT JOIN s5_t3 ON s5_t1.fk3 = s5_t3.id \
         JOIN s5_t2 ON s5_t1.k1 = s5_t2.k1 \
         ORDER BY s5_t1.id, s5_t2.v2, s5_t3.v3";
    let baseline = with_test_env(&[("MYDB_HASH_JOIN_STREAM", Some("off"))], || {
        rows_of(&engine, sql)
    });
    assert!(
        baseline.iter().any(|row| row[2] == Value::Null),
        "fixture must exercise INLJ LEFT NULL extension"
    );
    let streamed = rows_of(&engine, sql);
    assert_eq!(streamed, baseline);
}

/// Top-level preserved-probe LEFT with INLJ tail adds NULL extensions for missing t2 matches.
#[test]
fn inlj_tail_chain_top_left_matches() {
    let _guard = test_env_lock();
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    setup_inlj_chain_tables(&engine);
    engine.execute("DELETE FROM s5_t2 WHERE k1 >= 10").unwrap();

    let sql = "SELECT /*+ ORDERED */ s5_t1.id, s5_t2.v2, s5_t3.v3 FROM s5_t1 \
         JOIN s5_t3 ON s5_t1.fk3 = s5_t3.id \
         LEFT JOIN s5_t2 ON s5_t1.k1 = s5_t2.k1 \
         ORDER BY s5_t1.id, s5_t2.v2, s5_t3.v3";
    let baseline = with_test_env(&[("MYDB_HASH_JOIN_STREAM", Some("off"))], || {
        rows_of(&engine, sql)
    });
    assert!(
        baseline.iter().any(|row| row[1] == Value::Null),
        "fixture must exercise top-level LEFT NULL extension"
    );
    let streamed = rows_of(&engine, sql);
    assert_eq!(streamed, baseline);
}

/// LIMIT with INLJ tail stops chunked streaming well before full output.
#[test]
fn inlj_tail_chain_limit_early_stops() {
    let _guard = test_env_lock();
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    setup_inlj_chain_tables(&engine);

    let full = rows_of(&engine, S5_SQL).len();
    assert!(full > 400, "fixture should fan out (got {full})");
    let limited = rows_of(
        &engine,
        "SELECT /*+ ORDERED */ s5_t1.id, s5_t2.v2, s5_t3.v3 FROM s5_t1 \
         JOIN s5_t3 ON s5_t1.fk3 = s5_t3.id \
         JOIN s5_t2 ON s5_t1.k1 = s5_t2.k1 LIMIT 5",
    );
    assert_eq!(limited.len(), 5);
}

/// Mixed chain: two hash levels above an INLJ tail, consumed by aggregation.
#[test]
fn inlj_tail_under_two_hash_levels_matches() {
    let _guard = test_env_lock();
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    setup_inlj_chain_tables(&engine);
    engine
        .execute("CREATE TABLE s5_t4 (id INT PRIMARY KEY, v2 INT, w INT)")
        .unwrap();
    let mut t4 = String::from("INSERT INTO s5_t4 VALUES ");
    for i in 0..30 {
        if i > 0 {
            t4.push(',');
        }
        t4.push_str(&format!("({}, {}, {})", i, i * 11, i))
    }
    engine.execute(&t4).unwrap();

    let sql = "SELECT /*+ ORDERED */ s5_t3.v3, COUNT(*), SUM(s5_t4.w) FROM s5_t1 \
         JOIN s5_t3 ON s5_t1.fk3 = s5_t3.id \
         JOIN s5_t2 ON s5_t1.k1 = s5_t2.k1 \
         JOIN s5_t4 ON s5_t2.v2 = s5_t4.v2 \
         GROUP BY s5_t3.v3 ORDER BY s5_t3.v3";
    let baseline = with_test_env(&[("MYDB_HASH_JOIN_STREAM", Some("off"))], || {
        rows_of(&engine, sql)
    });
    assert!(!baseline.is_empty());
    let streamed = rows_of(&engine, sql);
    assert_eq!(streamed, baseline);
}

#[test]
fn system_metadata_join_inherits_statement_registry() {
    let _guard = test_env_lock();
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE fk_parent (a INT, b INT, PRIMARY KEY(a,b))")
        .unwrap();
    engine.execute("CREATE TABLE fk_child (id INT PRIMARY KEY, a INT, b INT, CONSTRAINT fk_pair FOREIGN KEY(a,b) REFERENCES fk_parent(a,b) ON UPDATE CASCADE ON DELETE SET NULL)").unwrap();
    let sql = "/* ApplicationName=nineData */ SELECT DISTINCT \
        A.REFERENCED_TABLE_NAME AS PKTABLE_NAME, A.REFERENCED_COLUMN_NAME AS PKCOLUMN_NAME, \
        A.TABLE_NAME AS FKTABLE_NAME, A.COLUMN_NAME AS FKCOLUMN_NAME, A.ORDINAL_POSITION AS KEY_SEQ, \
        CASE WHEN R.UPDATE_RULE='CASCADE' THEN 0 ELSE 3 END AS UPDATE_RULE, \
        CASE WHEN R.DELETE_RULE='SET NULL' THEN 2 ELSE 3 END AS DELETE_RULE \
        FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE A \
        JOIN INFORMATION_SCHEMA.TABLE_CONSTRAINTS B USING (CONSTRAINT_SCHEMA,CONSTRAINT_NAME,TABLE_NAME) \
        JOIN INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS R ON \
        (R.CONSTRAINT_NAME=B.CONSTRAINT_NAME AND R.TABLE_NAME=B.TABLE_NAME AND R.CONSTRAINT_SCHEMA=B.TABLE_SCHEMA) \
        WHERE B.CONSTRAINT_TYPE='FOREIGN KEY' AND A.TABLE_SCHEMA='test' AND A.TABLE_NAME='fk_child' \
        AND A.REFERENCED_TABLE_SCHEMA IS NOT NULL \
        ORDER BY A.REFERENCED_TABLE_NAME,A.ORDINAL_POSITION";
    let baseline = with_test_env(&[("MYDB_HASH_JOIN_STREAM", Some("off"))], || {
        rows_of(&engine, sql)
    });
    assert_eq!(baseline.len(), 2);
    for (i, row) in baseline.iter().enumerate() {
        assert_eq!(
            row.iter().map(Value::to_display_string).collect::<Vec<_>>(),
            vec![
                "fk_parent".to_string(),
                ["a", "b"][i].to_string(),
                "fk_child".to_string(),
                ["a", "b"][i].to_string(),
                (i + 1).to_string(),
                "0".to_string(),
                "2".to_string()
            ]
        );
    }
    // Scalar and batch engines use different internal integer/string encodings.
    let display_rows = |rows: Vec<Vec<Value>>| {
        rows.into_iter()
            .map(|row| row.iter().map(Value::to_display_string).collect::<Vec<_>>())
            .collect::<Vec<_>>()
    };
    let baseline = display_rows(baseline);
    for mode in ["off", "on", "auto"] {
        for (pipeline, prebuild) in [("off", "on"), ("on", "off"), ("on", "on")] {
            with_test_env(
                &[
                    ("MYDB_VECTORIZED", Some(mode)),
                    ("MYDB_HASH_JOIN_STREAM", Some("on")),
                    ("MYDB_HASH_JOIN_STREAM_PIPELINE", Some(pipeline)),
                    ("MYDB_JOIN_BUILD_PARALLEL", Some(prebuild)),
                ],
                || {
                    for _ in 0..2 {
                        assert_eq!(
                            display_rows(rows_of(&engine, sql)),
                            baseline,
                            "mode={mode}, pipeline={pipeline}, prebuild={prebuild}"
                        );
                    }
                    let response = engine.execute_streaming_for_test(sql).unwrap();
                    // Consume after execute_select has restored the caller's TLS.
                    let QueryResponse::ResultSet(result) = response.materialize().unwrap() else {
                        panic!("expected materialized metadata result");
                    };
                    assert_eq!(display_rows(result.rows), baseline);
                    assert!(
                        rows_of(&engine, &sql.replace("'fk_child'", "'missing_child'")).is_empty()
                    );
                    let analyzed = explain_analyze_tree(&engine, sql);
                    assert!(analyzed.contains("System table scan"), "{analyzed}");
                },
            );
        }
    }
}

#[test]
fn prepared_mixed_system_join_inherits_statement_registry() {
    let dir = tempfile::tempdir().unwrap();
    let mut engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE registry_probe (id INT PRIMARY KEY)")
        .unwrap();
    engine
        .execute("INSERT INTO registry_probe VALUES (77)")
        .unwrap();
    let _handle = engine
        .registry()
        .register(77, "root".into(), "localhost".into(), "test".into());
    let sql = "SELECT p.id, s.USER FROM registry_probe p JOIN information_schema.PROCESSLIST s ON p.id=s.ID WHERE p.id=?";
    for cache_enabled in [true, false] {
        engine.plan_cache_enabled = cache_enabled;
        let mut session = SessionContext::default();
        let parsed = engine.prepare_cached(&mut session, sql, 1).unwrap();
        let mut state = crate::engine::api::PreparedSelectMemoState::default();
        for id in [77, 78, 77] {
            let response = engine
                .execute_prepared_select(&mut session, &mut state, &parsed, sql, &[Value::Int(id)])
                .expect("mixed join must exercise compiled SELECT")
                .unwrap()
                .materialize()
                .unwrap();
            let QueryResponse::ResultSet(result) = response else {
                panic!("expected rows")
            };
            assert_eq!(
                result.rows,
                if id == 77 {
                    vec![vec![Value::Int(77), Value::String("root".into())]]
                } else {
                    vec![]
                }
            );
        }
    }
}

#[test]
fn system_set_operations_and_ctes_inherit_statement_registry() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let _handle = engine
        .registry()
        .register(77, "root".into(), "localhost".into(), "test".into());
    for mode in ["off", "on", "auto"] {
        with_test_env(&[("MYDB_VECTORIZED", Some(mode))], || {
            for sql in [
                "SELECT ID FROM information_schema.PROCESSLIST UNION SELECT ID FROM information_schema.PROCESSLIST",
                "SELECT ID FROM information_schema.PROCESSLIST UNION ALL SELECT ID FROM information_schema.PROCESSLIST WHERE ID=78",
                "WITH x AS (SELECT ID FROM information_schema.PROCESSLIST UNION ALL SELECT ID FROM information_schema.PROCESSLIST WHERE ID=78) SELECT ID FROM x",
                "SELECT ID FROM (SELECT ID FROM information_schema.PROCESSLIST UNION SELECT ID FROM information_schema.PROCESSLIST) x",
            ] {
                assert_eq!(rows_of(&engine, sql).iter().map(|row| row[0].to_display_string()).collect::<Vec<_>>(), vec!["77"]);
            }
        });
    }
}
