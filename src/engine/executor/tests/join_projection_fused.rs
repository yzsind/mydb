//! S2 fused projection: join-level projection emits narrow rows at concatenation
//! through emit helpers/StreamEmitSlot; top-level joins prune by default via MYDB_JOIN_OUTPUT_PRUNE_TOP.
//!
//! Differential baseline: MYDB_JOIN_OUTPUT_PRUNE=off and MYDB_HASH_JOIN_STREAM=off,
//! fully materialized without pruning. Normalize comparisons with ORDER BY.

use super::support::query_rows as rows_of;
use super::*;

fn projection_fused() -> u64 {
    native_batch_hits()
}

/// Two-table narrow-SELECT fixture: unreferenced t1.pad/t2.pad columns
/// should be removed from top-level join output. String pads ensure that
/// avoiding a column clone actually avoids heap allocation.
fn setup_two_tables(engine: &StorageQueryEngine) {
    engine
        .execute("CREATE TABLE t1 (id INT PRIMARY KEY, k1 INT, v1 INT, pad TEXT)")
        .unwrap();
    engine
        .execute("CREATE TABLE t2 (id INT PRIMARY KEY, k1 INT, v2 INT, pad TEXT)")
        .unwrap();
    insert_fixture_rows(
        engine,
        "test.t1",
        (0..400).map(|i| {
            vec![
                Value::Int(i),
                Value::Int(i % 20),
                Value::Int(i * 3),
                Value::String(format!("pad-one-{i}")),
            ]
        }),
    );
    insert_fixture_rows(
        engine,
        "test.t2",
        (0..50).map(|i| {
            vec![
                Value::Int(i),
                Value::Int(i % 25),
                Value::Int(i * 5),
                Value::String(format!("pad-two-{i}")),
            ]
        }),
    );
}

const BASELINE_ENV: &[(&str, Option<&str>)] = &[
    ("MYDB_JOIN_OUTPUT_PRUNE", Some("off")),
    ("MYDB_HASH_JOIN_STREAM", Some("off")),
];

/// Narrow top-level INNER: compare default fusion, top-level disabled, and all disabled;
/// also require the fusion counter to increase.
#[test]
fn top_level_pruned_inner_join_matches_baseline() {
    let _guard = test_env_lock();
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    setup_two_tables(&engine);

    let sql = "SELECT t1.id, t2.v2 FROM t1 JOIN t2 ON t1.k1 = t2.k1 ORDER BY t1.id, t2.v2";
    let before = projection_fused();
    let fused = rows_of(&engine, sql);
    assert!(
        projection_fused() > before,
        "top-level narrow SELECT must take the fused-projection path"
    );
    let top_off = with_test_env(&[("MYDB_JOIN_OUTPUT_PRUNE_TOP", Some("off"))], || {
        rows_of(&engine, sql)
    });
    let baseline = with_test_env(BASELINE_ENV, || rows_of(&engine, sql));
    assert!(!baseline.is_empty());
    assert_eq!(fused, top_off, "top-level pruning must not change results");
    assert_eq!(fused, baseline, "fused projection must match ground truth");
}

/// Ordinary aggregate blocks must retain only the join columns consumed by
/// GROUP BY / aggregate arguments.  This used to disable join-output pruning
/// wholesale, which made wide multi-join pipelines carry dead columns
/// into both Grace spill and aggregation.
#[test]
fn aggregate_over_pruned_join_matches_baseline() {
    let _guard = test_env_lock();
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    setup_two_tables(&engine);

    for sql in [
        "SELECT t1.k1, SUM(t2.v2) FROM t1 JOIN t2 ON t1.k1 = t2.k1 GROUP BY t1.k1 ORDER BY t1.k1",
        "SELECT t1.k1, COUNT(t2.id) FROM t1 LEFT JOIN t2 ON t1.k1 = t2.k1 GROUP BY t1.k1 HAVING COUNT(t2.id) > 0 ORDER BY t1.k1",
    ] {
        let baseline = with_test_env(BASELINE_ENV, || rows_of(&engine, sql));
        let pruned = rows_of(&engine, sql);
        assert_eq!(pruned, baseline, "aggregate join pruning mismatch: {sql}");
    }
}

#[test]
fn dense_preserved_join_count_aggregate_matches_row_oracle() {
    let _guard = test_env_lock();
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE dc_build (id INT PRIMARY KEY, k INT)")
        .unwrap();
    engine
        .execute("CREATE TABLE dc_probe (id INT PRIMARY KEY, k INT, v INT)")
        .unwrap();
    engine
        .execute("INSERT INTO dc_build VALUES (1,1),(2,1),(3,2),(4,4),(5,NULL),(6,NULL)")
        .unwrap();
    engine
        .execute("INSERT INTO dc_probe VALUES (10,1,7),(11,1,NULL),(12,2,8),(13,3,9)")
        .unwrap();
    let sql = "SELECT /*+ ORDERED */ dc_build.k, COUNT(dc_probe.id), \
               COUNT(dc_probe.v), COUNT(*) FROM dc_build \
               LEFT JOIN dc_probe ON dc_build.k = dc_probe.k \
               GROUP BY dc_build.k ORDER BY dc_build.k";
    use crate::engine::executor::vec::{with_vec_mode, VecMode};
    let vector = with_vec_mode(VecMode::On, || rows_of(&engine, sql));
    let row = with_vec_mode(VecMode::Off, || rows_of(&engine, sql));
    assert_eq!(vector, row);
    assert_eq!(
        vector,
        vec![
            vec![Value::Null, Value::Int(0), Value::Int(0), Value::Int(2)],
            vec![Value::Int(1), Value::Int(4), Value::Int(2), Value::Int(4)],
            vec![Value::Int(2), Value::Int(1), Value::Int(1), Value::Int(1)],
            vec![Value::Int(4), Value::Int(0), Value::Int(0), Value::Int(1)],
        ]
    );
}

/// Preserved-probe LEFT plus projection: NULL extensions use fused null_extend.
/// Large t1 probe and small t2 build make LEFT t1 probe-preserved;
/// also cover the reverse t2 LEFT t1 direction.
#[test]
fn top_level_pruned_left_join_null_extension_matches() {
    let _guard = test_env_lock();
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    setup_two_tables(&engine);
    // t1.k1 spans [0,20); remove some t1 rows to produce NULL extensions in t2 LEFT t1.
    engine.execute("DELETE FROM t1 WHERE k1 >= 15").unwrap();

    for sql in [
        "SELECT t2.id, t1.v1 FROM t2 LEFT JOIN t1 ON t1.k1 = t2.k1 ORDER BY t2.id, t1.v1",
        // t1.id greatly exceeds t2.id's range, yielding many NULL extensions in the reverse direction.
        "SELECT t1.id, t2.v2 FROM t1 LEFT JOIN t2 ON t1.id = t2.id ORDER BY t1.id, t2.v2",
    ] {
        let baseline = with_test_env(BASELINE_ENV, || rows_of(&engine, sql));
        assert!(
            baseline.iter().any(|row| row[1] == Value::Null),
            "fixture must exercise NULL extension for {sql}"
        );
        let fused = rows_of(&engine, sql);
        assert_eq!(
            fused, baseline,
            "LEFT join fused projection mismatch: {sql}"
        );
    }
}

/// Old residual path with MYDB_HASH_JOIN_RESIDUAL_PRECHECK=off concatenates full rows
/// before evaluation; project_joined_row_in_place narrows the prebuilt row.
#[test]
fn legacy_residual_path_with_projection_matches() {
    let _guard = test_env_lock();
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    setup_two_tables(&engine);

    let sql = "SELECT t1.id, t2.v2 FROM t1 JOIN t2 ON t1.k1 = t2.k1 AND t1.v1 < t2.v2 \
         ORDER BY t1.id, t2.v2";
    let baseline = with_test_env(BASELINE_ENV, || rows_of(&engine, sql));
    assert!(!baseline.is_empty());
    let fused = rows_of(&engine, sql);
    assert_eq!(fused, baseline);
    let legacy = with_test_env(&[("MYDB_HASH_JOIN_RESIDUAL_PRECHECK", Some("off"))], || {
        rows_of(&engine, sql)
    });
    assert_eq!(legacy, baseline, "legacy residual path must project too");
}

/// String-key generic buckets exercise fused emit outside the integer fast path.
#[test]
fn string_key_gen_path_with_projection_matches() {
    let _guard = test_env_lock();
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE s1 (id INT PRIMARY KEY, ks TEXT, v INT, pad TEXT)")
        .unwrap();
    engine
        .execute("CREATE TABLE s2 (id INT PRIMARY KEY, ks TEXT, w INT, pad TEXT)")
        .unwrap();
    let mut s1 = String::from("INSERT INTO s1 VALUES ");
    for i in 0..300 {
        if i > 0 {
            s1.push(',');
        }
        // One tenth of keys are NULL and neither enter nor match buckets.
        if i % 10 == 0 {
            s1.push_str(&format!("({i}, NULL, {i}, 'p{i}')"));
        } else {
            s1.push_str(&format!("({i}, 'key-{}', {i}, 'p{i}')", i % 12));
        }
    }
    engine.execute(&s1).unwrap();
    let mut s2 = String::from("INSERT INTO s2 VALUES ");
    for i in 0..40 {
        if i > 0 {
            s2.push(',');
        }
        s2.push_str(&format!("({i}, 'key-{}', {i}, 'q{i}')", i % 15));
    }
    engine.execute(&s2).unwrap();

    let sql = "SELECT s1.id, s2.w FROM s1 JOIN s2 ON s1.ks = s2.ks ORDER BY s1.id, s2.w";
    let baseline = with_test_env(BASELINE_ENV, || rows_of(&engine, sql));
    assert!(!baseline.is_empty());
    let fused = rows_of(&engine, sql);
    assert_eq!(fused, baseline);
}

/// SEMI/ANTI pruning: the planner excludes top-level projection for these joins,
/// while intermediate levels still prune; all three result sets agree.
#[test]
fn semi_anti_with_pruning_matches() {
    let _guard = test_env_lock();
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    setup_two_tables(&engine);

    for sql in [
        "SELECT t1.id FROM t1 WHERE t1.k1 IN (SELECT k1 FROM t2 WHERE v2 > 50) ORDER BY t1.id",
        "SELECT t1.id FROM t1 WHERE t1.k1 NOT IN (SELECT k1 FROM t2 WHERE v2 > 50) ORDER BY t1.id",
    ] {
        let baseline = with_test_env(BASELINE_ENV, || rows_of(&engine, sql));
        let fused = rows_of(&engine, sql);
        assert_eq!(fused, baseline, "semi/anti mismatch: {sql}");
    }
}

/// Lower parallel thresholds to exercise parallel-probe and partition-parallel projection.
#[test]
fn parallel_paths_with_projection_match() {
    let _guard = test_env_lock();
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    setup_two_tables(&engine);

    let sql = "SELECT t1.id, t2.v2 FROM t1 JOIN t2 ON t1.k1 = t2.k1 ORDER BY t1.id, t2.v2";
    let baseline = with_test_env(BASELINE_ENV, || rows_of(&engine, sql));

    // Shared-build parallel probing with a small build below the byte limit.
    let parallel_probe = with_test_env(
        &[
            ("MYDB_HASH_JOIN_PARALLELISM_THRESHOLD", Some("1")),
            ("MYDB_HASH_JOIN_PARALLELISM", Some("4")),
            ("MYDB_HASH_JOIN_STREAM", Some("off")),
        ],
        || rows_of(&engine, sql),
    );
    assert_eq!(
        parallel_probe, baseline,
        "parallel probe projection mismatch"
    );

    // Disable parallel probing to force execute_hash_join_parallel partitioning.
    let partitioned = with_test_env(
        &[
            ("MYDB_HASH_JOIN_PARALLELISM_THRESHOLD", Some("1")),
            ("MYDB_HASH_JOIN_PARALLELISM", Some("4")),
            ("MYDB_HASH_JOIN_PARALLEL_PROBE_DISABLE", Some("1")),
            ("MYDB_HASH_JOIN_STREAM", Some("off")),
        ],
        || rows_of(&engine, sql),
    );
    assert_eq!(
        partitioned, baseline,
        "partitioned parallel projection mismatch"
    );
}

/// Grace spill/hot-key chunking plus projection: tiny row limits force partitions;
/// minimum legal recursion depth then forces bounded chunks.
#[test]
fn grace_and_chunked_hot_partition_with_projection_match() {
    let _guard = test_env_lock();
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    setup_two_tables(&engine);

    let sql = "SELECT t1.id, t2.v2 FROM t1 JOIN t2 ON t1.k1 = t2.k1 ORDER BY t1.id, t2.v2";
    let baseline = with_test_env(BASELINE_ENV, || rows_of(&engine, sql));

    let grace = with_test_env(
        &[
            ("MYDB_HASH_JOIN_MAX_BUILD_ROWS", Some("8")),
            ("MYDB_HASH_JOIN_STREAM", Some("off")),
        ],
        || rows_of(&engine, sql),
    );
    assert_eq!(grace, baseline, "grace spill projection mismatch");

    let chunked = with_test_env(
        &[
            ("MYDB_HASH_JOIN_MAX_BUILD_ROWS", Some("8")),
            ("MYDB_HASH_JOIN_GRACE_MAX_DEPTH", Some("1")),
            ("MYDB_HASH_JOIN_STREAM", Some("off")),
        ],
        || rows_of(&engine, sql),
    );
    assert_eq!(chunked, baseline, "chunked hash projection mismatch");
}

/// Streaming-inner with a bare probe scan uses fused StreamEmitSlot projection;
/// also cover build-preserved streaming LEFT outer joins.
#[test]
fn streaming_inner_with_projection_matches() {
    let _guard = test_env_lock();
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    setup_two_tables(&engine);

    // INNER: small t2 build with bare-scan t1 probe selects streaming-inner.
    let inner = "SELECT t1.id, t2.v2 FROM t1 JOIN t2 ON t1.k1 = t2.k1 ORDER BY t1.id, t2.v2";
    // Build-preserved LEFT: CBO usually builds small preserved t2.
    let left = "SELECT t2.id, t1.v1 FROM t2 LEFT JOIN t1 ON t2.k1 = t1.k1 ORDER BY t2.id, t1.v1";
    for sql in [inner, left] {
        let baseline = with_test_env(BASELINE_ENV, || rows_of(&engine, sql));
        let streamed = with_test_env(&[("MYDB_HASH_JOIN_STREAM", Some("off"))], || {
            rows_of(&engine, sql)
        });
        assert_eq!(
            streamed, baseline,
            "streaming-inner projection mismatch: {sql}"
        );
        let default_path = rows_of(&engine, sql);
        assert_eq!(default_path, baseline, "default path mismatch: {sql}");
    }
}

/// A4 chained generic buckets: string keys and heavy build-key duplication require
/// fan-out order identical to SmallVec layout. Cover in-memory,
/// parallel-probe, and streaming-inner paths.
#[test]
fn chained_gen_layout_matches_smallvec_layout() {
    let _guard = test_env_lock();
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE c1 (id INT PRIMARY KEY, ks TEXT, v INT)")
        .unwrap();
    engine
        .execute("CREATE TABLE c2 (id INT PRIMARY KEY, ks TEXT, w INT)")
        .unwrap();
    // c2 build has eight rows per duplicate key; c1 probe has 300 rows.
    let mut c1 = String::from("INSERT INTO c1 VALUES ");
    for i in 0..300 {
        if i > 0 {
            c1.push(',');
        }
        c1.push_str(&format!("({i}, 'k{}', {i})", i % 6));
    }
    engine.execute(&c1).unwrap();
    let mut c2 = String::from("INSERT INTO c2 VALUES ");
    for i in 0..48 {
        if i > 0 {
            c2.push(',');
        }
        c2.push_str(&format!("({i}, 'k{}', {i})", i % 6));
    }
    engine.execute(&c2).unwrap();

    // Compare without ORDER BY: both layouts must preserve build insertion order inside buckets,
    // with identical row-by-row output, the A4 ordering contract.
    let sql = "SELECT c1.id, c2.id, c2.w FROM c1 JOIN c2 ON c1.ks = c2.ks";
    for env in [
        vec![("MYDB_HASH_JOIN_STREAM", Some("off"))],
        vec![
            ("MYDB_HASH_JOIN_STREAM", Some("off")),
            ("MYDB_HASH_JOIN_PARALLELISM_THRESHOLD", Some("1")),
            ("MYDB_HASH_JOIN_PARALLELISM", Some("4")),
        ],
        vec![],
    ] {
        let chained = with_test_env(&env, || rows_of(&engine, sql));
        let mut legacy_env = env.clone();
        legacy_env.push(("MYDB_HASH_JOIN_CHAINED_LAYOUT", Some("off")));
        let legacy = with_test_env(&legacy_env, || rows_of(&engine, sql));
        assert_eq!(
            chained, legacy,
            "chained layout must match SmallVec layout row-for-row, env={env:?}"
        );
    }

    // LEFT plus duplicate keys: match bitmaps and NULL extensions agree across layouts after sorting.
    let left = "SELECT c2.id, c1.v FROM c2 LEFT JOIN c1 ON c2.ks = c1.ks AND c1.v > 250 \
         ORDER BY c2.id, c1.v";
    let chained = rows_of(&engine, left);
    let legacy = with_test_env(&[("MYDB_HASH_JOIN_CHAINED_LAYOUT", Some("off"))], || {
        rows_of(&engine, left)
    });
    assert_eq!(chained, legacy);
}

/// WS-2/3a parallel scatter: enough probe rows trigger parallel partition-ID calculation;
/// on/off results match the serial baseline row-for-row and counters increase.
#[test]
fn parallel_scatter_matches_serial() {
    let _guard = test_env_lock();
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE ps_b (id INT PRIMARY KEY, k INT, v INT)")
        .unwrap();
    engine
        .execute("CREATE TABLE ps_p (id INT PRIMARY KEY, k INT, w INT)")
        .unwrap();
    let mut b = String::from("INSERT INTO ps_b VALUES ");
    for i in 0..200 {
        if i > 0 {
            b.push(',');
        }
        b.push_str(&format!("({i}, {}, {i})", i % 97));
    }
    engine.execute(&b).unwrap();
    // Test builds use a 2K threshold to avoid repeatedly generating the production 32K rows.
    // Probe: double a 256-row seed three times to reach 2048 rows.
    let mut p = String::from("INSERT INTO ps_p VALUES ");
    for i in 0..256 {
        if i > 0 {
            p.push(',');
        }
        // All probe keys fall within build keys 0..97, so Phase E scan-side Bloom
        // cannot remove rows before materialization; all rows reach scatter.
        p.push_str(&format!("({i}, {}, {i})", i % 97));
    }
    engine.execute(&p).unwrap();
    for step in 0..3 {
        let off = 256 << step;
        engine
            .execute(&format!(
                "INSERT INTO ps_p SELECT id + {off}, k, w FROM ps_p"
            ))
            .unwrap();
    }

    let sql = "SELECT ps_p.id, ps_b.v FROM ps_p JOIN ps_b ON ps_p.k = ps_b.k \
         ORDER BY ps_p.id, ps_b.v LIMIT 500";
    use crate::engine::executor::vec::{with_vec_mode, VecMode};
    let before = native_batch_hits();
    let parallel = with_vec_mode(VecMode::On, || rows_of(&engine, sql));
    assert!(
        native_batch_hits() > before,
        "test-threshold probe should use the common batch join"
    );
    let scalar = with_vec_mode(VecMode::Off, || rows_of(&engine, sql));
    assert_eq!(parallel, scalar, "batch and scalar kernels must agree");
}

/// Wide-row multicolumn projection must preserve referenced column order relative to
/// the unpruned plan in a three-table chain with intermediate and top-level pruning.
#[test]
fn chained_joins_top_and_mid_level_pruning_matches() {
    let _guard = test_env_lock();
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    setup_two_tables(&engine);
    engine
        .execute("CREATE TABLE t3 (id INT PRIMARY KEY, v2 INT, name TEXT, pad TEXT)")
        .unwrap();
    let mut t3 = String::from("INSERT INTO t3 VALUES ");
    for i in 0..120 {
        if i > 0 {
            t3.push(',');
        }
        t3.push_str(&format!("({}, {}, 'n{}', 'p{}')", i, i * 5, i, i));
    }
    engine.execute(&t3).unwrap();

    let sql = "SELECT /*+ ORDERED */ t1.id, t3.name FROM t1 \
         JOIN t2 ON t1.k1 = t2.k1 \
         JOIN t3 ON t2.v2 = t3.v2 \
         ORDER BY t1.id, t3.name";
    let baseline = with_test_env(BASELINE_ENV, || rows_of(&engine, sql));
    assert!(!baseline.is_empty());
    let fused = rows_of(&engine, sql);
    assert_eq!(fused, baseline);
}

/// Bounded MRR chunks must preserve both existence decisions independently,
/// including NULL keys/values, duplicate candidates and right-only residuals.
#[test]
fn consecutive_existence_mrr_batches_match_row_oracle() {
    let _guard = test_env_lock();
    for (ty, values) in [
        ("INT", ["1", "2", "3"]),
        ("DECIMAL(12,2)", ["1.25", "2.50", "3.75"]),
        ("DATE", ["'2020-01-01'", "'2020-02-01'", "'2020-03-01'"]),
        ("VARCHAR(20)", ["'alpha'", "'beta'", "'gamma'"]),
    ] {
        let dir = tempfile::tempdir().unwrap();
        let engine = open_test_engine(dir.path());
        engine
            .execute(&format!(
                "CREATE TABLE probes (id INT PRIMARY KEY, k INT, v {ty})"
            ))
            .unwrap();
        engine.execute(&format!("CREATE TABLE candidates (id INT PRIMARY KEY, k INT, v {ty}, flag INT, INDEX ix(k))")).unwrap();
        engine.execute(&format!(
            "INSERT INTO probes VALUES (1,1,{}),(2,1,{}),(3,2,{}),(4,3,{}),(5,4,{}),(6,NULL,{}),(7,1,NULL),(8,5,{})",
            values[0],values[1],values[0],values[0],values[0],values[0],values[2]
        )).unwrap();
        let mut insert = format!(
            "INSERT INTO candidates VALUES (1,1,{},0),(2,1,{},1),(3,1,{},1),(4,2,{},0),(5,3,NULL,1),(6,NULL,{},1),(7,5,{},NULL)",
            values[0],values[1],values[1],values[1],values[0],values[0]
        );
        for id in 8..1008 {
            insert.push_str(&format!(",({id},{id},{},1)", values[0]));
        }
        engine.execute(&insert).unwrap();
        engine.execute("ANALYZE TABLE probes,candidates").unwrap();
        let fusion_counter = &crate::engine::executor::select::plan::INLJ_FUSED_SEMI_ANTI_COUNT;
        let fused_before = fusion_counter.load(std::sync::atomic::Ordering::Relaxed);
        for first_not in [false, true] {
            for second_not in [false, true] {
                let first = if first_not { "NOT " } else { "" };
                let second = if second_not { "NOT " } else { "" };
                let sql = format!(
                    "SELECT /*+ INDEX_NLJ(o,r) INDEX_NLJ(o,s) */ o.id,COUNT(*) FROM probes o \
                     WHERE {first}EXISTS (SELECT 1 FROM candidates r WHERE r.k=o.k AND r.v<>o.v) \
                     AND {second}EXISTS (SELECT 1 FROM candidates s WHERE s.k=o.k AND s.v<>o.v AND s.flag>0) \
                     GROUP BY o.id ORDER BY o.id"
                );
                let oracle = with_test_env(
                    &[
                        ("MYDB_VECTORIZED", Some("off")),
                        ("MYDB_INLJ_FUSE_SEMI_ANTI", Some("off")),
                    ],
                    || rows_of(&engine, &sql),
                );
                let actual = with_test_env(
                    &[
                        ("MYDB_VECTORIZED", Some("on")),
                        ("MYDB_HASH_JOIN_STREAM_INLJ_CHUNK", Some("3")),
                    ],
                    || rows_of(&engine, &sql),
                );
                assert_eq!(actual, oracle, "{ty}: {sql}");
                let expected = [
                    (1, true, true),
                    (2, true, false),
                    (3, true, false),
                    (4, false, false),
                    (5, false, false),
                    (6, false, false),
                    (7, false, false),
                    (8, true, false),
                ]
                .into_iter()
                .filter(|(_, a, b)| (*a != first_not) && (*b != second_not))
                .map(|(id, _, _)| vec![Value::Int(id), Value::Int(1)])
                .collect::<Vec<_>>();
                assert_eq!(actual, expected, "{ty}: {sql}");
            }
        }
        assert!(
            fusion_counter.load(std::sync::atomic::Ordering::Relaxed) > fused_before,
            "{ty}: the differential test must execute the fused MRR path"
        );
    }
}

#[test]
fn spilled_join_locality_is_requested_only_by_matching_index_probes() {
    let _guard = test_env_lock();
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    for table in ["locality_a", "locality_b"] {
        engine
            .execute(&format!("CREATE TABLE {table} (k INT, v INT)"))
            .unwrap();
        let rows = (0..256)
            .map(|key| format!("({key},{key})"))
            .collect::<Vec<_>>()
            .join(",");
        engine
            .execute(&format!("INSERT INTO {table} VALUES {rows}"))
            .unwrap();
    }
    engine
        .execute("CREATE TABLE locality_c (k INT, v INT, INDEX ix(k))")
        .unwrap();
    let rows = (0..1024)
        .map(|key| format!("({key},{})", key + 1))
        .collect::<Vec<_>>()
        .join(",");
    engine
        .execute(&format!("INSERT INTO locality_c VALUES {rows}"))
        .unwrap();
    engine
        .execute("ANALYZE TABLE locality_a, locality_b, locality_c")
        .unwrap();
    let counter = &crate::engine::executor::select::vec_bridge::VEC_HASH_JOIN_RANGE_PARTITION_COUNT;
    with_test_env(
        &[
            ("MYDB_HASH_JOIN_MAX_BUILD_ROWS", Some("64")),
            ("MYDB_HASH_JOIN_GRACE_PARTITIONS", Some("8")),
            ("MYDB_PARALLEL_DEGREE", Some("1")),
            ("MYDB_HASH_JOIN_RANGE_PARTITION", Some("on")),
        ],
        || {
            let plain = "SELECT /*+ ORDERED HASH_JOIN(a,b) */ a.k, b.v FROM locality_a a JOIN locality_b b ON a.k=b.k AND a.v<=b.v ORDER BY a.k";
            let before = counter.load(std::sync::atomic::Ordering::Relaxed);
            let expected = rows_of(&engine, plain);
            assert_eq!(expected.len(), 256);
            assert_eq!(
                counter.load(std::sync::atomic::Ordering::Relaxed),
                before,
                "a join without index consumers must retain hash partitioning"
            );
            for key in ["b.k", "b.v"] {
                let sql = format!("SELECT /*+ ORDERED INDEX_NLJ(b,r) INDEX_NLJ(b,s) HASH_JOIN(a,b) */ a.k,b.v \
                FROM locality_a a JOIN locality_b b ON a.k=b.k AND a.v<=b.v \
                WHERE EXISTS (SELECT 1 FROM locality_c r WHERE r.k={key} AND r.v<>b.v) \
                AND NOT EXISTS (SELECT 1 FROM locality_c s WHERE s.k={key} AND s.v<b.v) ORDER BY a.k");
                let oracle =
                    with_test_env(&[("MYDB_HASH_JOIN_RANGE_PARTITION", Some("off"))], || {
                        rows_of(&engine, &sql)
                    });
                let before = counter.load(std::sync::atomic::Ordering::Relaxed);
                assert_eq!(rows_of(&engine, &sql), oracle);
                assert_eq!(
                    counter.load(std::sync::atomic::Ordering::Relaxed) > before,
                    key == "b.k",
                    "{sql}\n{:?}",
                    rows_of(&engine, &format!("EXPLAIN FORMAT=TREE {sql}"))
                );
                // Profiling wrappers must forward the optional hint, not alter
                // which physical execution strategy the query can use.
                let before = counter.load(std::sync::atomic::Ordering::Relaxed);
                engine.execute(&format!("EXPLAIN ANALYZE {sql}")).unwrap();
                assert_eq!(
                    counter.load(std::sync::atomic::Ordering::Relaxed) > before,
                    key == "b.k"
                );
            }
        },
    );
}

#[test]
fn columnar_fused_existence_checks_preserve_typed_keys_nulls_and_duplicates() {
    let _guard = test_env_lock();
    std::fs::create_dir_all("/tmp/mydb").unwrap();
    for (key_type, key_a, key_b, lookup_a) in [
        ("BIGINT UNSIGNED", "1", "2", "1"),
        ("DECIMAL(18,2)", "1.25", "2.50", "1.25"),
        (
            "VARCHAR(20) COLLATE utf8mb4_0900_ai_ci",
            "'Alpha'",
            "'Beta'",
            "'alpha'",
        ),
    ] {
        let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
        let engine = open_test_engine(dir.path());
        engine.execute(&format!("CREATE TABLE probes (id INT PRIMARY KEY, k {key_type}, part INT, v BIGINT UNSIGNED)")).unwrap();
        engine.execute(&format!("CREATE TABLE candidates (id INT PRIMARY KEY, k {key_type}, part INT, v BIGINT UNSIGNED, flag INT, INDEX ix(k,part))")).unwrap();
        let (v1, v2) = ("18446744073709551614", "18446744073709551615");
        engine.execute(&format!("INSERT INTO probes VALUES (1,{key_a},1,{v1}),(2,{key_a},1,{v2}),(3,{key_b},1,{v1}),(4,NULL,1,{v1}),(5,{key_a},2,{v1}),(6,{key_a},1,NULL)")).unwrap();
        engine.execute(&format!("INSERT INTO candidates VALUES (1,{lookup_a},1,{v1},0),(2,{lookup_a},1,{v2},1),(3,{lookup_a},1,{v2},1),(4,{key_b},1,NULL,1),(5,{lookup_a},2,{v1},1),(6,NULL,1,{v2},1)")).unwrap();
        engine.execute("ANALYZE TABLE probes,candidates").unwrap();
        let counter = &crate::engine::executor::select::vec_bridge::FUSED_INLJ_BATCH_HITS;
        let before = counter.load(std::sync::atomic::Ordering::Relaxed);
        for first_not in [false, true] {
            for second_not in [false, true] {
                let first = if first_not { "NOT " } else { "" };
                let second = if second_not { "NOT " } else { "" };
                let sql = format!("SELECT /*+ INDEX_NLJ(o,r) INDEX_NLJ(o,s) */ o.id,COUNT(*) FROM probes o WHERE {first}EXISTS (SELECT 1 FROM candidates r WHERE r.k=o.k AND r.part=o.part AND r.v<>o.v) AND {second}EXISTS (SELECT 1 FROM candidates s WHERE s.k=o.k AND s.part=o.part AND s.v<>o.v AND s.flag>0) GROUP BY o.id ORDER BY o.id");
                let expected = [
                    (1, true, true),
                    (2, true, false),
                    (3, false, false),
                    (4, false, false),
                    (5, false, false),
                    (6, false, false),
                ]
                .into_iter()
                .filter(|(_, a, b)| *a != first_not && *b != second_not)
                .map(|(id, _, _)| vec![Value::Int(id), Value::Int(1)])
                .collect::<Vec<_>>();
                let oracle = with_test_env(
                    &[
                        ("MYDB_VECTORIZED", Some("off")),
                        ("MYDB_INLJ_FUSE_SEMI_ANTI", Some("off")),
                    ],
                    || rows_of(&engine, &sql),
                );
                let actual = with_test_env(
                    &[
                        ("MYDB_VECTORIZED", Some("on")),
                        ("MYDB_HASH_JOIN_STREAM_INLJ_CHUNK", Some("3")),
                    ],
                    || rows_of(&engine, &sql),
                );
                assert_eq!(actual, expected, "{key_type}: {sql}");
                assert_eq!(
                    actual, oracle,
                    "{key_type}: columnar and ordinary probes must agree"
                );
            }
        }
        assert!(
            counter.load(std::sync::atomic::Ordering::Relaxed) > before,
            "{key_type}: native fused input must execute"
        );
    }
}

#[test]
fn fused_existence_stops_decided_keys_but_exhausts_negative_proofs() {
    let _guard = test_env_lock();
    std::fs::create_dir_all("/tmp/mydb").unwrap();
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE probes (id INT PRIMARY KEY,k INT,a INT,b INT)")
        .unwrap();
    engine
        .execute(
            "CREATE TABLE candidates (id INT PRIMARY KEY,k INT,a INT,b INT,flag INT,INDEX ix(k))",
        )
        .unwrap();
    engine
        .execute("INSERT INTO probes VALUES (1,1,NULL,20),(2,1,10,NULL),(3,2,10,20),(4,NULL,10,20)")
        .unwrap();
    let values = (1..=400)
        .map(|id| {
            if id <= 200 {
                format!("({id},1,11,21,1)")
            } else {
                format!("({id},2,10,20,1)")
            }
        })
        .collect::<Vec<_>>();
    engine
        .execute(&format!(
            "INSERT INTO candidates VALUES {}",
            values.join(",")
        ))
        .unwrap();
    engine.execute("ANALYZE TABLE probes,candidates").unwrap();
    for first_not in [false, true] {
        for second_not in [false, true] {
            let first = if first_not { "NOT " } else { "" };
            let second = if second_not { "NOT " } else { "" };
            let sql = format!("SELECT /*+ INDEX_NLJ(o,r) INDEX_NLJ(o,s) */ o.id FROM probes o WHERE {first}EXISTS(SELECT 1 FROM candidates r WHERE r.k=o.k AND r.a<>o.a) AND {second}EXISTS(SELECT 1 FROM candidates s WHERE s.k=o.k AND s.b<>o.b AND s.flag>0) ORDER BY o.id");
            let oracle = with_test_env(
                &[
                    ("MYDB_VECTORIZED", Some("off")),
                    ("MYDB_INLJ_FUSE_SEMI_ANTI", Some("off")),
                ],
                || rows_of(&engine, &sql),
            );
            let entries_before = crate::engine::executor::select::plan::INLJ_INDEX_ENTRIES
                .load(std::sync::atomic::Ordering::Relaxed);
            let rows_before = crate::engine::executor::select::plan::INLJ_CANDIDATE_ROW_IDS
                .load(std::sync::atomic::Ordering::Relaxed);
            let actual = with_test_env(&[("MYDB_VECTORIZED", Some("on"))], || {
                rows_of(&engine, &sql)
            });
            assert_eq!(actual, oracle, "{sql}");
            let entries = crate::engine::executor::select::plan::INLJ_INDEX_ENTRIES
                .load(std::sync::atomic::Ordering::Relaxed)
                - entries_before;
            let read = crate::engine::executor::select::plan::INLJ_CANDIDATE_ROW_IDS
                .load(std::sync::atomic::Ordering::Relaxed)
                - rows_before;
            assert!(
                read < entries,
                "decided keys must skip unread candidates: read={read} entries={entries}"
            );
            if first_not && second_not {
                assert!(actual.contains(&vec![Value::Int(3)]), "all same-value candidates must be exhausted before negative existence is proved");
            }
        }
    }
}
