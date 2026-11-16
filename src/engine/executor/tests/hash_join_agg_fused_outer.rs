//! S1/S2/S3/S4: admission and correctness of fused HashAggregate-over-HashJoin
//! for all outer-join forms. Differential baseline: MYDB_HASH_JOIN_AGG_FUSED_DISABLE=1,
//! which runs unfused join followed by aggregation as the correctness oracle.
//!
//! Key semantics:
//! When build is preserved (LEFT/build-left or RIGHT/build-right), probe behaves
//! like INNER; only unmatched build rows add NULL extensions with all probe columns NULL.
//! Probe-column COUNT/SUM/AVG/MIN/MAX are exact over matched pairs;
//! COUNT(*) and COUNT(build column) use per-bucket match bitmaps and final compensation
//! for unmatched buckets and NULL-key build rows.
//! When probe is preserved (LEFT/build-right or RIGHT/build-left), each unmatched probe
//! row appears once with its original values and NULL build columns; probe miss/NULL-key
//! branches update the accumulators directly.
//! Historical bug: outer joins silently returned INNER results on the fused path, losing rows.
//!
//! Coverage includes default streaming parallel page scans and materialized bucket maps
//! with STREAM_PROBE_DISABLE, specialized i64/generic string keys, sliced parallelism,
//! and sampled delayed Bloom filters.

use super::*;

fn fused_count() -> u64 {
    native_batch_hits()
}

fn one_row(engine: &StorageQueryEngine, sql: &str) -> Vec<Value> {
    let QueryResponse::ResultSet(rs) = engine.execute(sql).unwrap() else {
        panic!("expected result set for {sql}");
    };
    assert_eq!(rs.rows.len(), 1, "single aggregate row for {sql}");
    rs.rows.into_iter().next().unwrap()
}

/// Compare every value with the unfused baseline. The switch is read at execution time,
/// enabling two executions of one cached plan; thread-local overrides isolate concurrent tests.
fn assert_matches_unfused_baseline(engine: &StorageQueryEngine, sql: &str) -> Vec<Value> {
    let baseline = with_test_env(&[("MYDB_VECTORIZED", Some("off"))], || one_row(engine, sql));
    let fused = with_test_env(&[("MYDB_VECTORIZED", Some("on"))], || one_row(engine, sql));
    assert_eq!(fused, baseline, "fused vs unfused mismatch for {sql}");
    fused
}

/// Data shape: unindexed join keys leave the planner only hash candidates.
/// tl has 202 rows: i<100 gives k=i%20, five per key, all matching tr; v=i*3.
/// i in [100,200) gives unmatched k=1000+i; IDs 200/201 have NULL keys.
/// tr has 502 rows: i<400 gives k=i%20, twenty per key, all matching tl; v=i*7.
/// j in [0,100) gives unmatched k=5000+j; IDs 3000/3001 have NULL keys and non-NULL v.
///
/// Hand-calculated expectations:
/// Matched pairs = 100*20 = 2000; matched SUM(tr.v) = 5*7*sum(0..399) = 2_793_000.
/// tl LEFT JOIN tr has 102 unmatched tl rows, including two NULL keys.
///   COUNT(*)=2102, COUNT(tr.v)=2000, COUNT(tl.v)=2102。
/// tr LEFT JOIN tl has 102 unmatched tr rows.
///   COUNT(*)=2102, COUNT(tr.v)=2102, COUNT(tl.v)=2000。
fn setup_outer_tables(engine: &StorageQueryEngine) {
    engine
        .execute("CREATE TABLE tl (id INT PRIMARY KEY, k INT, v INT)")
        .unwrap();
    engine
        .execute("CREATE TABLE tr (id INT PRIMARY KEY, k INT, v INT)")
        .unwrap();
    insert_fixture_rows(
        engine,
        "test.tl",
        (0..202).map(|i| {
            vec![
                Value::Int(i),
                if i >= 200 {
                    Value::Null
                } else {
                    Value::Int(if i < 100 { i % 20 } else { 1000 + i })
                },
                Value::Int(i * 3),
            ]
        }),
    );
    let rows = (0..500)
        .map(|i| {
            vec![
                Value::Int(i),
                Value::Int(if i < 400 { i % 20 } else { 4600 + i }),
                Value::Int(i * 7),
            ]
        })
        .chain([
            vec![Value::Int(3000), Value::Null, Value::Int(1)],
            vec![Value::Int(3001), Value::Null, Value::Int(2)],
        ]);
    insert_fixture_rows(engine, "test.tr", rows);
}

/// Build-preserved joins with only probe aggregates qualify under S1; fused counters
/// must advance, proving no fallback, and results must match hand calculations and the baseline.
#[test]
fn build_preserved_left_probe_side_aggregates_fused_and_exact() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    setup_outer_tables(&engine);

    let before = fused_count();
    let row = assert_matches_unfused_baseline(
        &engine,
        "SELECT COUNT(tr.v) FROM tl LEFT JOIN tr ON tl.k = tr.k",
    );
    assert_eq!(row, vec![Value::Int(2000)]);
    assert!(
        fused_count() > before,
        "probe-side COUNT(col) over build-preserved LEFT should take the fused path"
    );

    let row = assert_matches_unfused_baseline(
        &engine,
        "SELECT SUM(tr.v), AVG(tr.v), MIN(tr.v), MAX(tr.v) \
         FROM tl LEFT JOIN tr ON tl.k = tr.k",
    );
    // The baseline fixes SUM/AVG/MIN/MAX Value variants; also assert numeric values here.
    // AVG over INT returns DECIMAL with scale=div_precision_increment=4.
    assert_eq!(row[0], Value::DecimalCompact(2_793_000, 0));
    assert_eq!(row[1], Value::DecimalCompact(13_965_000, 4));
    assert_eq!(row[2], Value::Int(0));
    assert_eq!(row[3], Value::Int(2793));
}

/// COUNT(*) and COUNT(build column) observe NULL extensions; S2 match bitmaps and
/// final compensation make the fused path exact, including two NULL-key build rows.
/// Regression: before S1, unmatched build rows were lost and count was 2000; S1 fell back unfused.
#[test]
fn build_preserved_left_count_star_and_build_col_exact() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    setup_outer_tables(&engine);

    let row = assert_matches_unfused_baseline(
        &engine,
        "SELECT COUNT(*) FROM tl LEFT JOIN tr ON tl.k = tr.k",
    );
    assert_eq!(row, vec![Value::Int(2102)]);

    let row = assert_matches_unfused_baseline(
        &engine,
        "SELECT COUNT(tl.v) FROM tl LEFT JOIN tr ON tl.k = tr.k",
    );
    assert_eq!(row, vec![Value::Int(2102)]);

    // Mixed probe aggregates and COUNT(*) must likewise fall back as a whole.
    let row = assert_matches_unfused_baseline(
        &engine,
        "SELECT COUNT(*), COUNT(tr.v) FROM tl LEFT JOIN tr ON tl.k = tr.k",
    );
    assert_eq!(row, vec![Value::Int(2102), Value::Int(2000)]);
}

/// For a preserved probe side, unmatched probe rows (including two NULL-key tr rows)
/// must appear exactly once. Since S2, streaming miss/NULL-key branches handle them
/// within the fused path. Previously COUNT(*) and COUNT(tr.v) omitted
/// all 102 unmatched tr rows.
#[test]
fn probe_preserved_left_exact() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    setup_outer_tables(&engine);

    let row = assert_matches_unfused_baseline(
        &engine,
        "SELECT COUNT(*) FROM tr LEFT JOIN tl ON tr.k = tl.k",
    );
    assert_eq!(row, vec![Value::Int(2102)]);

    let row = assert_matches_unfused_baseline(
        &engine,
        "SELECT COUNT(tr.v) FROM tr LEFT JOIN tl ON tr.k = tl.k",
    );
    assert_eq!(row, vec![Value::Int(2102)]);

    let row = assert_matches_unfused_baseline(
        &engine,
        "SELECT COUNT(tl.v) FROM tr LEFT JOIN tl ON tr.k = tl.k",
    );
    assert_eq!(row, vec![Value::Int(2000)]);
}

/// RIGHT mirror: tr RIGHT JOIN tl is equivalent to tl LEFT JOIN tr.
/// Results must match the mirrored LEFT form regardless of planner normalization.
#[test]
fn right_join_mirror_exact() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    setup_outer_tables(&engine);

    let row = assert_matches_unfused_baseline(
        &engine,
        "SELECT COUNT(*) FROM tr RIGHT JOIN tl ON tr.k = tl.k",
    );
    assert_eq!(row, vec![Value::Int(2102)]);

    let row = assert_matches_unfused_baseline(
        &engine,
        "SELECT COUNT(tr.v) FROM tr RIGHT JOIN tl ON tr.k = tl.k",
    );
    assert_eq!(row, vec![Value::Int(2000)]);

    let row = assert_matches_unfused_baseline(
        &engine,
        "SELECT COUNT(tl.v) FROM tr RIGHT JOIN tl ON tr.k = tl.k",
    );
    assert_eq!(row, vec![Value::Int(2102)]);
}

/// With streaming disabled, outer joins use materialized bucket-map probing, which also
/// handles probe aggregates exactly; streaming, materialized fused, and unfused results agree.
#[test]
fn outer_streaming_vs_materialized_vs_unfused_agree() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    setup_outer_tables(&engine);

    let sql = "SELECT COUNT(tr.v), SUM(tr.v), MIN(tr.v), MAX(tr.v) \
               FROM tl LEFT JOIN tr ON tl.k = tr.k";
    let streaming = one_row(&engine, sql);
    let before = fused_count();
    let materialized = with_test_env(
        &[("MYDB_HASH_JOIN_FUSED_STREAM_PROBE_DISABLE", Some("1"))],
        || one_row(&engine, sql),
    );
    assert!(
        fused_count() > before,
        "stream-probe disabled should still take the fused materialized path"
    );
    let unfused = with_test_env(&[("MYDB_HASH_JOIN_AGG_FUSED_DISABLE", Some("1"))], || {
        one_row(&engine, sql)
    });
    assert_eq!(streaming, materialized);
    assert_eq!(streaming, unfused);
}

/// Q13-style probe scan filter, with NOT LIKE compiled into streaming scan:
/// LEFT and equivalent INNER results must match each other and the unfused baseline.
#[test]
fn q13_shape_filtered_probe_left_matches_inner_and_baseline() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE c (ck INT PRIMARY KEY, pad INT)")
        .unwrap();
    engine
        .execute("CREATE TABLE o (ok INT PRIMARY KEY, ck INT, comment_s VARCHAR(64))")
        .unwrap();
    let mut s = String::from("INSERT INTO c VALUES ");
    for i in 0..50 {
        if i > 0 {
            s.push(',');
        }
        s.push_str(&format!("({i}, {i})"));
    }
    engine.execute(&s).unwrap();
    // ck in [0,40) has five orders each; comments on ok%4==0 contain 'special requests'
    // and are filtered. ck in [40,50) has no orders, contributing COUNT(o.ok)=0 under LEFT.
    // Without GROUP BY, verify only global COUNT equivalence with INNER.
    let mut s = String::from("INSERT INTO o VALUES ");
    for i in 0..200 {
        if i > 0 {
            s.push(',');
        }
        let comment = if i % 4 == 0 {
            "ab special zz requests cd"
        } else {
            "regular order"
        };
        s.push_str(&format!("({i}, {}, '{comment}')", i % 40));
    }
    engine.execute(&s).unwrap();

    let left_sql = "SELECT COUNT(o.ok) FROM c LEFT JOIN o \
                    ON c.ck = o.ck AND o.comment_s NOT LIKE '%special%requests%'";
    let inner_sql = "SELECT COUNT(o.ok) FROM c JOIN o \
                     ON c.ck = o.ck AND o.comment_s NOT LIKE '%special%requests%'";
    let left = assert_matches_unfused_baseline(&engine, left_sql);
    // Filter 50 of 200 orders, leaving 150; every ck matches c.
    assert_eq!(left, vec![Value::Int(150)]);
    assert_eq!(left, one_row(&engine, inner_sql));

    // COUNT(*) under S2: 150 matches plus ten customers with no orders plus
    // ten customers whose five orders are all filtered (ck%4==0, ok congruent to ck mod 4) = 170.
    let star = assert_matches_unfused_baseline(
        &engine,
        "SELECT COUNT(*) FROM c LEFT JOIN o \
         ON c.ck = o.ck AND o.comment_s NOT LIKE '%special%requests%'",
    );
    assert_eq!(star, vec![Value::Int(170)]);
}

/// S2 fuses build-preserved COUNT(*), COUNT(build column), and mixed slots
/// using streaming match bitmaps and final compensation; counters must advance.
#[test]
fn build_preserved_count_star_fused_with_hit_compensation() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    setup_outer_tables(&engine);

    let before = fused_count();
    let row = assert_matches_unfused_baseline(
        &engine,
        "SELECT COUNT(*) FROM tl LEFT JOIN tr ON tl.k = tr.k",
    );
    assert_eq!(row, vec![Value::Int(2102)]);
    assert!(
        fused_count() > before,
        "build-preserved COUNT(*) should now take the fused path"
    );

    // Mixed slots combine bitmap compensation (CountStar/BuildCountCol) with probe aggregates.
    let row = assert_matches_unfused_baseline(
        &engine,
        "SELECT COUNT(*), COUNT(tr.v), COUNT(tl.v), SUM(tr.v) \
         FROM tl LEFT JOIN tr ON tl.k = tr.k",
    );
    assert_eq!(row[0], Value::Int(2102));
    assert_eq!(row[1], Value::Int(2000));
    assert_eq!(row[2], Value::Int(2102));
    assert_eq!(row[3], Value::DecimalCompact(2_793_000, 0));
}

/// Build-side SUM/AVG/MIN/MAX use one mergeable partial state per join-key
/// bucket. Probe hits merge that state directly into the output aggregate;
/// unmatched preserved build rows merge it once during outer compensation.
/// This is the regression for the former COUNT(build_col)-only admission.
#[test]
fn build_side_mergeable_aggregates_fused_for_both_outer_arrangements() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    setup_outer_tables(&engine);

    let before = fused_count();
    let preserved = assert_matches_unfused_baseline(
        &engine,
        "SELECT SUM(tl.v), AVG(tl.v), MIN(tl.v), MAX(tl.v) \
         FROM tl LEFT JOIN tr ON tl.k = tr.k",
    );
    assert_eq!(preserved[0], Value::DecimalCompact(343_053, 0));
    assert_eq!(preserved[2], Value::Int(0));
    assert_eq!(preserved[3], Value::Int(603));
    assert!(
        fused_count() > before,
        "build-side mergeable aggregates must not fall back to joined PlanRows"
    );

    let before = fused_count();
    let non_preserved = assert_matches_unfused_baseline(
        &engine,
        "SELECT SUM(tl.v), AVG(tl.v), MIN(tl.v), MAX(tl.v) \
         FROM tr LEFT JOIN tl ON tr.k = tl.k",
    );
    assert_eq!(non_preserved[0], Value::DecimalCompact(297_000, 0));
    assert_eq!(non_preserved[2], Value::Int(0));
    assert_eq!(non_preserved[3], Value::Int(297));
    assert!(
        fused_count() > before,
        "non-preserved build-side partials must remain in the fused pipeline"
    );
}

#[test]
fn build_side_complex_expressions_share_partial_state_pipeline() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    setup_outer_tables(&engine);
    let sql = "SELECT COUNT(NULLIF(tl.v, 3)), SUM(tl.v * 2 + 1), \
               AVG(CASE WHEN tl.v >= 0 THEN tl.v + 5 ELSE NULL END), \
               MIN(-tl.v), MAX(tl.v + 7) \
               FROM tl LEFT JOIN tr ON tl.k = tr.k";

    let before = fused_count();
    let streaming = assert_matches_unfused_baseline(&engine, sql);
    assert!(
        fused_count() > before,
        "complex build expressions must fuse"
    );

    let materialized = with_test_env(
        &[("MYDB_HASH_JOIN_FUSED_STREAM_PROBE_DISABLE", Some("1"))],
        || one_row(&engine, sql),
    );
    assert_eq!(streaming, materialized);

    let before = fused_count();
    let _ = one_row(
        &engine,
        "SELECT SUM(tl.v + RAND()) FROM tl JOIN tr ON tl.k = tr.k",
    );
    assert!(
        fused_count() > before,
        "volatile expressions must stay in the batch pipeline and use its scalar kernel"
    );
}

#[test]
fn build_side_partials_cover_null_safe_and_composite_keys() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    setup_outer_tables(&engine);

    let before = fused_count();
    let null_safe = assert_matches_unfused_baseline(
        &engine,
        "SELECT SUM(tl.v), AVG(tl.v), MIN(tl.v), MAX(tl.v) \
         FROM tl JOIN tr ON tl.k <=> tr.k",
    );
    assert!(fused_count() > before, "NULL-safe build partials must fuse");
    assert!(!matches!(null_safe[0], Value::Null));

    engine
        .execute("CREATE TABLE cb (id INT PRIMARY KEY, k1 INT, k2 VARCHAR(8), v DECIMAL(12,2))")
        .unwrap();
    engine
        .execute("CREATE TABLE cp (id INT PRIMARY KEY, k1 INT, k2 VARCHAR(8))")
        .unwrap();
    engine
        .execute(
            "INSERT INTO cb VALUES (1,1,'a',10.25),(2,1,'a',20.50),\
             (3,2,'b',NULL),(4,NULL,'z',40.75)",
        )
        .unwrap();
    engine
        .execute(
            "INSERT INTO cp VALUES (10,1,'a'),(11,1,'a'),(12,2,'b'),\
             (13,9,'x'),(14,NULL,'z')",
        )
        .unwrap();
    let before = fused_count();
    let composite = assert_matches_unfused_baseline(
        &engine,
        "SELECT COUNT(cb.v), SUM(cb.v), AVG(cb.v), MIN(cb.v), MAX(cb.v) \
         FROM cb JOIN cp ON cb.k1 = cp.k1 AND cb.k2 = cp.k2",
    );
    assert!(
        fused_count() > before,
        "composite-key build partials must fuse"
    );
    assert_eq!(composite[0], Value::Int(4));
}

#[test]
fn inner_residual_pairs_feed_aggregate_without_joined_rows() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    setup_outer_tables(&engine);
    let sql = "SELECT COUNT(*), COUNT(tl.v), SUM(tl.v + 1), AVG(tl.v), \
               MIN(tl.v), MAX(tl.v), SUM(tr.v) \
               FROM tl JOIN tr ON tl.k = tr.k AND tl.v * 7 <= tr.v";

    let before = fused_count();
    let row = assert_matches_unfused_baseline(&engine, sql);
    assert!(fused_count() > before, "INNER residual pair sink must fuse");
    assert!(!matches!(row[0], Value::Int(0)));

    for outer_sql in [
        "SELECT COUNT(*), COUNT(tl.v), COUNT(tr.v), SUM(tl.v + 1), SUM(tr.v) \
         FROM tl LEFT JOIN tr ON tl.k = tr.k AND tl.v * 7 <= tr.v",
        "SELECT COUNT(*), COUNT(tl.v), COUNT(tr.v), SUM(tl.v + 1), SUM(tr.v) \
         FROM tr LEFT JOIN tl ON tl.k = tr.k AND tl.v * 7 <= tr.v",
    ] {
        let before = fused_count();
        let row = assert_matches_unfused_baseline(&engine, outer_sql);
        assert!(
            fused_count() > before,
            "outer residual pair sink must fuse for {outer_sql}"
        );
        assert!(!row.is_empty());
    }
}

/// S2 fuses preserved-probe joins through streaming miss/NULL-key branches;
/// counters advance and unmatched tr values contribute to SUM/MAX. MAX=3493 comes only
/// from unmatched rows, distinguishing INNER's maximum of 2793.
#[test]
fn probe_preserved_fused_streaming_exact() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    setup_outer_tables(&engine);

    let before = fused_count();
    let row = assert_matches_unfused_baseline(
        &engine,
        "SELECT COUNT(*), COUNT(tr.v), COUNT(tl.v) FROM tr LEFT JOIN tl ON tr.k = tl.k",
    );
    assert_eq!(
        row,
        vec![Value::Int(2102), Value::Int(2102), Value::Int(2000)]
    );
    assert!(
        fused_count() > before,
        "probe-preserved LEFT should now take the fused path"
    );

    let row = assert_matches_unfused_baseline(
        &engine,
        "SELECT SUM(tr.v), MIN(tr.v), MAX(tr.v) FROM tr LEFT JOIN tl ON tr.k = tl.k",
    );
    // Matched 2_793_000 plus unmatched 7*sum(400..499)=314_650 plus NULL-key values 1+2.
    assert_eq!(row[0], Value::DecimalCompact(3_107_653, 0));
    assert_eq!(row[1], Value::Int(0));
    assert_eq!(row[2], Value::Int(3493));
}

/// Materialized bucket maps with STREAM_PROBE_DISABLE handle all outer forms exactly,
/// matching streaming and unfused paths using specialized i64 maps, match compensation,
/// and preserved-probe unmatched branches.
#[test]
fn materialized_bucket_path_outer_exact() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    setup_outer_tables(&engine);

    for sql in [
        "SELECT COUNT(*), COUNT(tr.v), COUNT(tl.v), SUM(tr.v) FROM tl LEFT JOIN tr ON tl.k = tr.k",
        "SELECT COUNT(*), COUNT(tr.v), COUNT(tl.v), SUM(tr.v), MAX(tr.v) \
         FROM tr LEFT JOIN tl ON tr.k = tl.k",
        "SELECT COUNT(*), COUNT(tr.v), COUNT(tl.v) FROM tr RIGHT JOIN tl ON tr.k = tl.k",
    ] {
        let streaming = one_row(&engine, sql);
        let (materialized, unfused) = with_test_env(
            &[("MYDB_HASH_JOIN_FUSED_STREAM_PROBE_DISABLE", Some("1"))],
            || {
                let m = one_row(&engine, sql);
                let u = with_test_env(&[("MYDB_HASH_JOIN_AGG_FUSED_DISABLE", Some("1"))], || {
                    one_row(&engine, sql)
                });
                (m, u)
            },
        );
        assert_eq!(streaming, materialized, "streaming vs bucket-map for {sql}");
        assert_eq!(streaming, unfused, "fused vs unfused for {sql}");
    }
}

/// S3: force bucket-map sliced parallelism with PARALLELISM_THRESHOLD=1;
/// worker accumulator merges and bitmap OR merges must remain exact.
#[test]
fn materialized_bucket_path_parallel_forced_exact() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    setup_outer_tables(&engine);

    let envs: &[(&'static str, Option<&str>)] = &[
        ("MYDB_HASH_JOIN_FUSED_STREAM_PROBE_DISABLE", Some("1")),
        ("MYDB_HASH_JOIN_PARALLELISM_THRESHOLD", Some("1")),
        ("MYDB_HASH_JOIN_PARALLELISM", Some("4")),
    ];
    for (sql, expected) in [
        (
            "SELECT COUNT(*), COUNT(tr.v), COUNT(tl.v) FROM tl LEFT JOIN tr ON tl.k = tr.k",
            vec![Value::Int(2102), Value::Int(2000), Value::Int(2102)],
        ),
        (
            "SELECT COUNT(*), COUNT(tr.v), COUNT(tl.v) FROM tr LEFT JOIN tl ON tr.k = tl.k",
            vec![Value::Int(2102), Value::Int(2102), Value::Int(2000)],
        ),
    ] {
        let row = with_test_env(envs, || one_row(&engine, sql));
        assert_eq!(row, expected, "parallel bucket-map for {sql}");
    }
}

/// S4: string keys use generic HashJoinKey bucket maps and bypass the integer streaming gate,
/// so default settings select materialization; both outer forms must remain exact.
#[test]
fn string_key_generic_bucket_path_exact() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE sl (id INT PRIMARY KEY, k VARCHAR(8), v INT)")
        .unwrap();
    engine
        .execute("CREATE TABLE sr (id INT PRIMARY KEY, k VARCHAR(8), v INT)")
        .unwrap();
    engine
        .execute(
            "INSERT INTO sl VALUES (1,'a',1),(2,'a',2),(3,'b',3),(4,'c',4),\
             (5,NULL,5),(6,NULL,6)",
        )
        .unwrap();
    engine
        .execute(
            "INSERT INTO sr VALUES (10,'a',10),(11,'a',20),(12,'a',30),(13,'b',40),\
             (14,'x',50),(15,'y',60),(16,NULL,70)",
        )
        .unwrap();

    // Matched pairs: 'a' contributes 2*3=6 and 'b' contributes 1, totaling 7.
    // sl LEFT sr has unmatched 'c' and two NULL rows: COUNT(*)=10.
    let row = assert_matches_unfused_baseline(
        &engine,
        "SELECT COUNT(*), COUNT(sr.v), COUNT(sl.v) FROM sl LEFT JOIN sr ON sl.k = sr.k",
    );
    assert_eq!(row, vec![Value::Int(10), Value::Int(7), Value::Int(10)]);
    // sr LEFT sl has unmatched 'x', 'y', and NULL: COUNT(*)=10.
    let row = assert_matches_unfused_baseline(
        &engine,
        "SELECT COUNT(*), COUNT(sr.v), COUNT(sl.v) FROM sr LEFT JOIN sl ON sr.k = sl.k",
    );
    assert_eq!(row, vec![Value::Int(10), Value::Int(10), Value::Int(7)]);
}

/// S4 delayed Bloom filter: high miss rates beyond the 64-row test sample trigger retrospective construction;
/// results remain exact. All-hit FK shapes skip the filter to avoid pure overhead.
/// Exact counter assertions are isolated only under nextest's per-test processes.
#[test]
fn adaptive_bloom_sampled_decision() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE bb (id INT PRIMARY KEY, k INT, v INT)")
        .unwrap();
    engine
        .execute("CREATE TABLE bp (id INT PRIMARY KEY, k INT, v INT)")
        .unwrap();
    let mut s = String::from("INSERT INTO bb VALUES ");
    for i in 0..50 {
        if i > 0 {
            s.push(',');
        }
        s.push_str(&format!("({i}, {i}, {i})"));
    }
    engine.execute(&s).unwrap();
    // bp: first 70 rows miss (>64 sample window), followed by 30 hits.
    let mut s = String::from("INSERT INTO bp VALUES ");
    for i in 0..100 {
        if i > 0 {
            s.push(',');
        }
        let k = if i < 70 { 100_000 + i } else { i % 50 };
        s.push_str(&format!("({i}, {k}, {})", i * 3));
    }
    engine.execute(&s).unwrap();

    let envs: &[(&'static str, Option<&str>)] = &[
        ("MYDB_HASH_JOIN_FUSED_STREAM_PROBE_DISABLE", Some("1")),
        ("MYDB_HASH_JOIN_BLOOM_MIN_BUILD_ROWS", Some("1")),
    ];
    let high_miss_sql = "SELECT COUNT(bb.v) FROM bb JOIN bp ON bb.k = bp.k";
    let before = native_batch_hits();
    let row = with_test_env(envs, || one_row(&engine, high_miss_sql));
    assert_eq!(row, vec![Value::Int(30)]);
    assert!(
        native_batch_hits() > before,
        "high-miss probe must stay in the common batch join"
    );
    let baseline = with_test_env(&[("MYDB_HASH_JOIN_AGG_FUSED_DISABLE", Some("1"))], || {
        one_row(&engine, high_miss_sql)
    });
    assert_eq!(row, baseline);

    // FK shape: every probe hits, so skip Bloom construction after sampling.
    engine.execute("DELETE FROM bp").unwrap();
    let mut s = String::from("INSERT INTO bp VALUES ");
    for i in 0..100 {
        if i > 0 {
            s.push(',');
        }
        s.push_str(&format!("({i}, {}, {})", i % 50, i * 3));
    }
    engine.execute(&s).unwrap();
    let before = native_batch_hits();
    let row = with_test_env(envs, || one_row(&engine, high_miss_sql));
    assert_eq!(row, vec![Value::Int(100)]);
    assert!(
        native_batch_hits() > before,
        "all-hit probe must stay in the common batch join"
    );
}
