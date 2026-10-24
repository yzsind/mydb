//! INLJ grouped aggregate direct path regression.
//!
//! The fast path avoids materialising one joined `PlanRow` per index hit
//! when GROUP BY keys and aggregate arguments are plain joined columns.

use super::*;

fn setup(engine: &StorageQueryEngine) {
    engine
        .execute("CREATE TABLE dim_n7 (d INT PRIMARY KEY, bucket INT, tag INT)")
        .unwrap();
    engine
        .execute("INSERT INTO dim_n7 VALUES (1, 10, 100), (2, 10, 200), (3, 20, 300)")
        .unwrap();
    engine
        .execute(
            "CREATE TABLE fact_n7 (id INT PRIMARY KEY, d INT, item INT, price INT, INDEX idx_d(d))",
        )
        .unwrap();
    engine
        .execute(
            "INSERT INTO fact_n7 VALUES \
             (1,1,7,5), (2,1,7,6), (3,1,8,10), \
             (4,2,7,20), (5,2,9,30), (6,3,7,40), (7,99,7,50)",
        )
        .unwrap();
    engine.execute("ANALYZE TABLE dim_n7").unwrap();
    engine.execute("ANALYZE TABLE fact_n7").unwrap();
}

fn sorted_rows(engine: &StorageQueryEngine, sql: &str) -> Vec<Vec<Value>> {
    let QueryResponse::ResultSet(result) = execute_permissive(engine, sql).unwrap() else {
        panic!("expected result set for {sql}");
    };
    let mut rows = result.rows;
    rows.sort_by_cached_key(|row| format!("{row:?}"));
    rows
}

fn native_result_batch_hits() -> u64 {
    crate::engine::executor::select::vec_bridge::NATIVE_RESULT_BATCH_HITS
        .load(std::sync::atomic::Ordering::Relaxed)
}

fn explain_text(engine: &StorageQueryEngine, sql: &str) -> String {
    let QueryResponse::ResultSet(result) = execute_permissive(engine, sql).unwrap() else {
        panic!("expected EXPLAIN result set for {sql}");
    };
    result
        .rows
        .iter()
        .flat_map(|row| row.iter())
        .map(|value| match value {
            Value::String(text) => text.clone(),
            other => format!("{other:?}"),
        })
        .collect::<Vec<_>>()
        .join("\n")
}

#[test]
fn grouped_inlj_direct_agg_matches_materialized_fused_path() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    setup(&engine);

    let sql = "SELECT /*+ INDEX_NLJ(dim_n7, fact_n7) NO_HASH_JOIN(dim_n7, fact_n7) */ \
               dim_n7.bucket, fact_n7.item, COUNT(*), SUM(fact_n7.price), SUM(dim_n7.tag) \
               FROM dim_n7 JOIN fact_n7 ON dim_n7.d = fact_n7.d \
               GROUP BY dim_n7.bucket, fact_n7.item \
               ORDER BY dim_n7.bucket, fact_n7.item";

    let fallback = with_test_env(&[("MYDB_INLJ_GROUP_AGG_DIRECT_DISABLE", Some("1"))], || {
        sorted_rows(&engine, sql)
    });
    let before = native_result_batch_hits();
    let direct = sorted_rows(&engine, sql);
    let after = native_result_batch_hits();

    assert_eq!(
        direct, fallback,
        "direct grouped INLJ aggregate diverged from materialized fused path"
    );
    assert!(
        after > before,
        "grouped INLJ aggregate did not reach the unified batch pipeline"
    );
    assert_eq!(direct.len(), 4);
}

#[test]
fn parallel_grouped_inlj_uses_fusion() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    setup(&engine);
    let sql = "SELECT /*+ PARALLEL(4) INDEX_NLJ(dim_n7, fact_n7) \
               NO_HASH_JOIN(dim_n7, fact_n7) */ \
               dim_n7.bucket, COUNT(*), SUM(fact_n7.price * (1 - 0)) \
               FROM dim_n7 JOIN fact_n7 \
               ON dim_n7.d = fact_n7.d AND fact_n7.item = 7 \
               GROUP BY dim_n7.bucket ORDER BY dim_n7.bucket";

    let oracle = with_test_env(
        &[
            ("MYDB_INLJ_GROUP_AGG_FUSED_DISABLE", Some("1")),
            ("MYDB_INLJ_PREDICATE_FIRST_DISABLE", Some("1")),
        ],
        || sorted_rows(&engine, sql),
    );
    let grouped_before = native_result_batch_hits();
    let fast = with_test_env(
        &[
            ("MYDB_INLJ_GROUP_AGG_KEYSET_SCAN", Some("off")),
            ("MYDB_LATE_DIM_AGG_DISABLE", Some("1")),
        ],
        || sorted_rows(&engine, sql),
    );
    let grouped_after = native_result_batch_hits();

    assert_eq!(fast, oracle);
    assert_eq!(fast.len(), 2);
    let tree = explain_text(&engine, &format!("EXPLAIN FORMAT=TREE {sql}"));
    assert!(
        grouped_after > grouped_before,
        "parallel grouped INLJ did not reach the unified batch pipeline: {tree}"
    );
}

/// TPC-H Q9 groups by `nation, YEAR(o_orderdate)`. Routing this computed key
/// into late-dimension fusion creates an almost one-state-per-row partial
/// aggregate before the unique orders lookup. Keep the normal parallel
/// aggregate path, while simple group columns still use direct fusion.
#[test]
fn parallel_grouped_inlj_computed_group_key_keeps_parallel_aggregate() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    setup(&engine);
    let select = "SELECT /*+ PARALLEL(4) INDEX_NLJ(dim_n7, fact_n7) \
                  NO_HASH_JOIN(dim_n7, fact_n7) */ \
                  dim_n7.bucket, fact_n7.item + 0 AS computed_item, \
                  SUM(fact_n7.price * (1 - 0)) \
                  FROM dim_n7 JOIN fact_n7 ON dim_n7.d = fact_n7.d \
                  GROUP BY dim_n7.bucket, fact_n7.item + 0 \
                  ORDER BY dim_n7.bucket, computed_item";
    let oracle = with_test_env(&[("MYDB_INLJ_GROUP_AGG_FUSED_DISABLE", Some("1"))], || {
        sorted_rows(&engine, select)
    });
    let actual = sorted_rows(&engine, select);
    assert_eq!(actual, oracle);

    let tree = explain_text(&engine, &format!("EXPLAIN ANALYZE FORMAT=TREE {select}"));
    assert!(
        tree.contains("Parallel aggregate"),
        "PARALLEL hint did not produce the regression target plan: {tree}"
    );
    assert!(
        !tree.contains("never executed"),
        "computed group key incorrectly entered late-dimension fusion: {tree}"
    );
}

#[test]
fn grouped_inlj_keyset_scan_matches_batched_inlj() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    setup(&engine);

    let sql = "SELECT /*+ INDEX_NLJ(dim_n7, fact_n7) NO_HASH_JOIN(dim_n7, fact_n7) */ \
               dim_n7.bucket, fact_n7.item, COUNT(*), SUM(fact_n7.price), SUM(dim_n7.tag) \
               FROM dim_n7 JOIN fact_n7 ON dim_n7.d = fact_n7.d \
               WHERE fact_n7.item IS NOT NULL \
               GROUP BY dim_n7.bucket, fact_n7.item \
               ORDER BY dim_n7.bucket, fact_n7.item";

    let batched = with_test_env(&[("MYDB_INLJ_GROUP_AGG_KEYSET_SCAN", Some("off"))], || {
        sorted_rows(&engine, sql)
    });
    // Arena off so this exercises the FusedGroups keyset path (its intent); with
    // the arena default-on the same shape would take the int-key arena instead.
    let keyset = with_test_env(
        &[
            ("MYDB_INLJ_GROUP_AGG_KEYSET_SCAN", Some("force")),
            ("MYDB_FUSED_INT_ARENA", Some("off")),
        ],
        || sorted_rows(&engine, sql),
    );

    assert_eq!(
        keyset, batched,
        "keyset scan grouped INLJ aggregate diverged"
    );
}

/// P0-1: `needs_sample_fields == false`. The projection references only group
/// keys and aggregates (no bare columns), so the fused INLJ grouped-aggregate
/// path emits empty `sample_fields`. It must stay byte-identical to the generic
/// row-based aggregate oracle (`MYDB_INLJ_GROUP_AGG_FUSED_DISABLE=1` routes the
/// same query through `execute_plan_rows` + `aggregate_plan_rows_maybe_spilled`).
/// Covers both the default (direct/general) builder and the keyset-scan worker
/// builder (gate site #1).
#[test]
fn grouped_inlj_empty_sample_fields_matches_generic_agg() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    setup(&engine);

    let sql = "SELECT /*+ INDEX_NLJ(dim_n7, fact_n7) NO_HASH_JOIN(dim_n7, fact_n7) */ \
               dim_n7.bucket, fact_n7.item, COUNT(*), SUM(fact_n7.price) \
               FROM dim_n7 JOIN fact_n7 ON dim_n7.d = fact_n7.d \
               GROUP BY dim_n7.bucket, fact_n7.item \
               ORDER BY dim_n7.bucket, fact_n7.item";

    let generic = with_test_env(&[("MYDB_INLJ_GROUP_AGG_FUSED_DISABLE", Some("1"))], || {
        sorted_rows(&engine, sql)
    });

    let before = native_result_batch_hits();
    let fused = sorted_rows(&engine, sql);
    let after = native_result_batch_hits();
    assert_eq!(
        fused, generic,
        "fused empty-sample_fields path diverged from generic aggregate"
    );
    assert!(
        after > before,
        "grouped INLJ aggregate did not reach the unified batch pipeline"
    );

    let keyset = with_test_env(
        &[("MYDB_INLJ_GROUP_AGG_KEYSET_SCAN", Some("force"))],
        || sorted_rows(&engine, sql),
    );
    assert_eq!(
        keyset, generic,
        "keyset-scan empty-sample_fields path diverged from generic aggregate"
    );
}

/// P0-1: `needs_sample_fields == true`. A bare (non-group, non-aggregate)
/// column is projected under mydb's default empty `sql_mode`, so the planner
/// appends a `__hidden_first_value` slot and the gate must KEEP `sample_fields`.
/// The bare column is constant within each group (functionally determined by
/// the group key), so MySQL "any value" semantics are deterministic and the
/// fused output must match the generic aggregate oracle byte-for-byte. Mirrors
/// q97's shape: GROUP BY on the inner/right (indexed fact) column.
#[test]
fn grouped_inlj_bare_column_keeps_sample_fields_matches_generic_agg() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE dimr (d INT PRIMARY KEY)")
        .unwrap();
    engine
        .execute("INSERT INTO dimr VALUES (1), (2), (3)")
        .unwrap();
    engine
        .execute(
            "CREATE TABLE factr (id INT PRIMARY KEY, d INT, grp INT, label INT, INDEX idx_d(d))",
        )
        .unwrap();
    // label is constant within each grp (grp 100 → 7, grp 200 → 9).
    engine
        .execute(
            "INSERT INTO factr VALUES \
             (1,1,100,7), (2,1,100,7), (3,2,100,7), (4,2,200,9), (5,3,200,9)",
        )
        .unwrap();
    engine.execute("ANALYZE TABLE dimr").unwrap();
    engine.execute("ANALYZE TABLE factr").unwrap();

    // factr.label is bare (not in GROUP BY); forces needs_sample_fields=true.
    let sql = "SELECT /*+ INDEX_NLJ(dimr, factr) NO_HASH_JOIN(dimr, factr) */ \
               factr.grp, COUNT(*), factr.label \
               FROM dimr JOIN factr ON dimr.d = factr.d \
               GROUP BY factr.grp \
               ORDER BY factr.grp";

    let generic = with_test_env(&[("MYDB_INLJ_GROUP_AGG_FUSED_DISABLE", Some("1"))], || {
        sorted_rows(&engine, sql)
    });
    let fused = sorted_rows(&engine, sql);
    assert_eq!(
        fused, generic,
        "fused bare-column path (needs_sample_fields=true) diverged from generic aggregate"
    );
    assert_eq!(fused.len(), 2, "expected one row per grp");
}

/// O3: the fused INLJ finalize hoists the group-key `ColMeta` interning, the
/// field-lookup `Arc`, and (for a pure GROUP BY) the empty agg-cache `Arc` out
/// of the per-group loop. `MYDB_FUSED_AGG_FINALIZE_FAST=off` restores the
/// per-row intern path as the differential oracle; on/off must be byte-identical
/// and the fused grouped path must actually fire (so the assertion is not vacuous).
#[test]
fn grouped_inlj_finalize_fast_matches_oracle_with_aggregates() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    setup(&engine);

    let sql = "SELECT /*+ INDEX_NLJ(dim_n7, fact_n7) NO_HASH_JOIN(dim_n7, fact_n7) */ \
               dim_n7.bucket, fact_n7.item, COUNT(*), SUM(fact_n7.price) \
               FROM dim_n7 JOIN fact_n7 ON dim_n7.d = fact_n7.d \
               GROUP BY dim_n7.bucket, fact_n7.item \
               ORDER BY dim_n7.bucket, fact_n7.item";

    let oracle = with_test_env(&[("MYDB_FUSED_AGG_FINALIZE_FAST", Some("off"))], || {
        sorted_rows(&engine, sql)
    });
    let before = native_result_batch_hits();
    let fast = with_test_env(&[("MYDB_FUSED_AGG_FINALIZE_FAST", Some("on"))], || {
        sorted_rows(&engine, sql)
    });
    let after = native_result_batch_hits();

    assert_eq!(
        fast, oracle,
        "fused INLJ finalize fast path diverged from per-row intern oracle (with aggregates)"
    );
    assert!(
        after > before,
        "fused grouped INLJ aggregate path did not fire (test would be vacuous)"
    );
}

fn arena_count() -> u64 {
    native_result_batch_hits()
}

/// Integer-key arena fast path: the keyset-scan sub-path, when the GROUP BY
/// keys are signed integers and every aggregate is SUM/COUNT/AVG, drives a
/// `super::parallel::IntKeyArena` instead of the `FusedGroups` HashMap.
/// `MYDB_FUSED_INT_ARENA=off` restores the FusedGroups path as the differential
/// oracle; on/off must be byte-identical and the arena counter must advance.
/// Covers mixed left (`dim_n7.bucket`, `SUM(dim_n7.tag)`) and right
/// (`fact_n7.item`, `SUM(fact_n7.price)`) key/arg sources plus COUNT/AVG.
#[test]
fn grouped_inlj_keyset_arena_matches_fused_oracle() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    setup(&engine);

    let sql = "SELECT /*+ INDEX_NLJ(dim_n7, fact_n7) NO_HASH_JOIN(dim_n7, fact_n7) */ \
               dim_n7.bucket, fact_n7.item, COUNT(*), SUM(fact_n7.price), \
               SUM(dim_n7.tag), AVG(fact_n7.price), COUNT(fact_n7.price) \
               FROM dim_n7 JOIN fact_n7 ON dim_n7.d = fact_n7.d \
               WHERE fact_n7.item IS NOT NULL \
               GROUP BY dim_n7.bucket, fact_n7.item \
               ORDER BY dim_n7.bucket, fact_n7.item";

    let oracle = with_test_env(
        &[
            ("MYDB_LATE_DIM_AGG_DISABLE", Some("1")),
            ("MYDB_INLJ_GROUP_AGG_KEYSET_SCAN", Some("force")),
            ("MYDB_FUSED_INT_ARENA", Some("off")),
        ],
        || sorted_rows(&engine, sql),
    );
    let before = arena_count();
    let arena = with_test_env(
        &[
            ("MYDB_LATE_DIM_AGG_DISABLE", Some("1")),
            ("MYDB_INLJ_GROUP_AGG_KEYSET_SCAN", Some("force")),
            ("MYDB_FUSED_INT_ARENA", Some("on")),
        ],
        || sorted_rows(&engine, sql),
    );
    let after = arena_count();

    assert_eq!(
        arena, oracle,
        "int-key arena diverged from FusedGroups keyset-scan oracle"
    );
    assert!(
        after > before,
        "int-key arena path did not fire (test would be vacuous)"
    );
}

/// MIN/MAX now folds into the arena's `InlineAggState::MinMax` — the same
/// single-source accumulator the FusedGroups path uses — so on/off must be
/// byte-identical and the arena counter must advance. Mixed with COUNT/SUM and a
/// left-side MIN(`dim_n7.tag`) to exercise both key/arg sources.
#[test]
fn grouped_inlj_keyset_arena_minmax_matches_oracle() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    setup(&engine);

    let sql = "SELECT /*+ INDEX_NLJ(dim_n7, fact_n7) NO_HASH_JOIN(dim_n7, fact_n7) */ \
               dim_n7.bucket, fact_n7.item, MAX(fact_n7.price), MIN(fact_n7.price), \
               MIN(dim_n7.tag), COUNT(*), SUM(fact_n7.price) \
               FROM dim_n7 JOIN fact_n7 ON dim_n7.d = fact_n7.d \
               GROUP BY dim_n7.bucket, fact_n7.item \
               ORDER BY dim_n7.bucket, fact_n7.item";

    let oracle = with_test_env(
        &[
            ("MYDB_LATE_DIM_AGG_DISABLE", Some("1")),
            ("MYDB_INLJ_GROUP_AGG_KEYSET_SCAN", Some("force")),
            ("MYDB_FUSED_INT_ARENA", Some("off")),
        ],
        || sorted_rows(&engine, sql),
    );
    let before = arena_count();
    let arena = with_test_env(
        &[
            ("MYDB_LATE_DIM_AGG_DISABLE", Some("1")),
            ("MYDB_INLJ_GROUP_AGG_KEYSET_SCAN", Some("force")),
            ("MYDB_FUSED_INT_ARENA", Some("on")),
        ],
        || sorted_rows(&engine, sql),
    );
    let after = arena_count();

    assert_eq!(
        arena, oracle,
        "int-key arena MIN/MAX diverged from FusedGroups keyset-scan oracle"
    );
    assert!(
        after > before,
        "int-key arena path did not fire for MIN/MAX"
    );
}

/// R1 for aggregate *arguments*: MIN/MAX stores the argument `Value` verbatim
/// (no numericisation like SUM/AVG), so a non-integer argument (VARCHAR, with a
/// NULL row) must emit the exact same `Value` shape as the FusedGroups oracle.
/// Integer GROUP BY keys keep the arena eligible; the MIN/MAX arg is a string
/// column scanned from the right (inner) side.
#[test]
fn grouped_inlj_keyset_arena_minmax_string_arg_matches_oracle() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE dims (d INT PRIMARY KEY, bucket INT)")
        .unwrap();
    engine
        .execute("INSERT INTO dims VALUES (1, 10), (2, 10), (3, 20)")
        .unwrap();
    engine
        .execute(
            "CREATE TABLE facts (id INT PRIMARY KEY, d INT, grp INT, name VARCHAR(32), INDEX idx_d(d))",
        )
        .unwrap();
    // Group (bucket=10, grp=5) gets a NULL name row → MIN/MAX must skip it.
    engine
        .execute(
            "INSERT INTO facts VALUES \
             (1,1,5,'pear'), (2,1,5,'apple'), (3,1,5,NULL), (4,2,6,'fig'), (5,3,7,'kiwi')",
        )
        .unwrap();
    engine.execute("ANALYZE TABLE dims").unwrap();
    engine.execute("ANALYZE TABLE facts").unwrap();

    let sql = "SELECT /*+ INDEX_NLJ(dims, facts) NO_HASH_JOIN(dims, facts) */ \
               dims.bucket, facts.grp, MIN(facts.name), MAX(facts.name), COUNT(facts.name) \
               FROM dims JOIN facts ON dims.d = facts.d \
               GROUP BY dims.bucket, facts.grp \
               ORDER BY dims.bucket, facts.grp";

    let oracle = with_test_env(
        &[
            ("MYDB_LATE_DIM_AGG_DISABLE", Some("1")),
            ("MYDB_INLJ_GROUP_AGG_KEYSET_SCAN", Some("force")),
            ("MYDB_FUSED_INT_ARENA", Some("off")),
        ],
        || sorted_rows(&engine, sql),
    );
    let before = arena_count();
    let arena = with_test_env(
        &[
            ("MYDB_LATE_DIM_AGG_DISABLE", Some("1")),
            ("MYDB_INLJ_GROUP_AGG_KEYSET_SCAN", Some("force")),
            ("MYDB_FUSED_INT_ARENA", Some("on")),
        ],
        || sorted_rows(&engine, sql),
    );
    let after = arena_count();

    assert_eq!(
        arena, oracle,
        "int-key arena MIN/MAX over VARCHAR arg diverged from oracle"
    );
    assert!(
        after > before,
        "int-key arena path did not fire for string MIN/MAX"
    );
}

/// Phase 1c: integer-backed temporal GROUP BY keys (DATE/YEAR) pack into the
/// arena's i64 lane, and finalize rebuilds the exact `Value::Date`/`Value::Year`
/// (never widened to `Value::Int`). on/off must be byte-identical and the arena
/// must fire. DATE is a scanned (right) key typed from schema; the build-side
/// (left) integer key keeps the arena's mixed key/temporal handling honest.
#[test]
fn grouped_inlj_keyset_arena_temporal_key_matches_oracle() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE dimt (d INT PRIMARY KEY, bucket INT)")
        .unwrap();
    engine
        .execute("INSERT INTO dimt VALUES (1, 10), (2, 10), (3, 20)")
        .unwrap();
    engine
        .execute(
            "CREATE TABLE factt (id INT PRIMARY KEY, d INT, ddate DATE, yr YEAR, price INT, INDEX idx_d(d))",
        )
        .unwrap();
    engine
        .execute(
            "INSERT INTO factt VALUES \
             (1,1,'2021-03-01',2021,5), (2,1,'2021-03-01',2021,6), \
             (3,2,'2022-06-15',2022,20), (4,3,'2021-03-01',2021,40)",
        )
        .unwrap();
    engine.execute("ANALYZE TABLE dimt").unwrap();
    engine.execute("ANALYZE TABLE factt").unwrap();

    let sql = "SELECT /*+ INDEX_NLJ(dimt, factt) NO_HASH_JOIN(dimt, factt) */ \
               dimt.bucket, factt.ddate, factt.yr, COUNT(*), SUM(factt.price) \
               FROM dimt JOIN factt ON dimt.d = factt.d \
               GROUP BY dimt.bucket, factt.ddate, factt.yr \
               ORDER BY dimt.bucket, factt.ddate, factt.yr";

    let oracle = with_test_env(
        &[
            ("MYDB_LATE_DIM_AGG_DISABLE", Some("1")),
            ("MYDB_INLJ_GROUP_AGG_KEYSET_SCAN", Some("force")),
            ("MYDB_FUSED_INT_ARENA", Some("off")),
        ],
        || sorted_rows(&engine, sql),
    );
    let before = arena_count();
    let arena = with_test_env(
        &[
            ("MYDB_LATE_DIM_AGG_DISABLE", Some("1")),
            ("MYDB_INLJ_GROUP_AGG_KEYSET_SCAN", Some("force")),
            ("MYDB_FUSED_INT_ARENA", Some("on")),
        ],
        || sorted_rows(&engine, sql),
    );
    let after = arena_count();

    assert_eq!(
        arena, oracle,
        "int-key arena temporal (DATE/YEAR) key diverged from oracle"
    );
    assert!(
        after > before,
        "int-key arena did not fire for temporal keys"
    );
}

/// NULL group keys and NULL aggregate arguments must group/accumulate
/// identically to the FusedGroups oracle (arena nullmask + `accumulate_numeric_state`
/// NULL handling).
#[test]
fn grouped_inlj_keyset_arena_nulls_match_oracle() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE dimn (d INT PRIMARY KEY, bucket INT)")
        .unwrap();
    engine
        .execute("INSERT INTO dimn VALUES (1, 10), (2, 10), (3, 20)")
        .unwrap();
    engine
        .execute(
            "CREATE TABLE factn (id INT PRIMARY KEY, d INT, item INT, price INT, INDEX idx_d(d))",
        )
        .unwrap();
    // NULL group key (item) and NULL aggregate arg (price) rows.
    engine
        .execute(
            "INSERT INTO factn VALUES \
             (1,1,7,5), (2,1,NULL,6), (3,1,7,NULL), (4,2,NULL,NULL), (5,3,8,40)",
        )
        .unwrap();
    engine.execute("ANALYZE TABLE dimn").unwrap();
    engine.execute("ANALYZE TABLE factn").unwrap();

    let sql = "SELECT /*+ INDEX_NLJ(dimn, factn) NO_HASH_JOIN(dimn, factn) */ \
               dimn.bucket, factn.item, COUNT(*), SUM(factn.price), AVG(factn.price), COUNT(factn.price) \
               FROM dimn JOIN factn ON dimn.d = factn.d \
               GROUP BY dimn.bucket, factn.item \
               ORDER BY dimn.bucket, factn.item";

    // Disable the late-dimension fused path so the keyset-scan sub-path (where
    // the arena lives) is reached; force keyset-scan over the small test data.
    let oracle = with_test_env(
        &[
            ("MYDB_LATE_DIM_AGG_DISABLE", Some("1")),
            ("MYDB_INLJ_GROUP_AGG_KEYSET_SCAN", Some("force")),
            ("MYDB_FUSED_INT_ARENA", Some("off")),
        ],
        || sorted_rows(&engine, sql),
    );
    let before = arena_count();
    let arena = with_test_env(
        &[
            ("MYDB_LATE_DIM_AGG_DISABLE", Some("1")),
            ("MYDB_INLJ_GROUP_AGG_KEYSET_SCAN", Some("force")),
            ("MYDB_FUSED_INT_ARENA", Some("on")),
        ],
        || sorted_rows(&engine, sql),
    );
    let after = arena_count();

    assert_eq!(
        arena, oracle,
        "int-key arena NULL handling diverged from oracle"
    );
    assert!(
        after > before,
        "int-key arena path did not fire on NULL data"
    );
}

/// R1: an UNSIGNED group-key column decodes to `Value::UInt`, not `Value::Int`,
/// so the arena (which emits `Value::Int`) must NOT fire — it would diverge from
/// the FusedGroups path's `Value::UInt`. Verify fallback (counter unchanged) and
/// that the result keeps the `Value::UInt` key shape.
#[test]
fn grouped_inlj_keyset_arena_unsigned_key_falls_back() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    // `INDEX_NLJ(dimu, factu)` makes dimu the inner (scanned) side of the
    // keyset-scan; `ubucket` is an UNSIGNED scanned column → decodes to
    // Value::UInt, so the arena's signed-int metadata check must reject it (the
    // arena would emit Value::Int). `bucket` (signed) keeps it a 2-key shape so
    // keyset-scan fires and the arena eligibility is the decision point.
    engine
        .execute("CREATE TABLE dimu (d INT PRIMARY KEY, bucket INT, ubucket INT UNSIGNED)")
        .unwrap();
    engine
        .execute("INSERT INTO dimu VALUES (1, 10, 100), (2, 10, 100), (3, 20, 200)")
        .unwrap();
    engine
        .execute("CREATE TABLE factu (id INT PRIMARY KEY, d INT, price INT, INDEX idx_d(d))")
        .unwrap();
    engine
        .execute("INSERT INTO factu VALUES (1,1,5), (2,1,6), (3,2,20), (4,3,40)")
        .unwrap();
    engine.execute("ANALYZE TABLE dimu").unwrap();
    engine.execute("ANALYZE TABLE factu").unwrap();

    let sql = "SELECT /*+ INDEX_NLJ(dimu, factu) NO_HASH_JOIN(dimu, factu) */ \
               dimu.bucket, dimu.ubucket, COUNT(*), SUM(factu.price) \
               FROM dimu JOIN factu ON dimu.d = factu.d \
               GROUP BY dimu.bucket, dimu.ubucket \
               ORDER BY dimu.bucket, dimu.ubucket";

    let oracle = with_test_env(&[("MYDB_FUSED_INT_ARENA", Some("off"))], || {
        sorted_rows(&engine, sql)
    });
    let batch_before = native_result_batch_hits();
    let forced = with_test_env(
        &[
            ("MYDB_LATE_DIM_AGG_DISABLE", Some("1")),
            ("MYDB_INLJ_GROUP_AGG_KEYSET_SCAN", Some("force")),
            ("MYDB_FUSED_INT_ARENA", Some("on")),
        ],
        || sorted_rows(&engine, sql),
    );

    assert_eq!(forced, oracle, "unsigned-key keyset-scan result diverged");
    assert!(
        native_result_batch_hits() > batch_before,
        "unsigned-key grouped INLJ did not reach the unified batch pipeline"
    );
    assert!(forced
        .iter()
        .all(|row| matches!(row.get(1), Some(Value::UInt(_)))));
}

/// O3 pure-GROUP-BY (no aggregates) shape — TPC-DS q78's `ss` CTE body run
/// directly: `… GROUP BY ss_customer_sk, ss_item_sk` with no aggregate slots,
/// which exercises the shared empty-cache `Arc` branch. on/off must be
/// byte-identical and the fused grouped path must fire.
#[test]
fn grouped_inlj_finalize_fast_matches_oracle_pure_group_by() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    setup(&engine);

    let sql = "SELECT /*+ INDEX_NLJ(dim_n7, fact_n7) NO_HASH_JOIN(dim_n7, fact_n7) */ \
               dim_n7.bucket, fact_n7.item \
               FROM dim_n7 JOIN fact_n7 ON dim_n7.d = fact_n7.d \
               GROUP BY dim_n7.bucket, fact_n7.item \
               ORDER BY dim_n7.bucket, fact_n7.item";

    let oracle = with_test_env(&[("MYDB_FUSED_AGG_FINALIZE_FAST", Some("off"))], || {
        sorted_rows(&engine, sql)
    });
    let before = native_result_batch_hits();
    let fast = with_test_env(&[("MYDB_FUSED_AGG_FINALIZE_FAST", Some("on"))], || {
        sorted_rows(&engine, sql)
    });
    let after = native_result_batch_hits();

    assert_eq!(
        fast, oracle,
        "fused INLJ finalize fast path diverged from per-row intern oracle (pure GROUP BY)"
    );
    assert!(
        after > before,
        "fused grouped INLJ aggregate path did not fire (test would be vacuous)"
    );
}

/// Phase 2a: the fused SeqScan hash-aggregate takes the integer-key arena fast
/// path for integer + integer-backed-temporal GROUP BY keys with
/// SUM/COUNT/AVG/MIN/MAX. `MYDB_SEQSCAN_INT_ARENA=off` restores the per-group
/// HashMap path as the oracle; on/off must be byte-identical and the arena
/// counter must advance. `MYDB_PARALLEL_DISABLE=1` keeps the serial SeqScan
/// fused path (not the parallel Path B arena) under test.
#[test]
fn seqscan_hash_agg_int_arena_matches_oracle() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE st (id INT PRIMARY KEY, k INT, dt DATE, v INT)")
        .unwrap();
    let mut vals = Vec::new();
    for i in 0..300 {
        let k = i % 20;
        // A couple of distinct dates + a NULL agg arg every 50 rows.
        let dt = if i % 2 == 0 {
            "2021-01-01"
        } else {
            "2022-06-15"
        };
        let v = if i % 50 == 0 {
            "NULL".to_string()
        } else {
            (i * 2).to_string()
        };
        vals.push(format!("({i}, {k}, '{dt}', {v})"));
    }
    engine
        .execute(&format!("INSERT INTO st VALUES {}", vals.join(",")))
        .unwrap();
    engine.execute("ANALYZE TABLE st").unwrap();

    let sql = "SELECT k, dt, COUNT(*), SUM(v), MIN(v), MAX(v), AVG(v), COUNT(v) \
               FROM st GROUP BY k, dt ORDER BY k, dt";

    let oracle = with_test_env(
        &[
            ("MYDB_SEQSCAN_INT_ARENA", Some("off")),
            ("MYDB_PARALLEL_DISABLE", Some("1")),
        ],
        || sorted_rows(&engine, sql),
    );
    let before = native_result_batch_hits();
    let arena = with_test_env(
        &[
            ("MYDB_SEQSCAN_INT_ARENA", Some("on")),
            ("MYDB_PARALLEL_DISABLE", Some("1")),
        ],
        || sorted_rows(&engine, sql),
    );
    let after = native_result_batch_hits();

    assert_eq!(
        arena, oracle,
        "SeqScan int-key arena diverged from the per-group HashMap oracle"
    );
    assert!(
        after > before,
        "unified SeqScan→HashAgg batch pipeline did not fire"
    );
}
