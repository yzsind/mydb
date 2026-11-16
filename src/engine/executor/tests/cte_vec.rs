//! CTE columnar (increment A) differential tests: serving a materialised CTE's rows
//! as columnar `Batch`es straight into the vec aggregate (`try_build_cte_batch_scan`)
//! must produce identical results to (a) the PlanRow→batch fallback adapter and
//! (b) the pure row pipeline.
//!
//! Two harness facts make this fiddly, hence the explicit setup:
//!   * The vec mode (`MYDB_VECTORIZED`) is a cached `OnceLock` read from the real
//!     process env, so `with_test_env` can't force it — tests use the
//!     `with_vec_mode` thread-local override instead.
//!   * A single-use CTE is inlined (no `CteScan`), so the queries reference the
//!     CTE twice (a `UNION ALL` branch) to keep it materialised — the production
//!     shape for the year_total-style CTEs this targets. A per-run hit counter
//!     asserts the columnar branch actually fired, guarding against a vacuous
//!     all-row-path comparison.

use super::*;
use crate::engine::executor::vec::config::{with_vec_mode, VecMode};

fn extract_rows(response: QueryResponse) -> Vec<Vec<Value>> {
    match response {
        QueryResponse::ResultSet(r) => r.rows,
        other => panic!("expected result set, got {other:?}"),
    }
}

/// Canonicalise a cell so the comparison is independent of which string *lane*
/// (`Value::String` vs `Value::SmallString`) a path happens to produce — they
/// carry identical text and serialise identically, but the vec path normalises
/// short strings to the `String`/`Utf8` lane while the row path keeps
/// `SmallString`. All non-string variants use their (stable) `Debug` form.
fn canon_cell(v: &Value) -> String {
    match crate::types::value_str(v) {
        Some(s) => format!("s:{s}"),
        None => format!("{v:?}"),
    }
}

fn sorted(engine: &StorageQueryEngine, sql: &str) -> Vec<Vec<String>> {
    let mut v: Vec<Vec<String>> = extract_rows(engine.execute(sql).unwrap())
        .iter()
        .map(|row| row.iter().map(canon_cell).collect())
        .collect();
    v.sort();
    v
}

fn cte_batch_scan_hits() -> u64 {
    crate::engine::executor::select::vec_bridge::CTE_BATCH_SCAN_HITS
        .load(std::sync::atomic::Ordering::Relaxed)
}

/// Run `sql` through vector and scalar kernels in the same batch graph and
/// assert both agree. Also asserts the native CTE source fired.
fn assert_three_paths_agree(engine: &StorageQueryEngine, sql: &str) {
    let before = cte_batch_scan_hits();
    let columnar_vec = with_vec_mode(VecMode::On, || sorted(engine, sql));
    assert!(
        cte_batch_scan_hits() > before,
        "columnar batch-scan branch did not fire for: {sql}"
    );
    let row_path = with_vec_mode(VecMode::Off, || sorted(engine, sql));
    assert_eq!(
        columnar_vec, row_path,
        "CTE columnar vec must match the pure row path for: {sql}"
    );
    assert!(
        !columnar_vec.is_empty(),
        "expected non-empty result for: {sql}"
    );
}

fn seed_base(engine: &StorageQueryEngine) {
    engine
        .execute(
            "CREATE TABLE base ( \
                g INT NOT NULL, \
                label VARCHAR(16) NOT NULL, \
                v INT NOT NULL, \
                amt DECIMAL(10,2) NOT NULL \
            )",
        )
        .unwrap();
    // Read-only CTE fixtures need the complete input, not 600 individual
    // autocommits. Preserve every integer, label and exact decimal cent.
    let labels = ["alpha", "beta", "gamma"];
    insert_fixture_rows(
        engine,
        "test.base",
        (0..600).map(|i| {
            vec![
                Value::Int(i % 7),
                Value::String(labels[i as usize % 3].into()),
                Value::Int(i),
                Value::DecimalCompact(i * 300 + i % 100, 2),
            ]
        }),
    );
}

#[test]
fn cte_vec_group_aggregate_matches_row_path() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    seed_base(&engine);
    // `c` referenced twice → materialised. Branch 1 (GROUP BY) drives the
    // columnar batch-scan; branch 2 is a shape-matched bare aggregate.
    assert_three_paths_agree(
        &engine,
        "WITH c AS (SELECT g, v FROM base) \
         SELECT g, SUM(v), COUNT(*), MIN(v), MAX(v), AVG(v) FROM c GROUP BY g \
         UNION ALL \
         SELECT 999, SUM(v), COUNT(*), MIN(v), MAX(v), AVG(v) FROM c",
    );
}

#[test]
fn cte_vec_string_group_key_matches_row_path() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    seed_base(&engine);
    assert_three_paths_agree(
        &engine,
        "WITH c AS (SELECT label, g, v FROM base) \
         SELECT label, g, SUM(v), COUNT(*) FROM c GROUP BY label, g \
         UNION ALL \
         SELECT 'ZZZ', 999, SUM(v), COUNT(*) FROM c",
    );
}

#[test]
fn cte_vec_projected_subset_matches_row_path() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    seed_base(&engine);
    // CTE projects 4 columns; the aggregate references only `g` and `amt`, so
    // the materialised CteScan carries `projected_columns`.
    assert_three_paths_agree(
        &engine,
        "WITH c AS (SELECT g, label, v, amt FROM base) \
         SELECT g, SUM(amt) FROM c GROUP BY g \
         UNION ALL \
         SELECT 999, SUM(amt) FROM c",
    );
}

#[test]
fn cte_vec_decimal_aggregate_matches_row_path() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    seed_base(&engine);
    assert_three_paths_agree(
        &engine,
        "WITH c AS (SELECT g, amt FROM base) \
         SELECT g, SUM(amt), MIN(amt), MAX(amt), COUNT(*) FROM c GROUP BY g \
         UNION ALL \
         SELECT 999, SUM(amt), MIN(amt), MAX(amt), COUNT(*) FROM c",
    );
}

#[test]
fn cte_scalar_kernel_keeps_native_cte_source_and_matches() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    seed_base(&engine);
    let sql = "WITH c AS (SELECT g, v FROM base) \
               SELECT g, SUM(v) FROM c GROUP BY g \
               UNION ALL \
               SELECT 999, SUM(v) FROM c";
    let before = cte_batch_scan_hits();
    let scalar = with_vec_mode(VecMode::Off, || sorted(&engine, sql));
    assert!(
        cte_batch_scan_hits() > before,
        "scalar kernel must retain the native CTE batch source"
    );
    let vector = with_vec_mode(VecMode::On, || sorted(&engine, sql));
    assert_eq!(scalar, vector, "scalar and vector batch kernels differ");
}

/// A disk-backed materialised CTE must remain a native vec leaf. This catches
/// regressions where opening the spill file silently rebuilds PlanRows (or
/// declines vec after input has already been materialised). The two references
/// use different predicates so the incremental reader also exercises filter
/// evaluation before projection.
#[test]
fn spilled_cte_streams_typed_batches_and_matches_row_path() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    seed_base(&engine);
    let sql = "WITH c AS (SELECT g, label, v, amt FROM base) \
               SELECT g, SUM(amt), COUNT(*) FROM c WHERE v >= 113 GROUP BY g \
               UNION ALL \
               SELECT 999, SUM(amt), COUNT(*) FROM c WHERE v < 113";
    let spill_env = [("MYDB_CTE_SPILL_THRESHOLD_MB", Some("0"))];
    let before = cte_batch_scan_hits();
    let columnar = with_vec_mode(VecMode::On, || {
        with_test_env(&spill_env, || sorted(&engine, sql))
    });
    assert!(
        cte_batch_scan_hits() > before,
        "spilled CTE must be consumed by the native batch source"
    );
    let row = with_vec_mode(VecMode::Off, || {
        with_test_env(&[("MYDB_CTE_SPILL_THRESHOLD_MB", Some("0"))], || {
            sorted(&engine, sql)
        })
    });
    assert_eq!(columnar, row, "spilled CTE vec and row paths differ");
    assert!(!columnar.is_empty());
}

// =====================================================================
// Increment 2: filtered columnar CTE leaf plus vectorized hash join over CTE references.
//
// The TPC-DS CTE-heavy queries (Q4/Q11/Q74) self-join a materialised CTE
// on a key, each reference carrying a per-alias filter, joined by ratio
// residuals — none of which increment-1 could serve columnar (it bailed on
// `filter.is_some()` and never fed the join). These tests drive that shape:
//   * a filtered CTE reference feeding the vec aggregate (the new
//     `compile_cte_row_predicate` path in `try_build_cte_batch_scan`), and
//   * a CTE self-join routed through the common HashJoin, whose build/probe pull the columnar CTE
//     leaves via `build_vec_input_stream`.
// Each must be byte-identical to (a) the same vec join with row-adapter
// leaves and (b) the pure row hash join. Two hit counters assert the vec
// join *and* the columnar leaf actually fired, so a silent row fallback
// can't make the comparison vacuous.
// =====================================================================

fn vec_hash_join_hits() -> u64 {
    crate::engine::executor::select::vec_bridge::VEC_HASH_JOIN_HITS
        .load(std::sync::atomic::Ordering::Relaxed)
        + crate::engine::executor::select::vec_bridge::VEC_HASH_JOIN_STREAM_HITS
            .load(std::sync::atomic::Ordering::Relaxed)
}

fn vec_hash_join_stream_hits() -> u64 {
    crate::engine::executor::select::vec_bridge::VEC_HASH_JOIN_STREAM_HITS
        .load(std::sync::atomic::Ordering::Relaxed)
}

/// 40 rows: 5 customers × 4 's'/'w' pairs, mixing int (`total`) and decimal
/// (`amt`). Shaped like TPC-DS `year_total` so a self-join on `cust` with
/// per-`styp` filters and a `total` ratio residual exercises the real path.
fn seed_evt(engine: &StorageQueryEngine) {
    engine
        .execute(
            "CREATE TABLE evt ( \
                cust INT NOT NULL, \
                styp VARCHAR(8) NOT NULL, \
                total INT NOT NULL, \
                amt DECIMAL(10,2) NOT NULL \
            )",
        )
        .unwrap();
    insert_fixture_rows(
        engine,
        "test.evt",
        (0..5).flat_map(|cust| {
            (0..4).flat_map(move |j| {
                [
                    vec![
                        Value::Int(cust),
                        Value::String("s".into()),
                        Value::Int(cust * 10 + j),
                        Value::DecimalCompact((cust * 7 + j) * 100 + (cust + j) % 100, 2),
                    ],
                    vec![
                        Value::Int(cust),
                        Value::String("w".into()),
                        Value::Int(cust * 10 + j + 5),
                        Value::DecimalCompact((cust * 9 + j) * 100 + (cust * 2 + j) % 100, 2),
                    ],
                ]
            })
        }),
    );
}

/// Run `sql` with vector and scalar kernels over the same native CTE/HashJoin
/// graph and assert both agree.
fn assert_join_paths_agree(engine: &StorageQueryEngine, sql: &str) {
    let cte_before = cte_batch_scan_hits();
    let join_before = vec_hash_join_hits();
    let columnar = with_vec_mode(VecMode::On, || sorted(engine, sql));
    assert!(
        vec_hash_join_hits() > join_before,
        "vec hash join did not fire for: {sql}"
    );
    assert!(
        cte_batch_scan_hits() > cte_before,
        "columnar CTE leaf did not fire for: {sql}"
    );
    let row_path = with_vec_mode(VecMode::Off, || sorted(engine, sql));
    assert_eq!(
        columnar, row_path,
        "columnar vec join must match the pure row hash join for: {sql}"
    );
    assert!(!columnar.is_empty(), "expected non-empty result for: {sql}");
}

/// Increment-2 filtered columnar leaf, on its own (no join): a filtered CTE
/// reference feeding the vec aggregate. Reuses the three-path agg harness,
/// which asserts the columnar leaf fired — proving the filter is now served
/// columnar rather than bailing to the row fallback.
#[test]
fn cte_vec_filtered_leaf_into_aggregate_matches_row_path() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    seed_base(&engine);
    // `WHERE label='alpha'` on a multi-ref CTE stays on the per-reference
    // CteScan.filter (it can't be pushed into the shared body), so the
    // columnar leaf must apply it itself.
    assert_three_paths_agree(
        &engine,
        "WITH c AS (SELECT g, label, v FROM base) \
         SELECT g, SUM(v), COUNT(*) FROM c WHERE label = 'alpha' GROUP BY g \
         UNION ALL \
         SELECT 999, SUM(v), COUNT(*) FROM c WHERE label = 'beta'",
    );
}

/// The core TPC-DS shape: self-join a materialised CTE on a key, per-alias
/// filters, plus a non-equi `total` residual.
#[test]
fn cte_vec_inner_self_join_with_residual_matches_row_path() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    seed_evt(&engine);
    assert_join_paths_agree(
        &engine,
        "WITH yt AS ( \
            SELECT cust, styp, total, amt FROM evt WHERE styp = 's' \
            UNION ALL \
            SELECT cust, styp, total, amt FROM evt WHERE styp = 'w' \
         ) \
         SELECT a.cust, a.total, b.total, b.amt \
         FROM yt a JOIN yt b ON a.cust = b.cust \
         WHERE a.styp = 's' AND b.styp = 'w' AND b.total > a.total",
    );
}

/// Pure equi inner self-join (no residual) — exercises the inner-join arm of
/// `try_vec_hash_join` with columnar leaves on both build and probe.
#[test]
fn cte_vec_inner_self_join_no_residual_matches_row_path() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    seed_evt(&engine);
    assert_join_paths_agree(
        &engine,
        "WITH yt AS ( \
            SELECT cust, styp, total FROM evt WHERE styp = 's' \
            UNION ALL \
            SELECT cust, styp, total FROM evt WHERE styp = 'w' \
         ) \
         SELECT a.cust, a.total, b.total \
         FROM yt a JOIN yt b ON a.cust = b.cust \
         WHERE a.styp = 's' AND b.styp = 'w'",
    );
}

/// LEFT join over CTE references — exercises the preserved-probe arm. The
/// left (preserved) side is filtered to a subset so some rows NULL-extend.
#[test]
fn cte_vec_left_join_over_cte_matches_row_path() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    seed_evt(&engine);
    assert_join_paths_agree(
        &engine,
        "WITH yt AS ( \
            SELECT cust, styp, total FROM evt WHERE styp = 's' \
            UNION ALL \
            SELECT cust, styp, total FROM evt WHERE styp = 'w' \
         ) \
         SELECT a.cust, a.total, b.total \
         FROM yt a LEFT JOIN yt b \
            ON a.cust = b.cust AND b.styp = 'w' AND b.total > a.total + 100 \
         WHERE a.styp = 's'",
    );
}

#[test]
fn cte_vec_hash_join_overflow_continues_with_grace_spill() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    seed_evt(&engine);
    let sql = "WITH yt AS ( \
                  SELECT cust, styp, total, amt FROM evt WHERE styp = 's' \
                  UNION ALL \
                  SELECT cust, styp, total, amt FROM evt WHERE styp = 'w' \
               ) \
               SELECT a.cust, a.total, b.total, b.amt \
               FROM yt a LEFT JOIN yt b \
                 ON a.cust = b.cust AND b.styp = 'w' AND b.total > a.total + 100 \
               WHERE a.styp = 's'";
    let spill_before = crate::engine::executor::select::vec_bridge::VEC_HASH_JOIN_SPILL_BYTES
        .load(std::sync::atomic::Ordering::Relaxed);
    let hit_before = vec_hash_join_hits();
    let vector = with_vec_mode(VecMode::On, || {
        with_test_env(
            &[
                ("MYDB_HASH_JOIN_MAX_BUILD_ROWS", Some("2")),
                ("MYDB_HASH_JOIN_GRACE_PARTITIONS", Some("4")),
            ],
            || sorted(&engine, sql),
        )
    });
    assert!(vec_hash_join_hits() > hit_before, "vec join must finish");
    assert!(
        crate::engine::executor::select::vec_bridge::VEC_HASH_JOIN_SPILL_BYTES
            .load(std::sync::atomic::Ordering::Relaxed)
            > spill_before,
        "tiny build cap must trigger vec grace spill"
    );
    let row = with_vec_mode(VecMode::Off, || sorted(&engine, sql));
    assert_eq!(vector, row, "grace vec join must match the row oracle");
    let tmp = dir.path().join("tmp");
    assert!(
        !tmp.exists()
            || std::fs::read_dir(tmp)
                .unwrap()
                .filter_map(Result::ok)
                .all(|entry| !entry
                    .file_name()
                    .to_string_lossy()
                    .starts_with("vec-hashjoin")),
        "vec grace temporary directory must be cleaned"
    );
}

#[test]
fn cte_scalar_and_vector_join_kernels_match() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    seed_evt(&engine);
    let sql = "WITH yt AS ( \
                  SELECT cust, styp, total FROM evt WHERE styp = 's' \
                  UNION ALL \
                  SELECT cust, styp, total FROM evt WHERE styp = 'w' \
               ) \
               SELECT a.cust, a.total, b.total \
               FROM yt a JOIN yt b ON a.cust = b.cust \
               WHERE a.styp = 's' AND b.styp = 'w' AND b.total > a.total";
    let vector = with_vec_mode(VecMode::On, || sorted(&engine, sql));
    let row = with_vec_mode(VecMode::Off, || sorted(&engine, sql));
    assert_eq!(vector, row, "scalar and vector join kernels differ");
}

/// A CTE-only self-join must engage the common batch join by default.
#[test]
fn cte_vec_join_engages_by_default_for_cte_only() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    seed_evt(&engine);
    let sql = "WITH yt AS ( \
                  SELECT cust, styp, total FROM evt WHERE styp = 's' \
                  UNION ALL \
                  SELECT cust, styp, total FROM evt WHERE styp = 'w' \
               ) \
               SELECT a.cust, a.total, b.total \
               FROM yt a JOIN yt b ON a.cust = b.cust \
               WHERE a.styp = 's' AND b.styp = 'w' AND b.total > a.total";
    let join_before = vec_hash_join_hits();
    let cte_before = cte_batch_scan_hits();
    let by_default = with_vec_mode(VecMode::On, || sorted(&engine, sql));
    assert!(
        vec_hash_join_hits() > join_before,
        "CTE-only join must engage the vec join by default (selective routing)"
    );
    assert!(
        cte_batch_scan_hits() > cte_before,
        "columnar CTE leaf must fire by default"
    );
    let row = with_vec_mode(VecMode::Off, || sorted(&engine, sql));
    assert_eq!(
        by_default, row,
        "default-routed vec join must match the row path"
    );
}

/// A mixed CTE/base-table join uses the same native join operator rather than
/// a query-shape-specific row route.
#[test]
fn cte_vec_join_skips_real_table_join_by_default() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    seed_evt(&engine);
    // `yt a JOIN evt e` — right leaf is a real-table SeqScan, so `plan_is_cte_only`
    // is false and the default selective routing must decline.
    let sql = "WITH yt AS ( \
                  SELECT cust, styp, total FROM evt WHERE styp = 's' \
                  UNION ALL \
                  SELECT cust, styp, total FROM evt WHERE styp = 'w' \
               ) \
               SELECT a.cust, a.total, e.styp \
               FROM yt a JOIN evt e ON a.cust = e.cust \
               WHERE a.styp = 's' AND a.total > 5";
    let join_before = vec_hash_join_hits();
    let cte_before = cte_batch_scan_hits();
    let by_default = with_vec_mode(VecMode::On, || sorted(&engine, sql));
    assert!(vec_hash_join_hits() > join_before);
    assert!(cte_batch_scan_hits() > cte_before);
    let row = with_vec_mode(VecMode::Off, || sorted(&engine, sql));
    assert_eq!(by_default, row, "row path result must be unchanged");
}

/// A HashAgg parent and its nested real-table HashJoin form one batch pipeline;
/// retired internal overrides cannot change the physical graph.
#[test]
fn nested_real_table_join_ignores_retired_shape_overrides() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    seed_evt(&engine);
    // DISTINCT aggregate deliberately bypasses the older row fused
    // HashJoin→HashAgg path so the generic native Batch compiler owns the
    // HashJoin input and the routing gate is observable.
    let sql = "SELECT a.cust, COUNT(DISTINCT b.total) \
               FROM evt a JOIN evt b ON a.cust = b.cust \
               GROUP BY a.cust";

    let before = vec_hash_join_stream_hits();
    let default_result = with_vec_mode(VecMode::On, || sorted(&engine, sql));
    assert!(vec_hash_join_stream_hits() > before);

    let override_before = vec_hash_join_stream_hits();
    let override_result = with_vec_mode(VecMode::On, || sorted(&engine, sql));
    assert!(
        vec_hash_join_stream_hits() > override_before,
        "nested batch join must execute on every run"
    );
    let row = with_vec_mode(VecMode::Off, || sorted(&engine, sql));
    assert_eq!(default_result, row);
    assert_eq!(override_result, row);
}

fn vec_filter_input_hits() -> u64 {
    crate::engine::executor::select::vec_bridge::VEC_FILTER_INPUT_HITS
        .load(std::sync::atomic::Ordering::Relaxed)
}

/// The q78 body shape, in miniature: three aggregating CTEs, a LEFT-join chain
/// (`a ⟕ b ⟕ c`), then a post-join WHERE whose OR spans *two distinct build
/// sides* (`b` and `c`) — so it can't fold into a single join's residual and
/// stays a `Filter` node above the top join — followed by ORDER BY + LIMIT.
/// This is exactly the chain that makes q78's body collapse to a fully
/// columnar pipeline, exercising all three new pieces: the build-side
/// correction (LEFT → preserve probe), the native vec `Filter` over the join
/// (`try_build_vec_filter_input`), and the vec TopN over that filter. Asserts
/// vec == row AND that the vec filter fired, so a silent row fallback can't make
/// the comparison vacuous.
#[test]
fn cte_vec_q78_body_shape_left_join_filter_topn() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    seed_evt(&engine);
    let sql = "WITH a AS (SELECT cust, SUM(total) AS at FROM evt WHERE styp = 's' GROUP BY cust), \
                    b AS (SELECT cust, SUM(total) AS bt FROM evt WHERE styp = 'w' GROUP BY cust), \
                    c AS (SELECT cust, SUM(amt) AS ct FROM evt GROUP BY cust) \
               SELECT a.cust, a.at, COALESCE(b.bt, 0) AS obt, COALESCE(c.ct, 0) AS oct \
               FROM a \
               LEFT JOIN b ON a.cust = b.cust \
               LEFT JOIN c ON a.cust = c.cust \
               WHERE COALESCE(b.bt, 0) > 0 OR COALESCE(c.ct, 0) > 0 \
               ORDER BY a.at DESC, a.cust \
               LIMIT 5";
    let filter_before = vec_filter_input_hits();
    let cte_before = cte_batch_scan_hits();
    let vec_on = with_vec_mode(VecMode::On, || sorted(&engine, sql));
    let filter_fired = vec_filter_input_hits() > filter_before;
    let cte_fired = cte_batch_scan_hits() > cte_before;
    let row = with_vec_mode(VecMode::Off, || sorted(&engine, sql));
    assert_eq!(vec_on, row, "q78-shape vec body must match the row path");
    assert!(!vec_on.is_empty(), "expected non-empty result");
    assert!(
        cte_fired,
        "columnar CTE leaf must fire for the q78 body shape"
    );
    assert!(
        filter_fired,
        "native vec Filter-over-join must fire for the q78 body shape"
    );

    // The same nested join chain under a tiny build cap must switch each
    // overflowing fused build to vec grace partitions, not abandon the
    // already-started subtree and re-run it through the row executor.
    let spill_before = crate::engine::executor::select::vec_bridge::VEC_HASH_JOIN_SPILL_BYTES
        .load(std::sync::atomic::Ordering::Relaxed);
    let spilled = with_vec_mode(VecMode::On, || {
        with_test_env(
            &[
                ("MYDB_HASH_JOIN_MAX_BUILD_ROWS", Some("2")),
                ("MYDB_HASH_JOIN_GRACE_PARTITIONS", Some("4")),
            ],
            || sorted(&engine, sql),
        )
    });
    assert_eq!(spilled, row, "spilled q78-shape vec chain differs");
    assert!(
        crate::engine::executor::select::vec_bridge::VEC_HASH_JOIN_SPILL_BYTES
            .load(std::sync::atomic::Ordering::Relaxed)
            > spill_before,
        "nested vec join chain must report grace spill bytes"
    );
}
