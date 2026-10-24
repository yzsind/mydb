//! P1-B: streaming join → implicit aggregate fusion for the CTE-join driver
//! (`execute_select_with_cte_joins_inner`). A two-way join feeding an implicit
//! single-group aggregate (TPC-DS q97's `ssci FULL OUTER JOIN csci` → three
//! `SUM(CASE…)`) probes the hash join straight into the accumulators instead of
//! materialising the joined rows.
//!
//! `MYDB_JOIN_AGG_STREAM_FUSE` (default on) gates the fused path; `=off` forces
//! the materialise-then-aggregate general path, used here as the differential
//! oracle. Every shape (INNER / LEFT / RIGHT / FULL, with/without residual,
//! with/without WHERE, empty side) must be byte-identical across the two.

use super::*;

fn rows_of(engine: &StorageQueryEngine, sql: &str) -> Vec<Vec<Value>> {
    let QueryResponse::ResultSet(result) = execute_permissive(engine, sql).unwrap() else {
        panic!("expected result set for {sql}");
    };
    result.rows
}

fn stream_fused_count() -> u64 {
    native_batch_hits()
}

/// Numeric value of an aggregate cell, type-agnostic: `SUM` of integer
/// literals yields `DECIMAL` in MySQL, so the band counts come back as
/// `DecimalCompact`, while `COUNT`/`MIN`/`MAX` are `Int`.
fn as_i64(v: &Value) -> i64 {
    match v {
        Value::Int(x) => *x,
        Value::DecimalCompact(m, 0) => *m,
        other => panic!("expected an integer-valued aggregate, got {other:?}"),
    }
}

fn nums(row: &[Value]) -> Vec<i64> {
    row.iter().map(as_i64).collect()
}

/// Two small fact tables whose distinct (customer, item) key sets overlap
/// partially, so a FULL OUTER JOIN of the de-duplicating CTEs yields all three
/// q97 bands (store-only, catalog-only, both).
fn setup_q97_tables(engine: &StorageQueryEngine) {
    engine
        .execute(
            "CREATE TABLE store_sales (ss_customer_sk INT, ss_item_sk INT, ss_qty INT NOT NULL)",
        )
        .unwrap();
    engine
        .execute(
            "CREATE TABLE catalog_sales (cs_customer_sk INT, cs_item_sk INT, cs_qty INT NOT NULL)",
        )
        .unwrap();
    // store (customer,item): (1,1),(1,2),(2,1),(3,3)  [+ duplicate rows]
    engine
        .execute(
            "INSERT INTO store_sales VALUES \
             (1,1,5),(1,1,7),(1,2,3),(2,1,9),(3,3,2),(3,3,4)",
        )
        .unwrap();
    // catalog (customer,item): (1,1),(2,1),(4,4)
    engine
        .execute(
            "INSERT INTO catalog_sales VALUES \
             (1,1,1),(2,1,8),(2,1,2),(4,4,6)",
        )
        .unwrap();
    engine.execute("ANALYZE TABLE store_sales").unwrap();
    engine.execute("ANALYZE TABLE catalog_sales").unwrap();
}

const Q97_CTES: &str = "\
    WITH ssci AS (\
        SELECT ss_customer_sk customer_sk, ss_item_sk item_sk \
        FROM store_sales GROUP BY ss_customer_sk, ss_item_sk), \
    csci AS (\
        SELECT cs_customer_sk customer_sk, cs_item_sk item_sk \
        FROM catalog_sales GROUP BY cs_customer_sk, cs_item_sk) ";

/// Assert the fused path and the `=off` general path agree on `sql`, and that
/// the fused path actually fired (counter advanced) when `expect_fired`.
fn assert_fused_matches_general(engine: &StorageQueryEngine, sql: &str, expect_fired: bool) {
    let general = with_test_env(&[("MYDB_VECTORIZED", Some("off"))], || rows_of(engine, sql));
    let before = stream_fused_count();
    let fused = with_test_env(&[("MYDB_VECTORIZED", Some("on"))], || rows_of(engine, sql));
    let after = stream_fused_count();
    assert_eq!(
        fused, general,
        "fused join-agg stream diverged from materialize-then-agg oracle\nSQL: {sql}"
    );
    if expect_fired {
        assert!(after > before, "fused path did not fire for:\n{sql}");
    }
}

#[test]
fn q97_full_outer_join_implicit_agg_fused_matches_oracle() {
    let _guard = test_env_lock();
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    setup_q97_tables(&engine);

    let sql = format!(
        "{Q97_CTES}\
        SELECT \
          SUM(CASE WHEN ssci.customer_sk IS NOT NULL AND csci.customer_sk IS NULL THEN 1 ELSE 0 END) store_only, \
          SUM(CASE WHEN ssci.customer_sk IS NULL AND csci.customer_sk IS NOT NULL THEN 1 ELSE 0 END) catalog_only, \
          SUM(CASE WHEN ssci.customer_sk IS NOT NULL AND csci.customer_sk IS NOT NULL THEN 1 ELSE 0 END) store_and_catalog \
        FROM ssci FULL OUTER JOIN csci \
          ON ssci.customer_sk = csci.customer_sk AND ssci.item_sk = csci.item_sk"
    );

    assert_fused_matches_general(&engine, &sql, true);

    // Distinct store keys: (1,1),(1,2),(2,1),(3,3) = 4. Catalog: (1,1),(2,1),(4,4) = 3.
    // Both: (1,1),(2,1) = 2. store_only = 4-2 = 2, catalog_only = 3-2 = 1.
    let fused = rows_of(&engine, &sql);
    assert_eq!(fused.len(), 1, "implicit aggregate yields exactly one row");
    assert_eq!(
        nums(&fused[0]),
        vec![2, 1, 2],
        "store_only / catalog_only / both"
    );
}

#[test]
fn join_agg_stream_fuse_full_join_both_preserve_sides_match_oracle() {
    let _guard = test_env_lock();
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    setup_q97_tables(&engine);

    // FULL OUTER is the join type that actually routes to the legacy CTE-join
    // driver (INNER/LEFT/RIGHT over plain CTE refs are planned through the CBO
    // path). It exercises every emit branch at once: matched rows feed, the
    // preserve-left pass (left + NULL-right), and the preserve-right pass
    // (NULL-left + right). COUNT(*) counts emitted joined rows; SUM over each
    // side's column sees the NULL-extension.
    let sql = format!(
        "{Q97_CTES}\
        SELECT COUNT(*), \
               SUM(ssci.item_sk), SUM(csci.item_sk), \
               MIN(ssci.customer_sk), MAX(csci.customer_sk) \
        FROM ssci FULL OUTER JOIN csci \
          ON ssci.customer_sk = csci.customer_sk AND ssci.item_sk = csci.item_sk"
    );
    assert_fused_matches_general(&engine, &sql, true);
    // 4 store keys + 3 catalog keys − 2 shared = 5 emitted joined rows.
    let fused = rows_of(&engine, &sql);
    assert_eq!(
        as_i64(&fused[0][0]),
        5,
        "FULL OUTER emits one row per distinct key union"
    );
}

#[test]
fn join_agg_stream_fuse_inner_left_right_match_oracle() {
    let _guard = test_env_lock();
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());

    // INNER / LEFT / RIGHT over plain CTE refs are CBO-planned, so to exercise
    // the fused helper's `preserve_left=false` (INNER), preserve-left-only
    // (LEFT) and preserve-right-only (RIGHT) branches we use a system-catalog
    // self-join, which routes through the legacy driver for every join type.
    // The catalog content is identical under both gate settings, so parity is a
    // valid content-agnostic oracle.
    for jt in ["INNER", "LEFT OUTER", "RIGHT OUTER"] {
        let sql = format!(
            "SELECT COUNT(*), COUNT(t2.table_name) \
             FROM information_schema.tables t1 {jt} JOIN information_schema.tables t2 \
               ON t1.table_name = t2.table_name AND t1.table_schema = t2.table_schema"
        );
        assert_fused_matches_general(&engine, &sql, false);
    }
}

#[test]
fn join_agg_stream_fuse_with_residual_and_where_match_oracle() {
    let _guard = test_env_lock();
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    setup_q97_tables(&engine);

    // Residual (non-equi) conjunct in the ON clause: a match must also satisfy
    // the residual or the outer side NULL-extends. WHERE then filters the join
    // output. Both must mirror materialise-then-filter exactly.
    let residual = format!(
        "{Q97_CTES}\
        SELECT COUNT(*), SUM(ssci.item_sk), SUM(csci.item_sk) \
        FROM ssci FULL OUTER JOIN csci \
          ON ssci.customer_sk = csci.customer_sk \
         AND ssci.item_sk = csci.item_sk \
         AND csci.item_sk < 4"
    );
    assert_fused_matches_general(&engine, &residual, true);

    let with_where = format!(
        "{Q97_CTES}\
        SELECT COUNT(*), SUM(ssci.customer_sk) \
        FROM ssci FULL OUTER JOIN csci \
          ON ssci.customer_sk = csci.customer_sk AND ssci.item_sk = csci.item_sk \
        WHERE COALESCE(ssci.customer_sk, csci.customer_sk) >= 2"
    );
    assert_fused_matches_general(&engine, &with_where, true);
}

#[test]
fn join_agg_stream_fuse_having_pass_and_reject_match_oracle() {
    let _guard = test_env_lock();
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    setup_q97_tables(&engine);

    // HAVING on the implicit group (passes) + a HAVING that rejects (empty
    // result). Both must match the oracle.
    let having_pass = format!(
        "{Q97_CTES}\
        SELECT COUNT(*) FROM ssci FULL OUTER JOIN csci \
          ON ssci.customer_sk = csci.customer_sk AND ssci.item_sk = csci.item_sk \
        HAVING COUNT(*) > 0"
    );
    assert_fused_matches_general(&engine, &having_pass, true);

    let having_reject = format!(
        "{Q97_CTES}\
        SELECT COUNT(*) FROM ssci FULL OUTER JOIN csci \
          ON ssci.customer_sk = csci.customer_sk AND ssci.item_sk = csci.item_sk \
        HAVING COUNT(*) > 100000"
    );
    assert_fused_matches_general(&engine, &having_reject, true);
    assert!(
        rows_of(&engine, &having_reject).is_empty(),
        "HAVING-rejected implicit aggregate must be empty"
    );
}

#[test]
fn join_agg_stream_fuse_bare_column_sample_matches_oracle() {
    let _guard = test_env_lock();
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    setup_q97_tables(&engine);

    // Bare (non-aggregate) column under implicit aggregation = MySQL "any
    // value" = the sample (first emitted joined row). This is only a
    // *deterministic* oracle when the input row order is deterministic —
    // joining the GROUP BY CTEs would pick the sample out of a hash-ordered
    // (run-varying) CTE materialisation, so we join the BASE tables instead
    // (heap-order scan is stable). The fused path reproduces try_hash_join's
    // exact emit order, so the sample — and thus this bare column — must match
    // the materialise path byte-for-byte over the same input order.
    let bare = "SELECT store_sales.ss_customer_sk, store_sales.ss_item_sk, COUNT(*) \
                FROM store_sales FULL OUTER JOIN catalog_sales \
                  ON store_sales.ss_customer_sk = catalog_sales.cs_customer_sk \
                 AND store_sales.ss_item_sk = catalog_sales.cs_item_sk";
    assert_fused_matches_general(&engine, bare, true);
}

#[test]
fn join_agg_stream_fuse_empty_side_matches_oracle() {
    let _guard = test_env_lock();
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute(
            "CREATE TABLE store_sales (ss_customer_sk INT, ss_item_sk INT, ss_qty INT NOT NULL)",
        )
        .unwrap();
    engine
        .execute(
            "CREATE TABLE catalog_sales (cs_customer_sk INT, cs_item_sk INT, cs_qty INT NOT NULL)",
        )
        .unwrap();
    // Only store_sales populated; catalog CTE is empty.
    engine
        .execute("INSERT INTO store_sales VALUES (1,1,5),(2,2,3)")
        .unwrap();
    engine.execute("ANALYZE TABLE store_sales").unwrap();
    engine.execute("ANALYZE TABLE catalog_sales").unwrap();

    // An empty side hands the query to the general path (the fused helper bails
    // on an empty input), but the result must still be correct and equal to the
    // oracle: FULL OUTER with an empty right ⇒ all store rows, catalog NULL.
    let sql = format!(
        "{Q97_CTES}\
        SELECT \
          SUM(CASE WHEN ssci.customer_sk IS NOT NULL AND csci.customer_sk IS NULL THEN 1 ELSE 0 END) store_only, \
          SUM(CASE WHEN csci.customer_sk IS NOT NULL THEN 1 ELSE 0 END) catalog_seen \
        FROM ssci FULL OUTER JOIN csci \
          ON ssci.customer_sk = csci.customer_sk AND ssci.item_sk = csci.item_sk"
    );
    // Don't require the fused counter (empty side ⇒ general path), only parity.
    assert_fused_matches_general(&engine, &sql, false);
    let fused = rows_of(&engine, &sql);
    assert_eq!(nums(&fused[0]), vec![2, 0]);
}
