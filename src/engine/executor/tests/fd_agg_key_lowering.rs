use super::support::query_rows as rows_of;
use super::*;

/// Stable row ordering for differential comparison: the fused-agg map's
/// iteration order is nondeterministic, so two runs of the same query can
/// emit groups in different order. Sort by the debug rendering before
/// comparing the packed vs wide key encodings.
fn sorted_rows(engine: &StorageQueryEngine, sql: &str) -> Vec<Vec<Value>> {
    let mut rows = rows_of(engine, sql);
    rows.sort_by_cached_key(|row| format!("{row:?}"));
    rows
}

fn inlj_fused_grouped_count() -> u64 {
    native_result_batch_hits()
}

fn native_result_batch_hits() -> u64 {
    crate::engine::executor::select::vec_bridge::NATIVE_RESULT_BATCH_HITS
        .load(std::sync::atomic::Ordering::Relaxed)
}

/// q97-shaped INLJ fused grouped aggregate: a small outer dimension
/// (`dimd`) drives an index lookup into a fact (`factf`), and the GROUP BY
/// keys are two integer fact columns — exactly the `ss_customer_sk,
/// ss_item_sk` shape the packed-key path (P1-A) targets. `cust` is nullable
/// and `tag` is a short string so the same fixture exercises the NULL
/// sentinel and the collation-key encode branches of `write_value_inline_key`.
fn setup_q97_shape_schema(engine: &StorageQueryEngine) {
    engine
        .execute("CREATE TABLE dimd (d INT NOT NULL PRIMARY KEY)")
        .unwrap();
    engine
        .execute("INSERT INTO dimd VALUES (1),(2),(3),(4),(5)")
        .unwrap();
    engine
        .execute(
            "CREATE TABLE factf (\
             id INT NOT NULL PRIMARY KEY, \
             d INT NOT NULL, \
             cust INT, \
             item INT NOT NULL, \
             amt INT NOT NULL, \
             tag VARCHAR(8) NOT NULL, \
             INDEX idx_factf_d(d))",
        )
        .unwrap();
    insert_fixture_rows(
        engine,
        "test.factf",
        (1..=400).map(|id| {
            vec![
                Value::Int(id),
                Value::Int(1 + id % 5),
                if id % 23 == 0 {
                    Value::Null
                } else {
                    Value::Int(1 + id % 40)
                },
                Value::Int(1 + id % 13),
                Value::Int(id % 19 + 1),
                Value::String(format!("t{}", id % 7)),
            ]
        }),
    );
    engine.execute("ANALYZE TABLE dimd").unwrap();
    engine.execute("ANALYZE TABLE factf").unwrap();
}

fn setup_sales_schema(engine: &StorageQueryEngine) {
    engine
        .execute(
            "CREATE TABLE customer (\
             c_customer_sk INT NOT NULL PRIMARY KEY, \
             c_customer_id CHAR(16) NOT NULL, \
             c_first_name CHAR(20), \
             c_last_name CHAR(30), \
             c_email_address CHAR(50))",
        )
        .unwrap();
    engine
        .execute(
            "CREATE TABLE sales (\
             id INT NOT NULL PRIMARY KEY, \
             customer_sk INT NOT NULL, \
             sold_year INT NOT NULL, \
             amount INT NOT NULL, \
             INDEX idx_sales_customer(customer_sk))",
        )
        .unwrap();
    engine
        .execute(
            "INSERT INTO customer VALUES \
             (1,'C000000000000001','Ann','Lee','ann@example.com'),\
             (2,'C000000000000002','Ann','Lee','ann@example.com'),\
             (3,'C000000000000003','Bo','Ray','bo@example.com')",
        )
        .unwrap();
    insert_fixture_rows(
        engine,
        "test.sales",
        (1..=600).map(|id| {
            vec![
                Value::Int(id),
                Value::Int(id % 3 + 1),
                Value::Int(1999 + id % 2),
                Value::Int(id * 2),
            ]
        }),
    );
    engine.execute("ANALYZE TABLE customer").unwrap();
    engine.execute("ANALYZE TABLE sales").unwrap();
}

fn setup_sales_date_schema(engine: &StorageQueryEngine) {
    setup_sales_schema(engine);
    engine
        .execute(
            "CREATE TABLE date_dim (\
             d_date_sk INT NOT NULL PRIMARY KEY, \
             d_year INT NOT NULL, \
             INDEX idx_date_year(d_year))",
        )
        .unwrap();
    engine
        .execute(
            "INSERT INTO date_dim VALUES \
             (1999,1999),(2000,2000),(2001,2001)",
        )
        .unwrap();
    engine.execute("ANALYZE TABLE date_dim").unwrap();
}

fn setup_q43_shape_schema(engine: &StorageQueryEngine) {
    engine
        .execute(
            "CREATE TABLE date_dim (\
             d_date_sk INT NOT NULL PRIMARY KEY, \
             d_year INT NOT NULL, \
             d_day_name VARCHAR(12) NOT NULL, \
             INDEX idx_date_year(d_year))",
        )
        .unwrap();
    engine
        .execute(
            "CREATE TABLE store_sales (\
             ss_item_sk INT NOT NULL PRIMARY KEY, \
             ss_sold_date_sk INT NOT NULL, \
             ss_store_sk INT NOT NULL, \
             ss_sales_price INT NOT NULL, \
             INDEX idx_ss_sold_date(ss_sold_date_sk))",
        )
        .unwrap();
    engine
        .execute(
            "CREATE TABLE store (\
             s_store_sk INT NOT NULL PRIMARY KEY, \
             s_store_name VARCHAR(20) NOT NULL, \
             s_store_id VARCHAR(20) NOT NULL, \
             s_gmt_offset INT NOT NULL)",
        )
        .unwrap();
    engine
        .execute(
            "INSERT INTO date_dim VALUES \
             (1,2000,'Sunday'),(2,2000,'Monday'),(3,2000,'Tuesday'),\
             (4,2000,'Wednesday'),(5,1999,'Sunday')",
        )
        .unwrap();
    engine
        .execute(
            "INSERT INTO store VALUES \
             (10,'north','S001',-5),(20,'south','S002',-5),(30,'west','S003',-6)",
        )
        .unwrap();
    insert_fixture_rows(
        engine,
        "test.store_sales",
        (1..=360).map(|id| {
            vec![
                Value::Int(id),
                Value::Int(1 + id % 5),
                Value::Int(match id % 3 {
                    0 => 10,
                    1 => 20,
                    _ => 30,
                }),
                Value::Int(3 + id % 19),
            ]
        }),
    );
    engine.execute("ANALYZE TABLE date_dim").unwrap();
    engine.execute("ANALYZE TABLE store_sales").unwrap();
    engine.execute("ANALYZE TABLE store").unwrap();
}

#[test]
fn fd_agg_key_lowering_matches_disabled_path_and_counts() {
    let _guard = test_env_lock();
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    setup_sales_schema(&engine);

    let sql = "\
        SELECT /*+ HASH_JOIN(c,s) */ c.c_first_name, c.c_last_name, c.c_email_address, s.sold_year, SUM(s.amount) \
        FROM customer c, sales s \
        WHERE c.c_customer_sk = s.customer_sk \
        GROUP BY c.c_first_name, c.c_last_name, c.c_email_address, s.sold_year \
        ORDER BY c.c_first_name, c.c_last_name, c.c_email_address, s.sold_year";
    let disabled = with_test_env(&[("MYDB_FD_AGG_KEY_LOWERING", Some("off"))], || {
        rows_of(&engine, sql)
    });
    let before = native_result_batch_hits();
    let enabled = rows_of(&engine, sql);
    assert_eq!(enabled, disabled);
    assert!(
        native_result_batch_hits() > before,
        "unified batch aggregate pipeline did not execute"
    );
    assert!(
        enabled.iter().any(|row| {
            row[0] == Value::String("Ann".into())
                && row[1] == Value::String("Lee".into())
                && row[3] == Value::Int(1999)
        }),
        "duplicate customer attributes must remain grouped by visible values"
    );
}

#[test]
fn nullable_unique_key_does_not_trigger_fd_lowering() {
    let _guard = test_env_lock();
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute(
            "CREATE TABLE dim_u (\
             id INT NOT NULL PRIMARY KEY, \
             code INT, \
             label VARCHAR(20), \
             UNIQUE KEY uq_code(code))",
        )
        .unwrap();
    engine
        .execute(
            "CREATE TABLE fact_u (\
             id INT NOT NULL PRIMARY KEY, \
             code INT, \
             amount INT NOT NULL, \
             INDEX idx_fact_u_code(code))",
        )
        .unwrap();
    engine
        .execute("INSERT INTO dim_u VALUES (1,10,'a'),(2,20,'b'),(3,NULL,'n')")
        .unwrap();
    engine
        .execute("INSERT INTO fact_u VALUES (1,10,5),(2,20,7),(3,NULL,11)")
        .unwrap();

    let sql = "\
        SELECT d.code, d.label, SUM(f.amount) \
        FROM dim_u d, fact_u f \
        WHERE d.code = f.code \
        GROUP BY d.code, d.label \
        ORDER BY d.code";
    let enabled = rows_of(&engine, sql);
    let disabled = with_test_env(&[("MYDB_FD_AGG_KEY_LOWERING", Some("off"))], || {
        rows_of(&engine, sql)
    });
    assert_eq!(enabled, disabled);
}

#[test]
fn q4_like_cte_union_self_join_matches_disabled_path() {
    let _guard = test_env_lock();
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    setup_sales_schema(&engine);

    let sql = "\
        WITH year_total AS (\
            SELECT /*+ HASH_JOIN(c,s) */ c.c_customer_id customer_id, c.c_first_name first_name, \
                   c.c_last_name last_name, c.c_email_address email, \
                   s.sold_year dyear, SUM(s.amount) total_amt, 's' sale_type \
            FROM customer c, sales s \
            WHERE c.c_customer_sk = s.customer_sk \
            GROUP BY c.c_customer_id, c.c_first_name, c.c_last_name, \
                     c.c_email_address, s.sold_year \
            UNION ALL \
            SELECT /*+ HASH_JOIN(c,s) */ c.c_customer_id, c.c_first_name, c.c_last_name, \
                   c.c_email_address, s.sold_year, SUM(s.amount * 2), 'c' \
            FROM customer c, sales s \
            WHERE c.c_customer_sk = s.customer_sk \
            GROUP BY c.c_customer_id, c.c_first_name, c.c_last_name, \
                     c.c_email_address, s.sold_year) \
        SELECT y2.customer_id, y2.first_name, y2.last_name, y2.email \
        FROM year_total y1, year_total y2 \
        WHERE y1.customer_id = y2.customer_id \
          AND y1.sale_type = 's' AND y2.sale_type = 'c' \
          AND y1.dyear = 1999 AND y2.dyear = 2000 \
          AND y1.total_amt > 0 AND y2.total_amt > y1.total_amt \
        ORDER BY y2.customer_id, y2.first_name, y2.last_name, y2.email";
    let disabled = with_test_env(&[("MYDB_FD_AGG_KEY_LOWERING", Some("off"))], || {
        rows_of(&engine, sql)
    });
    let before = native_result_batch_hits();
    let enabled = rows_of(&engine, sql);
    assert_eq!(enabled, disabled);
    assert!(
        native_result_batch_hits() > before,
        "Q4-like CTE UNION query did not use the unified batch result boundary"
    );
}

#[test]
fn late_inlj_streaming_group_aggregate_handles_fact_in_subquery_residual() {
    let _guard = test_env_lock();
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    setup_sales_date_schema(&engine);

    let sql = "\
        SELECT /*+ ORDERED INDEX_NLJ(c,s) HASH_JOIN(s,d) */ \
               c.c_customer_id, c.c_first_name, c.c_last_name, \
               c.c_email_address, d.d_year, SUM(s.amount) \
        FROM customer c, sales s, date_dim d \
        WHERE c.c_customer_sk = s.customer_sk \
          AND s.sold_year = d.d_date_sk \
          AND d.d_year IN (1999, 2000) \
        GROUP BY c.c_customer_id, c.c_first_name, c.c_last_name, \
                 c.c_email_address, d.d_year \
        ORDER BY c.c_customer_id, d.d_year";

    let disabled = with_test_env(&[("MYDB_FD_AGG_KEY_LOWERING", Some("off"))], || {
        rows_of(&engine, sql)
    });
    let before = native_result_batch_hits();
    let enabled = rows_of(&engine, sql);

    assert_eq!(enabled, disabled);
    assert!(
        native_result_batch_hits() > before,
        "unified batch aggregate should handle fact-only IN-subquery residuals"
    );
}

#[test]
fn late_inlj_streaming_group_aggregate_handles_q43_pivot_shape() {
    let _guard = test_env_lock();
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    setup_q43_shape_schema(&engine);

    let sql = "\
        SELECT /*+ ORDERED INDEX_NLJ(d,ss) HASH_JOIN(ss,s) */ \
               s.s_store_name, s.s_store_id, \
               SUM(CASE WHEN d.d_day_name = 'Sunday' THEN ss.ss_sales_price ELSE NULL END) AS sun_sales, \
               SUM(CASE WHEN d.d_day_name = 'Monday' THEN ss.ss_sales_price ELSE NULL END) AS mon_sales, \
               SUM(CASE WHEN d.d_day_name = 'Tuesday' THEN ss.ss_sales_price ELSE NULL END) AS tue_sales \
        FROM date_dim d, store_sales ss, store s \
        WHERE d.d_date_sk = ss.ss_sold_date_sk \
          AND ss.ss_store_sk = s.s_store_sk \
          AND s.s_gmt_offset = -5 \
        GROUP BY s.s_store_name, s.s_store_id \
        ORDER BY s.s_store_name, s.s_store_id";
    let QueryResponse::ResultSet(explain) = engine
        .execute(&format!("EXPLAIN FORMAT=TREE {sql}"))
        .unwrap()
    else {
        panic!("expected explain result set");
    };
    let explain_text = explain
        .rows
        .iter()
        .flatten()
        .map(value_to_string)
        .collect::<Vec<_>>()
        .join("\n");

    let disabled = crate::engine::executor::vec::config::with_vec_mode(
        crate::engine::executor::vec::config::VecMode::Off,
        || rows_of(&engine, sql),
    );
    let before = native_result_batch_hits();
    let enabled = crate::engine::executor::vec::config::with_vec_mode(
        crate::engine::executor::vec::config::VecMode::On,
        || rows_of(&engine, sql),
    );

    assert_eq!(enabled, disabled);
    assert_eq!(enabled.len(), 2);
    assert!(
        native_result_batch_hits() > before,
        "Q43-shaped pivot SUM should use the unified batch aggregate; plan:\n{explain_text}"
    );
}

#[test]
fn unified_inlj_group_aggregate_accepts_complex_pivot_value() {
    let _guard = test_env_lock();
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    setup_q43_shape_schema(&engine);

    let sql = "\
        SELECT /*+ ORDERED INDEX_NLJ(d,ss) HASH_JOIN(ss,s) */ \
               s.s_store_name, \
               SUM(CASE WHEN d.d_day_name = 'Sunday' THEN ss.ss_sales_price * 2 ELSE NULL END) \
        FROM date_dim d, store_sales ss, store s \
        WHERE d.d_date_sk = ss.ss_sold_date_sk \
          AND ss.ss_store_sk = s.s_store_sk \
          AND s.s_gmt_offset = -5 \
        GROUP BY s.s_store_name \
        ORDER BY s.s_store_name";

    let disabled = crate::engine::executor::vec::config::with_vec_mode(
        crate::engine::executor::vec::config::VecMode::Off,
        || rows_of(&engine, sql),
    );
    let before = native_result_batch_hits();
    let enabled = crate::engine::executor::vec::config::with_vec_mode(
        crate::engine::executor::vec::config::VecMode::On,
        || rows_of(&engine, sql),
    );

    assert_eq!(enabled, disabled);
    assert!(
        native_result_batch_hits() > before,
        "complex pivot value expressions must stay in the unified pipeline"
    );
}

// ---------------------------------------------------------------------------
// P1-A: packed fixed-width group key for the fused INLJ grouped aggregate.
// `MYDB_FUSED_AGG_PACKED_KEY` (default on) serialises the composite GROUP BY
// key into one inline `InlineKeyBuf` via `write_value_inline_key` instead of a
// per-row `Vec<ValueKey>`. The byte encoding is the single source of truth for
// packed-key ⟺ `ValueKey` equality, so the packed path must group/aggregate
// byte-identically to the `=off` wide path on every key shape. Each test uses
// the wide path as the differential oracle.
// ---------------------------------------------------------------------------

#[test]
fn packed_group_key_matches_wide_q97_shape() {
    let _guard = test_env_lock();
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    setup_q97_shape_schema(&engine);

    // Two integer fact GROUP BY keys + COUNT/SUM — the canonical q97 ssci/csci
    // CTE shape that drives the packed-key allocation win.
    let sql = "SELECT /*+ ORDERED INDEX_NLJ(dimd, factf) NO_HASH_JOIN(dimd, factf) */ \
               factf.cust, factf.item, COUNT(*), SUM(factf.amt) \
               FROM dimd JOIN factf ON dimd.d = factf.d \
               GROUP BY factf.cust, factf.item";

    let wide = with_test_env(&[("MYDB_FUSED_AGG_PACKED_KEY", Some("off"))], || {
        sorted_rows(&engine, sql)
    });
    let before = inlj_fused_grouped_count();
    let packed = sorted_rows(&engine, sql);
    let after = inlj_fused_grouped_count();

    assert_eq!(
        packed, wide,
        "packed group key diverged from wide Vec<ValueKey> oracle (q97 shape)"
    );
    assert!(
        after > before,
        "fused INLJ grouped aggregate counter did not advance — query did not hit the packed-key path"
    );
    // Sanity: the NULL-cust band must form its own group, distinct from any
    // integer cust, and survive the packed 0xFF sentinel encode.
    assert!(
        packed.iter().any(|row| row[0] == Value::Null),
        "expected a NULL cust group"
    );
}

#[test]
fn packed_group_key_matches_wide_string_and_mixed_keys() {
    let _guard = test_env_lock();
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    setup_q97_shape_schema(&engine);

    // String (collation-key encode) leading a mixed string+int composite key,
    // plus a nullable int aggregate argument.
    let sql = "SELECT /*+ ORDERED INDEX_NLJ(dimd, factf) NO_HASH_JOIN(dimd, factf) */ \
               factf.tag, factf.item, COUNT(*), SUM(factf.amt), SUM(factf.cust) \
               FROM dimd JOIN factf ON dimd.d = factf.d \
               GROUP BY factf.tag, factf.item";

    let wide = with_test_env(&[("MYDB_FUSED_AGG_PACKED_KEY", Some("off"))], || {
        sorted_rows(&engine, sql)
    });
    let before = inlj_fused_grouped_count();
    let packed = sorted_rows(&engine, sql);
    let after = inlj_fused_grouped_count();

    assert_eq!(
        packed, wide,
        "packed group key diverged from wide oracle (string+int composite key)"
    );
    assert!(after > before, "fused INLJ grouped path did not fire");
}

#[test]
fn packed_group_key_matches_wide_keyset_scan_arm() {
    let _guard = test_env_lock();
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    setup_q97_shape_schema(&engine);

    // Force the keyset-scan worker arm (a separate packed-key build site from
    // the direct/general arms) and confirm packed == wide there too.
    let sql = "SELECT /*+ ORDERED INDEX_NLJ(dimd, factf) NO_HASH_JOIN(dimd, factf) */ \
               factf.cust, factf.item, COUNT(*), SUM(factf.amt) \
               FROM dimd JOIN factf ON dimd.d = factf.d \
               GROUP BY factf.cust, factf.item";

    // Arena off so this exercises the FusedGroups keyset arm (packed vs wide
    // FusedKey encoding); the int-key arena bypasses FusedKey entirely, and with
    // it default-on this arena-eligible shape would skip the path under test.
    let wide = with_test_env(
        &[
            ("MYDB_INLJ_GROUP_AGG_KEYSET_SCAN", Some("force")),
            ("MYDB_FUSED_INT_ARENA", Some("off")),
            ("MYDB_FUSED_AGG_PACKED_KEY", Some("off")),
        ],
        || sorted_rows(&engine, sql),
    );
    let packed = with_test_env(
        &[
            ("MYDB_INLJ_GROUP_AGG_KEYSET_SCAN", Some("force")),
            ("MYDB_FUSED_INT_ARENA", Some("off")),
        ],
        || sorted_rows(&engine, sql),
    );

    assert_eq!(
        packed, wide,
        "packed group key diverged from wide oracle (keyset-scan arm)"
    );
}

#[test]
fn packed_group_key_matches_wide_through_fd_lowering() {
    let _guard = test_env_lock();
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    setup_sales_schema(&engine);

    // FD lowering rewrites the visible multi-column group key to the PK
    // determinant and re-keys at the end — both the lowered eval_key and the
    // re-key pass now build packed keys. Exercise both with packed on/off.
    let sql = "\
        SELECT /*+ HASH_JOIN(c,s) */ c.c_first_name, c.c_last_name, c.c_email_address, s.sold_year, SUM(s.amount) \
        FROM customer c, sales s \
        WHERE c.c_customer_sk = s.customer_sk \
        GROUP BY c.c_first_name, c.c_last_name, c.c_email_address, s.sold_year \
        ORDER BY c.c_first_name, c.c_last_name, c.c_email_address, s.sold_year";

    let wide = with_test_env(&[("MYDB_FUSED_AGG_PACKED_KEY", Some("off"))], || {
        rows_of(&engine, sql)
    });
    let before = native_result_batch_hits();
    let packed = rows_of(&engine, sql);
    let after = native_result_batch_hits();

    assert_eq!(
        packed, wide,
        "packed group key diverged from wide oracle through FD key lowering"
    );
    assert!(after > before, "unified batch aggregate did not fire");
}

/// TPC-H Q9 shape: `HashJoin(nation, INLJ(INLJ(lineitem, orders), supplier))`
/// under a GROUP BY.
///
/// `lineitem` is the *dimension* of the inner INLJ but is not unique on the
/// join key (`PRIMARY KEY (l_orderkey, l_suppkey)` — an order has many lines),
/// so the late-INLJ streaming grouped-aggregate rewrite cannot apply. It used
/// to discover that only after `execute_plan_rows(dim_plan)` had materialised
/// the *whole* join subtree, then `return Ok(None)` and let the generic
/// aggregate path execute the identical subtree a second time — doubling
/// every scan, index probe and page read below the aggregate. At TPC-H sf=1
/// that was 143k buffer page accesses instead of 72k, and 1.83s instead of
/// 1.08s.
fn setup_q9_shape_schema(engine: &StorageQueryEngine) {
    engine
        .execute(
            "CREATE TABLE nation (\
             n_nationkey INT NOT NULL PRIMARY KEY, \
             n_name VARCHAR(25) NOT NULL)",
        )
        .unwrap();
    engine
        .execute(
            "CREATE TABLE supplier (\
             s_suppkey INT NOT NULL PRIMARY KEY, \
             s_nationkey INT NOT NULL)",
        )
        .unwrap();
    engine
        .execute(
            "CREATE TABLE orders (\
             o_orderkey INT NOT NULL PRIMARY KEY, \
             o_year INT NOT NULL)",
        )
        .unwrap();
    engine
        .execute(
            "CREATE TABLE lineitem (\
             l_orderkey INT NOT NULL, \
             l_suppkey INT NOT NULL, \
             l_amount INT NOT NULL, \
             PRIMARY KEY (l_orderkey, l_suppkey))",
        )
        .unwrap();
    engine
        .execute("INSERT INTO nation VALUES (1,'CHINA'),(2,'JAPAN'),(3,'KOREA')")
        .unwrap();
    insert_fixture_rows(
        engine,
        "test.supplier",
        (1..=9).map(|s| vec![Value::Int(s), Value::Int(1 + s % 3)]),
    );
    insert_fixture_rows(
        engine,
        "test.orders",
        (1..=200).map(|o| vec![Value::Int(o), Value::Int(1998 + o % 3)]),
    );
    insert_fixture_rows(
        engine,
        "test.lineitem",
        (1..=200).flat_map(|order| {
            (0..4).map(move |line| {
                vec![
                    Value::Int(order),
                    Value::Int(1 + (order + line) % 9),
                    Value::Int(order * 10 + line),
                ]
            })
        }),
    );
    for table in ["nation", "supplier", "orders", "lineitem"] {
        engine.execute(&format!("ANALYZE TABLE {table}")).unwrap();
    }
}

const Q9_SHAPE_SQL: &str = "\
    SELECT /*+ ORDERED INDEX_NLJ(l,o) INDEX_NLJ(o,s) HASH_JOIN(s,n) */ \
           n.n_name, o.o_year, SUM(l.l_amount) \
    FROM lineitem l, orders o, supplier s, nation n \
    WHERE o.o_orderkey = l.l_orderkey \
      AND s.s_suppkey = l.l_suppkey \
      AND n.n_nationkey = s.s_nationkey \
    GROUP BY n.n_name, o.o_year \
    ORDER BY n.n_name, o.o_year";

#[test]
fn late_inlj_probe_does_not_re_execute_non_unique_dimension_subtree() {
    let _guard = test_env_lock();
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    setup_q9_shape_schema(&engine);

    let QueryResponse::ResultSet(explain) = engine
        .execute(&format!("EXPLAIN FORMAT=TREE {Q9_SHAPE_SQL}"))
        .unwrap()
    else {
        panic!("expected explain result set");
    };
    let plan_text = explain
        .rows
        .iter()
        .flatten()
        .map(value_to_string)
        .collect::<Vec<_>>()
        .join("\n");
    assert_eq!(
        plan_text.matches("Index nested loop join").count(),
        2,
        "fixture must plan the Q9 shape (hash join over a two-level INLJ chain) \
         for this regression to be reachable; plan:\n{plan_text}"
    );

    // Page accesses are the sensor: a second execution of the dimension
    // subtree cannot happen without re-reading its pages. Measured against the
    // same plan with the fused grouped-aggregate probe removed entirely, which
    // by construction executes everything exactly once.
    let page_io = || {
        let (hits, misses) = engine.database().buffer_stats_counts();
        hits + misses
    };
    let run_oracle = || {
        with_test_env(
            &[("MYDB_HASH_JOIN_GROUP_AGG_FUSED_DISABLE", Some("1"))],
            || rows_of(&engine, Q9_SHAPE_SQL),
        )
    };
    let _ = run_oracle();
    let before = page_io();
    let oracle = run_oracle();
    let oracle_io = page_io() - before;

    let _ = rows_of(&engine, Q9_SHAPE_SQL);
    let before = page_io();
    let fused = rows_of(&engine, Q9_SHAPE_SQL);
    let fused_io = page_io() - before;

    assert_eq!(
        fused, oracle,
        "fused grouped-aggregate probe changed the result"
    );
    assert!(
        oracle_io > 0,
        "fixture produced no page accesses — the sensor cannot detect a re-execution"
    );
    assert_eq!(
        fused_io, oracle_io,
        "the late-INLJ grouped-aggregate probe re-executed the dimension subtree it \
         could not fuse ({fused_io} page accesses vs {oracle_io} for a single pass)"
    );
}
