use super::*;

fn query_rows(engine: &StorageQueryEngine, sql: &str) -> Vec<Vec<Value>> {
    let QueryResponse::ResultSet(result) = engine.execute(sql).unwrap() else {
        panic!("expected result set for: {sql}");
    };
    result.rows
}

fn explain_text(engine: &StorageQueryEngine, sql: &str) -> String {
    query_rows(engine, sql)
        .into_iter()
        .flat_map(|row| row.into_iter())
        .map(|value| match value {
            Value::String(s) => s,
            other => format!("{other:?}"),
        })
        .collect::<Vec<_>>()
        .join("\n")
}

fn setup_sales(engine: &StorageQueryEngine) {
    engine
        .execute(
            "CREATE TABLE sales (\
                id INT PRIMARY KEY, \
                region VARCHAR(8), \
                product VARCHAR(8), \
                amount INT\
            )",
        )
        .unwrap();
    engine
        .execute(
            "INSERT INTO sales VALUES \
             (1, 'east', 'book', 10), \
             (2, 'east', 'pen', 5), \
             (3, 'west', 'book', 7), \
             (4, 'west', NULL, 3), \
             (5, NULL, 'book', 4)",
        )
        .unwrap();
}

#[test]
fn rollup_multicolumn_grouping_masks_and_real_nulls() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    setup_sales(&engine);

    let rows = query_rows(
        &engine,
        "SELECT region, product, COUNT(*), SUM(amount), MIN(amount), MAX(amount), \
                AVG(amount), GROUPING(region), GROUPING(product), GROUPING(region, product) \
         FROM sales \
         GROUP BY region, product WITH ROLLUP \
         ORDER BY GROUPING(region, product), GROUPING(region), region, product",
    );

    assert_eq!(
        rows,
        vec![
            vec![
                Value::Null,
                Value::String("book".into()),
                Value::Int(1),
                Value::DecimalCompact(4, 0),
                Value::Int(4),
                Value::Int(4),
                Value::DecimalCompact(40000, 4),
                Value::Int(0),
                Value::Int(0),
                Value::Int(0),
            ],
            vec![
                Value::String("east".into()),
                Value::String("book".into()),
                Value::Int(1),
                Value::DecimalCompact(10, 0),
                Value::Int(10),
                Value::Int(10),
                Value::DecimalCompact(100000, 4),
                Value::Int(0),
                Value::Int(0),
                Value::Int(0),
            ],
            vec![
                Value::String("east".into()),
                Value::String("pen".into()),
                Value::Int(1),
                Value::DecimalCompact(5, 0),
                Value::Int(5),
                Value::Int(5),
                Value::DecimalCompact(50000, 4),
                Value::Int(0),
                Value::Int(0),
                Value::Int(0),
            ],
            vec![
                Value::String("west".into()),
                Value::Null,
                Value::Int(1),
                Value::DecimalCompact(3, 0),
                Value::Int(3),
                Value::Int(3),
                Value::DecimalCompact(30000, 4),
                Value::Int(0),
                Value::Int(0),
                Value::Int(0),
            ],
            vec![
                Value::String("west".into()),
                Value::String("book".into()),
                Value::Int(1),
                Value::DecimalCompact(7, 0),
                Value::Int(7),
                Value::Int(7),
                Value::DecimalCompact(70000, 4),
                Value::Int(0),
                Value::Int(0),
                Value::Int(0),
            ],
            vec![
                Value::Null,
                Value::Null,
                Value::Int(1),
                Value::DecimalCompact(4, 0),
                Value::Int(4),
                Value::Int(4),
                Value::DecimalCompact(40000, 4),
                Value::Int(0),
                Value::Int(1),
                Value::Int(1),
            ],
            vec![
                Value::String("east".into()),
                Value::Null,
                Value::Int(2),
                Value::DecimalCompact(15, 0),
                Value::Int(5),
                Value::Int(10),
                Value::DecimalCompact(75000, 4),
                Value::Int(0),
                Value::Int(1),
                Value::Int(1),
            ],
            vec![
                Value::String("west".into()),
                Value::Null,
                Value::Int(2),
                Value::DecimalCompact(10, 0),
                Value::Int(3),
                Value::Int(7),
                Value::DecimalCompact(50000, 4),
                Value::Int(0),
                Value::Int(1),
                Value::Int(1),
            ],
            vec![
                Value::Null,
                Value::Null,
                Value::Int(5),
                Value::DecimalCompact(29, 0),
                Value::Int(3),
                Value::Int(10),
                Value::DecimalCompact(58000, 4),
                Value::Int(1),
                Value::Int(1),
                Value::Int(3),
            ],
        ]
    );
}

#[test]
fn rollup_having_order_and_limit_apply_after_subtotals() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    setup_sales(&engine);

    let rows = query_rows(
        &engine,
        "SELECT region, product, SUM(amount), GROUPING(product) \
         FROM sales \
         GROUP BY region, product WITH ROLLUP \
         HAVING GROUPING(product) = 1 \
         ORDER BY GROUPING(region), region \
         LIMIT 2",
    );

    assert_eq!(
        rows,
        vec![
            vec![
                Value::Null,
                Value::Null,
                Value::DecimalCompact(4, 0),
                Value::Int(1),
            ],
            vec![
                Value::String("east".into()),
                Value::Null,
                Value::DecimalCompact(15, 0),
                Value::Int(1),
            ],
        ]
    );
}

#[test]
fn rollup_binds_alias_ordinal_and_expression_grouping_args() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());

    engine
        .execute("CREATE TABLE expr_rollup (id INT PRIMARY KEY, label VARCHAR(8), v INT)")
        .unwrap();
    engine
        .execute("INSERT INTO expr_rollup VALUES (1, 'x', 10), (2, 'x', 20)")
        .unwrap();

    let rows = query_rows(
        &engine,
        "SELECT label AS l, v + 1 AS bucket, COUNT(*), \
                GROUPING(l), GROUPING(2), GROUPING(l, 2) \
         FROM expr_rollup \
         GROUP BY l, 2 WITH ROLLUP \
         ORDER BY GROUPING(l, 2), l, bucket",
    );

    assert_eq!(
        rows,
        vec![
            vec![
                Value::String("x".into()),
                // `v + 1` grouping key is integral in MySQL (INT + INT).
                Value::Int(11),
                Value::Int(1),
                Value::Int(0),
                Value::Int(0),
                Value::Int(0),
            ],
            vec![
                Value::String("x".into()),
                Value::Int(21),
                Value::Int(1),
                Value::Int(0),
                Value::Int(0),
                Value::Int(0),
            ],
            vec![
                Value::String("x".into()),
                Value::Null,
                Value::Int(2),
                Value::Int(0),
                Value::Int(1),
                Value::Int(1),
            ],
            vec![
                Value::Null,
                Value::Null,
                Value::Int(2),
                Value::Int(1),
                Value::Int(1),
                Value::Int(3),
            ],
        ]
    );
}

#[test]
fn rollup_distinct_aggregates_merge_across_prefixes() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    setup_sales(&engine);

    let rows = query_rows(
        &engine,
        "SELECT region, COUNT(DISTINCT product), SUM(DISTINCT amount), GROUPING(region) \
         FROM sales \
         GROUP BY region WITH ROLLUP \
         ORDER BY GROUPING(region), region",
    );

    assert_eq!(
        rows,
        vec![
            vec![
                Value::Null,
                Value::Int(1),
                Value::DecimalCompact(4, 0),
                Value::Int(0),
            ],
            vec![
                Value::String("east".into()),
                Value::Int(2),
                Value::DecimalCompact(15, 0),
                Value::Int(0),
            ],
            vec![
                Value::String("west".into()),
                Value::Int(1),
                Value::DecimalCompact(10, 0),
                Value::Int(0),
            ],
            vec![
                Value::Null,
                Value::Int(2),
                Value::DecimalCompact(29, 0),
                Value::Int(1),
            ],
        ]
    );
}

#[test]
fn grouped_empty_input_with_rollup_emits_grand_total_row_in_mysql80_shape() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE empty_sales (region INT, amount INT)")
        .unwrap();

    // ROLLUP always includes the `()` grouping set; over an empty input MySQL
    // 8.0 still emits that single grand-total row (verified against MySQL 9.6:
    // `region=NULL, COUNT(*)=0, SUM(amount)=NULL, GROUPING(region)=1`), rather
    // than returning no rows.
    let rows = query_rows(
        &engine,
        "SELECT region, COUNT(*), SUM(amount), GROUPING(region) \
         FROM empty_sales \
         GROUP BY region WITH ROLLUP",
    );

    assert_eq!(
        rows,
        vec![vec![Value::Null, Value::Int(0), Value::Null, Value::Int(1)]]
    );
}

#[test]
fn rollup_over_cte_union_referencing_aggregating_cte_binds_to_outer_query() {
    // Regression (TPC-DS q5/q77/q80): a stripped trailing `WITH ROLLUP` on the
    // WITH body must bind to the body's GROUP BY, not to the first CTE that
    // carries a plain (non-rollup) GROUP BY. The earlier CTEs-before-body
    // traversal mis-attached the rollup to `agg` and the outer query degraded
    // to a plain aggregate, dropping the last channel subtotal + grand total.
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE base (sid VARCHAR(4), amount INT)")
        .unwrap();
    engine
        .execute("CREATE TABLE wbase (wid VARCHAR(4), amount INT)")
        .unwrap();
    engine
        .execute("INSERT INTO base VALUES ('b', 10), ('c', 20)")
        .unwrap();
    engine
        .execute("INSERT INTO wbase VALUES ('a', 30), ('b', 40)")
        .unwrap();

    let rows = query_rows(
        &engine,
        "WITH agg AS (SELECT sid, SUM(amount) AS amount FROM base GROUP BY sid) \
         , chan AS ( \
             SELECT 'store' AS channel, sid AS id, amount FROM agg \
             UNION ALL \
             SELECT 'web' AS channel, wid AS id, amount FROM wbase) \
         SELECT channel, id, SUM(amount) FROM chan \
         GROUP BY channel, id WITH ROLLUP \
         ORDER BY channel, id",
    );

    // 4 detail rows + 2 channel subtotals + 1 grand total = 7 (NULLs sort
    // first under default MySQL ordering, so subtotals lead each channel).
    assert_eq!(
        rows,
        vec![
            vec![Value::Null, Value::Null, Value::DecimalCompact(100, 0)],
            vec![
                Value::String("store".into()),
                Value::Null,
                Value::DecimalCompact(30, 0),
            ],
            vec![
                Value::String("store".into()),
                Value::String("b".into()),
                Value::DecimalCompact(10, 0),
            ],
            vec![
                Value::String("store".into()),
                Value::String("c".into()),
                Value::DecimalCompact(20, 0),
            ],
            vec![
                Value::String("web".into()),
                Value::Null,
                Value::DecimalCompact(70, 0),
            ],
            vec![
                Value::String("web".into()),
                Value::String("a".into()),
                Value::DecimalCompact(30, 0),
            ],
            vec![
                Value::String("web".into()),
                Value::String("b".into()),
                Value::DecimalCompact(40, 0),
            ],
        ]
    );
}

#[test]
fn window_over_rollup_preserves_grouping_and_partitions_by_grouping_expr() {
    // Regression (TPC-DS q36/q70/q86): a window over `GROUP BY ... WITH ROLLUP`
    // must (a) project the super-aggregate rows with NULL keys + correct
    // GROUPING(), and (b) partition the window by `GROUPING(a)+GROUPING(b)` —
    // the grouping operands are hoisted into the materialised window input so
    // the partition key is no longer re-evaluated (as 0) over the windowless
    // scan. Verified against MySQL 9.6.
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE wr (cat VARCHAR(4), cls VARCHAR(4), np INT)")
        .unwrap();
    engine
        .execute(
            "INSERT INTO wr VALUES \
             ('A','x',10),('A','y',20),('B','x',30),('B','y',40)",
        )
        .unwrap();

    let rows = query_rows(
        &engine,
        "SELECT SUM(np) AS gp, cat, cls, \
                GROUPING(cat)+GROUPING(cls) AS loch, \
                RANK() OVER (PARTITION BY GROUPING(cat)+GROUPING(cls) \
                             ORDER BY SUM(np)) AS rnk \
         FROM wr GROUP BY cat, cls WITH ROLLUP \
         ORDER BY loch DESC, CASE WHEN loch = 0 THEN cat END, rnk",
    );

    // loch DESC groups the grand total (2), then the two channel subtotals (1),
    // then the four detail rows (0); RANK is per-loch (the partition key), so
    // each level restarts at 1. Matches MySQL 9.6 exactly.
    assert_eq!(
        rows,
        vec![
            // grand total
            vec![
                Value::DecimalCompact(100, 0),
                Value::Null,
                Value::Null,
                Value::Int(2),
                Value::Int(1),
            ],
            // cat subtotals (loch = 1), ranked by SUM within the partition
            vec![
                Value::DecimalCompact(30, 0),
                Value::String("A".into()),
                Value::Null,
                Value::Int(1),
                Value::Int(1),
            ],
            vec![
                Value::DecimalCompact(70, 0),
                Value::String("B".into()),
                Value::Null,
                Value::Int(1),
                Value::Int(2),
            ],
            // detail rows (loch = 0), ordered by cat then rank
            vec![
                Value::DecimalCompact(10, 0),
                Value::String("A".into()),
                Value::String("x".into()),
                Value::Int(0),
                Value::Int(1),
            ],
            vec![
                Value::DecimalCompact(20, 0),
                Value::String("A".into()),
                Value::String("y".into()),
                Value::Int(0),
                Value::Int(2),
            ],
            vec![
                Value::DecimalCompact(30, 0),
                Value::String("B".into()),
                Value::String("x".into()),
                Value::Int(0),
                Value::Int(3),
            ],
            vec![
                Value::DecimalCompact(40, 0),
                Value::String("B".into()),
                Value::String("y".into()),
                Value::Int(0),
                Value::Int(4),
            ],
        ]
    );
}

#[test]
fn window_over_rollup_materializes_composite_group_context_partition_key() {
    // TPC-DS q36/q70/q86: the CASE combines GROUPING() with a group key. The
    // complete expression must be evaluated before the window-input CTE loses
    // its synthetic $group markers.
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE wrc (cat VARCHAR(4), cls VARCHAR(4), np INT)")
        .unwrap();
    engine
        .execute(
            "INSERT INTO wrc VALUES \
             ('A','x',10),('A','y',20),('B','x',30),('B','y',40)",
        )
        .unwrap();

    let rows = query_rows(
        &engine,
        "SELECT cat, cls, GROUPING(cat)+GROUPING(cls) AS loch, \
                RANK() OVER ( \
                  PARTITION BY GROUPING(cat)+GROUPING(cls), \
                    CASE WHEN GROUPING(cls)=0 THEN cat END \
                  ORDER BY SUM(np)) AS rnk \
         FROM wrc GROUP BY cat, cls WITH ROLLUP \
         ORDER BY loch DESC, cat, rnk",
    );

    assert_eq!(rows.len(), 7);
    assert_eq!(
        rows[0],
        vec![Value::Null, Value::Null, Value::Int(2), Value::Int(1)]
    );
    assert_eq!(rows[1][3], Value::Int(1));
    assert_eq!(rows[2][3], Value::Int(2));
    assert_eq!(rows[3][3], Value::Int(1));
    assert_eq!(rows[4][3], Value::Int(2));
    assert_eq!(rows[5][3], Value::Int(1));
    assert_eq!(rows[6][3], Value::Int(2));
}

#[test]
fn grouping_sets_empty_set_on_empty_input_returns_single_group() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine.execute("CREATE TABLE e (a INT)").unwrap();

    let rows = query_rows(
        &engine,
        "SELECT COUNT(*) FROM e GROUP BY GROUPING SETS (())",
    );

    assert_eq!(rows, vec![vec![Value::Int(0)]]);
}

#[test]
fn grouping_sets_fixed_prefix_and_duplicate_sets() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE gs (id INT PRIMARY KEY, d INT, a INT, b INT)")
        .unwrap();
    engine
        .execute(
            "INSERT INTO gs VALUES \
             (1, 1, 10, 100), \
             (2, 1, 10, 200), \
             (3, 1, 20, 100)",
        )
        .unwrap();

    let rows = query_rows(
        &engine,
        "SELECT d, a, b, COUNT(*), GROUPING(a), GROUPING(b), GROUPING(a, b) \
         FROM gs \
         GROUP BY d, GROUPING SETS ((a), (b), (a)) \
         ORDER BY d, GROUPING(a, b), GROUPING(a), a, b",
    );

    assert_eq!(
        rows,
        vec![
            vec![
                Value::Int(1),
                Value::Int(10),
                Value::Null,
                Value::Int(2),
                Value::Int(0),
                Value::Int(1),
                Value::Int(1),
            ],
            vec![
                Value::Int(1),
                Value::Int(10),
                Value::Null,
                Value::Int(2),
                Value::Int(0),
                Value::Int(1),
                Value::Int(1),
            ],
            vec![
                Value::Int(1),
                Value::Int(20),
                Value::Null,
                Value::Int(1),
                Value::Int(0),
                Value::Int(1),
                Value::Int(1),
            ],
            vec![
                Value::Int(1),
                Value::Int(20),
                Value::Null,
                Value::Int(1),
                Value::Int(0),
                Value::Int(1),
                Value::Int(1),
            ],
            vec![
                Value::Int(1),
                Value::Null,
                Value::Int(100),
                Value::Int(2),
                Value::Int(1),
                Value::Int(0),
                Value::Int(2),
            ],
            vec![
                Value::Int(1),
                Value::Null,
                Value::Int(200),
                Value::Int(1),
                Value::Int(1),
                Value::Int(0),
                Value::Int(2),
            ],
        ]
    );
}

#[test]
fn cube_grouping_masks() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE cube_t (id INT PRIMARY KEY, a INT, b INT)")
        .unwrap();
    engine
        .execute("INSERT INTO cube_t VALUES (1, 1, 10), (2, 1, 20), (3, 2, 10)")
        .unwrap();

    let rows = query_rows(
        &engine,
        "SELECT a, b, COUNT(*), GROUPING(a), GROUPING(b), GROUPING(a, b) \
         FROM cube_t \
         GROUP BY CUBE(a, b) \
         ORDER BY GROUPING(a, b), GROUPING(a), a, b",
    );

    assert_eq!(
        rows,
        vec![
            vec![
                Value::Int(1),
                Value::Int(10),
                Value::Int(1),
                Value::Int(0),
                Value::Int(0),
                Value::Int(0)
            ],
            vec![
                Value::Int(1),
                Value::Int(20),
                Value::Int(1),
                Value::Int(0),
                Value::Int(0),
                Value::Int(0)
            ],
            vec![
                Value::Int(2),
                Value::Int(10),
                Value::Int(1),
                Value::Int(0),
                Value::Int(0),
                Value::Int(0)
            ],
            vec![
                Value::Int(1),
                Value::Null,
                Value::Int(2),
                Value::Int(0),
                Value::Int(1),
                Value::Int(1)
            ],
            vec![
                Value::Int(2),
                Value::Null,
                Value::Int(1),
                Value::Int(0),
                Value::Int(1),
                Value::Int(1)
            ],
            vec![
                Value::Null,
                Value::Int(10),
                Value::Int(2),
                Value::Int(1),
                Value::Int(0),
                Value::Int(2)
            ],
            vec![
                Value::Null,
                Value::Int(20),
                Value::Int(1),
                Value::Int(1),
                Value::Int(0),
                Value::Int(2)
            ],
            vec![
                Value::Null,
                Value::Null,
                Value::Int(3),
                Value::Int(1),
                Value::Int(1),
                Value::Int(3)
            ],
        ]
    );
}

#[test]
fn grouping_argument_must_match_group_by_expression() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    setup_sales(&engine);

    let err = engine
        .execute(
            "SELECT GROUPING(product) \
             FROM sales \
             GROUP BY region WITH ROLLUP",
        )
        .unwrap_err();
    assert_eq!(err.code(), 3580);
}

#[test]
fn explain_shows_hash_and_streaming_rollup_nodes() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());

    engine
        .execute("CREATE TABLE h (id INT PRIMARY KEY, g INT, v INT)")
        .unwrap();
    engine
        .execute("INSERT INTO h VALUES (1, 1, 10), (2, 1, 20), (3, 2, 30)")
        .unwrap();
    let hash_plan = explain_text(
        &engine,
        "EXPLAIN FORMAT=TREE SELECT g, COUNT(*) FROM h GROUP BY g WITH ROLLUP",
    );
    assert!(
        hash_plan.contains("Hash rollup aggregate"),
        "expected hash rollup plan, got: {hash_plan}"
    );

    engine
        .execute("CREATE TABLE s (id INT PRIMARY KEY, g INT, v INT, INDEX g_idx (g))")
        .unwrap();
    engine
        .execute("INSERT INTO s VALUES (1, 1, 10), (2, 1, 20), (3, 2, 30)")
        .unwrap();
    let streaming_plan = explain_text(
        &engine,
        "EXPLAIN FORMAT=TREE SELECT g, COUNT(*) FROM s WHERE g >= 0 GROUP BY g WITH ROLLUP",
    );
    assert!(
        streaming_plan.contains("Streaming rollup aggregate"),
        "expected streaming rollup plan, got: {streaming_plan}"
    );
}

#[test]
fn explain_shows_hash_and_streaming_grouping_sets_nodes() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());

    engine
        .execute("CREATE TABLE hgs (id INT PRIMARY KEY, g INT, v INT)")
        .unwrap();
    engine
        .execute("INSERT INTO hgs VALUES (1, 1, 10), (2, 1, 20), (3, 2, 30)")
        .unwrap();
    let hash_plan = explain_text(
        &engine,
        "EXPLAIN FORMAT=TREE \
         SELECT g, v, COUNT(*) FROM hgs GROUP BY CUBE(g, v)",
    );
    assert!(
        hash_plan.contains("Hash grouping sets aggregate"),
        "expected hash grouping sets plan, got: {hash_plan}"
    );

    engine
        .execute("CREATE TABLE sgs (id INT PRIMARY KEY, g INT, v INT, INDEX g_idx (g))")
        .unwrap();
    engine
        .execute("INSERT INTO sgs VALUES (1, 1, 10), (2, 1, 20), (3, 2, 30)")
        .unwrap();
    let streaming_plan = explain_text(
        &engine,
        "EXPLAIN FORMAT=TREE \
         SELECT g, COUNT(*) FROM sgs WHERE g >= 0 GROUP BY GROUPING SETS ((g), ())",
    );
    assert!(
        streaming_plan.contains("Streaming grouping sets aggregate"),
        "expected streaming grouping sets plan, got: {streaming_plan}"
    );
}

#[test]
fn rollup_inside_derived_table_executes_and_propagates_subtotals() {
    // `WITH ROLLUP` nested in a FROM-clause derived table (TPC-DS q67 shape)
    // must parse, re-attach to the inner SELECT, and produce its subtotal /
    // grand-total rows for the enclosing query to consume.
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE rd (id INT PRIMARY KEY, g VARCHAR(8), h VARCHAR(8), v INT)")
        .unwrap();
    engine
        .execute("INSERT INTO rd VALUES (1, 'a', 'x', 1), (2, 'a', 'y', 2), (3, 'b', 'x', 4)")
        .unwrap();

    let rows = query_rows(
        &engine,
        "SELECT g, h, s FROM ( \
             SELECT g, h, SUM(v) s FROM rd GROUP BY g, h WITH ROLLUP \
         ) dw \
         ORDER BY g IS NULL, g, h IS NULL, h",
    );

    assert_eq!(
        rows,
        vec![
            vec![
                Value::String("a".into()),
                Value::String("x".into()),
                Value::DecimalCompact(1, 0),
            ],
            vec![
                Value::String("a".into()),
                Value::String("y".into()),
                Value::DecimalCompact(2, 0),
            ],
            vec![
                Value::String("a".into()),
                Value::Null,
                Value::DecimalCompact(3, 0),
            ],
            vec![
                Value::String("b".into()),
                Value::String("x".into()),
                Value::DecimalCompact(4, 0),
            ],
            vec![
                Value::String("b".into()),
                Value::Null,
                Value::DecimalCompact(4, 0),
            ],
            vec![Value::Null, Value::Null, Value::DecimalCompact(7, 0)],
        ]
    );
}

/// `GROUP BY ... WITH ROLLUP` over a bare derived table (no JOINs) — the
/// shape of TPC-DS q14 statement 1's outer query. The derived-table
/// execution fast path used to aggregate `group_by` directly and emit no
/// `group_N` super-aggregate columns, so the ROLLUP-bound projection
/// failed with `unknown column 'group_0'`. The derived body must now be
/// re-routed through the planner's `HashRollupAggregate`.
#[test]
fn rollup_over_derived_table_emits_super_aggregates() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE base (id INT PRIMARY KEY, k INT)")
        .unwrap();
    engine
        .execute("INSERT INTO base VALUES (1,10),(2,10),(3,20)")
        .unwrap();

    let rows = query_rows(
        &engine,
        "SELECT k, COUNT(*) FROM (SELECT k FROM base) y \
         GROUP BY k WITH ROLLUP ORDER BY k",
    );
    // NULL (grand total) sorts first under ASC, then each key group.
    assert_eq!(
        rows,
        vec![
            vec![Value::Null, Value::Int(3)],
            vec![Value::Int(10), Value::Int(2)],
            vec![Value::Int(20), Value::Int(1)],
        ]
    );
}

// ---------------------------------------------------------------------------
// Two-phase ("subtotal-derived") ROLLUP differential oracle.
//
// `GROUP BY c0..c{n-1} WITH ROLLUP` must equal, as a multiset, the union over
// every prefix length k in 0..=n of a plain `GROUP BY c0..c{k-1}` with the
// rolled-up columns padded to NULL literals. The plain-aggregate path is an
// entirely independent code path from the rollup path, so this validates the
// finest-level aggregation AND the coarser-level merge derivation at once,
// including real NULLs in the grouping columns (a real-NULL group and a
// rolled-up super-aggregate both collapse to NULL and are matched 1:1).
fn rollup_multiset_key(row: &[Value]) -> String {
    row.iter()
        .map(|v| format!("{v:?}"))
        .collect::<Vec<_>>()
        .join("\u{1}")
}

pub(super) fn assert_rollup_matches_plain_groupbys(
    engine: &StorageQueryEngine,
    table: &str,
    group_cols: &[&str],
    aggs: &str,
    parallel_hint: bool,
) {
    let n = group_cols.len();
    let cols = group_cols.join(", ");
    let hint = if parallel_hint {
        "/*+ PARALLEL(4) */ "
    } else {
        ""
    };
    let rollup_sql =
        format!("SELECT {hint}{cols}, {aggs} FROM {table} GROUP BY {cols} WITH ROLLUP");
    let mut got = query_rows(engine, &rollup_sql);

    let mut expected: Vec<Vec<Value>> = Vec::new();
    for k in 0..=n {
        let mut proj: Vec<String> = group_cols[..k].iter().map(|s| s.to_string()).collect();
        for _ in k..n {
            proj.push("NULL".to_string());
        }
        let group_clause = if k == 0 {
            String::new()
        } else {
            format!(" GROUP BY {}", group_cols[..k].join(", "))
        };
        let sql = format!(
            "SELECT {}, {aggs} FROM {table}{group_clause}",
            proj.join(", ")
        );
        expected.extend(query_rows(engine, &sql));
    }

    got.sort_by_key(|r| rollup_multiset_key(r));
    expected.sort_by_key(|r| rollup_multiset_key(r));
    assert_eq!(
        got, expected,
        "ROLLUP must equal multiset-union of plain GROUP BY prefixes\n  rollup_sql = {rollup_sql}"
    );
}

fn setup_wide(engine: &StorageQueryEngine) {
    engine
        .execute(
            "CREATE TABLE wide (\
                id INT PRIMARY KEY, \
                a VARCHAR(8), b VARCHAR(8), c VARCHAR(8), d VARCHAR(8), \
                amt INT\
            )",
        )
        .unwrap();
    // 24 rows: several distinct values per column, repeated combos (so coarser
    // levels truly aggregate multiple finest groups), and real NULLs in every
    // grouping column. Small integer amounts keep float STDDEV/VAR exact so the
    // merge-order-vs-row-order difference cannot perturb low bits.
    engine
        .execute(
            "INSERT INTO wide VALUES \
             (1,'x','p','m','u',10),(2,'x','p','m','v',20),(3,'x','p','n','u',5),\
             (4,'x','q','m','u',7),(5,'x','q','m','u',3),(6,'x','q','n','w',9),\
             (7,'y','p','m','u',1),(8,'y','p','m','v',2),(9,'y','p','n','u',4),\
             (10,'y','q','m','u',6),(11,'y','q','n','w',8),(12,'y','q','n','w',8),\
             (13,'x','p','m','u',11),(14,'x',NULL,'n','u',12),(15,'y','p',NULL,'v',13),\
             (16,'y','q','m',NULL,14),(17,NULL,'p','m','u',15),(18,NULL,NULL,NULL,NULL,16),\
             (19,'x','p','m','u',2),(20,'x','q','m','u',2),(21,'y','p','m','u',2),\
             (22,'y','q','n','w',2),(23,'x','p','n','u',2),(24,'y','p','n','v',2)",
        )
        .unwrap();
}

#[test]
fn rollup_two_phase_matches_plain_groupbys_basic_aggs() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    setup_wide(&engine);
    // 1..=4 columns deep, exercising one and multi-level prefix derivation.
    assert_rollup_matches_plain_groupbys(
        &engine,
        "wide",
        &["a", "b", "c", "d"],
        "COUNT(*), SUM(amt), MIN(amt), MAX(amt), AVG(amt)",
        false,
    );
    assert_rollup_matches_plain_groupbys(
        &engine,
        "wide",
        &["a", "b"],
        "COUNT(*), SUM(amt), AVG(amt)",
        false,
    );
    assert_rollup_matches_plain_groupbys(&engine, "wide", &["a"], "COUNT(*), SUM(amt)", false);
}

#[test]
fn rollup_two_phase_matches_plain_groupbys_distinct_and_variance() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    setup_wide(&engine);
    // COUNT/SUM DISTINCT exercise the set-union merge; STDDEV/VAR exercise the
    // n/sum/sum_sq merge (exact at these magnitudes).
    assert_rollup_matches_plain_groupbys(
        &engine,
        "wide",
        &["a", "b", "c"],
        "COUNT(DISTINCT amt), SUM(DISTINCT amt)",
        false,
    );
    assert_rollup_matches_plain_groupbys(
        &engine,
        "wide",
        &["a", "b", "c"],
        "STDDEV_POP(amt), VAR_POP(amt), STDDEV_SAMP(amt)",
        false,
    );
}

#[test]
fn rollup_two_phase_spills_under_tight_budget_and_matches_oracle() {
    use crate::engine::executor::mem_budget::{with_query_budget, MemBudget};
    use std::sync::Arc;

    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE rs (id INT PRIMARY KEY, a VARCHAR(220), b VARCHAR(220), amt INT)")
        .unwrap();
    // Wide string keys + many independent (a,b) finest groups: the finest map
    // overflows a tight budget (so the two-phase path bails via `add_within`)
    // AND the input's sort work exceeds the compact cfg(test) run floor (so the spilling
    // rollup actually spills to disk instead of charging in memory). `amt`
    // repeats heavily so DISTINCT still has cross-group work to dedup.
    let pad = "x".repeat(180);
    for chunk_start in (0..300).step_by(100) {
        let mut sql = String::from("INSERT INTO rs VALUES ");
        for i in chunk_start..chunk_start + 100 {
            if i > chunk_start {
                sql.push_str(", ");
            }
            sql.push_str(&format!(
                "({i}, 'a{:04}{pad}', 'b{:03}{pad}', {})",
                (i / 20) % 30,
                i % 20,
                (i * 7) % 100
            ));
        }
        engine.execute(&sql).unwrap();
    }

    let aggs = "COUNT(*), SUM(amt), MIN(amt), MAX(amt), AVG(amt), \
                COUNT(DISTINCT amt), SUM(DISTINCT amt)";
    let rollup_sql = format!("SELECT a, b, {aggs} FROM rs GROUP BY a, b WITH ROLLUP");

    // Oracle: multiset-union of plain GROUP BY prefixes, normal budget.
    let mut expected: Vec<Vec<Value>> = Vec::new();
    for (proj, grp) in [
        ("a, b", " GROUP BY a, b"),
        ("a, NULL", " GROUP BY a"),
        ("NULL, NULL", ""),
    ] {
        expected.extend(query_rows(
            &engine,
            &format!("SELECT {proj}, {aggs} FROM rs{grp}"),
        ));
    }

    // 32 KiB query budget: far below the wide-key finest map, so the two-phase
    // path bails to `aggregate_rollup_plan_rows_spilled` mid-build, which in
    // turn spills its external sort to disk.
    let sort_hits = crate::engine::executor::select::vec_bridge::COLUMNAR_ROLLUP_SORT_HITS
        .load(std::sync::atomic::Ordering::Relaxed);
    let budget = Arc::new(MemBudget::new(32 * 1024));
    let mut got = with_query_budget(budget, || query_rows(&engine, &rollup_sql));
    assert!(
        crate::engine::executor::select::vec_bridge::COLUMNAR_ROLLUP_SORT_HITS
            .load(std::sync::atomic::Ordering::Relaxed)
            > sort_hits
    );

    got.sort_by_key(|r| rollup_multiset_key(r));
    expected.sort_by_key(|r| rollup_multiset_key(r));
    assert_eq!(
        got, expected,
        "spilled ROLLUP must still equal the plain GROUP BY prefix union"
    );
}

#[test]
fn grouping_sets_two_phase_matches_plain_groupbys() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    setup_wide(&engine);
    let aggs = "COUNT(*), SUM(amt), MIN(amt), MAX(amt), AVG(amt), \
                COUNT(DISTINCT amt), SUM(DISTINCT amt)";
    // Non-prefix, overlapping, and duplicate grouping sets exercise deriving an
    // arbitrary subset from the all-columns finest grouping.
    let gs_sql = format!(
        "SELECT a, b, c, {aggs} FROM wide \
         GROUP BY GROUPING SETS ((a, b, c), (a, c), (b), (a), ())"
    );
    let mut got = query_rows(&engine, &gs_sql);

    // Oracle: each set -> plain GROUP BY on its columns, other columns NULL.
    // Projection column order is fixed (a, b, c).
    let oracle_sets: &[(&str, &str)] = &[
        ("a, b, c", " GROUP BY a, b, c"),
        ("a, NULL, c", " GROUP BY a, c"),
        ("NULL, b, NULL", " GROUP BY b"),
        ("a, NULL, NULL", " GROUP BY a"),
        ("NULL, NULL, NULL", ""),
    ];
    let mut expected: Vec<Vec<Value>> = Vec::new();
    for (proj, grp) in oracle_sets {
        expected.extend(query_rows(
            &engine,
            &format!("SELECT {proj}, {aggs} FROM wide{grp}"),
        ));
    }

    got.sort_by_key(|r| rollup_multiset_key(r));
    expected.sort_by_key(|r| rollup_multiset_key(r));
    assert_eq!(
        got, expected,
        "GROUPING SETS must equal multiset-union of plain GROUP BY per set\n  gs_sql = {gs_sql}"
    );
}

#[test]
fn rollup_approximate_prefixes_preserve_finest_addition_order() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE approx_rollup (a INT, b INT, c INT, v DOUBLE)")
        .unwrap();
    engine
        .execute("INSERT INTO approx_rollup VALUES (0,1,1,1e16),(0,2,1,-1e16),(0,2,2,1)")
        .unwrap();
    crate::util::env::with_vars(&[("MYDB_AGG_STRATEGY", Some("hash"))], || {
        let rows = query_rows(
            &engine,
            "SELECT a,b,c,SUM(v),GROUPING(a,b,c) FROM approx_rollup GROUP BY a,b,c WITH ROLLUP",
        );
        let total = rows
            .iter()
            .find(|row| row[4].to_display_string() == "7")
            .unwrap();
        assert_eq!(total[3], Value::Double(1.0));
    });
}
