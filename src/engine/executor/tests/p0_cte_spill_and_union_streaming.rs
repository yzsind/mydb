//! P0-3 regression coverage.
//!
//! 1. **CTE materialization spill**: when MYDB_CTE_SPILL_THRESHOLD_MB=0, every
//!    non-empty CTE goes through the on-disk spill path
//!    (`CteSpillBuilder::start_spill` → `ValueRowReader`). Verifies the
//!    spilled-CTE consumer path (`spilled_cte_read_all`) returns the
//!    same rows as the in-memory path.
//!
//! 2. **UNION ALL CTE streaming aggregate pushdown** (`lazy_union_aggregate_plans`).
//!    Verifies that an aggregate consumer over a UNION-ALL CTE produces
//!    the same answer with and without the pushdown
//!    (`MYDB_CTE_UNION_AGG_PUSHDOWN`). The two paths bottom out in
//!    completely different code:
//!    - pushdown OFF: full UNION-ALL materialisation, then row-wise group by;
//!    - pushdown ON: per-branch group-by, merge of partials
//!      (`merge_union_aggregate_partials`).

use super::*;

fn rows_with_env(
    vars: &[(&'static str, Option<&str>)],
    setup: &[&str],
    sql: &str,
) -> Vec<Vec<Value>> {
    with_test_env(vars, || {
        let dir = tempfile::tempdir().unwrap();
        let engine = open_test_engine(dir.path());
        for s in setup {
            engine.execute(s).unwrap();
        }
        let QueryResponse::ResultSet(rs) = engine.execute(sql).unwrap() else {
            panic!("expected result set: {sql}");
        };
        rs.rows
    })
}

#[test]
fn cte_spill_threshold_zero_forces_disk_path() {
    // Threshold 0 → every CTE row immediately spills. The test passes
    // iff the spilled-CTE consumer (`spilled_cte_read_all`) returns the
    // same rows as the in-memory path. We feed the CTE into a downstream
    // join + ORDER BY so the spill consumer is exercised.
    let setup = [
        "CREATE TABLE big (id INT NOT NULL PRIMARY KEY, k INT NOT NULL, v INT NOT NULL)",
        "CREATE TABLE dim (k INT NOT NULL PRIMARY KEY, label VARCHAR(20) NOT NULL)",
        "INSERT INTO big VALUES (1,1,10),(2,2,20),(3,1,30),(4,3,40),(5,2,50),(6,3,60),(7,1,70),(8,2,80)",
        "INSERT INTO dim VALUES (1,'a'),(2,'b'),(3,'c')",
    ];
    let sql = "\
        WITH agg AS (SELECT k, SUM(v) AS sv FROM big GROUP BY k) \
        SELECT d.label, a.sv FROM agg a JOIN dim d ON d.k = a.k \
        ORDER BY d.label";

    let in_mem = rows_with_env(&[("MYDB_CTE_SPILL_THRESHOLD_MB", None)], &setup, sql);
    let spilled = rows_with_env(&[("MYDB_CTE_SPILL_THRESHOLD_MB", Some("0"))], &setup, sql);
    assert_eq!(in_mem, spilled, "spill path must match in-memory path");
    // Sanity: shape is what we expect (sums: a=10+30+70=110, b=20+50+80=150, c=40+60=100).
    let unwrap_int = |v: &Value| match v {
        Value::Int(x) => *x as i128,
        Value::DecimalCompact(m, _) => *m as i128,
        _ => panic!("non-numeric: {v:?}"),
    };
    let labels: Vec<String> = in_mem
        .iter()
        .map(|r| match &r[0] {
            Value::String(s) => s.clone(),
            Value::SmallString(s) => std::str::from_utf8(s.as_slice()).unwrap().to_string(),
            other => panic!("non-string label: {other:?}"),
        })
        .collect();
    assert_eq!(labels, vec!["a", "b", "c"]);
    let sums: Vec<i128> = in_mem.iter().map(|r| unwrap_int(&r[1])).collect();
    assert_eq!(sums, vec![110, 150, 100]);
}

#[test]
fn recursive_cte_working_delta_and_total_spools_round_trip() {
    // Threshold zero forces every nonempty delta to disk. Thirty-three
    // rounds exercise repeated spool replacement without hundreds of
    // redundant file lifecycles; verify the spill count as well as results.
    let sql = "WITH RECURSIVE seq(n) AS (\
        SELECT 1 UNION ALL SELECT n + 1 FROM seq WHERE n < 33\
        ) SELECT COUNT(*), SUM(n), MIN(n), MAX(n) FROM seq";
    let in_memory = rows_with_env(&[("MYDB_CTE_SPILL_THRESHOLD_MB", None)], &[], sql);
    let spills = super::super::spill_quota::cte_spill_count();
    let spilled = rows_with_env(&[("MYDB_CTE_SPILL_THRESHOLD_MB", Some("0"))], &[], sql);
    assert!(super::super::spill_quota::cte_spill_count() >= spills + 33);
    assert_eq!(spilled, in_memory);
    assert_eq!(spilled[0][0], Value::Int(33));
    assert_eq!(spilled[0][1].to_display_string(), "561");
    assert_eq!(spilled[0][2], Value::Int(1));
    assert_eq!(spilled[0][3], Value::Int(33));
}

#[test]
fn recursive_union_distinct_uses_spillable_history_set() {
    let setup = [
        "CREATE TABLE recursive_dupe (x INT NOT NULL)",
        "INSERT INTO recursive_dupe VALUES (1),(1)",
    ];
    // Every recursive row is produced twice. UNION (without ALL) must retain
    // one value per iteration, and the DISTINCT membership state must remain
    // correct when both deltas and the hash-count state are forced to spill.
    let sql = "WITH RECURSIVE seq(n) AS (\
        SELECT 1 UNION SELECT n + 1 FROM seq JOIN recursive_dupe d ON d.x = 1 WHERE n < 33\
        ) SELECT COUNT(*), SUM(n), MIN(n), MAX(n) FROM seq";
    let in_memory = rows_with_env(&[("MYDB_CTE_SPILL_THRESHOLD_MB", None)], &setup, sql);
    let spills = super::super::spill_quota::cte_spill_count();
    let spilled = rows_with_env(&[("MYDB_CTE_SPILL_THRESHOLD_MB", Some("0"))], &setup, sql);
    assert!(super::super::spill_quota::cte_spill_count() >= spills + 33);
    assert_eq!(spilled, in_memory);
    assert_eq!(spilled[0][0], Value::Int(33));
    assert_eq!(spilled[0][1].to_display_string(), "561");
    assert_eq!(spilled[0][2], Value::Int(1));
    assert_eq!(spilled[0][3], Value::Int(33));
}

#[test]
fn union_all_cte_streaming_pushdown_matches_full_materialisation() {
    // Two-branch UNION ALL inside a CTE consumed by GROUP BY SUM().
    // This is exactly the shape the lazy_union_aggregate_plans path
    // targets (TPC-DS Q2 / Q5 / Q14 family). With pushdown ON, the
    // executor must NOT materialise the full union (which would be
    // 2 * branch_rows wide); with OFF, the executor materialises and
    // groups conventionally. Both paths must yield identical rows.
    let setup = [
        "CREATE TABLE store_sales (k INT NOT NULL, v INT NOT NULL, PRIMARY KEY(k, v))",
        "CREATE TABLE web_sales (k INT NOT NULL, v INT NOT NULL, PRIMARY KEY(k, v))",
        "INSERT INTO store_sales VALUES (1,10),(1,20),(2,30),(3,40)",
        "INSERT INTO web_sales VALUES (1,100),(2,200),(2,300),(4,400)",
    ];
    // Use SUM (covered by aggregate_union_consumer_supported gate)
    // and a GROUP BY column k that is present in both branches.
    let sql = "\
        WITH u AS ( \
            SELECT k, v FROM store_sales \
            UNION ALL \
            SELECT k, v FROM web_sales \
        ) \
        SELECT k, SUM(v) AS sv FROM u GROUP BY k ORDER BY k";

    let pushdown_on = rows_with_env(&[("MYDB_CTE_UNION_AGG_PUSHDOWN", None)], &setup, sql);
    let pushdown_off = rows_with_env(&[("MYDB_CTE_UNION_AGG_PUSHDOWN", Some("off"))], &setup, sql);
    assert_eq!(
        pushdown_on, pushdown_off,
        "pushdown must preserve semantics"
    );

    // Sanity: k=1 → 10+20+100 = 130; k=2 → 30+200+300 = 530;
    // k=3 → 40; k=4 → 400.
    let kv: Vec<(i64, i128)> = pushdown_on
        .iter()
        .map(|r| {
            let k = match &r[0] {
                Value::Int(x) => *x,
                other => panic!("non-int k: {other:?}"),
            };
            let v = match &r[1] {
                Value::Int(x) => *x as i128,
                Value::DecimalCompact(m, _) => *m as i128,
                other => panic!("non-numeric sum: {other:?}"),
            };
            (k, v)
        })
        .collect();
    assert_eq!(kv, vec![(1, 130), (2, 530), (3, 40), (4, 400)]);
}

#[test]
fn materialized_grouped_numeric_native_state_matches_scalar_oracle() {
    let setup = [
        "CREATE TABLE sales (id INT NOT NULL PRIMARY KEY, g INT NOT NULL, h INT NOT NULL, qty INT, price DECIMAL(7,2))",
        "INSERT INTO sales VALUES \
            (1,1,10,2,10.50),\
            (2,1,10,3,20.00),\
            (3,1,20,NULL,7.25),\
            (4,2,10,5,NULL),\
            (5,2,20,1,3.00),\
            (6,2,20,4,4.50),\
            (7,3,30,NULL,NULL)",
    ];
    let sql = "\
        SELECT g, h, COUNT(*), COUNT(amount), SUM(amount), AVG(amount) \
        FROM (SELECT g, h, qty * price AS amount FROM sales) d \
        GROUP BY g, h \
        HAVING SUM(amount) > 0 \
        ORDER BY g, h";

    let native = rows_with_env(&[("MYDB_VECTORIZED", Some("on"))], &setup, sql);
    let oracle = rows_with_env(&[("MYDB_VECTORIZED", Some("off"))], &setup, sql);
    assert_eq!(
        native, oracle,
        "native grouped numeric aggregate must match scalar state oracle"
    );
}

#[test]
fn derived_count_frequency_collapse_matches_materialized_oracle() {
    let setup = [
        "CREATE TABLE counted (id INT NOT NULL PRIMARY KEY, k INT NOT NULL, v INT)",
        "INSERT INTO counted VALUES (1,1,10),(2,1,11),(3,2,20),(4,3,30),(5,4,NULL),(6,5,NULL)",
    ];
    let optimized = rows_with_env(
        &[],
        &setup,
        "SELECT c, COUNT(*) AS f \
         FROM (SELECT k, COUNT(v) AS c FROM counted GROUP BY k) d \
         GROUP BY c ORDER BY c",
    );
    // The harmless predicate deliberately keeps the conventional derived
    // materialization path as a semantic oracle for the collapsed pipeline.
    let materialized = rows_with_env(
        &[],
        &setup,
        "SELECT c, COUNT(*) AS f \
         FROM (SELECT k, COUNT(v) AS c FROM counted GROUP BY k) d \
         WHERE c >= 0 GROUP BY c ORDER BY c",
    );
    assert_eq!(optimized, materialized);
    assert_eq!(
        optimized,
        vec![
            vec![Value::Int(0), Value::Int(2)],
            vec![Value::Int(1), Value::Int(2)],
            vec![Value::Int(2), Value::Int(1)],
        ]
    );
}

#[test]
fn hash_join_grouped_fused_aggregate_having_sum_matches_legacy() {
    let setup = [
        "CREATE TABLE dim (k INT NOT NULL PRIMARY KEY, label VARCHAR(16) NOT NULL)",
        "CREATE TABLE fact (id INT NOT NULL PRIMARY KEY, k INT NOT NULL, qty INT NOT NULL, price DECIMAL(8,2) NOT NULL)",
        "INSERT INTO dim VALUES (1,'a'),(2,'b'),(3,'c')",
        "INSERT INTO fact VALUES \
            (1,1,2,10.00),(2,1,3,10.00),(3,1,1,5.00),\
            (4,2,1,7.00),(5,2,2,7.00),\
            (6,3,10,1.00)",
    ];
    let sql = "\
        SELECT d.label, SUM(f.qty * f.price) AS sales \
        FROM fact f JOIN dim d ON f.k = d.k \
        GROUP BY d.label \
        HAVING SUM(f.qty * f.price) > 20 \
        ORDER BY d.label";

    let fused = rows_with_env(
        &[
            ("MYDB_HASH_JOIN_GROUP_AGG_FUSED", Some("1")),
            ("MYDB_HASH_JOIN_GROUP_AGG_FUSED_DISABLE", None),
        ],
        &setup,
        sql,
    );
    let legacy = rows_with_env(
        &[
            ("MYDB_HASH_JOIN_GROUP_AGG_FUSED", Some("1")),
            ("MYDB_HASH_JOIN_GROUP_AGG_FUSED_DISABLE", Some("1")),
        ],
        &setup,
        sql,
    );
    assert_eq!(
        fused, legacy,
        "hash-join grouped fused HAVING fast path must match legacy"
    );
    assert_eq!(fused.len(), 2);
}

#[test]
fn derived_union_all_implicit_aggregate_streams_without_materialising_union() {
    let setup = [
        "CREATE TABLE a (id INT NOT NULL PRIMARY KEY, v INT)",
        "CREATE TABLE b (id INT NOT NULL PRIMARY KEY, v INT)",
        "INSERT INTO a VALUES (1,10),(2,NULL)",
        "INSERT INTO b VALUES (1,3),(2,7)",
    ];
    let sql = "\
        SELECT SUM(amount) AS total, COUNT(*) AS n, COUNT(amount) AS nn \
        FROM ( \
            SELECT v * 2 AS amount FROM a \
            UNION ALL \
            SELECT v + 1 AS amount FROM b \
        ) u";

    let fast = rows_with_env(&[("MYDB_DERIVED_UNION_AGG_PUSHDOWN", None)], &setup, sql);
    let old = rows_with_env(
        &[("MYDB_DERIVED_UNION_AGG_PUSHDOWN", Some("off"))],
        &setup,
        sql,
    );
    assert_eq!(
        fast, old,
        "derived UNION aggregate fast path must match old path"
    );
    assert_eq!(fast.len(), 1);
    let numeric = |v: &Value| match v {
        Value::Int(x) => *x as i128,
        Value::UInt(x) => *x as i128,
        Value::Double(x) => *x as i128,
        Value::DecimalCompact(m, _) => *m as i128,
        other => panic!("non-numeric aggregate: {other:?}"),
    };
    assert_eq!(numeric(&fast[0][0]), 32);
    assert_eq!(numeric(&fast[0][1]), 4);
    assert_eq!(numeric(&fast[0][2]), 3);
}

#[test]
fn derived_union_all_grouped_aggregate_streams_and_preserves_alias_mapping() {
    let setup = [
        "CREATE TABLE a (id INT NOT NULL PRIMARY KEY, g INT NOT NULL, v INT NOT NULL)",
        "CREATE TABLE b (id INT NOT NULL PRIMARY KEY, g INT NOT NULL, v INT NOT NULL)",
        "INSERT INTO a VALUES (1,1,10),(2,1,20),(3,2,30)",
        "INSERT INTO b VALUES (1,1,5),(2,3,7)",
    ];
    let sql = "\
        SELECT g, SUM(x) AS sx \
        FROM ( \
            SELECT g, v AS x FROM a \
            UNION ALL \
            SELECT g, v + 10 AS x FROM b \
        ) u \
        GROUP BY g \
        ORDER BY g";

    let fast = rows_with_env(&[("MYDB_DERIVED_UNION_AGG_PUSHDOWN", None)], &setup, sql);
    let old = rows_with_env(
        &[("MYDB_DERIVED_UNION_AGG_PUSHDOWN", Some("off"))],
        &setup,
        sql,
    );
    assert_eq!(fast, old);
    let values: Vec<(i64, i128)> = fast
        .iter()
        .map(|row| {
            let g = match &row[0] {
                Value::Int(x) => *x,
                other => panic!("non-int group: {other:?}"),
            };
            let sx = match &row[1] {
                Value::Int(x) => *x as i128,
                Value::DecimalCompact(m, _) => *m as i128,
                other => panic!("non-numeric sum: {other:?}"),
            };
            (g, sx)
        })
        .collect();
    assert_eq!(values, vec![(1, 45), (2, 30), (3, 17)]);
}

#[test]
fn derived_union_all_empty_implicit_aggregate_matches_materialised_path() {
    let setup = [
        "CREATE TABLE a (id INT NOT NULL PRIMARY KEY, v INT)",
        "CREATE TABLE b (id INT NOT NULL PRIMARY KEY, v INT)",
    ];
    let sql = "\
        SELECT SUM(v) AS total, COUNT(*) AS n \
        FROM ( \
            SELECT v FROM a \
            UNION ALL \
            SELECT v FROM b \
        ) u";

    let fast = rows_with_env(&[("MYDB_DERIVED_UNION_AGG_PUSHDOWN", None)], &setup, sql);
    let old = rows_with_env(
        &[("MYDB_DERIVED_UNION_AGG_PUSHDOWN", Some("off"))],
        &setup,
        sql,
    );
    assert_eq!(fast, old);
    assert!(matches!(&fast[0][0], Value::Null));
    assert!(matches!(&fast[0][1], Value::Int(0)));
}

#[test]
fn derived_select_grouped_aggregate_streaming_fallback_matches_materialised_path() {
    let setup = [
        "CREATE TABLE src (id INT NOT NULL PRIMARY KEY, g INT NOT NULL, v INT NOT NULL)",
        "INSERT INTO src VALUES (1,1,10),(2,1,20),(3,2,30),(4,2,40)",
    ];
    let sql = "\
        SELECT g, SUM(v) AS sv \
        FROM (SELECT g, v FROM src WHERE id > 1) d \
        GROUP BY g \
        ORDER BY g";

    let streaming = rows_with_env(&[("MYDB_DERIVED_STREAM_AGG", None)], &setup, sql);
    let materialised = rows_with_env(&[("MYDB_DERIVED_STREAM_AGG", Some("off"))], &setup, sql);
    assert_eq!(streaming, materialised);
    assert_eq!(streaming.len(), 2);
}

#[test]
fn multi_use_cte_only_in_in_subqueries_can_defer_materialisation() {
    let setup = [
        "CREATE TABLE fact_a (id INT NOT NULL PRIMARY KEY, k INT NOT NULL)",
        "CREATE TABLE fact_b (id INT NOT NULL PRIMARY KEY, k INT NOT NULL)",
        "CREATE TABLE dim (k INT NOT NULL PRIMARY KEY, keep_flag INT NOT NULL)",
        "INSERT INTO fact_a VALUES (1,1),(2,2),(3,3)",
        "INSERT INTO fact_b VALUES (1,2),(2,3),(3,4)",
        "INSERT INTO dim VALUES (1,1),(2,0),(3,1),(4,1)",
    ];
    let sql = "\
        WITH kept AS (SELECT k FROM dim WHERE keep_flag = 1) \
        SELECT COUNT(*) FROM ( \
            SELECT id FROM fact_a WHERE k IN (SELECT k FROM kept) \
            UNION ALL \
            SELECT id FROM fact_b WHERE k IN (SELECT k FROM kept) \
        ) u";

    let deferred = rows_with_env(&[("MYDB_CTE_INLINE_IN_SUBQUERY_MULTI", None)], &setup, sql);
    let materialised = rows_with_env(
        &[("MYDB_CTE_INLINE_IN_SUBQUERY_MULTI", Some("off"))],
        &setup,
        sql,
    );
    assert_eq!(deferred, materialised);
    assert!(matches!(&deferred[0][0], Value::Int(4)));
}

#[test]
fn independent_ctes_parallel_materialize_matches_serial() {
    let setup = [
        "CREATE TABLE a (id INT NOT NULL PRIMARY KEY, k INT NOT NULL, v INT NOT NULL)",
        "CREATE TABLE b (id INT NOT NULL PRIMARY KEY, k INT NOT NULL, v INT NOT NULL)",
        "INSERT INTO a VALUES (1,1,10),(2,1,20),(3,2,30)",
        "INSERT INTO b VALUES (1,1,7),(2,2,11),(3,2,13)",
    ];
    let sql = "\
        WITH ax AS (SELECT k, SUM(v) s FROM a GROUP BY k), \
             bx AS (SELECT k, COUNT(*) c FROM b GROUP BY k) \
        SELECT SUM(s + c) FROM ax, bx WHERE ax.k = bx.k";

    let parallel = rows_with_env(&[("MYDB_CTE_PARALLEL_MATERIALIZE", None)], &setup, sql);
    let serial = rows_with_env(
        &[("MYDB_CTE_PARALLEL_MATERIALIZE", Some("off"))],
        &setup,
        sql,
    );
    assert_eq!(parallel, serial);
    assert!(matches!(&parallel[0][0], Value::DecimalCompact(63, 0)));
}

/// O3: a no-GROUP-BY aggregate over a `UNION ALL` of ≥2 branches runs the
/// branches in parallel and merges per-branch partial accumulators. The result
/// must be byte-identical to the serial path (`MYDB_UNION_AGG_PARALLEL=off`)
/// across several aggregate kinds (SUM/COUNT/AVG/MIN/MAX) and a branch-local
/// WHERE filter.
#[test]
fn union_all_implicit_aggregate_parallel_matches_serial() {
    let setup = [
        "CREATE TABLE a (id INT NOT NULL PRIMARY KEY, q INT NOT NULL, p INT NOT NULL)",
        "CREATE TABLE b (id INT NOT NULL PRIMARY KEY, q INT NOT NULL, p INT NOT NULL)",
        "CREATE TABLE c (id INT NOT NULL PRIMARY KEY, q INT NOT NULL, p INT NOT NULL)",
        "INSERT INTO a VALUES (1,2,10),(2,3,20),(3,5,30),(4,1,40)",
        "INSERT INTO b VALUES (1,4,15),(2,6,25),(3,2,35)",
        "INSERT INTO c VALUES (1,7,5),(2,8,45),(3,9,55),(4,1,1),(5,2,2)",
    ];
    let sql = "\
        SELECT SUM(q*p) s, COUNT(*) n, AVG(q*p) av, MIN(q) mn, MAX(p) mx FROM ( \
            SELECT q, p FROM a WHERE p > 5 \
            UNION ALL \
            SELECT q, p FROM b WHERE p > 5 \
            UNION ALL \
            SELECT q, p FROM c WHERE p > 5 \
        ) u";
    let parallel = rows_with_env(&[("MYDB_UNION_AGG_PARALLEL", None)], &setup, sql);
    let serial = rows_with_env(&[("MYDB_UNION_AGG_PARALLEL", Some("off"))], &setup, sql);
    assert_eq!(parallel, serial, "parallel union-agg must match serial");
    // Sanity on the aggregate (all rows have p>5 except c's (7,5),(1,1),(2,2)):
    // a: 2*10+3*20+5*30+1*40 = 270; b: 4*15+6*25+2*35 = 280; c: 8*45+9*55 = 855
    // → SUM(q*p) = 1405, COUNT(*) = 4+3+2 = 9.
    let unwrap_int = |v: &Value| match v {
        Value::Int(x) => *x as i128,
        Value::UInt(x) => *x as i128,
        Value::DecimalCompact(m, s) if *s == 0 => *m as i128,
        other => panic!("non-int: {other:?}"),
    };
    assert_eq!(unwrap_int(&parallel[0][0]), 1405);
    assert_eq!(unwrap_int(&parallel[0][1]), 9);
}

/// O1: a multi-reference CTE referenced only via IN-subqueries is inlined into
/// each consumer only when its estimated base-table scan volume stays under
/// `MYDB_CTE_INLINE_MAX_SCAN_ROWS`. Above the threshold it must stay in the
/// materialise-once set (the q14 `cross_items` case) instead of being
/// recomputed once per consumer.
#[test]
fn in_subquery_multi_cte_inline_is_gated_by_scan_volume() {
    use crate::parser::{CteDef, Statement};

    fn pipeline(
        engine: &StorageQueryEngine,
        sql: &str,
        max_scan_rows: &str,
    ) -> (Vec<CteDef>, Statement) {
        let stmt = crate::parser::parse_statement(sql).expect("parse WITH");
        let Statement::WithCte(w) = stmt else {
            panic!("expected WITH");
        };
        let mut ctes = w.ctes;
        let mut body = *w.body;
        with_test_env(
            &[("MYDB_CTE_INLINE_MAX_SCAN_ROWS", Some(max_scan_rows))],
            || {
                crate::engine::executor::cte::apply_pre_materialization_rewrites(
                    engine.database(),
                    "test",
                    &mut ctes,
                    &mut body,
                    true,
                );
            },
        );
        (ctes, body)
    }

    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    for s in [
        "CREATE TABLE fact_a (id INT NOT NULL PRIMARY KEY, k INT NOT NULL)",
        "CREATE TABLE fact_b (id INT NOT NULL PRIMARY KEY, k INT NOT NULL)",
        "CREATE TABLE dim (k INT NOT NULL PRIMARY KEY, keep_flag INT NOT NULL)",
        "INSERT INTO fact_a VALUES (1,1),(2,2),(3,3)",
        "INSERT INTO fact_b VALUES (1,2),(2,3),(3,4)",
        "INSERT INTO dim VALUES (1,1),(2,0),(3,1),(4,1),(5,1),(6,0)",
        "ANALYZE TABLE dim",
    ] {
        engine.execute(s).unwrap();
    }
    // dim now has an analysed row_count of 6.
    assert_eq!(
        engine.database().catalog_row_count_estimate("test", "dim"),
        Some(6)
    );

    let sql = "\
        WITH kept AS (SELECT k FROM dim WHERE keep_flag = 1) \
        SELECT COUNT(*) FROM ( \
            SELECT id FROM fact_a WHERE k IN (SELECT k FROM kept) \
            UNION ALL \
            SELECT id FROM fact_b WHERE k IN (SELECT k FROM kept) \
        ) u";

    // Threshold below dim's row_count (6 > 1): keep materialised-once.
    let (gated, _) = pipeline(&engine, sql, "1");
    assert!(
        gated.iter().any(|c| c.name.eq_ignore_ascii_case("kept")),
        "above-threshold CTE must NOT be inlined (stays in materialise set)"
    );
    // Threshold above dim's row_count (6 <= 100): cheap body, inline allowed.
    let (inlined, _) = pipeline(&engine, sql, "100");
    assert!(
        !inlined.iter().any(|c| c.name.eq_ignore_ascii_case("kept")),
        "below-threshold CTE should be inlined into its IN-subquery consumers"
    );
}

#[test]
fn grouped_cte_pruning_removes_fd_redundant_group_key() {
    use crate::parser::{Expr, SelectItem, Statement};

    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    for sql in [
        "CREATE TABLE item (i_item_sk INT NOT NULL PRIMARY KEY, i_item_desc VARCHAR(64))",
        "CREATE TABLE store_sales (ss_item_sk INT NOT NULL, ss_sold_date_sk INT NOT NULL)",
    ] {
        engine.execute(sql).unwrap();
    }

    let stmt = crate::parser::parse_statement(
        "WITH frequent AS (\
            SELECT substr(i_item_desc,1,30) AS itemdesc, \
                   i_item_sk AS item_sk, \
                   ss_sold_date_sk AS solddate, \
                   count(*) AS cnt \
            FROM store_sales, item \
            WHERE ss_item_sk = i_item_sk \
            GROUP BY substr(i_item_desc,1,30), i_item_sk, ss_sold_date_sk \
            HAVING count(*) > 4) \
         SELECT count(*) FROM store_sales \
         WHERE ss_item_sk IN (SELECT item_sk FROM frequent)",
    )
    .expect("parse WITH");
    let Statement::WithCte(w) = stmt else {
        panic!("expected WITH");
    };
    let mut ctes = w.ctes;
    let mut body = *w.body;
    crate::engine::executor::cte::apply_pre_materialization_rewrites(
        engine.database(),
        "test",
        &mut ctes,
        &mut body,
        true,
    );

    let frequent = ctes
        .iter()
        .find(|cte| cte.name.eq_ignore_ascii_case("frequent"))
        .expect("frequent CTE retained");
    let crate::parser::QueryExpr::Select(select) = &frequent.query else {
        panic!("expected select CTE body");
    };
    let names: Vec<String> = select
        .projection
        .iter()
        .map(|item| match item {
            SelectItem::Expr {
                alias: Some(alias), ..
            } => alias.clone(),
            other => panic!("unexpected projection item: {other:?}"),
        })
        .collect();
    assert_eq!(names, vec!["item_sk"]);
    assert_eq!(
        select.group_by.len(),
        2,
        "substr(desc) is determined by item primary key and is not consumed"
    );
    assert!(
        select.group_by.iter().all(|expr| !matches!(
            expr,
            Expr::Function { name, .. } if name.eq_ignore_ascii_case("substr")
        )),
        "redundant string group key should be removed: {:?}",
        select.group_by
    );
}

#[test]
fn count_only_grouped_cte_keeps_cheapest_representative_column() {
    use crate::parser::{Expr, SelectItem, Statement};

    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    for sql in [
        "CREATE TABLE item (i_item_sk INT NOT NULL PRIMARY KEY, i_item_desc VARCHAR(64))",
        "CREATE TABLE store_sales (ss_item_sk INT NOT NULL, ss_sold_date_sk INT NOT NULL)",
    ] {
        engine.execute(sql).unwrap();
    }

    let stmt = crate::parser::parse_statement(
        "WITH frequent AS (\
            SELECT substr(i_item_desc,1,30) AS itemdesc, \
                   i_item_sk AS item_sk, \
                   ss_sold_date_sk AS solddate, \
                   count(*) AS cnt \
            FROM store_sales, item \
            WHERE ss_item_sk = i_item_sk \
            GROUP BY substr(i_item_desc,1,30), i_item_sk, ss_sold_date_sk \
            HAVING count(*) > 4) \
         SELECT count(*) FROM frequent",
    )
    .expect("parse WITH");
    let Statement::WithCte(w) = stmt else {
        panic!("expected WITH");
    };
    let mut ctes = w.ctes;
    let mut body = *w.body;
    crate::engine::executor::cte::apply_pre_materialization_rewrites(
        engine.database(),
        "test",
        &mut ctes,
        &mut body,
        false,
    );

    let frequent = ctes
        .iter()
        .find(|cte| cte.name.eq_ignore_ascii_case("frequent"))
        .expect("frequent CTE retained");
    let crate::parser::QueryExpr::Select(select) = &frequent.query else {
        panic!("expected select CTE body");
    };
    let names: Vec<String> = select
        .projection
        .iter()
        .map(|item| match item {
            SelectItem::Expr {
                alias: Some(alias), ..
            } => alias.clone(),
            other => panic!("unexpected projection item: {other:?}"),
        })
        .collect();
    assert_eq!(names, vec!["item_sk"]);
    assert_eq!(select.group_by.len(), 2);
    assert!(
        select.group_by.iter().all(|expr| !matches!(
            expr,
            Expr::Function { name, .. } if name.eq_ignore_ascii_case("substr")
        )),
        "COUNT(*) consumer should not retain the redundant substr group key: {:?}",
        select.group_by
    );
}

#[test]
fn q23_shape_prunes_fd_key_before_multi_in_subquery_inline() {
    use crate::parser::{Expr, SelectItem, Statement};

    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    for sql in [
        "CREATE TABLE item (i_item_sk INT NOT NULL PRIMARY KEY, i_item_desc VARCHAR(64))",
        "CREATE TABLE date_dim (d_date_sk INT NOT NULL PRIMARY KEY, d_date DATE, d_year INT, d_moy INT)",
        "CREATE TABLE store_sales (ss_item_sk INT NOT NULL, ss_sold_date_sk INT NOT NULL, ss_customer_sk INT, ss_quantity INT, ss_sales_price DECIMAL(7,2))",
        "CREATE TABLE customer (c_customer_sk INT NOT NULL PRIMARY KEY)",
        "CREATE TABLE catalog_sales (cs_item_sk INT, cs_bill_customer_sk INT, cs_sold_date_sk INT, cs_quantity INT, cs_list_price DECIMAL(7,2))",
        "CREATE TABLE web_sales (ws_item_sk INT, ws_bill_customer_sk INT, ws_sold_date_sk INT, ws_quantity INT, ws_list_price DECIMAL(7,2))",
    ] {
        engine.execute(sql).unwrap();
    }

    let stmt = crate::parser::parse_statement(
        "WITH frequent AS (\
            SELECT substr(i_item_desc,1,30) AS itemdesc, \
                   i_item_sk AS item_sk, \
                   d_date AS solddate, \
                   count(*) AS cnt \
            FROM store_sales, date_dim, item \
            WHERE ss_sold_date_sk = d_date_sk \
              AND ss_item_sk = i_item_sk \
              AND d_year IN (2000,2001,2002,2003) \
            GROUP BY substr(i_item_desc,1,30), i_item_sk, d_date \
            HAVING count(*) > 4), \
         max_sales AS (\
            SELECT max(csales) AS mx \
            FROM (SELECT c_customer_sk, sum(ss_quantity * ss_sales_price) AS csales \
                  FROM store_sales, customer, date_dim \
                  WHERE ss_customer_sk = c_customer_sk \
                    AND ss_sold_date_sk = d_date_sk \
                  GROUP BY c_customer_sk) s), \
         best_customer AS (\
            SELECT c_customer_sk \
            FROM store_sales, customer \
            WHERE ss_customer_sk = c_customer_sk \
            GROUP BY c_customer_sk \
            HAVING sum(ss_quantity * ss_sales_price) > (SELECT * FROM max_sales)) \
         SELECT sum(sales) FROM (\
            SELECT cs_quantity * cs_list_price AS sales \
            FROM catalog_sales, date_dim \
            WHERE d_year = 2000 \
              AND cs_sold_date_sk = d_date_sk \
              AND cs_item_sk IN (SELECT item_sk FROM frequent) \
              AND cs_bill_customer_sk IN (SELECT c_customer_sk FROM best_customer) \
            UNION ALL \
            SELECT ws_quantity * ws_list_price AS sales \
            FROM web_sales, date_dim \
            WHERE d_year = 2000 \
              AND ws_sold_date_sk = d_date_sk \
              AND ws_item_sk IN (SELECT item_sk FROM frequent) \
              AND ws_bill_customer_sk IN (SELECT c_customer_sk FROM best_customer)) u",
    )
    .expect("parse q23-shaped WITH");
    let Statement::WithCte(w) = stmt else {
        panic!("expected WITH");
    };
    let mut ctes = w.ctes;
    let mut body = *w.body;
    crate::engine::executor::cte::apply_pre_materialization_rewrites(
        engine.database(),
        "test",
        &mut ctes,
        &mut body,
        true,
    );

    let frequent = ctes
        .iter()
        .find(|cte| cte.name.eq_ignore_ascii_case("frequent"))
        .expect("unanalysed multi-use fact CTE should stay materialised once");
    let crate::parser::QueryExpr::Select(select) = &frequent.query else {
        panic!("expected select CTE body");
    };
    let names: Vec<String> = select
        .projection
        .iter()
        .map(|item| match item {
            SelectItem::Expr {
                alias: Some(alias), ..
            } => alias.clone(),
            other => panic!("unexpected projection item: {other:?}"),
        })
        .collect();
    assert_eq!(names, vec!["item_sk"]);
    assert_eq!(select.group_by.len(), 2);
    assert!(
        select.group_by.iter().all(|expr| !matches!(
            expr,
            Expr::Function { name, .. } if name.eq_ignore_ascii_case("substr")
        )),
        "q23-shaped frequent CTE should not retain the redundant substr group key: {:?}",
        select.group_by
    );
    assert!(
        select.selection.as_ref().is_some_and(|expr| {
            let mut found = false;
            fn walk(expr: &Expr, found: &mut bool) {
                match expr {
                    Expr::InSubquery { .. } => *found = true,
                    Expr::Binary { left, right, .. } => {
                        walk(left, found);
                        walk(right, found);
                    }
                    Expr::Unary { expr, .. }
                    | Expr::IsNull(expr)
                    | Expr::IsNotNull(expr)
                    | Expr::GroupKeyRef { expr, .. } => walk(expr, found),
                    Expr::Function { args, .. } => {
                        for arg in args {
                            walk(arg, found);
                        }
                    }
                    Expr::InList { expr, list, .. } => {
                        walk(expr, found);
                        for item in list {
                            walk(item, found);
                        }
                    }
                    Expr::Between {
                        expr, low, high, ..
                    } => {
                        walk(expr, found);
                        walk(low, found);
                        walk(high, found);
                    }
                    Expr::Like { expr, pattern, .. } => {
                        walk(expr, found);
                        walk(pattern, found);
                    }
                    Expr::Tuple(items) => {
                        for item in items {
                            walk(item, found);
                        }
                    }
                    _ => {}
                }
            }
            walk(expr, &mut found);
            found
        }),
        "q23-shaped frequent CTE should receive an IN-subquery consumer domain: {:?}",
        select.selection
    );
}

#[test]
fn q23_second_statement_derived_union_order_columns_resolve() {
    let setup = [
        "CREATE TABLE item (i_item_sk INT NOT NULL PRIMARY KEY, i_item_desc VARCHAR(64))",
        "CREATE TABLE date_dim (d_date_sk INT NOT NULL PRIMARY KEY, d_date DATE, d_year INT, d_moy INT)",
        "CREATE TABLE customer (c_customer_sk INT NOT NULL PRIMARY KEY, c_last_name VARCHAR(32), c_first_name VARCHAR(32))",
        "CREATE TABLE store_sales (ss_item_sk INT NOT NULL, ss_sold_date_sk INT NOT NULL, ss_customer_sk INT, ss_quantity INT, ss_sales_price DECIMAL(7,2))",
        "CREATE TABLE catalog_sales (cs_item_sk INT, cs_bill_customer_sk INT, cs_sold_date_sk INT, cs_quantity INT, cs_list_price DECIMAL(7,2))",
        "CREATE TABLE web_sales (ws_item_sk INT, ws_bill_customer_sk INT, ws_sold_date_sk INT, ws_quantity INT, ws_list_price DECIMAL(7,2))",
        "INSERT INTO item VALUES (1,'alpha long description'),(2,'beta long description')",
        "INSERT INTO date_dim VALUES (1,'2000-01-01',2000,1),(2,'2001-01-01',2001,1)",
        "INSERT INTO customer VALUES (10,'Able','Ann'),(20,'Baker','Bob')",
        "INSERT INTO store_sales VALUES (1,1,10,10,10.00),(1,1,10,9,9.00),(2,2,20,1,1.00)",
        "INSERT INTO catalog_sales VALUES (1,10,1,2,5.00),(2,20,1,1,7.00)",
        "INSERT INTO web_sales VALUES (1,10,1,3,4.00),(2,20,1,1,8.00)",
    ];
    let sql = "\
        WITH frequent AS (\
            SELECT substr(i_item_desc,1,30) AS itemdesc, i_item_sk AS item_sk, d_date AS solddate, count(*) AS cnt \
            FROM store_sales, date_dim, item \
            WHERE ss_sold_date_sk = d_date_sk \
              AND ss_item_sk = i_item_sk \
              AND d_year IN (2000,2001) \
            GROUP BY substr(i_item_desc,1,30), i_item_sk, d_date \
            HAVING count(*) > 0), \
        max_sales AS (\
            SELECT max(csales) AS mx \
            FROM (SELECT c_customer_sk, sum(ss_quantity * ss_sales_price) AS csales \
                  FROM store_sales, customer, date_dim \
                  WHERE ss_customer_sk = c_customer_sk \
                    AND ss_sold_date_sk = d_date_sk \
                    AND d_year IN (2000,2001) \
                  GROUP BY c_customer_sk) s), \
        best_customer AS (\
            SELECT c_customer_sk \
            FROM store_sales, customer \
            WHERE ss_customer_sk = c_customer_sk \
            GROUP BY c_customer_sk \
            HAVING sum(ss_quantity * ss_sales_price) >= 0 * (SELECT * FROM max_sales)) \
        SELECT c_last_name, c_first_name, sales \
        FROM (\
            SELECT c_last_name, c_first_name, sum(cs_quantity * cs_list_price) AS sales \
            FROM catalog_sales, customer, date_dim \
            WHERE d_year = 2000 \
              AND d_moy = 1 \
              AND cs_sold_date_sk = d_date_sk \
              AND cs_item_sk IN (SELECT item_sk FROM frequent) \
              AND cs_bill_customer_sk IN (SELECT c_customer_sk FROM best_customer) \
              AND cs_bill_customer_sk = c_customer_sk \
            GROUP BY c_last_name, c_first_name \
            UNION ALL \
            SELECT c_last_name, c_first_name, sum(ws_quantity * ws_list_price) AS sales \
            FROM web_sales, customer, date_dim \
            WHERE d_year = 2000 \
              AND d_moy = 1 \
              AND ws_sold_date_sk = d_date_sk \
              AND ws_item_sk IN (SELECT item_sk FROM frequent) \
              AND ws_bill_customer_sk IN (SELECT c_customer_sk FROM best_customer) \
              AND ws_bill_customer_sk = c_customer_sk \
            GROUP BY c_last_name, c_first_name) u \
        ORDER BY c_last_name, c_first_name, sales \
        LIMIT 100";

    let rows = rows_with_env(
        &[("MYDB_CTE_IN_SUBQUERY_DOMAIN_PUSHDOWN", None)],
        &setup,
        sql,
    );
    let old = rows_with_env(
        &[("MYDB_CTE_IN_SUBQUERY_DOMAIN_PUSHDOWN", Some("off"))],
        &setup,
        sql,
    );
    assert_eq!(rows, old);
    assert_eq!(rows.len(), 4);
}

#[test]
fn session_cte_result_cache_is_permanently_disabled() {
    with_test_env(&[("MYDB_CTE_RESULT_CACHE", None)], || {
        let dir = tempfile::tempdir().unwrap();
        let engine = open_test_engine(dir.path());
        let mut session = SessionContext::default();
        engine
            .execute_with_session(
                &mut session,
                "CREATE TABLE src (id INT NOT NULL PRIMARY KEY, v INT NOT NULL)",
            )
            .unwrap();
        engine
            .execute_with_session(&mut session, "INSERT INTO src VALUES (1,10),(2,20)")
            .unwrap();
        let sql = "WITH c AS (SELECT id, v FROM src) \
                   SELECT SUM(c1.v + c2.v) FROM c c1, c c2 WHERE c1.id = c2.id";
        let before = super::super::cte::CTE_RESULT_CACHE_HITS.with(|hits| hits.get());
        engine.execute_with_session(&mut session, sql).unwrap();
        engine.execute_with_session(&mut session, sql).unwrap();
        let after = super::super::cte::CTE_RESULT_CACHE_HITS.with(|hits| hits.get());
        assert_eq!(after, before, "a second statement must re-execute the CTE");
        assert!(session.cte_result_cache.lock().unwrap().entries.is_empty());
    });
}

#[test]
fn session_cte_result_cache_invalidates_after_commit_frontier_moves() {
    with_test_env(&[("MYDB_CTE_RESULT_CACHE", Some("on"))], || {
        let dir = tempfile::tempdir().unwrap();
        let engine = open_test_engine(dir.path());
        let mut session = SessionContext::default();
        engine
            .execute_with_session(
                &mut session,
                "CREATE TABLE src (id INT NOT NULL PRIMARY KEY, v INT NOT NULL)",
            )
            .unwrap();
        engine
            .execute_with_session(&mut session, "INSERT INTO src VALUES (1,10)")
            .unwrap();
        let sql = "WITH c AS (SELECT SUM(v) AS sv FROM src) SELECT sv FROM c";
        let first = match engine.execute_with_session(&mut session, sql).unwrap() {
            QueryResponse::ResultSet(rs) => rs.rows,
            other => panic!("expected result set: {other:?}"),
        };
        engine
            .execute_with_session(&mut session, "INSERT INTO src VALUES (2,5)")
            .unwrap();
        let second = match engine.execute_with_session(&mut session, sql).unwrap() {
            QueryResponse::ResultSet(rs) => rs.rows,
            other => panic!("expected result set: {other:?}"),
        };
        let as_i128 = |v: &Value| match v {
            Value::Int(x) => *x as i128,
            Value::UInt(x) => *x as i128,
            Value::DecimalCompact(m, scale) if *scale == 0 => *m as i128,
            other => panic!("unexpected SUM value: {other:?}"),
        };
        assert_eq!(as_i128(&first[0][0]), 10);
        assert_eq!(as_i128(&second[0][0]), 15);
    });
}

#[test]
fn session_cte_result_cache_uses_default_byte_budget_for_large_row_counts() {
    with_test_env(
        &[
            // Keep the gate explicit so the byte budget is the only admission
            // variable exercised by this test.
            ("MYDB_CTE_RESULT_CACHE", Some("on")),
            ("MYDB_CTE_RESULT_CACHE_ROWS", None),
            ("MYDB_CTE_RESULT_CACHE_MB", None),
        ],
        || {
            let dir = tempfile::tempdir().unwrap();
            let engine = open_test_engine(dir.path());
            let mut session = SessionContext::default();
            engine
                .execute_with_session(
                    &mut session,
                    "CREATE TABLE src (id INT NOT NULL PRIMARY KEY, v INT NOT NULL)",
                )
                .unwrap();
            engine
                .execute_with_session(
                    &mut session,
                    "INSERT INTO src \
                     SELECT n, n FROM generate_series(0, 10000) AS gs(n)",
                )
                .unwrap();

            let sql = "\
                WITH c AS (SELECT id, v FROM src) \
                SELECT COUNT(*) FROM c c1 JOIN c c2 ON c1.id = c2.id";
            match engine.execute_with_session(&mut session, sql).unwrap() {
                QueryResponse::ResultSet(rs) => assert_eq!(rs.rows.len(), 1),
                other => panic!("expected result set: {other:?}"),
            }
            let cache = session.cte_result_cache.lock().unwrap();
            assert!(
                cache.entries.is_empty(),
                "legacy cache settings must not retain CTE results"
            );
            assert_eq!(cache.bytes, 0);
        },
    );
}

#[test]
fn cte_spill_with_multi_column_string_values_round_trips() {
    // Strings + ints + null padding to exercise the value-row codec
    // (`ValueRowWriter`/`ValueRowReader`) on the spill path. The
    // assertion compares spill vs in-memory directly so any codec drift
    // (decimal rescale, string copy bytes) shows up immediately.
    let setup = [
        "CREATE TABLE src (id INT NOT NULL PRIMARY KEY, s VARCHAR(32), n INT)",
        "INSERT INTO src VALUES \
           (1,'alpha',100), (2,'beta',NULL), (3,NULL,300), \
           (4,'delta',400), (5,'epsilon',500), (6,'zeta',600)",
    ];
    let sql = "\
        WITH renamed AS (SELECT id, s, n FROM src) \
        SELECT id, s, n FROM renamed ORDER BY id";

    let in_mem = rows_with_env(&[("MYDB_CTE_SPILL_THRESHOLD_MB", None)], &setup, sql);
    let spilled = rows_with_env(&[("MYDB_CTE_SPILL_THRESHOLD_MB", Some("0"))], &setup, sql);
    assert_eq!(in_mem, spilled);
    assert_eq!(in_mem.len(), 6);
    // Spot-check rows 2 and 3 (NULL on different columns).
    assert!(matches!(&in_mem[1][2], Value::Null));
    assert!(matches!(&in_mem[2][1], Value::Null));
}
