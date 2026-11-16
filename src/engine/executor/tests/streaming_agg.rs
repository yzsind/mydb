//! End-to-end tests for the vectorized streaming HashAggregate.
//!
//! Three concerns covered:
//!  1. Oracle parity — vectorized streaming output matches the legacy
//!     row-based aggregate.
//!  2. Spill — a tight memory budget forces partition-spill and the
//!     query still completes correctly.
//!  3. EXPLAIN ANALYZE — `peak_mem` / `spill_bytes` / `streaming=true`
//!     surface in the output when the vectorized HashAgg ran.

use super::*;
use crate::engine::executor::mem_budget::{set_query_mem_limit_mb, MemBudget};
use crate::engine::executor::vec::config::{with_vec_mode, VecMode};

struct EnvRestore {
    key: &'static str,
    prior: Option<String>,
}

impl Drop for EnvRestore {
    fn drop(&mut self) {
        match self.prior.take() {
            Some(v) => std::env::set_var(self.key, v),
            None => std::env::remove_var(self.key),
        }
    }
}

fn set_env_for_test(key: &'static str, value: &str) -> EnvRestore {
    let prior = std::env::var(key).ok();
    std::env::set_var(key, value);
    EnvRestore { key, prior }
}

fn extract_rows(response: QueryResponse) -> Vec<Vec<Value>> {
    match response {
        QueryResponse::ResultSet(r) => r.rows,
        QueryResponse::StreamingResultSet(mut r) => {
            let mut out = Vec::new();
            while let Ok(Some(row)) = r.cursor.next_row() {
                out.push(row);
            }
            out
        }
        other => panic!("expected result set, got {other:?}"),
    }
}

fn query_rows(engine: &StorageQueryEngine, sql: &str) -> Vec<Vec<Value>> {
    extract_rows(engine.execute(sql).unwrap())
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

fn build_seed_table(engine: &StorageQueryEngine, rows: usize, cardinality: usize) {
    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY, k INT, v INT)")
        .unwrap();
    let mut sql = String::from("INSERT INTO t VALUES ");
    for i in 0..rows {
        if i > 0 {
            sql.push_str(", ");
        }
        let k = i % cardinality;
        let v = (i * 31 + 7) as i64 % 991;
        sql.push_str(&format!("({i}, {k}, {v})"));
    }
    engine.execute(&sql).unwrap();
}

#[test]
fn ordered_key_only_aggregate_matches_hash_for_typed_keys_and_having() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine.execute("CREATE TABLE ordered_keys (id INT PRIMARY KEY, k BIGINT, u BIGINT UNSIGNED, d DECIMAL(20,3), f DOUBLE, s VARCHAR(20) COLLATE utf8mb4_general_ci, b VARBINARY(20), dt DATETIME)").unwrap();
    engine
        .execute(
            "INSERT INTO ordered_keys VALUES
        (1, NULL, NULL, NULL, NULL, NULL, NULL, NULL),
        (2, -5, 18446744073709551615, 1.250, -0.0, 'Alpha', X'0041', '2020-01-02 03:04:05'),
        (3, 7, 0, -9.100, 2.5, 'beta', X'0042', '2021-02-03 04:05:06'),
        (4, -5, 18446744073709551615, 1.250, 0.0, 'alpha', X'0041', '2020-01-02 03:04:05'),
        (5, NULL, NULL, NULL, NULL, NULL, NULL, NULL)",
        )
        .unwrap();
    for keys in ["k", "u", "d", "f", "s", "b", "dt", "k,u,d,f,s,b,dt"] {
        for having in ["", " HAVING k IS NOT NULL"] {
            if !having.is_empty() && !keys.contains('k') {
                continue;
            }
            // Normalize the representative spelling of a case-insensitive
            // key; SQL does not prescribe which equivalent spelling wins.
            let projection = keys
                .split(',')
                .map(|key| if key == "s" { "LOWER(s)" } else { key })
                .collect::<Vec<_>>()
                .join(",");
            let query = format!(
                "SELECT {projection} FROM ordered_keys GROUP BY {keys}{having} ORDER BY {keys}"
            );
            let expected =
                crate::util::env::with_vars(&[("MYDB_AGG_STRATEGY", Some("hash"))], || {
                    query_rows(&engine, &query)
                });
            crate::util::env::with_vars(&[("MYDB_AGG_STRATEGY", Some("sort_stream"))], || {
                assert_eq!(query_rows(&engine, &query), expected, "{query}");
                let plan = explain_text(&engine, &format!("EXPLAIN ANALYZE {query}"));
                assert!(plan.contains("aggregate_mode=ordered_distinct"), "{plan}");
                assert!(!plan.contains("aggregate_mode=grouped_typed"), "{plan}");
            });
        }
    }
    crate::util::env::with_vars(&[("MYDB_AGG_STRATEGY", Some("sort_stream"))], || {
        assert!(query_rows(
            &engine,
            "SELECT k FROM ordered_keys WHERE id < 0 GROUP BY k"
        )
        .is_empty());
        assert!(query_rows(
            &engine,
            "SELECT k FROM ordered_keys GROUP BY k HAVING k > 100"
        )
        .is_empty());
        assert_eq!(
            query_rows(
                &engine,
                "SELECT k FROM ordered_keys GROUP BY k ORDER BY k LIMIT 1 OFFSET 1"
            ),
            vec![vec![Value::Int(-5)]]
        );
    });
}

/// 1. The vectorized streaming HashAggregate's output must equal the
///    legacy row-based aggregate's output for the same query.
#[test]
fn vec_hash_agg_matches_legacy_oracle() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    build_seed_table(&engine, 512, 47);

    let legacy = query_rows(
        &engine,
        "SELECT k, COUNT(*), SUM(v), AVG(v), MIN(v), MAX(v) FROM t GROUP BY k ORDER BY k",
    );
    let vec_rows = with_vec_mode(VecMode::On, || {
        query_rows(
            &engine,
            "SELECT k, COUNT(*), SUM(v), AVG(v), MIN(v), MAX(v) FROM t GROUP BY k ORDER BY k",
        )
    });
    assert_eq!(legacy.len(), 47);
    assert_eq!(legacy, vec_rows, "vec hash-agg must match legacy oracle");
}

/// 2. With a tight memory budget the vec HashAgg must spill its
///    partitions to disk and still return the correct result.
#[test]
fn vec_hash_agg_spills_under_tight_budget_and_stays_correct() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());

    // High-cardinality table to force spilling once the budget is tight.
    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY, k INT, v INT)")
        .unwrap();
    let mut sql = String::from("INSERT INTO t VALUES ");
    for i in 0..800 {
        if i > 0 {
            sql.push_str(", ");
        }
        let k = i; // high cardinality → many partitions populated
        let v = (i * 17 + 3) as i64 % 1009;
        sql.push_str(&format!("({i}, {k}, {v})"));
    }
    engine.execute(&sql).unwrap();

    let legacy = query_rows(
        &engine,
        "SELECT k, COUNT(*), SUM(v), MIN(v), MAX(v) FROM t GROUP BY k ORDER BY k LIMIT 10",
    );

    // The mem_budget_strict test path uses `with_query_budget`. We do
    // the same by installing a tight budget for this query only.
    let vec_rows = with_vec_mode(VecMode::On, || {
        use crate::engine::executor::mem_budget::with_query_budget;
        use std::sync::Arc;
        // 8 KiB budget — small enough to spill at least one partition
        // for 800 distinct keys, but large enough that the in-memory
        // accumulators won't trip the recursion limit.
        let budget = Arc::new(MemBudget::new(8 * 1024));
        with_query_budget(budget, || {
            query_rows(
                &engine,
                "SELECT k, COUNT(*), SUM(v), MIN(v), MAX(v) FROM t GROUP BY k ORDER BY k LIMIT 10",
            )
        })
    });
    assert_eq!(
        legacy, vec_rows,
        "spilled vec hash-agg must match the legacy oracle"
    );
}

#[test]
fn single_column_native_distinct_agg_is_correct() {
    // DISTINCT owns a mergeable native state in the same aggregate plan;
    // it never rewrites into a query-shape-specific nested aggregate.
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE d (id INT PRIMARY KEY, g INT, v INT)")
        .unwrap();
    engine
        .execute(
            "INSERT INTO d VALUES \
             (1, 1, 10), (2, 1, 10), (3, 1, 20), (4, 1, NULL), \
             (5, 2, NULL), (6, 2, 5), (7, 2, 5), (8, 2, 15)",
        )
        .unwrap();

    let rows = query_rows(
        &engine,
        "SELECT g, COUNT(DISTINCT v), SUM(DISTINCT v), AVG(DISTINCT v) \
         FROM d GROUP BY g ORDER BY g",
    );
    let display_rows: Vec<Vec<String>> = rows
        .iter()
        .map(|row| row.iter().map(Value::to_display_string).collect())
        .collect();
    assert_eq!(
        display_rows,
        vec![
            vec!["1", "2", "30", "15.0000"],
            vec!["2", "2", "20", "10.0000"],
        ]
    );

    let plan = explain_text(
        &engine,
        "EXPLAIN FORMAT=TREE \
         SELECT g, COUNT(DISTINCT v), SUM(DISTINCT v), AVG(DISTINCT v) \
         FROM d GROUP BY g",
    );
    // DISTINCT stays in one native aggregate. Some plan shapes use only `Parallel aggregate`
    // and may show zero `HashAggregate` strings.
    assert!(
        plan.matches("HashAggregate").count() <= 1,
        "expected native DISTINCT to produce at most one HashAggregate, got:\n{plan}"
    );
}

#[test]
fn tpch_q16_shape_native_distinct_keeps_group_keys_resolvable() {
    fn body() {
        let dir = tempfile::tempdir().unwrap();
        let engine = open_test_engine(dir.path());
        engine
            .execute(
                "CREATE TABLE supplier (\
                 s_suppkey INT NOT NULL PRIMARY KEY, \
                 s_comment VARCHAR(80) NOT NULL)",
            )
            .unwrap();
        engine
            .execute(
                "CREATE TABLE part (\
                 p_partkey INT NOT NULL PRIMARY KEY, \
                 p_brand VARCHAR(20) NOT NULL, \
                 p_type VARCHAR(40) NOT NULL, \
                 p_size INT NOT NULL)",
            )
            .unwrap();
        engine
            .execute(
                "CREATE TABLE partsupp (\
                 ps_partkey INT NOT NULL, \
                 ps_suppkey INT NOT NULL, \
                 PRIMARY KEY (ps_partkey, ps_suppkey))",
            )
            .unwrap();
        engine
            .execute(
                "INSERT INTO supplier VALUES \
                 (1, 'good'), \
                 (2, 'Customer Complaints abound'), \
                 (3, 'good'), \
                 (4, 'has Customer Complaints'), \
                 (5, 'good')",
            )
            .unwrap();
        engine
            .execute(
                "INSERT INTO part VALUES \
                 (10, 'Brand#11', 'STANDARD BRUSHED TIN', 49), \
                 (20, 'Brand#11', 'STANDARD BRUSHED TIN', 49), \
                 (30, 'Brand#22', 'ECONOMY ANODIZED STEEL', 14), \
                 (40, 'Brand#45', 'STANDARD BRUSHED TIN', 49), \
                 (50, 'Brand#33', 'MEDIUM POLISHED COPPER', 49)",
            )
            .unwrap();
        engine
            .execute(
                "INSERT INTO partsupp VALUES \
                 (10, 1), (10, 2), (10, 3), \
                 (20, 1), (20, 3), (20, 5), \
                 (30, 1), (30, 4), \
                 (40, 1), \
                 (50, 5)",
            )
            .unwrap();

        let query = "\
            SELECT p_brand, p_type, p_size, COUNT(DISTINCT ps_suppkey) AS supplier_cnt \
            FROM partsupp INNER JOIN part ON p_partkey = ps_partkey \
            WHERE p_brand <> 'Brand#45' \
              AND p_type NOT LIKE 'MEDIUM POLISHED%' \
              AND p_size IN (49, 14, 23, 45, 19, 3, 36, 9) \
              AND ps_suppkey NOT IN (\
                  SELECT s_suppkey FROM supplier \
                  WHERE s_comment LIKE '%Customer%Complaints%'\
              ) \
            GROUP BY p_brand, p_type, p_size \
            ORDER BY supplier_cnt DESC, p_brand, p_type, p_size";
        let rows = query_rows(&engine, query);
        let display_rows: Vec<Vec<String>> = rows
            .iter()
            .map(|row| row.iter().map(Value::to_display_string).collect())
            .collect();
        assert_eq!(
            display_rows,
            vec![
                vec!["Brand#11", "STANDARD BRUSHED TIN", "49", "3"],
                vec!["Brand#22", "ECONOMY ANODIZED STEEL", "14", "1"],
            ],
            "native DISTINCT Q16 rows differ"
        );

        let plan = explain_text(&engine, &format!("EXPLAIN FORMAT=TREE {query}"));
        assert!(
            plan.matches("HashAggregate").count() <= 1,
            "expected Q16 native DISTINCT to stay in one HashAggregate, got:\n{plan}"
        );
    }

    body();
}

#[test]
fn row_hash_agg_spills_under_tight_budget_and_matches_oracle() {
    use crate::engine::executor::mem_budget::with_query_budget;
    use std::sync::Arc;

    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE row_spill (id INT PRIMARY KEY, g INT, v INT)")
        .unwrap();
    for chunk_start in (0..1_000).step_by(500) {
        let mut sql = String::from("INSERT INTO row_spill VALUES ");
        for i in chunk_start..chunk_start + 500 {
            if i > chunk_start {
                sql.push_str(", ");
            }
            let v = (i * 37 + 11) % 10_007;
            sql.push_str(&format!("({i}, {i}, {v})"));
        }
        engine.execute(&sql).unwrap();
    }

    let query = "SELECT g + 0, COUNT(*), SUM(v), MIN(v), MAX(v), AVG(v) \
                 FROM row_spill GROUP BY g + 0 ORDER BY g + 0 LIMIT 20";
    let oracle = with_vec_mode(VecMode::Off, || query_rows(&engine, query));

    let budget = Arc::new(MemBudget::new(32 * 1024));
    let spilled = with_vec_mode(VecMode::Off, || {
        with_query_budget(Arc::clone(&budget), || query_rows(&engine, query))
    });

    assert_eq!(oracle, spilled, "spilled row HashAgg must match oracle");
    assert_eq!(budget.used(), 0, "row HashAgg spill must release budget");
}

#[test]
fn compact_partial_hash_agg_spill_matches_generic_rows_oracle() {
    use crate::engine::executor::mem_budget::with_query_budget;
    use std::sync::Arc;

    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE compact_spill (id INT PRIMARY KEY, g INT, v INT)")
        .unwrap();
    for chunk_start in (0..320).step_by(80) {
        let mut sql = String::from("INSERT INTO compact_spill VALUES ");
        for i in chunk_start..chunk_start + 80 {
            if i > chunk_start {
                sql.push_str(", ");
            }
            let value = (i * 37 + 11) % 10_007;
            sql.push_str(&format!("({i}, {i}, {value})"));
        }
        engine.execute(&sql).unwrap();
    }
    let query = "SELECT g, COUNT(*), SUM(v), MIN(v), MAX(v), AVG(v) \
                 FROM compact_spill GROUP BY g HAVING SUM(v) >= 0 ORDER BY g LIMIT 50";

    let oracle = with_vec_mode(VecMode::Off, || query_rows(&engine, query));
    let budget = Arc::new(MemBudget::new(32 * 1024));
    let compact = with_vec_mode(VecMode::On, || {
        with_query_budget(Arc::clone(&budget), || query_rows(&engine, query))
    });
    let analyze = with_vec_mode(VecMode::On, || {
        with_query_budget(Arc::clone(&budget), || explain_analyze_tree(&engine, query))
    });

    assert_eq!(
        oracle, compact,
        "compact partial spill must match generic rows"
    );
    assert!(
        analyze.contains("spill_bytes=") && analyze.contains("aggregate_mode="),
        "tight budget must expose aggregate spill through generic metrics: {analyze}"
    );
    assert_eq!(budget.used(), 0, "compact spill must release query memory");
    assert_eq!(
        budget.spill_bytes(),
        0,
        "compact spill must release temp quota"
    );
}

/// Expression aggregate regression: `SUM(price * (1 - discount))` —
/// the TPC-H Q1 shape — must produce identical output through the
/// parallel vec path as through the row-based fused path.
#[test]
fn parallel_vec_hash_agg_handles_expression_aggregates() {
    let _degree = set_env_for_test("MYDB_PARALLEL_DEGREE", "2");
    let _threshold = set_env_for_test("MYDB_PARALLEL_THRESHOLD_ROWS", "100");
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE items (id INT PRIMARY KEY, flag INT, price INT, disc INT)")
        .unwrap();
    // 1k rows × 8 groups; the lowered test threshold makes the planner pick
    // the parallel fused
    // path and there's enough cardinality for the merge step to do
    // real work.
    let chunk = 500;
    for chunk_start in (0..1_000).step_by(chunk) {
        let mut sql = String::from("INSERT INTO items VALUES ");
        for i in chunk_start..chunk_start + chunk {
            if i > chunk_start {
                sql.push_str(", ");
            }
            let flag = i % 8;
            let price = (i * 11 + 3) % 1000 + 1;
            // disc in [0, 99] — keep `1 - disc/100` non-degenerate.
            let disc = i % 100;
            sql.push_str(&format!("({i}, {flag}, {price}, {disc})"));
        }
        engine.execute(&sql).unwrap();
    }

    let q = "SELECT flag, COUNT(*), SUM(price + disc), SUM(price * (100 - disc)) \
             FROM items GROUP BY flag ORDER BY flag";
    let legacy = query_rows(&engine, q);
    let vec_rows = with_vec_mode(VecMode::On, || query_rows(&engine, q));
    assert_eq!(legacy.len(), 8);
    assert_eq!(
        legacy, vec_rows,
        "expression-aggregate output must match between vec parallel and row-based path"
    );
}

/// ClickBench Q30 family: many exact affine SUMs over one signed integer lane
/// must share a single SUM+COUNT scan instead of materialising one computed
/// column per aggregate. The row executor remains the semantic oracle,
/// including NULL, negative-value and empty-input behavior.
#[test]
fn implicit_affine_sum_family_is_factorized() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE affine_values (id INT PRIMARY KEY, v SMALLINT)")
        .unwrap();
    engine
        .execute(
            "INSERT INTO affine_values VALUES \
             (1, 7), (2, -2), (3, NULL), (4, 32767), (5, -32768)",
        )
        .unwrap();
    let query = "SELECT SUM(v), SUM(v + 1), SUM(2 + v), SUM(v - 3), SUM(10 - v) \
                 FROM affine_values";
    let oracle = with_vec_mode(VecMode::Off, || query_rows(&engine, query));
    let vector = with_vec_mode(VecMode::On, || query_rows(&engine, query));
    assert_eq!(vector, oracle);

    let analyze = with_vec_mode(VecMode::On, || explain_analyze_tree(&engine, query));
    assert!(
        analyze.contains("expression_mode=affine") && analyze.contains("native_expression_rows=5"),
        "affine SUM family was not factorized:\n{analyze}"
    );

    engine
        .execute("CREATE TABLE empty_affine (v SMALLINT)")
        .unwrap();
    let empty_query = "SELECT SUM(v + 1), SUM(5 - v) FROM empty_affine";
    let empty_oracle = with_vec_mode(VecMode::Off, || query_rows(&engine, empty_query));
    let empty_vector = with_vec_mode(VecMode::On, || query_rows(&engine, empty_query));
    assert_eq!(empty_vector, empty_oracle);
    assert_eq!(empty_vector, vec![vec![Value::Null, Value::Null]]);

    // BIGINT's declared range cannot prove `v + 1` safe. It must retain the
    // row-expression oracle, including error 1690 at the positive endpoint.
    engine
        .execute("CREATE TABLE wide_affine (v BIGINT)")
        .unwrap();
    engine
        .execute("INSERT INTO wide_affine VALUES (9223372036854775807)")
        .unwrap();
    let overflow_query = "SELECT SUM(v + 1), SUM(v + 2) FROM wide_affine";
    let oracle_error = with_vec_mode(VecMode::Off, || engine.execute(overflow_query).unwrap_err());
    let vector_error = with_vec_mode(VecMode::On, || engine.execute(overflow_query).unwrap_err());
    assert_eq!(oracle_error.code(), 1690);
    assert_eq!(vector_error.code(), oracle_error.code());
    assert_eq!(vector_error.sql_state(), oracle_error.sql_state());
}

/// A validated constant REGEXP_REPLACE group key is deterministic and cannot
/// raise a row-dependent diagnostic. It should therefore be computed inside
/// the parallel HashAgg workers, while dynamic/erroring regex shapes retain
/// the ordered producer-side oracle.
#[test]
fn parallel_vec_hash_agg_computes_const_regexp_group_key_in_workers() {
    let _degree = set_env_for_test("MYDB_PARALLEL_DEGREE", "2");
    let _threshold = set_env_for_test("MYDB_PARALLEL_THRESHOLD_ROWS", "100");
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE refs (id INT PRIMARY KEY, referer VARCHAR(128))")
        .unwrap();
    for chunk_start in (0..1_000).step_by(250) {
        let mut sql = String::from("INSERT INTO refs VALUES ");
        for i in chunk_start..chunk_start + 250 {
            if i > chunk_start {
                sql.push_str(", ");
            }
            let referer = match i % 4 {
                0 => "https://www.example.com/a",
                1 => "http://other.test/path",
                2 => "plain",
                _ => "",
            };
            sql.push_str(&format!("({i}, '{referer}')"));
        }
        engine.execute(&sql).unwrap();
    }

    let query = "SELECT REGEXP_REPLACE(referer, \
                 '^https?://(?:www\\.)?([^/]+)/.*$', '$1') AS host, \
                 AVG(LENGTH(referer)), COUNT(*), MIN(referer) \
                 FROM refs WHERE referer <> '' GROUP BY host ORDER BY host";
    let oracle = with_vec_mode(VecMode::Off, || query_rows(&engine, query));
    let vector = with_vec_mode(VecMode::On, || query_rows(&engine, query));
    assert_eq!(vector, oracle);

    let analyze = with_vec_mode(VecMode::On, || explain_analyze_tree(&engine, query));
    assert!(
        analyze.contains("expression_mode=compiled")
            && analyze.contains("native_expression_rows=750"),
        "constant regex group key escaped the compiled program:\n{analyze}"
    );
    assert!(
        analyze.contains("workers=2"),
        "constant regex group key did not reach parallel workers:\n{analyze}"
    );
}

/// Parallel vec HashAgg regression: the `Exchange(ParallelSeqScan)` →
/// vec parallel fused path must produce identical output to the
/// row-based fused path for a multi-group simple-aggregate query.
#[test]
fn parallel_vec_hash_agg_matches_legacy_oracle() {
    let _degree = set_env_for_test("MYDB_PARALLEL_DEGREE", "2");
    let _threshold = set_env_for_test("MYDB_PARALLEL_THRESHOLD_ROWS", "100");
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE big (id INT PRIMARY KEY, g INT, v INT)")
        .unwrap();
    // 1k rows × 200 groups; the lowered test threshold makes the planner
    // parallel agg plan (and the page count is > 1 page-partition's
    // worth so `effective_degree_for_pages` returns > 1).
    let chunk = 500;
    for chunk_start in (0..1_000).step_by(chunk) {
        let mut sql = String::from("INSERT INTO big VALUES ");
        for i in chunk_start..chunk_start + chunk {
            if i > chunk_start {
                sql.push_str(", ");
            }
            let g = i % 200;
            let v = (i * 37 + 11) % 991;
            sql.push_str(&format!("({i}, {g}, {v})"));
        }
        engine.execute(&sql).unwrap();
    }

    let q = "SELECT g, COUNT(*), SUM(v), MIN(v), MAX(v), AVG(v) FROM big GROUP BY g ORDER BY g";
    let legacy = query_rows(&engine, q);
    let vec_rows = with_vec_mode(VecMode::On, || query_rows(&engine, q));
    assert_eq!(legacy.len(), 200);
    assert_eq!(
        legacy, vec_rows,
        "parallel vec hash-agg must match legacy fused oracle"
    );
}

/// Phase B/C regression: a chain of `TopN(HashAggregate(HashJoin(...)))`
/// must produce the same answer as the legacy row-based path. This is
/// the TPC-H-shaped pattern (multi-table inner join → GROUP BY → ORDER
/// BY agg → LIMIT) that motivates the streaming refactor.
#[test]
fn fused_join_agg_topn_chain_matches_legacy() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE orders (id INT PRIMARY KEY, cust_id INT, total INT)")
        .unwrap();
    engine
        .execute("CREATE TABLE customers (id INT PRIMARY KEY, name VARCHAR(16))")
        .unwrap();

    // 100 customers, each with 6 orders -> 600 fact rows.
    let mut sql = String::from("INSERT INTO customers VALUES ");
    for i in 0..100 {
        if i > 0 {
            sql.push_str(", ");
        }
        sql.push_str(&format!("({i}, 'c{i}')"));
    }
    engine.execute(&sql).unwrap();
    engine
        .execute(
            "INSERT INTO orders \
             SELECT n, MOD(n, 100), MOD(n * 13 + 7, 997) \
             FROM generate_series(0, 599) AS gs(n)",
        )
        .unwrap();

    let q = "SELECT c.id, c.name, SUM(o.total), COUNT(*) \
             FROM customers c INNER JOIN orders o ON c.id = o.cust_id \
             GROUP BY c.id, c.name \
             ORDER BY SUM(o.total) DESC, c.id ASC \
             LIMIT 10";

    let legacy = query_rows(&engine, q);
    let vec_rows = with_vec_mode(VecMode::On, || query_rows(&engine, q));
    assert_eq!(legacy.len(), 10);
    assert_eq!(
        legacy, vec_rows,
        "fused HashJoin → HashAgg → TopN must match legacy"
    );
}

/// Phase A regression: the fused `TopN(HashAggregate(...))` path must
/// produce the same answer as the legacy row-based path on
/// `GROUP BY ... ORDER BY agg DESC LIMIT k` queries.
#[test]
fn fused_topn_over_hash_agg_matches_legacy() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    build_seed_table(&engine, 600, 200);

    // ORDER BY a group column.
    let q_by_group = "SELECT k, COUNT(*) FROM t GROUP BY k ORDER BY k LIMIT 5";
    // ORDER BY an aggregate expression.
    let q_by_agg = "SELECT k, COUNT(*) FROM t GROUP BY k ORDER BY COUNT(*) DESC, k ASC LIMIT 10";

    for sql in [q_by_group, q_by_agg] {
        let legacy = query_rows(&engine, sql);
        let vec_rows = with_vec_mode(VecMode::On, || query_rows(&engine, sql));
        assert_eq!(legacy, vec_rows, "fused TopN(HashAgg) mismatch for: {sql}");
    }
}

#[test]
fn high_ndv_count_topn_single_scan_matches_full_aggregate() {
    let dir = tempfile::tempdir().unwrap();
    // Keep fixture population out of buffer-pressure waits; query/spill budgets stay unchanged.
    let engine = open_test_engine_with_buffer_pool_pages(dir.path(), 2048);
    engine
        .execute(
            "CREATE TABLE high_ndv (\
                id INT PRIMARY KEY, \
                k BIGINT NOT NULL, \
                ip INT NOT NULL, \
                v SMALLINT NULL, \
                w SMALLINT NULL, \
                f INT NOT NULL)",
        )
        .unwrap();
    insert_fixture_rows(
        &engine,
        "test.high_ndv",
        (0..70_000i64).map(|n| {
            let key = if n < 200 { n % 10 } else { n };
            vec![
                Value::Int(n),
                Value::Int(key),
                Value::Int(key),
                if n % 7 == 0 {
                    Value::Null
                } else {
                    Value::Int(n % 101 - 50)
                },
                if n % 11 == 0 {
                    Value::Null
                } else {
                    Value::Int(n % 211)
                },
                Value::Int(1),
            ]
        }),
    );
    engine.execute("ANALYZE TABLE high_ndv").unwrap();

    // Adding the deterministic group-key tie breaker deliberately keeps the
    // reference on the complete aggregate path. The ten winning groups all
    // have count=20, while every omitted group has count=1.
    let reference = query_rows(
        &engine,
        "SELECT k, ip, COUNT(*), SUM(v), AVG(w) \
         FROM high_ndv GROUP BY k, ip \
         ORDER BY COUNT(*) DESC, k ASC LIMIT 10",
    );
    let before = super::super::select::vec_bridge::DUPLICATE_AWARE_AGG_TOPN_HITS
        .load(std::sync::atomic::Ordering::Relaxed);
    let mut optimized = query_rows(
        &engine,
        "SELECT k, ip, COUNT(*), SUM(v), AVG(w) \
         FROM high_ndv GROUP BY k, ip \
         ORDER BY COUNT(*) DESC LIMIT 10",
    );
    let after = super::super::select::vec_bridge::DUPLICATE_AWARE_AGG_TOPN_HITS
        .load(std::sync::atomic::Ordering::Relaxed);
    assert!(after > before, "high-NDV COUNT Top-N kernel did not run");

    optimized.sort_by_key(|row| row[0].to_display_string().parse::<i64>().unwrap());
    assert_eq!(optimized, reference);
    assert!(optimized
        .iter()
        .all(|row| row[2].to_display_string() == "20"));

    // The resident fingerprint + compact-column representation is larger
    // than this budget, so the same exact kernel must partition its compact
    // records instead of falling back to the full grouping sort.
    let spill_before = super::super::select::vec_bridge::DUPLICATE_AWARE_AGG_TOPN_SPILLS
        .load(std::sync::atomic::Ordering::Relaxed);
    let live_spill_before = crate::engine::executor::spill_quota::global_spilled_bytes();
    let budget = std::sync::Arc::new(crate::engine::executor::mem_budget::MemBudget::new(
        2 * 1024 * 1024,
    ));
    let mut low_memory = crate::engine::executor::mem_budget::with_query_budget(budget, || {
        query_rows(
            &engine,
            "SELECT k, ip, COUNT(*), SUM(v), AVG(w) \
             FROM high_ndv GROUP BY k, ip \
             ORDER BY COUNT(*) DESC LIMIT 10",
        )
    });
    let spill_after = super::super::select::vec_bridge::DUPLICATE_AWARE_AGG_TOPN_SPILLS
        .load(std::sync::atomic::Ordering::Relaxed);
    assert!(
        spill_after > spill_before,
        "low-memory high-NDV COUNT Top-N did not use partitioned replay"
    );
    assert_eq!(
        crate::engine::executor::spill_quota::global_spilled_bytes(),
        live_spill_before,
        "partitioned replay leaked spill quota or temp files"
    );
    low_memory.sort_by_key(|row| row[0].to_display_string().parse::<i64>().unwrap());
    assert_eq!(low_memory, reference);

    // Analyze a 70/30 predicate distribution, then make it fully selective
    // without refreshing statistics. The scan estimate is below the actual
    // 100,000 rows by more than the detector's 10% estimate slack. The 32K
    // vector batches add a bounded two-batch capacity cushion, so this case
    // now remains resident while still checking that stale selectivity cannot
    // truncate the exact result.
    insert_fixture_rows(
        &engine,
        "test.high_ndv",
        (70_000..100_000i64).map(|n| {
            vec![
                Value::Int(n),
                Value::Int(n),
                Value::Int(n),
                Value::Int(n % 101 - 50),
                Value::Int(n % 211),
                Value::Int(0),
            ]
        }),
    );
    engine.execute("ANALYZE TABLE high_ndv").unwrap();
    engine.execute("UPDATE high_ndv SET f = 1").unwrap();
    let underestimated_predicate = "f = 1";
    let underestimated_select = "SELECT k, ip, COUNT(*), SUM(f)";
    let underestimated_reference = query_rows(
        &engine,
        &format!(
            "{underestimated_select} FROM high_ndv \
             WHERE {underestimated_predicate} GROUP BY k, ip \
             ORDER BY COUNT(*) DESC, k ASC LIMIT 10"
        ),
    );
    let mut underestimated = query_rows(
        &engine,
        &format!(
            "{underestimated_select} FROM high_ndv \
             WHERE {underestimated_predicate} GROUP BY k, ip \
             ORDER BY COUNT(*) DESC LIMIT 10"
        ),
    );
    underestimated.sort_by_key(|row| row[0].to_display_string().parse::<i64>().unwrap());
    assert_eq!(underestimated, underestimated_reference);
}
/// 3. EXPLAIN ANALYZE FORMAT=TREE on a query that routes through the
///    vec HashAgg must surface `streaming=true` and a non-zero
///    `peak_mem` on the HashAggregate node. JSON format intentionally
///    elides wrapper operators (Sort/HashAggregate/Limit etc.) — see
///    `select/explain.rs:2575` — so TREE is the right surface to test
///    per-operator EXPLAIN ANALYZE fields.
#[test]
fn explain_analyze_surfaces_streaming_and_peak_mem() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    build_seed_table(&engine, 512, 11);

    let response = with_vec_mode(VecMode::On, || {
        engine
            .execute(
                "EXPLAIN ANALYZE FORMAT=TREE \
                 SELECT k, COUNT(*), SUM(v) FROM t GROUP BY k",
            )
            .unwrap()
    });
    let rows = extract_rows(response);
    let joined: String = rows
        .into_iter()
        .flatten()
        .filter_map(|v| match v {
            Value::String(s) => Some(s),
            _ => None,
        })
        .collect::<Vec<_>>()
        .join("\n");
    assert!(
        joined.contains("streaming=true"),
        "expected streaming=true marker, got: {joined}"
    );
    assert!(
        joined.contains("peak_mem="),
        "expected peak_mem= field, got: {joined}"
    );
    let _ = set_query_mem_limit_mb;
    let _ = set_env_for_test;
}

#[test]
fn auto_mode_routes_eligible_hash_agg_to_vec_path() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    build_seed_table(&engine, 1100, 11);

    let text = with_vec_mode(VecMode::Auto, || {
        explain_text(
            &engine,
            "EXPLAIN ANALYZE FORMAT=TREE \
             SELECT k, COUNT(*), SUM(v) FROM t GROUP BY k",
        )
    });
    assert!(
        text.contains("streaming=true"),
        "expected auto mode to run vec HashAgg, got: {text}"
    );
    assert!(
        text.contains("peak_mem="),
        "expected auto mode to surface vec HashAgg memory, got: {text}"
    );
}

/// TPC-H Q1 shape regression: a multi-column CHAR(1) GROUP BY over a
/// DECIMAL table with both column SUMs (`SUM(qty)`) and expression
/// SUMs (`SUM(price * (1 - disc))`). The post-P3+P4+P7 row engine drives
/// this through `eval_decimal_parts` (i128 magnitude path) +
/// `InlineKeyBuf` (single byte-sequence GROUP BY key) +
/// `accumulate_numeric_state_decimal_parts` (in-place mutate). The
/// vectorized engine takes a different code path entirely. Both must
/// produce bit-identical aggregate output and the same row order.
#[test]
fn tpch_q1_shape_row_path_matches_vec_oracle() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute(
            "CREATE TABLE q1_repro ( \
                id INT PRIMARY KEY, \
                flag CHAR(1) NOT NULL, \
                status CHAR(1) NOT NULL, \
                qty DECIMAL(15,2) NOT NULL, \
                price DECIMAL(15,2) NOT NULL, \
                disc DECIMAL(15,2) NOT NULL, \
                tax DECIMAL(15,2) NOT NULL \
            )",
        )
        .unwrap();
    let flags = ["A", "N", "R"];
    let statuses = ["F", "O"];
    let chunk = 1000;
    let total = 4_000usize;
    for chunk_start in (0..total).step_by(chunk) {
        let mut sql = String::from("INSERT INTO q1_repro VALUES ");
        for i in chunk_start..chunk_start + chunk {
            if i > chunk_start {
                sql.push_str(", ");
            }
            let flag = flags[i % flags.len()];
            let status = statuses[(i / 7) % statuses.len()];
            let qty = (i % 50 + 1) as f64;
            let price = (i * 13 % 9000 + 100) as f64 / 100.0;
            let disc = (i % 11) as f64 / 100.0;
            let tax = (i % 9) as f64 / 100.0;
            sql.push_str(&format!(
                "({i}, '{flag}', '{status}', {qty:.2}, {price:.2}, {disc:.2}, {tax:.2})"
            ));
        }
        engine.execute(&sql).unwrap();
    }

    // Mirror of TPC-H Q1's projection, sans the date filter (irrelevant
    // for this regression). Drives every post-P3/P4/P7 hot path:
    //  - SUM(qty) / SUM(price) : SlotInput::Column borrow path
    //  - SUM(price * (1 - disc)) : NumericDecimalExpr -> eval_decimal_parts
    //  - AVG(qty) : SlotInput::Column + AvgAcc decimal-parts override
    //  - COUNT(*) : SlotInput::CountStar
    //  - GROUP BY flag, status : InlineKeyBuf multi-column key
    let q = "SELECT flag, status, \
             SUM(qty) AS sq, \
             SUM(price) AS sp, \
             SUM(price * (1 - disc)) AS sdp, \
             SUM(price * (1 - disc) * (1 + tax)) AS sc, \
             AVG(qty) AS aq, \
             AVG(price) AS ap, \
             AVG(disc) AS ad, \
             COUNT(*) AS n \
             FROM q1_repro \
             GROUP BY flag, status \
             ORDER BY flag, status";
    let row_rows = query_rows(&engine, q);
    let vec_rows = with_vec_mode(VecMode::On, || query_rows(&engine, q));
    assert!(
        !row_rows.is_empty(),
        "TPC-H Q1 shape produced no rows; check seed data"
    );
    assert_eq!(
        row_rows, vec_rows,
        "TPC-H Q1 shape: row engine must agree with vectorized engine"
    );
}

/// Selective scalar aggregate shaped like TPC-H Q6. Forced vector mode must
/// use the parallel fused aggregate without changing exact DECIMAL semantics;
/// the scan bridge's direct unit test locks down that its predicate-first
/// input is compact and late-materialised.
#[test]
fn selective_scalar_aggregate_matches_row_oracle() {
    let _degree = set_env_for_test("MYDB_PARALLEL_DEGREE", "2");
    let _threshold = set_env_for_test("MYDB_PARALLEL_THRESHOLD_ROWS", "100");
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute(
            "CREATE TABLE q6_repro ( \
                id INT PRIMARY KEY, \
                shipdate DATE NOT NULL, \
                qty DECIMAL(12,2) NOT NULL, \
                price DECIMAL(15,2) NOT NULL, \
                disc DECIMAL(5,2) NOT NULL \
            )",
        )
        .unwrap();
    for chunk_start in (0..320).step_by(80) {
        let mut sql = String::from("INSERT INTO q6_repro VALUES ");
        for i in chunk_start..chunk_start + 80 {
            if i > chunk_start {
                sql.push_str(", ");
            }
            let selected = i % 20 == 0;
            let shipdate = if selected { "1994-06-01" } else { "1993-06-01" };
            let qty = if selected { "12.00" } else { "30.00" };
            let disc = if selected { "0.06" } else { "0.02" };
            sql.push_str(&format!(
                "({i}, '{shipdate}', {qty}, {}.{:02}, {disc})",
                100 + i / 100,
                i % 100,
            ));
        }
        engine.execute(&sql).unwrap();
    }
    engine.execute("ANALYZE TABLE q6_repro").unwrap();

    let query = "SELECT SUM(price * disc) FROM q6_repro \
                 WHERE shipdate >= '1994-01-01' \
                   AND shipdate < '1995-01-01' \
                   AND disc BETWEEN 0.05 AND 0.07 \
                   AND qty < 24";
    let row_rows = with_vec_mode(VecMode::Off, || query_rows(&engine, query));
    let vec_rows = with_vec_mode(VecMode::On, || query_rows(&engine, query));
    let auto_rows = with_vec_mode(VecMode::Auto, || query_rows(&engine, query));
    assert_eq!(
        row_rows, vec_rows,
        "selective scalar vec aggregate diverged"
    );
    assert_eq!(
        row_rows, auto_rows,
        "auto-routed selective scalar aggregate diverged"
    );

    let explain = with_vec_mode(VecMode::On, || {
        explain_text(&engine, &format!("EXPLAIN ANALYZE FORMAT=TREE {query}"))
    });
    assert!(
        explain.contains("execution_mode=vector"),
        "forced vector mode did not reach the fused aggregate: {explain}"
    );
}

/// NULL propagation regression for the post-P3 row-mode i128 decimal fast
/// path. `eval_decimal_parts` must return `EvalDecimal::Null` (not
/// silently treat NULL as 0) for SUM / AVG inputs, so the aggregate
/// output matches the SQL three-valued semantics the legacy path
/// preserved.
#[test]
fn fused_numeric_decimal_expr_handles_nulls_like_legacy() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute(
            "CREATE TABLE nulls_t ( \
                id INT PRIMARY KEY, \
                g CHAR(1) NOT NULL, \
                a DECIMAL(12,2), \
                b DECIMAL(12,2) \
            )",
        )
        .unwrap();
    let mut sql = String::from("INSERT INTO nulls_t VALUES ");
    let total = 3_000;
    for i in 0..total {
        if i > 0 {
            sql.push_str(", ");
        }
        let g = if i % 3 == 0 { "X" } else { "Y" };
        let a = if i % 7 == 0 {
            "NULL".to_string()
        } else {
            format!("{:.2}", (i % 200) as f64 / 4.0)
        };
        let b = if i % 11 == 0 {
            "NULL".to_string()
        } else {
            format!("{:.2}", ((i % 137) + 1) as f64 / 8.0)
        };
        sql.push_str(&format!("({i}, '{g}', {a}, {b})"));
    }
    engine.execute(&sql).unwrap();

    // SUM/AVG of a NULL operand must skip the row (SQL semantics).
    // SUM/AVG of `a + b` must yield NULL only when *both* are NULL?
    // No: `a + b` yields NULL whenever *either* operand is NULL, then
    // SUM skips it.
    let q = "SELECT g, COUNT(*), SUM(a), AVG(a), SUM(a + b), AVG(a * b) \
             FROM nulls_t \
             GROUP BY g \
             ORDER BY g";
    let row_rows = query_rows(&engine, q);
    let vec_rows = with_vec_mode(VecMode::On, || query_rows(&engine, q));
    assert!(!row_rows.is_empty());
    assert_eq!(
        row_rows, vec_rows,
        "post-P3 row engine must match vectorized output under three-valued logic"
    );
}

/// `InlineKeyBuf` (P4) regression: two distinct row encodings that share
/// a prefix (e.g. `["AB", ""]` vs `["A", "B"]`) must hash to different
/// groups. The encoder writes length prefixes for variable-width
/// payloads specifically to make this safe; drop the prefix and the
/// test fails.
#[test]
fn inline_key_distinguishes_concatenation_prefixes() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute(
            "CREATE TABLE k (id INT PRIMARY KEY, c1 VARCHAR(8) NOT NULL, c2 VARCHAR(8) NOT NULL)",
        )
        .unwrap();
    engine
        .execute(
            "INSERT INTO k VALUES \
                (1, 'AB', ''), \
                (2, 'A', 'B'), \
                (3, '', 'AB'), \
                (4, 'AB', ''), \
                (5, 'A', 'B')",
        )
        .unwrap();
    // 3 groups, NOT 1: rows {1,4} share (AB,_), {2,5} share (A,B), {3}
    // alone in (_,AB).
    let rows = query_rows(
        &engine,
        "SELECT c1, c2, COUNT(*) FROM k GROUP BY c1, c2 ORDER BY c1, c2",
    );
    assert_eq!(
        rows.len(),
        3,
        "InlineKeyBuf prefix encoding must distinguish concatenation \
         collisions; got {rows:?}"
    );
}

/// Regression (TPC-H Q18 sf=5): a `GROUP BY <pk-prefix>` whose hash table
/// is *estimated* to exceed the query memory budget must NOT be forced
/// onto a single-threaded `StreamingAggregate` over a full PRIMARY index
/// range scan when the (parallel) seq scan it gives up is cheaper.
///
/// `choose_groupby_index_scan` used to return the ordered index scan
/// unconditionally once `NDV × 256B` crossed 50% of the query mem limit,
/// committing the aggregate to a serial streaming pass (the ordered scan
/// is gated out of parallelization, and there is no parallel streaming
/// aggregate). For Q18's `o_orderkey IN (SELECT l_orderkey FROM lineitem
/// GROUP BY l_orderkey HAVING SUM(l_quantity) > 300)` that flipped sf=5
/// from a 4-worker hash aggregate (~1.6 s) to a single-threaded 30M-row
/// index walk (~16 s) the moment NDV(l_orderkey) passed the threshold.
/// The cost guard now keeps the parallel-friendly seq-scan + hash-agg plan
/// unless the ordered scan is genuinely cheaper.
#[test]
fn groupby_pk_prefix_under_mem_pressure_avoids_serial_streaming_scan() {
    use crate::engine::executor::mem_budget::{query_mem_limit_bytes, set_query_mem_limit_mb};

    // `MYDB_PARALLEL_DISABLE` unset so the cost guard's parallel branch is
    // reachable, matching the real query path.
    crate::util::env::with_vars(
        &[
            ("MYDB_PARALLEL_DISABLE", None),
            // The fixture is intentionally only 8k rows. Scale the parallel
            // admission gates down so it represents Q18's multi-million-row
            // scan and validates the complete parallel-hash-vs-stream cost.
            ("MYDB_PARALLEL_THRESHOLD_ROWS", Some("1")),
            ("MYDB_PARALLEL_MIN_PAGES", Some("1")),
            ("MYDB_PARALLEL_DEGREE", Some("8")),
        ],
        || {
            let prior_parallel_setup = crate::engine::cbo::consts::consts().parallel_setup_cost();
            crate::engine::cbo::consts::set_variable("cbo_parallel_setup_cost", 0.01).unwrap();
            let dir = tempfile::tempdir().unwrap();
            let engine = open_test_engine(dir.path());
            let mut session = SessionContext::default();
            engine
                .execute_with_session(&mut session, "SET optimizer_trace='enabled=on'")
                .unwrap();
            // `v` is not part of the PRIMARY (k) index, so an ordered scan over
            // PRIMARY would still have to heap-fetch `v` for SUM(v): exactly
            // Q18's non-covering `SUM(l_quantity)` shape, which makes the full
            // index walk strictly pricier than a plain seq scan.
            engine
                .execute("CREATE TABLE t (k INT PRIMARY KEY, v INT)")
                .unwrap();
            engine
                .execute(
                    "INSERT INTO t \
                     SELECT n, MOD(n * 37 + 11, 9973) \
                     FROM generate_series(0, 7999) AS gs(n)",
                )
                .unwrap();
            // ANALYZE populates NDV(k) = 8000, which drives the memory trigger.
            engine.execute("ANALYZE TABLE t").unwrap();

            // Keep the budget small so this exercises the low-memory cost
            // comparison. The planner now uses SUM's real transition-state
            // estimate instead of the former fixed 256 B/group trigger.
            let prior_mb = (query_mem_limit_bytes() / (1024 * 1024)).max(1);
            set_query_mem_limit_mb(1).unwrap();

            let plan = extract_rows(
                engine
                    .execute_with_session(
                        &mut session,
                        "EXPLAIN FORMAT=TREE \
                         SELECT k FROM t GROUP BY k HAVING SUM(v) > 1000000",
                    )
                    .unwrap(),
            )
            .into_iter()
            .flatten()
            .map(|value| value.to_display_string())
            .collect::<Vec<_>>()
            .join("\n");
            let trace = extract_rows(
                engine
                    .execute_with_session(
                        &mut session,
                        "SELECT TRACE FROM information_schema.optimizer_trace",
                    )
                    .unwrap(),
            )[0][0]
                .to_display_string();

            // Restore the process-global budget before asserting so a failure
            // can't leak into a sibling test sharing the process.
            set_query_mem_limit_mb(prior_mb).unwrap();
            crate::engine::cbo::consts::set_variable(
                "cbo_parallel_setup_cost",
                prior_parallel_setup,
            )
            .unwrap();

            assert!(
                !plan.contains("Streaming aggregate"),
                "GROUP BY over a non-covering PK prefix must not fall back to a \
             single-threaded streaming aggregate under memory pressure; \
                plan was:\n{plan}\noptimizer trace:\n{trace}"
            );
            assert!(
                !plan.contains("Index range scan"),
                "expected a table scan feeding a hash aggregate, not a full \
                PRIMARY index walk; plan was:\n{plan}\noptimizer trace:\n{trace}"
            );
        },
    );
}

/// Regression (TPC-H Q18 sf=1): even when the heap and secondary index are
/// fully warm, a full non-covering GROUP BY index walk still performs one heap
/// probe per base row and serializes the aggregate. Prefer the parallel
/// seq-scan + hash aggregate path for that shape.
#[test]
fn warm_noncovering_groupby_secondary_index_avoids_serial_streaming_scan() {
    let _guard = super::test_env_lock();
    crate::util::env::with_vars(
        &[
            ("MYDB_PARALLEL_DISABLE", None),
            ("MYDB_PARALLEL_THRESHOLD_ROWS", Some("1")),
            ("MYDB_PARALLEL_MIN_PAGES", Some("1")),
            ("MYDB_PARALLEL_DEGREE", Some("8")),
        ],
        || {
            let prior_parallel_setup = crate::engine::cbo::consts::consts().parallel_setup_cost();
            let prior_effective_cache = crate::engine::cbo::consts::consts().effective_cache_mb();
            crate::engine::cbo::consts::set_variable("cbo_parallel_setup_cost", 0.01).unwrap();
            crate::engine::cbo::consts::set_variable("cbo_effective_cache_mb", 4096.0).unwrap();

            let dir = tempfile::tempdir().unwrap();
            let engine = open_test_engine(dir.path());
            let mut session = SessionContext::default();
            engine
                .execute_with_session(&mut session, "SET optimizer_trace='enabled=on'")
                .unwrap();
            engine
                .execute(
                    "CREATE TABLE t (\
                     id INT PRIMARY KEY, \
                     k INT NOT NULL, \
                     v INT NOT NULL, \
                     INDEX idx_k (k))",
                )
                .unwrap();
            for chunk_start in (0..400).step_by(100) {
                let mut sql = String::from("INSERT INTO t VALUES ");
                for i in chunk_start..chunk_start + 100 {
                    if i > chunk_start {
                        sql.push_str(", ");
                    }
                    sql.push_str(&format!("({i}, {i}, {})", (i % 17) + 1));
                }
                engine.execute(&sql).unwrap();
            }
            engine.execute("ANALYZE TABLE t").unwrap();

            let plan = extract_rows(
                engine
                    .execute_with_session(
                        &mut session,
                        "EXPLAIN FORMAT=TREE \
                         SELECT k FROM t GROUP BY k HAVING SUM(v) > 1000000",
                    )
                    .unwrap(),
            )
            .into_iter()
            .flatten()
            .map(|value| value.to_display_string())
            .collect::<Vec<_>>()
            .join("\n");
            let trace = extract_rows(
                engine
                    .execute_with_session(
                        &mut session,
                        "SELECT TRACE FROM information_schema.optimizer_trace",
                    )
                    .unwrap(),
            )[0][0]
                .to_display_string();

            crate::engine::cbo::consts::set_variable(
                "cbo_parallel_setup_cost",
                prior_parallel_setup,
            )
            .unwrap();
            crate::engine::cbo::consts::set_variable(
                "cbo_effective_cache_mb",
                prior_effective_cache,
            )
            .unwrap();

            assert!(
                !plan.contains("Streaming aggregate"),
                "warm non-covering GROUP BY index walk must not force a serial \
                 streaming aggregate; plan was:\n{plan}\noptimizer trace:\n{trace}"
            );
            assert!(
                !plan.contains("Index range scan on t using idx_k"),
                "expected a table scan feeding hash aggregate, not a full \
                 secondary-index walk; plan was:\n{plan}\noptimizer trace:\n{trace}"
            );
            assert!(
                trace.contains("group_order_index_rejected_noncovering_full_scan"),
                "optimizer trace should record the non-covering full-scan guard: {trace}"
            );
        },
    );
}
