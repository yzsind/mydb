//! Non-covering index range scan and heap-fetch optimization regressions.
//!
//! Cover four implemented changes:
//! P0-2: read_plan_table_ids_range_chunked_limited uses ICP for LIMIT queries
//! through the same IndexConditionPushdown::try_new entry as unlimited scans.
//! P0-3: icp_safe_column admits BINARY/VARBINARY/BLOB and binary-collation
//! CHAR/VARCHAR/TEXT into its safe set.
//! P1-2: read_row_columns_many_with_handle fallback combines one heap.read(),
//! page-batched decoding, and second-stage visibility checks.
//! P1-1: default MYDB_MRR_SORT expands RID windows according to work-area grants,
//! reads by (page_id, slot), then restores index order; explicit off is the oracle.
//!
//! These paths occur in OLTP point/range lookups and INLJ inners of chained analytical joins;
//! differential comparisons expose any missed shape.

use super::*;
use crate::engine::api::ResultSet;
use std::collections::HashSet;

fn explain_text(engine: &StorageQueryEngine, sql: &str) -> String {
    let QueryResponse::ResultSet(explain) = engine.execute(sql).unwrap() else {
        panic!("expected explain result set");
    };
    explain
        .rows
        .iter()
        .flat_map(|row| row.iter())
        .map(|value| match value {
            Value::String(s) => s.clone(),
            other => format!("{other:?}"),
        })
        .collect::<Vec<_>>()
        .join("\n")
}

fn collect_int_set(result: &ResultSet, col: usize) -> HashSet<i64> {
    result
        .rows
        .iter()
        .map(|row| match &row[col] {
            Value::Int(v) => *v,
            other => panic!("expected Value::Int, got {other:?}"),
        })
        .collect()
}

fn row_int(row: &[Value], col: usize) -> i64 {
    match &row[col] {
        Value::Int(v) => *v,
        other => panic!("expected Int at col {col}, got {other:?}"),
    }
}

/// P0-2: non-covering LIMIT IndexRangeScan uses ICP, exposed as index_condition
/// in EXPLAIN. o_orderdate is the range-bound column, so the pushed residual is
/// o_totalprice > 1000, with early LIMIT termination.
#[test]
fn limit_path_index_range_scan_pushes_residual_predicate() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());

    engine
        .execute(
            "CREATE TABLE orders (\
             o_orderkey INT PRIMARY KEY, \
             o_orderdate DATE NOT NULL, \
             o_totalprice INT NOT NULL, \
             o_comment VARCHAR(32), \
             INDEX idx_orders_orderdate(o_orderdate))",
        )
        .unwrap();
    let mut sql = String::from("INSERT INTO orders VALUES ");
    for id in 1..=500 {
        if id > 1 {
            sql.push(',');
        }
        let day = (id % 28) + 1;
        let date = format!("1994-03-{day:02}");
        let price = if id % 2 == 0 { 5_000 } else { 100 };
        sql.push_str(&format!("({id}, '{date}', {price}, 'o {id}')"));
    }
    engine.execute(&sql).unwrap();
    engine.execute("ANALYZE TABLE orders").unwrap();

    let QueryResponse::ResultSet(result) = engine
        .execute(
            "SELECT o_orderkey FROM orders \
             WHERE o_orderdate < '1995-03-15' AND o_totalprice > 1000 \
             LIMIT 50",
        )
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(result.rows.len(), 50);
    for row in &result.rows {
        let id = row_int(row, 0);
        assert_eq!(id % 2, 0, "LIMIT path must respect totalprice residual");
    }

    // ICP is explicitly visible in EXPLAIN JSON.
    let text = explain_text(
        &engine,
        "EXPLAIN FORMAT=JSON \
         SELECT o_orderkey FROM orders \
         WHERE o_orderdate < '1995-03-15' AND o_totalprice > 1000 \
         LIMIT 50",
    );
    assert!(
        text.contains("index_condition"),
        "EXPLAIN JSON expected to carry index_condition on the LIMIT path; got: {text}"
    );
}

/// P0-3: residuals on VARBINARY range scans can use ICP; binary types compare
/// bytewise, with identical IndexKey and heap semantics.
#[test]
fn icp_safe_for_varbinary_column() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());

    engine
        .execute(
            "CREATE TABLE events (\
             id INT PRIMARY KEY, \
             bucket INT NOT NULL, \
             payload VARBINARY(64) NOT NULL, \
             INDEX idx_events_bucket(bucket))",
        )
        .unwrap();
    let mut sql = String::from("INSERT INTO events VALUES ");
    for id in 1..=300 {
        if id > 1 {
            sql.push(',');
        }
        let bucket = (id % 5) + 1;
        let payload = if id % 3 == 0 { "alpha" } else { "beta" };
        sql.push_str(&format!("({id}, {bucket}, '{payload}')"));
    }
    engine.execute(&sql).unwrap();
    engine.execute("ANALYZE TABLE events").unwrap();

    let QueryResponse::ResultSet(result) = engine
        .execute(
            "SELECT id FROM events \
             WHERE bucket BETWEEN 1 AND 5 AND payload = 'alpha'",
        )
        .unwrap()
    else {
        panic!("expected result set");
    };
    let alpha_ids = collect_int_set(&result, 0);
    let expected: HashSet<i64> = (1..=300i64).filter(|i| i % 3 == 0).collect();
    assert_eq!(alpha_ids, expected);

    let text = explain_text(
        &engine,
        "EXPLAIN FORMAT=JSON SELECT id FROM events \
         WHERE bucket BETWEEN 1 AND 5 AND payload = 'alpha'",
    );
    assert!(
        text.contains("index_condition"),
        "VARBINARY equality should reach ICP; got: {text}"
    );
}

/// P0-3: ICP should also activate on utf8mb4_bin VARCHAR columns.
/// For info.binary collations, compare_strings_for_info uses as_bytes().cmp,
/// matching KeyPart byte order.
#[test]
fn icp_safe_for_binary_collated_varchar() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());

    engine
        .execute(
            "CREATE TABLE items (\
             id INT PRIMARY KEY, \
             tier INT NOT NULL, \
             code VARCHAR(16) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL, \
             INDEX idx_items_tier(tier))",
        )
        .unwrap();
    let mut sql = String::from("INSERT INTO items VALUES ");
    for id in 1..=200 {
        if id > 1 {
            sql.push(',');
        }
        let tier = (id % 4) + 1;
        let code = if id % 2 == 0 { "GOLD" } else { "silver" };
        sql.push_str(&format!("({id}, {tier}, '{code}')"));
    }
    engine.execute(&sql).unwrap();
    engine.execute("ANALYZE TABLE items").unwrap();

    let QueryResponse::ResultSet(result) = engine
        .execute("SELECT id FROM items WHERE tier BETWEEN 1 AND 4 AND code = 'GOLD'")
        .unwrap()
    else {
        panic!("expected result set");
    };
    let ids = collect_int_set(&result, 0);
    let expected: HashSet<i64> = (1..=200i64).filter(|i| i % 2 == 0).collect();
    assert_eq!(ids, expected);

    let text = explain_text(
        &engine,
        "EXPLAIN FORMAT=JSON SELECT id FROM items \
         WHERE tier BETWEEN 1 AND 4 AND code = 'GOLD'",
    );
    assert!(
        text.contains("index_condition"),
        "utf8mb4_bin VARCHAR should reach ICP; got: {text}"
    );
}

/// P0-3 counterexample: do not push ICP under case-insensitive collation,
/// which would regress 'GOLD' = 'gold'. This test protects
/// the conservative icp_safe_column boundary.
#[test]
fn icp_skips_case_insensitive_collated_varchar() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());

    engine
        .execute(
            "CREATE TABLE items_ci (\
             id INT PRIMARY KEY, \
             tier INT NOT NULL, \
             code VARCHAR(16) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NOT NULL, \
             INDEX idx_items_ci_tier(tier))",
        )
        .unwrap();
    let mut sql = String::from("INSERT INTO items_ci VALUES ");
    for id in 1..=100 {
        if id > 1 {
            sql.push(',');
        }
        let tier = (id % 3) + 1;
        let code = if id % 2 == 0 { "GOLD" } else { "gold" };
        sql.push_str(&format!("({id}, {tier}, '{code}')"));
    }
    engine.execute(&sql).unwrap();
    engine.execute("ANALYZE TABLE items_ci").unwrap();

    // Under CI collation, 'GOLD' = 'gold' must match all 100 rows.
    let QueryResponse::ResultSet(result) = engine
        .execute("SELECT id FROM items_ci WHERE tier BETWEEN 1 AND 3 AND code = 'gold'")
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(result.rows.len(), 100);
}

/// P1-1: default MRR must match explicit-off row values and order for forward
/// ranges and reverse ORDER BY LIMIT, preventing page-read order from leaking into output.
#[test]
fn mrr_default_on_preserves_forward_and_reverse_limit_order() {
    let _guard = test_env_lock();
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute(
            "CREATE TABLE t (\
             id INT PRIMARY KEY, \
             bucket INT NOT NULL, \
             v INT NOT NULL, \
             INDEX idx_bucket(bucket))",
        )
        .unwrap();
    let mut sql = String::from("INSERT INTO t VALUES ");
    for id in 1..=400 {
        if id > 1 {
            sql.push(',');
        }
        let bucket = (id * 7919) % 50;
        sql.push_str(&format!("({id}, {bucket}, {})", id * 3));
    }
    engine.execute(&sql).unwrap();
    engine.execute("ANALYZE TABLE t").unwrap();

    let sql = "SELECT id FROM t FORCE INDEX (idx_bucket) WHERE bucket BETWEEN 5 AND 25";
    let off = with_test_env(&[("MYDB_MRR_SORT", Some("0"))], || {
        match engine.execute(sql).unwrap() {
            QueryResponse::ResultSet(result) => result.rows,
            other => panic!("expected result set, got {other:?}"),
        }
    });
    let on = with_test_env(&[("MYDB_MRR_SORT", None)], || {
        match engine.execute(sql).unwrap() {
            QueryResponse::ResultSet(result) => result.rows,
            other => panic!("expected result set, got {other:?}"),
        }
    });
    assert_eq!(on, off, "MRR must restore the exact index output order");

    let reverse_sql = "SELECT id FROM t FORCE INDEX (idx_bucket) \
                       WHERE bucket BETWEEN 5 AND 25 ORDER BY bucket DESC LIMIT 50";
    let reverse_off = with_test_env(&[("MYDB_MRR_SORT", Some("off"))], || {
        match engine.execute(reverse_sql).unwrap() {
            QueryResponse::ResultSet(result) => result.rows,
            other => panic!("expected result set, got {other:?}"),
        }
    });
    let reverse_on = with_test_env(&[("MYDB_MRR_SORT", None)], || {
        match engine.execute(reverse_sql).unwrap() {
            QueryResponse::ResultSet(result) => result.rows,
            other => panic!("expected result set, got {other:?}"),
        }
    });
    assert_eq!(reverse_on, reverse_off);

    let got: HashSet<i64> = on
        .iter()
        .map(|row| match row.first() {
            Some(Value::Int(v)) => *v,
            other => panic!("expected int row, got {other:?}"),
        })
        .collect();
    let expected: HashSet<i64> = (1..=400i64)
        .filter(|id| {
            let bucket = (*id as i64 * 7919) % 50;
            (5..=25).contains(&bucket)
        })
        .collect();
    assert_eq!(got, expected);
}

/// P1-2: non-covering scan with INT equality eligible for ICP and VARCHAR LIKE ineligible
/// for ICP exercises fallback plus second-stage visibility. Compare against a manually
/// rebuilt expected set; missing or extra rows immediately fail assert_eq!.
#[test]
fn non_covering_index_range_scan_with_residual_filter_full_coverage() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());

    engine
        .execute(
            "CREATE TABLE wide (\
             id INT PRIMARY KEY, \
             k INT NOT NULL, \
             a INT NOT NULL, \
             b INT NOT NULL, \
             c VARCHAR(32), \
             INDEX idx_wide_k(k))",
        )
        .unwrap();
    let mut sql = String::from("INSERT INTO wide VALUES ");
    for id in 1..=600 {
        if id > 1 {
            sql.push(',');
        }
        let k = (id * 31 + 7) % 100;
        sql.push_str(&format!("({id}, {k}, {}, {}, 'p{id}')", id % 7, id % 11));
    }
    engine.execute(&sql).unwrap();

    let direct_before = crate::engine::executor::vec::index_scan::DIRECT_HEAP_BATCH_HITS
        .load(std::sync::atomic::Ordering::Relaxed);
    let QueryResponse::ResultSet(result) = engine
        .execute(
            "SELECT id, a, b FROM wide \
             WHERE k BETWEEN 10 AND 80 AND a = 3 AND c LIKE '%5'",
        )
        .unwrap()
    else {
        panic!("expected result set");
    };
    let direct_after = crate::engine::executor::vec::index_scan::DIRECT_HEAP_BATCH_HITS
        .load(std::sync::atomic::Ordering::Relaxed);
    assert!(
        direct_after > direct_before,
        "resident non-covering scan must decode heap payloads directly into typed batches"
    );
    let got: HashSet<(i64, i64, i64)> = result
        .rows
        .iter()
        .map(|row| (row_int(row, 0), row_int(row, 1), row_int(row, 2)))
        .collect();
    let expected: HashSet<(i64, i64, i64)> = (1..=600i64)
        .filter_map(|id| {
            let k = (id * 31 + 7) % 100;
            let a = id % 7;
            let last = format!("p{id}");
            if (10..=80).contains(&k) && a == 3 && last.ends_with('5') {
                Some((id, a, id % 11))
            } else {
                None
            }
        })
        .collect();
    assert_eq!(got, expected);

    // Equality lookup takes the non-covering index -> heap batch source used
    // by prepared OLTP point reads. Keep a heap-only conjunct in the residual
    // so the test proves that path stays in the compiled batch expression
    // program instead of interpreting the AST for every returned row.
    let before = crate::engine::expr_program::expr_program_stats();
    let QueryResponse::ResultSet(point) = engine
        .execute(
            "SELECT id, a, b FROM wide FORCE INDEX (idx_wide_k) \
             WHERE k = 38 AND a = 3 AND c LIKE '%5'",
        )
        .unwrap()
    else {
        panic!("expected result set");
    };
    for row in &point.rows {
        let id = row_int(row, 0);
        assert_eq!((id * 31 + 7) % 100, 38);
        assert_eq!(row_int(row, 1), 3);
        assert!(format!("p{id}").ends_with('5'));
    }
    let after = crate::engine::expr_program::expr_program_stats();
    assert_eq!(
        after.legacy_fallback_rows, before.legacy_fallback_rows,
        "compilable non-covering residual must not fall back to legacy evaluation"
    );
    let analyzed = explain_text(
        &engine,
        "EXPLAIN ANALYZE FORMAT=TREE \
         SELECT id, a, b FROM wide FORCE INDEX (idx_wide_k) \
         WHERE k = 38 AND a = 3 AND c LIKE '%5'",
    );
    assert!(
        analyzed.contains("expression_mode=decoded") && !analyzed.contains("scalar_rows="),
        "non-covering heap residual did not use the native batch program:\n{analyzed}"
    );
}

/// Rows written under an older layout cannot use the active-layout typed
/// decoder. The complete RID window must fall back to the row oracle so an
/// INSTANT-added column receives its catalog default instead of NULL or a
/// shifted value.
#[test]
fn direct_non_covering_batch_falls_back_for_old_layout_defaults() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute(
            "CREATE TABLE instant_rows (\
             id INT PRIMARY KEY, k INT NOT NULL, v VARCHAR(16), \
             INDEX idx_instant_k(k))",
        )
        .unwrap();
    let mut insert = String::from("INSERT INTO instant_rows VALUES ");
    for id in 1..=12 {
        if id != 1 {
            insert.push_str(", ");
        }
        if id == 2 {
            insert.push_str("(2, 20, NULL)");
        } else {
            insert.push_str(&format!("({id}, {}, 'v{id}')", id * 10));
        }
    }
    engine.execute(&insert).unwrap();
    engine
        .execute("ALTER TABLE instant_rows ADD COLUMN added INT NOT NULL DEFAULT 7")
        .unwrap();

    let fallback_before = crate::engine::executor::vec::index_scan::DIRECT_HEAP_BATCH_FALLBACKS
        .load(std::sync::atomic::Ordering::Relaxed);
    let QueryResponse::ResultSet(result) = engine
        .execute(
            "SELECT id, v, added FROM instant_rows FORCE INDEX (idx_instant_k) \
             WHERE k BETWEEN 1 AND 999 ORDER BY k",
        )
        .unwrap()
    else {
        panic!("expected result set");
    };
    let fallback_after = crate::engine::executor::vec::index_scan::DIRECT_HEAP_BATCH_FALLBACKS
        .load(std::sync::atomic::Ordering::Relaxed);
    assert!(
        fallback_after > fallback_before,
        "old-layout rows must reject the typed payload shortcut"
    );
    assert_eq!(result.rows.len(), 12);
    assert_eq!(row_int(&result.rows[0], 0), 1);
    assert_eq!(result.rows[0][1], Value::String("v1".to_string()));
    assert_eq!(result.rows[1][1], Value::Null);
    assert_eq!(result.rows[2][1], Value::String("v3".to_string()));
    for row in &result.rows {
        assert_eq!(row_int(row, 2), 7);
    }
}

/// An exact single-column hash-build filter immediately after an index
/// equality prefix must become complete-key probes. This is the generic form
/// of TPC-C Stock-Level: reading every stock row in one warehouse and then
/// rejecting almost all of them is avoidable once the order-line item set is
/// known exactly.
#[test]
fn exact_runtime_filter_turns_index_prefix_scan_into_multi_lookup() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute(
            "CREATE TABLE stock_rf (\
             s_w_id INT NOT NULL, s_i_id INT NOT NULL, s_quantity INT NOT NULL, \
             pad VARCHAR(32), PRIMARY KEY (s_w_id, s_i_id))",
        )
        .unwrap();
    engine
        .execute(
            "CREATE TABLE order_line_rf (\
             ol_w_id INT NOT NULL, ol_d_id INT NOT NULL, ol_o_id INT NOT NULL, \
             ol_number INT NOT NULL, ol_i_id INT NOT NULL, \
             PRIMARY KEY (ol_w_id, ol_d_id, ol_o_id, ol_number))",
        )
        .unwrap();

    let mut stock = String::from("INSERT INTO stock_rf VALUES ");
    for item in 1..=1_000 {
        if item != 1 {
            stock.push(',');
        }
        stock.push_str(&format!("(1,{item},{},'p{item}')", item % 100));
    }
    engine.execute(&stock).unwrap();
    let mut lines = String::from("INSERT INTO order_line_rf VALUES ");
    for item in 1..=20 {
        if item != 1 {
            lines.push(',');
        }
        lines.push_str(&format!("(1,1,{},1,{item})", item + 100));
    }
    engine.execute(&lines).unwrap();
    engine.execute("ANALYZE TABLE stock_rf").unwrap();
    engine.execute("ANALYZE TABLE order_line_rf").unwrap();

    let before = crate::engine::executor::vec::index_scan::RUNTIME_EXACT_MULTI_LOOKUP_HITS
        .load(std::sync::atomic::Ordering::Relaxed);
    let QueryResponse::ResultSet(result) = engine
        .execute(
            "SELECT /*+ ORDERED HASH_JOIN(stock_rf, order_line_rf) */ \
             COUNT(DISTINCT s_i_id) \
             FROM stock_rf FORCE INDEX (PRIMARY), order_line_rf FORCE INDEX (PRIMARY) \
             WHERE s_w_id = 1 AND ol_w_id = 1 AND ol_d_id = 1 \
               AND ol_o_id < 121 AND ol_o_id >= 101 \
               AND s_i_id = ol_i_id AND s_quantity < 15",
        )
        .unwrap()
    else {
        panic!("expected result set");
    };
    let after = crate::engine::executor::vec::index_scan::RUNTIME_EXACT_MULTI_LOOKUP_HITS
        .load(std::sync::atomic::Ordering::Relaxed);
    assert_eq!(result.rows, vec![vec![Value::Int(14)]]);
    assert!(
        after > before,
        "exact build keys should replace the stock prefix scan with multi-lookups"
    );
}

/// A live writer keeps range predicates residual: heap rows can come from
/// older visible versions whose indexed keys differ from the newest entry.
/// The compact projection and streaming SUM must both evaluate that residual.
#[test]
fn range_residual_compact_projection_and_sum_preserve_mvcc() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine.execute("CREATE TABLE residual_rows (id INT PRIMARY KEY, amount DECIMAL(12,2), label VARCHAR(32), spare VARCHAR(32))").unwrap();
    engine.execute("INSERT INTO residual_rows VALUES (1,1.25,'Alpha','x'),(2,2.50,'beta','x'),(3,NULL,'beta','x'),(4,9.00,'outside','x')").unwrap();
    let mut writer = SessionContext::default();
    engine.execute_with_session(&mut writer, "BEGIN").unwrap();
    engine
        .execute_with_session(
            &mut writer,
            "UPDATE residual_rows SET id=20,amount=100 WHERE id=2",
        )
        .unwrap();
    engine
        .execute_with_session(
            &mut writer,
            "UPDATE residual_rows SET amount=200,label='uncommitted' WHERE id=1",
        )
        .unwrap();
    assert!(engine.database().has_active_write_txns());
    let rows = |sql: &str| match engine.execute(sql).unwrap().materialize().unwrap() {
        QueryResponse::ResultSet(result) => result.rows,
        QueryResponse::SharedResultSet(result) => result.rows,
        other => panic!("expected rows, got {other:?}"),
    };
    assert_eq!(
        rows("SELECT label FROM residual_rows WHERE id BETWEEN 1 AND 3 ORDER BY label"),
        vec![
            vec![Value::String("Alpha".into())],
            vec![Value::String("beta".into())],
            vec![Value::String("beta".into())]
        ]
    );
    assert_eq!(
        rows("SELECT DISTINCT label FROM residual_rows WHERE id BETWEEN 1 AND 3 ORDER BY label"),
        vec![
            vec![Value::String("Alpha".into())],
            vec![Value::String("beta".into())]
        ]
    );
    let total = rows("SELECT SUM(amount) FROM residual_rows WHERE id BETWEEN 1 AND 3");
    assert_eq!(total[0][0].to_display_string(), "3.75");
    assert_eq!(
        rows("SELECT SUM(amount) FROM residual_rows WHERE id BETWEEN 1 AND 3 AND amount > 20"),
        vec![vec![Value::Null]]
    );
    assert_eq!(
        rows("SELECT SUM(amount) FROM residual_rows WHERE id BETWEEN 3 AND 3"),
        vec![vec![Value::Null]]
    );
    engine
        .execute_with_session(&mut writer, "ROLLBACK")
        .unwrap();
    assert_eq!(rows("SELECT label FROM residual_rows WHERE id BETWEEN 1 AND 3 AND amount > 2 ORDER BY label"), vec![vec![Value::String("beta".into())]]);
}

#[test]
fn range_residual_kernels_match_vector_oracle_across_types() {
    use crate::engine::executor::vec::config::{with_vec_mode, VecMode};
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine.execute("CREATE TABLE residual_types (id INT PRIMARY KEY, n BIGINT, u BIGINT UNSIGNED, d DECIMAL(20,4), f DOUBLE, label VARCHAR(32), day DATE)").unwrap();
    engine.execute("INSERT INTO residual_types VALUES (1,-4,18446744073709551610,1.2500,1.5,'Alpha','2025-01-01'),(2,8,2,2.5000,2.25,'beta','2025-01-02'),(3,NULL,NULL,NULL,NULL,NULL,NULL),(4,2,1,-0.1250,-0.5,'BETA','2025-01-04'),(5,100,100,100,100,'other','2025-01-05')").unwrap();
    let mut writer = SessionContext::default();
    engine.execute_with_session(&mut writer, "BEGIN").unwrap();
    engine
        .execute_with_session(&mut writer, "UPDATE residual_types SET n=101 WHERE id=5")
        .unwrap();
    for sql in [
        "SELECT SUM(n) FROM residual_types WHERE id BETWEEN 1 AND 4 AND id<>2",
        "SELECT SUM(u) FROM residual_types WHERE id BETWEEN 1 AND 4",
        "SELECT SUM(d) FROM residual_types WHERE id BETWEEN 1 AND 4 AND (label='beta' OR label IS NULL)",
        "SELECT SUM(f) FROM residual_types WHERE id BETWEEN 1 AND 4 AND day >= '2025-01-01'",
        "SELECT SUM(n) FROM residual_types WHERE id BETWEEN 1 AND 4 AND n IS NULL",
        "SELECT label FROM residual_types WHERE id BETWEEN 1 AND 4 AND id<>2 ORDER BY label DESC",
        "SELECT DISTINCT label FROM residual_types WHERE id BETWEEN 1 AND 3 ORDER BY label",
        "SELECT day,label FROM residual_types WHERE id BETWEEN 1 AND 4 AND n<10 ORDER BY day DESC,label",
        "SELECT label,n,day,u FROM residual_types WHERE id BETWEEN 1 AND 4 ORDER BY id LIMIT 3 OFFSET 1",
        "SELECT day,label,n FROM residual_types WHERE id BETWEEN 1 AND 4 ORDER BY id DESC",
        "SELECT u,n,label,day FROM residual_types WHERE id BETWEEN 1 AND 4 ORDER BY id LIMIT 0",
        "SELECT label FROM residual_types WHERE id BETWEEN 1 AND 4 ORDER BY id LIMIT 2 OFFSET 20",
        "SELECT label,label FROM residual_types WHERE id BETWEEN 1 AND 4 ORDER BY id",
        "SELECT SUM(n) FROM residual_types WHERE id BETWEEN 1 AND 4 AND COALESCE(n,0)>0",
    ] {
        let read = |mode| with_vec_mode(mode, || {
            match engine.execute(sql).unwrap().materialize().unwrap() {
                QueryResponse::ResultSet(result) => result.rows,
                QueryResponse::SharedResultSet(result) => result.rows,
                other => panic!("expected rows, got {other:?}"),
            }
        });
        assert_eq!(read(VecMode::Auto), read(VecMode::On), "{sql}");
    }
    engine
        .execute_with_session(&mut writer, "ROLLBACK")
        .unwrap();
}
