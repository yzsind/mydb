use super::*;
use crate::types::Value;

fn open_engine() -> (tempfile::TempDir, StorageQueryEngine) {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    (dir, engine)
}

#[test]
fn index_min_max_preserves_catalog_result_types_and_flags() {
    let (_dir, engine) = open_engine();
    engine.execute("CREATE TABLE edge_meta(id INT PRIMARY KEY, u BIGINT UNSIGNED, d DECIMAL(20,4), f FLOAT, x DOUBLE, dt DATETIME(6), s VARCHAR(16) COLLATE utf8mb4_bin, b VARBINARY(16), y YEAR, da DATE, tm TIME(6), e ENUM('b','a'), bt BIT(64), w DECIMAL(65,30), KEY ku(u), KEY kd(d), KEY kf(f), KEY kx(x), KEY kt(dt), KEY ks(s), KEY kb(b), KEY ky(y), KEY ka(da), KEY km(tm), KEY ke(e), KEY kbt(bt), KEY kw(w))").unwrap();
    engine.execute("INSERT INTO edge_meta VALUES(1,18446744073709551615,7.2500,1.25,1.25,'2026-10-13 12:34:56.123456','a',X'FF00',2026,'2026-10-13','12:34:56.123456','b',18446744073709551615,12345678901234567890123456789012345.125000000000000000000000000000),(2,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL)").unwrap();
    for state in 0..3 {
        if state == 1 {
            engine.execute("DELETE FROM edge_meta WHERE id=1").unwrap();
        }
        if state == 2 {
            engine.execute("DELETE FROM edge_meta").unwrap();
        }
        for (column, index) in [
            ("u", "ku"),
            ("d", "kd"),
            ("f", "kf"),
            ("x", "kx"),
            ("dt", "kt"),
            ("s", "ks"),
            ("b", "kb"),
            ("id", "PRIMARY"),
            ("y", "ky"),
            ("da", "ka"),
            ("tm", "km"),
            ("e", "ke"),
            ("bt", "kbt"),
            ("w", "kw"),
        ] {
            for aggregate in ["MIN", "MAX"] {
                for filter in ["", " WHERE id>0"] {
                    let fast =
                        format!("SELECT {aggregate}({column}) AS edge FROM edge_meta{filter}");
                    let scan = format!("SELECT {aggregate}({column}) AS edge FROM edge_meta IGNORE INDEX({index}){filter}");
                    let QueryResponse::ResultSet(actual) = engine.execute(&fast).unwrap() else {
                        panic!("result set")
                    };
                    let QueryResponse::ResultSet(expected) = engine.execute(&scan).unwrap() else {
                        panic!("result set")
                    };
                    assert_eq!(
                        actual.columns, expected.columns,
                        "state={state} query={fast}"
                    );
                    assert_eq!(actual.rows, expected.rows, "state={state} query={fast}");
                }
            }
        }
    }
}

fn extra_of(explain: &crate::engine::api::ResultSet, row: usize) -> String {
    match &explain.rows[row][11] {
        Value::String(s) => s.clone(),
        Value::Null => String::new(),
        other => panic!("unexpected extra value: {other:?}"),
    }
}

fn key_of(explain: &crate::engine::api::ResultSet, row: usize) -> String {
    match &explain.rows[row][6] {
        Value::String(s) => s.clone(),
        other => panic!("unexpected key value: {other:?}"),
    }
}

fn table_of(explain: &crate::engine::api::ResultSet, row: usize) -> String {
    match &explain.rows[row][2] {
        Value::String(s) => s.clone(),
        Value::Null => String::new(),
        other => panic!("unexpected table value: {other:?}"),
    }
}

#[test]
fn covering_numeric_prefix_matches_scalar_with_unsigned_nulls_and_dates() {
    use crate::engine::executor::vec::config::{with_vec_mode, VecMode};
    let (_dir, engine) = open_engine();
    engine.execute("CREATE TABLE t (id INT PRIMARY KEY, u BIGINT UNSIGNED NULL, d DATE NULL, x INT, y INT, UNIQUE KEY cover (id,u,d,x,y))").unwrap();
    let rows = (0..2000)
        .map(|id| {
            format!(
                "({id},{},{},1,2)",
                match id % 4 {
                    0 => "NULL",
                    1 => "18446744073709551615",
                    2 => "0",
                    _ => "9223372036854775808",
                },
                if id % 3 == 0 { "NULL" } else { "'2024-02-29'" }
            )
        })
        .collect::<Vec<_>>()
        .join(",");
    engine
        .execute(&format!("INSERT INTO t VALUES {rows}"))
        .unwrap();
    engine.execute("ANALYZE TABLE t").unwrap();
    let queries = [
        "SELECT /*+ PARALLEL(4) */ AVG(u),SUM(u),COUNT(u),COUNT(*) FROM t FORCE INDEX(cover)",
        "SELECT /*+ PARALLEL(4) */ COUNT(DISTINCT u) FROM t FORCE INDEX(cover)",
        "SELECT /*+ PARALLEL(4) */ MIN(d),MAX(d),COUNT(d),COUNT(*) FROM t FORCE INDEX(cover)",
    ];
    let QueryResponse::ResultSet(plan) = engine
        .execute(&format!("EXPLAIN FORMAT=TREE {}", queries[0]))
        .unwrap()
    else {
        panic!("expected plan");
    };
    assert!(
        format!("{:?}", plan.rows)
            .to_ascii_lowercase()
            .contains("index aggregate"),
        "{:?}",
        plan.rows
    );
    let read = |sql: &str, mode| {
        with_vec_mode(mode, || {
            let QueryResponse::ResultSet(result) = engine.execute(sql).unwrap() else {
                panic!("expected rows");
            };
            result.rows
        })
    };
    for mutation in [
        None,
        Some("DELETE FROM t WHERE id % 7 = 0"),
        Some("UPDATE t SET u=17,d='2023-01-02' WHERE id % 5 = 0"),
    ] {
        if let Some(sql) = mutation {
            engine.execute(sql).unwrap();
        }
        for sql in queries {
            assert_eq!(read(sql, VecMode::On), read(sql, VecMode::Off), "{sql}");
        }
    }
    engine.execute("BEGIN").unwrap();
    engine
        .execute("UPDATE t SET u=99,d='2025-01-01' WHERE id % 3 = 0")
        .unwrap();
    for sql in queries {
        assert_eq!(
            read(sql, VecMode::On),
            read(sql, VecMode::Off),
            "pending writer {sql}"
        );
    }
    engine.execute("ROLLBACK").unwrap();
    engine.execute("DELETE FROM t").unwrap();
    for sql in queries {
        assert_eq!(
            read(sql, VecMode::On),
            read(sql, VecMode::Off),
            "empty {sql}"
        );
    }
}

fn find_table_row(explain: &crate::engine::api::ResultSet, table: &str) -> usize {
    explain
        .rows
        .iter()
        .enumerate()
        .find_map(|(idx, _)| (table_of(explain, idx) == table).then_some(idx))
        .unwrap_or_else(|| panic!("expected EXPLAIN row for table {table}: {:?}", explain.rows))
}

#[test]
fn covering_index_marks_using_index_for_unique_lookup() {
    let (_dir, engine) = open_engine();
    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY, k INT NOT NULL, UNIQUE INDEX uk_k (k))")
        .unwrap();
    engine
        .execute("INSERT INTO t VALUES (1, 100), (2, 200)")
        .unwrap();

    let QueryResponse::ResultSet(explain) = engine
        .execute("EXPLAIN SELECT k FROM t WHERE k = 100")
        .unwrap()
    else {
        panic!("expected explain result set");
    };
    assert_eq!(key_of(&explain, 0), "uk_k");
    assert!(
        extra_of(&explain, 0).contains("Using index"),
        "extra should contain Using index, got {:?}",
        extra_of(&explain, 0)
    );
}

#[test]
fn covering_index_lookup_consumes_leaf_visibility_bound() {
    let (_dir, engine) = open_engine();
    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY, k INT NOT NULL, UNIQUE INDEX uk_k (k))")
        .unwrap();
    engine
        .execute("INSERT INTO t VALUES (1, 100), (2, 200)")
        .unwrap();

    engine.db_for_tests().gc_committed_txn_slots();
    let before = engine.db_for_tests().storage_status();
    let QueryResponse::ResultSet(rs) = engine.execute("SELECT k FROM t WHERE k = 100").unwrap()
    else {
        panic!("expected result set");
    };
    let after = engine.db_for_tests().storage_status();

    assert_eq!(rs.rows, vec![vec![Value::Int(100)]]);
    assert!(
        after.index_page_max_trx_shortcut_hits > before.index_page_max_trx_shortcut_hits,
        "covering IndexLookup must propagate the leaf page_max_trx_id to visibility filtering",
    );
}

#[test]
fn covering_index_returns_correct_values_without_heap_fetch() {
    let (_dir, engine) = open_engine();
    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY, k INT NOT NULL, INDEX k_idx (k))")
        .unwrap();
    engine
        .execute("INSERT INTO t VALUES (1, 100), (2, 200), (3, 300)")
        .unwrap();

    let QueryResponse::ResultSet(rs) = engine
        .execute("SELECT k FROM t WHERE k BETWEEN 100 AND 250 ORDER BY k")
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(rs.rows.len(), 2);
    assert_eq!(rs.rows[0][0], Value::Int(100));
    assert_eq!(rs.rows[1][0], Value::Int(200));
}

#[test]
fn covering_char_index_returns_trimmed_char_value() {
    let (_dir, engine) = open_engine();
    engine
        .execute(
            "CREATE TABLE t (\
                id INT PRIMARY KEY, \
                c CHAR(8) COLLATE utf8mb4_bin, \
                INDEX c_idx (c))",
        )
        .unwrap();
    engine
        .execute("INSERT INTO t VALUES (1, 'abcd'), (2, 'wxyz')")
        .unwrap();

    let QueryResponse::ResultSet(rs) = engine
        .execute("SELECT c FROM t WHERE c = 'abcd    '")
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(rs.rows, vec![vec![Value::String("abcd".into())]]);

    let QueryResponse::ResultSet(explain) = engine
        .execute("EXPLAIN SELECT c FROM t WHERE c = 'abcd    '")
        .unwrap()
    else {
        panic!("expected explain");
    };
    assert!(extra_of(&explain, 0).contains("Using index"));
}

#[test]
fn non_covering_select_does_not_set_using_index() {
    let (_dir, engine) = open_engine();
    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY, k INT NOT NULL, v INT, INDEX k_idx (k))")
        .unwrap();
    engine
        .execute("INSERT INTO t VALUES (1, 10, 100), (2, 20, 200)")
        .unwrap();

    let QueryResponse::ResultSet(explain) = engine
        .execute("EXPLAIN SELECT v FROM t WHERE k = 10")
        .unwrap()
    else {
        panic!("expected explain result set");
    };
    let extra = extra_of(&explain, 0);
    assert!(
        !extra.contains("Using index"),
        "non-covering scan should not advertise Using index: extra={extra:?}"
    );
}

#[test]
fn composite_index_subset_is_covering() {
    let (_dir, engine) = open_engine();
    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY, a INT NOT NULL, b INT NOT NULL, c INT, INDEX ab_idx (a, b))")
        .unwrap();
    engine
        .execute("INSERT INTO t VALUES (1, 1, 10, 100), (2, 1, 20, 200), (3, 2, 30, 300)")
        .unwrap();

    let QueryResponse::ResultSet(explain) = engine
        .execute("EXPLAIN SELECT a, b FROM t WHERE a = 1")
        .unwrap()
    else {
        panic!("expected explain result set");
    };
    assert_eq!(key_of(&explain, 0), "ab_idx");
    assert!(
        extra_of(&explain, 0).contains("Using index"),
        "covering range scan extra should contain Using index, got {:?}",
        extra_of(&explain, 0)
    );

    let QueryResponse::ResultSet(rs) = engine
        .execute("SELECT a, b FROM t WHERE a = 1 ORDER BY b")
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(rs.rows.len(), 2);
    assert_eq!(rs.rows[0][1], Value::Int(10));
    assert_eq!(rs.rows[1][1], Value::Int(20));
}

#[test]
fn select_star_disables_covering_when_columns_outside_index() {
    let (_dir, engine) = open_engine();
    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY, k INT NOT NULL, v INT, INDEX k_idx (k))")
        .unwrap();
    engine
        .execute("INSERT INTO t VALUES (1, 10, 100), (2, 20, 200)")
        .unwrap();

    let QueryResponse::ResultSet(explain) = engine
        .execute("EXPLAIN SELECT * FROM t WHERE k = 10")
        .unwrap()
    else {
        panic!("expected explain result set");
    };
    assert!(
        !extra_of(&explain, 0).contains("Using index"),
        "SELECT * needs the row, not just the index"
    );
}

#[test]
fn order_by_on_indexed_column_is_covering() {
    let (_dir, engine) = open_engine();
    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY, k INT NOT NULL, INDEX k_idx (k))")
        .unwrap();
    engine
        .execute("INSERT INTO t VALUES (1, 30), (2, 10), (3, 20)")
        .unwrap();

    let QueryResponse::ResultSet(explain) = engine
        .execute("EXPLAIN SELECT k FROM t ORDER BY k")
        .unwrap()
    else {
        panic!("expected explain result set");
    };
    assert!(
        extra_of(&explain, 0).contains("Using index"),
        "covered ORDER BY should advertise Using index, got {:?}",
        extra_of(&explain, 0)
    );

    let QueryResponse::ResultSet(rs) = engine.execute("SELECT k FROM t ORDER BY k").unwrap() else {
        panic!("expected result set");
    };
    assert_eq!(rs.rows.len(), 3);
    assert_eq!(rs.rows[0][0], Value::Int(10));
    assert_eq!(rs.rows[1][0], Value::Int(20));
    assert_eq!(rs.rows[2][0], Value::Int(30));
}

#[test]
fn group_by_on_indexed_column_is_covering() {
    let (_dir, engine) = open_engine();
    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY, k INT NOT NULL, INDEX k_idx (k))")
        .unwrap();
    engine
        .execute("INSERT INTO t VALUES (1, 1), (2, 1), (3, 2)")
        .unwrap();

    let QueryResponse::ResultSet(explain) = engine
        .execute("EXPLAIN SELECT k, COUNT(*) FROM t GROUP BY k")
        .unwrap()
    else {
        panic!("expected explain result set");
    };
    let extras: Vec<String> = (0..explain.rows.len())
        .map(|i| extra_of(&explain, i))
        .collect();
    assert!(
        extras.iter().any(|e| e.contains("Using index")),
        "GROUP BY over an indexed column should advertise Using index in some row, got {:?}",
        extras
    );
}

#[test]
fn join_inner_side_can_be_covering() {
    let (_dir, engine) = open_engine();
    engine
        .execute("CREATE TABLE outer_t (id INT PRIMARY KEY, k INT NOT NULL)")
        .unwrap();
    engine
        .execute("CREATE TABLE inner_t (id INT PRIMARY KEY, k INT NOT NULL, INDEX k_idx (k))")
        .unwrap();
    engine
        .execute("INSERT INTO outer_t VALUES (1, 10), (2, 20)")
        .unwrap();
    engine
        .execute("INSERT INTO inner_t VALUES (10, 10), (11, 10), (20, 20)")
        .unwrap();
    engine.execute("ANALYZE TABLE outer_t").unwrap();
    engine.execute("ANALYZE TABLE inner_t").unwrap();

    let QueryResponse::ResultSet(rs) = engine
        .execute(
            "SELECT outer_t.k, inner_t.k FROM outer_t \
             INNER JOIN inner_t ON outer_t.k = inner_t.k \
             ORDER BY outer_t.k, inner_t.k",
        )
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(rs.rows.len(), 3);
    for row in &rs.rows {
        assert_eq!(row[0], row[1]);
    }
}

#[test]
fn in_subquery_with_covering_index_returns_index_values() {
    let (_dir, engine) = open_engine();
    engine
        .execute("CREATE TABLE outer_t (id INT PRIMARY KEY, k INT NOT NULL)")
        .unwrap();
    engine
        .execute("CREATE TABLE inner_t (id INT PRIMARY KEY, k INT NOT NULL, INDEX k_idx (k))")
        .unwrap();
    engine
        .execute("INSERT INTO outer_t VALUES (1, 10), (2, 20), (3, 30)")
        .unwrap();
    engine
        .execute("INSERT INTO inner_t VALUES (1, 10), (2, 20)")
        .unwrap();

    let QueryResponse::ResultSet(rs) = engine
        .execute(
            "SELECT k FROM inner_t WHERE k IN (SELECT k FROM outer_t WHERE id IN (1, 2)) ORDER BY k",
        )
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(rs.rows.len(), 2);
    assert_eq!(rs.rows[0][0], Value::Int(10));
    assert_eq!(rs.rows[1][0], Value::Int(20));
}

#[test]
fn count_star_pushed_down_to_index_aggregate() {
    let (_dir, engine) = open_engine();
    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY, k INT NOT NULL, INDEX k_idx (k))")
        .unwrap();
    engine
        .execute("INSERT INTO t VALUES (1, 10), (2, 20), (3, 30)")
        .unwrap();

    let QueryResponse::ResultSet(rs) = engine.execute("SELECT COUNT(*) FROM t").unwrap() else {
        panic!("expected result set");
    };
    assert_eq!(rs.rows.len(), 1);
    assert_eq!(rs.rows[0][0], Value::Int(3));

    let QueryResponse::ResultSet(explain) =
        engine.execute("EXPLAIN SELECT COUNT(*) FROM t").unwrap()
    else {
        panic!("expected explain result set");
    };
    assert_eq!(extra_of(&explain, 0), "Using index for aggregate");
}

#[test]
fn full_scan_hint_disables_count_star_index_aggregate() {
    let (_dir, engine) = open_engine();
    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY, k INT NOT NULL, INDEX k_idx (k))")
        .unwrap();
    engine
        .execute("INSERT INTO t VALUES (1, 10), (2, 20), (3, 30)")
        .unwrap();

    let QueryResponse::ResultSet(explain) = engine
        .execute("EXPLAIN SELECT /*+ FULL_SCAN(t) */ COUNT(*) FROM t")
        .unwrap()
    else {
        panic!("expected explain result set");
    };
    assert_eq!(access_type_of(&explain, 0), "ALL");
    let extra = extra_of(&explain, 0);
    assert!(extra.contains("FULL_SCAN"), "extra={extra:?}");
    assert!(
        !extra.contains("Using index for aggregate"),
        "FULL_SCAN must not be rewritten to IndexAggregate: {extra:?}"
    );
}

#[test]
fn full_scan_alias_disables_count_star_index_aggregate() {
    let (_dir, engine) = open_engine();
    engine
        .execute(
            "CREATE TABLE lineitem (\
              l_orderkey INT NOT NULL,\
              l_quantity INT NOT NULL,\
              INDEX idx_lineitem_orderkey (l_orderkey)\
            )",
        )
        .unwrap();
    engine
        .execute("INSERT INTO lineitem VALUES (1, 10), (2, 20), (3, 30)")
        .unwrap();

    let QueryResponse::ResultSet(explain) = engine
        .execute("EXPLAIN SELECT /*+ FULL_SCAN(l) */ COUNT(*) FROM lineitem l")
        .unwrap()
    else {
        panic!("expected explain result set");
    };
    assert_eq!(access_type_of(&explain, 0), "ALL");
    let extra = extra_of(&explain, 0);
    assert!(extra.contains("FULL_SCAN"), "extra={extra:?}");
    assert!(
        !extra.contains("Using index for aggregate"),
        "FULL_SCAN alias must not be rewritten to IndexAggregate: {extra:?}"
    );
}

#[test]
fn explain_tree_full_scan_count_star_stays_table_scan() {
    let (_dir, engine) = open_engine();
    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY, k INT NOT NULL, INDEX k_idx (k))")
        .unwrap();
    engine
        .execute("INSERT INTO t VALUES (1, 10), (2, 20), (3, 30)")
        .unwrap();

    let QueryResponse::ResultSet(rs) = engine
        .execute("EXPLAIN FORMAT=TREE SELECT /*+ FULL_SCAN(t) */ COUNT(*) FROM t")
        .unwrap()
    else {
        panic!("expected result set");
    };
    let tree = match &rs.rows[0][0] {
        Value::String(s) => s,
        other => panic!("unexpected explain row: {other:?}"),
    };
    assert!(
        tree.contains("Table scan on t"),
        "expected FULL_SCAN count to use a table scan, got: {tree}"
    );
    assert!(
        !tree.contains("Index aggregate") && !tree.contains("Index-only scan"),
        "FULL_SCAN count should not show an index aggregate path: {tree}"
    );
}

#[test]
fn no_index_hint_filters_count_star_index_aggregate_choice() {
    let (_dir, engine) = open_engine();
    engine
        .execute(
            "CREATE TABLE t (\
              a INT NOT NULL,\
              b INT NOT NULL,\
              INDEX idx_a (a),\
              INDEX idx_b (b)\
            )",
        )
        .unwrap();
    engine
        .execute("INSERT INTO t VALUES (10, 100), (20, 200), (30, 300)")
        .unwrap();

    let QueryResponse::ResultSet(explain) = engine
        .execute("EXPLAIN SELECT /*+ NO_INDEX(t idx_a) */ COUNT(*) FROM t")
        .unwrap()
    else {
        panic!("expected explain result set");
    };
    assert_eq!(extra_of(&explain, 0), "Using index for aggregate");
    assert_eq!(key_of(&explain, 0), "idx_b");
}

#[test]
fn explain_tree_index_aggregate_shows_index_scan_and_serial_workers() {
    let (_dir, engine) = open_engine();
    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY, k INT NOT NULL, INDEX k_idx (k))")
        .unwrap();
    engine
        .execute("INSERT INTO t VALUES (1, 10), (2, 20), (3, 30)")
        .unwrap();

    let QueryResponse::ResultSet(rs) = engine
        .execute("EXPLAIN FORMAT=TREE SELECT COUNT(*) FROM t")
        .unwrap()
    else {
        panic!("expected result set");
    };
    let tree = match &rs.rows[0][0] {
        Value::String(s) => s,
        other => panic!("unexpected explain row: {other:?}"),
    };
    assert!(
        tree.contains("Index aggregate on t using"),
        "expected index aggregate in tree explain, got: {tree}"
    );
    assert!(
        tree.contains("Index-only scan on t using"),
        "expected underlying index scan in tree explain, got: {tree}"
    );
    assert!(
        tree.contains("workers=1"),
        "expected tree explain to surface serial worker count, got: {tree}"
    );
}

#[test]
fn min_pushed_down_when_index_starts_with_column() {
    let (_dir, engine) = open_engine();
    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY, k INT NOT NULL, INDEX k_idx (k))")
        .unwrap();
    engine
        .execute("INSERT INTO t VALUES (1, 30), (2, 10), (3, 20)")
        .unwrap();

    let QueryResponse::ResultSet(rs) = engine.execute("SELECT MIN(k) FROM t").unwrap() else {
        panic!("expected result set");
    };
    assert_eq!(rs.rows.len(), 1);
    assert_eq!(rs.rows[0][0], Value::Int(10));

    let QueryResponse::ResultSet(explain) = engine.execute("EXPLAIN SELECT MIN(k) FROM t").unwrap()
    else {
        panic!("expected explain result set");
    };
    assert_eq!(extra_of(&explain, 0), "Using index for aggregate");
}

#[test]
fn max_pushed_down_when_index_starts_with_column() {
    let (_dir, engine) = open_engine();
    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY, k INT NOT NULL, INDEX k_idx (k))")
        .unwrap();
    engine
        .execute("INSERT INTO t VALUES (1, 30), (2, 10), (3, 20)")
        .unwrap();

    let QueryResponse::ResultSet(rs) = engine.execute("SELECT MAX(k) FROM t").unwrap() else {
        panic!("expected result set");
    };
    assert_eq!(rs.rows.len(), 1);
    assert_eq!(rs.rows[0][0], Value::Int(30));
}

#[test]
fn filtered_min_max_uses_ordered_index_edge_walk() {
    // Keep parallel scans competitive even though the regression fixture is
    // much smaller than the production parallelism threshold.
    let _tuning = super::parallel::auto_parallel_test_tuning();
    let (_dir, engine) = open_engine();
    engine
        .execute(
            "CREATE TABLE t (
                id INT PRIMARY KEY,
                k INT NULL,
                status VARCHAR(16) NOT NULL,
                INDEX k_idx (k)
            )",
        )
        .unwrap();
    // Multiple heap/index pages cover the edge walk, NULL skipping and residual filter.
    insert_fixture_rows(
        &engine,
        "test.t",
        (0..=4095i64).map(|n| {
            vec![
                Value::Int(n),
                if n % 9 == 0 {
                    Value::Null
                } else {
                    Value::Int(n)
                },
                Value::String(if n % 3 == 0 { "wanted" } else { "other" }.into()),
            ]
        }),
    );
    engine.execute("ANALYZE TABLE t").unwrap();

    let QueryResponse::ResultSet(max) = engine
        .execute("SELECT MAX(id) FROM t WHERE status = 'wanted'")
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(max.rows, vec![vec![Value::Int(4095)]]);

    // The lowest qualifying index entry (k for id=0) is NULL. MIN must skip
    // it and continue the ordered walk to the first non-NULL match.
    let QueryResponse::ResultSet(min) = engine
        .execute("SELECT MIN(k) FROM t WHERE status = 'wanted'")
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(min.rows, vec![vec![Value::Int(3)]]);

    let QueryResponse::ResultSet(explain) = engine
        .execute("EXPLAIN FORMAT=TREE SELECT MAX(id) FROM t WHERE status = 'wanted'")
        .unwrap()
    else {
        panic!("expected explain result set");
    };
    let Value::String(tree) = &explain.rows[0][0] else {
        panic!("expected tree explain");
    };
    assert!(
        tree.contains("Index aggregate on t using PRIMARY")
            && tree.contains("filter: status='wanted'")
            && !tree.contains("Exchange"),
        "filtered MAX should be a serial ordered edge walk: {tree}"
    );

    let QueryResponse::ResultSet(full_scan) = engine
        .execute(
            "EXPLAIN FORMAT=TREE
             SELECT /*+ FULL_SCAN(t) */ MAX(id) FROM t WHERE status = 'wanted'",
        )
        .unwrap()
    else {
        panic!("expected explain result set");
    };
    let Value::String(full_scan_tree) = &full_scan.rows[0][0] else {
        panic!("expected tree explain");
    };
    assert!(
        full_scan_tree.contains("Exchange"),
        "FULL_SCAN must exercise the parallel alternative: {full_scan_tree}"
    );
    assert!(
        !full_scan_tree.contains("Index aggregate on t"),
        "FULL_SCAN must disable filtered MIN/MAX index pushdown: {full_scan_tree}"
    );
}

#[test]
fn min_not_pushed_down_when_column_is_not_first_index_column() {
    let (_dir, engine) = open_engine();
    engine
        .execute(
            "CREATE TABLE t (id INT PRIMARY KEY, a INT NOT NULL, b INT NOT NULL, INDEX ab (a, b))",
        )
        .unwrap();
    engine
        .execute("INSERT INTO t VALUES (1, 1, 30), (2, 1, 10), (3, 2, 20)")
        .unwrap();

    // b is not the first column of any index, so this falls back to the
    // generic aggregate path. Result must still be correct.
    let QueryResponse::ResultSet(rs) = engine.execute("SELECT MIN(b) FROM t").unwrap() else {
        panic!("expected result set");
    };
    assert_eq!(rs.rows[0][0], Value::Int(10));
}

#[test]
fn count_star_range_where_pushed_down_to_index_aggregate() {
    let (_dir, engine) = open_engine();
    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY, k INT NOT NULL, INDEX k_idx (k))")
        .unwrap();
    engine
        .execute("INSERT INTO t VALUES (1, 10), (2, 20), (3, 30)")
        .unwrap();

    let QueryResponse::ResultSet(rs) = engine
        .execute("SELECT COUNT(*) FROM t WHERE k >= 20")
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(rs.rows[0][0], Value::Int(2));

    let QueryResponse::ResultSet(explain) = engine
        .execute("EXPLAIN SELECT COUNT(*) FROM t WHERE k >= 20")
        .unwrap()
    else {
        panic!("expected explain result set");
    };
    assert_eq!(key_of(&explain, 0), "k_idx");
    assert_eq!(access_type_of(&explain, 0), "index");
    assert!(
        extra_of(&explain, 0).contains("Using index for aggregate"),
        "range COUNT should use index aggregate, got {:?}",
        extra_of(&explain, 0)
    );
}

#[test]
fn parallel_hint_count_star_range_where_keeps_index_aggregate() {
    let (_dir, engine) = open_engine();
    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY, k INT NOT NULL, INDEX k_idx (k))")
        .unwrap();
    engine
        .execute("INSERT INTO t VALUES (1, 10), (2, 20), (3, 30), (4, 40)")
        .unwrap();

    let QueryResponse::ResultSet(rs) = engine
        .execute("SELECT /*+ PARALLEL(4) */ COUNT(*) FROM t WHERE k >= 20")
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(rs.rows[0][0], Value::Int(3));

    let QueryResponse::ResultSet(explain) = engine
        .execute("EXPLAIN FORMAT=TREE SELECT /*+ PARALLEL(4) */ COUNT(*) FROM t WHERE k >= 20")
        .unwrap()
    else {
        panic!("expected explain result set");
    };
    let tree = match &explain.rows[0][0] {
        Value::String(s) => s,
        other => panic!("unexpected explain row: {other:?}"),
    };
    assert!(
        tree.contains("Index aggregate on t using k_idx")
            || tree.contains("Parallel index aggregate on t using k_idx"),
        "parallel hint should keep bounded index aggregate, got: {tree}"
    );
    assert!(
        !tree.contains("Exchange"),
        "bounded COUNT should not materialize through Exchange, got: {tree}"
    );
}

#[test]
fn explain_analyze_count_star_range_where_uses_index_aggregate() {
    let (_dir, engine) = open_engine();
    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY, k INT NOT NULL, INDEX k_idx (k))")
        .unwrap();
    engine
        .execute("INSERT INTO t VALUES (1, 10), (2, 20), (3, 30), (4, 40)")
        .unwrap();

    let QueryResponse::ResultSet(explain) = engine
        .execute("EXPLAIN ANALYZE FORMAT=TREE SELECT COUNT(*) FROM t WHERE k >= 20")
        .unwrap()
    else {
        panic!("expected explain result set");
    };
    let tree = match &explain.rows[0][0] {
        Value::String(s) => s,
        other => panic!("unexpected explain row: {other:?}"),
    };
    assert!(
        tree.contains("Index aggregate on t using k_idx")
            && tree.contains("actual time=")
            && tree.contains("rows=1"),
        "EXPLAIN ANALYZE should execute bounded index aggregate as one output row, got: {tree}"
    );
}

#[test]
fn count_star_range_with_residual_filter_not_pushed_down() {
    let (_dir, engine) = open_engine();
    engine
        .execute(
            "CREATE TABLE t (id INT PRIMARY KEY, k INT NOT NULL, v INT NOT NULL, INDEX k_idx (k))",
        )
        .unwrap();
    engine
        .execute("INSERT INTO t VALUES (1, 10, 1), (2, 20, 1), (3, 30, 2)")
        .unwrap();

    let QueryResponse::ResultSet(rs) = engine
        .execute("SELECT COUNT(*) FROM t WHERE k >= 20 AND v = 1")
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(rs.rows[0][0], Value::Int(1));

    let QueryResponse::ResultSet(explain) = engine
        .execute("EXPLAIN SELECT COUNT(*) FROM t WHERE k >= 20 AND v = 1")
        .unwrap()
    else {
        panic!("expected explain result set");
    };
    assert!(
        !extra_of(&explain, 0).contains("Using index for aggregate"),
        "residual filter must not be pushed into raw index count: {:?}",
        explain.rows
    );
}

#[test]
fn no_index_hint_disables_count_star_range_index_aggregate() {
    let (_dir, engine) = open_engine();
    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY, k INT NOT NULL, INDEX k_idx (k))")
        .unwrap();
    engine
        .execute("INSERT INTO t VALUES (1, 10), (2, 20), (3, 30)")
        .unwrap();

    let QueryResponse::ResultSet(explain) = engine
        .execute("EXPLAIN SELECT /*+ NO_INDEX(t k_idx) */ COUNT(*) FROM t WHERE k >= 20")
        .unwrap()
    else {
        panic!("expected explain result set");
    };
    assert!(
        !extra_of(&explain, 0).contains("Using index for aggregate"),
        "NO_INDEX must block bounded index aggregate: {:?}",
        explain.rows
    );
}

#[test]
fn count_star_range_after_delete_uses_visibility_filter() {
    let (_dir, engine) = open_engine();
    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY, k INT NOT NULL, INDEX k_idx (k))")
        .unwrap();
    engine
        .execute("INSERT INTO t VALUES (1, 10), (2, 20), (3, 30)")
        .unwrap();
    engine.execute("DELETE FROM t WHERE id = 2").unwrap();

    let QueryResponse::ResultSet(rs) = engine
        .execute("SELECT COUNT(*) FROM t WHERE k >= 20")
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(rs.rows[0][0], Value::Int(1));
}

#[test]
fn count_star_correct_after_delete() {
    // Visibility filter must still drop tombstones from the count even
    // though the index entry remains until vacuum.
    let (_dir, engine) = open_engine();
    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY, k INT NOT NULL, INDEX k_idx (k))")
        .unwrap();
    engine
        .execute("INSERT INTO t VALUES (1, 10), (2, 20), (3, 30)")
        .unwrap();
    engine.execute("DELETE FROM t WHERE id = 2").unwrap();

    let QueryResponse::ResultSet(rs) = engine.execute("SELECT COUNT(*) FROM t").unwrap() else {
        panic!("expected result set");
    };
    assert_eq!(rs.rows[0][0], Value::Int(2));
}

#[test]
fn count_not_null_column_pushed_down_to_index_aggregate() {
    // `COUNT(col)` over a NOT NULL column is semantically equivalent
    // to `COUNT(*)` — the planner should recognise this and emit the
    // same `IndexAggregate` operator (so EXPLAIN says
    // "Using index for aggregate") rather than the generic
    // HashAggregate-over-IndexFastScan path.
    let (_dir, engine) = open_engine();
    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY, k INT NOT NULL, INDEX k_idx (k))")
        .unwrap();
    engine
        .execute("INSERT INTO t VALUES (1, 10), (2, 20), (3, 30)")
        .unwrap();

    let QueryResponse::ResultSet(rs) = engine.execute("SELECT COUNT(k) FROM t").unwrap() else {
        panic!("expected result set");
    };
    assert_eq!(rs.rows.len(), 1);
    assert_eq!(rs.rows[0][0], Value::Int(3));

    let QueryResponse::ResultSet(explain) =
        engine.execute("EXPLAIN SELECT COUNT(k) FROM t").unwrap()
    else {
        panic!("expected explain result set");
    };
    assert_eq!(extra_of(&explain, 0), "Using index for aggregate");
}

#[test]
fn count_nullable_column_skips_nulls_via_covering_index() {
    // For a nullable covered column, `COUNT(col)` still pushes down
    // but must skip rows whose index entry stores NULL for that
    // column.
    let (_dir, engine) = open_engine();
    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY, k INT NULL, INDEX k_idx (k))")
        .unwrap();
    engine
        .execute("INSERT INTO t VALUES (1, 10), (2, NULL), (3, 30), (4, NULL), (5, 50)")
        .unwrap();

    let QueryResponse::ResultSet(rs) = engine.execute("SELECT COUNT(k) FROM t").unwrap() else {
        panic!("expected result set");
    };
    assert_eq!(rs.rows.len(), 1);
    assert_eq!(rs.rows[0][0], Value::Int(3));

    // COUNT(*) must still see all 5 rows even though two have NULL k.
    let QueryResponse::ResultSet(rs) = engine.execute("SELECT COUNT(*) FROM t").unwrap() else {
        panic!("expected result set");
    };
    assert_eq!(rs.rows[0][0], Value::Int(5));
}

#[test]
fn count_distinct_leading_index_streams_nullable_duplicates() {
    let (_dir, engine) = open_engine();
    engine
        .execute(
            "CREATE TABLE t (
                id INT PRIMARY KEY,
                k INT NULL,
                payload INT NOT NULL,
                INDEX k_idx (k)
            )",
        )
        .unwrap();
    engine
        .execute(
            "INSERT INTO t VALUES
                (1,NULL,10),(2,NULL,20),
                (3,10,30),(4,10,40),
                (5,20,50),(6,20,60),(7,30,70)",
        )
        .unwrap();

    let QueryResponse::ResultSet(rs) = engine.execute("SELECT COUNT(DISTINCT k) FROM t").unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(rs.rows[0][0], Value::Int(3));

    let QueryResponse::ResultSet(explain) = engine
        .execute("EXPLAIN FORMAT=TREE SELECT COUNT(DISTINCT k) FROM t")
        .unwrap()
    else {
        panic!("expected result set");
    };
    let Value::String(tree) = &explain.rows[0][0] else {
        panic!("expected tree explain");
    };
    assert!(
        tree.contains("Index aggregate on t using k_idx"),
        "COUNT(DISTINCT leading_col) must avoid row materialization: {tree}"
    );
    assert!(
        !tree.contains("Exchange") && !tree.contains("HashAggregate"),
        "streaming index DISTINCT must not gather/hash input rows: {tree}"
    );

    engine.execute("DELETE FROM t WHERE id IN (5,6)").unwrap();
    let QueryResponse::ResultSet(after_delete) =
        engine.execute("SELECT COUNT(DISTINCT k) FROM t").unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(
        after_delete.rows[0][0],
        Value::Int(2),
        "invisible index entries must not contribute a distinct key"
    );
}

#[test]
fn count_distinct_index_pushdown_preserves_collation_and_range_semantics() {
    let (_dir, engine) = open_engine();
    engine
        .execute(
            "CREATE TABLE t (
                id INT PRIMARY KEY,
                k VARCHAR(16) NULL,
                INDEX k_idx (k)
            )",
        )
        .unwrap();
    engine
        .execute(
            "INSERT INTO t VALUES
                (1,'alpha'),(2,'ALPHA'),(3,'beta'),(4,'BETA'),
                (5,'gamma'),(6,NULL)",
        )
        .unwrap();

    for sql in [
        "SELECT COUNT(DISTINCT k) FROM t",
        "SELECT COUNT(DISTINCT k) FROM t WHERE k >= 'beta'",
    ] {
        let oracle_sql = sql.replacen("SELECT ", "SELECT /*+ FULL_SCAN(t) */ ", 1);
        let QueryResponse::ResultSet(oracle) = engine.execute(&oracle_sql).unwrap() else {
            panic!("expected oracle result set");
        };
        let QueryResponse::ResultSet(actual) = engine.execute(sql).unwrap() else {
            panic!("expected result set");
        };
        assert_eq!(
            actual.rows, oracle.rows,
            "ordered index dedup must match the row/hash oracle for {sql}"
        );
    }

    let QueryResponse::ResultSet(explain) = engine
        .execute(
            "EXPLAIN SELECT COUNT(DISTINCT k) FROM t
             WHERE k >= 'beta'",
        )
        .unwrap()
    else {
        panic!("expected explain result set");
    };
    assert_eq!(key_of(&explain, 0), "k_idx");
    assert!(
        extra_of(&explain, 0).contains("Using index for aggregate"),
        "a bound fully represented by the leading key remains eligible: {:?}",
        explain.rows
    );
}

#[test]
fn count_distinct_non_leading_or_prefix_index_keeps_generic_oracle() {
    let (_dir, engine) = open_engine();
    engine
        .execute(
            "CREATE TABLE t (
                id INT PRIMARY KEY,
                a INT NOT NULL,
                b VARCHAR(16) NOT NULL,
                INDEX ab_idx (a, b),
                INDEX b_prefix (b(2))
            )",
        )
        .unwrap();
    engine
        .execute(
            "INSERT INTO t VALUES
                (1,1,'alpha'),(2,1,'alpine'),(3,2,'alpha'),(4,2,'beta')",
        )
        .unwrap();

    let QueryResponse::ResultSet(rs) = engine.execute("SELECT COUNT(DISTINCT b) FROM t").unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(rs.rows[0][0], Value::Int(3));

    let QueryResponse::ResultSet(explain) = engine
        .execute("EXPLAIN FORMAT=TREE SELECT COUNT(DISTINCT b) FROM t")
        .unwrap()
    else {
        panic!("expected result set");
    };
    let Value::String(tree) = &explain.rows[0][0] else {
        panic!("expected tree explain");
    };
    assert!(
        !tree.contains("Index aggregate on t"),
        "non-leading and prefix-only keys cannot prove exact adjacency: {tree}"
    );
}

#[test]
fn count_distinct_leading_index_is_constant_query_memory() {
    use crate::engine::executor::mem_budget::{with_query_budget, MemBudget};
    use std::sync::Arc;

    let (_dir, engine) = open_engine();
    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY, k INT NOT NULL, INDEX k_idx (k))")
        .unwrap();
    engine
        .execute("INSERT INTO t SELECT n, n FROM generate_series(0, 9999) AS gs(n)")
        .unwrap();

    // A generic CountDistinctAcc needs one hash entry per value. The
    // ordered index aggregate retains only the previous key, so query
    // memory does not scale with NDV.
    let budget = Arc::new(MemBudget::new(32 * 1024));
    let QueryResponse::ResultSet(rs) = with_query_budget(Arc::clone(&budget), || {
        engine.execute("SELECT COUNT(DISTINCT k) FROM t")
    })
    .unwrap() else {
        panic!("expected result set");
    };
    assert_eq!(rs.rows[0][0], Value::Int(10_000));
    assert_eq!(budget.used(), 0, "index aggregate leaked its query charge");
}

#[test]
fn explain_tree_for_ungrouped_count_drops_temporary_qualifier() {
    // MySQL's `format=tree` writes "Aggregate using temporary" only
    // when the aggregate materialises a hash table for grouping. An
    // ungrouped aggregate (no GROUP BY) emits exactly one row, so
    // mydb should also drop the qualifier and report rows=1 — the
    // earlier behaviour reported "Aggregate using temporary
    // (... rows=N*0.25)" which leaked into downstream cost estimates.
    let (_dir, engine) = open_engine();
    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY, k INT NULL, INDEX k_idx (k))")
        .unwrap();
    engine
        .execute("INSERT INTO t VALUES (1, 1), (2, NULL), (3, 3)")
        .unwrap();

    // SUM/AVG don't have an IndexAggregate pushdown, so they exercise
    // the HashAggregate path. Verify the tree-format label and row
    // estimate.
    let QueryResponse::ResultSet(rs) = engine
        .execute("EXPLAIN FORMAT=TREE SELECT SUM(k) FROM t")
        .unwrap()
    else {
        panic!("expected result set");
    };
    let tree = match &rs.rows[0][0] {
        Value::String(s) => s.clone(),
        other => panic!("unexpected explain row: {other:?}"),
    };
    assert!(
        !tree.contains("Aggregate using temporary"),
        "ungrouped aggregate should not say 'using temporary': {tree}"
    );
    assert!(
        (tree.contains("Aggregate ") || tree.contains("Index aggregate "))
            && tree.contains("rows=1"),
        "ungrouped aggregate row estimate must be 1: {tree}"
    );
}

#[test]
fn include_columns_extend_covering_set() {
    let (_dir, engine) = open_engine();
    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY, k INT NOT NULL, v INT NOT NULL)")
        .unwrap();
    engine
        .execute("CREATE INDEX k_with_v ON t (k) INCLUDE (v)")
        .unwrap();
    engine
        .execute("INSERT INTO t VALUES (1, 10, 100), (2, 20, 200), (3, 30, 300)")
        .unwrap();

    // The selected index normally would cover only `k`; with INCLUDE,
    // `v` is reachable from the index entry too, so this is covering.
    let QueryResponse::ResultSet(explain) = engine
        .execute("EXPLAIN SELECT k, v FROM t WHERE k = 20")
        .unwrap()
    else {
        panic!("expected explain result set");
    };
    assert!(
        extra_of(&explain, 0).contains("Using index"),
        "INCLUDE columns should make the scan covering, got {:?}",
        extra_of(&explain, 0)
    );

    let QueryResponse::ResultSet(rs) = engine.execute("SELECT k, v FROM t WHERE k = 20").unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(rs.rows.len(), 1);
    assert_eq!(rs.rows[0][0], Value::Int(20));
    assert_eq!(rs.rows[0][1], Value::Int(200));
}

#[test]
fn unique_index_rejects_include_clause() {
    let (_dir, engine) = open_engine();
    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY, k INT NOT NULL, v INT)")
        .unwrap();
    let result = engine.execute("CREATE UNIQUE INDEX uk ON t (k) INCLUDE (v)");
    assert!(
        result.is_err(),
        "INCLUDE on UNIQUE INDEX should be rejected"
    );
}

#[test]
fn include_columns_handle_updates_correctly() {
    let (_dir, engine) = open_engine();
    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY, k INT NOT NULL, v INT NOT NULL)")
        .unwrap();
    engine
        .execute("CREATE INDEX k_with_v ON t (k) INCLUDE (v)")
        .unwrap();
    engine
        .execute("INSERT INTO t VALUES (1, 10, 100), (2, 20, 200)")
        .unwrap();
    engine.execute("UPDATE t SET v = 999 WHERE id = 1").unwrap();

    let QueryResponse::ResultSet(rs) = engine.execute("SELECT k, v FROM t WHERE k = 10").unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(rs.rows.len(), 1);
    assert_eq!(rs.rows[0][1], Value::Int(999));
}

#[test]
fn buffered_visibility_path_returns_correct_results() {
    // Sanity check that the cache-only visibility path still excludes
    // tombstones from a covering scan after DELETE — this is the
    // spot a regression in `is_row_id_visible_buffered` would surface
    // first.
    let (_dir, engine) = open_engine();
    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY, k INT NOT NULL, INDEX k_idx (k))")
        .unwrap();
    engine
        .execute("INSERT INTO t VALUES (1, 10), (2, 20), (3, 30), (4, 40)")
        .unwrap();
    engine.execute("DELETE FROM t WHERE k = 20").unwrap();

    let QueryResponse::ResultSet(rs) = engine
        .execute("SELECT k FROM t WHERE k BETWEEN 10 AND 40 ORDER BY k")
        .unwrap()
    else {
        panic!("expected result set");
    };
    let values: Vec<_> = rs.rows.iter().map(|r| r[0].clone()).collect();
    assert_eq!(values, vec![Value::Int(10), Value::Int(30), Value::Int(40)]);
}

#[test]
fn covering_range_scan_filters_residual_predicate() {
    // Filter columns must also be in the index for covering to apply, so
    // residual evaluation reads the column from the index entry, not the
    // heap. This guards against `index_entry_to_plan_row` regressions
    // that would silently fill non-index columns with NULL.
    let (_dir, engine) = open_engine();
    engine
        .execute(
            "CREATE TABLE t (id INT PRIMARY KEY, a INT NOT NULL, b INT NOT NULL, INDEX ab (a, b))",
        )
        .unwrap();
    engine
        .execute("INSERT INTO t VALUES (1, 1, 5), (2, 1, 10), (3, 1, 15), (4, 2, 20)")
        .unwrap();

    let QueryResponse::ResultSet(rs) = engine
        .execute("SELECT a, b FROM t WHERE a = 1 AND b > 7 ORDER BY b")
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(rs.rows.len(), 2);
    assert_eq!(rs.rows[0][1], Value::Int(10));
    assert_eq!(rs.rows[1][1], Value::Int(15));

    let QueryResponse::ResultSet(explain) = engine
        .execute("EXPLAIN SELECT a, b FROM t WHERE a = 1 AND b > 7")
        .unwrap()
    else {
        panic!("expected explain result set");
    };
    assert!(
        extra_of(&explain, 0).contains("Using index"),
        "extra should contain Using index, got {:?}",
        extra_of(&explain, 0)
    );

    // Keep `a = 1` as the index bound while forcing `b + 0 > 7` to remain a
    // residual. Covering scans should compile that residual once and evaluate
    // it positionally for every index entry, without whole-expression legacy
    // fallback or heap access.
    let before = crate::engine::expr_program::expr_program_stats();
    let QueryResponse::ResultSet(rs) = engine
        .execute("SELECT a, b FROM t WHERE a = 1 AND b + 0 > 7 ORDER BY b")
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(
        rs.rows,
        vec![
            vec![Value::Int(1), Value::Int(10)],
            vec![Value::Int(1), Value::Int(15)],
        ]
    );
    let after = crate::engine::expr_program::expr_program_stats();
    assert!(
        after.program_eval_rows > before.program_eval_rows,
        "covering residual should use the compiled expression program"
    );
    assert_eq!(
        after.legacy_fallback_rows, before.legacy_fallback_rows,
        "compiled covering residual must not fall back to the legacy evaluator"
    );
}

fn access_type_of(explain: &crate::engine::api::ResultSet, row: usize) -> String {
    match &explain.rows[row][4] {
        Value::String(s) => s.clone(),
        other => panic!("unexpected access_type value: {other:?}"),
    }
}

#[test]
fn range_predicate_with_date_interval_uses_covering_index() {
    // TPC-H Q1 shape: `WHERE c <= DATE 'x' - INTERVAL 'n' DAY` over
    // `SELECT COUNT(c)`. The parser rewrites the RHS to
    // `DATE_SUB(date_literal, __INTERVAL(n, 'DAY'))`, which is a
    // function expression rather than a literal. The planner's
    // constraint extractor used to reject anything that wasn't a bare
    // literal/placeholder, so the range bound was dropped and a
    // single-column covering index never got picked — collapsing the
    // plan to a full table scan.
    let (_dir, engine) = open_engine();
    engine
        .execute(
            "CREATE TABLE lineitem (\
               id INT PRIMARY KEY,\
               l_shipdate DATE NOT NULL,\
               INDEX idx_shipdate (l_shipdate)\
             )",
        )
        .unwrap();
    engine
        .execute(
            "INSERT INTO lineitem VALUES \
                (1, '1998-01-01'), (2, '1998-06-15'), (3, '1998-08-01'),\
                (4, '1998-09-30'), (5, '1998-11-15')",
        )
        .unwrap();

    let QueryResponse::ResultSet(explain) = engine
        .execute(
            "EXPLAIN SELECT COUNT(l_shipdate) FROM lineitem \
             WHERE l_shipdate <= DATE '1998-12-01' - INTERVAL '90' DAY",
        )
        .unwrap()
    else {
        panic!("expected explain result set");
    };
    let table_row = find_table_row(&explain, "lineitem");
    assert_eq!(
        key_of(&explain, table_row),
        "idx_shipdate",
        "should pick the covering index, got rows={:?}",
        explain.rows
    );
    assert!(
        extra_of(&explain, table_row).contains("Using index"),
        "covering range scan over DATE - INTERVAL should advertise Using index, got {:?}",
        extra_of(&explain, table_row)
    );

    let QueryResponse::ResultSet(rs) = engine
        .execute(
            "SELECT COUNT(l_shipdate) FROM lineitem \
             WHERE l_shipdate <= DATE '1998-12-01' - INTERVAL '90' DAY",
        )
        .unwrap()
    else {
        panic!("expected result set");
    };
    // Cutoff is 1998-09-02. Rows 1, 2, 3 qualify; rows 4, 5 do not.
    assert_eq!(rs.rows.len(), 1);
    assert_eq!(rs.rows[0][0], Value::Int(3));
}

#[test]
fn covering_range_scan_uses_index_fast_scan_operator() {
    let (_dir, engine) = open_engine();
    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY, a INT, b INT, INDEX idx_ab (a, b))")
        .unwrap();
    engine
        .execute("INSERT INTO t VALUES (1, 1, 10), (2, 2, 20), (3, 3, 30), (4, 4, 40)")
        .unwrap();

    // Bounded range over (a) projecting only indexed columns: planner
    // should pick the index-only fast-scan operator.
    let QueryResponse::ResultSet(explain) = engine
        .execute("EXPLAIN SELECT a, b FROM t WHERE a BETWEEN 2 AND 3")
        .unwrap()
    else {
        panic!("expected explain result set");
    };
    assert_eq!(access_type_of(&explain, 0), "index");
    assert_eq!(key_of(&explain, 0), "idx_ab");
    assert!(
        extra_of(&explain, 0).contains("Using index"),
        "expected Using index in Extra, got {:?}",
        extra_of(&explain, 0)
    );

    // The same SELECT (without EXPLAIN) must still produce the right
    // rows — fast scan reads values directly from the index entries.
    let QueryResponse::ResultSet(rs) = engine
        .execute("SELECT a, b FROM t WHERE a BETWEEN 2 AND 3 ORDER BY a")
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(rs.rows.len(), 2);
    assert_eq!(rs.rows[0][0], Value::Int(2));
    assert_eq!(rs.rows[0][1], Value::Int(20));
    assert_eq!(rs.rows[1][0], Value::Int(3));
    assert_eq!(rs.rows[1][1], Value::Int(30));
}

#[test]
fn full_covering_index_scan_chooses_index_fast_scan() {
    let (_dir, engine) = open_engine();
    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY, a INT, b INT, INDEX idx_ab (a, b))")
        .unwrap();
    engine
        .execute("INSERT INTO t VALUES (1, 1, 10), (2, 2, 20), (3, 3, 30)")
        .unwrap();

    // No WHERE: full scan, only indexed columns projected → covering.
    let QueryResponse::ResultSet(explain) = engine.execute("EXPLAIN SELECT a, b FROM t").unwrap()
    else {
        panic!("expected explain result set");
    };
    assert_eq!(access_type_of(&explain, 0), "index");
    assert_eq!(key_of(&explain, 0), "idx_ab");
    assert!(extra_of(&explain, 0).contains("Using index"));
}

fn setup_order_lineitem_join(engine: &StorageQueryEngine) {
    engine
        .execute(
            "CREATE TABLE orders (\
             o_orderkey INT PRIMARY KEY,\
             o_totalprice INT,\
             INDEX idx_orders_totalprice (o_totalprice)\
             )",
        )
        .unwrap();
    engine
        .execute(
            "CREATE TABLE lineitem (\
             l_orderkey INT,\
             l_linenumber INT,\
             l_quantity INT,\
             PRIMARY KEY (l_orderkey, l_linenumber),\
             INDEX idx_lineitem_orderkey (l_orderkey)\
             )",
        )
        .unwrap();
    engine
        .execute("INSERT INTO orders VALUES (1, 10), (2, 20), (3, 30)")
        .unwrap();
    engine
        .execute("INSERT INTO lineitem VALUES (1, 1, 5), (1, 2, 6), (2, 1, 7), (4, 1, 8)")
        .unwrap();
}

#[test]
fn joined_index_ffs_hint_uses_covering_scans() {
    let (_dir, engine) = open_engine();
    setup_order_lineitem_join(&engine);

    let sql = "SELECT /*+ HASH_JOIN(a,b) INDEX_FFS(orders) */ COUNT(*) \
               FROM orders a, lineitem b \
               WHERE a.o_orderkey = b.l_orderkey";
    let QueryResponse::ResultSet(rs) = engine.execute(sql).unwrap() else {
        panic!("expected result set");
    };
    assert_eq!(rs.rows[0][0], Value::Int(3));

    let QueryResponse::ResultSet(explain) = engine.execute(&format!("EXPLAIN {sql}")).unwrap()
    else {
        panic!("expected explain result set");
    };
    let orders = find_table_row(&explain, "a");
    assert_eq!(access_type_of(&explain, orders), "index");
    assert_eq!(key_of(&explain, orders), "PRIMARY");
    let orders_extra = extra_of(&explain, orders);
    assert!(
        orders_extra.contains("Using index") && orders_extra.contains("INDEX_FFS"),
        "orders scan should be index-only with INDEX_FFS noted, got {orders_extra:?}"
    );

    let lineitem = find_table_row(&explain, "b");
    assert_eq!(access_type_of(&explain, lineitem), "index");
    assert_eq!(key_of(&explain, lineitem), "idx_lineitem_orderkey");
    assert!(
        extra_of(&explain, lineitem).contains("Using index"),
        "lineitem join key should be covered by idx_lineitem_orderkey"
    );
}

#[test]
fn joined_index_ffs_hint_matches_table_alias() {
    let (_dir, engine) = open_engine();
    setup_order_lineitem_join(&engine);

    let QueryResponse::ResultSet(explain) = engine
        .execute(
            "EXPLAIN SELECT /*+ HASH_JOIN(a,b) INDEX_FFS(a) */ COUNT(*) \
             FROM orders a, lineitem b \
             WHERE a.o_orderkey = b.l_orderkey",
        )
        .unwrap()
    else {
        panic!("expected explain result set");
    };
    let orders = find_table_row(&explain, "a");
    assert_eq!(access_type_of(&explain, orders), "index");
    assert_eq!(key_of(&explain, orders), "PRIMARY");
    assert!(
        extra_of(&explain, orders).contains("INDEX_FFS"),
        "alias-targeted INDEX_FFS should apply to orders alias a"
    );
}

#[test]
fn joined_index_ffs_hint_falls_back_when_index_does_not_cover() {
    let (_dir, engine) = open_engine();
    setup_order_lineitem_join(&engine);

    let sql = "SELECT /*+ HASH_JOIN(a,b) INDEX_FFS(orders) */ a.o_totalprice \
               FROM orders a, lineitem b \
               WHERE a.o_orderkey = b.l_orderkey \
               ORDER BY a.o_totalprice";
    let QueryResponse::ResultSet(rs) = engine.execute(sql).unwrap() else {
        panic!("expected result set");
    };
    assert_eq!(rs.rows.len(), 3);
    assert_eq!(rs.rows[0][0], Value::Int(10));
    assert_eq!(rs.rows[1][0], Value::Int(10));
    assert_eq!(rs.rows[2][0], Value::Int(20));

    let QueryResponse::ResultSet(explain) = engine.execute(&format!("EXPLAIN {sql}")).unwrap()
    else {
        panic!("expected explain result set");
    };
    let orders = find_table_row(&explain, "a");
    let orders_extra = extra_of(&explain, orders);
    assert!(
        !orders_extra.contains("INDEX_FFS"),
        "INDEX_FFS must be ignored when PRIMARY does not cover o_totalprice: {orders_extra:?}"
    );
}

#[test]
fn index_nlj_left_covering_check_keeps_join_key_available() {
    let (_dir, engine) = open_engine();
    engine
        .execute("CREATE TABLE o (id INT PRIMARY KEY, d INT, p INT, INDEX d_idx (d))")
        .unwrap();
    engine
        .execute("CREATE TABLE l (k INT, v INT, INDEX k_idx (k))")
        .unwrap();
    engine
        .execute("INSERT INTO o VALUES (1, 1, 10), (2, 2, 20), (3, 99, 30)")
        .unwrap();
    engine
        .execute("INSERT INTO l VALUES (1, 100), (1, 101), (2, 200), (3, 300)")
        .unwrap();

    let QueryResponse::ResultSet(rs) = engine
        .execute(
            "SELECT /*+ no_hash_join(o,l) ordered */ COUNT(*) \
             FROM o INNER JOIN l ON l.k = o.id \
             WHERE o.d < 10",
        )
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(rs.rows[0][0], Value::Int(3));
}

#[test]
fn large_index_nlj_uses_batched_path_without_changing_results() {
    const ROW_COUNT: i64 = 1_000;
    let (_dir, engine) = open_engine();
    engine
        .execute("CREATE TABLE o (id INT PRIMARY KEY, d INT, INDEX d_idx (d))")
        .unwrap();
    engine
        .execute("CREATE TABLE l (k INT, v INT, INDEX k_idx (k))")
        .unwrap();

    for chunk_start in (1..=ROW_COUNT).step_by(500) {
        let chunk_end = (chunk_start + 499).min(ROW_COUNT);
        let orders = (chunk_start..=chunk_end)
            .map(|id| format!("({id}, 1)"))
            .collect::<Vec<_>>()
            .join(",");
        engine
            .execute(&format!("INSERT INTO o VALUES {orders}"))
            .unwrap();
        let lines = (chunk_start..=chunk_end)
            .map(|id| format!("({id}, {})", if id % 2 == 0 { 10 } else { -10 }))
            .collect::<Vec<_>>()
            .join(",");
        engine
            .execute(&format!("INSERT INTO l VALUES {lines}"))
            .unwrap();
    }

    let QueryResponse::ResultSet(rs) = engine
        .execute(
            "SELECT /*+ no_hash_join(o,l) ordered */ COUNT(*) \
             FROM o INNER JOIN l ON l.k = o.id \
             WHERE o.d = 1 AND l.v > 0",
        )
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(rs.rows[0][0], Value::Int(500));
}

#[test]
fn batched_covering_index_nlj_handles_residuals_and_expressions() {
    let (_dir, engine) = open_engine();
    engine
        .execute("CREATE TABLE o (id INT PRIMARY KEY, d INT, INDEX d_idx (d))")
        .unwrap();
    engine
        .execute("CREATE TABLE l (k INT, v INT, marker INT, INDEX k_v_marker_idx (k, v, marker))")
        .unwrap();

    // Once the indexed inner reaches 1k rows the production admission gate
    // lowers the outer batch threshold to 64.  One thousand outer rows are
    // therefore ample to exercise the exact same covering batch kernel.
    for chunk_start in (1..=1_000).step_by(500) {
        let chunk_end = (chunk_start + 499).min(1_000);
        let orders = (chunk_start..=chunk_end)
            .map(|id| format!("({id}, 2)"))
            .collect::<Vec<_>>()
            .join(",");
        engine
            .execute(&format!("INSERT INTO o VALUES {orders}"))
            .unwrap();
        let lines = (chunk_start..=chunk_end)
            .flat_map(|id| [(id, id, 1), (id, -id, 0)])
            .map(|(k, v, marker)| format!("({k}, {v}, {marker})"))
            .collect::<Vec<_>>()
            .join(",");
        engine
            .execute(&format!("INSERT INTO l VALUES {lines}"))
            .unwrap();
    }

    let QueryResponse::ResultSet(rs) = engine
        .execute(
            "SELECT /*+ no_hash_join(o,l) ordered INDEX_NLJ(l) */ SUM(l.v * o.d) \
             FROM o INNER JOIN l ON l.k = o.id \
             WHERE o.d = 2 AND l.marker = 1",
        )
        .unwrap()
    else {
        panic!("expected result set");
    };
    // `l.v * o.d` is now an integer product (MySQL keeps INT*INT integral),
    // so SUM over it finalises as DECIMAL — matching MySQL's `SUM(<int>)`.
    assert_eq!(rs.rows[0][0], Value::DecimalCompact(1_001_000, 0));
}

#[test]
fn batched_index_nlj_preserves_left_join_unmatched_rows() {
    const ROW_COUNT: i64 = 1_000;
    let (_dir, engine) = open_engine();
    engine
        .execute("CREATE TABLE o (id INT PRIMARY KEY, d INT, INDEX d_idx (d))")
        .unwrap();
    engine
        .execute("CREATE TABLE l (k INT, v INT, INDEX k_idx (k))")
        .unwrap();

    for chunk_start in (1..=ROW_COUNT).step_by(500) {
        let chunk_end = (chunk_start + 499).min(ROW_COUNT);
        let orders = (chunk_start..=chunk_end)
            .map(|id| format!("({id}, 1)"))
            .collect::<Vec<_>>()
            .join(",");
        engine
            .execute(&format!("INSERT INTO o VALUES {orders}"))
            .unwrap();
        let lines = (chunk_start..=chunk_end)
            .filter(|id| id % 2 == 0)
            .map(|id| format!("({id}, {id})"))
            .collect::<Vec<_>>()
            .join(",");
        if !lines.is_empty() {
            engine
                .execute(&format!("INSERT INTO l VALUES {lines}"))
                .unwrap();
        }
    }

    let QueryResponse::ResultSet(rs) = engine
        .execute(
            "SELECT /*+ no_hash_join(o,l) ordered */ COUNT(*), COUNT(l.k) \
             FROM o LEFT JOIN l ON l.k = o.id \
             WHERE o.d = 1",
        )
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(rs.rows[0][0], Value::Int(1_000));
    assert_eq!(rs.rows[0][1], Value::Int(500));
}

#[test]
fn index_fast_scan_cost_is_strictly_below_range_scan_cost() {
    use crate::engine::cbo::{operator_cost, CboContext};

    // Same inputs to both formulas: a covering range over a moderately
    // sized table where heap fetches would dominate the non-covering
    // cost. The fast scan drops the heap term entirely.
    let ctx = CboContext::defaults_for_test();
    let height = 3.0;
    let leaf_pages = 200.0;
    let rows = 20_000.0;
    let table_pages = 10_000.0;
    let row_width = 64.0;

    let range = operator_cost::index_range_scan(
        &ctx,
        height,
        leaf_pages,
        rows,
        100_000.0,
        table_pages,
        row_width,
        // Best case for range scan: perfectly correlated heap order.
        1.0,
    );
    let fast = operator_cost::index_fast_scan(&ctx, height, leaf_pages, rows, row_width);

    assert!(
        fast.total < range.total,
        "expected index_fast_scan total ({fast_total}) < index_range_scan total ({range_total})",
        fast_total = fast.total,
        range_total = range.total,
    );
    // The fast-scan output should declare zero random heap pages and no
    // heap-page sequential traffic beyond the index leaves.
    assert_eq!(fast.io_seq_pages, leaf_pages);
    assert_eq!(fast.io_rnd_pages, height);
}

/// Regression guard for full-scan aggregate fusion: verify the forced heap
/// scan returns the same count as index-only count and does not fall back to
/// the old PlanRow-materialising path. Runtime comparisons live outside the
/// default test suite.
#[test]
fn full_scan_count_is_not_slower_than_index_ffs_count() {
    let (_dir, engine) = open_engine();
    engine
        .execute(
            "CREATE TABLE wide (\
              id INT PRIMARY KEY,\
              c1 INT, c2 INT, c3 INT, c4 INT, c5 INT, c6 INT, c7 INT, c8 INT,\
              shipdate INT,\
              INDEX idx_shipdate (shipdate)\
            )",
        )
        .unwrap();
    let total = 64u64;
    let mut sql = String::from("INSERT INTO wide VALUES ");
    for i in 0..total {
        if i > 0 {
            sql.push(',');
        }
        sql.push_str(&format!(
            "({i}, {}, {}, {}, {}, {}, {}, {}, {}, {})",
            i,
            i,
            i,
            i,
            i,
            i,
            i,
            i,
            i % 1000,
        ));
    }
    engine.execute(&sql).unwrap();

    let QueryResponse::ResultSet(full) = engine
        .execute("SELECT /*+ FULL_SCAN(wide) */ count(shipdate) FROM wide")
        .unwrap()
    else {
        panic!("expected full-scan result set");
    };
    let QueryResponse::ResultSet(ffs) = engine
        .execute("SELECT /*+ INDEX_FFS(wide idx_shipdate) */ count(shipdate) FROM wide")
        .unwrap()
    else {
        panic!("expected index-ffs result set");
    };
    assert_eq!(full.rows, ffs.rows);
    assert_eq!(full.rows[0][0], Value::Int(total as i64));

    let QueryResponse::ResultSet(full_tree) = engine
        .execute("EXPLAIN FORMAT=TREE SELECT /*+ FULL_SCAN(wide) */ count(shipdate) FROM wide")
        .unwrap()
    else {
        panic!("expected full-scan explain");
    };
    let full_text = match &full_tree.rows[0][0] {
        Value::String(s) => s,
        other => panic!("expected tree text, got {other:?}"),
    };
    assert!(
        full_text.contains("Table scan on wide") && !full_text.contains("Index-only scan"),
        "FULL_SCAN aggregate should stay on the heap scan path: {full_text}"
    );
}

#[test]
fn index_ffs_hint_forces_index_fast_scan() {
    let (_dir, engine) = open_engine();
    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY, a INT, b INT, INDEX idx_ab (a, b))")
        .unwrap();
    engine
        .execute("INSERT INTO t VALUES (1, 1, 10), (2, 2, 20), (3, 3, 30)")
        .unwrap();

    // Without the hint, the planner is already choosing IndexFastScan
    // for this covering full scan; assert the hint produces the same
    // operator AND surfaces "INDEX_FFS" in Extra.
    let QueryResponse::ResultSet(explain) = engine
        .execute("EXPLAIN SELECT /*+ INDEX_FFS(t) */ a, b FROM t")
        .unwrap()
    else {
        panic!("expected explain result set");
    };
    assert_eq!(access_type_of(&explain, 0), "index");
    assert_eq!(key_of(&explain, 0), "idx_ab");
    let extra = extra_of(&explain, 0);
    assert!(extra.contains("Using index"), "extra={extra:?}");
    assert!(extra.contains("INDEX_FFS"), "extra={extra:?}");

    // The query also has to return the right rows.
    let QueryResponse::ResultSet(rs) = engine
        .execute("SELECT /*+ INDEX_FFS(t) */ a, b FROM t ORDER BY a")
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(rs.rows.len(), 3);
    assert_eq!(rs.rows[0][0], Value::Int(1));
    assert_eq!(rs.rows[2][1], Value::Int(30));
}

#[test]
fn index_ffs_hint_with_specific_index_name_picks_that_index() {
    let (_dir, engine) = open_engine();
    engine
        .execute(
            "CREATE TABLE t (\
              id INT PRIMARY KEY,\
              a INT, b INT, c INT,\
              INDEX idx_ab (a, b),\
              INDEX idx_abc (a, b, c)\
            )",
        )
        .unwrap();
    engine
        .execute("INSERT INTO t VALUES (1, 1, 10, 100), (2, 2, 20, 200)")
        .unwrap();

    // Without the hint, choose_covering_index_scan would pick the
    // narrower idx_ab. With INDEX_FFS naming idx_abc, we should switch
    // to that index.
    let QueryResponse::ResultSet(explain) = engine
        .execute("EXPLAIN SELECT /*+ INDEX_FFS(t idx_abc) */ a, b FROM t")
        .unwrap()
    else {
        panic!("expected explain result set");
    };
    assert_eq!(access_type_of(&explain, 0), "index");
    assert_eq!(key_of(&explain, 0), "idx_abc");
    assert!(extra_of(&explain, 0).contains("INDEX_FFS(idx_abc)"));
}

#[test]
fn index_ffs_hint_ignored_when_index_does_not_cover() {
    let (_dir, engine) = open_engine();
    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY, a INT, b INT, c INT, INDEX idx_a (a))")
        .unwrap();
    engine
        .execute("INSERT INTO t VALUES (1, 1, 10, 100), (2, 2, 20, 200)")
        .unwrap();

    // idx_a only contains column `a`, but the projection asks for `b`
    // and `c` — the hint cannot be honoured. Planner must fall back to
    // a regular plan rather than producing wrong results, and the rows
    // returned must still be correct.
    let QueryResponse::ResultSet(rs) = engine
        .execute("SELECT /*+ INDEX_FFS(t idx_a) */ b, c FROM t ORDER BY b")
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(rs.rows.len(), 2);
    assert_eq!(rs.rows[0][0], Value::Int(10));
    assert_eq!(rs.rows[0][1], Value::Int(100));
    assert_eq!(rs.rows[1][0], Value::Int(20));
    assert_eq!(rs.rows[1][1], Value::Int(200));
}

/// Read the `Create Table` text from `SHOW CREATE TABLE`.
fn show_create(engine: &StorageQueryEngine, table: &str) -> String {
    let QueryResponse::ResultSet(rs) = engine
        .execute(&format!("SHOW CREATE TABLE {table}"))
        .unwrap()
    else {
        panic!("expected result set");
    };
    match &rs.rows[0][1] {
        Value::String(s) => s.clone(),
        other => panic!("unexpected create-table cell: {other:?}"),
    }
}

#[test]
fn inline_create_table_index_include_is_covering() {
    let (_dir, engine) = open_engine();
    // sqlparser cannot parse INCLUDE in an inline key; the parser strips
    // it and re-attaches via sentinel. The resulting index must cover v.
    engine
        .execute(
            "CREATE TABLE t (id INT PRIMARY KEY, k INT NOT NULL, v INT NOT NULL, \
             INDEX k_with_v (k) INCLUDE (v))",
        )
        .unwrap();
    engine
        .execute("INSERT INTO t VALUES (1, 10, 100), (2, 20, 200), (3, 30, 300)")
        .unwrap();

    let QueryResponse::ResultSet(explain) = engine
        .execute("EXPLAIN SELECT k, v FROM t WHERE k = 20")
        .unwrap()
    else {
        panic!("expected explain result set");
    };
    assert!(
        extra_of(&explain, 0).contains("Using index"),
        "inline INCLUDE should make the scan covering, got {:?}",
        extra_of(&explain, 0)
    );
    assert_eq!(key_of(&explain, 0), "k_with_v");

    let QueryResponse::ResultSet(rs) = engine.execute("SELECT k, v FROM t WHERE k = 20").unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(rs.rows.len(), 1);
    assert_eq!(rs.rows[0][0], Value::Int(20));
    assert_eq!(rs.rows[0][1], Value::Int(200));
}

#[test]
fn alter_add_index_include_is_covering() {
    let (_dir, engine) = open_engine();
    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY, k INT NOT NULL, v INT NOT NULL)")
        .unwrap();
    engine
        .execute("ALTER TABLE t ADD INDEX k_with_v (k) INCLUDE (v)")
        .unwrap();
    engine
        .execute("INSERT INTO t VALUES (1, 10, 100), (2, 20, 200)")
        .unwrap();

    let QueryResponse::ResultSet(explain) = engine
        .execute("EXPLAIN SELECT k, v FROM t WHERE k = 10")
        .unwrap()
    else {
        panic!("expected explain result set");
    };
    assert!(
        extra_of(&explain, 0).contains("Using index"),
        "ALTER ADD INDEX INCLUDE should be covering, got {:?}",
        extra_of(&explain, 0)
    );

    let QueryResponse::ResultSet(rs) = engine.execute("SELECT k, v FROM t WHERE k = 10").unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(rs.rows[0][1], Value::Int(100));
}

#[test]
fn inline_include_binds_to_correct_index_with_interleaved_keys() {
    // Robustness: a column-level UNIQUE prepends an entry to the table's
    // index list, and a plain index sits between two INCLUDE indexes.
    // Sentinel binding must attach each INCLUDE to its own index — an
    // ordinal-based scheme would misattribute here.
    let (_dir, engine) = open_engine();
    engine
        .execute(
            "CREATE TABLE t (\
                id INT PRIMARY KEY, \
                u INT UNIQUE, \
                a INT NOT NULL, av INT NOT NULL, \
                b INT NOT NULL, \
                c INT NOT NULL, cv INT NOT NULL, \
                INDEX ia (a) INCLUDE (av), \
                INDEX ib (b), \
                INDEX ic (c) INCLUDE (cv))",
        )
        .unwrap();
    let create = show_create(&engine, "t");
    assert!(
        create.contains("KEY `ia` (`a`) INCLUDE (`av`)"),
        "ia INCLUDE missing/misbound: {create}"
    );
    assert!(
        create.contains("KEY `ib` (`b`)") && !create.contains("`ib` (`b`) INCLUDE"),
        "ib must have no INCLUDE: {create}"
    );
    assert!(
        create.contains("KEY `ic` (`c`) INCLUDE (`cv`)"),
        "ic INCLUDE missing/misbound: {create}"
    );

    engine
        .execute("INSERT INTO t VALUES (1, 7, 10, 100, 20, 30, 300)")
        .unwrap();
    // ic covers cv.
    let QueryResponse::ResultSet(explain) = engine
        .execute("EXPLAIN SELECT c, cv FROM t WHERE c = 30")
        .unwrap()
    else {
        panic!("expected explain result set");
    };
    assert_eq!(key_of(&explain, 0), "ic");
    assert!(extra_of(&explain, 0).contains("Using index"));
}

#[test]
fn show_create_table_include_round_trips() {
    let (_dir, engine) = open_engine();
    engine
        .execute(
            "CREATE TABLE t (id INT PRIMARY KEY, k INT NOT NULL, a INT NOT NULL, b INT NOT NULL, \
             INDEX k_idx (k) INCLUDE (a, b))",
        )
        .unwrap();
    let create = show_create(&engine, "t");
    assert!(
        create.contains("KEY `k_idx` (`k`) INCLUDE (`a`, `b`)"),
        "SHOW CREATE must echo INCLUDE: {create}"
    );

    // Re-running the emitted DDL on a fresh table must succeed and yield
    // the same INCLUDE clause (full round-trip through the inline strip).
    let recreated = create.replace("`t`", "`t2`");
    engine.execute(&recreated).unwrap();
    let create2 = show_create(&engine, "t2");
    assert!(create2.contains("KEY `k_idx` (`k`) INCLUDE (`a`, `b`)"));
}

#[test]
fn inline_unique_index_rejects_include() {
    let (_dir, engine) = open_engine();
    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY, k INT NOT NULL, v INT)")
        .unwrap();
    let err = engine
        .execute(
            "CREATE TABLE u (id INT PRIMARY KEY, k INT NOT NULL, v INT, UNIQUE uk (k) INCLUDE (v))",
        )
        .unwrap_err();
    assert!(
        format!("{err:?}").to_uppercase().contains("INCLUDE"),
        "inline UNIQUE INCLUDE should be rejected, got {err:?}"
    );
}

#[test]
fn alter_add_unique_index_rejects_include() {
    let (_dir, engine) = open_engine();
    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY, k INT NOT NULL, v INT)")
        .unwrap();
    let err = engine
        .execute("ALTER TABLE t ADD UNIQUE INDEX uk (k) INCLUDE (v)")
        .unwrap_err();
    assert!(
        format!("{err:?}").to_uppercase().contains("INCLUDE"),
        "ALTER ADD UNIQUE INCLUDE should be rejected, got {err:?}"
    );
}

#[test]
fn inline_include_unknown_column_errors() {
    let (_dir, engine) = open_engine();
    let err = engine
        .execute(
            "CREATE TABLE t (id INT PRIMARY KEY, k INT NOT NULL, \
             INDEX k_idx (k) INCLUDE (nonexistent))",
        )
        .unwrap_err();
    let msg = format!("{err:?}");
    assert!(
        msg.contains("nonexistent") || msg.to_lowercase().contains("column"),
        "unknown INCLUDE column should error, got {err:?}"
    );
}

#[test]
fn standalone_create_index_include_still_works() {
    // Regression guard: the native sqlparser CREATE INDEX INCLUDE path
    // must be untouched by the inline strip (which head-gates to
    // CREATE TABLE / ALTER only).
    let (_dir, engine) = open_engine();
    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY, k INT NOT NULL, v INT NOT NULL)")
        .unwrap();
    engine
        .execute("CREATE INDEX k_with_v ON t (k) INCLUDE (v)")
        .unwrap();
    let create = show_create(&engine, "t");
    assert!(
        create.contains("KEY `k_with_v` (`k`) INCLUDE (`v`)"),
        "standalone INCLUDE must persist + echo: {create}"
    );
}

#[test]
fn constant_predicates_and_exact_index_residuals() {
    let (_dir, engine) = open_engine();
    engine
        .execute(
            "CREATE TABLE t (id BIGINT UNSIGNED PRIMARY KEY, v INT, KEY vi(v,id), KEY iv(id,v))",
        )
        .unwrap();
    engine
        .execute("INSERT INTO t VALUES (1,NULL),(2,20),(3,30),(4,40)")
        .unwrap();
    for predicate in [
        "1=1 AND id >= '2' AND id < '4'",
        "2 AND id >= 2 AND id < 4 AND 1",
        "(2+2=4) AND (id >= 2 AND 3>2) AND id < 4",
        "id >= 2 AND id < 4 AND (NULL OR TRUE)",
    ] {
        let QueryResponse::ResultSet(rs) = engine
            .execute(&format!("SELECT COUNT(*) FROM t WHERE {predicate}"))
            .unwrap()
        else {
            panic!()
        };
        assert_eq!(rs.rows[0][0], Value::Int(2), "{predicate}");
        let QueryResponse::ResultSet(rs) = engine
            .execute(&format!(
                "EXPLAIN ANALYZE SELECT COUNT(*) FROM t FORCE INDEX(PRIMARY) WHERE {predicate}"
            ))
            .unwrap()
        else {
            panic!()
        };
        let tree = format!("{:?}", rs.rows);
        assert!(
            tree.contains("aggregate_mode=ordered_index"),
            "{predicate}: {tree}"
        );
        assert!(!tree.contains("filter:"), "{predicate}: {tree}");
    }
    let QueryResponse::ResultSet(rs) = engine.execute("EXPLAIN FORMAT=TREE SELECT id FROM t FORCE INDEX(iv) WHERE id >= 2 AND id < 4 AND v=30").unwrap() else { panic!() };
    let tree = format!("{:?}", rs.rows);
    assert!(tree.contains("filter: v=30"), "{tree}");
    assert!(!tree.contains("filter: id"), "{tree}");
    for (predicate, expected) in [
        ("1=1", 4),
        ("1=0", 0),
        ("NULL AND TRUE", 0),
        ("v=v", 3),
        ("TRUE AND (v=20 OR FALSE)", 1),
        ("id >= 2 AND id < 4 AND v=30", 1),
    ] {
        let QueryResponse::ResultSet(rs) = engine
            .execute(&format!("SELECT COUNT(*) FROM t WHERE {predicate}"))
            .unwrap()
        else {
            panic!()
        };
        assert_eq!(rs.rows[0][0], Value::Int(expected), "{predicate}");
    }
}

#[test]
fn exact_index_residuals_preserve_null_prefix_and_lossy_casts() {
    let (_dir, engine) = open_engine();
    engine.execute("CREATE TABLE r (id INT PRIMARY KEY, a INT NULL, b INT, s VARCHAR(20), d DECIMAL(8,2), f FLOAT, KEY ab(a,b), KEY sp(s(2)), KEY di(d), KEY fi(f))").unwrap();
    engine.execute("INSERT INTO r VALUES (1,NULL,3,'abcd',1.20,1.5),(2,2,3,'abxy',1.21,2.5),(3,2,4,'acde',2.00,3.5),(4,3,5,'abcd',2.50,4.5)").unwrap();
    for (index, predicate, expected) in [
        ("ab", "a < 3", vec![2, 3]),
        ("ab", "a=2 AND b>=3 AND b<4", vec![2]),
        ("sp", "s='abcd'", vec![1, 4]),
        ("di", "d>=1.205 AND d<2", vec![2]),
        ("fi", "f>2.50000001", vec![3, 4]),
        ("PRIMARY", "id>=2 AND id<4", vec![2, 3]),
        ("PRIMARY", "id=2", vec![2]),
    ] {
        let sql = format!("SELECT id FROM r FORCE INDEX({index}) WHERE {predicate} ORDER BY id");
        let QueryResponse::ResultSet(rs) = engine.execute(&sql).unwrap() else {
            panic!()
        };
        let ids: Vec<_> = rs.rows.iter().map(|r| r[0].clone()).collect();
        assert_eq!(
            ids,
            expected.into_iter().map(Value::Int).collect::<Vec<_>>(),
            "{sql}"
        );
        let QueryResponse::ResultSet(rs) = engine
            .execute(&format!("EXPLAIN FORMAT=TREE {sql}"))
            .unwrap()
        else {
            panic!()
        };
        let tree = format!("{:?}", rs.rows);
        if matches!(index, "sp" | "di" | "fi") || predicate == "a < 3" {
            assert!(tree.contains("filter:"), "{sql}: {tree}");
        } else if index == "PRIMARY" {
            assert!(!tree.contains("filter:"), "{sql}: {tree}");
        }
    }
    for (predicate, expected) in [
        ("d=1.205", 0),
        ("1=1 AND d=1.205", 0),
        ("a<3", 2),
        ("1=1 AND a<3", 2),
    ] {
        let QueryResponse::ResultSet(rs) = engine
            .execute(&format!("SELECT COUNT(*) FROM r WHERE {predicate}"))
            .unwrap()
        else {
            panic!()
        };
        assert_eq!(rs.rows[0][0], Value::Int(expected), "{predicate}");
    }
    for sql in [
        "SELECT COUNT(*) FROM r FORCE INDEX(di) WHERE d>=1.20",
        "SELECT COUNT(*) FROM r FORCE INDEX(fi) WHERE f>=1.5",
    ] {
        let QueryResponse::ResultSet(rs) =
            engine.execute(&format!("EXPLAIN ANALYZE {sql}")).unwrap()
        else {
            panic!()
        };
        assert!(
            format!("{:?}", rs.rows).contains("aggregate_mode=ordered_index"),
            "{sql}: {:?}",
            rs.rows
        );
    }
    engine
        .execute("CREATE TABLE temporal_range (d DATE NOT NULL, KEY di(d))")
        .unwrap();
    engine
        .execute("INSERT INTO temporal_range VALUES ('2024-01-01'),('2024-01-02')")
        .unwrap();
    let QueryResponse::ResultSet(rs) = engine.execute("EXPLAIN ANALYZE SELECT COUNT(*) FROM temporal_range FORCE INDEX(di) WHERE 1=1 AND d>='2024-01-01'").unwrap() else { panic!() };
    assert!(
        format!("{:?}", rs.rows).contains("aggregate_mode=ordered_index"),
        "{:?}",
        rs.rows
    );
    // SQL preparation must not bake one execution's parameter domain or value
    // into a cached proof of exactness.
    let mut session = SessionContext::default();
    engine.execute_with_session(&mut session, "PREPARE p FROM 'SELECT id FROM r FORCE INDEX(PRIMARY) WHERE 1=1 AND id >= ? AND id < ? ORDER BY id'").unwrap();
    for (lo, hi, expected) in [
        ("2", "4", vec![2, 3]),
        ("3", "5", vec![3, 4]),
        ("'2.5'", "'5'", vec![3, 4]),
    ] {
        engine
            .execute_with_session(&mut session, &format!("SET @lo={lo}, @hi={hi}"))
            .unwrap();
        let QueryResponse::ResultSet(rs) = engine
            .execute_with_session(&mut session, "EXECUTE p USING @lo,@hi")
            .unwrap()
        else {
            panic!()
        };
        assert_eq!(
            rs.rows,
            expected
                .into_iter()
                .map(|v| vec![Value::Int(v)])
                .collect::<Vec<_>>()
        );
    }
}
